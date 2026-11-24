/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.upgrade;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import com.vdurmont.semver4j.Semver;
import org.junit.Assume;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.distributed.shared.Versions.Version;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_APACHE_DTEST_JAR_DIR;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.impl.AbstractCluster.CURRENT_VERSION;
import static org.awaitility.Awaitility.await;

/**
 * Base for tests of clusters that mix nodes of this build with Apache Cassandra nodes, connected by real internode
 * and native connections. The Apache nodes run from the single dtest jar in the directory named by
 * {@code cassandra.test.apache_dtest_jar_dir}; without that property the tests are skipped.
 */
public abstract class ApacheMixedClusterTestBase extends UpgradeTestBase
{
    private static final Pattern TABLE_OPTIONS = Pattern.compile("\\) WITH (CLUSTERING ORDER BY \\([^)]*\\))?.*", Pattern.DOTALL);

    /**
     * The Apache Cassandra version of the dtest jar in the configured directory.
     */
    protected static Version apacheVersion()
    {
        String dir = TEST_APACHE_DTEST_JAR_DIR.getString();
        Assume.assumeTrue("No Apache dtest jar directory set with -D" + TEST_APACHE_DTEST_JAR_DIR.getKey(), dir != null && !dir.isEmpty());

        File[] jars = new File(dir).listFiles((d, name) -> name.startsWith("dtest-") && name.endsWith(".jar"));
        if (jars == null || jars.length != 1)
            throw new IllegalStateException("Expected one dtest jar in " + dir + ", found " + (jars == null ? 0 : jars.length));

        // Versions#find keys a jar by its version without suffixes such as -SNAPSHOT
        String name = jars[0].getName();
        Semver version = new Semver(name.substring("dtest-".length(), name.length() - ".jar".length()), Semver.SemverType.LOOSE);
        return Versions.find(new File(dir)).getLatest(version);
    }

    /**
     * Starts a cluster whose node {@code n} runs {@code versions[n - 1]}, with tokens and topology for
     * {@code maxNodes} nodes so that later nodes can be added with {@link #bootstrapNode}. {@link #configureFor}
     * applies after {@code configUpdater} on every node.
     */
    protected static UpgradeableCluster startCluster(int maxNodes, Consumer<IInstanceConfig> configUpdater, Version... versions) throws IOException
    {
        UpgradeableCluster cluster = UpgradeableCluster.build(1)
                                                       .withVersion(versions[0])
                                                       .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(maxNodes))
                                                       .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(maxNodes, "dc0", "rack0"))
                                                       .withConfig(config -> {
                                                           config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL);
                                                           configUpdater.accept(config);
                                                           if (config.num() == 1)
                                                               configureFor(config, versions[0]);
                                                       })
                                                       .createWithoutStarting();
        for (int i = 1; i < versions.length; i++)
        {
            IInstanceConfig config = cluster.newInstanceConfig();
            configureFor(config, versions[i]);
            cluster.bootstrap(config, versions[i]);
        }
        cluster.startup();
        return cluster;
    }

    /**
     * Adds a node running {@code version} and bootstraps it into the ring.
     */
    protected static IUpgradeableInstance bootstrapNode(UpgradeableCluster cluster, Version version)
    {
        IInstanceConfig config = cluster.newInstanceConfig();
        configureFor(config, version);
        config.set("auto_bootstrap", true);
        IUpgradeableInstance instance = cluster.bootstrap(config, version);
        instance.startup(cluster);
        return instance;
    }

    /**
     * Runs a schema statement on node 1 and waits until every running node has the same definition of {@code keyspace}
     * in its schema, apart from table options and {@code frozen<>}, which the two kinds of node don't all print alike.
     * Unlike {@link UpgradeableCluster#schemaChange(String)} it doesn't wait for equal schema versions, which nodes of
     * this build and Apache nodes don't compute alike.
     */
    protected static void schemaChange(UpgradeableCluster cluster, String keyspace, String query)
    {
        cluster.coordinator(1).execute(query, ConsistencyLevel.ALL);
        await().atMost(1, TimeUnit.MINUTES)
               .until(() -> cluster.stream()
                                   .filter(instance -> !instance.isShutdown())
                                   .allMatch(instance -> describe(instance, keyspace).equals(describe(cluster.get(1), keyspace))));
    }

    private static List<String> describe(IUpgradeableInstance instance, String keyspace)
    {
        // DESCRIBE reads the schema a node has loaded, not the schema tables, which it writes first
        List<String> statements = new ArrayList<>();
        for (Object[] row : instance.executeInternal("DESCRIBE SCHEMA"))
        {
            if (row[0].equals(keyspace))
                statements.add(withoutFrozen(TABLE_OPTIONS.matcher((String) row[3]).replaceFirst(")$1")));
        }
        return statements;
    }

    /**
     * Removes the {@code frozen<>} around types, which this build leaves out in some places where Apache prints it, such as
     * around tuples.
     */
    private static String withoutFrozen(String statement)
    {
        StringBuilder builder = new StringBuilder(statement);
        for (int start = builder.indexOf("frozen<"); start >= 0; start = builder.indexOf("frozen<", start))
        {
            int end = start + "frozen<".length();
            for (int depth = 0; depth > 0 || builder.charAt(end) != '>'; end++)
            {
                if (builder.charAt(end) == '<')
                    depth++;
                else if (builder.charAt(end) == '>')
                    depth--;
            }
            builder.deleteCharAt(end).delete(start, start + "frozen<".length());
        }
        return builder.toString();
    }

    /**
     * Replaces settings of this build's test configuration that the Apache node of {@code version} doesn't accept.
     */
    protected static void configureFor(IInstanceConfig config, Version version)
    {
        if (version != CURRENT_VERSION)
            config.set("storage_compatibility_mode", "NONE");
    }
}
