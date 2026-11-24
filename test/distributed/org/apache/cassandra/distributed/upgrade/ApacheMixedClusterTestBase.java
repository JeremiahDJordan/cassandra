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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Assume;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.distributed.shared.Versions.Version;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_APACHE_DTEST_JAR_DIR;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.awaitility.Awaitility.await;

/**
 * Base for tests of clusters that mix nodes of this build with Apache Cassandra nodes, connected by real internode
 * and native connections. The Apache nodes run from the single dtest jar in the directory named by
 * {@code cassandra.test.apache_dtest_jar_dir}; without that property the tests are skipped.
 */
public abstract class ApacheMixedClusterTestBase extends UpgradeTestBase
{
    protected static final Version CURRENT = AbstractCluster.CURRENT_VERSION;

    /**
     * The Apache Cassandra version of the dtest jar in the configured directory.
     */
    protected static Version apacheVersion()
    {
        String dir = TEST_APACHE_DTEST_JAR_DIR.getString();
        Assume.assumeTrue("No Apache dtest jar directory set with -D" + TEST_APACHE_DTEST_JAR_DIR.getKey(), dir != null);

        File[] jars = new File(dir).listFiles((d, name) -> name.startsWith("dtest-") && name.endsWith(".jar"));
        if (jars == null || jars.length != 1)
            throw new IllegalStateException("Expected one dtest jar in " + dir + ", found " + (jars == null ? 0 : jars.length));

        String name = jars[0].getName();
        return Versions.find(new File(dir)).get(name.substring("dtest-".length(), name.length() - ".jar".length()));
    }

    /**
     * Starts a cluster whose node {@code n} runs {@code versions[n - 1]}, with tokens and topology for
     * {@code maxNodes} nodes so that later nodes can be added with {@link #bootstrapNode}.
     */
    protected static UpgradeableCluster startCluster(int maxNodes, Consumer<IInstanceConfig> configUpdater, Version... versions) throws IOException
    {
        UpgradeableCluster cluster = UpgradeableCluster.build(1)
                                                       .withVersion(versions[0])
                                                       .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(maxNodes))
                                                       .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(maxNodes, "dc0", "rack0"))
                                                       .withConfig(config -> {
                                                           config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL);
                                                           if (config.num() == 1)
                                                               configureFor(config, versions[0]);
                                                           configUpdater.accept(config);
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
     * Runs a schema statement on node 1 and waits until every node has the same definition of {@code keyspace}. Unlike
     * {@link UpgradeableCluster#schemaChange(String)} it doesn't wait for equal schema versions, which nodes of this
     * build and Apache nodes don't compute alike.
     */
    protected static void schemaChange(UpgradeableCluster cluster, String keyspace, String query)
    {
        cluster.coordinator(1).execute(query, ConsistencyLevel.ALL);
        await().atMost(1, TimeUnit.MINUTES)
               .until(() -> cluster.stream().allMatch(instance -> describe(instance, keyspace).equals(describe(cluster.get(1), keyspace))));
    }

    private static List<List<Object>> describe(IUpgradeableInstance instance, String keyspace)
    {
        List<List<Object>> rows = new ArrayList<>();
        for (String query : new String[]{ "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?",
                                          "SELECT table_name, column_name, type FROM system_schema.columns WHERE keyspace_name = ?",
                                          "SELECT table_name, index_name, options FROM system_schema.indexes WHERE keyspace_name = ?" })
        {
            for (Object[] row : instance.executeInternal(query, keyspace))
                rows.add(Arrays.asList(row));
        }
        return rows;
    }

    /**
     * Replaces settings of this build's test configuration that the Apache node of {@code version} doesn't accept.
     */
    protected static void configureFor(IInstanceConfig config, Version version)
    {
        if (version != CURRENT)
            config.set("storage_compatibility_mode", "NONE");
    }
}
