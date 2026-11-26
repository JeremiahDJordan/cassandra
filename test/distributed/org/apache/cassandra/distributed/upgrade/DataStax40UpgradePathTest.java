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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.vdurmont.semver4j.Semver;
import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.distributed.shared.Versions.Version;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_DATASTAX_40_DTEST_JAR_DIR;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.impl.AbstractCluster.CURRENT_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * DataStax 4.0 nodes moved along the upgrade procedure keep their node metadata. The 4.0 nodes run from the single
 * dtest jar in the directory named by {@code cassandra.test.datastax_40_dtest_jar_dir}; without that property the
 * tests are skipped.
 */
public class DataStax40UpgradePathTest extends UpgradeTestBase
{
    private static final int NODES = 2;
    private static final int ROWS = 10;

    @Test
    public void testRollbackFromHcd1() throws Throwable
    {
        Version v40 = dataStax40Version();
        try (UpgradeableCluster cluster = startCluster(v40))
        {
            Map<Integer, List<Object>> ids = nodeIds(cluster);
            for (int node = NODES; node >= 1; node--)
            {
                restart(cluster.get(node), CURRENT_VERSION, "HCD_1");
                assertNodeMetadata(cluster, ids, ROWS);
            }
            for (int node = NODES; node >= 1; node--)
            {
                restart(cluster.get(node), v40, null);
                assertNodeMetadata(cluster, ids, ROWS);
            }

            // upgraded again, the nodes migrate what changed on 4.0
            cluster.coordinator(1).execute(withKeyspace("TRUNCATE %s.tbl"), ALL);
            for (int node = NODES; node >= 1; node--)
            {
                restart(cluster.get(node), CURRENT_VERSION, "HCD_1");
                assertTruncationRecorded(cluster.get(node));
                assertNodeMetadata(cluster, ids, 0);
            }
        }
    }

    @Test
    public void testForwardThroughUpgrading() throws Throwable
    {
        try (UpgradeableCluster cluster = startCluster(dataStax40Version()))
        {
            Map<Integer, List<Object>> ids = nodeIds(cluster);
            for (int node = NODES; node >= 1; node--)
                restart(cluster.get(node), CURRENT_VERSION, "HCD_1");
            cluster.coordinator(1).execute(withKeyspace("TRUNCATE %s.tbl"), ALL);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (0, 0)"), ALL);

            for (String mode : new String[]{ "UPGRADING", "NONE" })
            {
                for (int node = NODES; node >= 1; node--)
                {
                    IUpgradeableInstance instance = cluster.get(node);
                    restart(instance, CURRENT_VERSION, mode);
                    assertFalse(Files.exists(metadataDirectory(instance).resolve("nodes")));
                    assertTrue(Files.isDirectory(metadataDirectory(instance).resolve("nodes.migrated")));
                    // the truncation made in HCD_1 stays recorded, so the truncated writes aren't replayed from the commit log
                    assertTruncationRecorded(instance);
                    assertNodeMetadata(cluster, ids, 1);
                }
            }
        }
    }

    private static UpgradeableCluster startCluster(Version v40) throws Throwable
    {
        UpgradeableCluster cluster = UpgradeableCluster.build(NODES)
                                                       .withVersion(v40)
                                                       .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                                       // DataStax 4.0's NativeLibrary classes have other method signatures than this build's
                                                       .withSharedClasses(name -> !name.startsWith("org.apache.cassandra.utils.NativeLibrary") && AbstractCluster.SHARED_PREDICATE.test(name))
                                                       .start();
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + NODES + '}'));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
        for (int pk = 0; pk < ROWS; pk++)
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)"), ALL, pk, pk);
        return cluster;
    }

    private static void restart(IUpgradeableInstance instance, Version version, String mode) throws Exception
    {
        instance.shutdown().get();
        if (mode == null)
            ((InstanceConfig) instance.config()).remove("storage_compatibility_mode");
        else
            instance.config().set("storage_compatibility_mode", mode);
        instance.setVersion(version);
        instance.startup();
    }

    /**
     * The host id and tokens of each node.
     */
    private static Map<Integer, List<Object>> nodeIds(UpgradeableCluster cluster)
    {
        Map<Integer, List<Object>> ids = new HashMap<>();
        cluster.forEach(instance -> ids.put(instance.config().num(), Arrays.asList(instance.executeInternal("SELECT host_id, tokens FROM system.local")[0])));
        return ids;
    }

    private static void assertNodeMetadata(UpgradeableCluster cluster, Map<Integer, List<Object>> ids, int rows)
    {
        assertEquals(ids, nodeIds(cluster));
        cluster.forEach(instance -> {
            Set<List<Object>> peerIds = new HashSet<>();
            for (Object[] row : instance.executeInternal("SELECT host_id, tokens FROM system.peers_v2"))
                peerIds.add(Arrays.asList(row));
            Set<List<Object>> expected = new HashSet<>(ids.values());
            expected.remove(ids.get(instance.config().num()));
            assertEquals(expected, peerIds);
            assertEquals(rows, instance.coordinator().execute(withKeyspace("SELECT pk FROM %s.tbl"), ALL).length);
        });
    }

    private static void assertTruncationRecorded(IUpgradeableInstance instance)
    {
        Object tableId = instance.executeInternal("SELECT id FROM system_schema.tables WHERE keyspace_name = ? AND table_name = 'tbl'", KEYSPACE)[0][0];
        Map<?, ?> truncations = (Map<?, ?>) instance.executeInternal("SELECT truncated_at FROM system.local")[0][0];
        assertTrue(truncations.containsKey(tableId));
    }

    private static Path metadataDirectory(IUpgradeableInstance instance)
    {
        return Paths.get((String) instance.config().get("metadata_directory"));
    }

    private static Version dataStax40Version()
    {
        String dir = TEST_DATASTAX_40_DTEST_JAR_DIR.getString();
        Assume.assumeTrue("No dtest jar directory set with -D" + TEST_DATASTAX_40_DTEST_JAR_DIR.getKey(), dir != null && !dir.isEmpty());

        File[] jars = new File(dir).listFiles((d, name) -> name.startsWith("dtest-") && name.endsWith(".jar"));
        if (jars == null || jars.length != 1)
            throw new IllegalStateException("Expected one dtest jar in " + dir + ", found " + (jars == null ? 0 : jars.length));

        // Versions#find keys a jar by its version without suffixes such as -SNAPSHOT
        String name = jars[0].getName();
        Semver version = new Semver(name.substring("dtest-".length(), name.length() - ".jar".length()), Semver.SemverType.LOOSE);
        return Versions.find(new File(dir)).getLatest(version);
    }
}
