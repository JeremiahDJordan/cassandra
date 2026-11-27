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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.io.sstable.ApacheReadability;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.impl.AbstractCluster.CURRENT_VERSION;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A node of this build in storage_compatibility_mode CASSANDRA_5 beside an Apache node: messaging version 13, Apache's
 * schema tables, equal schema versions, and batches, repair and analyzed index reads across both kinds of node.
 */
public class Cassandra5ModeMixedClusterTest extends ApacheMixedClusterTestBase
{
    // 20 years, so that the cells expire after 2038
    private static final int MAX_TTL = 630720000;

    @Test
    @SuppressWarnings("unchecked")
    public void testModeNodeBesideApacheNode() throws Throwable
    {
        try (UpgradeableCluster cluster = startCluster(2,
                                                       config -> {
                                                           config.set("user_defined_functions_enabled", true)
                                                                 .set("hinted_handoff_enabled", false);
                                                           if (config.num() == 2)
                                                               config.set("storage_compatibility_mode", "CASSANDRA_5");
                                                       },
                                                       apacheVersion(), CURRENT_VERSION))
        {
            IUpgradeableInstance apache = cluster.get(1);
            IUpgradeableInstance mode = cluster.get(2);
            assertEquals(MessagingService.VERSION_50, (int) ((IInvokableInstance) mode).callOnInstance(() -> MessagingService.current_version));

            String schemaColumns = "SELECT table_name, column_name, kind, position, type FROM system_schema.columns WHERE keyspace_name = 'system_schema'";
            assertArrayEquals(apache.executeInternal(schemaColumns), mode.executeInternal(schemaColumns));
            assertSchemaAgreement(cluster);

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"), false, mode);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))"), false, apache);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.counters (pk int PRIMARY KEY, c counter)"), false, mode);
            cluster.schemaChange(withKeyspace("CREATE FUNCTION %s.plus (a int, b int) CALLED ON NULL INPUT RETURNS int " +
                                              "LANGUAGE java AS 'return a == null || b == null ? null : a + b;'"), false, mode);
            assertSchemaAgreement(cluster);

            Map<String, String> compaction = (Map<String, String>) mode.executeInternal("SELECT compaction FROM system_schema.tables WHERE keyspace_name = ? AND table_name = 'counters'", KEYSPACE)[0][0];
            assertEquals("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy", compaction.get("class"));
            assertThatThrownBy(() -> mode.coordinator().execute(withKeyspace("ALTER TABLE %s.tbl WITH compaction = {'class': 'UnifiedCompactionStrategy', 'adaptive': 'false'}"), ALL))
            .hasMessageContaining("Apache Cassandra 5.0 doesn't support");

            // the batches stay in the batchlog of the other node, which serialized them, and only their replay writes
            // them to that node
            IMessageFilters.Filter keepBatches = cluster.filters().verbs(Verb.BATCH_REMOVE_REQ.id).drop();
            for (int node = 1; node <= 2; node++)
            {
                IMessageFilters.Filter skipReplica = cluster.filters().verbs(Verb.MUTATION_REQ.id).from(node).to(3 - node).drop();
                cluster.coordinator(node).execute("BEGIN BATCH " +
                                                  "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 0, 'a') USING TTL " + MAX_TTL + "; " +
                                                  "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 1, 'b'); " +
                                                  "APPLY BATCH", ONE, node, node + 10);
                assertEquals(0, cluster.get(3 - node).executeInternal(withKeyspace("SELECT ck FROM %s.tbl WHERE pk = ") + node).length);
                skipReplica.off();
                cluster.coordinator(node).execute("BEGIN COUNTER BATCH " +
                                                  "UPDATE " + KEYSPACE + ".counters SET c = c + 1 WHERE pk = 0; " +
                                                  "UPDATE " + KEYSPACE + ".counters SET c = c + 2 WHERE pk = ?; " +
                                                  "APPLY BATCH", ALL, node);
            }
            for (IUpgradeableInstance instance : new IUpgradeableInstance[]{ apache, mode })
                assertRows(instance.executeInternal("SELECT version FROM system.batches"), row(MessagingService.VERSION_50));
            keepBatches.off();
            for (int node = 1; node <= 2; node++)
            {
                IUpgradeableInstance replica = cluster.get(3 - node);
                String select = withKeyspace("SELECT ck FROM %s.tbl WHERE pk = ") + node;
                await().atMost(1, TimeUnit.MINUTES).until(() -> {
                    replica.nodetoolResult("replaybatchlog").asserts().success();
                    return replica.executeInternal(select).length == 1;
                });
            }

            cluster.coordinator(2).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (100, 0, 'x') IF NOT EXISTS"), ALL);
            assertRows(mode.executeInternal("SELECT most_recent_commit_version FROM system.paxos"), row(MessagingService.VERSION_50));

            for (int node = 1; node <= 2; node++)
            {
                for (int pk = 1; pk <= 2; pk++)
                {
                    Object[][] rows = cluster.coordinator(node).execute(withKeyspace("SELECT ck, v, ttl(v) FROM %s.tbl WHERE pk = ?"), ALL, pk);
                    assertEquals(1, rows.length);
                    assertTrue((int) rows[0][2] > MAX_TTL - 3600);
                    assertRows(cluster.coordinator(node).execute(withKeyspace("SELECT ck, v FROM %s.tbl WHERE pk = ?"), ALL, pk + 10), row(1, "b"));
                }
                for (int pk = 0; pk <= 2; pk++)
                    assertRows(cluster.coordinator(node).execute(withKeyspace("SELECT c FROM %s.counters WHERE pk = ?"), ALL, pk), row(2L));
            }

            // repair with a row on one replica only, from each side
            for (int node = 1; node <= 2; node++)
            {
                int pk = 200 + node;
                cluster.get(node).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?, 0, 'r')"), pk);
                cluster.get(node).nodetoolResult("repair", "-full", KEYSPACE).asserts().success();
                assertEquals(1, cluster.get(3 - node).executeInternal(withKeyspace("SELECT ck FROM %s.tbl WHERE pk = ?"), pk).length);
            }
        }
    }

    @Test
    public void testApacheNodeBootstrapsFromModeNode() throws Throwable
    {
        try (UpgradeableCluster cluster = startCluster(3,
                                                       config -> {
                                                           if (config.num() == 2)
                                                               config.set("storage_compatibility_mode", "CASSANDRA_5");
                                                       },
                                                       apacheVersion(), CURRENT_VERSION))
        {
            // with two replicas, the new node takes a range from each node, and that of the mode node from it
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"), false, cluster.get(2));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.indexed (pk int PRIMARY KEY, v text)"), false, cluster.get(2));
            cluster.schemaChange(withKeyspace("CREATE INDEX indexed_v ON %s.indexed (v) USING 'sai'"), false, cluster.get(2));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.plain (pk int PRIMARY KEY, v text)"), false, cluster.get(2));
            assertSchemaAgreement(cluster);
            for (int pk = 0; pk < 100; pk++)
            {
                cluster.coordinator(2).execute(withKeyspace("INSERT INTO %s.indexed (pk, v) VALUES (?, 'v')"), ALL, pk);
                cluster.coordinator(2).execute(withKeyspace("INSERT INTO %s.plain (pk, v) VALUES (?, 'v')"), ALL, pk);
            }
            cluster.forEach(instance -> instance.flush(KEYSPACE));

            IUpgradeableInstance apache = bootstrapNode(cluster, apacheVersion());
            assertTrue(apache.executeInternal(withKeyspace("SELECT pk FROM %s.indexed")).length > 0);
            assertTrue(apache.executeInternal(withKeyspace("SELECT pk FROM %s.plain")).length > 0);
            for (int node = 1; node <= 3; node++)
            {
                assertEquals(100, cluster.coordinator(node).execute(withKeyspace("SELECT pk FROM %s.indexed WHERE v = 'v'"), ONE).length);
                assertEquals(100, cluster.coordinator(node).execute(withKeyspace("SELECT pk FROM %s.plain"), ONE).length);
            }
        }
    }

    @Test
    public void testSchemaFromApacheNode() throws Throwable
    {
        try (UpgradeableCluster cluster = startCluster(3,
                                                       config -> {
                                                           if (config.num() > 1)
                                                               config.set("storage_compatibility_mode", "CASSANDRA_5");
                                                       },
                                                       apacheVersion(), CURRENT_VERSION))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"), false, cluster.get(1));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"), false, cluster.get(1));

            // a node in the mode bootstraps beside the Apache node and the mode node
            IUpgradeableInstance bootstrapped = bootstrapNode(cluster, CURRENT_VERSION);
            assertSchemaAgreement(cluster);

            // node 2, in UPGRADING after CASSANDRA_5, gets a change it missed from the Apache node alone
            IUpgradeableInstance upgrading = cluster.get(2);
            upgrading.shutdown().get();
            bootstrapped.shutdown().get();
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl WITH comment = 'from Apache'"), true, cluster.get(1));
            upgrading.config().set("storage_compatibility_mode", "UPGRADING");
            upgrading.startup();
            await().atMost(1, TimeUnit.MINUTES)
                   .until(() -> "from Apache".equals(upgrading.executeInternal("SELECT comment FROM system_schema.tables WHERE keyspace_name = ? AND table_name = 'tbl'", KEYSPACE)[0][0]));
        }
    }

    @Test
    public void testDecommissionOfNodeInUpgradingToApacheNode() throws Throwable
    {
        try (UpgradeableCluster cluster = startCluster(3,
                                                       config -> config.set("storage_compatibility_mode", "CASSANDRA_5"),
                                                       CURRENT_VERSION, CURRENT_VERSION))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"), false, cluster.get(1));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.indexed (pk int PRIMARY KEY, v text)"), false, cluster.get(1));
            cluster.schemaChange(withKeyspace("CREATE INDEX indexed_v ON %s.indexed (v) USING 'sai'"), false, cluster.get(1));

            // node 2 has left the mode, and its rows have indexes of our formats
            IUpgradeableInstance departing = cluster.get(2);
            departing.shutdown().get();
            departing.config().set("storage_compatibility_mode", "UPGRADING");
            departing.startup();
            for (int pk = 0; pk < 100; pk++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.indexed (pk, v) VALUES (?, 'v')"), ALL, pk);
            departing.flush(KEYSPACE);
            assertTrue(((IInvokableInstance) departing).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("indexed").getLiveSSTables().stream()
                                                                                    .anyMatch(sstable -> !ApacheReadability.hasCompleteAaGroups(sstable))));
            Set<Integer> departingKeys = keys(departing.executeInternal(withKeyspace("SELECT pk FROM %s.indexed WHERE v = 'v'")));
            assertFalse(departingKeys.isEmpty());

            // an Apache node takes node 2's range when node 2 is decommissioned
            IUpgradeableInstance apache = bootstrapNode(cluster, apacheVersion());
            Set<Integer> expected = keys(apache.executeInternal(withKeyspace("SELECT pk FROM %s.indexed")));
            expected.addAll(departingKeys);
            departing.nodetoolResult("decommission", "--force").asserts().success();
            assertEquals(expected, keys(apache.executeInternal(withKeyspace("SELECT pk FROM %s.indexed WHERE v = 'v'"))));
        }
    }

    @Test
    public void testAnalyzedRestrictionsOnFullPrimaryKey() throws Throwable
    {
        try (UpgradeableCluster cluster = startCluster(2,
                                                       config -> {
                                                           if (config.num() == 2)
                                                               config.set("storage_compatibility_mode", "CASSANDRA_5");
                                                       },
                                                       apacheVersion(), CURRENT_VERSION))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"), false, cluster.get(2));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, s set<text>, l list<text>, m map<text, text>, t text, v int)"), false, cluster.get(2));
            for (String target : Arrays.asList("s", "l", "keys(m)", "values(m)", "t"))
                cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.tbl (" + target + ") USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}"), false, cluster.get(2));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.tbl (v) USING 'legacy_local_table'"), false, cluster.get(2));
            assertSchemaAgreement(cluster);
            for (int pk = 0; pk < 20; pk++)
                cluster.coordinator(2).execute(withKeyspace("INSERT INTO %s.tbl (pk, s, l, m, t, v) VALUES (?, {'Apple'}, ['Apple'], {'Apple': 'Pear'}, 'Apple', 1)"), ALL, pk);

            // a row of each node, read from each node; Apache Cassandra 5.0 has no : operator, and its coordinators read
            // v = 1 AND s CONTAINS 'apple' through the legacy index, comparing raw values of s
            for (int replica = 1; replica <= 2; replica++)
            {
                int pk = (int) cluster.get(replica).executeInternal(withKeyspace("SELECT pk FROM %s.tbl LIMIT 1"))[0][0];
                for (int node = 1; node <= 2; node++)
                {
                    for (String restriction : Arrays.asList("t = 'apple'", "t : 'apple'", "s CONTAINS 'apple'", "l CONTAINS 'apple'", "m CONTAINS KEY 'apple'", "m CONTAINS 'pear'", "v = 1 AND s CONTAINS 'apple'"))
                    {
                        if (node == 1 && (restriction.contains(":") || restriction.contains("v = 1")))
                            continue;
                        String select = withKeyspace("SELECT pk FROM %s.tbl WHERE pk = ? AND ") + restriction + " ALLOW FILTERING";
                        assertRows(cluster.coordinator(node).execute(select, ONE, pk), row(pk));
                    }
                }
            }
            assertEquals(20, cluster.coordinator(2).execute(withKeyspace("SELECT pk FROM %s.tbl WHERE v = 1 AND s CONTAINS 'apple' ALLOW FILTERING"), ONE).length);
        }
    }

    private static Set<Integer> keys(Object[][] rows)
    {
        return Arrays.stream(rows).map(row -> (Integer) row[0]).collect(Collectors.toCollection(HashSet::new));
    }
}
