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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_5_MODE_ENABLED;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.impl.AbstractCluster.CURRENT_VERSION;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A node of this build in storage_compatibility_mode CASSANDRA_5 beside an Apache node: messaging version 13, Apache's
 * schema tables, equal schema versions, and batches and repair across both kinds of node.
 */
public class Cassandra5ModeMixedClusterTest extends ApacheMixedClusterTestBase
{
    // 20 years, so that the cells expire after 2038
    private static final int MAX_TTL = 630720000;

    @Test
    @SuppressWarnings("unchecked")
    public void testModeNodeBesideApacheNode() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_5_MODE_ENABLED, true);
             UpgradeableCluster cluster = startCluster(2,
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
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_5_MODE_ENABLED, true);
             UpgradeableCluster cluster = startCluster(3,
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
}
