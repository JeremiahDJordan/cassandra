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

package org.apache.cassandra.distributed.test.sai;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertTrue;

/**
 * A replica purges the tombstones older than gc_grace_seconds before it counts the tombstones of a read, so a read
 * fails on tombstones only for those it can't purge. With a filter that has a static expression, SAI holds or drops
 * the deletions before the first live match of a partition whose static row doesn't satisfy the filter, and counts
 * them: it must not count those that the read would purge.
 * <p>
 * Checked at ALL and ONE, with and without indexes, with gc_grace_seconds = 0, where the read returns the live row, and
 * with the default, where it fails. The tests run with non-strict replica filtering disabled and enabled.
 */
@RunWith(Parameterized.class)
public class StaticDisjunctionPurgeableTombstonesTest extends TestBaseImpl
{
    private static final int ROWS = 1001;

    @Parameterized.Parameter
    public boolean nonStrict;

    @Parameterized.Parameters(name = "nonStrict={0}")
    public static Collection<Object[]> parameters()
    {
        return SAIUtil.nonStrictReplicaFilteringModes();
    }

    @Test
    public void testPurgeableTombstonesAreNotCounted() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config ->
                                                       SAIUtil.withNonStrictReplicaFiltering(config, nonStrict)
                                                              .set("hinted_handoff_enabled", false)
                                                              .set("read_request_timeout", "60s")
                                                              .set("range_request_timeout", "60s")
                                                              .with(GOSSIP).with(NETWORK))
                                           .start(), 3))
        {
            cluster.forEach(instance -> instance.runOnInstance(() -> DatabaseDescriptor.getGuardrailsConfig().setTombstonesThreshold(100, 1000)));
            List<String> tables = new ArrayList<>();
            for (String gc : new String[]{ "gc0", "gcdefault" })
            {
                for (String kind : new String[]{ "plain", "indexed" })
                {
                    String table = KEYSPACE + '.' + kind + '_' + gc;
                    tables.add(table);
                    cluster.schemaChange("CREATE TABLE " + table + " (pk int, ck int, v int, st int static, PRIMARY KEY (pk, ck))" +
                                         (gc.equals("gc0") ? " WITH gc_grace_seconds = 0" : ""));
                    if (kind.equals("indexed"))
                        for (String column : new String[]{ "ck", "v", "st" })
                            cluster.schemaChange("CREATE CUSTOM INDEX ON " + table + '(' + column + ") USING 'StorageAttachedIndex'");
                }
            }
            SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);

            for (String table : tables)
            {
                for (int pk = 1; pk <= 2; pk++)
                    cluster.coordinator(1).execute("INSERT INTO " + table + " (pk, st) VALUES (?, 0)", ConsistencyLevel.ALL, pk);
                for (int ck = 0; ck < ROWS; ck++)
                {
                    cluster.coordinator(1).execute("DELETE FROM " + table + " WHERE pk = 1 AND ck = ?", ConsistencyLevel.ALL, ck);
                    cluster.coordinator(1).execute("INSERT INTO " + table + " (pk, ck, v) VALUES (2, ?, 1) USING TTL 1", ConsistencyLevel.ALL, ck);
                }
                for (int pk = 1; pk <= 2; pk++)
                    cluster.coordinator(1).execute("INSERT INTO " + table + " (pk, ck, v) VALUES (?, 2000, 1)", ConsistencyLevel.ALL, pk);
            }
            // purgeable once older than gc_grace_seconds, in seconds
            Thread.sleep(2500);

            List<String> failures = new ArrayList<>();
            check(cluster, tables, "memtable", failures);
            cluster.forEach(instance -> instance.flush(KEYSPACE));
            check(cluster, tables, "sstable", failures);
            assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
        }
    }

    private static void check(Cluster cluster, List<String> tables, String source, List<String> failures)
    {
        for (String table : tables)
        {
            boolean purgeable = table.endsWith("_gc0");
            for (int pk = 1; pk <= 2; pk++)
            {
                String query = "SELECT pk, ck FROM " + table + " WHERE pk = " + pk + " AND (st = 5 OR ck >= 0) ALLOW FILTERING";
                for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.ALL, ConsistencyLevel.ONE })
                {
                    String expected = purgeable ? "[[" + pk + ", 2000]]" : "TombstoneOverwhelmingException";
                    String actual;
                    try
                    {
                        actual = Arrays.deepToString(cluster.coordinator(1).execute(query, consistency));
                    }
                    catch (Throwable t)
                    {
                        // a replica's failure reaches the coordinator as a read failure with the reason
                        actual = String.valueOf(t).contains("READ_TOO_MANY_TOMBSTONES") || String.valueOf(t).contains("TombstoneOverwhelming")
                                 ? "TombstoneOverwhelmingException" : "failed: " + t;
                    }
                    if (!actual.equals(expected))
                        failures.add(String.format("[%s %s] %s at %s: expected %s but got %s", source, table, query, consistency, expected, actual));
                }
            }
        }
    }
}
