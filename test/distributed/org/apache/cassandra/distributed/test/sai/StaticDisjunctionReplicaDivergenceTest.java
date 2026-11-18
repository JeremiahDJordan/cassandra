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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertTrue;

/**
 * A replica doesn't return a partition that has no live row matching a filter with a disjunction of static and other
 * columns when its static row doesn't satisfy the filter on its own, even if the partition has range tombstones or
 * deleted rows (see {@code RowFilter#filterWithStaticRow} and SAI's {@code FilteringPartitionIterator}). A coordinator
 * that reconciles must still see the deletions that replica holds, through replica filtering protection, which reads
 * the rows other replicas returned, or the whole partition, from the replicas that were silent about them: no row that
 * is live on a stale replica and deleted on the others may come back.
 * <p>
 * Node 1 holds the stale data. The other replicas hold its deletion, newer: a row deletion, a range tombstone, a
 * partition deletion, an overwrite whose TTL expired, a deletion of the cells that a filter matches, and the newer
 * static values. With RF 2 the deletion is on node 2; with RF 3 on nodes 2 and 3, so that any QUORUM has it. Each query
 * is run at QUORUM and ALL, unpaged and paged, with LIMIT and PER PARTITION LIMIT, with and without indexes, with and
 * without read repair, from memtables and SSTables, against the same query on a table that holds only the live data.
 * The tests run with non-strict replica filtering disabled and enabled.
 */
@RunWith(Parameterized.class)
public class StaticDisjunctionReplicaDivergenceTest extends TestBaseImpl
{
    private static final String SCHEMA = " (pk int, ck int, v int, r set<int>, st int static, sx int static, " +
                                         "PRIMARY KEY (pk, ck))";

    private static final String[] RESTRICTIONS = {
        "st = 5 OR v = 1",
        // the stale replica has no match in 46 and 48, so it is silent and its whole partition is read
        "st = 5 OR v = 2",
        "st = 5 OR ck = 1",
        "st = 5 OR r NOT CONTAINS 3",
        "st = 0 OR v = 1",
        "(st = 0 AND v = 1) OR sx = 5",
        "st = 5 OR ck = 1 OR v = 2",
        "pk = 41 AND (st = 5 OR ck = 1)",
        "pk = 46 AND (st = 5 OR v = 1)",
    };

    private static final String[] LIMITS = { "", " LIMIT 3", " PER PARTITION LIMIT 1", " PER PARTITION LIMIT 1 LIMIT 4" };

    // the writes on every replica, the oldest
    private static final String[] BASE = {
        // live everywhere: partitions with matching rows, with and without a static row, and one with a static row only
        "INSERT INTO %s (pk, st, sx) VALUES (20, 0, 1) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (20, 1, 1, {1}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (20, 2, 2, {2}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, st, sx) VALUES (21, 0, 1) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (21, 1, 1, {1}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (22, 1, 1, {1}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (22, 2, 1, {1}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (23, 1, 1, {1}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, st, sx) VALUES (30, 5, 1) USING TIMESTAMP 1",
        // a static row that doesn't match st = 5 and a row matching v = 1, which the other replicas delete
        "INSERT INTO %s (pk, st, sx) VALUES (40, 0, 1) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (40, 1, 1, {1}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, st, sx) VALUES (41, 0, 1) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (41, 1, 1, {1}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (41, 2, 1, {1}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, st, sx) VALUES (42, 0, 1) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (42, 1, 1, {1}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, st, sx) VALUES (43, 0, 1) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (43, 1, 1, {1}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, st, sx) VALUES (44, 0, 1) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (44, 1, 1, {1}) USING TIMESTAMP 1",
        // a static row matching st = 5, and a row the other replicas delete
        "INSERT INTO %s (pk, st, sx) VALUES (45, 5, 1) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (45, 1, 1, {1}) USING TIMESTAMP 1",
        // a static row that only matches st = 5 on the other replicas, which also delete the rows
        "INSERT INTO %s (pk, st, sx) VALUES (46, 0, 1) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (46, 1, 1, {1}) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (46, 2, 1, {1}) USING TIMESTAMP 1",
        // a row whose matching cells the other replicas delete
        "INSERT INTO %s (pk, st, sx) VALUES (47, 0, 1) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (47, 1, 1, {1}) USING TIMESTAMP 1",
        // as 46, with a row deletion
        "INSERT INTO %s (pk, st, sx) VALUES (48, 0, 1) USING TIMESTAMP 1",
        "INSERT INTO %s (pk, ck, v, r) VALUES (48, 1, 1, {1}) USING TIMESTAMP 1",
    };

    // the writes on every replica but node 1, newer
    private static final String[] DELETIONS = {
        "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 40 AND ck = 1",
        "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 41 AND ck >= 0",
        "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 42",
        "INSERT INTO %s (pk, ck, v, r) VALUES (43, 1, 1, {1}) USING TIMESTAMP 2 AND TTL 1",
        "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 44 AND ck >= 0",
        "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 45 AND ck = 1",
        "UPDATE %s USING TIMESTAMP 2 SET st = 5 WHERE pk = 46",
        "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 46 AND ck >= 0",
        "DELETE v, r FROM %s USING TIMESTAMP 2 WHERE pk = 47 AND ck = 1",
        "UPDATE %s USING TIMESTAMP 2 SET st = 5 WHERE pk = 48",
        "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 48 AND ck = 1",
    };

    // the writes on every replica after the deletions
    private static final String[] AFTER = {
        "INSERT INTO %s (pk, ck, v, r) VALUES (44, 2, 2, {2}) USING TIMESTAMP 3",
    };

    // the live data once the replicas agree
    private static final String[] LIVE = {
        "INSERT INTO %s (pk, st, sx) VALUES (20, 0, 1)",
        "INSERT INTO %s (pk, ck, v, r) VALUES (20, 1, 1, {1})",
        "INSERT INTO %s (pk, ck, v, r) VALUES (20, 2, 2, {2})",
        "INSERT INTO %s (pk, st, sx) VALUES (21, 0, 1)",
        "INSERT INTO %s (pk, ck, v, r) VALUES (21, 1, 1, {1})",
        "INSERT INTO %s (pk, ck, v, r) VALUES (22, 1, 1, {1})",
        "INSERT INTO %s (pk, ck, v, r) VALUES (22, 2, 1, {1})",
        "INSERT INTO %s (pk, ck, v, r) VALUES (23, 1, 1, {1})",
        "INSERT INTO %s (pk, st, sx) VALUES (30, 5, 1)",
        "INSERT INTO %s (pk, st, sx) VALUES (40, 0, 1)",
        "INSERT INTO %s (pk, st, sx) VALUES (41, 0, 1)",
        "INSERT INTO %s (pk, st, sx) VALUES (43, 0, 1)",
        "INSERT INTO %s (pk, st, sx) VALUES (44, 0, 1)",
        "INSERT INTO %s (pk, ck, v, r) VALUES (44, 2, 2, {2})",
        "INSERT INTO %s (pk, st, sx) VALUES (45, 5, 1)",
        "INSERT INTO %s (pk, st, sx) VALUES (46, 5, 1)",
        "INSERT INTO %s (pk, st, sx) VALUES (47, 0, 1)",
        "INSERT INTO %s (pk, ck) VALUES (47, 1)",
        "INSERT INTO %s (pk, st, sx) VALUES (48, 5, 1)",
    };

    @Parameterized.Parameter
    public boolean nonStrict;

    @Parameterized.Parameters(name = "nonStrict={0}")
    public static Collection<Object[]> parameters()
    {
        return SAIUtil.nonStrictReplicaFilteringModes();
    }

    @Test
    public void testTwoReplicas() throws Throwable
    {
        checkWithReplicas(2);
    }

    @Test
    public void testThreeReplicas() throws Throwable
    {
        checkWithReplicas(3);
    }

    private void checkWithReplicas(int replicas) throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(replicas)
                                           .withConfig(config ->
                                                       SAIUtil.withNonStrictReplicaFiltering(config, nonStrict)
                                                              .set("hinted_handoff_enabled", false)
                                                              .set("read_request_timeout", "60s")
                                                              .set("range_request_timeout", "60s")
                                                              .with(GOSSIP).with(NETWORK))
                                           .start(), replicas))
        {
            List<String> tables = new ArrayList<>();
            for (boolean indexed : new boolean[]{ false, true })
            {
                for (String readRepair : new String[]{ "NONE", "BLOCKING" })
                {
                    String table = KEYSPACE + '.' + (indexed ? "indexed" : "plain") + '_' + readRepair.toLowerCase();
                    cluster.schemaChange("CREATE TABLE " + table + SCHEMA + " WITH read_repair = '" + readRepair + '\'');
                    if (indexed)
                        for (String column : new String[]{ "ck", "v", "r", "st", "sx" })
                            cluster.schemaChange("CREATE CUSTOM INDEX ON " + table + '(' + column + ") USING 'StorageAttachedIndex'");
                    tables.add(table);
                }
            }
            String live = KEYSPACE + ".live";
            cluster.schemaChange("CREATE TABLE " + live + SCHEMA);
            SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);

            for (String table : tables)
            {
                for (int node = 1; node <= replicas; node++)
                {
                    for (String write : BASE)
                        cluster.get(node).executeInternal(write.replace("%s", table));
                    if (node > 1)
                        for (String write : DELETIONS)
                            cluster.get(node).executeInternal(write.replace("%s", table));
                    for (String write : AFTER)
                        cluster.get(node).executeInternal(write.replace("%s", table));
                }
            }
            for (String write : LIVE)
                cluster.coordinator(1).execute(write.replace("%s", live), ConsistencyLevel.ALL);
            // let the TTL expire
            Thread.sleep(2500);

            List<String> failures = new ArrayList<>();
            check(cluster, tables, live, "memtable", failures);
            cluster.forEach(instance -> instance.flush(KEYSPACE));
            check(cluster, tables, live, "sstable", failures);
            assertTrue(String.format("%d failures with RF %d:%n%s", failures.size(), replicas, String.join("\n", failures)),
                       failures.isEmpty());
        }
    }

    private static void check(Cluster cluster, List<String> tables, String live, String source, List<String> failures)
    {
        for (String restriction : RESTRICTIONS)
        {
            String query = "SELECT pk, ck FROM %s WHERE " + restriction;
            List<String> all = rows(cluster, query.replace("%s", live) + " ALLOW FILTERING", ConsistencyLevel.ALL, 0);
            for (String limit : LIMITS)
            {
                List<String> expected = limit(all, limit);
                for (String table : tables)
                {
                    for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.QUORUM, ConsistencyLevel.ALL })
                    {
                        for (int pageSize : new int[]{ 0, 1, 2, 100 })
                        {
                            String limited = query.replace("%s", table) + limit + " ALLOW FILTERING";
                            List<String> actual = rows(cluster, limited, cl, pageSize);
                            if (!expected.equals(actual))
                                failures.add(String.format("[%s, %s, %s] %s: expected %s but got %s", source, cl,
                                                           pageSize == 0 ? "unpaged" : "page size " + pageSize,
                                                           limited, expected, actual));
                        }
                    }
                }
            }
        }
    }

    private static List<String> rows(Cluster cluster, String query, ConsistencyLevel cl, int pageSize)
    {
        List<String> rows = new ArrayList<>();
        try
        {
            Iterator<Object[]> iterator = pageSize == 0
                                          ? Arrays.asList(cluster.coordinator(1).execute(query, cl)).iterator()
                                          : cluster.coordinator(1).executeWithPaging(query, cl, pageSize);
            while (iterator.hasNext())
            {
                Object[] row = iterator.next();
                rows.add(row[0] + "/" + row[1]);
            }
        }
        catch (Throwable t)
        {
            rows.add("failed: " + t);
        }
        return rows;
    }

    /**
     * Applies the given LIMIT and PER PARTITION LIMIT to the rows of the query without limits, in their order.
     */
    private static List<String> limit(List<String> all, String limits)
    {
        int limit = Integer.MAX_VALUE;
        int perPartitionLimit = Integer.MAX_VALUE;
        String[] words = limits.trim().split(" ");
        for (int i = 0; i < words.length; i++)
        {
            if (words[i].equals("LIMIT"))
            {
                int value = Integer.parseInt(words[i + 1]);
                if (i > 0 && words[i - 1].equals("PARTITION"))
                    perPartitionLimit = value;
                else
                    limit = value;
            }
        }
        List<String> result = new ArrayList<>();
        Map<String, Integer> perPartition = new HashMap<>();
        for (String row : all)
        {
            int count = perPartition.merge(row.substring(0, row.indexOf('/')), 1, Integer::sum);
            if (count <= perPartitionLimit && result.size() < limit)
                result.add(row);
        }
        return result;
    }
}
