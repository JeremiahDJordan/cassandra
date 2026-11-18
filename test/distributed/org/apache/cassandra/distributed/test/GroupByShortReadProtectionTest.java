/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.metrics.TableMetrics;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Short read protection of GROUP BY queries with a limit or a page size, with replicas that return exactly as many
 * groups as the limit, or reach the limit as a partition starts.
 */
@RunWith(Parameterized.class)
public class GroupByShortReadProtectionTest extends TestBaseImpl
{
    private static final ConsistencyLevel[] ABOVE_ONE = { ConsistencyLevel.QUORUM, ConsistencyLevel.ALL };
    private static final int[] PAGE_SIZES = { 0, 1, 3, 7, 100 };
    private static final int[] ALL_NODES = { 1, 2, 3 };

    private static Cluster CLUSTER;

    @Parameterized.Parameter
    public boolean nonStrict;

    @Parameterized.Parameters(name = "nonStrict={0}")
    public static Collection<Object[]> parameters()
    {
        return SAIUtil.nonStrictReplicaFilteringModes();
    }

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        // The in-JVM coordinator API pages a query within a single request time, which native_transport_timeout also
        // bounds, so give the pages of small page sizes together more time than the default.
        CLUSTER = init(Cluster.build(3)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .set("native_transport_timeout", "60s")
                                                          .set("read_request_timeout", "60s")
                                                          .set("range_request_timeout", "60s")
                                                          .with(GOSSIP).with(NETWORK))
                              .start());
    }

    @Before
    public void setUp()
    {
        SAIUtil.setNonStrictReplicaFiltering(CLUSTER, nonStrict);
        CLUSTER.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(CLUSTER, 3);
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /**
     * Rows shadowed by the newer deletions of another replica. Rows c = 0..11 of partitions 0 and 1 are on nodes 1 and
     * 3, and node 2 only has newer deletions of c = 0..7, so the live rows are c = 8..11. Nodes 1 and 3 return as many
     * groups as the limit, all shadowed.
     */
    @Test
    public void testShadowedGroups()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.shadowed_groups (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.shadowed_groups(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);
        for (int node = 1; node <= 3; node++)
            for (int k = 0; k < 2; k++)
                for (int c = 0; c < (node == 2 ? 8 : 12); c++)
                    write("shadowed_groups", new int[]{ node }, "INSERT INTO %s (k, c, a, b) VALUES (?, ?, 1, 2) USING TIMESTAMP 1", k, c);
        for (int k = 0; k < 2; k++)
            for (int c = 0; c < 8; c++)
                write("shadowed_groups", new int[]{ 2 }, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = ? AND c = ?", k, c);

        List<String> mismatches = new ArrayList<>();
        for (boolean flush : BOOLEANS)
        {
            if (flush)
                CLUSTER.forEach(node -> node.flush(KEYSPACE));
            String when = flush ? " after flush" : "";
            checkAll("shadowed_groups", "SELECT c, count(*) FROM %s WHERE k = 0 GROUP BY k, c LIMIT 2", when, PAGE_SIZES, mismatches,
                     "[8, 1]", "[9, 1]");
            checkAll("shadowed_groups", "SELECT c, count(*) FROM %s WHERE k = 0 AND a = 1 GROUP BY k, c LIMIT 2", when, PAGE_SIZES, mismatches,
                     "[8, 1]", "[9, 1]");
            checkAll("shadowed_groups", "SELECT c, count(*) FROM %s WHERE k = 0 AND b = 2 GROUP BY k, c LIMIT 2 ALLOW FILTERING", when, PAGE_SIZES, mismatches,
                     "[8, 1]", "[9, 1]");
            checkAll("shadowed_groups", "SELECT c, count(*) FROM %s WHERE k = 0 GROUP BY k, c", when, PAGE_SIZES, mismatches,
                     "[8, 1]", "[9, 1]", "[10, 1]", "[11, 1]");
            checkAll("shadowed_groups", "SELECT k, count(*) FROM %s WHERE a = 1 GROUP BY k", when, PAGE_SIZES, mismatches,
                     "[1, 4]", "[0, 4]");
            checkAll("shadowed_groups", "SELECT k, c, count(*) FROM %s WHERE a = 1 GROUP BY k, c LIMIT 3", when, PAGE_SIZES, mismatches,
                     "[1, 8, 1]", "[1, 9, 1]", "[1, 10, 1]");
            checkAll("shadowed_groups", "SELECT k, c, count(*) FROM %s GROUP BY k, c LIMIT 3", when, PAGE_SIZES, mismatches,
                     "[1, 8, 1]", "[1, 9, 1]", "[1, 10, 1]");
        }
        assertThat(mismatches).isEmpty();
    }

    /**
     * CASSANDRA-21340: GROUP BY of the partition key over partitions of several rows: nodes 1 and 2 each delete half of
     * the rows of partitions 0 to 4, so these partitions have no live rows once reconciled, and short read protection
     * has to ask the replicas for more partitions.
     */
    @Test
    public void testGroupByPartialCrossNodeDeletions()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.cross_deletions (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
        for (int pk = 0; pk < 15; pk++)
            for (int ck = 0; ck < 6; ck++)
                write("cross_deletions", ALL_NODES, "INSERT INTO %s (pk, ck, v) VALUES (?, ?, 1) USING TIMESTAMP 1", pk, ck);
        for (int pk = 0; pk < 5; pk++)
            for (int ck = 0; ck < 6; ck++)
                write("cross_deletions", new int[]{ ck % 2 == 0 ? 1 : 2 }, "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = ? AND ck = ?", pk, ck);

        List<String> mismatches = new ArrayList<>();
        // the first five partitions in token order among partitions 5 to 14
        checkAll("cross_deletions", "SELECT pk, count(*) FROM %s GROUP BY pk LIMIT 5", "", new int[]{ 0, 1, 2, 3, 5, 6, 10 }, mismatches,
                 "[5, 6]", "[10, 6]", "[13, 6]", "[11, 6]", "[8, 6]");
        assertThat(mismatches).isEmpty();
    }

    /**
     * GROUP BY of the partition key over partitions of 10 rows. Partition 5 is deleted on node 3 only, so nodes 1 and 2
     * reach the limit one partition before node 3, and short read protection asks them for more partitions. That
     * request asks for the missing groups, which include node 1's newer value in partition 0.
     */
    @Test
    public void testGroupsOfSeveralRowsOverPartitions()
    {
        createGroupTable("multi_row_groups");
        // token order: 5, 1, 8, 0, 2
        for (int k : new int[]{ 5, 1, 8, 0, 2 })
            for (int c = 0; c < 10; c++)
                write("multi_row_groups", ALL_NODES, "INSERT INTO %s (k, c, d, a, b, x) VALUES (?, ?, 0, 1, 2, 0) USING TIMESTAMP 1", k, c);
        write("multi_row_groups", new int[]{ 3 }, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = 5");
        write("multi_row_groups", new int[]{ 3 }, "INSERT INTO %s (k, c, d, x) VALUES (8, 9, 0, 7) USING TIMESTAMP 2");
        write("multi_row_groups", new int[]{ 1 }, "INSERT INTO %s (k, c, d, x) VALUES (0, 9, 0, 9) USING TIMESTAMP 2");

        List<String> mismatches = new ArrayList<>();
        checkAll("multi_row_groups", "SELECT k, count(*), max(x) FROM %s GROUP BY k LIMIT 2", "", new int[]{ 0, 100 }, mismatches,
                 "[1, 10, 0]", "[8, 10, 7]");
        checkAll("multi_row_groups", "SELECT k, count(*), max(x) FROM %s GROUP BY k LIMIT 3", "", new int[]{ 0, 100 }, mismatches,
                 "[1, 10, 0]", "[8, 10, 7]", "[0, 10, 9]");
        checkAll("multi_row_groups", "SELECT k, count(*), max(x) FROM %s GROUP BY k", "", new int[]{ 0, 1, 2, 3 }, mismatches,
                 "[1, 10, 0]", "[8, 10, 7]", "[0, 10, 9]", "[2, 10, 0]");
        assertThat(mismatches).isEmpty();
    }

    /**
     * The partition at which a replica's GROUP BY counter reaches the limit. Each node has three groups in partition 5
     * (node 1 has a newer deletion of c = 1, nodes 2 and 3 of c = 0), and the reconciled partition has two, so the
     * replicas stop as partition 1 starts, and the result has the static row and rows of partition 1. The table is
     * written with and without a partition deletion of partition 1 older than its data.
     */
    @Test
    public void testPartitionAtGroupLimit()
    {
        List<String> mismatches = new ArrayList<>();
        for (boolean deleted : BOOLEANS)
        {
            String table = deleted ? "deleted_groups" : "plain_groups";
            createGroupTable(table);
            if (deleted)
                write(table, ALL_NODES, "DELETE FROM %s USING TIMESTAMP 1 WHERE k = 1");
            // token order: 5, 1, 8
            write(table, ALL_NODES, "INSERT INTO %s (k, s) VALUES (5, 105) USING TIMESTAMP 2");
            for (int c = 0; c < 4; c++)
                write(table, ALL_NODES, "INSERT INTO %s (k, c, d, a, b, x) VALUES (5, ?, 0, 1, 2, 0) USING TIMESTAMP 2", c);
            write(table, new int[]{ 1 }, "DELETE FROM %s USING TIMESTAMP 3 WHERE k = 5 AND c = 1 AND d = 0");
            write(table, new int[]{ 2, 3 }, "DELETE FROM %s USING TIMESTAMP 3 WHERE k = 5 AND c = 0 AND d = 0");
            write(table, ALL_NODES, "INSERT INTO %s (k, s) VALUES (1, 101) USING TIMESTAMP 2");
            for (int c = 0; c < 2; c++)
                write(table, ALL_NODES, "INSERT INTO %s (k, c, d, a, b, x) VALUES (1, ?, 0, 1, 2, 0) USING TIMESTAMP 2", c);
            write(table, ALL_NODES, "INSERT INTO %s (k, s) VALUES (8, 108) USING TIMESTAMP 2");
            for (boolean flush : BOOLEANS)
            {
                if (flush)
                    CLUSTER.forEach(node -> node.flush(KEYSPACE));
                String when = (deleted ? " with a partition deletion" : "") + (flush ? " after flush" : "");
                checkAll(table, "SELECT k, c, s, count(*) FROM %s GROUP BY k, c LIMIT 3", when, new int[]{ 0, 100 }, mismatches,
                         "[5, 2, 105, 1]", "[5, 3, 105, 1]", "[1, 0, 101, 1]");
                checkAll(table, "SELECT k, c, s, count(*) FROM %s GROUP BY k, c", when, new int[]{ 3 }, mismatches,
                         "[5, 2, 105, 1]", "[5, 3, 105, 1]", "[1, 0, 101, 1]", "[1, 1, 101, 1]", "[8, null, 108, 1]");
                checkAll(table, "SELECT k, c, s, count(*) FROM %s WHERE s = 101 GROUP BY k, c LIMIT 3 ALLOW FILTERING", when, new int[]{ 0, 3 }, mismatches,
                         "[1, 0, 101, 1]", "[1, 1, 101, 1]");
            }
        }
        assertThat(mismatches).isEmpty();
    }

    /**
     * As testPartitionAtGroupLimit, with a partition that only has a static row where the replicas stop: partition 5
     * has a group on every node (row c = 0 only live on node 1, c = 1 only on nodes 2 and 3) and none once reconciled.
     */
    @Test
    public void testStaticOnlyPartitionAtGroupLimit()
    {
        List<String> mismatches = new ArrayList<>();
        for (boolean deleted : BOOLEANS)
        {
            String table = deleted ? "deleted_static_groups" : "plain_static_groups";
            createGroupTable(table);
            if (deleted)
                write(table, ALL_NODES, "DELETE FROM %s USING TIMESTAMP 1 WHERE k = 1");
            for (int c = 0; c < 2; c++)
                write(table, ALL_NODES, "INSERT INTO %s (k, c, d, a, b, x) VALUES (5, ?, 0, 1, 2, 0) USING TIMESTAMP 2", c);
            write(table, new int[]{ 1 }, "DELETE FROM %s USING TIMESTAMP 3 WHERE k = 5 AND c = 1 AND d = 0");
            write(table, new int[]{ 2, 3 }, "DELETE FROM %s USING TIMESTAMP 3 WHERE k = 5 AND c = 0 AND d = 0");
            write(table, ALL_NODES, "INSERT INTO %s (k, s) VALUES (1, 101) USING TIMESTAMP 2");
            write(table, ALL_NODES, "INSERT INTO %s (k, s) VALUES (8, 108) USING TIMESTAMP 2");
            write(table, ALL_NODES, "INSERT INTO %s (k, c, d, a, b, x) VALUES (8, 0, 0, 1, 2, 0) USING TIMESTAMP 2");
            String when = deleted ? " with a partition deletion" : "";
            checkAll(table, "SELECT k, s, count(*) FROM %s GROUP BY k LIMIT 1", when, new int[]{ 0, 100 }, mismatches, "[1, 101, 1]");
            checkAll(table, "SELECT k, s, count(*) FROM %s GROUP BY k", when, new int[]{ 0, 1 }, mismatches, "[1, 101, 1]", "[8, 108, 1]");
        }
        assertThat(mismatches).isEmpty();
    }

    /**
     * The same rows on every replica, so every consistency level must return what ONE returns. A filter on two
     * SAI-indexed columns isn't strict above ONE (CASSANDRA-19018): the replicas return candidate rows, and the first 20
     * rows of each partition only match "a = 1", so the coordinator filters out the groups of the first responses. The
     * last queries filter on a single column.
     */
    @Test
    public void testNonStrictFilters()
    {
        createGroupTable("candidate_groups", "a", "b");
        // 4 partitions of 40 rows, a = 1 on all the rows, b = 2 on the rows with c >= 20
        for (int k = 0; k < 4; k++)
            for (int c = 0; c < 40; c++)
                write("candidate_groups", ALL_NODES, "INSERT INTO %s (k, c, d, a, b, x) VALUES (?, ?, 0, 1, ?, ?) USING TIMESTAMP 1", k, c, c >= 20 ? 2 : 3, c >= 20 ? 0 : 1);

        String[] queries = {
            "SELECT c, count(*) FROM %s WHERE k = 1 AND a = 1 AND b = 2 GROUP BY k, c LIMIT 5",
            "SELECT c, count(*) FROM %s WHERE k = 1 AND a = 1 AND b = 2 GROUP BY k, c",
            "SELECT k, c, count(*) FROM %s WHERE a = 1 AND b = 2 GROUP BY k, c LIMIT 5",
            "SELECT k, c, count(*) FROM %s WHERE a = 1 AND b = 2 GROUP BY k, c",
            "SELECT k, c, count(*) FROM %s WHERE a = 1 AND b = 2 GROUP BY k, c PER PARTITION LIMIT 2 LIMIT 5",
            "SELECT k, count(*) FROM %s WHERE a = 1 AND b = 2 GROUP BY k LIMIT 2",
            "SELECT k, count(*) FROM %s WHERE a = 1 AND b = 2 GROUP BY k",
            "SELECT k, count(*) FROM %s WHERE k = 1 AND a = 1 AND b = 2 GROUP BY k LIMIT 1",
            "SELECT k, count(*) FROM %s WHERE k = 1 AND b = 2 GROUP BY k LIMIT 1",
            "SELECT c, count(*) FROM %s WHERE k = 1 AND b = 2 GROUP BY k, c LIMIT 5",
            "SELECT c, count(*) FROM %s WHERE k = 1 AND x = 0 GROUP BY k, c LIMIT 5 ALLOW FILTERING",
        };

        List<String> mismatches = new ArrayList<>();
        for (boolean flush : BOOLEANS)
        {
            if (flush)
                CLUSTER.forEach(node -> node.flush(KEYSPACE));
            for (String query : queries)
            {
                String select = String.format(query, KEYSPACE + ".candidate_groups");
                List<String> expected = toStrings(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ONE));
                assertThat(expected).as(select).isNotEmpty();
                for (ConsistencyLevel cl : ABOVE_ONE)
                    for (int pageSize : PAGE_SIZES)
                        check(select, flush ? " after flush" : "", cl, pageSize, expected, mismatches);
            }
        }
        assertThat(mismatches).isEmpty();
    }

    /**
     * The same rows on every replica, so short read protection needs no rows: a replica that returns as many groups as
     * the limit has the rows of its last group that the merged result has, which has as many groups as the limit.
     * Partitions 0 to 2 have 20 groups of 2 rows, partition 3 has 100 groups of a single row. The queries run with and
     * without a filter.
     */
    @Test
    public void testNoShortReadProtectionWhenReplicasAgree()
    {
        createGroupTable("agreeing_groups", "a");
        for (int k = 0; k < 4; k++)
            for (int c = 0; c < (k == 3 ? 100 : 20); c++)
                for (int d = 0; d < (k == 3 ? 1 : 2); d++)
                    write("agreeing_groups", ALL_NODES, "INSERT INTO %s (k, c, d, a, b, x) VALUES (?, ?, ?, 1, 2, 0) USING TIMESTAMP 1", k, c, d);

        String[] queries = {
            "SELECT k, count(*) FROM %s GROUP BY k",
            "SELECT k, c, count(*) FROM %s GROUP BY k, c",
            "SELECT k, c, count(*) FROM %s WHERE b = 2 GROUP BY k, c ALLOW FILTERING",
            "SELECT k, c, count(*) FROM %s WHERE a = 1 GROUP BY k, c",
            "SELECT k, count(*) FROM %s WHERE a = 1 GROUP BY k",
            "SELECT c, count(*) FROM %s WHERE k = 3 GROUP BY k, c",
            "SELECT c, count(*) FROM %s WHERE k = 3 AND b = 2 GROUP BY k, c ALLOW FILTERING",
        };

        List<String> mismatches = new ArrayList<>();
        for (boolean flush : new boolean[]{ false, true })
        {
            if (flush)
                CLUSTER.forEach(node -> node.flush(KEYSPACE));
            String when = flush ? " after flush" : "";
            for (String query : queries)
            {
                String select = String.format(query, KEYSPACE + ".agreeing_groups");
                String limited = select.replace(" ALLOW FILTERING", "") + " LIMIT 2" + (select.endsWith(" ALLOW FILTERING") ? " ALLOW FILTERING" : "");
                List<String> expected = toStrings(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ONE));
                for (ConsistencyLevel cl : ABOVE_ONE)
                {
                    long before = protectionRequests("agreeing_groups");
                    check(limited, when, cl, 0, expected.subList(0, 2), mismatches);
                    for (int pageSize : new int[]{ 1, 2, 10 })
                        check(select, when, cl, pageSize, expected, mismatches);
                    long requests = protectionRequests("agreeing_groups") - before;
                    if (requests != 0)
                        mismatches.add(select + when + " at " + cl + ": " + requests + " short read or replica filtering protection requests");
                }
            }
        }
        assertThat(mismatches).isEmpty();
    }

    /**
     * A filtered GROUP BY query over a partition of 100 groups of a single row, whose first group node 2 deletes. The
     * replicas have to be asked for more rows once, as their first groups are shadowed, and not for the rest of the
     * partition: the merged result has as many groups as the limit, whose last one has every replica's rows.
     */
    @Test
    public void testBoundedShortReadProtectionWithFilter()
    {
        createGroupTable("filtered_groups", "a");
        for (int c = 0; c < 100; c++)
            write("filtered_groups", ALL_NODES, "INSERT INTO %s (k, c, d, a, b, x) VALUES (0, ?, 0, 1, 2, 0) USING TIMESTAMP 1", c);
        write("filtered_groups", new int[]{ 2 }, "DELETE FROM %s USING TIMESTAMP 2 WHERE k = 0 AND c = 0 AND d = 0");

        List<String> mismatches = new ArrayList<>();
        for (String query : new String[]{ "SELECT c, count(*) FROM %s WHERE b = 2 GROUP BY k, c LIMIT 2 ALLOW FILTERING",
                                          "SELECT c, count(*) FROM %s WHERE k = 0 AND b = 2 GROUP BY k, c LIMIT 2 ALLOW FILTERING",
                                          "SELECT c, count(*) FROM %s WHERE a = 1 GROUP BY k, c LIMIT 2" })
        {
            long before = protectionRequests("filtered_groups");
            checkAll("filtered_groups", query, "", new int[]{ 0 }, mismatches, "[1, 1]", "[2, 1]");
            long requests = protectionRequests("filtered_groups") - before;
            // one short read protection request to nodes 1 and 3, and one replica filtering protection request
            if (requests > 3)
                mismatches.add(query + ": " + requests + " short read or replica filtering protection requests");
        }
        assertThat(mismatches).isEmpty();
    }

    private static long protectionRequests(String table)
    {
        return CLUSTER.get(1).callOnInstance(() -> {
            TableMetrics metrics = Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metric;
            return metrics.shortReadProtectionRequests.getCount() + metrics.replicaFilteringProtectionRequests.getCount();
        });
    }

    /**
     * Creates a table with a static column and a two-column clustering key, so that GROUP BY k, c groups rows.
     */
    private static void createGroupTable(String table, String... indexed)
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (k int, c int, d int, s int static, a int, b int, x int, PRIMARY KEY (k, c, d))" +
                                          " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'"));
        for (String column : indexed)
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + '(' + column + ") USING 'sai'"));
        if (indexed.length > 0)
            SAIUtil.waitForIndexQueryableOnFirstNode(CLUSTER, KEYSPACE);
    }

    private static void write(String table, int[] nodes, String statement, Object... args)
    {
        for (int node : nodes)
            CLUSTER.get(node).executeInternal(String.format(statement, KEYSPACE + '.' + table), args);
    }

    // The data of most tests differs between replicas, so only ALL gives a known result.
    private static void checkAll(String table, String query, String when, int[] pageSizes, List<String> mismatches, String... expected)
    {
        String select = String.format(query, KEYSPACE + '.' + table);
        for (int pageSize : pageSizes)
            check(select, when, ConsistencyLevel.ALL, pageSize, Arrays.asList(expected), mismatches);
    }

    private static void check(String select, String when, ConsistencyLevel cl, int pageSize, List<String> expected, List<String> mismatches)
    {
        List<String> actual;
        try
        {
            actual = pageSize == 0 ? toStrings(CLUSTER.coordinator(1).execute(select, cl))
                                   : toStrings(CLUSTER.coordinator(1).executeWithPaging(select, cl, pageSize));
        }
        catch (Throwable t)
        {
            actual = List.of("ERROR " + t);
        }
        if (!expected.equals(actual))
            mismatches.add(select + when + " at " + cl + (pageSize == 0 ? "" : " page size " + pageSize) + ": expected " + expected + " but got " + actual);
    }

    private static List<String> toStrings(Object[][] rows)
    {
        return toStrings(Arrays.asList(rows).iterator());
    }

    private static List<String> toStrings(Iterator<Object[]> rows)
    {
        List<String> result = new ArrayList<>();
        while (rows.hasNext())
            result.add(Arrays.toString(rows.next()));
        return result;
    }
}
