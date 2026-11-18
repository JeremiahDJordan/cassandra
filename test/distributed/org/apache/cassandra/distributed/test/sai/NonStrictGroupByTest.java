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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.toStrings;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * GROUP BY queries with a limit or a page size, whose short read protection decides whether a replica has more groups
 * from a GROUP BY counter. That counter only counts a group once the next one starts, so a replica that returned exactly
 * as many groups as the limit looks like it returned one less, and like it has no more. Replicas that filter strictly
 * have the same problem when another replica shadows their first groups, and those that don't return more candidate
 * rows, so the tests run with non-strict replica filtering disabled and enabled.
 */
@RunWith(Parameterized.class)
public class NonStrictGroupByTest extends TestBaseImpl
{
    private static final ConsistencyLevel[] CONSISTENCY_LEVELS = { ConsistencyLevel.QUORUM, ConsistencyLevel.ALL };
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
        // The in-JVM coordinator API pages a query within a single request time, so give the pages of small page sizes
        // together more time than the default.
        CLUSTER = init(Cluster.build(3)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .set("read_request_timeout", "30s")
                                                          .set("range_request_timeout", "30s")
                                                          .with(GOSSIP).with(NETWORK))
                              .start());
    }

    @Before
    public void setUp()
    {
        SAIUtil.setNonStrictReplicaFiltering(CLUSTER, nonStrict);
        for (String table : new String[]{ "groups", "deleted_groups", "plain_groups" })
            CLUSTER.schemaChange(withKeyspace("DROP TABLE IF EXISTS %s." + table));
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /**
     * The same rows on all the replicas, so every consistency level must return what ONE returns. Non-strict replicas
     * return the candidate rows of the filter "a = 1 AND b = 2", and the first 20 rows of each partition only match
     * "a = 1", so the coordinator filters out the groups of the first responses, also when a replica returns exactly
     * LIMIT groups, or a page of them.
     */
    @Test
    public void testGroupByWithLimitsAndPaging()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.groups (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.groups(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.groups(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

        // 4 partitions of 40 rows, a = 1 on all the rows, b = 2 on the rows with c >= 20
        for (int node = 1; node <= 3; node++)
            for (int k = 0; k < 4; k++)
                for (int c = 0; c < 40; c++)
                    CLUSTER.get(node).executeInternal(withKeyspace("INSERT INTO %s.groups (k, c, a, b) VALUES (?, ?, 1, ?) USING TIMESTAMP 1"), k, c, c >= 20 ? 2 : 3);

        String[] queries = {
            "SELECT c, count(*) FROM %s.groups WHERE k = 1 AND a = 1 AND b = 2 GROUP BY k, c LIMIT 5",
            "SELECT c, count(*) FROM %s.groups WHERE k = 1 AND a = 1 AND b = 2 GROUP BY k, c",
            "SELECT c, count(*) FROM %s.groups WHERE k = 1 AND a = 1 AND b = 2 GROUP BY k, c PER PARTITION LIMIT 5",
            "SELECT k, c, count(*) FROM %s.groups WHERE a = 1 AND b = 2 GROUP BY k, c LIMIT 5",
            "SELECT k, c, count(*) FROM %s.groups WHERE a = 1 AND b = 2 GROUP BY k, c",
            "SELECT k, c, count(*) FROM %s.groups WHERE a = 1 AND b = 2 GROUP BY k, c PER PARTITION LIMIT 2 LIMIT 5",
            "SELECT k, count(*) FROM %s.groups WHERE a = 1 AND b = 2 GROUP BY k LIMIT 2",
            "SELECT k, count(*) FROM %s.groups WHERE a = 1 AND b = 2 GROUP BY k",
            "SELECT count(*) FROM %s.groups WHERE k = 1 AND a = 1 AND b = 2",
        };

        List<String> mismatches = new ArrayList<>();
        for (boolean flush : new boolean[]{ false, true })
        {
            if (flush)
                CLUSTER.forEach(i -> i.flush(KEYSPACE));

            for (String query : queries)
            {
                String select = withKeyspace(query);
                List<String> expected = toStrings(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ONE));
                assertThat(expected).as(select).isNotEmpty();
                for (ConsistencyLevel cl : CONSISTENCY_LEVELS)
                    for (int pageSize : PAGE_SIZES)
                        check(select, flush ? " after flush" : "", cl, pageSize, expected, mismatches);
            }
        }
        assertThat(mismatches).isEmpty();
    }

    /**
     * The partition at which a replica's GROUP BY counter reaches the limit comes back without its static row and rows,
     * but with its partition deletion, which is older than its data here; short read protection asks for that
     * partition again. The same rows are on every node, and the control table has no partition deletion.
     */
    @Test
    public void testPartitionDeletionAtGroupLimit()
    {
        List<String> mismatches = new ArrayList<>();
        for (boolean deleted : new boolean[]{ false, true })
        {
            String table = deleted ? "deleted_groups" : "plain_groups";
            createGroupTable(table, "a", "b", "s");
            if (deleted)
                write(table, ALL_NODES, "DELETE FROM %s USING TIMESTAMP 1 WHERE k = 1");
            // token order: 5, 1
            for (int k : new int[]{ 5, 1 })
            {
                write(table, ALL_NODES, "INSERT INTO %s (k, s) VALUES (?, ?) USING TIMESTAMP 2", k, 100 + k);
                for (int c = 0; c < 3; c++)
                    write(table, ALL_NODES, "INSERT INTO %s (k, c, d, a, b, x) VALUES (?, ?, 0, 1, ?, 0) USING TIMESTAMP 2", k, c, c == 0 ? 3 : 2);
            }
            for (boolean flush : new boolean[]{ false, true })
            {
                if (flush)
                    CLUSTER.forEach(node -> node.flush(KEYSPACE));
                String when = (deleted ? " with a partition deletion" : "") + (flush ? " after flush" : "");
                checkAll(table, "SELECT k, c, s, count(*) FROM %s WHERE a = 1 AND b = 2 GROUP BY k, c LIMIT 3", when, CONSISTENCY_LEVELS, new int[]{ 0, 100 }, mismatches,
                         "[5, 1, 105, 1]", "[5, 2, 105, 1]", "[1, 1, 101, 1]");
                checkAll(table, "SELECT k, c, s, count(*) FROM %s WHERE a = 1 AND b = 2 GROUP BY k, c", when, CONSISTENCY_LEVELS, new int[]{ 3 }, mismatches,
                         "[5, 1, 105, 1]", "[5, 2, 105, 1]", "[1, 1, 101, 1]", "[1, 2, 101, 1]");
                checkAll(table, "SELECT k, c, s, count(*) FROM %s WHERE s = 101 AND b = 2 GROUP BY k, c ALLOW FILTERING", when, CONSISTENCY_LEVELS, new int[]{ 0, 3 }, mismatches,
                         "[1, 1, 101, 1]", "[1, 2, 101, 1]");
                checkAll(table, "SELECT k, c, s, count(*) FROM %s WHERE s > 100 AND b = 2 GROUP BY k, c LIMIT 3 ALLOW FILTERING", when, CONSISTENCY_LEVELS, new int[]{ 0, 3 }, mismatches,
                         "[5, 1, 105, 1]", "[5, 2, 105, 1]", "[1, 1, 101, 1]");
            }
        }
        assertThat(mismatches).isEmpty();
    }

    private static void createGroupTable(String table, String... indexed)
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (k int, c int, d int, s int static, a int, b int, x int, PRIMARY KEY (k, c, d))" +
                                          " WITH read_repair = 'NONE' AND speculative_retry = 'NONE'"));
        for (String column : indexed)
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + '(' + column + ") USING 'sai'"));
        if (indexed.length > 0)
            SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);
    }

    private static void write(String table, int[] nodes, String statement, Object... args)
    {
        for (int node : nodes)
            CLUSTER.get(node).executeInternal(String.format(statement, KEYSPACE + '.' + table), args);
    }

    private static void checkAll(String table, String query, String when, ConsistencyLevel[] levels, int[] pageSizes, List<String> mismatches, String... expected)
    {
        String select = String.format(query, KEYSPACE + '.' + table);
        for (ConsistencyLevel cl : levels)
            for (int pageSize : pageSizes)
                check(select, when, cl, pageSize, Arrays.asList(expected), mismatches);
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
}
