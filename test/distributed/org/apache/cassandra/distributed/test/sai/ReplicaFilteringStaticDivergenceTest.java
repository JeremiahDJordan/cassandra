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

package org.apache.cassandra.distributed.test.sai;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Replica filtering protection for filters with an expression on a static column, with replicas that diverge.
 * <p>
 * Each replica filters its own data with its own static row, and leaves out of a partition it returns the rows that
 * don't satisfy the filter, deleted rows included. The coordinator reads the whole partition from the replicas that
 * are silent about the static row, so it must also find the deletions of the rows those reads bring, which the other
 * replicas left out.
 * <p>
 * One partition per case, three replicas, read at ALL, without index, with SAI indexes and with a legacy index on the
 * static column, with a partition restriction and over the whole table, with LIMIT 1 and LIMIT 2, unpaged and paged by
 * 1, 2 and 100, from memtables and SSTables, and, without indexes, with PER PARTITION LIMIT 1 and in reverse order.
 */
public class ReplicaFilteringStaticDivergenceTest extends TestBaseImpl
{
    private static final String SCHEMA = " (pk int, ck int, v int, w int, r set<int>, st int static, sx int static, " +
                                         "PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'";

    private static final String[] TABLES = { "plain", "indexed", "legacy" };

    private static final String ST0 = "INSERT INTO %s (pk, st, sx) VALUES ($PK, 0, 1) USING TIMESTAMP 1";
    private static final String ST5_ALL = "123: INSERT INTO %s (pk, st, sx) VALUES ($PK, 5, 1) USING TIMESTAMP 1";
    private static final String PARTITION_DELETION = "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = $PK";

    private static String row(int ck, int v, int timestamp)
    {
        return "INSERT INTO %s (pk, ck, v, r) VALUES ($PK, " + ck + ", " + v + ", {1}) USING TIMESTAMP " + timestamp;
    }

    private static String full(int ck, int timestamp)
    {
        return "INSERT INTO %s (pk, ck, v, w, r) VALUES ($PK, " + ck + ", 1, 1, {1, 2}) USING TIMESTAMP " + timestamp;
    }

    private static String delete(int ck, int timestamp)
    {
        return "DELETE FROM %s USING TIMESTAMP " + timestamp + " WHERE pk = $PK AND ck = " + ck;
    }

    private static String setStatic(int value, int timestamp)
    {
        return "UPDATE %s USING TIMESTAMP " + timestamp + " SET st = " + value + " WHERE pk = $PK";
    }

    private static String set(String assignment, int ck, int timestamp)
    {
        return "UPDATE %s USING TIMESTAMP " + timestamp + " SET " + assignment + " WHERE pk = $PK AND ck = " + ck;
    }

    /**
     * Each case: a name, the restriction, the clusterings of the rows expected ({@code null} for the static row of a
     * partition without live rows), then the writes, each prefixed with the nodes that have it.
     */
    private static final String[][] CASES = {
        // node 1 has an older static row and row 1, which node 3 has deleted; node 3 returns the partition for row 2
        // without that deletion, and node 1 is silent about the newer static row of nodes 2 and 3
        { "stale static row, deleted row", "st = 5 AND v = 1", "2",
          "1: " + ST0, "1: " + row(1, 1, 1), "23: " + setStatic(5, 3), "3: " + delete(1, 2), "3: " + row(2, 1, 1) },
        // the same with a deletion newer than everything else
        { "stale static row, newer deleted row", "st = 5 AND v = 1", "2",
          "1: " + ST0, "1: " + row(1, 1, 1), "23: " + setStatic(5, 3), "3: " + delete(1, 9), "3: " + row(2, 1, 1) },
        // the replicas with the newer static row have deleted the rows of node 1, which has deleted theirs
        { "stale static row, static row only", "st = 5", "null",
          "1: " + ST0, "1: " + delete(1, 5), "1: " + row(2, 2, 1),
          "23: " + setStatic(5, 2), "23: " + row(1, 7, 1), "23: " + delete(2, 3) },
        // identical static rows; a row whose columns are on two replicas that return nothing, deleted on the third
        { "split row, deleted", "st = 5 AND v = 1 AND w = 1", "2",
          ST5_ALL, "1: " + set("v = 1", 1, 1), "2: " + set("w = 1", 1, 1), "3: " + delete(1, 2), "3: " + full(2, 1) },
        // the same with two elements of a collection
        { "split collection, deleted", "st = 5 AND r CONTAINS 1 AND r CONTAINS 2", "2",
          ST5_ALL, "1: " + set("r = r + {1}", 1, 1), "2: " + set("r = r + {2}", 1, 1), "3: " + delete(1, 2), "3: " + full(2, 1) },
        // the same with a range deletion
        { "split row, range deletion", "st = 5 AND v = 1 AND w = 1", "2",
          ST5_ALL, "1: " + set("v = 1", 1, 1), "2: " + set("w = 1", 1, 1),
          "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = $PK AND ck <= 1", "3: " + full(2, 1) },
        // the same with static rows that differ in a column that the filter doesn't restrict
        { "split row, unrestricted static column", "st = 5 AND v = 1 AND w = 1", "2",
          "12: INSERT INTO %s (pk, st, sx) VALUES ($PK, 5, 1) USING TIMESTAMP 1",
          "3: INSERT INTO %s (pk, st, sx) VALUES ($PK, 5, 2) USING TIMESTAMP 2",
          "1: " + set("v = 1", 1, 1), "2: " + set("w = 1", 1, 1), "3: " + delete(1, 2), "3: " + full(2, 1) },
        // the column that makes the row of node 1 match is newer on node 2, and both return nothing
        { "newer column on another silent replica", "st = 5 AND v = 1 AND w = 1", "2",
          ST5_ALL, "1: " + set("v = 1, w = 3", 1, 1), "2: " + set("w = 1", 1, 2), "3: " + full(2, 1), "3: " + delete(1, 6) },
        // a row that node 1 returns is shadowed in the first merge by the partition deletion of node 2; node 3, which
        // returns nothing, has a newer version of it, and node 2 has its deletion
        { "returned row shadowed by a partition deletion", "st = 5 AND v = 1", "2",
          "1: " + row(1, 1, 1), "2: " + PARTITION_DELETION, "12: " + setStatic(5, 3), "3: " + ST0,
          "3: " + set("v = 1", 1, 5), "2: " + row(2, 1, 3), "2: " + delete(1, 6) },
        // the same without the deletion of the row
        { "returned row shadowed by a partition deletion, newer version", "st = 5 AND v = 1", "1,2",
          "1: " + row(1, 1, 1), "2: " + PARTITION_DELETION, "12: " + setStatic(5, 3), "3: " + ST0,
          "3: " + set("v = 1", 1, 5), "2: " + row(2, 1, 3) },
        // a replica that has none of the matching rows
        { "replica without matching rows", "st = 5 AND v = 1", "1,2",
          ST5_ALL, "12: " + row(1, 1, 1), "12: " + row(2, 1, 1), "3: " + row(3, 2, 1), "3: " + row(4, 2, 1), "3: " + row(5, 2, 1) },
    };

    /**
     * A row split between two replicas and deleted on the third, for 150 rows before the only match.
     */
    private static String[] wide()
    {
        List<String> c = new ArrayList<>(Arrays.asList("split rows, wide partition", "st = 5 AND v = 1 AND w = 1", "200",
                                                       ST5_ALL, "3: " + full(200, 1)));
        for (int ck = 1; ck <= 150; ck++)
        {
            c.add("1: " + set("v = 1", ck, 1));
            c.add("2: " + set("w = 1", ck, 1));
            c.add("3: " + delete(ck, 2));
        }
        return c.toArray(new String[0]);
    }

    private static List<String[]> cases()
    {
        List<String[]> cases = new ArrayList<>(Arrays.asList(CASES));
        cases.add(wide());
        return cases;
    }

    @Test
    public void testDivergentReplicas() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                                       .set("read_request_timeout", "60s")
                                                                       .set("range_request_timeout", "60s")
                                                                       .with(GOSSIP).with(NETWORK))
                                           .start(), 3))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.plain" + SCHEMA));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.indexed" + SCHEMA));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.legacy" + SCHEMA));
            for (String column : new String[]{ "v", "w", "r", "st", "sx" })
                cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.indexed(" + column + ") USING 'sai'"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.legacy(st) USING 'legacy_local_table'"));
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

            List<String[]> cases = cases();
            for (int c = 0; c < cases.size(); c++)
            {
                String[] testCase = cases.get(c);
                for (int i = 3; i < testCase.length; i++)
                {
                    String nodes = testCase[i].substring(0, testCase[i].indexOf(':'));
                    String write = testCase[i].substring(testCase[i].indexOf(':') + 2).replace("$PK", String.valueOf(c));
                    for (char node : nodes.toCharArray())
                        for (String table : TABLES)
                            cluster.get(node - '0').executeInternal(write.replace("%s", KEYSPACE + '.' + table));
                }
            }

            List<String> failures = new ArrayList<>();
            check(cluster, cases, "memtable", failures);
            cluster.forEach(instance -> instance.flush(KEYSPACE));
            check(cluster, cases, "sstable", failures);
            assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
        }
    }

    /**
     * With an analyzed index on the static column and only static columns restricted, the coordinator filters with
     * the index, which rejects a whole partition. Node 1 has the newer static row, which doesn't match.
     */
    @Test
    public void testAnalyzedStaticIndex() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                                       .with(GOSSIP).with(NETWORK))
                                           .start(), 3))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.analyzed (pk int, ck int, st text static, PRIMARY KEY (pk, ck)) " +
                                              "WITH read_repair = 'NONE'"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.analyzed(st) USING 'sai' WITH OPTIONS = { 'case_sensitive': 'false' }"));
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

            // partition 0 has a row, partition 1 only the static row
            for (int pk = 0; pk < 2; pk++)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.analyzed (pk, st) VALUES (?, 'Five') USING TIMESTAMP 1"),
                                               ConsistencyLevel.ALL, pk);
                cluster.get(1).executeInternal(withKeyspace("UPDATE %s.analyzed USING TIMESTAMP 2 SET st = 'Zero' WHERE pk = ?"), pk);
            }
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.analyzed (pk, ck) VALUES (0, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL);

            for (String source : new String[]{ "memtable", "sstable" })
            {
                if (source.equals("sstable"))
                    cluster.forEach(instance -> instance.flush(KEYSPACE));
                for (int pk = 0; pk < 2; pk++)
                    for (String where : new String[]{ "st = 'five'", "pk = " + pk + " AND st = 'five'" })
                        for (int pageSize : new int[]{ 0, 1, 100 })
                            assertEquals(source + ", page size " + pageSize + ": " + where, Collections.emptyList(),
                                         rows(cluster, withKeyspace("SELECT pk, ck FROM %s.analyzed WHERE " + where), pageSize, pk));
            }
        }
    }

    private static void check(Cluster cluster, List<String[]> cases, String source, List<String> failures)
    {
        for (int c = 0; c < cases.size(); c++)
        {
            String[] testCase = cases.get(c);
            List<String> all = Arrays.asList(testCase[2].split(","));
            for (String table : TABLES)
            {
                for (String query : queries(testCase[1], c, table))
                {
                    List<String> ordered = new ArrayList<>(all);
                    if (query.contains(" DESC"))
                        Collections.reverse(ordered);
                    int limit = query.contains("LIMIT 1") ? 1 : query.contains("LIMIT 2") ? 2 : Integer.MAX_VALUE;
                    List<String> expected = ordered.subList(0, Math.min(limit, ordered.size()));
                    for (int pageSize : new int[]{ 0, 1, 2, 100 })
                    {
                        String select = "SELECT pk, ck FROM " + KEYSPACE + '.' + table + " WHERE " + query + " ALLOW FILTERING";
                        List<String> actual = rows(cluster, select, pageSize, c);
                        if (!actual.equals(expected))
                            failures.add(String.format("[%s, %s, %s, %s] %s: expected %s but got %s", testCase[0], source, table,
                                                       pageSize == 0 ? "unpaged" : "page size " + pageSize, query, expected, actual));
                    }
                }
            }
        }
    }

    private static List<String> queries(String restriction, int pk, String table)
    {
        String partition = "pk = " + pk + " AND " + restriction;
        List<String> queries = new ArrayList<>(Arrays.asList(restriction, partition, partition + " LIMIT 1", partition + " LIMIT 2"));
        // ORDER BY isn't allowed with indexes; with indexes, a paged read of a partition with a per-partition limit
        // and a static expression returns its first row again on every page
        if (table.equals("plain"))
        {
            queries.add(partition + " PER PARTITION LIMIT 1");
            queries.add(partition + " ORDER BY ck DESC");
            queries.add(partition + " ORDER BY ck DESC LIMIT 1");
        }
        return queries;
    }

    /**
     * The clusterings of the rows of the given partition that the query returns, in order.
     */
    private static List<String> rows(Cluster cluster, String query, int pageSize, int pk)
    {
        List<String> rows = new ArrayList<>();
        try
        {
            Iterator<Object[]> iterator = pageSize == 0
                                          ? Arrays.asList(cluster.coordinator(1).execute(query, ConsistencyLevel.ALL)).iterator()
                                          : cluster.coordinator(1).executeWithPaging(query, ConsistencyLevel.ALL, pageSize);
            // no case has more than a few rows: stop a read that returns rows again and again
            while (iterator.hasNext() && rows.size() <= 10)
            {
                Object[] row = iterator.next();
                if ((Integer) row[0] == pk)
                    rows.add(String.valueOf(row[1]));
            }
        }
        catch (Throwable t)
        {
            rows.add("failed: " + t);
        }
        return rows;
    }
}
