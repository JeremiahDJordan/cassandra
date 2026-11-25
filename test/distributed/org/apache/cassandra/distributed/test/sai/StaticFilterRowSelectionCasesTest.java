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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertTrue;

/**
 * Replica filtering protection reads the whole partition of the replicas that are silent about the static row, or
 * whose static row is stricter than the static row of the partition, for a filter with a static expression. The rows
 * those reads bring can only be returned without their deletions if a replica that returned the partition left the
 * deletion out, so those rows must be read by name from such replicas;
 * which ones is decided on the merge of every version of the row that the coordinator has, with their deletions, not on
 * one replica's version: the match can need the columns of two silent replicas, a deletion that another replica
 * returned can remove what kept the row from matching, and a returned row can be shadowed in the first merge by
 * another replica's deletion while a silent replica has a newer version of it.
 * <p>
 * One partition per case, three replicas (RF 3), read at ALL, with and without indexes, with a partition restriction
 * and over the whole table, with LIMIT 1 and PER PARTITION LIMIT 1, unpaged and paged by 1, 2 and 100, in clustering
 * order and, without indexes, in reverse order, from memtables and SSTables.
 */
public class StaticFilterRowSelectionCasesTest extends TestBaseImpl
{
    private static final String SCHEMA = " (pk int, ck int, v int, w int, r set<int>, st int static, sx int static, " +
                                         "sr set<int> static, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'";

    private static final String ST0 = "INSERT INTO %s (pk, st, sx) VALUES ($PK, 0, 1) USING TIMESTAMP 1";
    private static final String ST5_ALL = "123: INSERT INTO %s (pk, st, sx) VALUES ($PK, 5, 1) USING TIMESTAMP 1";
    private static final String PARTITION_DELETION = "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = $PK";

    private static String row(int ck, int v, int ts)
    {
        return "INSERT INTO %s (pk, ck, v, r) VALUES ($PK, " + ck + ", " + v + ", {1}) USING TIMESTAMP " + ts;
    }

    private static String del(int ck, int ts)
    {
        return "DELETE FROM %s USING TIMESTAMP " + ts + " WHERE pk = $PK AND ck = " + ck;
    }

    private static String st(int value, int ts)
    {
        return "UPDATE %s USING TIMESTAMP " + ts + " SET st = " + value + " WHERE pk = $PK";
    }

    private static String set(String assignment, int ck, int ts)
    {
        return "UPDATE %s USING TIMESTAMP " + ts + " SET " + assignment + " WHERE pk = $PK AND ck = " + ck;
    }

    private static String full(int ck, int ts)
    {
        return "INSERT INTO %s (pk, ck, v, w, r) VALUES ($PK, " + ck + ", 1, 1, {1, 2}) USING TIMESTAMP " + ts;
    }

    /**
     * Each case: a name, the restriction, the clusterings of the rows expected, then the writes, each prefixed with the
     * nodes that have it.
     */
    private static final String[][] CASES = {
        // identical static rows; a row whose columns are on two replicas that return nothing, deleted on the third
        { "X1", "st = 5 AND v = 1 AND w = 1", "2",
          ST5_ALL, "1: " + set("v = 1", 1, 1), "2: " + set("w = 1", 1, 1), "3: " + del(1, 2), "3: " + full(2, 1) },
        { "X1b", "(st = 5 AND v = 1 AND w = 1) OR v = 9", "2",
          ST5_ALL, "1: " + set("v = 1", 1, 1), "2: " + set("w = 1", 1, 1), "3: " + del(1, 2), "3: " + full(2, 1) },
        { "X1c", "st = 5 AND r CONTAINS 1 AND r CONTAINS 2", "2",
          ST5_ALL, "1: " + set("r = r + {1}", 1, 1), "2: " + set("r = r + {2}", 1, 1), "3: " + del(1, 2),
          "3: " + full(2, 1) },
        { "X1d", "st = 5 AND v = 1 AND w = 1", "1,2",
          ST5_ALL, "1: " + set("v = 1", 1, 1), "2: " + set("w = 1", 1, 1), "3: " + full(2, 1) },
        { "X1e", "st = 5 AND v = 1 AND w = 1", "2",
          ST5_ALL, "1: " + set("v = 1", 1, 1), "2: " + set("w = 1", 1, 1),
          "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = $PK AND ck <= 1",
          "3: " + full(2, 1) },
        { "X1f", "st = 5 AND v = 1 AND w = 1", "2",
          "12: INSERT INTO %s (pk, st, sx) VALUES ($PK, 5, 1) USING TIMESTAMP 1", "3: INSERT INTO %s (pk, st, sx) VALUES ($PK, 5, 2) USING TIMESTAMP 2",
          "1: " + set("v = 1", 1, 1), "2: " + set("w = 1", 1, 1), "3: " + del(1, 2), "3: " + full(2, 1) },
        // one replica returns nothing; another's returned deletion removes what kept its row from matching; the third
        // has the row's deletion
        { "X2", "st = 5 AND v = 1 AND r NOT CONTAINS 3", "2",
          "1: " + set("r = {3}", 1, 1), "1: " + set("v = 1", 1, 5), "2: " + PARTITION_DELETION, "123: " + st(5, 3),
          "23: " + full(2, 3), "3: " + del(1, 6) },
        { "X2b", "st = 5 AND v = 1 AND r NOT CONTAINS 3", "2",
          ST5_ALL, "1: " + set("r = {3}", 1, 1), "1: " + set("v = 1", 1, 5),
          "2: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = $PK AND ck <= 1",
          "23: " + full(2, 3), "3: " + del(1, 6) },
        { "X2c", "st = 5 AND v = 1 AND r NOT CONTAINS 3", "1,2",
          "1: " + set("r = {3}", 1, 1), "1: " + set("v = 1", 1, 5), "2: " + PARTITION_DELETION, "123: " + st(5, 3),
          "23: " + full(2, 3) },
        // two replica states only: node 1 missed a partition (or range) deletion, a row, and the deletion of another row
        { "X2d", "st = 5 AND v = 1 AND r NOT CONTAINS 3", "2",
          "123: " + set("r = {3}", 1, 1), "23: " + PARTITION_DELETION, "123: " + st(5, 3), "23: " + full(2, 3),
          "123: " + set("v = 1", 1, 5), "23: " + del(1, 6) },
        { "X2e", "st = 5 AND v = 1 AND r NOT CONTAINS 3", "2",
          ST5_ALL, "123: " + set("r = {3}", 1, 1), "23: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = $PK AND ck <= 1",
          "23: " + full(2, 3), "123: " + set("v = 1", 1, 5), "23: " + del(1, 6) },
        { "X2f", "(st = 5 AND v = 1 AND r NOT CONTAINS 3) OR v = 9", "2",
          "123: " + set("r = {3}", 1, 1), "23: " + PARTITION_DELETION, "123: " + st(5, 3), "23: " + full(2, 3),
          "123: " + set("v = 1", 1, 5), "23: " + del(1, 6) },
        // the deletion that makes the row match is on a second replica that returns nothing
        { "X3", "st = 5 AND v = 1 AND r NOT CONTAINS 3", "2",
          ST5_ALL, "1: " + set("r = {3}", 1, 1), "1: " + set("v = 1", 1, 5),
          "2: DELETE r FROM %s USING TIMESTAMP 2 WHERE pk = $PK AND ck = 1",
          "3: " + full(2, 1), "3: " + del(1, 6) },
        { "X6", "st = 5 AND v = 1 AND w != 3", "2",
          ST5_ALL, "1: " + set("w = 3", 1, 1), "1: " + set("v = 1", 1, 5),
          "2: DELETE w FROM %s USING TIMESTAMP 2 WHERE pk = $PK AND ck = 1",
          "3: " + full(2, 1), "3: " + del(1, 6) },
        { "X7", "st = 5 AND v = 1 AND w = 1", "2",
          ST5_ALL, "1: " + set("v = 1, w = 3", 1, 1), "2: " + set("w = 1", 1, 2), "3: " + full(2, 1),
          "3: " + del(1, 6) },
        // a row that one replica returned, shadowed in the first merge by another's partition deletion; the replica that
        // returns nothing has a newer version of it, and the replica with the partition deletion has its deletion
        { "X8", "st = 5 AND v = 1", "2",
          "1: " + row(1, 1, 1), "2: " + PARTITION_DELETION, "12: " + st(5, 3), "3: " + ST0, "3: " + set("v = 1", 1, 5),
          "2: " + row(2, 1, 3), "2: " + del(1, 6) },
        { "X8b", "st = 5 AND v = 1", "1,2",
          "1: " + row(1, 1, 1), "2: " + PARTITION_DELETION, "12: " + st(5, 3), "3: " + ST0, "3: " + set("v = 1", 1, 5),
          "2: " + row(2, 1, 3) },
        // the replicas that the empty merged static row makes silent, with the row split between them
        { "H2o", "sr NOT CONTAINS 3 AND v = 1 AND w = 1", "3",
          "12: INSERT INTO %s (pk, sr) VALUES ($PK, {3}) USING TIMESTAMP 1", "1: " + set("v = 1", 1, 3), "2: " + set("w = 1", 1, 3),
          "3: " + PARTITION_DELETION, "3: " + full(3, 3), "3: " + del(1, 4) },
        // a replica that lacks the matching rows, and static rows that differ, read in both orders
        { "X11", "st = 5 AND v = 1", "1,2",
          ST5_ALL, "12: " + row(1, 1, 1), "12: " + row(2, 1, 1), "3: " + row(3, 2, 1), "3: " + row(4, 2, 1),
          "3: " + row(5, 2, 1) },
        { "X12", "st = 5 OR v = 1", "2,4",
          "1: " + ST0, "1: " + row(1, 1, 1), "1: " + row(3, 2, 1), "23: " + st(7, 2), "23: " + del(1, 2), "23: " + row(2, 1, 1), "23: " + row(4, 1, 1) },
        { "X13", "st = 5 OR v = 1", "3,5",
          "1: INSERT INTO %s (pk, st, sx) VALUES ($PK, 5, 1) USING TIMESTAMP 1", "1: " + row(1, 2, 1), "1: " + row(2, 1, 1), "1: " + row(4, 2, 1),
          "23: " + st(0, 2), "23: " + del(2, 3), "23: " + row(3, 1, 1), "23: " + row(5, 1, 1) },
        // the static row that matches is newer on two nodes; the last row is on the third alone (see KNOWN_GAPS)
        { "X14", "st = 5 AND v = 1", "1,2,3",
          "1: " + ST0, "1: " + row(1, 1, 1), "1: " + row(3, 1, 1), "23: " + st(5, 2), "23: " + row(2, 1, 1) },
        // a match split between replicas under an AND. X9: the only static row is on a replica that returns nothing
        // (see KNOWN_GAPS); X10: the two static columns of the AND are newer on different replicas
        // (see KNOWN_GAP_CASES)
        { "X9", "(st = 5 AND v = 1) OR v = 2", "1,2",
          "1: INSERT INTO %s (pk, st) VALUES ($PK, 5) USING TIMESTAMP 1", "2: " + row(1, 1, 1), "2: " + row(2, 2, 1) },
        { "X10", "(st = 5 AND sx = 5) OR v = 1", "1,2",
          "1: INSERT INTO %s (pk, st) VALUES ($PK, 5) USING TIMESTAMP 2", "1: INSERT INTO %s (pk, sx) VALUES ($PK, 0) USING TIMESTAMP 1",
          "1: " + row(1, 2, 1), "23: INSERT INTO %s (pk, st) VALUES ($PK, 0) USING TIMESTAMP 1",
          "23: INSERT INTO %s (pk, sx) VALUES ($PK, 5) USING TIMESTAMP 2", "23: " + row(2, 2, 1) },
    };

    /**
     * Cases whose rows no read returns, pinned (with indexes the right rows are accepted too): the match needs static
     * columns that are newer on different replicas under an AND, so no replica returns the partition.
     */
    private static final String[][] KNOWN_GAP_CASES = {
        { "X10", "" },
    };

    /**
     * Paged reads without a LIMIT, pinned (with indexes the right rows are accepted too): a page whose match only
     * replicas that return nothing have is not completed by replica filtering protection.
     */
    private static final String[][] KNOWN_GAPS = {
        // the split row is first in clustering order, so alone on the last page in reverse order
        { "X1d", "DESC", "1", "2" },
        { "X2c", "DESC", "1", "2" },
        // row 1 is alone on the last page in reverse order: node 2 doesn't return it without the static row of node 1
        { "X9", "DESC", "1", "2" },
        { "X14", "ASC", "1", "1,2" },
        { "X14", "ASC", "2", "1,2" },
        { "X14", "DESC", "1", "3,2" },
        { "X14", "DESC", "2", "3,2" },
    };

    private static List<String> knownGap(String testCase, String query, int pageSize)
    {
        if (query.contains("LIMIT"))
            return null;
        for (String[] gap : KNOWN_GAPS)
            if (gap[0].equals(testCase) && gap[1].equals(query.contains(" DESC") ? "DESC" : "ASC") && Integer.parseInt(gap[2]) == pageSize)
                return Arrays.asList(gap[3].split(","));
        return null;
    }

    /**
     * X5: X1 over 150 rows, wider than the batches of keys SAI reads.
     */
    private static String[] wide()
    {
        List<String> c = new ArrayList<>(Arrays.asList("X5", "st = 5 AND v = 1 AND w = 1", "200", ST5_ALL,
                                                       "3: " + full(200, 1)));
        for (int ck = 1; ck <= 150; ck++)
        {
            c.add("1: " + set("v = 1", ck, 1));
            c.add("2: " + set("w = 1", ck, 1));
            c.add("3: " + del(ck, 2));
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
    public void testRowsReadByName() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config ->
                                                       config.set("hinted_handoff_enabled", false)
                                                             .set("read_request_timeout", "60s")
                                                             .set("range_request_timeout", "60s")
                                                             .with(GOSSIP).with(NETWORK))
                                           .start(), 3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".plain" + SCHEMA);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".indexed" + SCHEMA);
            for (String column : new String[]{ "ck", "v", "w", "r", "st", "sx", "sr" })
                cluster.schemaChange("CREATE CUSTOM INDEX ON " + KEYSPACE + ".indexed(" + column + ") USING 'StorageAttachedIndex'");
            SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);

            List<String[]> cases = cases();
            for (int c = 0; c < cases.size(); c++)
            {
                String[] testCase = cases.get(c);
                for (int i = 3; i < testCase.length; i++)
                {
                    String nodes = testCase[i].substring(0, testCase[i].indexOf(':'));
                    String write = testCase[i].substring(testCase[i].indexOf(':') + 2).replace("$PK", String.valueOf(c));
                    for (char node : nodes.toCharArray())
                        for (String table : new String[]{ "plain", "indexed" })
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

    private static void check(Cluster cluster, List<String[]> cases, String source, List<String> failures)
    {
        for (int c = 0; c < cases.size(); c++)
        {
            String[] testCase = cases.get(c);
            List<String> all = testCase[2].isEmpty() ? new ArrayList<>() : Arrays.asList(testCase[2].split(","));
            for (String table : new String[]{ "plain", "indexed" })
            {
                for (String query : queries(testCase[1], c, table))
                {
                    List<String> expected = rowsOf(all, query);
                    List<String> gapOfCase = null;
                    for (String[] gap : KNOWN_GAP_CASES)
                        if (gap[0].equals(testCase[0]))
                            gapOfCase = rowsOf(gap[1].isEmpty() ? Collections.emptyList() : Arrays.asList(gap[1].split(",")), query);
                    for (int pageSize : new int[]{ 0, 1, 2, 100 })
                    {
                        String select = "SELECT pk, ck FROM " + KEYSPACE + '.' + table + " WHERE " + query + " ALLOW FILTERING";
                        List<String> actual = rows(cluster, select, pageSize, c);
                        List<String> gap = gapOfCase != null ? gapOfCase : knownGap(testCase[0], query, pageSize);
                        boolean rightAccepted = table.equals("indexed") || !query.startsWith("pk = ");
                        boolean right = gap == null ? actual.equals(expected)
                                                    : actual.equals(gap) || (rightAccepted && actual.equals(expected));
                        if (!right)
                            failures.add(String.format("[%s %s %s %s] %s: expected %s%s but got %s", testCase[0], source, table,
                                                       pageSize == 0 ? "unpaged" : "page size " + pageSize, query,
                                                       gap == null ? expected : gap, gap == null ? "" : " (known gap)", actual));
                    }
                }
            }
        }
    }

    /**
     * The given rows, in clustering order, as the given query returns them: in its order and with its limit.
     */
    private static List<String> rowsOf(List<String> rows, String query)
    {
        List<String> ordered = new ArrayList<>(rows);
        if (query.contains(" DESC"))
            Collections.reverse(ordered);
        int limit = query.contains(" LIMIT 1") ? 1 : Integer.MAX_VALUE;
        return ordered.subList(0, Math.min(limit, ordered.size()));
    }

    private static List<String> queries(String restriction, int pk, String table)
    {
        String partition = "pk = " + pk + " AND (" + restriction + ')';
        List<String> queries = new ArrayList<>(Arrays.asList(restriction,
                                                             partition,
                                                             partition + " LIMIT 1",
                                                             partition + " PER PARTITION LIMIT 1"));
        // ORDER BY isn't allowed with indexes
        if (table.equals("plain"))
        {
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
            while (iterator.hasNext())
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
