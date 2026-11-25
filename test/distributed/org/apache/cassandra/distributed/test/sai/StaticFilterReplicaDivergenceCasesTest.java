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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
 * Replicas that diverge, one partition per case, three replicas (RF 3), read at ALL: each case says which writes each
 * node has, and the rows expected from the merge of all of them, for a filter with an expression on a static column.
 * Each replica filters its own data, with its own static row, so the coordinator must find what no replica returns
 * on its own: the rows the merged static row selects, and the deletions that the replicas returning a partition left
 * out. Each case is read with and without a partition restriction, with LIMIT 1 and 2 and PER PARTITION LIMIT 1,
 * unpaged and paged by 1, 2 and 100, with and without indexes, from memtables and SSTables. SAI reads a partition in
 * batches of at most 100 keys, and of one key with LIMIT 1 or page size 1, so the cases with deleted rows before the
 * first live match also check that what SAI decides for a partition holds for all its batches. The tests run with
 * non-strict replica filtering disabled and enabled.
 */
@RunWith(Parameterized.class)
public class StaticFilterReplicaDivergenceCasesTest extends TestBaseImpl
{
    private static final String SCHEMA = " (pk int, ck int, v int, r set<int>, st int static, sx int static, " +
                                         "PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'";

    private static final String ST0 = "INSERT INTO %s (pk, st, sx) VALUES (1, 0, 1) USING TIMESTAMP 1";

    // the cases checked on the table without indexes only
    private static final Set<String> WITHOUT_INDEXES_ONLY = Set.of("K");

    /**
     * Each case: a name, the restriction, the clusterings of the rows expected without limits ({@code null} for a row
     * of static content, nothing for no row), then the writes, each prefixed with the nodes that have it.
     */
    private static final String[][] CASES = {
        // two replica states: the replicas with the newer static row that matches also have the deletion of the row
        // that the stale replica returns; a deleted row satisfies the negation, and there is a live match after it
        { "E2", "(st = 5 AND v = 3) OR r NOT CONTAINS 3", "2",
          "1: " + ST0, "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 3, {3}) USING TIMESTAMP 1",
          "23: UPDATE %s USING TIMESTAMP 2 SET st = 5 WHERE pk = 1", "23: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 1",
          "23: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 0, {1}) USING TIMESTAMP 1" },
        // three replica states: the newer static row is on node 2; node 3 has the deletion of the stale row, which
        // satisfies the negation, and a live match after it
        { "E", "st = 5 OR r NOT CONTAINS 3", "2",
          "1: " + ST0, "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 2, {3}) USING TIMESTAMP 1",
          "2: UPDATE %s USING TIMESTAMP 3 SET st = 5 WHERE pk = 1",
          "3: " + ST0, "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 1", "3: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 1, {1}) USING TIMESTAMP 1" },
        // E4, E in a partition wider than a batch of keys: added by wide()
        // a range tombstone and a deleted row on the replicas with the newer static row that matches
        { "C", "st = 5 OR v = 1", "null",
          "1: " + ST0, "1: DELETE FROM %s USING TIMESTAMP 5 WHERE pk = 1 AND ck = 1", "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 2, {1}) USING TIMESTAMP 1",
          "23: UPDATE %s USING TIMESTAMP 2 SET st = 5 WHERE pk = 1", "23: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 7, {1}) USING TIMESTAMP 1",
          "23: DELETE FROM %s USING TIMESTAMP 3 WHERE pk = 1 AND ck = 2" },
        { "D", "st = 5", "null",
          "1: " + ST0, "1: DELETE FROM %s USING TIMESTAMP 5 WHERE pk = 1 AND ck = 1", "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 2, {1}) USING TIMESTAMP 1",
          "23: UPDATE %s USING TIMESTAMP 2 SET st = 5 WHERE pk = 1", "23: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 7, {1}) USING TIMESTAMP 1",
          "23: DELETE FROM %s USING TIMESTAMP 3 WHERE pk = 1 AND ck = 2" },
        // the other replicas only have the deletion, and no static row
        { "F", "st = 5 OR v = 1", "null",
          "1: INSERT INTO %s (pk, st, sx) VALUES (1, 5, 1) USING TIMESTAMP 1", "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 1, {1}) USING TIMESTAMP 1",
          "23: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck >= 0" },
        { "F2", "st = 5 OR v = 1", "",
          "1: INSERT INTO %s (pk, st, sx) VALUES (1, 5, 1) USING TIMESTAMP 1", "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 1, {1}) USING TIMESTAMP 1",
          "23: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1" },
        { "F3", "st = 5 OR v = 1", "",
          "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 1, {1}) USING TIMESTAMP 1", "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 1, {1}) USING TIMESTAMP 1",
          "23: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 1", "23: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck >= 2" },
        { "F4", "st = 5 OR ck = 1", "null",
          "12: INSERT INTO %s (pk, st, sx) VALUES (1, 5, 1) USING TIMESTAMP 1", "12: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 1, {1}) USING TIMESTAMP 1",
          "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck >= 0" },
        // two stale replicas, the deletion on the third, whose static row doesn't match
        { "G", "st = 5 OR ck = 1", "",
          "123: " + ST0, "12: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 1, {1}) USING TIMESTAMP 1",
          "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 1" },
        { "G2", "st = 5 OR v = 1", "",
          "123: " + ST0, "12: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 1, {1}) USING TIMESTAMP 1", "12: INSERT INTO %s (pk, ck, v, r) VALUES (1, 3, 1, {1}) USING TIMESTAMP 1",
          "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck >= 0" },
        { "G3", "st = 5 OR r NOT CONTAINS 3", "",
          "123: " + ST0, "12: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 1, {1}) USING TIMESTAMP 1", "12: INSERT INTO %s (pk, ck, v, r) VALUES (1, 3, 1, {1}) USING TIMESTAMP 1",
          "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 1", "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 3" },
        // the deletions come after the rows a limit lets the replica return
        { "H", "st = 5 OR v = 1", "0",
          "123: " + ST0, "123: INSERT INTO %s (pk, ck, v, r) VALUES (1, 0, 1, {1}) USING TIMESTAMP 1",
          "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 1, {1}) USING TIMESTAMP 1", "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 3, 1, {1}) USING TIMESTAMP 1",
          "23: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck >= 1" },
        { "H2", "st = 5 OR ck >= 0", "0",
          "123: " + ST0, "123: INSERT INTO %s (pk, ck, v, r) VALUES (1, 0, 1, {1}) USING TIMESTAMP 1",
          "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 1, {1}) USING TIMESTAMP 1", "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 3, 1, {1}) USING TIMESTAMP 1",
          "23: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 2", "23: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 3" },
        // a match split across replicas: the newer static row on node 2 selects the row of node 1
        { "S", "st = 5 OR v = 1", "1",
          "1: " + ST0, "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 2, {1}) USING TIMESTAMP 1",
          "2: UPDATE %s USING TIMESTAMP 3 SET st = 5 WHERE pk = 1" },
        // KNOWN GAP, pinned without indexes only: the match is split under an AND between node 3 (static row) and
        // node 1 (live row), and each replica drops the partition, so row 1 isn't found
        { "K", "st = 5 AND (sx = 5 OR ck = 1)", "",
          "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 1, {1}) USING TIMESTAMP 3",
          "3: UPDATE %s USING TIMESTAMP 2 SET st = 5 WHERE pk = 1", "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck >= 0" },
        // three replica states: node 3 returns the partition for row 2 without the deletion of row 1, which v = 1
        // doesn't match; node 2 has the newer static row that matches; node 1 is silent and its whole partition read
        { "A", "st = 5 OR v = 1", "2",
          "1: " + ST0, "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 2, {1}) USING TIMESTAMP 1",
          "2: UPDATE %s USING TIMESTAMP 3 SET st = 5 WHERE pk = 1",
          "3: " + ST0, "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 1", "3: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 1, {1}) USING TIMESTAMP 1" },
        // the same with a range tombstone
        { "A2", "st = 5 OR v = 1", "2",
          "1: " + ST0, "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 2, {1}) USING TIMESTAMP 1",
          "2: UPDATE %s USING TIMESTAMP 3 SET st = 5 WHERE pk = 1",
          "3: " + ST0, "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck <= 1", "3: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 1, {1}) USING TIMESTAMP 1" },
        // two replica states, an AND of static and regular under the OR
        { "B", "(st = 5 AND v = 3) OR v = 1", "2",
          "1: " + ST0, "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 3, {1}) USING TIMESTAMP 1",
          "23: UPDATE %s USING TIMESTAMP 2 SET st = 5 WHERE pk = 1", "23: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 1",
          "23: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 1, {1}) USING TIMESTAMP 1" },
        // as B, with a clustering restriction on the other side of the OR, which the deleted row doesn't satisfy
        { "E3", "(st = 5 AND v = 3) OR ck >= 2", "2",
          "1: " + ST0, "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 3, {3}) USING TIMESTAMP 1",
          "23: UPDATE %s USING TIMESTAMP 2 SET st = 5 WHERE pk = 1", "23: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 1",
          "23: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 0, {1}) USING TIMESTAMP 1" },
        // without a disjunction: node 3 returns the partition for row 2 without the
        // deletion of row 1, node 2 has nothing that matches but the static row, node 1 the stale static row and row 1
        { "I", "st = 5 AND v = 1", "2",
          "1: " + ST0, "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 1, {1}) USING TIMESTAMP 1",
          "23: UPDATE %s USING TIMESTAMP 3 SET st = 5 WHERE pk = 1",
          "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 1", "3: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 1, {1}) USING TIMESTAMP 1" },
        // a live row missing: nodes 1 and 3 return the partition for row 1 with their older static row, without row 2
        // of node 1, which the newer static row of node 2 selects; node 2 returns its static row; nobody is silent
        // about the static row or any of its columns, so only comparing what the static rows select finds that they
        // diverge
        { "J", "v = 1 OR sx = 5", "1,2",
          "123: " + ST0, "13: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 1, {1}) USING TIMESTAMP 1",
          "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 2, {1}) USING TIMESTAMP 1",
          "2: UPDATE %s USING TIMESTAMP 3 SET sx = 5 WHERE pk = 1" },
        // static rows that diverge without changing what the filter selects (st = 7 doesn't match either): the whole
        // partitions aren't read, and the deletion of row 1 on node 3, which returns the partition for row 2, is still
        // found, as nodes 1 and 2 return row 1 and node 3 is silent about it
        { "L", "st = 5 OR v = 1", "2",
          "123: " + ST0, "123: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 1, {1}) USING TIMESTAMP 1",
          "2: UPDATE %s USING TIMESTAMP 3 SET st = 7 WHERE pk = 1",
          "3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = 1", "3: INSERT INTO %s (pk, ck, v, r) VALUES (1, 2, 1, {1}) USING TIMESTAMP 1" },
    };

    @Parameterized.Parameter
    public boolean nonStrict;

    @Parameterized.Parameters(name = "nonStrict={0}")
    public static Collection<Object[]> parameters()
    {
        return SAIUtil.nonStrictReplicaFilteringModes();
    }

    /**
     * E4: E in a partition wider than the 100 keys that SAI reads at a time: node 3 has 150 deleted rows, which satisfy
     * the negation, before its live match, so the first batch of keys only has deleted rows, without limit or paging.
     */
    private static String[] wide()
    {
        List<String> c = new ArrayList<>(Arrays.asList("E4", "st = 5 OR r NOT CONTAINS 3", "200",
                                                       "1: " + ST0, "1: INSERT INTO %s (pk, ck, v, r) VALUES (1, 1, 2, {3}) USING TIMESTAMP 1",
                                                       "2: UPDATE %s USING TIMESTAMP 3 SET st = 5 WHERE pk = 1",
                                                       "3: " + ST0, "3: INSERT INTO %s (pk, ck, v, r) VALUES (1, 200, 1, {1}) USING TIMESTAMP 1"));
        for (int ck = 1; ck <= 150; ck++)
            c.add("3: DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck = " + ck);
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
                                           .withConfig(config ->
                                                       SAIUtil.withNonStrictReplicaFiltering(config, nonStrict)
                                                              .set("hinted_handoff_enabled", false)
                                                              // 5 indexes per case
                                                              .set("sai_indexes_total_fail_threshold", 1000)
                                                              .set("read_request_timeout", "60s")
                                                              .set("range_request_timeout", "60s")
                                                              .with(GOSSIP).with(NETWORK))
                                           .start(), 3))
        {
            for (String[] c : cases())
            {
                cluster.schemaChange("CREATE TABLE " + table("plain", c) + SCHEMA);
                cluster.schemaChange("CREATE TABLE " + table("indexed", c) + SCHEMA);
                for (String column : new String[]{ "ck", "v", "r", "st", "sx" })
                    cluster.schemaChange("CREATE CUSTOM INDEX ON " + table("indexed", c) + '(' + column + ") USING 'StorageAttachedIndex'");
            }
            SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);
            for (String[] c : cases())
            {
                for (int i = 3; i < c.length; i++)
                {
                    String nodes = c[i].substring(0, c[i].indexOf(':'));
                    String write = c[i].substring(c[i].indexOf(':') + 2);
                    for (char node : nodes.toCharArray())
                        for (String kind : new String[]{ "plain", "indexed" })
                            cluster.get(node - '0').executeInternal(write.replace("%s", table(kind, c)));
                }
            }

            List<String> failures = new ArrayList<>();
            check(cluster, "memtable", failures);
            cluster.forEach(instance -> instance.flush(KEYSPACE));
            check(cluster, "sstable", failures);
            assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
        }
    }

    private static String table(String kind, String[] c)
    {
        return KEYSPACE + '.' + kind + '_' + c[0].toLowerCase();
    }

    private static void check(Cluster cluster, String source, List<String> failures)
    {
        for (String[] c : cases())
        {
            List<String> all = c[2].isEmpty() ? new ArrayList<>() : Arrays.asList(c[2].split(","));
            for (String kind : new String[]{ "plain", "indexed" })
            {
                if (kind.equals("indexed") && WITHOUT_INDEXES_ONLY.contains(c[0]))
                    continue;
                for (String prefix : new String[]{ "", "pk = 1 AND " })
                {
                    String restriction = prefix.isEmpty() ? c[1] : prefix + '(' + c[1] + ')';
                    List<String> variants = new ArrayList<>(Arrays.asList("", " LIMIT 1", " PER PARTITION LIMIT 1", " LIMIT 2"));
                    // reversed reads, which ORDER BY allows without indexes and with a partition restriction only
                    if (kind.equals("plain") && !prefix.isEmpty())
                        variants.addAll(Arrays.asList(" ORDER BY ck DESC", " ORDER BY ck DESC LIMIT 1"));
                    for (String limits : variants)
                    {
                        int limit = limits.endsWith("1") ? 1 : limits.endsWith("2") ? 2 : Integer.MAX_VALUE;
                        List<String> ordered = new ArrayList<>(all);
                        if (limits.contains(" DESC"))
                            Collections.reverse(ordered);
                        List<String> expected = ordered.subList(0, Math.min(limit, ordered.size()));
                        for (int pageSize : new int[]{ 0, 1, 2, 100 })
                        {
                            String query = "SELECT pk, ck FROM " + table(kind, c) + " WHERE " + restriction + limits + " ALLOW FILTERING";
                            List<String> actual = rows(cluster, query, pageSize);
                            if (!actual.equals(expected))
                                failures.add(String.format("[%s %s %s %s] %s%s: expected %s but got %s", c[0], source, kind,
                                                           pageSize == 0 ? "unpaged" : "page size " + pageSize,
                                                           restriction, limits, expected, actual));
                        }
                    }
                }
            }
        }
    }

    private static List<String> rows(Cluster cluster, String query, int pageSize)
    {
        List<String> rows = new ArrayList<>();
        try
        {
            Iterator<Object[]> iterator = pageSize == 0
                                          ? Arrays.asList(cluster.coordinator(1).execute(query, ConsistencyLevel.ALL)).iterator()
                                          : cluster.coordinator(1).executeWithPaging(query, ConsistencyLevel.ALL, pageSize);
            while (iterator.hasNext())
                rows.add(String.valueOf(iterator.next()[1]));
        }
        catch (Throwable t)
        {
            rows.add("failed: " + t);
        }
        return rows;
    }
}
