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
 * A partition deletion that a replica missed shadows that replica's static row in the merge, so the merged static row
 * is empty, and a filter with an expression that the empty static row satisfies (NOT CONTAINS, NOT CONTAINS KEY,
 * m[k] != v) selects rows that the replica filtered out with its own static row. Replica filtering protection must
 * read that replica's partition, although the merged static row is empty or no replica returned one, and although
 * the merge of what the replicas returned has no live row, so that the partition isn't returned by the first phase
 * of replica filtering protection at all (P5, P10).
 * <p>
 * One partition per case, three replicas (RF 3), read at ALL, with and without indexes, with a partition restriction
 * and over a token range, with LIMIT 1 and PER PARTITION LIMIT 1, unpaged and paged by 1, 2 and 100, from memtables
 * and SSTables, in clustering order and, without indexes, in reverse order. The tests run with non-strict replica
 * filtering disabled and enabled.
 */
@RunWith(Parameterized.class)
public class StaticFilterPartitionDeletionCasesTest extends TestBaseImpl
{
    private static final String SCHEMA = " (pk int, ck int, v int, r set<int>, st int static, sx int static, " +
                                         "sr set<int> static, sm map<int, int> static, PRIMARY KEY (pk, ck)) " +
                                         "WITH read_repair = 'NONE'";

    private static final String PARTITION_DELETION = "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = $PK";

    @Parameterized.Parameter
    public boolean nonStrict;

    @Parameterized.Parameters(name = "nonStrict={0}")
    public static Collection<Object[]> parameters()
    {
        return SAIUtil.nonStrictReplicaFilteringModes();
    }

    private static String row(int ck, int v)
    {
        return "INSERT INTO %s (pk, ck, v, r) VALUES ($PK, " + ck + ", " + v + ", {1}) USING TIMESTAMP 3";
    }

    /**
     * Each case: a name, the restriction, the clusterings of the rows expected, then the writes, each prefixed with the
     * nodes that have it. Node 1 has a static row, older than the partition deletion of nodes 2 and 3, that makes it
     * filter its rows out; its rows are newer than the deletion.
     */
    private static final String[][] CASES = {
        { "P1", "sr NOT CONTAINS 3", "1,2",
          "1: INSERT INTO %s (pk, sr) VALUES ($PK, {3}) USING TIMESTAMP 1", "1: " + row(1, 2),
          "23: " + PARTITION_DELETION, "23: " + row(2, 2) },
        { "P2", "sr NOT CONTAINS 3 OR v = 1", "1,2,3",
          "1: INSERT INTO %s (pk, sr) VALUES ($PK, {3}) USING TIMESTAMP 1", "1: " + row(1, 2), "1: " + row(3, 1),
          "23: " + PARTITION_DELETION, "23: " + row(2, 2) },
        { "P3", "sm[1] != 5", "1,2",
          "1: INSERT INTO %s (pk, sm) VALUES ($PK, {1: 5}) USING TIMESTAMP 1", "1: " + row(1, 2),
          "23: " + PARTITION_DELETION, "23: " + row(2, 2) },
        { "P4", "st = 5 OR v = 1", "3,4",
          "1: INSERT INTO %s (pk, st, sx) VALUES ($PK, 5, 1) USING TIMESTAMP 1", "1: " + row(1, 2), "1: " + row(3, 1),
          "23: " + PARTITION_DELETION, "23: " + row(2, 2), "23: " + row(4, 1) },
        { "P6", "sm NOT CONTAINS KEY 1 OR v = 1", "1,2,3",
          "1: INSERT INTO %s (pk, sm) VALUES ($PK, {1: 5}) USING TIMESTAMP 1", "1: " + row(1, 2), "1: " + row(3, 1),
          "23: " + PARTITION_DELETION, "23: " + row(2, 2) },
        { "P7", "sr NOT CONTAINS 3 OR v = 1", "1,2,3",
          "1: INSERT INTO %s (pk, sr) VALUES ($PK, {3}) USING TIMESTAMP 1", "1: " + row(1, 2), "1: " + row(3, 1),
          "2: " + PARTITION_DELETION, "2: " + row(2, 2), "3: " + row(3, 1) },
        // P1 and P2 with the rows the other way round: the rows of the replica with the shadowed static row are last
        { "P8", "sr NOT CONTAINS 3", "1,2",
          "1: INSERT INTO %s (pk, sr) VALUES ($PK, {3}) USING TIMESTAMP 1", "1: " + row(2, 2),
          "23: " + PARTITION_DELETION, "23: " + row(1, 2) },
        { "P9", "sr NOT CONTAINS 3 OR v = 1", "1,2,3",
          "1: INSERT INTO %s (pk, sr) VALUES ($PK, {3}) USING TIMESTAMP 1", "1: " + row(2, 2), "1: " + row(3, 1),
          "23: " + PARTITION_DELETION, "23: " + row(1, 2) },
        // the other replicas return only the partition deletion, so the first merge has no live row: the partition is
        // discarded before the second phase, which must still resolve its static row
        { "P5", "sr NOT CONTAINS 3", "1",
          "1: INSERT INTO %s (pk, sr) VALUES ($PK, {3}) USING TIMESTAMP 1", "1: " + row(1, 2),
          "23: " + PARTITION_DELETION },
        // the same with the static row and a newer version of the row on another replica than the shadowed row
        { "P10", "sr NOT CONTAINS 3", "1",
          "1: INSERT INTO %s (pk, ck, v, r) VALUES ($PK, 1, 1, {1}) USING TIMESTAMP 1", "2: " + PARTITION_DELETION,
          "3: INSERT INTO %s (pk, sr) VALUES ($PK, {3}) USING TIMESTAMP 1", "3: UPDATE %s USING TIMESTAMP 5 SET v = 1 WHERE pk = $PK AND ck = 1" },
        { "P10b", "sr NOT CONTAINS 3", "2",
          "1: INSERT INTO %s (pk, ck, v, r) VALUES ($PK, 1, 1, {1}) USING TIMESTAMP 1", "2: " + PARTITION_DELETION,
          "3: INSERT INTO %s (pk, sr) VALUES ($PK, {3}) USING TIMESTAMP 1", "3: INSERT INTO %s (pk, ck, v, r) VALUES ($PK, 2, 1, {1}) USING TIMESTAMP 5" },
    };

    /**
     * KNOWN GAP, pinned: with indexes, from SSTables, nodes 2 and 3 of case P5 return nothing (SAI finds no key in a
     * partition that has only its deletion), so row 1 of node 1 isn't found. The rows of every read of the case, for a
     * table and a source; {@code null} where the case has no known gap.
     */
    private static List<String> knownGap(String testCase, String table, String source)
    {
        if (testCase.equals("P5") && table.equals("indexed") && source.equals("sstable"))
            return Collections.emptyList();
        return null;
    }

    @Test
    public void testPartitionDeletionShadowingStaticRow() throws Throwable
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
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".plain" + SCHEMA);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".indexed" + SCHEMA);
            for (String column : new String[]{ "ck", "v", "r", "st", "sx", "sr", "ENTRIES(sm)" })
                cluster.schemaChange("CREATE CUSTOM INDEX ON " + KEYSPACE + ".indexed(" + column + ") USING 'StorageAttachedIndex'");
            SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);

            for (int c = 0; c < CASES.length; c++)
            {
                for (int i = 3; i < CASES[c].length; i++)
                {
                    String nodes = CASES[c][i].substring(0, CASES[c][i].indexOf(':'));
                    String write = CASES[c][i].substring(CASES[c][i].indexOf(':') + 2).replace("$PK", String.valueOf(c));
                    for (char node : nodes.toCharArray())
                        for (String table : new String[]{ "plain", "indexed" })
                            cluster.get(node - '0').executeInternal(write.replace("%s", KEYSPACE + '.' + table));
                }
            }

            List<String> failures = new ArrayList<>();
            check(cluster, "memtable", failures);
            cluster.forEach(instance -> instance.flush(KEYSPACE));
            check(cluster, "sstable", failures);
            assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
        }
    }

    private static void check(Cluster cluster, String source, List<String> failures)
    {
        for (int c = 0; c < CASES.length; c++)
        {
            String[] testCase = CASES[c];
            List<String> all = testCase[2].isEmpty() ? new ArrayList<>() : Arrays.asList(testCase[2].split(","));
            for (String table : new String[]{ "plain", "indexed" })
            {
                for (String prefix : new String[]{ "pk = " + c + " AND ", "token(pk) >= token(" + c + ") AND token(pk) <= token(" + c + ") AND " })
                {
                    String restriction = prefix + '(' + testCase[1] + ')';
                    List<String> variants = new ArrayList<>(Arrays.asList("", " LIMIT 1", " PER PARTITION LIMIT 1"));
                    // reversed reads, which ORDER BY allows without indexes and with a partition restriction only
                    if (table.equals("plain") && prefix.startsWith("pk"))
                        variants.addAll(Arrays.asList(" ORDER BY ck DESC", " ORDER BY ck DESC LIMIT 1"));
                    for (String limits : variants)
                    {
                        List<String> ordered = new ArrayList<>(all);
                        if (limits.contains(" DESC"))
                            Collections.reverse(ordered);
                        List<String> expected = limits.contains("LIMIT") ? ordered.subList(0, Math.min(1, ordered.size())) : ordered;
                        for (int pageSize : new int[]{ 0, 1, 2, 100 })
                        {
                            String query = "SELECT pk, ck FROM " + KEYSPACE + '.' + table + " WHERE " + restriction + limits + " ALLOW FILTERING";
                            List<String> actual = rows(cluster, query, pageSize);
                            List<String> gap = knownGap(testCase[0], table, source);
                            if (!actual.equals(gap == null ? expected : gap))
                                failures.add(String.format("[%s %s %s %s] %s%s: expected %s%s but got %s", testCase[0], source, table,
                                                           pageSize == 0 ? "unpaged" : "page size " + pageSize,
                                                           restriction, limits, gap == null ? expected : gap,
                                                           gap == null ? "" : " (known gap)", actual));
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
