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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;

import static org.junit.Assert.fail;

/**
 * ORDER BY (generic, BM25 and ANN) restricted by an index that produces keys without a clustering for static rows: an
 * index on a partition key component, or on a static column. Those keys stand for their whole partition, and the
 * ordering must order the rows of that partition, with every hybrid plan, at every stage of the index lifecycle.
 */
@RunWith(Parameterized.class)
public class OrderByWithPartitionLevelKeysTest extends SAITester
{
    private static final String SCHEMA = "CREATE TABLE %s (pk1 int, pk2 int, ck int, st int static, v int, txt text, " +
                                         "vec vector<float, 2>, PRIMARY KEY ((pk1, pk2), ck))";

    @Parameterized.Parameter
    public int maxBruteForceRows;

    @Parameterized.Parameters(name = "maxBruteForceRows={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[]{ 0 }, new Object[]{ 1000 });
    }

    private final List<String> failures = new ArrayList<>();
    private final TreeSet<String> expected = new TreeSet<>();
    private String where;

    private int previousSSTableBruteForceRows;
    private double previousBruteForceExpenseFactor;
    private int previousMemtableBruteForceRows;

    @Before
    public void setBruteForce()
    {
        previousSSTableBruteForceRows = V2VectorIndexSearcher.GLOBAL_BRUTE_FORCE_ROWS;
        previousBruteForceExpenseFactor = V2VectorIndexSearcher.BRUTE_FORCE_EXPENSE_FACTOR;
        previousMemtableBruteForceRows = VectorMemtableIndex.GLOBAL_BRUTE_FORCE_ROWS;
        VectorTester.setMaxBruteForceRows(maxBruteForceRows);
    }

    @After
    public void resetBruteForce()
    {
        V2VectorIndexSearcher.GLOBAL_BRUTE_FORCE_ROWS = previousSSTableBruteForceRows;
        V2VectorIndexSearcher.BRUTE_FORCE_EXPENSE_FACTOR = previousBruteForceExpenseFactor;
        VectorMemtableIndex.GLOBAL_BRUTE_FORCE_ROWS = previousMemtableBruteForceRows;
    }

    @Test
    public void testPartitionKeyComponentPredicate()
    {
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(pk1) USING 'StorageAttachedIndex'");
        createOrderingIndexes();
        where = "pk1 = 3";

        // static and regular rows
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 1, 5)");
        for (int ck = 1; ck <= 4; ck++)
            insertRow(3, 1, ck, true);
        // regular rows only
        for (int ck = 1; ck <= 3; ck++)
            insertRow(3, 2, ck, true);
        // a static row only
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 3, 5)");
        // static and regular rows
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 4, 6)");
        for (int ck = 1; ck <= 2; ck++)
            insertRow(3, 4, ck, true);
        // other values of pk1
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (4, 1, 7)");
        for (int ck = 1; ck <= 2; ck++)
            insertRow(4, 1, ck, false);
        for (int pk2 = 10; pk2 < 40; pk2++)
            insertRow(5, pk2, 1, false);

        verifyStages(() -> {
            insertRow(3, 1, 5, true);
            insertRow(3, 4, 3, true);
            execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 2, 9)");
        });
    }

    @Test
    public void testStaticColumnPredicate()
    {
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex'");
        createOrderingIndexes();
        where = "st = 5";

        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 1, 5)");
        for (int ck = 1; ck <= 4; ck++)
            insertRow(3, 1, ck, true);
        // a static row only
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 3, 5)");
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 4, 5)");
        for (int ck = 1; ck <= 2; ck++)
            insertRow(3, 4, ck, true);
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (4, 1, 7)");
        for (int ck = 1; ck <= 2; ck++)
            insertRow(4, 1, ck, false);
        for (int pk2 = 10; pk2 < 40; pk2++)
            insertRow(5, pk2, 1, false);

        verifyStages(() -> {
            insertRow(3, 1, 5, true);
            insertRow(3, 4, 3, true);
        });
    }

    /**
     * Every partition has a static row, so the first row of the flushed sstable is a static row, and so is the lower
     * bound of its index segments: the keys of the first partition must all stay in the segment's range.
     */
    @Test
    public void testEveryPartitionHasStaticRow()
    {
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex'");
        createOrderingIndexes();
        where = "st = 5";

        int[][] partitions = { { 3, 1 }, { 3, 2 }, { 3, 3 }, { 4, 1 }, { 4, 2 } };
        for (int[] partition : partitions)
            execute("INSERT INTO %s (pk1, pk2, st) VALUES (?, ?, 5)", partition[0], partition[1]);
        for (int[] partition : partitions)
            for (int ck = 1; ck <= 5; ck++)
                insertRow(partition[0], partition[1], ck, true);

        verifyStages(() -> {});
    }

    private void createOrderingIndexes()
    {
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(txt) USING 'StorageAttachedIndex' WITH OPTIONS = { " +
                    "'index_analyzer':'{\"tokenizer\":{\"name\":\"standard\"}}' }");
    }

    private void verifyStages(Runnable moreWrites)
    {
        verifyAll("memtable");
        flush();
        verifyAll("flushed sstable");
        moreWrites.run();
        verifyAll("memtable and flushed sstable");
        flush();
        verifyAll("2 flushed sstables");
        compact();
        verifyAll("compacted");
        rebuildTableIndexes();
        verifyAll("rebuilt");

        if (!failures.isEmpty())
            fail(failures.size() + " failures:\n" + String.join("\n", failures));
    }

    private void insertRow(int pk1, int pk2, int ck, boolean matches)
    {
        execute("INSERT INTO %s (pk1, pk2, ck, v, txt, vec) VALUES (?, ?, ?, ?, 'apple pie', ?)",
                pk1, pk2, ck, ck * 10 + pk2, vector((float) pk2, (float) ck));
        if (matches)
            expected.add(pk1 + "," + pk2 + ',' + ck);
    }

    /**
     * Whether an ANN query with the given options searches exhaustively (filter-then-sort with brute force allowed), so
     * its results must be exact. Graph searches are approximate: a graph written by a compaction or a rebuild can hold
     * a node that no search reaches.
     */
    private boolean exhaustiveSearch(String options)
    {
        return maxBruteForceRows > 0 && options.contains("filter_then_sort");
    }

    /**
     * Checks the results of an approximate (graph) ANN search: every row it returns must be expected, and returned
     * once, and the rows it misses must be all the expected rows of a single vector, or none.
     */
    private void checkApproximate(String label, TreeSet<String> actual, int count)
    {
        TreeSet<String> wrong = new TreeSet<>(actual);
        wrong.removeAll(expected);
        TreeSet<String> missing = new TreeSet<>(expected);
        missing.removeAll(actual);
        // the vectors whose expected rows are missing, and whether all of them are
        Map<String, Integer> expectedPerVector = new TreeMap<>();
        Map<String, Integer> missingPerVector = new TreeMap<>();
        for (String row : expected)
            expectedPerVector.merge(vectorOf(row), 1, Integer::sum);
        for (String row : missing)
            missingPerVector.merge(vectorOf(row), 1, Integer::sum);
        boolean oneWholeVector = missingPerVector.isEmpty()
                                 || (missingPerVector.size() == 1 && missingPerVector.entrySet().iterator().next().getValue()
                                                                     .equals(expectedPerVector.get(missingPerVector.keySet().iterator().next())));
        if (!wrong.isEmpty() || count != actual.size() || !oneWholeVector)
            failures.add(label + "\n    returned " + count + " rows (" + actual.size() + " distinct), expected " + expected.size() +
                         "; wrong: " + wrong + "; missing: " + missing + " (rows by vector: " + missingPerVector + ')');
    }

    private static String vectorOf(String row)
    {
        String[] key = row.split(",");
        return key[1] + ',' + key[2];
    }

    private void verifyAll(String stage)
    {
        for (String ordering : Arrays.asList("vec ANN OF [1, 0]", "v ASC", "v DESC", "txt BM25 OF 'apple'"))
        {
            for (String options : Arrays.asList("",
                                                " WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}",
                                                " WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'}"))
            {
                String query = "SELECT pk1, pk2, ck, v FROM %s WHERE " + where + " ORDER BY " + ordering + " LIMIT 100" + options;
                String label = String.format("[%s] %s", stage, query);
                try
                {
                    TreeSet<String> actual = new TreeSet<>();
                    int count = 0;
                    Integer previous = null;
                    for (UntypedResultSet.Row row : execute(query))
                    {
                        actual.add(row.getInt("pk1") + "," + row.getInt("pk2") + ',' + row.getInt("ck"));
                        count++;
                        int v = row.getInt("v");
                        if (previous != null && (ordering.equals("v ASC") ? previous > v : ordering.equals("v DESC") && previous < v))
                            failures.add(label + "\n    out of order: " + previous + " then " + v);
                        previous = v;
                    }
                    if (ordering.startsWith("vec ANN") && !exhaustiveSearch(options))
                        checkApproximate(label, actual, count);
                    else if (!actual.equals(expected) || count != expected.size())
                    {
                        TreeSet<String> missing = new TreeSet<>(expected);
                        missing.removeAll(actual);
                        failures.add(label + "\n    returned " + count + " rows, expected " + expected.size() + "; missing: " + missing);
                    }
                }
                catch (Throwable t)
                {
                    failures.add(label + "\n    threw " + t);
                }
            }
        }
    }
}
