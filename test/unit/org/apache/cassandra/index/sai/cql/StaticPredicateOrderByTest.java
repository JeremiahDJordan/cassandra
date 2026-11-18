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

import java.util.Arrays;
import java.util.Map;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.index.sai.disk.format.Version;

/**
 * Ordering by an index on a non-static column (ANN on a regular vector, generic {@code ORDER BY}, BM25) with a
 * predicate on an indexed static column. The index of the static column selects static rows, so when the ordering
 * index sorts the keys it selects (filter-then-sort), they have to be turned into the keys of the rows of their
 * partitions.
 */
public class StaticPredicateOrderByTest extends VectorTester.Versioned
{
    private static final String[] HYBRID_PLANS = { "",
                                                   "WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}",
                                                   "WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'}" };

    private void assertOrdered(String query, Object[]... expected)
    {
        for (String plan : HYBRID_PLANS)
        {
            for (int limit : new int[]{ 1, 2, 3, 100 })
            {
                Object[][] rows = Arrays.copyOf(expected, Math.min(limit, expected.length));
                assertRows(execute(String.format("%s LIMIT %d %s", query, limit, plan)), rows);
            }
        }
    }

    /**
     * The static values and the rows are in different sstables.
     */
    @Test
    public void testRegularVector() throws Throwable
    {
        testRegularVector(true);
    }

    /**
     * The static values and the rows are in the same sstable, so its segment of the vector index starts at the static
     * row of its first partition, partition 1.
     */
    @Test
    public void testRegularVectorWithStaticRowsAndRowsInOneSSTable() throws Throwable
    {
        testRegularVector(false);
    }

    /**
     * Partition 0 has s = 0, partitions 1 and 2 have s = 1 and s = 2. By the distance of v to [1, 0], the rows are
     * (0, 2), (1, 2), (2, 1), (1, 1), (0, 1) and (2, 2).
     */
    private void testRegularVector(boolean flushStaticValues) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, r int, s int static, v vector<float, 2>, PRIMARY KEY(pk, ck))");
        createVectorIndex("%s", "v", Map.of("similarity_function", "euclidean"));
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();

        for (int pk = 0; pk < 3; pk++)
            execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, pk);
        if (flushStaticValues)
            flush();
        double[][] data = { { 0, 1, 0.5 }, { 0, 2, 0.0 },
                            { 1, 1, 0.3 }, { 1, 2, 0.1 },
                            { 2, 1, 0.2 }, { 2, 2, 0.6 } };
        for (double[] d : data)
            execute("INSERT INTO %s (pk, ck, r, v) VALUES (?, ?, ?, [1, " + d[2] + "])", (int) d[0], (int) d[1], (int) d[1]);

        runThenFlushThenCompact(() -> {
            String ann = " ORDER BY v ANN OF [1, 0]";
            assertOrdered("SELECT pk, ck FROM %s WHERE s >= 1" + ann, row(1, 2), row(2, 1), row(1, 1), row(2, 2));
            assertOrdered("SELECT pk, ck FROM %s WHERE s = 2" + ann, row(2, 1), row(2, 2));
            assertOrdered("SELECT pk, ck FROM %s WHERE s >= 1 AND r = 2" + ann, row(1, 2), row(2, 2));
            assertOrdered("SELECT pk, ck FROM %s WHERE pk = 1 AND s = 1" + ann, row(1, 2), row(1, 1));
            assertOrdered("SELECT pk, ck FROM %s WHERE s = 7" + ann);
            assertRows(execute("SELECT pk, ck FROM %s WHERE s >= 1" + ann + " PER PARTITION LIMIT 1 LIMIT 10"), row(1, 2), row(2, 1));
        });

        // a partition whose rows are deleted, and one whose static value no longer matches
        execute("DELETE FROM %s WHERE pk = 1 AND ck >= 0");
        execute("UPDATE %s SET s = 0 WHERE pk = 2");
        execute("UPDATE %s SET s = 5 WHERE pk = 0");
        runThenFlushThenCompact(() -> assertOrdered("SELECT pk, ck FROM %s WHERE s >= 1 ORDER BY v ANN OF [1, 0]",
                                                    row(0, 2), row(0, 1)));
    }

    /**
     * A predicate on a regular column, sorted by another index whose sstable segment starts at the static row of the
     * first partition, which compares equal to every row of that partition: all those rows stay in range.
     */
    @Test
    public void testRegularPredicate() throws Throwable
    {
        // values of (0,1) (0,2) (1,1) (1,2) (2,1) (2,2): the rows with q = 1 are, in order, (1,2), (2,2), (1,1), (2,1)
        String[] vectors = { "[1, 0.5]", "[1, 0.0]", "[1, 0.3]", "[1, 0.1]", "[1, 0.4]", "[1, 0.2]" };
        String[] ints = { "50", "0", "30", "10", "40", "20" };
        for (boolean[] statics : new boolean[][]{ { true, true }, { true, false }, { false, false } })
        {
            testRegularPredicate(statics[0], statics[1], "vector<float, 2>", "ORDER BY v ANN OF [1, 0]", vectors);
            testRegularPredicate(statics[0], statics[1], "int", "ORDER BY v ASC", ints);
        }
    }

    private void testRegularPredicate(boolean withStaticColumn, boolean withStaticValues, String type, String orderBy,
                                      String[] values) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, r int, q int, " + (withStaticColumn ? "s int static, " : "") +
                    "v " + type + ", PRIMARY KEY(pk, ck))");
        if (type.startsWith("vector"))
            createVectorIndex("%s", "v", Map.of("similarity_function", "euclidean"));
        else
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(q) USING 'StorageAttachedIndex'");
        disableCompaction();

        // the static values and the rows in the same memtable
        if (withStaticValues)
            for (int pk = 0; pk < 3; pk++)
                execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, pk);
        int[][] keys = { { 0, 1 }, { 0, 2 }, { 1, 1 }, { 1, 2 }, { 2, 1 }, { 2, 2 } };
        for (int i = 0; i < keys.length; i++)
            execute("INSERT INTO %s (pk, ck, r, q, v) VALUES (?, ?, ?, ?, " + values[i] + ')',
                    keys[i][0], keys[i][1], keys[i][1], keys[i][0] >= 1 ? 1 : 0);

        runThenFlushThenCompact(() -> {
            assertOrdered("SELECT pk, ck FROM %s WHERE q = 1 " + orderBy, row(1, 2), row(2, 2), row(1, 1), row(2, 1));
            assertOrdered("SELECT pk, ck FROM %s WHERE q = 1 AND r = 2 " + orderBy, row(1, 2), row(2, 2));
            for (String plan : HYBRID_PLANS)
                assertRows(execute("SELECT pk, ck FROM %s WHERE q = 1 " + orderBy + " PER PARTITION LIMIT 1 LIMIT 100 " + plan),
                           row(1, 2), row(2, 2));
        });
    }

    /**
     * A selective static predicate on a larger table, without hints, so the optimizer sorts the selected rows with
     * the vector index (filter-then-sort).
     */
    @Test
    public void testSelectiveStaticPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s int static, v vector<float, 2>, PRIMARY KEY(pk, ck))");
        createVectorIndex("%s", "v", Map.of("similarity_function", "euclidean"));
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");

        for (int pk = 0; pk < 200; pk++)
        {
            execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, pk);
            for (int ck = 0; ck < 4; ck++)
            {
                // The rows of partitions 150 and 151 are the closest to [1, 0], and the last row of a partition is
                // the closest of the partition. The other partitions are far, so the expected rows don't depend on
                // the recall of the ANN search, whatever plan the optimizer picks.
                String v = pk == 150 || pk == 151
                           ? "[1, " + ((4 - ck) / 10.0 + (pk - 150) / 20.0) + ']'
                           : "[-1, " + (1 + pk / 100.0 + ck / 1000.0) + ']';
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, " + v + ')', pk, ck);
            }
        }

        // Not compacted: ANN on a compacted graph of this data can miss the rows of partitions 150 and 151
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk, ck FROM %s WHERE s = 150 ORDER BY v ANN OF [1, 0] LIMIT 3"), row(150, 3), row(150, 2), row(150, 1));
            assertRows(execute("SELECT pk, ck FROM %s WHERE s >= 150 AND s <= 151 ORDER BY v ANN OF [1, 0] LIMIT 3"), row(150, 3), row(151, 3), row(150, 2));
        });
    }

    @Test
    public void testGenericOrderBy() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, r int, s int static, val int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();

        for (int pk = 0; pk < 3; pk++)
            execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, pk);
        flush();
        int[][] data = { { 0, 1, 50 }, { 0, 2, 0 },
                         { 1, 1, 30 }, { 1, 2, 10 },
                         { 2, 1, 20 }, { 2, 2, 60 } };
        for (int[] d : data)
            execute("INSERT INTO %s (pk, ck, r, val) VALUES (?, ?, ?, ?)", d[0], d[1], d[1], d[2]);

        runThenFlushThenCompact(() -> {
            assertOrdered("SELECT pk, ck FROM %s WHERE s >= 1 ORDER BY val ASC", row(1, 2), row(2, 1), row(1, 1), row(2, 2));
            assertOrdered("SELECT pk, ck FROM %s WHERE s >= 1 ORDER BY val DESC", row(2, 2), row(1, 1), row(2, 1), row(1, 2));
            assertOrdered("SELECT pk, ck FROM %s WHERE s >= 1 AND r = 2 ORDER BY val ASC", row(1, 2), row(2, 2));
        });
    }

    @Test
    public void testBM25() throws Throwable
    {
        Assume.assumeTrue(version.onOrAfter(Version.BM25_EARLIEST));

        createTable("CREATE TABLE %s (pk int, ck int, s int static, t text, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();

        for (int pk = 0; pk < 3; pk++)
            execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, pk);
        flush();
        // { pk, ck, occurrences of the term in a document of 10 words }: the score grows with the occurrences
        int[][] data = { { 0, 1, 1 }, { 0, 2, 9 },
                         { 1, 1, 3 }, { 1, 2, 7 },
                         { 2, 1, 5 }, { 2, 2, 2 } };
        for (int[] d : data)
        {
            StringBuilder text = new StringBuilder();
            for (int w = 0; w < 10; w++)
                text.append(w < d[2] ? "apple " : "pear ");
            execute("INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?)", d[0], d[1], text.toString());
        }

        runThenFlushThenCompact(() -> assertOrdered("SELECT pk, ck FROM %s WHERE s >= 1 ORDER BY t BM25 OF 'apple'",
                                                    row(1, 2), row(2, 1), row(1, 1), row(2, 2)));
    }
}
