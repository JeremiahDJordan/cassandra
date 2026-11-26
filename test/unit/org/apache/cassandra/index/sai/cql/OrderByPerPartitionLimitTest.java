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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Index ordering (ANN, BM25 and generic {@code ORDER BY ... ASC/DESC}) with {@code PER PARTITION LIMIT}: the results
 * have the best rows of each partition, in the order of the query, so the best row of a partition must be kept even
 * when it isn't its first row in clustering order.
 * <p>
 * Each test gives the sort value of each row (lower is better), and the expected results are computed from them.
 */
public class OrderByPerPartitionLimitTest extends VectorTester.Versioned
{
    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    /** A row with its sort value: lower is better. */
    private static class SortedRow
    {
        final int pk, ck;
        final double sort;

        SortedRow(int pk, int ck, double sort)
        {
            this.pk = pk;
            this.ck = ck;
            this.sort = sort;
        }
    }

    /**
     * @return the rows that the query should return: by sort value, at most {@code perPartitionLimit} per partition
     */
    private static Object[][] expected(List<SortedRow> rows, int perPartitionLimit, int limit)
    {
        List<SortedRow> sorted = new ArrayList<>(rows);
        sorted.sort(Comparator.comparingDouble(r -> r.sort));
        Map<Integer, Integer> taken = new HashMap<>();
        List<Object[]> result = new ArrayList<>();
        for (SortedRow r : sorted)
        {
            if (result.size() >= limit)
                break;
            if (taken.merge(r.pk, 1, Integer::sum) > perPartitionLimit)
                continue;
            result.add(row(r.pk, r.ck));
        }
        return result.toArray(new Object[0][]);
    }

    private void assertOrdered(String where, String orderBy, List<SortedRow> rows, boolean paging) throws Throwable
    {
        for (int limit : new int[]{ 1, 2, 3, 4, 5, 7, 100 })
        {
            for (int perPartitionLimit : new int[]{ 1, 2, 3 })
            {
                String query = String.format("SELECT pk, ck FROM %%s %s %s PER PARTITION LIMIT %d LIMIT %d",
                                             where, orderBy, perPartitionLimit, limit);
                Object[][] expected = expected(rows, perPartitionLimit, limit);
                assertRows(execute(query), expected);
                if (paging && limit <= 5)
                    assertRowsNet(executeNetWithPaging(query, 2), expected);
            }

            String query = String.format("SELECT pk, ck FROM %%s %s %s LIMIT %d", where, orderBy, limit);
            assertRows(execute(query), expected(rows, Integer.MAX_VALUE, limit));
        }
    }

    @Test
    public void testRegularVector() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, r int, v vector<float, 2>, PRIMARY KEY(pk, ck))");
        createVectorIndex("%s", "v", Map.of("similarity_function", "euclidean"));
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");

        // The distance to [1, 0] is the second component. The best row of partitions 0 and 1 is their last one.
        double[][] data = { { 0, 1, 0.5 }, { 0, 2, 0.4 }, { 0, 3, 0.0 },
                            { 1, 1, 0.3 }, { 1, 2, 0.1 },
                            { 2, 1, 0.2 } };
        List<SortedRow> rows = new ArrayList<>();
        for (double[] d : data)
        {
            execute("INSERT INTO %s (pk, ck, r, v) VALUES (?, ?, ?, [1, " + d[2] + "])", (int) d[0], (int) d[1], (int) d[1]);
            rows.add(new SortedRow((int) d[0], (int) d[1], d[2]));
        }
        List<SortedRow> rowsWithR2 = rows.stream().filter(r -> r.ck >= 2).collect(Collectors.toList());

        runThenFlushThenCompact(() -> {
            assertOrdered("", "ORDER BY v ANN OF [1, 0]", rows, true);
            assertOrdered("WHERE r >= 2", "ORDER BY v ANN OF [1, 0]", rowsWithR2, false);
        });
    }

    @Test
    public void testRegularVectorRandom() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v vector<float, 2>, PRIMARY KEY(pk, ck))");
        createVectorIndex("%s", "v", Map.of("similarity_function", "euclidean"));
        disableCompaction();

        // distinct distances, so there are no ties
        List<Double> distances = new ArrayList<>();
        for (int i = 0; i < 40; i++)
            distances.add(i / 100.0);
        Collections.shuffle(distances, new Random(42));

        List<SortedRow> rows = new ArrayList<>();
        int i = 0;
        for (int pk = 0; pk < 8; pk++)
        {
            for (int ck = 0; ck < 5; ck++)
            {
                double distance = distances.get(i++);
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, [1, " + distance + "])", pk, ck);
                rows.add(new SortedRow(pk, ck, distance));
            }
            if (pk == 2 || pk == 5)
                flush();
        }

        runThenFlushThenCompact(() -> assertOrdered("", "ORDER BY v ANN OF [1, 0]", rows, false));
    }

    @Test
    public void testGenericOrderBy() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, r int, val int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");

        // The best row in ascending order of partitions 0 and 1 is their last one, and their first one in descending
        // order; the other way round for partition 3.
        int[][] data = { { 0, 1, 50 }, { 0, 2, 40 }, { 0, 3, 0 },
                         { 1, 1, 30 }, { 1, 2, 10 },
                         { 2, 1, 20 },
                         { 3, 1, 5 }, { 3, 2, 45 } };
        List<SortedRow> ascending = new ArrayList<>();
        List<SortedRow> descending = new ArrayList<>();
        for (int[] d : data)
        {
            execute("INSERT INTO %s (pk, ck, r, val) VALUES (?, ?, ?, ?)", d[0], d[1], d[1], d[2]);
            ascending.add(new SortedRow(d[0], d[1], d[2]));
            descending.add(new SortedRow(d[0], d[1], -d[2]));
        }
        List<SortedRow> ascendingWithR2 = ascending.stream().filter(r -> r.ck >= 2).collect(Collectors.toList());

        runThenFlushThenCompact(() -> {
            assertOrdered("", "ORDER BY val ASC", ascending, true);
            assertOrdered("", "ORDER BY val DESC", descending, true);
            assertOrdered("WHERE r >= 2", "ORDER BY val ASC", ascendingWithR2, false);
        });
    }

    @Test
    public void testBM25() throws Throwable
    {
        Assume.assumeTrue(version.onOrAfter(Version.BM25_EARLIEST));

        createTable("CREATE TABLE %s (pk int, ck int, t text, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");

        // { pk, ck, occurrences of the term in a document of 10 words }: the score grows with the occurrences
        int[][] data = { { 0, 1, 1 }, { 0, 2, 2 }, { 0, 3, 9 },
                         { 1, 1, 3 }, { 1, 2, 7 },
                         { 2, 1, 5 } };
        List<SortedRow> rows = new ArrayList<>();
        for (int[] d : data)
        {
            execute("INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?)", d[0], d[1], document(d[2]));
            rows.add(new SortedRow(d[0], d[1], -d[2]));
        }

        runThenFlushThenCompact(() -> assertOrdered("", "ORDER BY t BM25 OF 'apple'", rows, true));
    }

    /**
     * The rows of a partition beyond the per-partition limit aren't read: once a partition has enough rows, the keys
     * of its other rows are skipped.
     */
    @Test
    public void testRowsBeyondPerPartitionLimitAreNotRead()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v vector<float, 2>, PRIMARY KEY(pk, ck))");
        createVectorIndex("%s", "v", Map.of("similarity_function", "euclidean"));

        // the rows of partition p have distances p / 10 + c / 100, so the first rows of the partitions are the best
        int partitions = 5, rowsPerPartition = 10;
        for (int pk = 0; pk < partitions; pk++)
            for (int ck = 0; ck < rowsPerPartition; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, [1, " + (pk / 10.0 + ck / 100.0) + "])", pk, ck);

        for (int i = 0; i < 2; i++) // memtable, then sstable
        {
            QueryContext.Snapshot snapshot = search("SELECT pk, ck FROM %s ORDER BY v ANN OF [1, 0] PER PARTITION LIMIT 1 LIMIT 100");
            assertEquals(partitions, snapshot.keysFetched);
            assertRows(execute("SELECT pk, ck FROM %s ORDER BY v ANN OF [1, 0] PER PARTITION LIMIT 1 LIMIT 100"),
                       row(0, 0), row(1, 0), row(2, 0), row(3, 0), row(4, 0));

            snapshot = search("SELECT pk, ck FROM %s ORDER BY v ANN OF [1, 0] PER PARTITION LIMIT 2 LIMIT 100");
            assertEquals(2 * partitions, snapshot.keysFetched);
            flush();
        }
    }

    /**
     * The keys of the partitions that already have enough rows are skipped without being read, so the loop that skips
     * them has to check the timeout itself: with a few wide partitions, it goes through every key of the index.
     */
    @Test
    public void testSkippedKeysCheckTheTimeout() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v vector<float, 2>, PRIMARY KEY(pk, ck))");
        createVectorIndex("%s", "v", Map.of("similarity_function", "euclidean"));

        int partitions = 3, rowsPerPartition = 100;
        for (int pk = 0; pk < partitions; pk++)
            for (int ck = 0; ck < rowsPerPartition; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, [1, " + (pk / 10.0 + ck / 1000.0) + "])", pk, ck);

        Injections.Counter checkpoints = Injections.newCounter("query_context_checkpoints")
                                                   .add(InvokePointBuilder.newInvokePoint()
                                                                          .onClass(QueryContext.class)
                                                                          .onMethod("checkpoint"))
                                                   .build();
        Injections.inject(checkpoints);
        try
        {
            for (int i = 0; i < 2; i++) // memtable, then sstable
            {
                // fewer partitions than LIMIT, so all the keys of the index are gone through, and all but 3 skipped
                checkpoints.reset();
                QueryContext.Snapshot snapshot = search("SELECT pk, ck FROM %s ORDER BY v ANN OF [1, 0] PER PARTITION LIMIT 1 LIMIT 10");
                assertEquals(partitions, snapshot.keysFetched);
                assertThat(checkpoints.get()).isGreaterThanOrEqualTo(partitions * rowsPerPartition - partitions);
                flush();
            }
        }
        finally
        {
            checkpoints.disable();
        }
    }

    /**
     * Keys whose rows were deleted or changed after a flush, so the index of the sstable has keys that are no longer
     * valid: they mustn't take a slot of their partition. The best row of partition 0 is deleted, and the best row of
     * partition 1 gets a worse value.
     */
    @Test
    public void testStaleKeys() throws Throwable
    {
        // regular vector
        createTable("CREATE TABLE %s (pk int, ck int, v vector<float, 2>, PRIMARY KEY(pk, ck))");
        createVectorIndex("%s", "v", Map.of("similarity_function", "euclidean"));
        disableCompaction();
        double[][] vectors = { { 0, 1, 0.5 }, { 0, 2, 0.4 }, { 0, 3, 0.0 },
                               { 1, 1, 0.3 }, { 1, 2, 0.1 },
                               { 2, 1, 0.2 } };
        List<SortedRow> rows = new ArrayList<>();
        for (double[] d : vectors)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, [1, " + d[2] + "])", (int) d[0], (int) d[1]);
            rows.add(new SortedRow((int) d[0], (int) d[1], d[2]));
        }
        flush();
        execute("DELETE FROM %s WHERE pk = 0 AND ck = 3");
        execute("UPDATE %s SET v = [1, 0.9] WHERE pk = 1 AND ck = 2");
        List<SortedRow> updated = staleKeysRows(rows, 0.9);
        runThenFlushThenCompact(() -> assertOrdered("", "ORDER BY v ANN OF [1, 0]", updated, false));

        // generic ORDER BY
        createTable("CREATE TABLE %s (pk int, ck int, val int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction();
        rows.clear();
        for (double[] d : vectors)
        {
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", (int) d[0], (int) d[1], (int) (d[2] * 100));
            rows.add(new SortedRow((int) d[0], (int) d[1], (int) (d[2] * 100)));
        }
        flush();
        execute("DELETE FROM %s WHERE pk = 0 AND ck = 3");
        execute("UPDATE %s SET val = 90 WHERE pk = 1 AND ck = 2");
        List<SortedRow> updatedValues = staleKeysRows(rows, 90);
        runThenFlushThenCompact(() -> assertOrdered("", "ORDER BY val ASC", updatedValues, false));

        if (!version.onOrAfter(Version.BM25_EARLIEST))
            return;

        // BM25, where a lower sort value is a higher number of occurrences of the term in a document of 10 words
        createTable("CREATE TABLE %s (pk int, ck int, t text, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        disableCompaction();
        rows.clear();
        for (double[] d : vectors)
        {
            int occurrences = 10 - (int) (d[2] * 10);
            execute("INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?)", (int) d[0], (int) d[1], document(occurrences));
            rows.add(new SortedRow((int) d[0], (int) d[1], -occurrences));
        }
        flush();
        execute("DELETE FROM %s WHERE pk = 0 AND ck = 3");
        execute("UPDATE %s SET t = ? WHERE pk = 1 AND ck = 2", document(1));
        List<SortedRow> updatedDocuments = staleKeysRows(rows, -1);
        runThenFlushThenCompact(() -> assertOrdered("", "ORDER BY t BM25 OF 'apple'", updatedDocuments, false));
    }

    /**
     * @return the rows without (0, 3), and with the given sort value for (1, 2)
     */
    private static List<SortedRow> staleKeysRows(List<SortedRow> rows, double newSortOf12)
    {
        List<SortedRow> updated = new ArrayList<>();
        for (SortedRow r : rows)
        {
            if (r.pk == 0 && r.ck == 3)
                continue;
            updated.add(r.pk == 1 && r.ck == 2 ? new SortedRow(1, 2, newSortOf12) : r);
        }
        return updated;
    }

    private static String document(int occurrences)
    {
        StringBuilder text = new StringBuilder();
        for (int w = 0; w < 10; w++)
            text.append(w < occurrences ? "apple " : "pear ");
        return text.toString();
    }

    /**
     * A vector on a partition key column: a partition with only a static row has the vector of its key, so it's a
     * match on its own, as a row with a null clustering. Rows of a partition are tied, and come in clustering order.
     */
    @Test
    public void testPartitionKeyVectorWithStaticOnlyPartition() throws Throwable
    {
        Assume.assumeTrue(version.onOrAfter(Version.JVECTOR_EARLIEST));

        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, PRIMARY KEY((k1, k2), c))");
        createVectorIndex("%s", "k2", Map.of("similarity_function", "euclidean"));
        execute("INSERT INTO %s (k1, k2, c) VALUES (1, [1, 0], 1)");
        execute("INSERT INTO %s (k1, k2, c) VALUES (1, [1, 0], 2)");
        execute("INSERT INTO %s (k1, k2, c) VALUES (2, [1, 0.1], 1)");
        execute("INSERT INTO %s (k1, k2, s) VALUES (3, [1, 0.2], 3)");
        execute("INSERT INTO %s (k1, k2, c) VALUES (4, [1, 0.3], 1)");
        execute("INSERT INTO %s (k1, k2, c) VALUES (4, [1, 0.3], 2)");

        // In the memtable and flushed
        beforeAndAfterFlush(() -> {
            String select = "SELECT k1, c FROM %s ORDER BY k2 ANN OF [1, 0] ";
            assertRows(execute(select + "LIMIT 10"), row(1, 1), row(1, 2), row(2, 1), row(3, null), row(4, 1), row(4, 2));
            assertRows(execute(select + "PER PARTITION LIMIT 1 LIMIT 10"), row(1, 1), row(2, 1), row(3, null), row(4, 1));
            assertRows(execute(select + "PER PARTITION LIMIT 1 LIMIT 3"), row(1, 1), row(2, 1), row(3, null));
            assertRows(execute(select + "PER PARTITION LIMIT 2 LIMIT 4"), row(1, 1), row(1, 2), row(2, 1), row(3, null));
        });
    }

    private QueryContext.Snapshot search(String query)
    {
        ReadCommand command = parseReadCommand(query);
        StorageAttachedIndexQueryPlan plan = (StorageAttachedIndexQueryPlan) command.indexQueryPlan();
        StorageAttachedIndexSearcher searcher = plan.searcherFor(command);
        try (ReadExecutionController executionController = command.executionController();
             UnfilteredPartitionIterator partitions = searcher.search(executionController))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        partition.next();
                }
            }
        }
        return searcher.queryContext().snapshot();
    }

    @Test
    public void testStaticAndRegularVectors() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, r int, sv vector<float, 2> static, v vector<float, 2>, PRIMARY KEY(pk, ck))");
        createVectorIndex("%s", "sv", Map.of("similarity_function", "euclidean"));
        createVectorIndex("%s", "v", Map.of("similarity_function", "euclidean"));
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");

        // partitions by static vector: 2, 0, 3 (only a static row), 1; partition 4 has no static vector
        double[] staticDistance = { 0.2, 0.4, 0.1, 0.3 };
        double[][] data = { { 0, 1, 0.5 }, { 0, 2, 0.45 }, { 0, 3, 0.0 },
                            { 1, 1, 0.3 }, { 1, 2, 0.1 },
                            { 2, 1, 0.2 }, { 2, 2, 0.6 },
                            { 4, 1, 0.05 }, { 4, 2, 0.7 } };
        for (int pk = 0; pk < 4; pk++)
            execute("INSERT INTO %s (pk, sv) VALUES (?, [1, " + staticDistance[pk] + "])", pk);
        List<SortedRow> byRegular = new ArrayList<>();
        List<SortedRow> byStatic = new ArrayList<>();
        for (double[] d : data)
        {
            int pk = (int) d[0], ck = (int) d[1];
            execute("INSERT INTO %s (pk, ck, r, v) VALUES (?, ?, ?, [1, " + d[2] + "])", pk, ck, ck);
            byRegular.add(new SortedRow(pk, ck, d[2]));
            if (pk < 4)
                byStatic.add(new SortedRow(pk, ck, staticDistance[pk] + ck / 1000.0)); // tied rows by clustering
        }
        List<SortedRow> byStaticWithR2 = byStatic.stream().filter(r -> r.ck >= 2).collect(Collectors.toList());

        runThenFlushThenCompact(() -> {
            assertOrdered("", "ORDER BY v ANN OF [1, 0]", byRegular, true);
            assertOrdered("WHERE r >= 2", "ORDER BY sv ANN OF [1, 0]", byStaticWithR2, false);

            // the partition with only a static row comes third, as a row with a null clustering
            assertRows(execute("SELECT pk, ck FROM %s ORDER BY sv ANN OF [1, 0] LIMIT 7"),
                       row(2, 1), row(2, 2), row(0, 1), row(0, 2), row(0, 3), row(3, null), row(1, 1));
            assertRows(execute("SELECT pk, ck FROM %s ORDER BY sv ANN OF [1, 0] PER PARTITION LIMIT 1 LIMIT 100"),
                       row(2, 1), row(0, 1), row(3, null), row(1, 1));
            assertRows(execute("SELECT pk, ck FROM %s ORDER BY sv ANN OF [1, 0] PER PARTITION LIMIT 2 LIMIT 5"),
                       row(2, 1), row(2, 2), row(0, 1), row(0, 2), row(3, null));
            assertRowsNet(executeNetWithPaging("SELECT pk, ck FROM %s ORDER BY sv ANN OF [1, 0] PER PARTITION LIMIT 2 LIMIT 5", 2),
                          row(2, 1), row(2, 2), row(0, 1), row(0, 2), row(3, null));
        });
    }
}
