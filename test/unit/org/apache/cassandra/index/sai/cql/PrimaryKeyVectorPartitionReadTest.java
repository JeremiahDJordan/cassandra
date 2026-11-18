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
import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * ANN ordering by a vector in a partition key column: the static row and the rows of a partition share its vector, and
 * the first key of the partition that the search gives reads the partition once, as many rows as the limits can take.
 * A wide partition with a static row must not be read whole for a few rows, whichever key comes first, and however
 * many sources have the key of its static row.
 */
public class PrimaryKeyVectorPartitionReadTest extends SAITester.Versioned
{
    private static final int WIDE = 300;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.stream(new Version[]{ Version.EC, Version.ED, Version.GA }).map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Test
    public void testWidePartitionWithStaticRow() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, PRIMARY KEY ((k1, k2), c))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        disableCompaction();
        // the nearest partition: its static row, written first, then many rows
        execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
        for (int c = 0; c < WIDE; c++)
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], ?, ?)", c, c);
        for (int k = 2; k <= 21; k++)
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (?, ?, 0, 0)", k, vector(1f, k / 100f));

        for (String stage : new String[]{ "memtable", "sstable", "static row in the memtable and the sstable" })
        {
            if (stage.equals("sstable"))
                flush();
            if (stage.startsWith("static row in"))
                execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 2)");

            check(stage, "ORDER BY k2 ANN OF [1, 0] LIMIT 1", 1, row(1, 0));
            check(stage, "ORDER BY k2 ANN OF [1, 0] LIMIT 3", 3, row(1, 0), row(1, 1), row(1, 2));
            check(stage, "WHERE k1 = 1 AND k2 = [1, 0] ORDER BY k2 ANN OF [1, 0] LIMIT 3", 3, row(1, 0), row(1, 1), row(1, 2));
            check(stage, "ORDER BY k2 ANN OF [1, 0] PER PARTITION LIMIT 2 LIMIT 4", 4, row(1, 0), row(1, 1), row(2, 0), row(3, 0));
        }
    }

    /**
     * Filter-then-sort: the keys of the rows that the predicate selects are given to the ordering, and the partition is
     * read for those rows only, not scanned from its start until enough rows satisfy the predicate.
     */
    @Test
    public void testWidePartitionWithFewMatchingRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, PRIMARY KEY ((k1, k2), c))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();
        execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
        // r = c, but the last row, which alone has r = -1, and the rows from c = 100 on, which have r = -2
        for (int c = 0; c < WIDE; c++)
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], ?, ?)", c, c == WIDE - 1 ? -1 : c >= 100 ? -2 : c);
        for (int k = 2; k <= 21; k++)
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (?, ?, 0, -1)", k, vector(1f, k / 100f));

        String fts = " WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}";
        for (String stage : new String[]{ "memtable", "sstable" })
        {
            if (stage.equals("sstable"))
                flush();

            // one row of the wide partition matches, the last one
            check(stage, "WHERE r = -1 ORDER BY k2 ANN OF [1, 0] LIMIT 3" + fts, 3, row(1, WIDE - 1), row(2, 0), row(3, 0));
            check(stage, "WHERE k1 = 1 AND k2 = [1, 0] AND r = -1 ORDER BY k2 ANN OF [1, 0] LIMIT 3" + fts, 3, row(1, WIDE - 1));
            // a clustering restriction leaves out some of the rows that the predicate selects
            check(stage, "WHERE k1 = 1 AND k2 = [1, 0] AND c > 280 AND r = -2 ORDER BY k2 ANN OF [1, 0] LIMIT 3" + fts, 3,
                  row(1, 281), row(1, 282), row(1, 283));
            // more matching rows than are read by their clusterings: the partition is read from its start
            check(stage, "WHERE r = -2 ORDER BY k2 ANN OF [1, 0] LIMIT 2" + fts, 2, WIDE, row(1, 100), row(1, 101));
            // the predicate on the static column gives a key for the whole partition
            check(stage, "WHERE s = 1 ORDER BY k2 ANN OF [1, 0] LIMIT 2" + fts, 2, row(1, 0), row(1, 1));
        }
    }

    /**
     * Filter-then-sort, a partition with as many keys as are looked at for its candidate rows, or one more, most of them
     * outside the clustering restriction of the query: the first is read by the clusterings of its candidates, the
     * second whole, within that restriction.
     */
    @Test
    public void testPartitionWithManyKeysOutsideClusteringRestriction() throws Throwable
    {
        Injections.Counter readsByClusterings = Injections.newCounter("reads_by_clusterings")
                                                          .add(InvokePointBuilder.newInvokePoint()
                                                                                 .onClass(QueryController.class)
                                                                                 .onMethod("getPartition", "DecoratedKey", "NavigableSet",
                                                                                           "ColumnFamilyStore$ViewFragment", "ReadExecutionController")
                                                                                 .atEntry())
                                                          .build();
        Injections.inject(readsByClusterings);
        try
        {
            for (int keys : new int[]{ QueryController.MAX_CANDIDATE_KEYS_SCANNED, QueryController.MAX_CANDIDATE_KEYS_SCANNED + 1 })
            {
                createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, PRIMARY KEY ((k1, k2), c))");
                createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
                createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");
                disableCompaction();
                execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
                // every row matches, and gives a key
                for (int c = 0; c < keys; c++)
                    execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], ?, 1)", c);

                String fts = " WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}";
                long expectedReads = keys <= QueryController.MAX_CANDIDATE_KEYS_SCANNED ? 1 : 0;
                for (String stage : new String[]{ "memtable", "sstable" })
                {
                    if (stage.equals("sstable"))
                        flush();
                    String[] wheres = { "WHERE k1 = 1 AND k2 = [1, 0] AND c >= " + (keys - 10) + " AND r = 1 ",
                                        "WHERE k1 = 1 AND k2 = [1, 0] AND c < 10 AND r = 1 " };
                    Object[][][] expected = { { row(1, keys - 10), row(1, keys - 9), row(1, keys - 8) },
                                              { row(1, 0), row(1, 1), row(1, 2) } };
                    for (int i = 0; i < wheres.length; i++)
                    {
                        String query = wheres[i] + "ORDER BY k2 ANN OF [1, 0] LIMIT 3" + fts;
                        // the rows, and that the read is within the restriction (a few rows past the limit: the filter
                        // looks for the next matching row)
                        check(stage, query, 3, 10, expected[i]);
                        readsByClusterings.reset();
                        execute("SELECT k1, c FROM %s " + query);
                        assertEquals(query + ", " + stage + ", " + keys + " keys, on " + version + ": reads by clusterings",
                                     expectedReads, readsByClusterings.get());
                    }
                }
            }
        }
        finally
        {
            Injections.deleteAll();
        }
    }

    /**
     * Checks the rows of the query, then that the replica's searcher reads a few rows to give them, rather than the
     * whole wide partition: it is read lazily, so stop taking rows at the limit, as the replica does.
     */
    private void check(String stage, String where, int limit, Object[]... rows) throws Throwable
    {
        // a key of the wide partition gives at most `limit` of its rows; the other keys give a row each
        check(stage, where, limit, 2L * limit, rows);
    }

    private void check(String stage, String where, int limit, long maxFetched, Object[]... rows) throws Throwable
    {
        String query = "SELECT k1, c FROM %s " + where;
        String message = query + ", " + stage + ", on " + version;
        UntypedResultSet result = execute(query);
        assertRows(message, result, rows);

        ReadCommand command = parseReadCommand(query);
        StorageAttachedIndexSearcher searcher = ((StorageAttachedIndexQueryPlan) command.indexQueryPlan()).searcherFor(command);
        int taken = 0;
        try (ReadExecutionController executionController = command.executionController();
             UnfilteredPartitionIterator partitions = searcher.search(executionController))
        {
            while (taken < limit && partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (taken < limit && partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (unfiltered.isRow())
                            taken++;
                    }
                }
            }
        }
        long fetched = searcher.queryContext().snapshot().rowsFetched;
        assertTrue(message + ": " + fetched + " rows fetched for " + taken + " rows", fetched <= maxFetched);
    }
}
