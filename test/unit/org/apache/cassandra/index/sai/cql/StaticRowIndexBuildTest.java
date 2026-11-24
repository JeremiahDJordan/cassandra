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

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * An SAI index build (CREATE INDEX on existing sstables, or a rebuild) must number and index the rows of an sstable
 * as the flush or the compaction that wrote it did. In particular, it must skip the empty static row of a partition
 * without a static row, as flush and compaction do.
 */
public class StaticRowIndexBuildTest extends SAITester.Versioned
{
    private static final String SCHEMA = "CREATE TABLE %s (pk1 int, pk2 int, ck int, st int static, v int, PRIMARY KEY ((pk1, pk2), ck))";

    // the first partitions of the sstable in token order (see testRebuildOfOneIndexOnPerSSTableComponentsOfEarlierBuilds)
    private final List<Integer> firstPartitions = new ArrayList<>();

    /**
     * Creating an index on sstables that already have the per-sstable SAI components (written by the flush or the
     * compaction, for another index) gives an index that returns the rows that filtering returns.
     */
    @Test
    public void testCreateIndexOnSSTablesWithPerSSTableComponents()
    {
        testCreateIndexOnSSTablesWithPerSSTableComponents(false);
    }

    @Test
    public void testCreateIndexOnCompactedSSTablesWithPerSSTableComponents()
    {
        testCreateIndexOnSSTablesWithPerSSTableComponents(true);
    }

    private void testCreateIndexOnSSTablesWithPerSSTableComponents(boolean compact)
    {
        String reference = createTable(SCHEMA);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        for (String table : Arrays.asList(reference, currentTable()))
            writeData(table);
        flush();
        if (compact)
            compact();

        // these builds reuse the per-sstable components
        createIndex("CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");

        for (String restriction : Arrays.asList("st = 5", "st = 9", "ck = 1", "ck = 2", "v = 1", "st = 5 AND v = 1", "ck = 1 AND st = 5"))
        {
            String query = "SELECT * FROM %s WHERE " + restriction + " ALLOW FILTERING";
            assertEquals(restriction,
                         rows(executeFormattedQuery(String.format(query, KEYSPACE + '.' + reference))),
                         rows(execute(query)));
        }
    }

    /**
     * A rebuild and a compaction must write the same postings as the flush of the same data. Not for the
     * partition-aware version AA, whose flush writes a posting per indexed partition and value, including stale ones
     * for rows deleted in the memtable, and a build a posting per row.
     */
    @Test
    public void testFlushRebuildAndCompactionWriteTheSamePostings() throws Throwable
    {
        Assume.assumeTrue(version.onDiskFormat().indexFeatureSet().isRowAware());
        createTable(SCHEMA);
        disableCompaction();
        List<String> indexes = Arrays.asList(createIndex("CREATE CUSTOM INDEX ON %s(pk1) USING 'StorageAttachedIndex'"),
                                             createIndex("CREATE CUSTOM INDEX ON %s(pk2) USING 'StorageAttachedIndex'"),
                                             createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'"),
                                             createIndex("CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex'"),
                                             createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'"));
        writeData(currentTable());
        flush();

        List<List<String>> flushed = new ArrayList<>();
        for (String index : indexes)
            flushed.add(postings(index));

        rebuildTableIndexes();
        for (int i = 0; i < indexes.size(); i++)
            assertEquals("rebuild of " + indexes.get(i), flushed.get(i), postings(indexes.get(i)));

        compact();
        for (int i = 0; i < indexes.size(); i++)
            assertEquals("compaction of " + indexes.get(i), flushed.get(i), postings(indexes.get(i)));
    }

    /**
     * The builds of earlier releases numbered the empty static row of every partition without a static row in the
     * per-SSTable components they wrote (for the first index built on an sstable, or by a full rebuild). A build that
     * reuses such components must number the rows the same way, or the new index points to the wrong rows.
     */
    @Test
    public void testIndexBuiltOnPerSSTableComponentsOfEarlierBuilds()
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, v int, w int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        writeManyPartitions();
        flush();

        // the first index on the sstable writes its per-SSTable components, as an earlier release did
        StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = true;
        try
        {
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        }
        finally
        {
            StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = false;
        }

        // these reuse those components
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex'");
        verifyManyPartitions("built on the components of an earlier build");

        // a full rebuild rewrites the per-SSTable components, without numbering empty static rows
        rebuildTableIndexes();
        verifyManyPartitions("rebuilt");
    }

    /**
     * If a build that reuses per-SSTable components numbers the rows of the sstable differently anyway, it must fail
     * rather than publish an index that points to the wrong rows.
     */
    @Test
    public void testBuildFailsIfItNumbersRowsDifferentlyThanPerSSTableComponents() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, v int, w int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        writeManyPartitions();
        // the flush writes per-SSTable components that don't number empty static rows
        flush();

        // a build that numbers them (it only does so to match components that do)
        StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = true;
        String index;
        try
        {
            index = createIndexAsync("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
            waitForIndexBuilds(KEYSPACE, index);
        }
        finally
        {
            StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = false;
        }
        assertFalse(isIndexQueryable(KEYSPACE, index));
    }

    /**
     * A full rebuild of one index rewrites the per-SSTable components that the other indexes of the SSTable share: if
     * the replaced components numbered the empty static rows, the other indexes are rebuilt too, including an index
     * without values in the last rows of the SSTable (x), which would load without error.
     */
    @Test
    public void testRebuildOfOneIndexOnPerSSTableComponentsOfEarlierBuilds()
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, v int, w int, x int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        writeManyPartitions();
        // x only has values in the first rows of the sstable (its first partitions in token order), so after the
        // rebuild its segment would still load without error
        firstPartitions.clear();
        for (UntypedResultSet.Row row : execute("SELECT DISTINCT pk FROM %s"))
            if (firstPartitions.size() < 3)
                firstPartitions.add(row.getInt("pk"));
        for (int pk : firstPartitions)
            execute("UPDATE %s SET x = ? WHERE pk = ? AND ck = 0", pk, pk);
        flush();

        // the indexes were built by an earlier release
        StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = true;
        String v;
        try
        {
            v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        }
        finally
        {
            StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = false;
        }

        // nodetool rebuild_index of v only
        rebuildIndexes(v);
        verifyRebuildOfOneIndex();
        simulateNodeRestart();
        verifyRebuildOfOneIndex();
    }

    private void verifyRebuildOfOneIndex()
    {
        for (int pk = 0; pk < 20; pk++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = ?", pk * 10 + 1), row(pk, 1));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE w = ?", pk), row(pk, 0), row(pk, 1), row(pk, 2));
            if (firstPartitions.contains(pk))
                assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE x = ?", pk), row(pk, 0));
        }
    }

    /**
     * The check of how the existing per-SSTable components number the rows must not be fooled by rows they number but
     * the build can't read any more: here a row whose only cell belongs to a dropped column, in partitions without a
     * static row. On row-aware versions, the build can't number the rows as the components do, so it must fail, rather
     * than build an index that points to the wrong rows.
     */
    @Test
    public void testNumberingCheckWithRowsOfADroppedColumn()
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, v int, w int, d int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        writeDroppedColumnRows();
        // the flush writes per-SSTable components with the ck = 1 rows
        flush();
        schemaChange(String.format("ALTER TABLE %s.%s DROP d", KEYSPACE, currentTable()));

        String index = createIndexAsync("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        waitForIndexBuilds(KEYSPACE, index);
        if (version.onDiskFormat().indexFeatureSet().isRowAware())
        {
            assertFalse(isIndexQueryable(KEYSPACE, index));
        }
        else
        {
            // the partition-aware version AA numbers the rows the build can read, so the build succeeds
            verifyDroppedColumnRows();
        }
        for (int pk = 0; pk < 12; pk++)
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = ?", pk * 10 + 2), row(pk, 2));

        // the remedy: a rebuild of that index rewrites the per-SSTable components, and rebuilds v along with it
        rebuildIndexes(index);
        verifyDroppedColumnRows();
        simulateNodeRestart();
        verifyDroppedColumnRows();
    }

    /**
     * A rebuild of one index rewrites the per-SSTable components. If a column was dropped since they were written, the
     * rows whose only cells belonged to it are no longer numbered, so the other indexes must be rebuilt too.
     */
    @Test
    public void testRebuildOfOneIndexAfterDroppingAColumn()
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, v int, w int, d int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        writeDroppedColumnRows();
        flush();
        schemaChange(String.format("ALTER TABLE %s.%s DROP d", KEYSPACE, currentTable()));

        rebuildIndexes(v);
        verifyDroppedColumnRows();
        simulateNodeRestart();
        verifyDroppedColumnRows();
    }

    /**
     * A cell is dropped with its column if its timestamp is at most the time of the drop, so a column dropped at the
     * minimum timestamp of the sstable has cells the build can't read: a rebuild of one index must rebuild the others.
     */
    @Test
    public void testRebuildOfOneIndexAfterDroppingAColumnAtTheMinimumTimestamp()
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, v int, w int, d int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        for (int pk = 0; pk < 12; pk++)
        {
            execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, 0, ?, ?) USING TIMESTAMP 1000", pk, pk * 10, pk);
            execute("UPDATE %s USING TIMESTAMP 1000 SET d = 1 WHERE pk = ? AND ck = 1", pk);
            execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, 2, ?, ?) USING TIMESTAMP 1000", pk, pk * 10 + 2, pk);
        }
        flush();
        schemaChange(String.format("ALTER TABLE %s.%s DROP d USING TIMESTAMP 1000", KEYSPACE, currentTable()));

        rebuildIndexes(w);
        verifyDroppedColumnRows();
        simulateNodeRestart();
        verifyDroppedColumnRows();
    }

    /**
     * Once every static column is dropped, the build reads no static row at all, whatever the timestamps of their
     * cells (here newer than the drop), so a rebuild of one index must rebuild the others.
     */
    @Test
    public void testRebuildOfOneIndexAfterDroppingEveryStaticColumn()
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, st2 int static, v int, w int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        // timestamps in 2100, after the drop
        long timestamp = 4102444800000000L;
        for (int pk = 0; pk < 20; pk++)
        {
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, ?, ?, ?) USING TIMESTAMP ?", pk, ck, pk * 10 + ck, pk, timestamp);
            if (pk % 4 == 0)
                execute("INSERT INTO %s (pk, st, st2) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, pk, pk, timestamp);
        }
        flush();
        schemaChange(String.format("ALTER TABLE %s.%s DROP (st, st2)", KEYSPACE, currentTable()));

        rebuildIndexes(w);
        verifyEveryStaticColumnDropped();
        simulateNodeRestart();
        verifyEveryStaticColumnDropped();
    }

    private void verifyEveryStaticColumnDropped()
    {
        for (int pk = 0; pk < 20; pk++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = ?", pk * 10 + 1), row(pk, 1));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE w = ?", pk), row(pk, 0), row(pk, 1), row(pk, 2));
        }
    }

    private void writeDroppedColumnRows()
    {
        for (int pk = 0; pk < 12; pk++)
        {
            execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, 0, ?, ?)", pk, pk * 10, pk);
            execute("UPDATE %s SET d = 1 WHERE pk = ? AND ck = 1", pk);
            execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, 2, ?, ?)", pk, pk * 10 + 2, pk);
        }
    }

    private void verifyDroppedColumnRows()
    {
        for (int pk = 0; pk < 12; pk++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE w = ?", pk), row(pk, 0), row(pk, 2));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = ?", pk * 10 + 2), row(pk, 2));
        }
    }

    private void writeManyPartitions()
    {
        for (int pk = 0; pk < 20; pk++)
        {
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, ?, ?, ?)", pk, ck, pk * 10 + ck, pk);
            // most partitions have no static row
            if (pk % 4 == 0)
                execute("INSERT INTO %s (pk, st) VALUES (?, ?)", pk, pk);
        }
    }

    private void verifyManyPartitions(String stage)
    {
        for (int pk = 0; pk < 20; pk++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = ?", pk * 10 + 1), row(pk, 1));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE w = ?", pk), row(pk, 0), row(pk, 1), row(pk, 2));
            if (pk % 4 == 0)
                assertEquals(stage + " st = " + pk, 3, execute("SELECT pk, ck FROM %s WHERE st = ?", pk).size());
        }
    }

    private void writeData(String table)
    {
        String[] statements = {
            // static and regular rows
            "INSERT INTO %s (pk1, pk2, ck, st, v) VALUES (3, 1, 1, 5, 1)",
            "INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 1, 2, 2)",
            // a static row only
            "INSERT INTO %s (pk1, pk2, st) VALUES (3, 2, 5)",
            // regular rows only
            "INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 3, 1, 1)",
            "INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 3, 2, 2)",
            // a deleted regular row only
            "DELETE FROM %s WHERE pk1 = 4 AND pk2 = 1 AND ck = 1",
            // a partition deletion only
            "DELETE FROM %s WHERE pk1 = 4 AND pk2 = 2",
            // a partition deletion over a static row and a regular row
            "INSERT INTO %s (pk1, pk2, ck, st, v) VALUES (4, 3, 1, 9, 1)",
            "DELETE FROM %s WHERE pk1 = 4 AND pk2 = 3",
            // a range tombstone and a static row
            "INSERT INTO %s (pk1, pk2, ck, st, v) VALUES (4, 4, 1, 9, 1)",
            "DELETE FROM %s WHERE pk1 = 4 AND pk2 = 4 AND ck > 0",
            // a static cell tombstone
            "INSERT INTO %s (pk1, pk2, st) VALUES (4, 5, null)",
            // a regular row with only its primary key
            "INSERT INTO %s (pk1, pk2, ck) VALUES (4, 6, 1)",
        };
        for (String statement : statements)
            executeFormattedQuery(String.format(statement, KEYSPACE + '.' + table));
    }

    /**
     * Lists the postings of the sstable indexes of the given int column index, for terms 0 to 10, with their key kinds.
     */
    private List<String> postings(String indexName) throws Throwable
    {
        IndexContext context = getIndexContext(indexName);
        List<String> postings = new ArrayList<>();
        Collection<SSTableIndex> sstableIndexes = context.getView().getIndexes();
        postings.add("sstables=" + sstableIndexes.size());
        for (SSTableIndex index : sstableIndexes)
        {
            for (int term = 0; term <= 10; term++)
            {
                Expression expression = new Expression(context).add(Operator.EQ, Int32Type.instance.decompose(term));
                try (KeyRangeIterator keys = index.search(expression,
                                                          DataRange.allData(getCurrentColumnFamilyStore().getPartitioner()).keyRange(),
                                                          new QueryContext(), false))
                {
                    while (keys.hasNext())
                    {
                        PrimaryKey key = keys.next();
                        String partition = getCurrentColumnFamilyStore().metadata().partitionKeyType.getString(key.partitionKey().getKey());
                        String clustering = key.clustering() == null ? "none"
                                            : key.isStaticRow() ? "STATIC"
                                            : key.clustering().isEmpty() ? "empty"
                                            : "ck=" + Int32Type.instance.compose(key.clustering().bufferAt(0));
                        postings.add(term + "->(" + partition + "):" + clustering);
                    }
                }
            }
        }
        return postings;
    }

    private static List<String> rows(UntypedResultSet result)
    {
        List<String> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            rows.add(Arrays.asList(row.has("pk1") ? row.getInt("pk1") : null,
                                   row.has("pk2") ? row.getInt("pk2") : null,
                                   row.has("ck") ? row.getInt("ck") : null,
                                   row.has("st") ? row.getInt("st") : null,
                                   row.has("v") ? row.getInt("v") : null).toString());
        rows.sort(String::compareTo);
        return rows;
    }
}
