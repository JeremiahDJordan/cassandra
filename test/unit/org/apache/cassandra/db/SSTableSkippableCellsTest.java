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

package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithNowInSec;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests that the sstables of a read drop the fetched but not queried simple cells that are older than
 * the primary key liveness info of their row when none of them can hold a primary key liveness info that is not live,
 * and keep them otherwise (see {@link ReadCommand#withoutSkippableCells}, which drops them from the merged rows, and
 * reads again the rows that memtable data gives a primary key liveness info that is not live).
 */
public class SSTableSkippableCellsTest extends CQLTester
{
    private static final String[] QUERIES = { "SELECT k, c, x FROM %s WHERE k = 1",              // slices
                                              "SELECT k, c, x FROM %s WHERE k = 1 AND c = 1",   // names, in timestamp order
                                              "SELECT k, c, x FROM %s" };                       // range

    @Test
    public void testSSTablesWithoutExpiringData() throws Throwable
    {
        createOverwrittenRow();
        flush();

        for (String query : QUERIES)
        {
            long nowInSec = FBUtilities.nowInSeconds();
            Row row = storageRow(query, nowInSec);
            assertNull(query, cell(row, "r"));
            assertNull(query, cell(row, "t"));
            assertNotNull(query, cell(row, "x"));
            // the cells of a collection are not dropped by the sstables
            assertNotNull(query, row.getCell(column("l"), CellPath.create(Int32Type.instance.decompose(1))));
            assertRows(execute(query), row(1, 1, 3));
        }
    }

    @Test
    public void testMemtableData() throws Throwable
    {
        createOverwrittenRow("trie");
        flush();
        execute("UPDATE %s USING TIMESTAMP 3 SET x = 4 WHERE k = 1 AND c = 1");

        for (String query : QUERIES)
        {
            Row row = storageRow(query, FBUtilities.nowInSeconds());
            assertNull(query, cell(row, "r"));
            assertNull(query, cell(row, "t"));
        }
        assertRows(execute(QUERIES[1]), row(1, 1, 4));
    }

    @Test
    public void testMemtableWithExpiredRowMarker() throws Throwable
    {
        createRowAliveByOlderCell();
        execute("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 3 AND TTL 1");

        // the row marker of the memtable has expired at that time
        long nowInSec = FBUtilities.nowInSeconds() + 10;
        for (String query : QUERIES)
        {
            assertNotNull(query, cell(storageRow(query, nowInSec), "r"));
            assertEquals(query, 1, executeInternalWithNowInSec(formatQuery(query), nowInSec).size());
        }
    }

    @Test
    public void testMemtableWithExpiredRowMarkerNotSeenByCheck() throws Throwable
    {
        createRowAliveByOlderCell();
        execute("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 3 AND TTL 1");

        // as for an expired row marker written after the read checked the memtable: the sstable drops r, and the
        // read reads the row again
        Injection hideExpiredData = Injections.newCustom("hide_expired_memtable_data")
                                              .add(InvokePointBuilder.newInvokePoint()
                                                                     .onClass(TrieMemtable.class)
                                                                     .onMethod("getMinLocalDeletionTime")
                                                                     .atEntry())
                                              .add(ActionBuilder.newActionBuilder().actions().doReturn("Long.MAX_VALUE"))
                                              .build();
        Injections.inject(hideExpiredData);
        try
        {
            long nowInSec = FBUtilities.nowInSeconds() + 10;
            for (String query : QUERIES)
            {
                assertNull(query, cell(storageRow(query, nowInSec), "r"));
                assertEquals(query, 1, executeInternalWithNowInSec(formatQuery(query), nowInSec).size());
            }
        }
        finally
        {
            hideExpiredData.disable();
        }
    }

    @Test
    public void testPartitionsOfOneRead() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, x int, PRIMARY KEY (k, c))");
        disableCompaction();
        for (int c = 0; c < 10; c++)
            execute("INSERT INTO %s (k, c, x) VALUES (2, ?, ?) USING TTL 1", c, c);
        flush();
        execute("INSERT INTO %s (k, c, x) VALUES (1, 1, 1)");

        // the read of partition 1 has no sstable that can hold an expired row marker, that of partition 2 has: the
        // flag the first sets must not make the second read its expired rows again
        Injections.Counter rereads = Injections.newCounter("rereads")
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(SinglePartitionReadCommand.class)
                                                                      .onMethod("queryMemtableAndDisk", ColumnFamilyStore.class, ReadExecutionController.class)
                                                                      .atEntry())
                                               .build();
        Injections.inject(rereads);
        try
        {
            long nowInSec = FBUtilities.nowInSeconds() + 10;
            assertEquals(1, executeInternalWithNowInSec(formatQuery("SELECT k, c, x FROM %s WHERE k IN (1, 2)"), nowInSec).size());
            assertEquals(0, rereads.get());
        }
        finally
        {
            rereads.disable();
        }
    }

    @Test
    public void testSSTableWithExpiredData() throws Throwable
    {
        createOverwrittenRow();
        flush();
        execute("INSERT INTO %s (k, c, x) VALUES (1, 2, 4) USING TIMESTAMP 3 AND TTL 1");
        flush();

        // the TTL of the second sstable has expired at that time
        assertSSTablesKeepOlderCells(FBUtilities.nowInSeconds() + 10, 3);
    }

    @Test
    public void testSSTableWithExpiringData() throws Throwable
    {
        createOverwrittenRow();
        flush();
        execute("INSERT INTO %s (k, c, x) VALUES (1, 2, 4) USING TIMESTAMP 3 AND TTL 1000");
        flush();

        // the TTL of the second sstable has not expired
        for (String query : QUERIES)
        {
            Row row = storageRow(query, FBUtilities.nowInSeconds());
            assertNull(query, cell(row, "r"));
            assertNull(query, cell(row, "t"));
        }
    }

    @Test
    public void testSSTableWithExpiredViewLiveness() throws Throwable
    {
        createOverwrittenRow();
        flush();
        // an expired liveness (written by views) is never live, even before its local expiration time
        TableMetadata metadata = currentTableMetadata();
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.make(Int32Type.instance.decompose(1)));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.withExpirationTime(3, LivenessInfo.EXPIRED_LIVENESS_TTL, FBUtilities.nowInSeconds() + 1000));
        new Mutation(PartitionUpdate.singleRowUpdate(metadata, metadata.partitioner.decorateKey(Int32Type.instance.decompose(1)), builder.build())).apply();
        flush();

        assertSSTablesKeepOlderCells(FBUtilities.nowInSeconds(), 3);
    }

    @Test
    public void testColumnFilter() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, r int, x int, PRIMARY KEY (k, c))");

        ColumnFilter wildcard = ColumnFilter.all(currentTableMetadata());
        assertSame(wildcard, wildcard.withNotQueriedCellsOlderThanRowDropped());
        assertFalse(wildcard.dropsNotQueriedCellsOlderThanRow());

        ColumnFilter selection = readCommand(formatQuery(QUERIES[0]), FBUtilities.nowInSeconds()).columnFilter();
        assertFalse(selection.dropsNotQueriedCellsOlderThanRow());
        ColumnFilter dropping = selection.withNotQueriedCellsOlderThanRowDropped();
        assertTrue(dropping.dropsNotQueriedCellsOlderThanRow());
        assertSame(dropping, dropping.withNotQueriedCellsOlderThanRowDropped());
        assertNotEquals(selection, dropping);
        assertEquals(selection.fetchedColumns(), dropping.fetchedColumns());
        assertEquals(selection.queriedColumns(), dropping.queriedColumns());
    }

    /**
     * A row whose row marker (timestamp 2) is newer than the cells of its not queried columns: a live cell of r, a
     * tombstone of t and a live cell of the collection l (timestamp 1).
     */
    private void createOverwrittenRow() throws Throwable
    {
        createOverwrittenRow("default");
    }

    private void createOverwrittenRow(String memtable) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, r int, t int, x int, l set<int>, PRIMARY KEY (k, c)) WITH memtable = '" + memtable + "'");
        disableCompaction();
        execute("INSERT INTO %s (k, c, r, t, x, l) VALUES (1, 1, 1, 1, 1, {1}) USING TIMESTAMP 1");
        execute("DELETE t FROM %s USING TIMESTAMP 1 WHERE k = 1 AND c = 1");
        execute("INSERT INTO %s (k, c, x) VALUES (1, 1, 3) USING TIMESTAMP 2");
    }

    /**
     * A row that only r, older than its row marker and not queried, keeps alive once a newer row marker is not live.
     */
    private void createRowAliveByOlderCell() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, r int, x int, PRIMARY KEY (k, c)) WITH memtable = 'trie'");
        disableCompaction();
        execute("INSERT INTO %s (k, c, r) VALUES (1, 1, 1) USING TIMESTAMP 1");
        execute("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 2");
        flush();
    }

    private void assertSSTablesKeepOlderCells(long nowInSec, int x) throws Throwable
    {
        for (String query : QUERIES)
        {
            Row row = storageRow(query, nowInSec);
            assertNotNull(query, cell(row, "r"));
            assertNotNull(query, cell(row, "t"));
            assertNotNull(query, cell(row, "x"));
        }
        assertRows(execute(QUERIES[1]), row(1, 1, x));
    }

    /**
     * The row (k = 1, c = 1) as merged from the sources, before {@link ReadCommand#withoutSkippableCells}.
     */
    private Row storageRow(String query, long nowInSec)
    {
        ReadCommand command = readCommand(formatQuery(query), nowInSec);
        List<Row> rows = new ArrayList<>();
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.queryStorage(getCurrentColumnFamilyStore(), controller))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (unfiltered.isRow() && Int32Type.instance.compose(unfiltered.clustering().bufferAt(0)) == 1)
                            rows.add((Row) unfiltered);
                    }
                }
            }
        }
        assertEquals(query, 1, rows.size());
        return rows.get(0);
    }

    private Cell<?> cell(Row row, String column)
    {
        return row.getCell(column(column));
    }

    private ColumnMetadata column(String name)
    {
        return currentTableMetadata().getColumn(ColumnIdentifier.getInterned(name, false));
    }

    private static ReadCommand readCommand(String query, long nowInSec)
    {
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query).prepare(ClientState.forInternalCalls());
        ReadQuery readQuery = statement.getQuery(QueryOptions.DEFAULT, nowInSec);
        return readQuery instanceof SinglePartitionReadQuery.Group
               ? (ReadCommand) ((SinglePartitionReadQuery.Group<?>) readQuery).queries.get(0)
               : (ReadCommand) readQuery;
    }
}
