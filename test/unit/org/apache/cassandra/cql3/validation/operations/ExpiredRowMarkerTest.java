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

package org.apache.cassandra.cql3.validation.operations;

import java.util.EnumMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that a row whose row marker (primary key liveness info) was written again with a TTL that has expired is
 * still returned by every selection while some of its older cells are live, wherever its cells are stored.
 */
public class ExpiredRowMarkerTest extends CQLTester
{
    private static final String TABLE = "CREATE TABLE %s (k int, c int, r int, x int, l set<int>, m map<text, int>, s int static, PRIMARY KEY (k, c))";

    private static final String EXPIRING_MARKER = "INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 10 AND TTL 1";
    private static final String LIVE_EXPIRING_MARKER = "INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 10 AND TTL 100000";

    private static final String FLUSH = "flush";

    /**
     * Restrictions selecting row (1, 1): range scans, a partition read, slices and reads by name.
     */
    private static final String[] RESTRICTIONS = { "",
                                                   " WHERE token(k) >= token(1)",
                                                   " WHERE k = 1",
                                                   " WHERE k = 1 AND c >= 1",
                                                   " WHERE k = 1 AND c <= 1 ORDER BY c DESC",
                                                   " WHERE k = 1 AND c = 1",
                                                   " WHERE k = 1 AND c IN (1, 2)" };

    /**
     * Where the older writes and the row marker are when the row is read.
     */
    private enum Storage
    {
        MEMTABLE(false, false, false),
        SSTABLE(false, true, false),
        SSTABLE_AND_MEMTABLE(true, false, false),
        SSTABLES(true, true, false),
        COMPACTED(true, true, true),
        PURGED(true, true, true); // compacted with gc_grace_seconds = 0, after the expiry of the row marker

        final boolean flushOlderWrites;
        final boolean flushMarker;
        final boolean compact;

        Storage(boolean flushOlderWrites, boolean flushMarker, boolean compact)
        {
            this.flushOlderWrites = flushOlderWrites;
            this.flushMarker = flushMarker;
            this.compact = compact;
        }
    }

    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    @Test
    public void testRowAliveBySimpleCellOlderThanExpiredMarker() throws Throwable
    {
        Map<Storage, String> tables = writeRows(EXPIRING_MARKER, "INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1");

        for (Map.Entry<Storage, String> table : tables.entrySet())
        {
            assertRow(table, "SELECT count(*) FROM %s", row(1L));
            for (String restriction : RESTRICTIONS)
            {
                assertRow(table, "SELECT * FROM %s" + restriction, row(1, 1, null, null, null, 7, null));
                assertRow(table, "SELECT k, c FROM %s" + restriction, row(1, 1));
                assertRow(table, "SELECT k, c, r FROM %s" + restriction, row(1, 1, 7));
                assertRow(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
                assertRow(table, "SELECT k, c, writetime(x) FROM %s" + restriction, row(1, 1, null));
                assertRow(table, "SELECT k, c, ttl(x) FROM %s" + restriction, row(1, 1, null));
                assertRow(table, "SELECT k, c, l FROM %s" + restriction, row(1, 1, null));
                assertRow(table, "SELECT k, c, m['a'] FROM %s" + restriction, row(1, 1, null));
                assertRow(table, "SELECT k, c, s, x FROM %s" + restriction, row(1, 1, null, null));
            }
            assertRow(table, "SELECT k, c, x FROM %s WHERE r = 7 ALLOW FILTERING", row(1, 1, null));
        }
    }

    @Test
    public void testRowAliveByCollectionCellOlderThanExpiredMarker() throws Throwable
    {
        Map<Storage, String> tables = writeRows(EXPIRING_MARKER, "INSERT INTO %s (k, c, l) VALUES (1, 1, {7}) USING TIMESTAMP 1");

        for (Map.Entry<Storage, String> table : tables.entrySet())
        {
            assertRow(table, "SELECT count(*) FROM %s", row(1L));
            for (String restriction : RESTRICTIONS)
            {
                assertRow(table, "SELECT k, c, l FROM %s" + restriction, row(1, 1, set(7)));
                assertRow(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
                assertRow(table, "SELECT k, c, r, writetime(r) FROM %s" + restriction, row(1, 1, null, null));
                assertRow(table, "SELECT k, c, m['a'] FROM %s" + restriction, row(1, 1, null));
            }
        }
    }

    @Test
    public void testRowAliveByMapElementOutsideSelectionOlderThanExpiredMarker() throws Throwable
    {
        Map<Storage, String> tables = writeRows(EXPIRING_MARKER, "INSERT INTO %s (k, c, m) VALUES (1, 1, {'a': 7}) USING TIMESTAMP 1");

        for (Map.Entry<Storage, String> table : tables.entrySet())
        {
            for (String restriction : RESTRICTIONS)
            {
                assertRow(table, "SELECT k, c, m['a'] FROM %s" + restriction, row(1, 1, 7));
                assertRow(table, "SELECT k, c, m['b'] FROM %s" + restriction, row(1, 1, null));
                assertRow(table, "SELECT k, c, m['b'], x FROM %s" + restriction, row(1, 1, null, null));
                assertRow(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
            }
        }
    }

    @Test
    public void testRowAliveByOlderCellWithStaticColumn() throws Throwable
    {
        Map<Storage, String> tables = writeRows(EXPIRING_MARKER,
                                                "INSERT INTO %s (k, s) VALUES (1, 5) USING TIMESTAMP 1",
                                                "INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1");

        for (Map.Entry<Storage, String> table : tables.entrySet())
        {
            for (String restriction : RESTRICTIONS)
            {
                assertRow(table, "SELECT k, c, s FROM %s" + restriction, row(1, 1, 5));
                assertRow(table, "SELECT k, c, s, x FROM %s" + restriction, row(1, 1, 5, null));
                assertRow(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
            }
        }
    }

    /**
     * The older cell and a newer row marker without TTL are in the same source, and the expired row marker is in
     * another one: the row marker of the first source is live, but it is superseded by the expired one.
     */
    @Test
    public void testRowAliveByOlderCellWithSupersededLiveMarker() throws Throwable
    {
        Map<Storage, String> tables = writeRows(EXPIRING_MARKER,
                                                "INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1",
                                                "INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 2");

        for (Map.Entry<Storage, String> table : tables.entrySet())
        {
            assertRow(table, "SELECT count(*) FROM %s", row(1L));
            for (String restriction : RESTRICTIONS)
            {
                assertRow(table, "SELECT k, c, r FROM %s" + restriction, row(1, 1, 7));
                assertRow(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
                assertRow(table, "SELECT k, c, writetime(x) FROM %s" + restriction, row(1, 1, null));
            }
        }
    }

    @Test
    public void testRowWithLiveExpiringMarker() throws Throwable
    {
        Map<Storage, String> tables = writeRows(LIVE_EXPIRING_MARKER, "INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1");

        for (Map.Entry<Storage, String> table : tables.entrySet())
        {
            assertRow(table, "SELECT count(*) FROM %s", row(1L));
            for (String restriction : RESTRICTIONS)
            {
                assertRow(table, "SELECT k, c, r FROM %s" + restriction, row(1, 1, 7));
                assertRow(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
                assertRow(table, "SELECT k, c, writetime(r), writetime(x) FROM %s" + restriction, row(1, 1, 1L, null));
            }
        }
    }

    /**
     * The older cell is deleted, so the row has no live data once its row marker has expired.
     */
    @Test
    public void testRowDeadWithDeletedOlderCellAndExpiredMarker() throws Throwable
    {
        Map<Storage, String> tables = writeRows(EXPIRING_MARKER,
                                                "INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1",
                                                FLUSH,
                                                "DELETE r FROM %s USING TIMESTAMP 2 WHERE k = 1 AND c = 1");

        for (Map.Entry<Storage, String> table : tables.entrySet())
        {
            assertRow(table, "SELECT count(*) FROM %s", row(0L));
            for (String restriction : RESTRICTIONS)
            {
                assertRow(table, "SELECT * FROM %s" + restriction);
                assertRow(table, "SELECT k, c FROM %s" + restriction);
                assertRow(table, "SELECT k, c, r FROM %s" + restriction);
                assertRow(table, "SELECT k, c, x FROM %s" + restriction);
            }
        }
    }

    /**
     * When the row marker is live, a replica doesn't return the cells of the columns that are fetched but not queried
     * and older than the marker, wherever the cells and the marker are stored.
     */
    @Test
    public void testOlderNotQueriedCellsSkippedWithLiveMarker() throws Throwable
    {
        for (Storage storage : new Storage[]{ Storage.MEMTABLE, Storage.SSTABLE, Storage.SSTABLE_AND_MEMTABLE, Storage.SSTABLES })
        {
            createTable(TABLE);
            disableCompaction();
            execute("INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1");
            if (storage.flushOlderWrites)
                flush();
            execute("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 10");
            if (storage.flushMarker)
                flush();

            ColumnMetadata r = currentTableMetadata().getColumn(ColumnIdentifier.getInterned("r", false));
            for (String restriction : RESTRICTIONS)
            {
                int rows = 0;
                for (ImmutableBTreePartition partition : Util.getAllUnfiltered(readCommand(formatQuery("SELECT k, c, x FROM %s" + restriction))))
                {
                    for (Row row : partition)
                    {
                        assertNull(storage + ": " + restriction, row.getCell(r));
                        rows++;
                    }
                }
                assertEquals(storage + ": " + restriction, 1, rows);
            }
        }
    }

    /**
     * Whether the row marker is live or has expired, a replica doesn't return the tombstones of the columns that are
     * fetched but not queried and older than the marker, as they don't keep the row alive.
     */
    @Test
    public void testOlderNotQueriedTombstonesSkipped() throws Throwable
    {
        for (String marker : new String[]{ EXPIRING_MARKER, LIVE_EXPIRING_MARKER })
        {
            Map<Storage, String> tables = writeRows(marker,
                                                    "INSERT INTO %s (k, c, r, m) VALUES (1, 1, 7, {'a': 1, 'b': 2}) USING TIMESTAMP 1",
                                                    "DELETE r, m['a'] FROM %s USING TIMESTAMP 2 WHERE k = 1 AND c = 1");

            for (Map.Entry<Storage, String> table : tables.entrySet())
            {
                for (String restriction : RESTRICTIONS)
                {
                    String query = String.format("SELECT k, c, x FROM %s" + restriction, KEYSPACE + '.' + table.getValue());
                    int rows = 0;
                    for (ImmutableBTreePartition partition : Util.getAllUnfiltered(readCommand(query)))
                    {
                        for (Row row : partition)
                        {
                            for (Cell<?> cell : row.cells())
                                assertTrue(table.getKey() + ": " + query + ": " + cell, cell.isLive(FBUtilities.nowInSeconds()));
                            rows++;
                        }
                    }
                    assertEquals(table.getKey() + ": " + query, 1, rows);
                }
            }
        }
    }

    private ReadCommand readCommand(String query)
    {
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query).prepare(ClientState.forInternalCalls());
        ReadQuery readQuery = statement.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        return readQuery instanceof SinglePartitionReadQuery.Group
               ? (ReadCommand) ((SinglePartitionReadQuery.Group<?>) readQuery).queries.get(0)
               : (ReadCommand) readQuery;
    }

    /**
     * For every {@link Storage}, creates a table, applies {@code olderWrites} then {@code markerWrite}, flushing
     * and compacting according to the storage, and returns the tables once the TTL of {@link #EXPIRING_MARKER} has
     * expired.
     */
    private Map<Storage, String> writeRows(String markerWrite, String... olderWrites) throws Throwable
    {
        Map<Storage, String> tables = new EnumMap<>(Storage.class);
        for (Storage storage : Storage.values())
        {
            tables.put(storage, createTable(TABLE + (storage == Storage.PURGED ? " WITH gc_grace_seconds = 0" : "")));
            disableCompaction();
            for (String write : olderWrites)
            {
                if (write.equals(FLUSH))
                    flush();
                else
                    execute(write);
            }
            if (storage.flushOlderWrites)
                flush();
            execute(markerWrite);
            if (storage.flushMarker)
                flush();
        }

        // Wait for the TTL to expire, and for the expired row marker to be purgeable with gc_grace_seconds = 0
        Thread.sleep(2500);

        for (Map.Entry<Storage, String> table : tables.entrySet())
        {
            if (table.getKey().compact)
                compact(KEYSPACE, table.getValue());
        }
        return tables;
    }

    private void assertRow(Map.Entry<Storage, String> table, String query, Object[]... rows) throws Throwable
    {
        String formattedQuery = String.format(query, KEYSPACE + '.' + table.getValue());
        try
        {
            assertRows(execute(formattedQuery), rows);
            assertRowsNet(executeNet(formattedQuery), rows);
        }
        catch (AssertionError e)
        {
            throw new AssertionError(table.getKey() + ": " + formattedQuery + ": " + e.getMessage(), e);
        }
    }
}
