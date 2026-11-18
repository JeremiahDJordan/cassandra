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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
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
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.sensors.ActiveRequestSensors;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
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
     * The memtable configurations of test/conf/cassandra.yaml with different read paths; the default TrieMemtable
     * copies the rows it returns to the heap as BTreeRows.
     */
    private static final String[] MEMTABLES = { "default", "skiplist", "trie_stage1", "trie_stage2", "trie_stage3", "persistent_memory" };

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

        static final Storage[] WITH_MEMTABLE = { MEMTABLE, SSTABLE_AND_MEMTABLE };

        static final Storage[] NOT_COMPACTED = { MEMTABLE, SSTABLE, SSTABLE_AND_MEMTABLE, SSTABLES };

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
            assertRowsInternalAndNet(table, "SELECT count(*) FROM %s", row(1L));
            for (String restriction : RESTRICTIONS)
            {
                assertRowsInternalAndNet(table, "SELECT * FROM %s" + restriction, row(1, 1, null, null, null, 7, null));
                assertRowsInternalAndNet(table, "SELECT k, c FROM %s" + restriction, row(1, 1));
                assertRowsInternalAndNet(table, "SELECT k, c, r FROM %s" + restriction, row(1, 1, 7));
                assertRowsInternalAndNet(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
                assertRowsInternalAndNet(table, "SELECT k, c, writetime(x) FROM %s" + restriction, row(1, 1, null));
                assertRowsInternalAndNet(table, "SELECT k, c, ttl(x) FROM %s" + restriction, row(1, 1, null));
                assertRowsInternalAndNet(table, "SELECT k, c, l FROM %s" + restriction, row(1, 1, null));
                assertRowsInternalAndNet(table, "SELECT k, c, m['a'] FROM %s" + restriction, row(1, 1, null));
                assertRowsInternalAndNet(table, "SELECT k, c, s, x FROM %s" + restriction, row(1, 1, null, null));
            }
            assertRowsInternalAndNet(table, "SELECT k, c, x FROM %s WHERE r = 7 ALLOW FILTERING", row(1, 1, null));
        }
    }

    @Test
    public void testRowAliveByCollectionCellOlderThanExpiredMarker() throws Throwable
    {
        Map<Storage, String> tables = writeRows(EXPIRING_MARKER, "INSERT INTO %s (k, c, l) VALUES (1, 1, {7}) USING TIMESTAMP 1");

        for (Map.Entry<Storage, String> table : tables.entrySet())
        {
            assertRowsInternalAndNet(table, "SELECT count(*) FROM %s", row(1L));
            for (String restriction : RESTRICTIONS)
            {
                assertRowsInternalAndNet(table, "SELECT k, c, l FROM %s" + restriction, row(1, 1, set(7)));
                assertRowsInternalAndNet(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
                assertRowsInternalAndNet(table, "SELECT k, c, r, writetime(r) FROM %s" + restriction, row(1, 1, null, null));
                assertRowsInternalAndNet(table, "SELECT k, c, m['a'] FROM %s" + restriction, row(1, 1, null));
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
                assertRowsInternalAndNet(table, "SELECT k, c, m['a'] FROM %s" + restriction, row(1, 1, 7));
                assertRowsInternalAndNet(table, "SELECT k, c, m['b'] FROM %s" + restriction, row(1, 1, null));
                assertRowsInternalAndNet(table, "SELECT k, c, m['b'], x FROM %s" + restriction, row(1, 1, null, null));
                assertRowsInternalAndNet(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
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
                assertRowsInternalAndNet(table, "SELECT k, c, s FROM %s" + restriction, row(1, 1, 5));
                assertRowsInternalAndNet(table, "SELECT k, c, s, x FROM %s" + restriction, row(1, 1, 5, null));
                assertRowsInternalAndNet(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
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
            assertRowsInternalAndNet(table, "SELECT count(*) FROM %s", row(1L));
            for (String restriction : RESTRICTIONS)
            {
                assertRowsInternalAndNet(table, "SELECT k, c, r FROM %s" + restriction, row(1, 1, 7));
                assertRowsInternalAndNet(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
                assertRowsInternalAndNet(table, "SELECT k, c, writetime(x) FROM %s" + restriction, row(1, 1, null));
            }
        }
    }

    @Test
    public void testRowWithLiveExpiringMarker() throws Throwable
    {
        Map<Storage, String> tables = writeRows(LIVE_EXPIRING_MARKER, "INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1");

        for (Map.Entry<Storage, String> table : tables.entrySet())
        {
            assertRowsInternalAndNet(table, "SELECT count(*) FROM %s", row(1L));
            for (String restriction : RESTRICTIONS)
            {
                assertRowsInternalAndNet(table, "SELECT k, c, r FROM %s" + restriction, row(1, 1, 7));
                assertRowsInternalAndNet(table, "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
                assertRowsInternalAndNet(table, "SELECT k, c, writetime(r), writetime(x) FROM %s" + restriction, row(1, 1, 1L, null));
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
            assertRowsInternalAndNet(table, "SELECT count(*) FROM %s", row(0L));
            for (String restriction : RESTRICTIONS)
            {
                assertRowsInternalAndNet(table, "SELECT * FROM %s" + restriction);
                assertRowsInternalAndNet(table, "SELECT k, c FROM %s" + restriction);
                assertRowsInternalAndNet(table, "SELECT k, c, r FROM %s" + restriction);
                assertRowsInternalAndNet(table, "SELECT k, c, x FROM %s" + restriction);
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
                for (Partition partition : Util.getAllUnfiltered(parseReadCommand("SELECT k, c, x FROM %s" + restriction)))
                {
                    for (Row row : partition.rows())
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
                    for (Partition partition : Util.getAllUnfiltered(parseReadCommand(query)))
                    {
                        for (Row row : partition.rows())
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

    /**
     * A row whose row marker has expired is still returned while some of its older cells are live, and isn't once
     * those cells are deleted, with every memtable implementation.
     */
    @Test
    public void testRowAliveByOlderCellsWithEveryMemtable() throws Throwable
    {
        Map<String, String> alive = new LinkedHashMap<>();
        Map<String, String> dead = new LinkedHashMap<>();
        for (String memtable : MEMTABLES)
        {
            for (Storage storage : Storage.WITH_MEMTABLE)
            {
                String label = memtable + ", " + storage;
                alive.put(label, createRowTable(memtable, storage, EXPIRING_MARKER,
                                             "INSERT INTO %s (k, c, r, l, m) VALUES (1, 1, 7, {7}, {'a': 7}) USING TIMESTAMP 1"));
                dead.put(label, createRowTable(memtable, storage, EXPIRING_MARKER,
                                            "INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1",
                                            FLUSH,
                                            "DELETE r FROM %s USING TIMESTAMP 2 WHERE k = 1 AND c = 1"));
            }
        }
        awaitMarkerExpiry();

        for (Map.Entry<String, String> table : alive.entrySet())
        {
            assertRowsInternalAndNet(table.getKey(), table.getValue(), "SELECT count(*) FROM %s", row(1L));
            for (String restriction : RESTRICTIONS)
            {
                assertRowsInternalAndNet(table.getKey(), table.getValue(), "SELECT k, c, x FROM %s" + restriction, row(1, 1, null));
                assertRowsInternalAndNet(table.getKey(), table.getValue(), "SELECT k, c, r FROM %s" + restriction, row(1, 1, 7));
                assertRowsInternalAndNet(table.getKey(), table.getValue(), "SELECT k, c, l FROM %s" + restriction, row(1, 1, set(7)));
                assertRowsInternalAndNet(table.getKey(), table.getValue(), "SELECT k, c, m['b'] FROM %s" + restriction, row(1, 1, null));
            }
        }
        for (Map.Entry<String, String> table : dead.entrySet())
        {
            assertRowsInternalAndNet(table.getKey(), table.getValue(), "SELECT count(*) FROM %s", row(0L));
            for (String restriction : RESTRICTIONS)
            {
                assertRowsInternalAndNet(table.getKey(), table.getValue(), "SELECT * FROM %s" + restriction);
                assertRowsInternalAndNet(table.getKey(), table.getValue(), "SELECT k, c, x FROM %s" + restriction);
            }
        }
    }

    /**
     * A replica returns the same digest wherever the cells and the row marker are stored, with every memtable
     * implementation, when the not queried cells are older than the row marker, live or expired, or are tombstones.
     */
    @Test
    public void testDigestsMatchAcrossStoragesAndMemtables() throws Throwable
    {
        String olderWrite = "INSERT INTO %s (k, c, r, l, m) VALUES (1, 1, 7, {7}, {'a': 1, 'b': 2}) USING TIMESTAMP 1";
        String liveMarker = "INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 10";
        String[][] scenarios = { { liveMarker, olderWrite },
                                 { EXPIRING_MARKER, olderWrite },
                                 { liveMarker, olderWrite, "DELETE r, m['b'] FROM %s USING TIMESTAMP 2 WHERE k = 1 AND c = 1" } };

        List<Map<String, String>> tables = new ArrayList<>();
        for (String[] scenario : scenarios)
        {
            Map<String, String> scenarioTables = new LinkedHashMap<>();
            Map<String, Storage> storages = new LinkedHashMap<>();
            for (String memtable : MEMTABLES)
            {
                for (Storage storage : Storage.NOT_COMPACTED)
                {
                    String table = createTable(TABLE + " WITH memtable = '" + memtable + '\'');
                    disableCompaction();
                    scenarioTables.put(memtable + ", " + storage, table);
                    storages.put(table, storage);
                }
            }

            // The digests include the local deletion and expiration times, so each write is done to every table in
            // the same second.
            writeInOneSecond(storages.keySet(), Arrays.copyOfRange(scenario, 1, scenario.length));
            for (Map.Entry<String, Storage> table : storages.entrySet())
            {
                if (table.getValue().flushOlderWrites)
                    flush(KEYSPACE, table.getKey());
            }
            writeInOneSecond(storages.keySet(), scenario[0]);
            for (Map.Entry<String, Storage> table : storages.entrySet())
            {
                if (table.getValue().flushMarker)
                    flush(KEYSPACE, table.getKey());
            }
            tables.add(scenarioTables);
        }
        awaitMarkerExpiry();

        long nowInSec = FBUtilities.nowInSeconds();
        for (int i = 0; i < scenarios.length; i++)
        {
            for (String restriction : RESTRICTIONS)
            {
                for (String query : new String[]{ "SELECT k, c, x FROM %s" + restriction, "SELECT k, c, m['a'] FROM %s" + restriction })
                {
                    Map<String, String> digests = new LinkedHashMap<>();
                    for (Map.Entry<String, String> table : tables.get(i).entrySet())
                        digests.put(table.getKey(), digest(String.format(query, KEYSPACE + '.' + table.getValue()), nowInSec));
                    assertEquals(String.join("; ", scenarios[i]) + ": " + query + ": " + digests, 1, digests.values().stream().distinct().count());
                }
            }
        }
    }

    /**
     * Applies the writes to every table, starting at the beginning of a second and checking that they are done
     * within that second.
     */
    private void writeInOneSecond(Iterable<String> tables, String... writes) throws Throwable
    {
        while (System.currentTimeMillis() % 1000 > 100)
            Thread.sleep(10);
        long second = FBUtilities.nowInSeconds();
        for (String table : tables)
        {
            for (String write : writes)
                execute(String.format(write, KEYSPACE + '.' + table));
        }
        assertEquals("the writes took more than a second", second, FBUtilities.nowInSeconds());
    }

    /**
     * The read bytes sensor doesn't count the cells of the columns that are fetched but not queried and older than a
     * live row marker, which are not returned, wherever they are stored.
     */
    @Test
    public void testReadBytesSensorExcludesSkippedCells() throws Throwable
    {
        for (Storage storage : Storage.NOT_COMPACTED)
        {
            createTable(TABLE);
            disableCompaction();
            execute("INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1");
            if (storage.flushOlderWrites)
                flush();
            execute("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 10");
            execute("INSERT INTO %s (k, c) VALUES (1, 2) USING TIMESTAMP 10");
            if (storage.flushMarker)
                flush();

            // c = 1 has the older r cell, c = 2 has the row marker only
            for (String restriction : new String[]{ " WHERE k = 1 AND c = %d", " WHERE k = 1 AND c >= %1$d AND c <= %1$d" })
            {
                double markerOnly = readBytes(formatQuery("SELECT k, c, x FROM %s" + String.format(restriction, 2)));
                double withOlderCell = readBytes(formatQuery("SELECT k, c, x FROM %s" + String.format(restriction, 1)));
                assertTrue(storage + ": " + markerOnly, markerOnly > 0);
                assertEquals(storage + ": " + String.format(restriction, 1), markerOnly, withOlderCell, 0);
            }
        }
    }

    private ReadCommand readCommand(String query, long nowInSec)
    {
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query).prepare(ClientState.forInternalCalls());
        ReadQuery readQuery = statement.getQuery(QueryOptions.DEFAULT, nowInSec);
        return readQuery instanceof SinglePartitionReadQuery.Group
               ? (ReadCommand) ((SinglePartitionReadQuery.Group<?>) readQuery).queries.get(0)
               : (ReadCommand) readQuery;
    }

    private String digest(String query, long nowInSec)
    {
        ReadCommand command = readCommand(query, nowInSec);
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            return ByteBufferUtil.bytesToHex(ReadResponse.createDigestResponse(iterator, command).digest(command));
        }
    }

    private double readBytes(String query)
    {
        ReadCommand command = parseReadCommand(query);
        ActiveRequestSensors sensors = new ActiveRequestSensors();
        Context context = Context.from(command);
        sensors.registerSensor(context, Type.READ_BYTES);
        RequestTracker.instance.set(sensors);
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            ReadResponse.createDataResponse(iterator, command, controller.getRepairedDataInfo());
        }
        finally
        {
            RequestTracker.instance.set(null);
        }
        return sensors.getSensor(context, Type.READ_BYTES).get().getValue();
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
            tables.put(storage, createRowTable("default", storage, markerWrite, olderWrites));

        awaitMarkerExpiry();

        for (Map.Entry<Storage, String> table : tables.entrySet())
        {
            if (table.getKey().compact)
                compact(KEYSPACE, table.getValue());
        }
        return tables;
    }

    /**
     * Creates a table with the given memtable configuration, applies {@code olderWrites} then {@code markerWrite},
     * and flushes according to the storage (a compacted storage is compacted by {@link #writeRows}).
     */
    private String createRowTable(String memtable, Storage storage, String markerWrite, String... olderWrites) throws Throwable
    {
        String table = createTable(TABLE + " WITH memtable = '" + memtable + '\'' + (storage == Storage.PURGED ? " AND gc_grace_seconds = 0" : ""));
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
        return table;
    }

    /** Also waits for the expired row marker to be purgeable with gc_grace_seconds = 0. */
    private static void awaitMarkerExpiry() throws InterruptedException
    {
        Thread.sleep(2500);
    }

    private void assertRowsInternalAndNet(Map.Entry<Storage, String> table, String query, Object[]... rows) throws Throwable
    {
        assertRowsInternalAndNet(table.getKey().toString(), table.getValue(), query, rows);
    }

    private void assertRowsInternalAndNet(String label, String table, String query, Object[]... rows) throws Throwable
    {
        String formattedQuery = String.format(query, KEYSPACE + '.' + table);
        try
        {
            assertRows(execute(formattedQuery), rows);
            assertRowsNet(executeNet(formattedQuery), rows);
        }
        catch (AssertionError e)
        {
            throw new AssertionError(label + ": " + formattedQuery + ": " + e.getMessage(), e);
        }
    }
}
