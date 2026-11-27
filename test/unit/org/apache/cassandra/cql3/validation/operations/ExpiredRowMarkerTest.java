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

import java.nio.ByteBuffer;
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
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that a row whose row marker (primary key liveness info) was written again with a TTL that has expired is
 * still returned by every selection while some of its older cells are live, wherever its cells are stored.
 */
public class ExpiredRowMarkerTest extends CQLTester
{
    private static final String TABLE = "CREATE TABLE %s (k int, c int, r int, x int, l set<int>, m map<text, int>, s int static, PRIMARY KEY (k, c))";

    private static final String LIVE_MARKER = "INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 10";
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

        // the older writes and the row marker end up in the same row of a memtable or an sstable
        static final Storage[] SAME_SOURCE = { MEMTABLE, SSTABLE, COMPACTED };

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
     * another one (SSTABLE_AND_MEMTABLE, SSTABLES): the row marker of the first source is live, but it is superseded by
     * the expired one, in range reads, slices and reads by name.
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

    /**
     * Three sstables, read by name newest first: the first has the older cell and a row marker without TTL, the second
     * a newer row marker with a TTL, live, and the third an even newer row marker, expired.
     */
    @Test
    public void testRowAliveByOlderCellWithSupersededMarkersInThreeSources() throws Throwable
    {
        createTable(TABLE);
        disableCompaction();
        execute("INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1");
        execute("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 10");
        execute("INSERT INTO %s (k, c, r) VALUES (2, 1, 7) USING TIMESTAMP 100");
        flush();
        execute("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 15 AND TTL 100000");
        execute("INSERT INTO %s (k, c, r) VALUES (3, 1, 7) USING TIMESTAMP 50");
        flush();
        execute("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 20 AND TTL 1");
        flush();
        awaitMarkerExpiry();

        for (String restriction : RESTRICTIONS)
        {
            if (!restriction.startsWith(" WHERE k = 1"))
                continue;
            assertRows(execute("SELECT k, c, x FROM %s" + restriction), row(1, 1, null));
            assertRows(execute("SELECT k, c, r FROM %s" + restriction), row(1, 1, 7));
        }
    }

    /**
     * A row whose older cell is in four sstables, each with a newer row marker without TTL, superseded by an expired
     * row marker, is read again once by a read by name.
     */
    @Test
    public void testRowWithSupersededMarkersReadAgainOnce() throws Throwable
    {
        createTable(TABLE);
        disableCompaction();
        for (int sstable = 1; sstable <= 4; sstable++)
        {
            execute("INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP ?", 2L * sstable);
            execute("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP ?", 2L * sstable + 1);
            execute("INSERT INTO %s (k, c, r) VALUES (2, 1, 7) USING TIMESTAMP ?", 2L * sstable);
            flush();
        }
        execute(EXPIRING_MARKER.replace("TIMESTAMP 10", "TIMESTAMP 100"));
        awaitMarkerExpiry();

        var reads = getCurrentColumnFamilyStore().metric.sstablesPerReadHistogram.tableOrKeyspaceHistogram();
        long before = reads.getCount();
        assertRows(execute("SELECT k, c, x FROM %s WHERE k = 2 AND c = 1"), row(2, 1, null));
        long oneRead = reads.getCount() - before;

        before = reads.getCount();
        assertRows(execute("SELECT k, c, x FROM %s WHERE k = 1 AND c = 1"), row(1, 1, null));
        assertEquals(oneRead + 1, reads.getCount() - before);
    }

    /**
     * Reading a row again doesn't change the digest of the repaired data: the repaired sstable has the older cell and
     * a newer row marker without TTL, and an expired row marker is added in the memtable.
     */
    @Test
    public void testReadAgainKeepsRepairedDataDigest() throws Throwable
    {
        createTable(TABLE);
        disableCompaction();
        execute("INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1");
        execute("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 10");
        execute("INSERT INTO %s (k, c, r, x) VALUES (1, 2, 8, 8) USING TIMESTAMP 10");
        flush();
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, 1, null, false);
            sstable.reloadSSTableMetadata();
        }

        String[] queries = { "SELECT k, c, x FROM %s WHERE k = 1", "SELECT k, c, x FROM %s" };
        List<ByteBuffer> digests = new ArrayList<>();
        for (String query : queries)
            digests.add(repairedDataDigest(query));

        execute(EXPIRING_MARKER.replace("TIMESTAMP 10", "TIMESTAMP 20"));
        awaitMarkerExpiry();

        for (int i = 0; i < queries.length; i++)
        {
            assertRows(execute(queries[i]), row(1, 1, null), row(1, 2, 8));
            assertEquals(queries[i], digests.get(i), repairedDataDigest(queries[i]));
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
     * A replica drops the cells of the columns that are fetched but not queried and older than the row marker of the
     * same source when that marker has no TTL, and returns them with their values skipped when it has one, live or
     * expired, as another source or replica can have a newer row marker that has expired.
     */
    @Test
    public void testOlderNotQueriedCellsDroppedOnlyUnderMarkerWithoutTTL() throws Throwable
    {
        for (String marker : new String[]{ LIVE_MARKER, LIVE_EXPIRING_MARKER, EXPIRING_MARKER })
        {
            Map<Storage, String> tables = writeRows(marker, "INSERT INTO %s (k, c, r) VALUES (1, 1, 7) USING TIMESTAMP 1");
            for (Storage storage : Storage.SAME_SOURCE)
            {
                String table = tables.get(storage);
                ColumnMetadata r = Schema.instance.getTableMetadata(KEYSPACE, table).getColumn(ColumnIdentifier.getInterned("r", false));
                for (String restriction : RESTRICTIONS)
                {
                    String query = String.format("SELECT k, c, x FROM %s" + restriction, KEYSPACE + '.' + table);
                    String message = marker + ", " + storage + ": " + query;
                    int rows = 0;
                    for (Partition partition : Util.getAllUnfiltered(parseReadCommand(query)))
                    {
                        for (Row row : partition.rows())
                        {
                            Cell<?> cell = row.getCell(r);
                            if (marker.equals(LIVE_MARKER))
                            {
                                assertNull(message, cell);
                            }
                            else
                            {
                                assertNotNull(message, cell);
                                assertEquals(message, 0, cell.buffer().remaining());
                            }
                            rows++;
                        }
                    }
                    assertEquals(message, 1, rows);
                }
            }
        }
    }

    /**
     * Whether a row marker with a TTL is live or has expired, a replica returns the tombstones of the columns that are
     * fetched but not queried and older than the marker, as they can shadow the cells of another replica.
     */
    @Test
    public void testOlderNotQueriedTombstonesKept() throws Throwable
    {
        for (String marker : new String[]{ EXPIRING_MARKER, LIVE_EXPIRING_MARKER })
        {
            Map<Storage, String> tables = writeRows(marker,
                                                    "INSERT INTO %s (k, c, r, m) VALUES (1, 1, 7, {'a': 1, 'b': 2}) USING TIMESTAMP 1",
                                                    "DELETE r, m['a'] FROM %s USING TIMESTAMP 2 WHERE k = 1 AND c = 1");

            for (Map.Entry<Storage, String> table : tables.entrySet())
            {
                // compaction purges the tombstones
                if (table.getKey() == Storage.PURGED)
                    continue;

                for (String restriction : RESTRICTIONS)
                {
                    String query = String.format("SELECT k, c, x FROM %s" + restriction, KEYSPACE + '.' + table.getValue());
                    ColumnMetadata r = Schema.instance.getTableMetadata(KEYSPACE, table.getValue()).getColumn(ColumnIdentifier.getInterned("r", false));
                    int rows = 0;
                    for (Partition partition : Util.getAllUnfiltered(parseReadCommand(query)))
                    {
                        for (Row row : partition.rows())
                        {
                            Cell<?> cell = row.getCell(r);
                            assertTrue(table.getKey() + ": " + query, cell != null && !cell.isLive(FBUtilities.nowInSeconds()));
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
     * A read by name of a deleted partition returns the same digest whether the partition deletion is in a memtable or
     * in an sstable that the read doesn't intersect.
     */
    @Test
    public void testNamesReadDigestOfDeletedPartitionMatchesAcrossStorages() throws Throwable
    {
        createTable(TABLE);
        disableCompaction();
        execute("DELETE FROM %s USING TIMESTAMP 10 WHERE k = 1");
        long nowInSec = FBUtilities.nowInSeconds();
        String query = formatQuery("SELECT k, c, x FROM %s WHERE k = 1 AND c = 1");
        String inMemtable = digest(query, nowInSec);
        flush();
        assertEquals(inMemtable, digest(query, nowInSec));
    }

    /**
     * A replica returns the same digest wherever the cells and the row marker are stored, with every memtable
     * implementation, when the not queried cells are older than a row marker with a TTL, live or expired, or are
     * tombstones.
     */
    @Test
    public void testDigestsMatchAcrossStoragesAndMemtables() throws Throwable
    {
        String olderWrite = "INSERT INTO %s (k, c, r, l, m) VALUES (1, 1, 7, {7}, {'a': 1, 'b': 2}) USING TIMESTAMP 1";
        String[][] scenarios = { { LIVE_EXPIRING_MARKER, olderWrite },
                                 { EXPIRING_MARKER, olderWrite },
                                 { LIVE_EXPIRING_MARKER, olderWrite, "DELETE r, m['b'] FROM %s USING TIMESTAMP 2 WHERE k = 1 AND c = 1" } };

        // The digests include the local deletion and expiration times, so every write is done at the same time.
        long writeNowInSec = FBUtilities.nowInSeconds();
        List<Map<String, String>> tables = new ArrayList<>();
        for (String[] scenario : scenarios)
        {
            Map<String, String> scenarioTables = new LinkedHashMap<>();
            Map<String, Storage> storages = new LinkedHashMap<>();
            for (String memtable : MEMTABLES)
            {
                for (Storage storage : Storage.NOT_COMPACTED)
                {
                    String table = createTable(TABLE + withMemtable(memtable));
                    disableCompaction();
                    scenarioTables.put(memtable + ", " + storage, table);
                    storages.put(table, storage);
                }
            }

            writeAt(writeNowInSec, storages.keySet(), Arrays.copyOfRange(scenario, 1, scenario.length));
            for (Map.Entry<String, Storage> table : storages.entrySet())
            {
                if (table.getValue().flushOlderWrites)
                    flush(KEYSPACE, table.getKey());
            }
            writeAt(writeNowInSec, storages.keySet(), scenario[0]);
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

    private static void writeAt(long nowInSec, Iterable<String> tables, String... writes)
    {
        for (String table : tables)
        {
            for (String write : writes)
                QueryProcessor.executeInternalWithNowInSec(String.format(write, KEYSPACE + '.' + table), nowInSec);
        }
    }

    private ByteBuffer repairedDataDigest(String query)
    {
        ReadCommand command = parseReadCommand(query);
        try (ReadExecutionController controller = command.executionController(true);
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            ReadResponse.createDataResponse(partitions, command, controller.getRepairedDataInfo());
            return controller.getRepairedDataDigest();
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

    /**
     * For every {@link Storage}, creates a table, applies {@code olderWrites} then {@code markerWrite}, flushing
     * and compacting according to the storage, and returns the tables, once the TTL has expired if the marker is
     * {@link #EXPIRING_MARKER}.
     */
    private Map<Storage, String> writeRows(String markerWrite, String... olderWrites) throws Throwable
    {
        Map<Storage, String> tables = new EnumMap<>(Storage.class);
        for (Storage storage : Storage.values())
            tables.put(storage, createRowTable("default", storage, markerWrite, olderWrites));

        if (markerWrite.equals(EXPIRING_MARKER))
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
        String table = createTable(TABLE + withMemtable(memtable) + (storage == Storage.PURGED ? " AND gc_grace_seconds = 0" : ""));
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

    private static String withMemtable(String memtable)
    {
        return " WITH memtable = '" + memtable + '\'';
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
