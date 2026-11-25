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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A filter with a disjunction of static and other columns returns a partition whose static row doesn't satisfy it on
 * its own only from its first live row. A read that needs reconciliation holds the range tombstone markers and
 * deleted rows before that row, up to the tombstone failure threshold; a read that doesn't holds none. SAI counts the
 * deletions it holds or drops against the tombstone thresholds and in the metrics.
 */
public class StaticDisjunctionHeldDeletionsTest extends SAITester
{
    private static final int DELETIONS = 2000;

    @Test
    public void testWithoutIndexes() throws Throwable
    {
        test(false);
    }

    @Test
    public void testWithIndexes() throws Throwable
    {
        test(true);
    }

    private void test(boolean indexed) throws Throwable
    {
        Injections.Counter held = Injections.newCounter("held_" + indexed)
                                            .add(newInvokePoint().onClass("org.apache.cassandra.db.rows.UnfilteredRowIterators")
                                                                 .onMethod("withLiveRowsOrNullUnsafe")
                                                                 .at("INVOKE List.add"))
                                            .build();
        Injections.inject(held);

        createTable("CREATE TABLE %s (pk int, ck int, v int, r set<int>, st int static, PRIMARY KEY (pk, ck))");
        if (indexed)
            for (String column : new String[]{ "ck", "v", "r", "st" })
                createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
        disableCompaction();
        // 1: deleted rows before the live match; 2: range tombstones before it; both with a static row that doesn't match
        execute("INSERT INTO %s (pk, st) VALUES (1, 0)");
        execute("INSERT INTO %s (pk, st) VALUES (2, 0)");
        for (int i = 0; i < DELETIONS; i++)
        {
            execute("DELETE FROM %s WHERE pk = 1 AND ck = ?", i);
            execute("DELETE FROM %s WHERE pk = 2 AND ck >= ? AND ck < ?", 2 * i, 2 * i + 1);
        }
        for (int pk = 1; pk <= 2; pk++)
            execute("INSERT INTO %s (pk, ck, v, r) VALUES (?, ?, 1, {1})", pk, 2 * DELETIONS + 1);

        String[] queries = { "SELECT pk, ck FROM %s WHERE pk = 1 AND (st = 5 OR ck >= 0) ALLOW FILTERING",
                             "SELECT pk, ck FROM %s WHERE pk = 1 AND (st = 5 OR r NOT CONTAINS 3) ALLOW FILTERING",
                             "SELECT pk, ck FROM %s WHERE pk = 2 AND (st = 5 OR v = 1) ALLOW FILTERING",
                             "SELECT pk, ck FROM %s WHERE st = 5 OR v = 1 ALLOW FILTERING" };
        for (String source : new String[]{ "memtable", "sstable" })
        {
            if (source.equals("sstable"))
                flush();
            for (String query : queries)
            {
                held.reset();
                int rows = execute(query).size();
                assertEquals(source + ": " + query, query.contains("pk =") ? 1 : 2, rows);
                assertEquals(source + ", held: " + query, 0, held.get());
            }
        }
    }

    /**
     * With and without reconciliation, a read fails once SAI holds or drops more deletions than the tombstone failure
     * threshold, with and without indexes, and counts one failure and no warning. A read that holds deletions stops
     * reading the partition once it holds as many as the failure threshold: it doesn't hold all of them first.
     */
    @Test
    public void testHeldDeletionsAreBoundedByTheTombstoneFailureThreshold() throws Throwable
    {
        Injections.Counter held = Injections.newCounter("held_bounded")
                                            .add(newInvokePoint().onClass("org.apache.cassandra.db.rows.UnfilteredRowIterators")
                                                                 .onMethod("withLiveRowsOrNullUnsafe")
                                                                 .at("INVOKE List.add"))
                                            .build();
        Injections.inject(held);

        withTombstoneThresholds(() -> {
            for (boolean indexed : new boolean[]{ false, true })
            {
                createTable("CREATE TABLE %s (pk int, ck int, v int, r set<int>, st int static, PRIMARY KEY (pk, ck))");
                if (indexed)
                    for (String column : new String[]{ "ck", "v", "r", "st" })
                        createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
                disableCompaction();
                // 1: only deleted rows; 2: deleted rows, then a live row; both with a static row that doesn't match
                for (int pk = 1; pk <= 2; pk++)
                {
                    execute("INSERT INTO %s (pk, st) VALUES (?, 0)", pk);
                    for (int ck = 0; ck < 3 * 1000; ck++)
                        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", pk, ck);
                }
                execute("INSERT INTO %s (pk, ck, v, r) VALUES (2, ?, 1, {1})", 3 * 1000 + 1);
                flush();

                for (String query : new String[]{ "SELECT pk, ck FROM %s WHERE pk = 1 AND (st = 5 OR ck >= 0) ALLOW FILTERING",
                                                  "SELECT pk, ck FROM %s WHERE pk = 1 AND (st = 5 OR r NOT CONTAINS 3) ALLOW FILTERING",
                                                  "SELECT pk, ck FROM %s WHERE pk = 2 AND (st = 5 OR ck >= 0) ALLOW FILTERING" })
                {
                    String label = (indexed ? "indexed: " : "plain: ") + query;
                    for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.ALL, ConsistencyLevel.ONE })
                    {
                        held.reset();
                        assertFailsOnTombstones(label + " at " + consistency, query, consistency);
                        // the failure threshold is 1000, and the 1001st held deletion fails the read
                        assertTrue(label + " at " + consistency + ", held " + held.get(), held.get() <= 1001);
                        if (consistency == ConsistencyLevel.ALL)
                            assertTrue(label + " at ALL, held nothing", held.get() > 0);
                    }
                }
            }
        });
    }

    /**
     * Below the failure threshold, a read whose deletions SAI holds and drops, or drops without holding them, warns
     * about them and counts them in the tombstone metrics as the read without indexes does, at ALL and at ONE (RF 2,
     * executed locally). Each partition has 500 deleted rows, then a live row or nothing, and a static row that
     * doesn't match.
     */
    @Test
    public void testDroppedDeletionsAreWarnedAboutAndCounted() throws Throwable
    {
        withTombstoneThresholds(() -> {
            for (boolean indexed : new boolean[]{ false, true })
            {
                createTable("CREATE TABLE %s (pk int, ck int, v int, r set<int>, st int static, PRIMARY KEY (pk, ck))");
                if (indexed)
                    for (String column : new String[]{ "ck", "v", "r", "st" })
                        createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
                disableCompaction();
                // 1: only deleted rows; 2: deleted rows, then a live row
                for (int pk = 1; pk <= 2; pk++)
                {
                    execute("INSERT INTO %s (pk, st) VALUES (?, 0)", pk);
                    for (int ck = 0; ck < 500; ck++)
                        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", pk, ck);
                }
                execute("INSERT INTO %s (pk, ck, v, r) VALUES (2, 1000, 1, {1})");
                flush();

                TableMetrics metrics = getCurrentColumnFamilyStore().metric;
                for (String query : new String[]{ "SELECT pk, ck FROM %s WHERE pk = 1 AND (st = 5 OR ck >= 0) ALLOW FILTERING",
                                                  "SELECT pk, ck FROM %s WHERE pk = 1 AND (st = 5 OR r NOT CONTAINS 3) ALLOW FILTERING",
                                                  "SELECT pk, ck FROM %s WHERE pk = 2 AND (st = 5 OR ck >= 0) ALLOW FILTERING" })
                {
                    for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.ALL, ConsistencyLevel.ONE })
                    {
                        String label = (indexed ? "indexed: " : "plain: ") + query + " at " + consistency;
                        long warnings = metrics.tombstoneWarnings.getCount();
                        long scanned = metrics.tombstoneScannedCounter.getCount();
                        assertEquals(label, query.contains("pk = 2") ? 1 : 0, executeLocally(query, consistency));
                        assertEquals(label + ", tombstone warnings", 1, metrics.tombstoneWarnings.getCount() - warnings);
                        assertEquals(label + ", tombstones scanned", 500, metrics.tombstoneScannedCounter.getCount() - scanned);
                    }
                }
            }
        });
    }

    /**
     * The tombstones the read purges (older than gc_grace_seconds) don't count against the tombstone thresholds or in
     * the metrics, with indexes as without.
     */
    @Test
    public void testPurgeableTombstonesAreNotCounted() throws Throwable
    {
        withTombstoneThresholds(() -> {
            for (boolean indexed : new boolean[]{ false, true })
            {
                createTable("CREATE TABLE %s (pk int, ck int, v int, r set<int>, st int static, PRIMARY KEY (pk, ck)) " +
                            "WITH gc_grace_seconds = 0");
                if (indexed)
                    for (String column : new String[]{ "ck", "v", "r", "st" })
                        createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
                disableCompaction();
                for (int pk = 1; pk <= 3; pk++)
                    execute("INSERT INTO %s (pk, st) VALUES (?, 0)", pk);
                for (int ck = 0; ck < 1001; ck++)
                {
                    execute("DELETE FROM %s WHERE pk = 1 AND ck = ?", ck);
                    execute("INSERT INTO %s (pk, ck, v, r) VALUES (2, ?, 1, {1}) USING TTL 1", ck);
                    if (ck < 500)
                        execute("DELETE FROM %s WHERE pk = 3 AND ck = ?", ck);
                }
                for (int pk = 1; pk <= 2; pk++)
                    execute("INSERT INTO %s (pk, ck, v, r) VALUES (?, 2000, 1, {1})", pk);
                // the deletions and the expired cells are purgeable once they are older than gc_grace_seconds, in seconds
                Thread.sleep(2500);

                TableMetrics metrics = getCurrentColumnFamilyStore().metric;
                for (String source : new String[]{ "memtable", "sstable" })
                {
                    if (source.equals("sstable"))
                        flush();
                    for (int pk = 1; pk <= 3; pk++)
                    {
                        for (String query : new String[]{ "SELECT pk, ck FROM %s WHERE pk = " + pk + " AND (st = 5 OR ck >= 0) ALLOW FILTERING",
                                                          "SELECT pk, ck FROM %s WHERE pk = " + pk + " AND (st = 5 OR r NOT CONTAINS 3) ALLOW FILTERING" })
                        {
                            for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.ALL, ConsistencyLevel.ONE })
                            {
                                String label = (indexed ? "indexed, " : "plain, ") + source + ": " + query + " at " + consistency;
                                long warnings = metrics.tombstoneWarnings.getCount();
                                long failures = metrics.tombstoneFailures.getCount();
                                long scanned = metrics.tombstoneScannedCounter.getCount();
                                int rows;
                                try
                                {
                                    rows = executeLocally(query, consistency);
                                }
                                catch (Throwable t)
                                {
                                    throw new AssertionError(label + " failed", t);
                                }
                                assertEquals(label, pk == 3 ? 0 : 1, rows);
                                assertEquals(label + ", tombstone failures", 0, metrics.tombstoneFailures.getCount() - failures);
                                assertEquals(label + ", tombstone warnings", 0, metrics.tombstoneWarnings.getCount() - warnings);
                                assertEquals(label + ", tombstones scanned", 0, metrics.tombstoneScannedCounter.getCount() - scanned);
                            }
                        }
                    }
                }
            }
        });
    }

    /**
     * The deletions that SAI holds and drops are counted with the read's own tombstones, as the read without indexes
     * counts them: each dead cell of a row counts, and the read warns and fails on one total, with one histogram
     * sample, and reports the partition in the top partitions by tombstones.
     */
    @Test
    public void testDroppedDeletionsCountWithTheRead() throws Throwable
    {
        withTombstoneThresholds(() -> {
            for (boolean indexed : new boolean[]{ false, true })
            {
                String label = indexed ? "indexed: " : "plain: ";

                // dead cells: partition 1 with 400 rows, partition 2 with 600
                createDeletionsTable(indexed);
                for (int pk = 1; pk <= 2; pk++)
                {
                    execute("INSERT INTO %s (pk, st) VALUES (?, 0)", pk);
                    for (int ck = 0; ck < (pk == 1 ? 400 : 600); ck++)
                        execute("UPDATE %s SET v = null, w = null WHERE pk = ? AND ck = ?", pk, ck);
                }
                flush();
                TableMetrics metrics = getCurrentColumnFamilyStore().metric;
                for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.ALL, ConsistencyLevel.ONE })
                {
                    String query = "SELECT pk, ck FROM %s WHERE pk = 1 AND (st = 5 OR ck >= 0) ALLOW FILTERING";
                    metrics.topReadPartitionTombstoneCount.beginSampling(10, 60_000);
                    assertWarnsOnTombstones(label + query + " at " + consistency, query, consistency, 0, 800);
                    Sampler.samplerExecutor.submit(() -> {}).get();
                    List<Sampler.Sample<ByteBuffer>> samples = metrics.topReadPartitionTombstoneCount.finishSampling(10);
                    assertEquals(label + query + " at " + consistency + ", top partitions by tombstones",
                                 "[1: 800]", samples.stream()
                                                    .map(sample -> Int32Type.instance.compose(sample.value) + ": " + sample.count)
                                                    .collect(Collectors.toList())
                                                    .toString());

                    query = "SELECT pk, ck FROM %s WHERE pk = 2 AND (st = 5 OR ck >= 0) ALLOW FILTERING";
                    assertFailsOnTombstones(label + query + " at " + consistency, query, consistency);
                }

                // one total over a dropped partition and a returned one, of 600 and of 60 deleted rows each
                for (int rows : new int[]{ 600, 60 })
                {
                    createDeletionsTable(indexed);
                    execute("INSERT INTO %s (pk, st) VALUES (1, 0)");
                    execute("INSERT INTO %s (pk, st) VALUES (2, 5)");
                    for (int pk = 1; pk <= 2; pk++)
                        for (int ck = 0; ck < rows; ck++)
                            execute("DELETE FROM %s WHERE pk = ? AND ck = ?", pk, ck);
                    flush();
                    String query = "SELECT pk, ck FROM %s WHERE st = 5 OR ck >= 0 ALLOW FILTERING";
                    for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.ALL, ConsistencyLevel.ONE })
                    {
                        if (rows == 600)
                            assertFailsOnTombstones(label + rows + ": " + query + " at " + consistency, query, consistency);
                        else
                            // the static row of partition 2
                            assertWarnsOnTombstones(label + rows + ": " + query + " at " + consistency, query, consistency, 1, 120);
                    }
                }
            }
        });
    }

    private void createDeletionsTable(boolean indexed)
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, w int, r set<int>, st int static, PRIMARY KEY (pk, ck))");
        if (indexed)
            for (String column : new String[]{ "ck", "v", "w", "r", "st" })
                createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
        disableCompaction();
    }

    private void assertWarnsOnTombstones(String label, String query, ConsistencyLevel consistency, int rows, long tombstones)
    {
        TableMetrics metrics = getCurrentColumnFamilyStore().metric;
        long warnings = metrics.tombstoneWarnings.getCount();
        long failures = metrics.tombstoneFailures.getCount();
        long scanned = metrics.tombstoneScannedCounter.getCount();
        assertEquals(label, rows, executeLocally(query, consistency));
        assertEquals(label + ", tombstone failures", 0, metrics.tombstoneFailures.getCount() - failures);
        assertEquals(label + ", tombstone warnings", 1, metrics.tombstoneWarnings.getCount() - warnings);
        assertEquals(label + ", tombstones scanned", tombstones, metrics.tombstoneScannedCounter.getCount() - scanned);
    }

    private void assertFailsOnTombstones(String label, String query, ConsistencyLevel consistency)
    {
        TableMetrics metrics = getCurrentColumnFamilyStore().metric;
        long warnings = metrics.tombstoneWarnings.getCount();
        long failures = metrics.tombstoneFailures.getCount();
        int rows;
        try
        {
            rows = executeLocally(query, consistency);
        }
        catch (Throwable t)
        {
            assertTrue(label + " failed with " + t, Throwables.isCausedBy(t, TombstoneOverwhelmingException.class));
            assertEquals(label + ", tombstone failures", 1, metrics.tombstoneFailures.getCount() - failures);
            assertEquals(label + ", tombstone warnings", 0, metrics.tombstoneWarnings.getCount() - warnings);
            return;
        }
        fail(label + " returned " + rows + " rows instead of failing on tombstones");
    }

    private int executeLocally(String query, ConsistencyLevel consistency)
    {
        SelectStatement select = (SelectStatement) QueryProcessor.prepareInternal(formatQuery(query)).statement;
        return select.executeLocally(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(consistency, Collections.emptyList()))
                     .result.size();
    }

    private void withTombstoneThresholds(CheckedFunction test) throws Throwable
    {
        int warn = DatabaseDescriptor.getGuardrailsConfig().getTombstoneWarnThreshold();
        int failure = DatabaseDescriptor.getGuardrailsConfig().getTombstoneFailThreshold();
        execute("ALTER KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
        DatabaseDescriptor.getGuardrailsConfig().setTombstonesThreshold(100, 1000);
        try
        {
            test.apply();
        }
        finally
        {
            DatabaseDescriptor.getGuardrailsConfig().setTombstonesThreshold(warn, failure);
            execute("ALTER KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        }
    }
}
