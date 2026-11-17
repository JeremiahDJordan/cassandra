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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The SAI memtable indexes of a skip-list memtable must stay exact when writes to a partition race. The skip-list
 * memtables merge an update into a partition in a CAS loop, and a merge that loses the CAS to another writer is
 * retried. SAI removes the old values of a row on update and counts the occurrences of a value in the row, so the
 * events of a lost merge attempt, or of a retried one, would remove values the row still holds. SAI therefore requires
 * atomic updates, and the skip-list memtables apply its updates under the partition's lock, delivering SAI's events
 * for the merge they applied only.
 */
@RunWith(Parameterized.class)
public class SkipListAtomicUpdatesTest extends SAITester
{
    private static final long TIMEOUT_SECONDS = 60;

    /** The timestamp of the write to pause before its CAS, or Long.MIN_VALUE for none. */
    private static volatile long pausedTimestamp = Long.MIN_VALUE;
    private static volatile CountDownLatch paused = new CountDownLatch(1);
    private static volatile CountDownLatch resume = new CountDownLatch(1);

    private interface Write
    {
        void apply(int k) throws Throwable;
    }

    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> parameters()
    {
        // skiplist_sharded_locking serializes the writes of a shard anyway, so the race can't happen there; the stress
        // test runs it.
        return Arrays.asList(new Object[]{ "skiplist" }, new Object[]{ "skiplist_sharded" });
    }

    @BeforeClass
    public static void allowShardedMemtables()
    {
        // The sharded memtable configurations cannot be written to the schema in the CC4-compatible storage
        // compatibility modes (HCD_1, the test default, and CASSANDRA_4), see MemtableParams.asSchemaValueMap
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
    }

    /**
     * Called by the injected rule before AtomicBTreePartition tries the CAS that applies a merged update. Pauses the
     * first merge of the write to pause (writes run on the mutation stage, so it is told apart by its timestamp).
     */
    public static void beforeCas(BTreePartitionUpdate update)
    {
        long timestamp = pausedTimestamp;
        // a collection set to null has a deletion at the timestamp minus one, and no cell
        if (timestamp == Long.MIN_VALUE || (update.maxTimestamp() != timestamp && update.maxTimestamp() != timestamp - 1))
            return;
        pausedTimestamp = Long.MIN_VALUE;
        paused.countDown();
        try
        {
            resume.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    @Before
    public void injectPause() throws Throwable
    {
        Injection pause = Injections.newCustom("pause_before_cas")
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(AtomicBTreePartition.class, "Updater")
                                                           .onMethod("tryUpdateData")
                                                           .atInvoke("compareAndSet"))
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(SkipListAtomicUpdatesTest.class.getName() + ".beforeCas($1)"))
                                    .build();
        Injections.inject(pause);
    }

    /**
     * Runs the first write, whose timestamp must be the given one, until it pauses before its CAS, then the second
     * write, and resumes the first once the second has either completed or is waiting for the partition's lock.
     *
     * @return true if the second write completed while the first was paused, false if it waited for the lock
     */
    private boolean racePausedWrite(long timestamp, String pausedWrite, String otherWrite, Object... otherArgs)
    throws Throwable
    {
        paused = new CountDownLatch(1);
        resume = new CountDownLatch(1);
        pausedTimestamp = timestamp;
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread first = new Thread(() -> run(pausedWrite, new Object[0], failure), "skiplist-paused-writer");
        Thread second = new Thread(() -> run(otherWrite, otherArgs, failure), "skiplist-other-writer");
        try
        {
            first.start();
            assertTrue("the first write didn't reach its CAS", paused.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            second.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (second.isAlive() && !aWriteWaitsForAPartitionLock())
            {
                if (System.nanoTime() > deadline)
                    fail("the second write neither completed nor waited for the partition's lock");
                Thread.yield();
            }
            return !second.isAlive();
        }
        finally
        {
            pausedTimestamp = Long.MIN_VALUE;
            resume.countDown();
            first.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            second.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertNull(failure.get());
        }
    }

    private void run(String query, Object[] args, AtomicReference<Throwable> failure)
    {
        try
        {
            execute(query, args);
        }
        catch (Throwable t)
        {
            failure.compareAndSet(null, t);
        }
    }

    private static boolean aWriteWaitsForAPartitionLock()
    {
        for (Map.Entry<Thread, StackTraceElement[]> thread : Thread.getAllStackTraces().entrySet())
        {
            StackTraceElement[] stack = thread.getValue();
            if (thread.getKey().getState() == Thread.State.BLOCKED
                && stack.length > 0
                && stack[0].getClassName().startsWith(AtomicBTreePartition.class.getName()))
                return true;
        }
        return false;
    }

    private Set<Integer> cks(String query, Object... args)
    {
        Set<Integer> cks = new TreeSet<>();
        for (UntypedResultSet.Row row : execute(query, args))
            cks.add(row.getInt("ck"));
        return cks;
    }

    /**
     * A single value: v = 1 at 10, the first writer sets 2 at 20, the second sets 1 again at 30. The first writer's
     * lost merge would remove 1 from the index, and its retry makes no change. A legacy index on another column of the
     * same writes must keep working: it still gets the events of every merge attempt.
     */
    @Test
    public void testSingleValue() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(w) USING 'legacy_local_table'");
        execute("UPDATE %s USING TIMESTAMP 10 SET v = 1, w = 1 WHERE pk = 0 AND ck = 0");

        racePausedWrite(20, "UPDATE %s USING TIMESTAMP 20 SET v = 2, w = 2 WHERE pk = 0 AND ck = 0",
                        "UPDATE %s USING TIMESTAMP 30 SET v = 1, w = 1 WHERE pk = 0 AND ck = 0");

        assertRows(execute("SELECT v, w FROM %s WHERE pk = 0 AND ck = 0"), row(1, 1));
        for (String source : new String[]{ "memtable", "sstable" })
        {
            if (source.equals("sstable"))
                flush();
            assertEquals(source, Set.of(0), cks("SELECT ck FROM %s WHERE v = 1"));
            assertEquals(source, Set.of(), cks("SELECT ck FROM %s WHERE v = 2"));
            assertEquals(source, Set.of(0), cks("SELECT ck FROM %s WHERE w = 1"));
            assertEquals(source, Set.of(), cks("SELECT ck FROM %s WHERE w = 2"));
        }
    }

    /**
     * A set of a row written by UPDATE (no row marker): the first writer deletes it at 20, which reaches the index as
     * Indexer#removeRowData, and the second adds the same element at 30. The row ends with the element.
     */
    @Test
    public void testCollectionDeletionRacingAddition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, s set<int>, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        createIndex("CREATE INDEX ON %s(s) USING 'sai'");
        execute("UPDATE %s USING TIMESTAMP 10 SET v = 7, s = {1} WHERE pk = 0 AND ck = 0");

        racePausedWrite(20, "UPDATE %s USING TIMESTAMP 20 SET s = null WHERE pk = 0 AND ck = 0",
                        "UPDATE %s USING TIMESTAMP 30 SET s = s + {1} WHERE pk = 0 AND ck = 0");

        assertRows(execute("SELECT s FROM %s WHERE pk = 0 AND ck = 0"), row(Set.of(1)));
        for (String source : new String[]{ "memtable", "sstable" })
        {
            if (source.equals("sstable"))
                flush();
            assertEquals(source, Set.of(0), cks("SELECT ck FROM %s WHERE s CONTAINS 1"));
            assertEquals(source, Set.of(), cks("SELECT ck FROM %s WHERE s NOT CONTAINS 1"));
        }
    }

    /**
     * An analyzed text: 'apple pie crust' at 10, the first writer sets 'cherry' at 20, the second sets 'apple pie crust'
     * again at 30. The first writer's lost merge would remove the terms of the row's text.
     */
    @Test
    public void testAnalyzedText() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, t text, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        execute("UPDATE %s USING TIMESTAMP 10 SET t = 'apple pie crust' WHERE pk = 0 AND ck = 0");
        execute("UPDATE %s USING TIMESTAMP 10 SET t = 'apple' WHERE pk = 0 AND ck = 1");

        racePausedWrite(20, "UPDATE %s USING TIMESTAMP 20 SET t = 'cherry' WHERE pk = 0 AND ck = 0",
                        "UPDATE %s USING TIMESTAMP 30 SET t = 'apple pie crust' WHERE pk = 0 AND ck = 0");

        assertRows(execute("SELECT t FROM %s WHERE pk = 0 AND ck = 0"), row("apple pie crust"));
        for (String source : new String[]{ "memtable", "sstable" })
        {
            if (source.equals("sstable"))
                flush();
            assertEquals(source, Set.of(0), cks("SELECT ck FROM %s WHERE t : 'pie'"));
            assertEquals(source, Set.of(), cks("SELECT ck FROM %s WHERE t : 'cherry'"));
        }
    }

    /**
     * The writes of a table with SAI wait for the partition's lock while another write of the partition is applied,
     * and the SAI indexes don't see the merge that loses. Other indexes of the table keep getting the events of every
     * merge attempt as the memtable makes them.
     */
    @Test
    public void testOtherIndexesSeeEveryMergeAttempt() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        String stubName = createIndex("CREATE CUSTOM INDEX ON %s(w) USING '" + StubIndex.class.getName() + "'");
        StubIndex stub = (StubIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(stubName);
        execute("UPDATE %s USING TIMESTAMP 10 SET v = 1, w = 1 WHERE pk = 0 AND ck = 0");
        stub.reset();

        boolean secondCompletedFirst =
            racePausedWrite(30, "UPDATE %s USING TIMESTAMP 30 SET v = 3, w = 3 WHERE pk = 0 AND ck = 0",
                            "UPDATE %s USING TIMESTAMP 20 SET v = 2, w = 2 WHERE pk = 0 AND ck = 0");
        assertFalse("the second write didn't wait for the partition's lock", secondCompletedFirst);
        // The first writer's merge, then the second writer's, which loses on timestamp and changes nothing.
        assertEquals(1, stub.rowsUpdated.size());
        assertEquals(Set.of(0), cks("SELECT ck FROM %s WHERE v = 3"));
        assertEquals(Set.of(), cks("SELECT ck FROM %s WHERE v = 1"));
        assertEquals(Set.of(), cks("SELECT ck FROM %s WHERE v = 2"));
    }

    /**
     * Without SAI, nothing changes: the writes of a partition don't wait for each other, and the indexes get the events
     * of every merge attempt, including those of the attempt that loses.
     */
    @Test
    public void testNoLockWithoutSai() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        String stubName = createIndex("CREATE CUSTOM INDEX ON %s(w) USING '" + StubIndex.class.getName() + "'");
        StubIndex stub = (StubIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(stubName);
        execute("UPDATE %s USING TIMESTAMP 10 SET v = 1, w = 1 WHERE pk = 0 AND ck = 0");
        stub.reset();

        boolean secondCompletedFirst =
            racePausedWrite(20, "UPDATE %s USING TIMESTAMP 20 SET v = 2, w = 2 WHERE pk = 0 AND ck = 0",
                            "UPDATE %s USING TIMESTAMP 30 SET v = 3, w = 3 WHERE pk = 0 AND ck = 0");
        assertTrue("the second write waited for the first", secondCompletedFirst);
        // The first writer's lost merge, the second writer's merge, and the first writer's retry, which loses on
        // timestamp and changes nothing.
        assertEquals(2, stub.rowsUpdated.size());
        assertRows(execute("SELECT v, w FROM %s WHERE pk = 0 AND ck = 0"), row(3, 3));
    }

    /**
     * A write with SAI (which takes the partition's lock) is paused before its CAS, and a write without index
     * transaction (as ColumnFamilyStore.apply(update, ctx, false) makes, or a write that started before the index was
     * registered) changes the partition first, without waiting for the lock. The first write then loses its CAS under
     * the lock and merges again: SAI must see the merge that was applied only, and the other index both.
     */
    @Test
    public void testMergeLostUnderTheLock() throws Throwable
    {
        Injections.Counter lost = Injections.newCounter("attempt_lost")
                                            .add(InvokePointBuilder.newInvokePoint()
                                                                   .onClass("org.apache.cassandra.index.SecondaryIndexManager$WriteTimeTransaction")
                                                                   .onMethod("onAttemptLost"))
                                            .build();
        Injections.Counter applied = Injections.newCounter("applied")
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass("org.apache.cassandra.index.SecondaryIndexManager$WriteTimeTransaction")
                                                                      .onMethod("onApplied"))
                                               .build();
        Injections.inject(lost, applied);

        // The row holds v = 1 at 10, and the paused write sets v = 2 at 20. The other write sets, without index events:
        // v = 1 at 30, so that the retry changes nothing and the index keeps 1; w, so that the retry repeats the change;
        // or v = 3 at 15, older, so that the retry replaces a value SAI never heard of.
        for (int scenario = 0; scenario < 3; scenario++)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            createIndex("CREATE INDEX ON %s(v) USING 'sai'");
            String stubName = createIndex("CREATE CUSTOM INDEX ON %s(w) USING '" + StubIndex.class.getName() + "'");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            StubIndex stub = (StubIndex) cfs.indexManager.getIndexByName(stubName);
            execute("UPDATE %s USING TIMESTAMP 10 SET v = 1 WHERE pk = 0 AND ck = 0");
            stub.reset();
            lost.reset();
            applied.reset();

            paused = new CountDownLatch(1);
            resume = new CountDownLatch(1);
            pausedTimestamp = 20;
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread first = new Thread(() -> run("UPDATE %s USING TIMESTAMP 20 SET v = 2 WHERE pk = 0 AND ck = 0", new Object[0], failure));
            try
            {
                first.start();
                assertTrue("the first write didn't reach its CAS", paused.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                if (scenario == 0)
                    writeWithoutIndexes(cfs, 30, "v", 1);
                else if (scenario == 1)
                    writeWithoutIndexes(cfs, 30, "w", 5);
                else
                    writeWithoutIndexes(cfs, 15, "v", 3);
            }
            finally
            {
                pausedTimestamp = Long.MIN_VALUE;
                resume.countDown();
                first.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            }
            assertNull(failure.get());

            String context = memtable + ", scenario " + scenario;
            assertEquals(context, 1, lost.get());
            assertEquals(context, 1, applied.get());
            assertEquals(context + ": the other index sees both merges", 2, stub.rowsUpdated.size() + stub.rowsInserted.size());
            int expected = scenario == 0 ? 1 : 2;
            assertRows(execute("SELECT v FROM %s WHERE pk = 0 AND ck = 0"), row(expected));
            for (String source : new String[]{ "memtable", "sstable" })
            {
                if (source.equals("sstable"))
                    flush();
                for (int value = 1; value <= 3; value++)
                    assertEquals(context + ", " + source + ", v = " + value,
                                 value == expected ? Set.of(0) : Set.of(), cks("SELECT ck FROM %s WHERE v = ?", value));
            }
        }
    }

    private static void writeWithoutIndexes(ColumnFamilyStore cfs, long timestamp, String column, Object value)
    {
        PartitionUpdate update = new RowUpdateBuilder(cfs.metadata(), timestamp, 0).clustering(0).add(column, value).buildUpdate();
        try (WriteContext ctx = cfs.keyspace.getWriteHandler().createContextForIndexing())
        {
            cfs.getWriteHandler().write(update, ctx, false);
        }
    }

    /**
     * A memtable that inherits the flag that it applies updates atomically on request, but merges without asking the
     * index transaction for it: SAI must still get the events, as on any other memtable, rather than none.
     */
    @Test
    public void testMemtableThatDoesNotAskForAtomicApply() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        Injections.Counter inserts = Injections.newCounter("sai_insert_row")
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass("org.apache.cassandra.index.sai.StorageAttachedIndex$UpdateIndexer")
                                                                      .onMethod("insertRow"))
                                               .build();
        Injections.inject(inserts);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable current = cfs.getCurrentMemtable();
        assertTrue(current.appliesUpdatesAtomicallyOnRequest());
        PartitionUpdate update = new RowUpdateBuilder(cfs.metadata(), 10, 0).clustering(1).add("v", 1).buildUpdate();
        try (WriteContext ctx = cfs.keyspace.getWriteHandler().createContextForIndexing())
        {
            // what such a memtable's put() would do: report the row and complete the transaction
            UpdateTransaction transaction = cfs.indexManager.newUpdateTransaction(update, ctx, FBUtilities.nowInSeconds(), current);
            transaction.start();
            transaction.onInserted(update.rows().iterator().next());
            transaction.commit();
        }
        assertEquals(1, inserts.get());
    }

    /**
     * Writers released together for each row, so that the skip-list memtable retries merges, and the index must find
     * every row that holds the value, in the memtable and after the flush. A bounded version of the race the
     * deterministic tests above force, on both kinds of memtable.
     */
    @Test
    public void testConcurrentWritersStress() throws Throwable
    {
        int rows = 300;
        String[] memtables = memtable.equals("skiplist") ? new String[]{ "skiplist", "skiplist_sharded_locking", "trie" }
                                                         : new String[]{ memtable };
        for (String mt : memtables)
        {
            // a single value: v = 1 at 10, 2 at 20 and 1 at 30 racing
            stressTable(mt);
            for (int k = 0; k < rows; k++)
                execute("UPDATE %s USING TIMESTAMP 10 SET v = 1 WHERE pk = 0 AND ck = ?", k);
            raceWriters(rows,
                        k -> execute("UPDATE %s USING TIMESTAMP 20 SET v = 2 WHERE pk = 0 AND ck = ?", k),
                        k -> execute("UPDATE %s USING TIMESTAMP 30 SET v = 1 WHERE pk = 0 AND ck = ?", k));
            assertIndexFindsEveryRow(mt + ", single value", rows, "v = 1", r -> r.has("v") && r.getInt("v") == 1);

            // a set, rows with and without a row marker: deleted at 20 racing the addition of the same element at 30
            for (String insert : new String[]{ "UPDATE %s USING TIMESTAMP 10 SET v = 7, s = {1} WHERE pk = 0 AND ck = ?",
                                               "INSERT INTO %s (pk, ck, v, s) VALUES (0, ?, 7, {1}) USING TIMESTAMP 10" })
            {
                stressTable(mt);
                for (int k = 0; k < rows; k++)
                    execute(insert, k);
                raceWriters(rows,
                            k -> execute("UPDATE %s USING TIMESTAMP 20 SET s = null WHERE pk = 0 AND ck = ?", k),
                            k -> execute("UPDATE %s USING TIMESTAMP 30 SET s = s + {1} WHERE pk = 0 AND ck = ?", k));
                assertIndexFindsEveryRow(mt + ", " + insert, rows, "s CONTAINS 1",
                                         r -> r.has("s") && r.getSet("s", Int32Type.instance).contains(1));
            }
        }
    }

    private void stressTable(String mt)
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, s set<int>, PRIMARY KEY (pk, ck)) WITH memtable = '" + mt + "'");
        disableCompaction();
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(s) USING 'sai'");
    }

    private void raceWriters(int rows, Write... writers) throws Throwable
    {
        AtomicInteger go = new AtomicInteger(-1);
        AtomicInteger done = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>();
        for (Write writer : writers)
        {
            Thread thread = new Thread(() -> {
                try
                {
                    for (int k = 0; k < rows; k++)
                    {
                        while (go.get() < k)
                            Thread.onSpinWait();
                        writer.apply(k);
                        done.incrementAndGet();
                    }
                }
                catch (Throwable t)
                {
                    failure.compareAndSet(null, t);
                    done.addAndGet(1 << 28);
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (int k = 0; k < rows; k++)
        {
            go.set(k);
            while (done.get() < writers.length * (k + 1))
                Thread.onSpinWait();
        }
        go.set(Integer.MAX_VALUE);
        for (Thread thread : threads)
            thread.join();
        if (failure.get() != null)
            throw failure.get();
    }

    private void assertIndexFindsEveryRow(String context, int rows, String restriction,
                                          Predicate<UntypedResultSet.Row> holds)
    {
        for (String source : new String[]{ "memtable", "sstable" })
        {
            if (source.equals("sstable"))
                flush();
            Set<Integer> holding = new TreeSet<>();
            for (UntypedResultSet.Row row : execute("SELECT * FROM %s WHERE pk = 0"))
                if (row.getInt("ck") < rows && holds.test(row))
                    holding.add(row.getInt("ck"));
            Set<Integer> found = cks("SELECT ck FROM %s WHERE " + restriction);
            found.removeIf(ck -> ck >= rows);
            Set<Integer> missing = new TreeSet<>(holding);
            missing.removeAll(found);
            assertEquals(context + ", " + source + ": rows holding the value that the index misses (of " + holding.size() + ')',
                         Set.of(), missing);
        }
    }
}
