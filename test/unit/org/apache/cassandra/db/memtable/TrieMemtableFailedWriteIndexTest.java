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

package org.apache.cassandra.db.memtable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/// Tests that the indexes of a table stay consistent with its data when the memtable fails to apply a write, and when
/// concurrent writes to a row race.
///
/// Failures are injected where the memtable reports an event of a given kind to the index transaction, i.e. while it is
/// merging the update. Legacy index tables have no index transaction of their own, so their writes are not counted.
@RunWith(BMUnitRunner.class)
@BMRules(rules = {
    @BMRule(name = "Fail at a row reported by the memtable",
            targetClass = "org.apache.cassandra.index.SecondaryIndexManager$WriteTimeTransaction",
            targetMethod = "startRow",
            targetLocation = "AT ENTRY",
            condition = "!callerEquals(\"onUpdated\") && org.apache.cassandra.db.memtable.TrieMemtableFailedWriteIndexTest.shouldFail(\"row\")",
            action = "throw new java.lang.RuntimeException(\"Injected memtable failure\")"),
    @BMRule(name = "Fail at an updated row reported by the memtable",
            targetClass = "org.apache.cassandra.index.SecondaryIndexManager$WriteTimeTransaction",
            targetMethod = "onUpdated",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.memtable.TrieMemtableFailedWriteIndexTest.shouldFail(\"row\")",
            action = "throw new java.lang.RuntimeException(\"Injected memtable failure\")"),
    @BMRule(name = "Fail at an inserted row reported by the memtable",
            targetClass = "org.apache.cassandra.index.SecondaryIndexManager$WriteTimeTransaction",
            targetMethod = "onInserted",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.memtable.TrieMemtableFailedWriteIndexTest.shouldFail(\"row\")",
            action = "throw new java.lang.RuntimeException(\"Injected memtable failure\")"),
    @BMRule(name = "Fail at a range tombstone reported by the memtable",
            targetClass = "org.apache.cassandra.index.SecondaryIndexManager$WriteTimeTransaction",
            targetMethod = "onRangeTombstone",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.memtable.TrieMemtableFailedWriteIndexTest.shouldFail(\"rangeTombstone\")",
            action = "throw new java.lang.RuntimeException(\"Injected memtable failure\")"),
    @BMRule(name = "Fail at a partition deletion reported by the memtable",
            targetClass = "org.apache.cassandra.index.SecondaryIndexManager$WriteTimeTransaction",
            targetMethod = "onPartitionDeletion",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.memtable.TrieMemtableFailedWriteIndexTest.shouldFail(\"partitionDeletion\")",
            action = "throw new java.lang.RuntimeException(\"Injected memtable failure\")"),
    @BMRule(name = "Fail in the SAI indexer when it updates a row",
            targetClass = "org.apache.cassandra.index.sai.StorageAttachedIndex$UpdateIndexer",
            targetMethod = "updateRow",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.memtable.TrieMemtableFailedWriteIndexTest.shouldFail(\"saiUpdateRow\")",
            action = "throw new java.lang.RuntimeException(\"Injected indexer failure\")"),
    @BMRule(name = "Pause the first writer when it commits its index transaction",
            targetClass = "org.apache.cassandra.index.SecondaryIndexManager$WriteTimeTransaction",
            targetMethod = "commit",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.memtable.TrieMemtableFailedWriteIndexTest.shouldPause()",
            action = "org.apache.cassandra.db.memtable.TrieMemtableFailedWriteIndexTest.pause()")
})
public class TrieMemtableFailedWriteIndexTest extends CQLTester
{
    /// The memtables that apply updates atomically, which must not deliver the index events of a failed write.
    private static final ImmutableList<String> ATOMIC_MEMTABLES = ImmutableList.of("trie", "trie_stage1", "trie_stage2", "trie_stage3");

    private static final String FAILURE_MESSAGE = "Injected memtable failure";
    private static final String FIRST_WRITER = "first-writer";
    private static final String FIVE_ROWS_BATCH = "BEGIN UNLOGGED BATCH USING TIMESTAMP 20 " +
                                                  "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 1, 1, 1); " +
                                                  "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 2, 1, 1); " +
                                                  "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 3, 1, 1); " +
                                                  "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 4, 1, 1); " +
                                                  "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 5, 1, 1); " +
                                                  "APPLY BATCH";

    /// The kind of event at which to fail, or null for no failure.
    private static volatile String failAtKind;
    /// The number of events of that kind to let through before the failure.
    private static final AtomicInteger eventsBeforeFailure = new AtomicInteger(-1);

    private static volatile boolean pauseArmed;
    private static volatile CountDownLatch paused = new CountDownLatch(1);
    private static volatile CountDownLatch resume = new CountDownLatch(1);

    /// What a [ScriptedIndex] throws for an event, given its name ("pa" or "pb") and the event with its ordinal, for
    /// example "row#2" for its second row event; null for none.
    interface Script
    {
        Throwable failure(String index, String event);
    }

    private static final Script NO_FAILURES = (index, event) -> null;
    private static volatile Script script = NO_FAILURES;
    private static final Map<String, AtomicInteger> eventCounts = new ConcurrentHashMap<>();

    /// Called by the Byteman rules for every event of the given kind the memtable reports.
    public static boolean shouldFail(String kind)
    {
        return kind.equals(failAtKind) && eventsBeforeFailure.getAndDecrement() == 0;
    }

    public static boolean shouldPause()
    {
        return pauseArmed && FIRST_WRITER.equals(Thread.currentThread().getName());
    }

    public static void pause() throws InterruptedException
    {
        pauseArmed = false;
        paused.countDown();
        resume.await(60, TimeUnit.SECONDS);
    }

    @After
    public void disarm()
    {
        failAtKind = null;
        eventsBeforeFailure.set(-1);
        script = NO_FAILURES;
        pauseArmed = false;
        resume.countDown();
    }

    /// A write that fails at its fourth row leaves the memtable unchanged. The indexes must not be told about the rows
    /// before it: an index told that a row was updated drops its old value, and then misses the live row.
    @Test
    public void testFailedWriteAtRow()
    {
        for (String memtable : ATOMIC_MEMTABLES)
            testFailedWrite(memtable, "row", 3,
                            "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 1, 1, 1); " +
                            "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 2, 1, 1); " +
                            "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 3, 1, 1); " +
                            "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 4, 1, 1); " +
                            "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 5, 1, 1); ");
    }

    /// A write that fails at its range tombstone. The trie memtables report the rows before the tombstone first.
    @Test
    public void testFailedWriteAtRangeTombstone()
    {
        for (String memtable : ATOMIC_MEMTABLES)
            testFailedWrite(memtable, "rangeTombstone", 0,
                            "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 1, 1, 1); " +
                            "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 2, 1, 1); " +
                            "DELETE FROM %1$s WHERE pk = 1 AND ck >= 3 AND ck <= 4; " +
                            "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 5, 1, 1); ");
    }

    /// A write that fails at its partition deletion, which is older than the rows so that it changes no data.
    @Test
    public void testFailedWriteAtPartitionDeletion()
    {
        for (String memtable : ATOMIC_MEMTABLES)
            testFailedWrite(memtable, "partitionDeletion", 0,
                            "DELETE FROM %1$s USING TIMESTAMP 1 WHERE pk = 1; " +
                            "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 1, 1, 1); " +
                            "INSERT INTO %1$s (pk, ck, v, w) VALUES (1, 5, 1, 1); ");
    }

    private void testFailedWrite(String memtable, String failAt, int eventsBefore, String batchStatements)
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(w) USING 'legacy_local_table'");
        assertTrue(memtable, getCurrentColumnFamilyStore().getCurrentMemtable().appliesUpdatesAtomically());

        for (int ck = 1; ck <= 5; ++ck)
            execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, ?, 0, 0) USING TIMESTAMP 10", ck);

        eventsBeforeFailure.set(eventsBefore);
        failAtKind = failAt;
        try
        {
            execute("BEGIN UNLOGGED BATCH " + batchStatements + "APPLY BATCH");
            fail(memtable + ": expected the injected failure");
        }
        catch (RuntimeException e)
        {
            assertTrue(memtable + ": " + e, String.valueOf(e).contains(FAILURE_MESSAGE) ||
                                            String.valueOf(e.getCause()).contains(FAILURE_MESSAGE));
        }
        finally
        {
            failAtKind = null;
        }

        // The memtable is unchanged.
        assertRows(execute("SELECT ck, v, w FROM %s WHERE pk = 1"),
                   row(1, 0, 0), row(2, 0, 0), row(3, 0, 0), row(4, 0, 0), row(5, 0, 0));
        assertIndexedRows(memtable, 1, 2, 3, 4, 5);

        // A later write is indexed normally.
        execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, 2, 1, 1) USING TIMESTAMP 20");
        assertIndexedRows(memtable, 1, 3, 4, 5);

        flush();
        assertIndexedRows(memtable + ", flushed", 1, 3, 4, 5);
        compact();
        assertIndexedRows(memtable + ", compacted", 1, 3, 4, 5);
    }

    private void assertIndexedRows(String context, int... unchanged)
    {
        Object[][] rows = new Object[unchanged.length][];
        for (int i = 0; i < unchanged.length; ++i)
            rows[i] = row(unchanged[i]);
        assertRowsWithContext(context + ", SAI", execute("SELECT ck FROM %s WHERE v = 0"), rows);
        assertRowsWithContext(context + ", legacy index", execute("SELECT ck FROM %s WHERE w = 0"), rows);
    }

    private void assertRowsWithContext(String context, UntypedResultSet result, Object[]... rows)
    {
        try
        {
            assertRows(result, rows);
        }
        catch (AssertionError e)
        {
            throw new AssertionError(context + ": " + e.getMessage(), e);
        }
    }

    /// An indexer that fails on one event, when the memtable has already applied the update and the transaction
    /// delivers the held events. The update stays applied and the write fails. The other events must still be
    /// delivered, to the failing indexer and to the other indexers; only the failing indexer's entry for the failing
    /// row is missing, until the index is rebuilt (here by compaction).
    @Test
    public void testIndexerFailureAtCommit()
    {
        for (String memtable : ATOMIC_MEMTABLES)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            createIndex("CREATE INDEX ON %s(v) USING 'sai'");
            createIndex("CREATE INDEX ON %s(w) USING 'legacy_local_table'");

            for (int ck = 1; ck <= 5; ++ck)
                execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, ?, 0, 0) USING TIMESTAMP 10", ck);

            eventsBeforeFailure.set(3);
            failAtKind = "saiUpdateRow";
            try
            {
                execute(FIVE_ROWS_BATCH);
                fail(memtable + ": expected the injected failure");
            }
            catch (RuntimeException e)
            {
                assertTrue(memtable + ": " + e, String.valueOf(e).contains("Injected indexer failure") ||
                                                String.valueOf(e.getCause()).contains("Injected indexer failure"));
            }
            finally
            {
                failAtKind = null;
            }

            // The update is applied.
            assertRows(execute("SELECT ck, v, w FROM %s WHERE pk = 1"),
                       row(1, 1, 1), row(2, 1, 1), row(3, 1, 1), row(4, 1, 1), row(5, 1, 1));
            assertRowsWithContext(memtable + ", SAI", execute("SELECT ck FROM %s WHERE v = 1"), row(1), row(2), row(3), row(5));
            assertRowsWithContext(memtable + ", legacy index", execute("SELECT ck FROM %s WHERE w = 1"), row(1), row(2), row(3), row(4), row(5));
            assertEmpty(execute("SELECT ck FROM %s WHERE w = 0"));

            flush();
            assertRowsWithContext(memtable + ", flushed, SAI", execute("SELECT ck FROM %s WHERE v = 1"), row(1), row(2), row(3), row(5));
            assertRowsWithContext(memtable + ", flushed, legacy index", execute("SELECT ck FROM %s WHERE w = 1"), row(1), row(2), row(3), row(4), row(5));
            compact();
            assertRowsWithContext(memtable + ", compacted, SAI", execute("SELECT ck FROM %s WHERE v = 1"), row(1), row(2), row(3), row(4), row(5));
        }
    }

    /// An indexer that throws the same exception instance on two events. Every other event must still be delivered, to
    /// both indexers, each indexer must get exactly one finish(), and the write must fail with that exception.
    @Test
    public void testIndexerRethrowingSameFailure()
    {
        RuntimeException same = new RuntimeException("Injected repeated indexer failure");
        for (String memtable : ATOMIC_MEMTABLES)
        {
            Throwable failure = writeWithScriptedIndexes(memtable, (index, event) -> index.equals("pa") &&
                                                                                     (event.equals("row#2") || event.equals("row#3"))
                                                                                     ? same : null);
            assertTrue(memtable + ": " + failure, isOrIsCausedBy(failure, same));
            assertRows(execute("SELECT ck, v FROM %s WHERE pk = 1"), row(1, 1), row(2, 1), row(3, 1), row(4, 1), row(5, 1));
            assertEquals(memtable, 3, scriptedIndex("pa").rowsUpdated.size());
            assertEquals(memtable, 1, scriptedIndex("pa").finishCalls);
            assertEquals(memtable, 5, scriptedIndex("pb").rowsUpdated.size());
            assertEquals(memtable, 1, scriptedIndex("pb").finishCalls);
        }
    }

    /// An indexer that fails in finish() after the memtable has applied the update, with no other failure. The update
    /// stays applied, the other indexer still gets every event and finish(), and the write fails.
    @Test
    public void testIndexerFailureAtFinish()
    {
        for (String memtable : ATOMIC_MEMTABLES)
        {
            RuntimeException finishFailure = new RuntimeException("Injected finish failure");
            Throwable failure = writeWithScriptedIndexes(memtable, (index, event) -> index.equals("pa") && event.equals("finish#1")
                                                                                     ? finishFailure : null);
            assertTrue(memtable + ": " + failure, isOrIsCausedBy(failure, finishFailure));
            assertRows(execute("SELECT ck, v FROM %s WHERE pk = 1"), row(1, 1), row(2, 1), row(3, 1), row(4, 1), row(5, 1));
            assertEquals(memtable, 5, scriptedIndex("pb").rowsUpdated.size());
            assertEquals(memtable, 1, scriptedIndex("pb").finishCalls);
        }
    }

    /// An OutOfMemoryError from an indexer is rethrown at once, for the JVM to handle; it ends the delivery.
    @Test
    public void testIndexerOutOfMemoryErrorIsRethrown()
    {
        for (String memtable : ATOMIC_MEMTABLES)
        {
            OutOfMemoryError oom = new OutOfMemoryError("Java heap space");
            Throwable failure = writeWithScriptedIndexes(memtable, (index, event) -> index.equals("pb") && event.equals("row#2")
                                                                                     ? oom : null);
            assertSame(memtable + ": " + failure, oom, failure);

            // A later write is delivered as usual.
            for (String index : ImmutableList.of("pa", "pb"))
            {
                scriptedIndex(index).reset();
                scriptedIndex(index).finishCalls = 0;
            }
            execute("UPDATE %s USING TIMESTAMP 30 SET v = 2, w = 2 WHERE pk = 1 AND ck = 1");
            for (String index : ImmutableList.of("pa", "pb"))
            {
                assertEquals(memtable + ", " + index, 1, scriptedIndex(index).rowsUpdated.size());
                assertEquals(memtable + ", " + index, 1, scriptedIndex(index).finishCalls);
            }
        }
    }

    /// When the memtable fails to apply an update, here because an indexer's begin() throws, and the abort then fails
    /// too, the write must fail with the original failure, with the abort's failure suppressed under it.
    @Test
    public void testAbortFailureKeepsRootCause()
    {
        for (String memtable : ATOMIC_MEMTABLES)
        {
            Throwable failure = writeWithScriptedIndexes(memtable, (index, event) ->
                                                                   index.equals("pb") && event.equals("begin#1") ? new RuntimeException("pb begin")
                                                                   : event.equals("finish#1") ? new RuntimeException(index + " finish")
                                                                   : null);
            Throwable cause = failure;
            while (cause != null && !"pb begin".equals(cause.getMessage()))
                cause = cause.getCause();
            assertTrue(memtable + ": " + failure, cause != null);
            assertEquals(memtable, 1, cause.getSuppressed().length);
            Throwable abortFailure = cause.getSuppressed()[0];
            assertTrue(memtable + ": " + abortFailure, abortFailure.getMessage().endsWith(" finish"));
            assertEquals(memtable, 1, abortFailure.getSuppressed().length);
            assertTrue(memtable + ": " + abortFailure, abortFailure.getSuppressed()[0].getMessage().endsWith(" finish"));

            // The update was not applied, and each indexer got one finish().
            assertRows(execute("SELECT ck, v FROM %s WHERE pk = 1"), row(1, 0), row(2, 0), row(3, 0), row(4, 0), row(5, 0));
            assertEquals(memtable, 1, scriptedIndex("pa").finishCalls);
            assertEquals(memtable, 1, scriptedIndex("pb").finishCalls);
        }
    }

    /// Creates a table on the given memtable with [ScriptedIndex] indexes "pa" on `v` and "pb" on `w`, writes five rows,
    /// then updates all five in one batch while the indexes follow the given script, and returns what the update
    /// threw. Fails if it threw nothing.
    private Throwable writeWithScriptedIndexes(String memtable, Script failures)
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        createIndex("CREATE CUSTOM INDEX pa_" + memtable + " ON %s(v) USING '" + ScriptedIndex.class.getName() + "'");
        createIndex("CREATE CUSTOM INDEX pb_" + memtable + " ON %s(w) USING '" + ScriptedIndex.class.getName() + "'");

        for (int ck = 1; ck <= 5; ++ck)
            execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, ?, 0, 0) USING TIMESTAMP 10", ck);

        for (String index : ImmutableList.of("pa", "pb"))
        {
            scriptedIndex(index).reset();
            scriptedIndex(index).finishCalls = 0;
        }
        eventCounts.clear();
        script = failures;
        try
        {
            execute(FIVE_ROWS_BATCH);
        }
        catch (Throwable t)
        {
            return t;
        }
        finally
        {
            script = NO_FAILURES;
        }
        fail(memtable + ": expected the injected failure");
        return null;
    }

    private StubIndex scriptedIndex(String name)
    {
        for (Index index : getCurrentColumnFamilyStore().indexManager.listIndexes())
            if (index.getIndexMetadata().name.startsWith(name + '_'))
                return (StubIndex) index;
        throw new AssertionError("No index " + name);
    }

    private static boolean isOrIsCausedBy(Throwable t, Throwable expected)
    {
        for (Throwable c = t; c != null; c = c.getCause())
            if (c == expected)
                return true;
        return false;
    }

    private static void maybeFail(String index, String event)
    {
        Script current = script;
        if (current == NO_FAILURES)
            return;
        int ordinal = eventCounts.computeIfAbsent(index + ':' + event, k -> new AtomicInteger()).incrementAndGet();
        Throwable t = current.failure(index, event + '#' + ordinal);
        if (t instanceof RuntimeException)
            throw (RuntimeException) t;
        if (t instanceof Error)
            throw (Error) t;
    }

    /// A [StubIndex] whose indexer throws what [#script] returns. The index name starts with its script name.
    public static class ScriptedIndex extends StubIndex
    {
        public ScriptedIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public Indexer indexerFor(DecoratedKey key,
                                  RegularAndStaticColumns columns,
                                  long nowInSec,
                                  WriteContext ctx,
                                  IndexTransaction.Type transactionType,
                                  Memtable memtable)
        {
            Indexer indexer = super.indexerFor(key, columns, nowInSec, ctx, transactionType, memtable);
            String name = getIndexMetadata().name.substring(0, 2);
            return new Indexer()
            {
                public void begin()
                {
                    maybeFail(name, "begin");
                    indexer.begin();
                }

                public void partitionDelete(DeletionTime deletionTime)
                {
                    indexer.partitionDelete(deletionTime);
                }

                public void rangeTombstone(RangeTombstone tombstone)
                {
                    indexer.rangeTombstone(tombstone);
                }

                public void insertRow(Row row)
                {
                    maybeFail(name, "row");
                    indexer.insertRow(row);
                }

                public void removeRow(Row row)
                {
                    indexer.removeRow(row);
                }

                public void updateRow(Row oldRowData, Row newRowData)
                {
                    maybeFail(name, "row");
                    indexer.updateRow(oldRowData, newRowData);
                }

                public void finish()
                {
                    indexer.finish();
                    maybeFail(name, "finish");
                }
            };
        }
    }

    /// Two SAI indexes, which share one indexer for the transaction. The index that gets the fourth row first fails on
    /// it; the other must still index all five rows.
    @Test
    public void testOneOfTwoSaiIndexesFails()
    {
        for (String memtable : ATOMIC_MEMTABLES)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            createIndex("CREATE INDEX ON %s(v) USING 'sai'");
            createIndex("CREATE INDEX ON %s(w) USING 'sai'");

            for (int ck = 1; ck <= 5; ++ck)
                execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, ?, 0, 0) USING TIMESTAMP 10", ck);

            // Two SAI row updates per row: the seventh is the fourth row in whichever index the group iterates first.
            eventsBeforeFailure.set(6);
            failAtKind = "saiUpdateRow";
            try
            {
                execute(FIVE_ROWS_BATCH);
                fail(memtable + ": expected the injected failure");
            }
            catch (RuntimeException e)
            {
                assertTrue(memtable + ": " + e, String.valueOf(e).contains("Injected indexer failure") ||
                                                String.valueOf(e.getCause()).contains("Injected indexer failure"));
            }
            finally
            {
                failAtKind = null;
            }

            assertOneIndexMissesRowFour(memtable);
            flush();
            assertOneIndexMissesRowFour(memtable + ", flushed");
        }
    }

    /// The same failure on a skip-list memtable. SAI requires atomic updates (Index.Indexer#requiresAtomicUpdates), so
    /// the skip-list memtable applies the update under the partition's lock and delivers SAI's events once it has
    /// applied it, as the trie memtables do: the update stays applied, the write fails, and only the failing index
    /// misses row 4.
    @Test
    public void testOneOfTwoSaiIndexesFailsSkipList()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)) WITH memtable = 'skiplist'");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(w) USING 'sai'");
        assertFalse(getCurrentColumnFamilyStore().getCurrentMemtable().appliesUpdatesAtomically());
        assertTrue(getCurrentColumnFamilyStore().getCurrentMemtable().appliesUpdatesAtomicallyOnRequest());

        for (int ck = 1; ck <= 5; ++ck)
            execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, ?, 0, 0) USING TIMESTAMP 10", ck);

        eventsBeforeFailure.set(6);
        failAtKind = "saiUpdateRow";
        try
        {
            execute(FIVE_ROWS_BATCH);
            fail("expected the injected failure");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.toString(), String.valueOf(e).contains("Injected indexer failure") ||
                                     String.valueOf(e.getCause()).contains("Injected indexer failure"));
        }
        finally
        {
            failAtKind = null;
        }

        assertRows(execute("SELECT ck, v, w FROM %s WHERE pk = 1"),
                   row(1, 1, 1), row(2, 1, 1), row(3, 1, 1), row(4, 1, 1), row(5, 1, 1));
        assertOneIndexMissesRowFour("skiplist");
        flush();
        assertOneIndexMissesRowFour("skiplist, flushed");
    }

    private void assertOneIndexMissesRowFour(String context)
    {
        List<Integer> v = cks(execute("SELECT ck FROM %s WHERE v = 1"));
        List<Integer> w = cks(execute("SELECT ck FROM %s WHERE w = 1"));
        List<Integer> all = ImmutableList.of(1, 2, 3, 4, 5);
        List<Integer> missingFour = ImmutableList.of(1, 2, 3, 5);
        assertTrue(context + ": v = 1 -> " + v + ", w = 1 -> " + w,
                   (v.equals(all) && w.equals(missingFour)) || (v.equals(missingFour) && w.equals(all)));
    }

    private static List<Integer> cks(UntypedResultSet result)
    {
        List<Integer> cks = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            cks.add(row.getInt("ck"));
        return cks;
    }

    /// Two acknowledged writes to one row on a skip-list memtable, where the first one to be applied is the last one
    /// to commit its index transaction. The skip-list memtable completes index transactions outside its partition
    /// update, so it must deliver index events as it merges, in the order the writes are applied.
    /// The row starts at v = 0. The first writer sets v = 1 at timestamp 20, and pauses when it commits its index
    /// transaction, after its update is applied. The second writer sets v = 0 at timestamp 30 and completes. Then the
    /// first writer completes. The row ends at v = 0, and `v = 0` must return it.
    @Test
    public void testConcurrentWritersToRowSkipList() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)) WITH memtable = 'skiplist'");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(w) USING 'legacy_local_table'");

        execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, 1, 0, 0) USING TIMESTAMP 10");
        assertRows(execute("SELECT ck FROM %s WHERE v = 0"), row(1));

        paused = new CountDownLatch(1);
        resume = new CountDownLatch(1);
        AtomicReference<Throwable> firstWriterError = new AtomicReference<>();
        Thread firstWriter = new Thread(() -> {
            try
            {
                execute("UPDATE %s USING TIMESTAMP 20 SET v = 1, w = 1 WHERE pk = 1 AND ck = 1");
            }
            catch (Throwable t)
            {
                firstWriterError.set(t);
            }
        }, FIRST_WRITER);
        pauseArmed = true;
        firstWriter.start();
        assertTrue("The first writer did not commit", paused.await(60, TimeUnit.SECONDS));

        execute("UPDATE %s USING TIMESTAMP 30 SET v = 0, w = 0 WHERE pk = 1 AND ck = 1");

        resume.countDown();
        firstWriter.join(60000);
        if (firstWriterError.get() != null)
            throw firstWriterError.get();

        assertRows(execute("SELECT v, w FROM %s WHERE pk = 1 AND ck = 1"), row(0, 0));
        assertRowsWithContext("skiplist, SAI", execute("SELECT ck FROM %s WHERE v = 0"), row(1));
        assertRowsWithContext("skiplist, legacy index", execute("SELECT ck FROM %s WHERE w = 0"), row(1));
        flush();
        assertRowsWithContext("skiplist, flushed, SAI", execute("SELECT ck FROM %s WHERE v = 0"), row(1));
        assertRowsWithContext("skiplist, flushed, legacy index", execute("SELECT ck FROM %s WHERE w = 0"), row(1));
    }
}
