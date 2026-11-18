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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A write that fails while it is merged into the memtable leaves the memtable's content unchanged, and the flush must
 * then write that content. The failure is injected through the index transaction, at each of the index events of the
 * write in turn.
 * <p>
 * An index transaction that fails after the memtable applied the update leaves the update applied, and the memtable
 * accounts for it as for any other write: its columns, minimum timestamp, size and operation count.
 */
@RunWith(Parameterized.class)
public class FailedWriteFlushTest extends CQLTester
{
    private static final int MAX_FAILURE_POINTS = 40;

    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object> parameters()
    {
        return Arrays.asList("trie", "trie_stage1", "trie_stage2", "trie_stage3", "skiplist", "skiplist_sharded", "skiplist_sharded_locking");
    }

    @BeforeClass
    public static void allowShardedMemtables()
    {
        // The sharded memtable configurations cannot be written to the schema in the CC4-compatible storage
        // compatibility modes (HCD_1, the test default, and CASSANDRA_4), see MemtableParams.asSchemaValueMap
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
    }

    /// The index transaction fails when it completes, after the memtable applied the update, which is the memtable's
    /// only write and the first to use its columns, with an old timestamp. On the skip-list memtables, with and
    /// without the atomic apply that SAI asks for.
    @Test
    public void testIndexTransactionFailsAfterApply()
    {
        for (boolean atomicApply : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, w text, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
            disableCompaction();
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(cfs.metadata(), 1).timestamp(5);
            builder.row(1).add("v", 1).add("w", "a");
            PartitionUpdate update = builder.build();
            Memtable current = cfs.getCurrentMemtable();
            try (OpOrder.Group writeGroup = cfs.keyspace.writeOrder.start())
            {
                current.put(update, new FailingCommitTransaction(atomicApply), writeGroup);
                fail("expected the injected failure");
            }
            catch (InjectedFailure e)
            {
                // expected
            }

            String context = memtable + (atomicApply ? ", atomic apply" : "");
            assertEquals(context, Arrays.asList(1), rows());
            assertEquals(context, 5, current.getMinTimestamp());
            assertEquals(context, 1, current.operationCount());
            assertTrue(context, current.getLiveDataSize() > 0);
            assertEquals(context, update.columns(), current.getFlushSet(null, null).columns());

            flush();
            assertEquals(context + ", after flush", Arrays.asList(1), rows());
            assertEquals(context, 1, cfs.getLiveSSTables().size());
            for (SSTableReader sstable : cfs.getLiveSSTables())
                assertEquals(context, 5, sstable.getMinTimestamp());
            assertRows(execute("SELECT v, w FROM %s WHERE pk = 1 AND ck = 1"), row(1, "a"));
        }
    }

    /// The same where the memtable already holds a row that uses other columns.
    @Test
    public void testIndexTransactionFailsAfterApplyWithNewColumn()
    {
        for (boolean atomicApply : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, w text, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
            disableCompaction();
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 0, 0) USING TIMESTAMP 100");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(cfs.metadata(), 2).timestamp(50);
            builder.row(1).add("w", "a");
            PartitionUpdate update = builder.build();
            Memtable current = cfs.getCurrentMemtable();
            try (OpOrder.Group writeGroup = cfs.keyspace.writeOrder.start())
            {
                current.put(update, new FailingCommitTransaction(atomicApply), writeGroup);
                fail("expected the injected failure");
            }
            catch (InjectedFailure e)
            {
                // expected
            }

            String context = memtable + (atomicApply ? ", atomic apply" : "");
            assertEquals(context, 50, current.getMinTimestamp());
            assertEquals(context, 2, current.operationCount());
            flush();
            assertRows(execute("SELECT pk, ck, v, w FROM %s"), row(1, 0, 0, null), row(2, 1, null, "a"));
        }
    }

    /// Two rows in the memtable; the failed write deletes the partition.
    @Test
    public void testFailedPartitionDeletion()
    {
        checkFailedWrite(false, 2, metadata -> PartitionUpdate.simpleBuilder(metadata, 1).timestamp(20).delete().build());
    }

    /// The same with two rows in an older SSTable, which must not come back either.
    @Test
    public void testFailedPartitionDeletionOverSSTableRows()
    {
        checkFailedWrite(true, 2, metadata -> PartitionUpdate.simpleBuilder(metadata, 1).timestamp(20).delete().build());
    }

    /// One row in the memtable; the failed write is a batch of an old partition deletion, a deletion of that row and
    /// an insert.
    @Test
    public void testFailedBatchWithRowDeletion()
    {
        checkFailedWrite(false, 1, metadata -> {
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 1).timestamp(5).delete();
            builder.row(11).timestamp(20).delete();
            builder.row(30).timestamp(30).add("v", 1);
            return builder.build();
        });
    }

    /// A partition with a static row and three rows; a partition deletion fails, is applied again, and two of the rows
    /// are written again. The rewritten rows must be flushed.
    @Test
    public void testFailedPartitionDeletionRetriedThenRowsRewritten()
    {
        int failedAtSomePoint = 0;
        for (int failAt = 0; failAt < MAX_FAILURE_POINTS; ++failAt)
        {
            createTable("CREATE TABLE %s (pk int, ck int, st int static, v int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
            disableCompaction();
            execute("INSERT INTO %s (pk, ck, st, v) VALUES (1, 1, 0, 0) USING TIMESTAMP 10");
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 0) USING TIMESTAMP 10");
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 3, 0) USING TIMESTAMP 10");
            if (!putFailingAt(PartitionUpdate.simpleBuilder(getCurrentColumnFamilyStore().metadata(), 1).timestamp(20).delete().build(), failAt))
                break;
            ++failedAtSomePoint;

            execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 1");
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 30");
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 1) USING TIMESTAMP 30");

            String context = memtable + ": partition deletion failed at index event " + failAt + " and was applied again";
            assertEquals(context + ", memtable", Arrays.asList(1, 2), rows());
            flush();
            assertEquals(context + ", after flush", Arrays.asList(1, 2), rows());
        }
        assertTrue("The write never failed", failedAtSomePoint > 0);
    }

    /// For every failure point, a failed write of older rows leaves the memtable's minimum timestamp, operation count
    /// and live data size as they were.
    @Test
    public void testFailedWriteLeavesStatisticsUnchanged()
    {
        int failedAtSomePoint = 0;
        for (int failAt = 0; failAt < MAX_FAILURE_POINTS; ++failAt)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
            disableCompaction();
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 0) USING TIMESTAMP 10");
            Memtable current = getCurrentColumnFamilyStore().getCurrentMemtable();
            long minTimestamp = current.getMinTimestamp();
            long operationCount = current.operationCount();
            long liveDataSize = current.getLiveDataSize();

            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(getCurrentColumnFamilyStore().metadata(), 1).timestamp(5);
            builder.row(2).add("v", 1);
            builder.row(3).add("v", 1);
            if (!putFailingAt(builder.build(), failAt))
                break;
            ++failedAtSomePoint;

            String context = memtable + ": write failed at index event " + failAt;
            assertEquals(context, minTimestamp, current.getMinTimestamp());
            assertEquals(context, operationCount, current.operationCount());
            assertEquals(context, liveDataSize, current.getLiveDataSize());
        }
        assertTrue("The write never failed", failedAtSomePoint > 0);
    }

    /// For every failure point, and for the write failing once and twice: the memtable's content and the content
    /// flushed from it must be what they were before the write.
    private void checkFailedWrite(boolean rowsInSSTable, int memtableRows, Function<TableMetadata, PartitionUpdate> write)
    {
        int failedAtSomePoint = 0;
        for (int failAt = 0; failAt < MAX_FAILURE_POINTS; ++failAt)
        {
            boolean failed = false;
            for (int times = 1; times <= 2; ++times)
            {
                createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
                disableCompaction();
                if (rowsInSSTable)
                {
                    execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 0) USING TIMESTAMP 10");
                    execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 0) USING TIMESTAMP 10");
                    flush();
                }
                for (int ck = 11; ck < 11 + memtableRows; ++ck)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, 0) USING TIMESTAMP 11", ck);
                List<Integer> before = rows();

                for (int i = 0; i < times; ++i)
                    failed |= putFailingAt(write.apply(getCurrentColumnFamilyStore().metadata()), failAt);
                if (!failed)
                    break;

                String context = memtable + ": write failed " + times + " time(s) at index event " + failAt;
                assertEquals(context + ", memtable", before, rows());
                flush();
                assertEquals(context + ", after flush", before, rows());
            }
            if (!failed)
                break;
            ++failedAtSomePoint;
        }
        assertTrue("The write never failed", failedAtSomePoint > 0);
    }

    private List<Integer> rows()
    {
        List<Integer> cks = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT ck FROM %s WHERE pk = 1"))
            cks.add(row.getInt("ck"));
        return cks;
    }

    /// Applies the update to the memtable with an index transaction that throws at the given index event. Returns
    /// whether the write failed.
    private boolean putFailingAt(PartitionUpdate update, int failAt)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        try (OpOrder.Group writeGroup = cfs.keyspace.writeOrder.start())
        {
            cfs.getCurrentMemtable().put(update, new FailingUpdateTransaction(failAt), writeGroup);
            return false;
        }
        catch (InjectedFailure e)
        {
            return true;
        }
    }

    /// Index transaction that throws when it completes, after the memtable applied the update.
    private static class FailingCommitTransaction implements UpdateTransaction
    {
        private final boolean atomicApply;

        FailingCommitTransaction(boolean atomicApply)
        {
            this.atomicApply = atomicApply;
        }

        @Override
        public boolean requiresAtomicApply()
        {
            return atomicApply;
        }

        public void start() {}
        public void onPartitionDeletion(DeletionTime deletionTime) {}
        public void onRangeTombstone(RangeTombstone rangeTombstone) {}
        public void onInserted(Row row) {}
        public void onUpdated(Row existing, Row updated) {}
        public void startRow(Clustering clustering, LivenessInfo existingLiveness, Row.Deletion existingDeletion, LivenessInfo updatedLiveness, Row.Deletion updatedDeletion) {}
        public void onCellUpdate(Cell<?> original, Cell<?> merged) {}
        public void onComplexColumnDeletion(ColumnMetadata column, DeletionTime deletionTime) {}

        public void commit()
        {
            throw new InjectedFailure();
        }
    }

    private static class InjectedFailure extends RuntimeException
    {
        InjectedFailure()
        {
            super("Injected failure");
        }
    }

    /// Index transaction that throws at the given index event (counted from 0, not counting [#start] and [#commit]).
    private static class FailingUpdateTransaction implements UpdateTransaction
    {
        private int eventsBeforeFailure;

        FailingUpdateTransaction(int eventsBeforeFailure)
        {
            this.eventsBeforeFailure = eventsBeforeFailure;
        }

        private void event()
        {
            if (eventsBeforeFailure-- == 0)
                throw new InjectedFailure();
        }

        public void start() {}
        public void onPartitionDeletion(DeletionTime deletionTime) { event(); }
        public void onRangeTombstone(RangeTombstone rangeTombstone) { event(); }
        public void onInserted(Row row) { event(); }
        public void onUpdated(Row existing, Row updated) { event(); }
        public void startRow(Clustering clustering, LivenessInfo existingLiveness, Row.Deletion existingDeletion, LivenessInfo updatedLiveness, Row.Deletion updatedDeletion) { event(); }
        public void onCellUpdate(Cell<?> original, Cell<?> merged) { event(); }
        public void onComplexColumnDeletion(ColumnMetadata column, DeletionTime deletionTime) { event(); }
        public void commit() {}
    }
}
