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
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * An index transaction that fails after the memtable applied the update leaves the update applied, and the memtable
 * accounts for it as for any other write: its columns, minimum timestamp, size and operation count.
 */
@RunWith(Parameterized.class)
public class FailedWriteFlushTest extends CQLTester
{
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

    private List<Integer> rows()
    {
        List<Integer> cks = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT ck FROM %s WHERE pk = 1"))
            cks.add(row.getInt("ck"));
        return cks;
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

}
