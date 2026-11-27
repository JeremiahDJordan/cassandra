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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A failed merge leaves the trie of a trie memtable unchanged, so it must not change the memtable's live data size,
 * which {@link Memtable#estimateRowCount} divides by the average row size, nor its other statistics.
 */
@RunWith(Parameterized.class)
public class TrieMemtableLiveDataSizeTest extends CQLTester
{
    @Parameterized.Parameter
    public String memtableClass;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object> parameters()
    {
        return Arrays.asList("TrieMemtable", "TrieMemtableStage1");
    }

    private void createTable()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c set<int>, PRIMARY KEY (pk, ck)) " +
                    "WITH memtable = {'class': '" + memtableClass + "'}");
    }

    @Test
    public void testFailedMergeLeavesLiveDataSizeUnchanged()
    {
        createTable();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (0, 1, 1, {1, 2}) USING TIMESTAMP 10");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        long liveDataSize = memtable().getLiveDataSize();

        // A new row, failing once it has been merged.
        PartitionUpdate.SimpleBuilder insertBuilder = PartitionUpdate.simpleBuilder(cfs.metadata(), 0).timestamp(20);
        insertBuilder.row(2).add("v", 2).add("c", set(3, 4));
        PartitionUpdate insert = insertBuilder.build();
        putFailing(insert, new FailingUpdateTransaction(false, 0));
        assertEquals(liveDataSize, memtable().getLiveDataSize());

        // A row deletion, failing once it has been merged.
        PartitionUpdate.SimpleBuilder deleteBuilder = PartitionUpdate.simpleBuilder(cfs.metadata(), 0).timestamp(30);
        deleteBuilder.row(1).delete();
        PartitionUpdate deletion = deleteBuilder.build();
        putFailing(deletion, new FailingUpdateTransaction(false, 0));
        assertEquals(liveDataSize, memtable().getLiveDataSize());

        // A failure before anything is merged.
        putFailing(insert, new FailingUpdateTransaction(true, Integer.MAX_VALUE));
        assertEquals(liveDataSize, memtable().getLiveDataSize());

        put(insert, new FailingUpdateTransaction(false, Integer.MAX_VALUE));
        put(deletion, new FailingUpdateTransaction(false, Integer.MAX_VALUE));
        assertRows(execute("SELECT ck, v, c FROM %s WHERE pk = 0"), row(2, 2, set(3, 4)));
    }

    /**
     * Failed deletions of a row can't take the live data size below zero, which would make the estimated row count
     * negative and fail SAI queries.
     */
    @Test
    public void testSAIQueryAfterFailedDeletions()
    {
        createTable();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 1, 1) USING TIMESTAMP 10");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        long liveDataSize = memtable().getLiveDataSize();

        for (int i = 0; i < 5; ++i)
        {
            PartitionUpdate.SimpleBuilder deleteBuilder = PartitionUpdate.simpleBuilder(cfs.metadata(), 0).timestamp(20);
            deleteBuilder.row(1).delete();
            putFailing(deleteBuilder.build(), new FailingUpdateTransaction(false, 0));
        }

        assertEquals(liveDataSize, memtable().getLiveDataSize());
        assertTrue(Memtable.estimateRowCount(memtable()) >= 0);
        assertRows(execute("SELECT pk, ck FROM %s WHERE v = 1"), row(0, 1));
    }

    /**
     * The minimum timestamp of the memtable, which flush writes to the SSTable metadata, and its operation count.
     */
    @Test
    public void testFailedMergeLeavesStatisticsUnchanged()
    {
        createTable();
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 1, 1) USING TIMESTAMP 10");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = memtable();
        long minTimestamp = memtable.getMinTimestamp();
        long operations = memtable.getOperations();

        // An older row, failing once it has been merged.
        PartitionUpdate.SimpleBuilder insertBuilder = PartitionUpdate.simpleBuilder(cfs.metadata(), 0).timestamp(5);
        insertBuilder.row(2).add("v", 2);
        putFailing(insertBuilder.build(), new FailingUpdateTransaction(false, 0));
        // An older partition deletion, failing before anything is merged.
        putFailing(PartitionUpdate.simpleBuilder(cfs.metadata(), 0).timestamp(3).delete().build(),
                   new FailingUpdateTransaction(true, Integer.MAX_VALUE));

        assertEquals(minTimestamp, memtable.getMinTimestamp());
        assertEquals(operations, memtable.getOperations());
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(0, 1, 1));
    }

    private Memtable memtable()
    {
        Memtable memtable = getCurrentColumnFamilyStore().getTracker().getView().getCurrentMemtable();
        assertEquals(memtableClass, memtable.getClass().getSimpleName());
        return memtable;
    }

    private void put(PartitionUpdate update, UpdateTransaction indexer)
    {
        try (OpOrder.Group writeGroup = Keyspace.writeOrder.start())
        {
            memtable().put(update, indexer, writeGroup);
        }
    }

    private void putFailing(PartitionUpdate update, FailingUpdateTransaction indexer)
    {
        try
        {
            put(update, indexer);
            fail("Expected the write to fail");
        }
        catch (InjectedFailure e)
        {
            // expected
        }
    }

    private static class InjectedFailure extends RuntimeException
    {
        InjectedFailure()
        {
            super("Injected failure");
        }
    }

    /**
     * Index transaction that fails the write in {@link #start}, or at the row event that follows the given number of
     * successful ones. The trie memtables report a row while merging, after it has been applied.
     */
    private static class FailingUpdateTransaction implements UpdateTransaction
    {
        private final boolean failInStart;
        private int rowsBeforeFailure;

        FailingUpdateTransaction(boolean failInStart, int rowsBeforeFailure)
        {
            this.failInStart = failInStart;
            this.rowsBeforeFailure = rowsBeforeFailure;
        }

        public void start()
        {
            if (failInStart)
                throw new InjectedFailure();
        }

        public void onInserted(Row row)
        {
            maybeFail();
        }

        public void onUpdated(Row existing, Row updated)
        {
            maybeFail();
        }

        private void maybeFail()
        {
            if (rowsBeforeFailure-- == 0)
                throw new InjectedFailure();
        }

        public void onPartitionDeletion(DeletionTime deletionTime) {}
        public void onRangeTombstone(RangeTombstone rangeTombstone) {}
        public void commit() {}
    }
}
