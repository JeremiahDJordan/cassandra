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

package org.apache.cassandra.index.sai;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A full rebuild stopped by a truncate ends as a success only if the truncate has removed the SSTables it hadn't
 * rebuilt. It waits for the truncate to be over to tell, for at most
 * {@link StorageAttachedIndexBuilder#TRUNCATED_SSTABLES_WAIT_MILLIS}, and otherwise fails like any other stopped build.
 * The "truncate" here is {@link ColumnFamilyStore#runWithCompactionsDisabled} with the truncate trigger.
 */
public class StoppedIndexBuildTest extends SAITester
{
    private static final int ROWS = 100;

    private final long wait = StorageAttachedIndexBuilder.TRUNCATED_SSTABLES_WAIT_MILLIS;

    @After
    public void restore()
    {
        StorageAttachedIndexBuilder.TRUNCATED_SSTABLES_WAIT_MILLIS = wait;
        Injections.deleteAll();
    }

    @Test
    public void testFullRebuildStoppedByATruncateWaitsForItToBeOver() throws Throwable
    {
        String index = createTableWithIndexAndSSTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Injections.Barrier held = holdRebuild();
        CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(index));
        waitForAssert(() -> assertEquals(1, held.getCount()), 10, TimeUnit.SECONDS);

        CountDownLatch truncating = new CountDownLatch(1);
        CountDownLatch over = new CountDownLatch(1);
        CompletableFuture<Void> truncate = truncate(cfs, truncating, over, true);
        assertTrue(truncating.await(10, TimeUnit.SECONDS));
        held.countDown();

        // the rebuild waits for the truncate, and then sees its SSTable removed
        Thread.sleep(2000);
        assertFalse("the rebuild must wait for the truncate", rebuild.isDone());
        over.countDown();
        truncate.get(30, TimeUnit.SECONDS);
        rebuild.get(30, TimeUnit.SECONDS);
        assertThat(cfs.getLiveSSTables()).isEmpty();
        assertTrue(cfs.indexManager.isIndexQueryable(index));
    }

    @Test
    public void testFullRebuildStoppedByATruncateThatRemovesNothing() throws Throwable
    {
        String index = createTableWithIndexAndSSTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Injections.Barrier held = holdRebuild();
        CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(index));
        waitForAssert(() -> assertEquals(1, held.getCount()), 10, TimeUnit.SECONDS);

        CountDownLatch truncating = new CountDownLatch(1);
        CountDownLatch over = new CountDownLatch(1);
        CompletableFuture<Void> truncate = truncate(cfs, truncating, over, false);
        assertTrue(truncating.await(10, TimeUnit.SECONDS));
        held.countDown();

        // the rebuild waits for the truncate, and then fails
        Thread.sleep(2000);
        assertFalse("the rebuild must wait for the truncate", rebuild.isDone());
        over.countDown();
        truncate.get(30, TimeUnit.SECONDS);
        assertThatThrownBy(() -> rebuild.get(30, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
        assertFalse(cfs.indexManager.isIndexQueryable(index));

        assertRebuildReturnsEveryRow(cfs, index);
    }

    @Test
    public void testFullRebuildStoppedByATruncateThatOutlastsTheWait() throws Throwable
    {
        StorageAttachedIndexBuilder.TRUNCATED_SSTABLES_WAIT_MILLIS = 1500;
        String index = createTableWithIndexAndSSTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Injections.Barrier held = holdRebuild();
        CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(index));
        waitForAssert(() -> assertEquals(1, held.getCount()), 10, TimeUnit.SECONDS);

        CountDownLatch truncating = new CountDownLatch(1);
        CountDownLatch over = new CountDownLatch(1);
        CompletableFuture<Void> truncate = truncate(cfs, truncating, over, false);
        assertTrue(truncating.await(10, TimeUnit.SECONDS));
        held.countDown();
        try
        {
            // the rebuild fails while the truncate still runs
            assertThatThrownBy(() -> rebuild.get(15, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
            assertFalse(truncate.isDone());
        }
        finally
        {
            over.countDown();
        }
        truncate.get(30, TimeUnit.SECONDS);
        assertFalse(cfs.indexManager.isIndexQueryable(index));

        assertRebuildReturnsEveryRow(cfs, index);
    }

    @Test
    public void testFullRebuildStoppedByATruncateWhileItsSSTableIsOutOfTheLiveSet() throws Throwable
    {
        String index = createTableWithIndexAndSSTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Injections.Barrier held = holdRebuild();
        CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(index));
        waitForAssert(() -> assertEquals(1, held.getCount()), 10, TimeUnit.SECONDS);

        // what an operation that opens its results early does with the SSTables it has read all of
        LifecycleTransaction compaction = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        assertNotNull(compaction);
        try
        {
            compaction.originals().forEach(compaction::obsolete);
            compaction.checkpoint();
            assertThat(cfs.getLiveSSTables()).isEmpty();
            CompactionManager.instance.interruptCompactionFor(Collections.singleton(cfs.metadata()),
                                                              TableOperation.StopTrigger.TRUNCATE);
            held.countDown();
            assertThatThrownBy(() -> rebuild.get(30, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
        }
        finally
        {
            // it aborts, and its SSTable is live again
            compaction.abort();
        }
        assertThat(cfs.getLiveSSTables()).hasSize(1);
        assertFalse(cfs.indexManager.isIndexQueryable(index));

        assertRebuildReturnsEveryRow(cfs, index);
    }

    /**
     * @return the name of an index on a table with one SSTable of {@link #ROWS} rows
     */
    private String createTableWithIndexAndSSTable() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        String index = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForTableIndexesQueryable();
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s (id1, v1, v2) VALUES ('" + i + "', 0, '0');");
        flush();
        return index;
    }

    /** Holds the builder of a rebuild once its writers exist. */
    private static Injections.Barrier holdRebuild() throws Throwable
    {
        Injections.Barrier held = Injections.newBarrier("rebuildHeld", 2, false)
                                            .add(InvokePointBuilder.newInvokePoint()
                                                                   .onClass(StorageAttachedIndexWriter.class)
                                                                   .onMethod("startPartition")
                                                                   .atEntry())
                                            .build();
        Injections.inject(held);
        return held;
    }

    /**
     * Stops the operations of the table as a truncate, and then holds until {@code over} is counted down, removing the
     * SSTables of the table if {@code remove}.
     */
    private static CompletableFuture<Void> truncate(ColumnFamilyStore cfs, CountDownLatch truncating, CountDownLatch over, boolean remove)
    {
        return CompletableFuture.runAsync(() -> cfs.runWithCompactionsDisabled(() -> {
            truncating.countDown();
            over.await();
            if (remove)
                cfs.discardSSTables(System.currentTimeMillis());
            return null;
        }, true, true, TableOperation.StopTrigger.TRUNCATE));
    }

    private void assertRebuildReturnsEveryRow(ColumnFamilyStore cfs, String index)
    {
        Injections.deleteAll();
        rebuildIndexes(index);
        assertTrue(cfs.indexManager.isIndexQueryable(index));
        assertEquals(ROWS, execute("SELECT id1 FROM %s WHERE v1 >= 0").size());
    }
}
