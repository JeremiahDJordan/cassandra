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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Iterables;
import org.awaitility.Awaitility;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StorageAttachedIndexBuilderTest extends SAITester
{
    private static volatile ColumnFamilyStore columnFamilyStore;
    private static volatile boolean descriptorCacheEvicted;

    private static final AtomicBoolean holdNextRowWrite = new AtomicBoolean();
    private static volatile boolean failHeldRowWrite;
    private static volatile boolean failHeldBuildIndexComplete;
    private static volatile Thread heldBuild;
    private static volatile CountDownLatch rowWriteHeld;
    private static volatile CountDownLatch rowWriteRelease;
    private static volatile CountDownLatch otherBuildWritten;

    @Test
    public void testRegisterComponentsFromBuildDescriptorAfterCacheEviction() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, value int)");
        execute("INSERT INTO %s (id, value) VALUES ('k1', 1)");
        flush();

        columnFamilyStore = getCurrentColumnFamilyStore();
        descriptorCacheEvicted = false;

        Injection evictDescriptorCache = Injections.newCustom("evict_descriptor_cache_after_sai_build")
                                                    .add(InvokePointBuilder.newInvokePoint()
                                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                                           .onMethod("completeSSTable")
                                                                           .after("INVOKE " + StorageAttachedIndexWriter.class.getName() + ".complete"))
                                                    .add(ActionBuilder.newActionBuilder()
                                                                      .actions()
                                                                      .doAction(Expression.expr(StorageAttachedIndexBuilderTest.class.getName())
                                                                                          .method("evictDescriptorCache")
                                                                                          .args()))
                                                    .build();

        try
        {
            Injections.inject(evictDescriptorCache);

            createIndex(String.format(CREATE_INDEX_TEMPLATE, "value"));
            Awaitility.await("Byteman called to clear SSTableContextManager")
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(descriptorCacheEvicted).isTrue());

            StorageAttachedIndexGroup group = Objects.requireNonNull(StorageAttachedIndexGroup.getIndexGroup(columnFamilyStore));
            SSTableReader sstable = Iterables.getOnlyElement(columnFamilyStore.getLiveSSTables());

            // there is one descriptor populated by index build and it has all built index components
            assertThat(group.sstableContextManager().getDescriptorCount()).isEqualTo(1);
            assertCompleteComponents(group, sstable);

            // now clear the cached descriptor and reload from TOC
            group.sstableContextManager().clear();
            assertThat(group.sstableContextManager().getDescriptorCount()).isEqualTo(0);

            assertCompleteComponents(group, sstable);
        }
        finally
        {
            columnFamilyStore = null;
        }
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static void evictDescriptorCache()
    {
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(columnFamilyStore);
        if (group == null)
            throw new IllegalStateException("Storage-attached index group is not registered");

        group.sstableContextManager().clear();
        descriptorCacheEvicted = true;
    }

    @Test
    public void testBuildWaitingForFailedPerSSTableBuildFails() throws Throwable
    {
        String indexA = createTableWithIndexOnA();
        boolean immutableComponents = IMMUTABLE_SAI_COMPONENTS.getBoolean();
        // without immutable components, the rebuild deletes the per-SSTable components before it writes them again
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        try
        {
            injectHolds();

            // the rebuild of a takes the per-SSTable lock and is held, the initial build of b waits for it
            CompletableFuture<Void> rebuild = holdRebuild(indexA, true);
            String indexB = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "b"));
            assertThat(otherBuildWritten.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(rebuild).isNotDone();

            rowWriteRelease.countDown();
            assertThatThrownBy(rebuild::join).hasRootCauseMessage("Injected failure");
            waitForIndexBuilds(indexB);
            releaseHolds();

            assertThatThrownBy(() -> executeInternal("SELECT pk FROM %s WHERE b = 3")).isInstanceOf(IndexNotAvailableException.class);

            rebuildIndexes(indexA, indexB);
            assertRowCount(execute("SELECT pk FROM %s WHERE a < 50"), 50);
            assertRowCount(execute("SELECT pk FROM %s WHERE b = 3"), 14);
        }
        finally
        {
            releaseHolds();
            IMMUTABLE_SAI_COMPONENTS.setBoolean(immutableComponents);
        }
    }

    @Test
    public void testBuildWaitingForPerSSTableBuildThatFailsAfterWritingThemSucceeds() throws Throwable
    {
        String indexA = createTableWithIndexOnA();
        try
        {
            injectHolds();

            // the rebuild of a writes the per-SSTable components, then fails on its own per-index components
            failHeldBuildIndexComplete = true;
            CompletableFuture<Void> rebuild = holdRebuild(indexA, false);
            String indexB = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "b"));
            assertThat(otherBuildWritten.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(rebuild).isNotDone();

            rowWriteRelease.countDown();
            assertThatThrownBy(rebuild::join).hasRootCauseMessage("Injected failure");
            waitForIndexBuilds(indexB);
            releaseHolds();

            assertRowCount(execute("SELECT pk FROM %s WHERE b = 3"), 14);
            assertThatThrownBy(() -> executeInternal("SELECT pk FROM %s WHERE a < 50")).isInstanceOf(IndexNotAvailableException.class);
        }
        finally
        {
            releaseHolds();
        }
    }

    @Test
    public void testBuildWaitingForFailedPerSSTableBuildOfTruncatedSSTableSucceeds() throws Throwable
    {
        String indexA = createTableWithIndexOnA();
        boolean immutableComponents = IMMUTABLE_SAI_COMPONENTS.getBoolean();
        // without immutable components, the rebuild deletes the per-SSTable components before it writes them again
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        try
        {
            injectHolds();

            // the SSTable is truncated while the rebuild of a holds the per-SSTable lock and the initial build of b
            // waits for it, then the rebuild fails
            CompletableFuture<Void> rebuild = holdRebuild(indexA, true);
            String indexB = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "b"));
            assertThat(otherBuildWritten.await(30, TimeUnit.SECONDS)).isTrue();
            SSTableReader sstable = Iterables.getOnlyElement(getCurrentColumnFamilyStore().getLiveSSTables());
            truncate(false);
            assertThat(sstable.isMarkedCompacted()).isTrue();
            assertThat(rebuild).isNotDone();

            rowWriteRelease.countDown();
            assertThatThrownBy(rebuild::join).hasRootCauseMessage("Injected failure");
            waitForIndexBuilds(indexB);
            releaseHolds();

            waitForIndexQueryable(indexB);
            assertRowCount(execute("SELECT pk FROM %s WHERE b = 3"), 0);
            execute("INSERT INTO %s (pk, a, b) VALUES (?, ?, ?)", 1000, 1000, 3);
            flush();
            assertRowCount(execute("SELECT pk FROM %s WHERE b = 3"), 1);
        }
        finally
        {
            releaseHolds();
            IMMUTABLE_SAI_COMPONENTS.setBoolean(immutableComponents);
        }
    }

    private String createTableWithIndexOnA() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int, b int)");
        disableCompaction();
        for (int pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, a, b) VALUES (?, ?, ?)", pk, pk, pk % 7);
        flush();
        return createIndex(String.format(CREATE_INDEX_TEMPLATE, "a"));
    }

    private CompletableFuture<Void> holdRebuild(String index, boolean failRowWrite) throws InterruptedException
    {
        failHeldRowWrite = failRowWrite;
        holdNextRowWrite.set(true);
        CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(index));
        assertThat(rowWriteHeld.await(30, TimeUnit.SECONDS)).isTrue();
        return rebuild;
    }

    private void injectHolds() throws Throwable
    {
        heldBuild = null;
        rowWriteHeld = new CountDownLatch(1);
        rowWriteRelease = new CountDownLatch(1);
        otherBuildWritten = new CountDownLatch(1);

        Injections.inject(newHold("hold_first_row_write",
                                  InvokePointBuilder.newInvokePoint()
                                                    .onClass(SSTableIndexWriter.class)
                                                    .onMethod("addRow")
                                                    .atEntry(),
                                  "holdRowWrite"),
                          newHold("fail_held_build_index_complete",
                                  InvokePointBuilder.newInvokePoint()
                                                    .onClass(SSTableIndexWriter.class)
                                                    .onMethod("complete")
                                                    .atEntry(),
                                  "failHeldBuildIndexComplete"),
                          newHold("signal_per_index_components_written",
                                  InvokePointBuilder.newInvokePoint()
                                                    .onClass(StorageAttachedIndexBuilder.class)
                                                    .onMethod("completeSSTable")
                                                    .after("INVOKE " + StorageAttachedIndexWriter.class.getName() + ".complete"),
                                  "otherBuildWritten"));
    }

    private static Injection newHold(String name, InvokePointBuilder invokePoint, String method)
    {
        return Injections.newCustom(name)
                         .add(invokePoint)
                         .add(ActionBuilder.newActionBuilder()
                                           .actions()
                                           .doAction(Expression.expr(StorageAttachedIndexBuilderTest.class.getName())
                                                               .method(method)
                                                               .args()))
                         .build();
    }

    private static void releaseHolds()
    {
        holdNextRowWrite.set(false);
        failHeldBuildIndexComplete = false;
        Injections.deleteAll();
        if (rowWriteRelease != null)
            rowWriteRelease.countDown();
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static void holdRowWrite() throws InterruptedException
    {
        if (!holdNextRowWrite.compareAndSet(true, false))
            return;

        heldBuild = Thread.currentThread();
        rowWriteHeld.countDown();
        rowWriteRelease.await(60, TimeUnit.SECONDS);
        if (failHeldRowWrite)
            throw new RuntimeException("Injected failure");
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static void failHeldBuildIndexComplete()
    {
        if (failHeldBuildIndexComplete && Thread.currentThread() == heldBuild)
            throw new RuntimeException("Injected failure");
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static void otherBuildWritten()
    {
        if (Thread.currentThread() != heldBuild)
            otherBuildWritten.countDown();
    }

    private void assertCompleteComponents(StorageAttachedIndexGroup group, SSTableReader sstable)
    {
        IndexDescriptor descriptor = group.descriptorFor(sstable);
        assertThat(descriptor.perSSTableComponents().isComplete()).isTrue();
        for (StorageAttachedIndex index : group.getIndexes())
            assertThat(descriptor.perIndexComponents(index.getIndexContext()).isComplete()).isTrue();
    }
}
