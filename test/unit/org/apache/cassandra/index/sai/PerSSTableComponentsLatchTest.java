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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The per-SSTable latch of the builders ({@code StorageAttachedIndexBuilder#inProgress}), which one builder of an
 * SSTable gets to write its per-SSTable components while the others build their per-index components only.
 */
public class PerSSTableComponentsLatchTest extends SAITester
{
    private static final int ROWS = 1000;

    private static volatile boolean holding;
    private static final AtomicInteger arrivals = new AtomicInteger();
    private static final AtomicInteger failures = new AtomicInteger();

    /** called by the injected code before a builder tries to take the per-SSTable latch: holds the first one */
    public static void holdFirstBuilder() throws InterruptedException
    {
        if (arrivals.getAndIncrement() > 0)
            return;
        while (holding)
            Thread.sleep(10);
    }

    /** called by the injected code: true only the first time */
    public static boolean firstTime()
    {
        return failures.getAndIncrement() == 0;
    }

    @After
    public void release() throws Throwable
    {
        holding = false;
        Injections.deleteAll();
    }

    private void createTableWithTwoIndexes()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX v_idx ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX w_idx ON %s(w) USING 'StorageAttachedIndex'");
        for (int k = 0; k < ROWS; k++)
            execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 10, k % 7);
        flush();
    }

    /**
     * Two partial builds of an SSTable without complete per-SSTable components: the one that takes the latch second
     * uses the components the first one wrote, rather than write them again.
     */
    @Test
    public void testBuildUsesPerSSTableComponentsCompletedBeforeItTakesTheLatch() throws Throwable
    {
        createTableWithTwoIndexes();
        assertQueries();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertThat(cfs.getLiveSSTables()).hasSize(1);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        // every index component of the SSTable is lost, so that the only components there are when the second build
        // takes the latch are those of the first one
        IndexDescriptor descriptor = group.descriptorFor(sstable);
        descriptor.perSSTableComponents().invalidate(sstable, cfs.getTracker());
        for (StorageAttachedIndex index : group.getIndexes())
            descriptor.perIndexComponents(index.getIndexContext()).invalidate(sstable, cfs.getTracker());

        arrivals.set(0);
        holding = true;
        Injections.inject(Injections.newCustom("hold_first_builder_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                           .onMethod("shouldWritePerSSTableFiles")
                                                           .atInvoke("putIfAbsent"))
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(Expression.expr(PerSSTableComponentsLatchTest.class.getName())
                                                                          .method("holdFirstBuilder")
                                                                          .args()))
                                    .build());

        // the build of w is held before the latch...
        CompletableFuture<Throwable> buildW =
            CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, "w_idx"));
        waitForAssert(() -> assertThat(arrivals.get()).isEqualTo(1), 120, TimeUnit.SECONDS);
        // ...while the build of v takes it, writes the per-SSTable components, builds v and releases it
        assertThat(partialBuild(group, sstable, "v_idx")).isNull();
        assertThat(arrivals.get()).isEqualTo(2);
        holding = false;
        assertThat(buildW.get(120, TimeUnit.SECONDS)).isNull();
        Injections.deleteAll();

        assertQueries();
        assertTrue(loadDescriptor(sstable, cfs).perSSTableComponents().isComplete());
        // and once loaded again from disk
        simulateNodeRestart();
        assertQueries();
    }

    /**
     * The build that writes lost per-SSTable components invalidates the per-index components of the other indexes,
     * but not those that another build has completed on the new ones since.
     */
    @Test
    public void testLatchHolderKeepsComponentsBuiltOnItsOwn() throws Throwable
    {
        createTableWithTwoIndexes();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        IndexDescriptor descriptor = group.descriptorFor(sstable);
        descriptor.perSSTableComponents().invalidate(sstable, cfs.getTracker());
        IndexContext w = ((StorageAttachedIndex) cfs.indexManager.getIndexByName("w_idx")).getIndexContext();
        IndexComponents.ForRead lostOnes = descriptor.perIndexComponents(w);
        assertTrue(lostOnes.isComplete());

        arrivals.set(0);
        holding = true;
        Injections.inject(Injections.newCustom("stall_before_invalidating_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                           .onMethod("invalidateComponentsOfOtherIndexes")
                                                           .atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(Expression.expr(PerSSTableComponentsLatchTest.class.getName())
                                                                          .method("holdFirstBuilder")
                                                                          .args()))
                                    .build());

        // the build of v takes the latch, and stalls before it invalidates...
        CompletableFuture<Throwable> buildV =
            CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, "v_idx"));
        waitForAssert(() -> assertThat(arrivals.get()).isEqualTo(1), 120, TimeUnit.SECONDS);
        // ...while the build of w builds its components on the per-SSTable components v's build writes
        CompletableFuture<Throwable> buildW =
            CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, "w_idx"));
        waitForAssert(() -> {
            IndexComponents.ForRead components = descriptor.perIndexComponents(w);
            assertTrue(components != lostOnes && components.isComplete());
        }, 120, TimeUnit.SECONDS);
        holding = false;
        assertThat(buildV.get(120, TimeUnit.SECONDS)).isNull();
        assertThat(buildW.get(120, TimeUnit.SECONDS)).isNull();
        Injections.deleteAll();

        assertTrue(descriptor.perIndexComponents(w).isComplete());
        assertQueries();
        // and once loaded again from disk
        simulateNodeRestart();
        assertQueries();
    }

    /**
     * A build that gets the latch and fails as it deletes the per-SSTable components must release it, or the next builds
     * of the SSTable wait for it forever.
     */
    @Test
    public void testBuildFailingOnceItHoldsTheLatchReleasesIt() throws Throwable
    {
        createTableWithTwoIndexes();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        failures.set(0);
        Injections.inject(Injections.newCustom("fail_once_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                           .onMethod("prepareForRebuild")
                                                           .atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(Expression.expr(PerSSTableComponentsLatchTest.class.getName())
                                                                      .method("firstTime")
                                                                      .args())
                                                      .toActionBuilder()
                                                      .actions()
                                                      .doThrow(RuntimeException.class, Expression.quote("Injected failure!")))
                                    .build());
        assertThatThrownBy(() -> cfs.indexManager.rebuildIndexesBlocking(Collections.singleton("v_idx")))
            .hasStackTraceContaining("Injected failure!");
        assertThat(failures.get()).isEqualTo(1);

        CompletableFuture<Void> rebuild =
            CompletableFuture.runAsync(() -> cfs.indexManager.rebuildIndexesBlocking(Collections.singleton("v_idx")));
        rebuild.get(120, TimeUnit.SECONDS);
        assertQueries();
    }

    private void assertQueries()
    {
        assertTrue("v_idx must be queryable", isIndexQueryable(KEYSPACE, "v_idx"));
        assertTrue("w_idx must be queryable", isIndexQueryable(KEYSPACE, "w_idx"));
        assertEquals(ROWS / 10, execute("SELECT k FROM %s WHERE v = 3").size());
        assertEquals(143, execute("SELECT k FROM %s WHERE w = 3").size());
        assertEquals(15, execute("SELECT k FROM %s WHERE v = 3 AND w = 3").size());
    }
}
