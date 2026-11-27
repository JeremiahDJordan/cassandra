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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Without immutable components, a full rebuild of an index deletes the per-SSTable components of its SSTables before it
 * writes them again. The other indexes of the table must not stay queryable without an SSTable they can't load.
 */
public class FullRebuildPerSSTableComponentsTest extends SAITester
{
    private static final int SSTABLES = 8;
    private static final int ROWS_PER_SSTABLE = 100;

    private static volatile boolean holdBuilders;
    private static volatile boolean failFirstBuilder;
    private static final AtomicInteger buildersHeld = new AtomicInteger();

    /**
     * Called by the injected code: holds a builder there while {@link #holdBuilders} is set. With
     * {@link #failFirstBuilder}, only the first builder is held, and it fails once released.
     */
    public static void holdBuilder() throws InterruptedException
    {
        if (!holdBuilders)
            return;
        boolean first = buildersHeld.incrementAndGet() == 1;
        if (failFirstBuilder && !first)
            return;
        while (holdBuilders)
            Thread.sleep(20);
        if (failFirstBuilder)
            throw new RuntimeException("Injected index writer failure");
    }

    @After
    public void release()
    {
        holdBuilders = false;
        failFirstBuilder = false;
        Injections.deleteAll();
    }

    /**
     * A full rebuild of v stopped inside its writer leaves its SSTables without per-SSTable components; when they are
     * loaded again, w can't load them and doesn't stay queryable without them.
     */
    @Test
    public void testSSTablesLoadedAgainAfterAStoppedFullRebuild() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        writeSSTables();

        holdBuilders();
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForBuildersHeld();
        CompactionManager.instance.stopCompaction("INDEX_BUILD");
        holdBuilders = false;
        rebuild.get(180, TimeUnit.SECONDS);
        Injections.deleteAll();
        waitForIndexBuildsToSettle();

        // an operation that has read all of the SSTables (and so taken them out of the live set) aborts
        LifecycleTransaction compaction = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        assertNotNull(compaction);
        compaction.originals().forEach(compaction::obsolete);
        compaction.checkpoint();
        compaction.abort();
        assertThat(cfs.getLiveSSTables()).hasSize(SSTABLES);

        assertTrue("w must not be queryable without some of its SSTables",
                   !cfs.indexManager.isIndexQueryable(w) || rows("w") == SSTABLES * ROWS_PER_SSTABLE);

        // rebuilding them brings them back
        cfs.indexManager.rebuildIndexesBlocking(Set.of(v, w));
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertTrue(cfs.indexManager.isIndexQueryable(w));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("w"));
    }

    /**
     * While a full rebuild of v has deleted the per-SSTable components of its SSTables, a load of the SSTables (here by
     * the initial build of a new index) can't load them, but w still has them in its view and stays queryable.
     */
    @Test
    public void testOtherIndexStaysQueryableWhenSSTablesAreLoadedDuringAFullRebuild() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int, x int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        writeSSTables();

        holdBuilders();
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForBuildersHeld();

        // the initial build of x loads the SSTables of the table, and is held once it has
        Injections.Barrier holdSelection = Injections.newBarrier("hold_initial_build_selection", 2, false)
                                                     .add(InvokePointBuilder.newInvokePoint()
                                                                            .onClass(StorageAttachedIndex.class)
                                                                            .onMethod("findNonIndexedSSTables")
                                                                            .atExit())
                                                     .build();
        Injections.inject(holdSelection);
        String x = createIndexAsync("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        waitForAssert(() -> assertEquals(1, holdSelection.getCount()), 60, TimeUnit.SECONDS);
        assertTrue("w serves all its SSTables, and must stay queryable", cfs.indexManager.isIndexQueryable(w));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("w"));

        // the rebuild of v is over before the initial build of x goes on, as it would delete the per-SSTable
        // components of an SSTable x loads
        holdBuilders = false;
        assertNull(rebuild.get(180, TimeUnit.SECONDS));
        holdSelection.countDown();
        holdSelection.disable();
        Injections.deleteAll();
        waitForAssert(() -> assertTrue(cfs.indexManager.isIndexQueryable(x)), 120, TimeUnit.SECONDS);
        waitForIndexBuildsToSettle();
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertTrue("w must still be queryable", cfs.indexManager.isIndexQueryable(w));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("w"));
    }

    /**
     * Rebuilds of v and w overlap on one SSTable: the rebuild of v writes the per-SSTable components, and fails, while
     * the rebuild of w waits for them. The rebuild of w must fail too, as w can't serve the SSTable.
     */
    @Test
    public void testOverlappingRebuildWhenThePerSSTableComponentsFail() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v, w) VALUES (0, 0, 0)");
        flush();

        failFirstBuilder = true;
        holdBuilders();
        Injections.Counter completions = Injections.newCounter("sstable_completions")
                                                   .add(InvokePointBuilder.newInvokePoint()
                                                                          .onClass(StorageAttachedIndexBuilder.class)
                                                                          .onMethod("completeSSTable"))
                                                   .build();
        Injections.inject(completions);
        CompletableFuture<Throwable> rebuildV = rebuildAsync(cfs, v);
        waitForBuildersHeld();
        CompletableFuture<Throwable> rebuildW = rebuildAsync(cfs, w);
        // the rebuild of w has written its per-index components, and waits for the per-SSTable ones
        waitForAssert(() -> assertEquals(1, completions.get()), 60, TimeUnit.SECONDS);
        holdBuilders = false;

        assertNotNull("the rebuild of v must fail", rebuildV.get(60, TimeUnit.SECONDS));
        assertNotNull("the rebuild of w must fail", rebuildW.get(60, TimeUnit.SECONDS));
        assertTrue("w must not be queryable without the SSTable", !cfs.indexManager.isIndexQueryable(w) || rows("w") == 1);
        Injections.deleteAll();
        waitForIndexBuildsToSettle();

        cfs.indexManager.rebuildIndexesBlocking(Set.of(v, w));
        assertTrue(cfs.indexManager.isIndexQueryable(w));
        assertEquals(1, rows("w"));
    }

    /**
     * The rebuild of w has built its SSTable, and the SSTable is lost from the view of w (a rebuild of v deletes the
     * per-SSTable components and fails, then the SSTable is loaded again) before the rebuild completes. The rebuild
     * of w must not make w queryable without the SSTable.
     */
    @Test
    public void testRebuildCompletingAfterItsSSTableIsLost() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v, w) VALUES (0, 0, 0)");
        flush();
        StorageAttachedIndex wIndex = (StorageAttachedIndex) cfs.indexManager.getIndexByName(w);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Hold the rebuild of w once its builder is done
        Injections.Barrier holdW = Injections.newBarrier("hold_rebuild_completion", 2, false)
                                             .add(InvokePointBuilder.newInvokePoint()
                                                                    .onClass(StorageAttachedIndexBuilder.class)
                                                                    .onMethod("build")
                                                                    .atExit())
                                             .build();
        Injections.inject(holdW);
        CompletableFuture<Throwable> rebuildW = rebuildAsync(cfs, w);
        waitForAssert(() -> assertEquals(1, holdW.getCount()), 60, TimeUnit.SECONDS);
        holdW.disable();
        assertTrue(wIndex.getIndexContext().getView().containsSSTableIndex(sstable.descriptor));

        // A rebuild of v deletes the per-SSTable components and fails
        failFirstBuilder = true;
        holdBuilders();
        CompletableFuture<Throwable> rebuildV = rebuildAsync(cfs, v);
        waitForBuildersHeld();
        holdBuilders = false;
        assertNotNull("the rebuild of v must fail", rebuildV.get(60, TimeUnit.SECONDS));

        // An operation that has taken the SSTable out of the live set aborts, which loads it again
        LifecycleTransaction compaction = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        assertNotNull(compaction);
        compaction.originals().forEach(compaction::obsolete);
        compaction.checkpoint();
        compaction.abort();
        assertFalse(wIndex.getIndexContext().getView().containsSSTableIndex(sstable.descriptor));
        assertFalse(rebuildW.isDone());

        holdW.countDown();
        assertNotNull("the rebuild of w must fail", rebuildW.get(60, TimeUnit.SECONDS));
        assertFalse("w must not be queryable without the SSTable", cfs.indexManager.isIndexQueryable(w));
        Injections.deleteAll();
        waitForIndexBuildsToSettle();

        cfs.indexManager.rebuildIndexesBlocking(Set.of(v, w));
        assertTrue(cfs.indexManager.isIndexQueryable(w));
        assertEquals(1, rows("w"));
    }

    /**
     * A truncate stops a full rebuild of v inside its writer: the rebuild ends quietly and v stays queryable.
     */
    @Test
    public void testRebuildStoppedByTruncate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        writeSSTables();

        holdBuilders();
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForBuildersHeld();
        CompletableFuture<Void> truncate = CompletableFuture.runAsync(cfs::truncateBlocking);
        waitForAssert(() -> assertTrue(CompactionManager.instance.active.getTableOperations()
                                                                       .stream()
                                                                       .anyMatch(op -> op.getProgress().operationType() == OperationType.INDEX_BUILD
                                                                                       && op.isStopRequested())),
                      60, TimeUnit.SECONDS);
        holdBuilders = false;

        truncate.get(60, TimeUnit.SECONDS);
        assertNull(rebuild.get(60, TimeUnit.SECONDS));
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertFalse(cfs.indexManager.needsFullRebuild(v));
        execute("INSERT INTO %s (k, v, w) VALUES (0, 0, 0)");
        assertEquals(1, rows("v"));
    }

    /**
     * The initial build of a new index finds the per-SSTable components of an SSTable corrupt as it loads the SSTables,
     * and builds them again: that doesn't fail the build.
     */
    @Test
    public void testInitialBuildRepairingCorruptPerSSTableComponents() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, x int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v, x) VALUES (0, 0, 0)");
        flush();
        corruptIndexComponent(IndexComponentType.GROUP_META, CorruptionType.TRUNCATED_HEADER);
        StorageAttachedIndexGroup.getIndexGroup(cfs).reset();

        String x = createIndexAsync("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        waitForAssert(() -> assertFalse(cfs.indexManager.isIndexBuilding(x)), 60, TimeUnit.SECONDS);
        assertTrue(cfs.indexManager.isIndexQueryable(x));
        assertEquals(1, execute("SELECT k FROM %s WHERE x = 0").size());
    }

    /**
     * An instance of an index that was dropped, still in use, makes itself non-queryable while a rebuild of an index of
     * the same name runs: that doesn't fail the rebuild of the new index.
     */
    @Test
    public void testDroppedIndexDoesNotFailTheBuildOfItsReplacement() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String name = createIndex("CREATE CUSTOM INDEX v_index ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v) VALUES (0, 0)");
        flush();
        StorageAttachedIndex dropped = (StorageAttachedIndex) cfs.indexManager.getIndexByName(name);
        dropIndex("DROP INDEX %s." + name);
        createIndex("CREATE CUSTOM INDEX v_index ON %s(v) USING 'StorageAttachedIndex'");

        // Hold the rebuild of the new index once its builder is done
        Injections.Barrier holdBuild = Injections.newBarrier("hold_rebuild_completion", 2, false)
                                                 .add(InvokePointBuilder.newInvokePoint()
                                                                        .onClass(StorageAttachedIndexBuilder.class)
                                                                        .onMethod("build")
                                                                        .atExit())
                                                 .build();
        Injections.inject(holdBuild);
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, name);
        waitForAssert(() -> assertEquals(1, holdBuild.getCount()), 60, TimeUnit.SECONDS);
        holdBuild.disable();
        dropped.makeIndexNonQueryable();
        holdBuild.countDown();

        assertNull(rebuild.get(60, TimeUnit.SECONDS));
        assertTrue(cfs.indexManager.isIndexQueryable(name));
        assertEquals(1, execute("SELECT k FROM %s WHERE v = 0").size());
    }

    private static CompletableFuture<Throwable> rebuildAsync(ColumnFamilyStore cfs, String index)
    {
        return CompletableFuture.supplyAsync(() -> {
            try
            {
                cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(index));
                return null;
            }
            catch (Throwable t)
            {
                return t;
            }
        });
    }

    /** Holds the index builders inside their writer, once they have deleted the components they rebuild. */
    private void holdBuilders() throws Throwable
    {
        buildersHeld.set(0);
        holdBuilders = true;
        Injections.inject(Injections.newCustom("hold_builders_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("startPartition"))
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(Expression.expr(FullRebuildPerSSTableComponentsTest.class.getName())
                                                                          .method("holdBuilder")
                                                                          .args()))
                                    .build());
    }

    private void waitForBuildersHeld()
    {
        waitForAssert(() -> assertThat(buildersHeld.get()).isGreaterThan(0), 120, TimeUnit.SECONDS);
    }

    private void waitForIndexBuildsToSettle()
    {
        waitForAssert(() -> assertThat(CompactionManager.instance.active.getTableOperations()
                                                                       .stream()
                                                                       .filter(op -> op.getProgress().operationType() == OperationType.INDEX_BUILD)
                                                                       .count()).isZero(),
                      120, TimeUnit.SECONDS);
    }

    private void writeSSTables()
    {
        for (int s = 0; s < SSTABLES; s++)
        {
            for (int k = s * ROWS_PER_SSTABLE; k < (s + 1) * ROWS_PER_SSTABLE; k++)
                execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 2, k % 2);
            flush();
        }
    }

    private int rows(String column)
    {
        return execute("SELECT k FROM %s WHERE " + column + " = 0").size() + execute("SELECT k FROM %s WHERE " + column + " = 1").size();
    }
}
