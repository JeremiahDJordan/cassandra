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
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Index builds stopped, or held, inside their writer, and the other indexes of the SSTables they rebuild.
 */
public class StoppedIndexBuildTest extends SAITester
{
    private static final int SSTABLES = 8;
    private static final int ROWS_PER_SSTABLE = 100;

    private static volatile boolean holdBuilders;
    private static final AtomicInteger buildersHeld = new AtomicInteger();

    /** called by the injected code: holds a builder there while {@link #holdBuilders} is set */
    public static void holdBuilder() throws InterruptedException
    {
        if (!holdBuilders)
            return;
        buildersHeld.incrementAndGet();
        while (holdBuilders)
            Thread.sleep(20);
    }

    @After
    public void release() throws Throwable
    {
        holdBuilders = false;
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
        stop();
        holdBuilders = false;
        assertNotNull("the stopped rebuild must fail", rebuild.get(180, TimeUnit.SECONDS));
        Injections.deleteAll();
        waitForIndexBuildsToSettle();
        assertFalse(cfs.indexManager.isIndexQueryable(v));

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

        // the initial build of x loads the SSTables of the table, and then waits for the index build threads
        String x = createIndexAsync("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        waitForAssert(() -> assertTrue(cfs.indexManager.isIndexBuilding(x)), 120, TimeUnit.SECONDS);
        Thread.sleep(1000);
        assertTrue("w serves all its SSTables, and must stay queryable", cfs.indexManager.isIndexQueryable(w));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("w"));

        holdBuilders = false;
        assertThat(rebuild.get(180, TimeUnit.SECONDS)).isNull();
        Injections.deleteAll();
        waitForAssert(() -> assertTrue(cfs.indexManager.isIndexQueryable(x)), 120, TimeUnit.SECONDS);
        waitForIndexBuildsToSettle();
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertTrue("w must still be queryable", cfs.indexManager.isIndexQueryable(w));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("w"));
    }

    private void waitForIndexBuildsToSettle()
    {
        waitForAssert(() -> assertThat(CompactionManager.instance.active.getTableOperations()
                                                                       .stream()
                                                                       .filter(op -> op.getProgress().operationType() == OperationType.INDEX_BUILD)
                                                                       .count()).isZero(),
                      120, TimeUnit.SECONDS);
    }

    /** holds the builders inside their writer, once it has started writing the components of an SSTable */
    private void holdBuilders() throws Throwable
    {
        buildersHeld.set(0);
        holdBuilders = true;
        Injections.inject(Injections.newCustom("hold_builders_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexWriter.class)
                                                           .onMethod("startPartition"))
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(Expression.expr(StoppedIndexBuildTest.class.getName())
                                                                          .method("holdBuilder")
                                                                          .args()))
                                    .build());
    }

    private void waitForBuildersHeld()
    {
        waitForAssert(() -> assertThat(buildersHeld.get()).isGreaterThan(0), 120, TimeUnit.SECONDS);
    }

    private static void stop()
    {
        CompactionManager.instance.stopCompaction("INDEX_BUILD");
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
