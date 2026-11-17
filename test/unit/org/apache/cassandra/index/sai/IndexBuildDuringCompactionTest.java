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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicates;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A compaction result is opened early, and put in the live set of sstables, before the compaction has finished writing
 * its index components. Index builds and the index group must not use that early reader: its components are
 * incomplete, and the final reader that the compaction adds when it completes is equal to it.
 */
public class IndexBuildDuringCompactionTest extends SAITester
{
    private static final int SSTABLES = 4;
    private static final int ROWS_PER_SSTABLE = 10;
    // enough for a compaction of them to open its result early, with a preemptive open interval of 1 MiB
    private static final int LARGE_ROWS_PER_SSTABLE = 2000;
    private static final int LARGE_ROW_PAD = 2048;

    private int preemptiveOpenInterval;

    // The state of the injected hold of a compaction once it has moved the starts of its sources
    private static volatile ColumnFamilyStore holdTable;
    private static volatile boolean hold;
    private static volatile boolean holding;

    @Before
    public void saveConfig()
    {
        preemptiveOpenInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
    }

    @After
    public void cleanUp()
    {
        hold = false;
        holdTable = null;
        Injections.deleteAll();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(preemptiveOpenInterval);
    }

    /**
     * Called by the injected rule after each partition a compaction writes: holds it, while {@link #hold} is set, once
     * it has moved the start of one of its sources, i.e. opened early a part of its result.
     */
    @SuppressWarnings("unused")
    public static void holdOnceStartsAreMoved() throws InterruptedException
    {
        ColumnFamilyStore cfs = holdTable;
        if (cfs == null || !hold)
            return;
        if (cfs.getLiveSSTables().stream().noneMatch(sstable -> sstable.openReason == SSTableReader.OpenReason.MOVED_START))
            return;
        holding = true;
        while (hold)
            Thread.sleep(20);
        holding = false;
    }

    /**
     * Scanning the live sstables while a compaction result is opened early, as an initial build does to select the
     * sstables to build, must not leave the index group with the result's incomplete descriptor: the result would
     * then be missing from every index once the compaction completes.
     */
    @Test
    public void testLiveSSTablesScannedWhileCompactionResultIsOpenedEarly() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String index = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForTableIndexesQueryable();
        writeSSTables();

        Injections.Barrier holdCompaction = holdCompactionBeforeItCompletesItsResult();
        CompletableFuture<Void> compaction = CompletableFuture.runAsync(cfs::forceMajorCompaction);
        waitForAssert(() -> assertThat(holdCompaction.getCount()).isEqualTo(1), 60, TimeUnit.SECONDS);
        assertThat(cfs.getLiveSSTables()).anyMatch(sstable -> sstable.openReason == SSTableReader.OpenReason.EARLY);

        // What an initial build does to select the sstables it builds
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        group.onSSTableChanged(Collections.emptyList(), cfs.getLiveSSTables(), group.getIndexes(), false);

        holdCompaction.countDown();
        holdCompaction.disable();
        compaction.get(60, TimeUnit.SECONDS);

        assertThat(cfs.getLiveSSTables()).hasSize(1);
        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
        assertAllRowsFound("v", SSTABLES * ROWS_PER_SSTABLE);
    }

    /**
     * An index created while a major compaction completes its result, which its writer created without the index,
     * must not become queryable without that result: its initial build waits for the compaction, and then builds the
     * index on the result.
     */
    @Test
    public void testIndexCreatedWhileMajorCompactionCompletes() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForTableIndexesQueryable();
        writeSSTables();

        Injections.Barrier holdCompaction = holdCompactionBeforeItCompletesItsResult();
        CompletableFuture<Void> compaction = CompletableFuture.runAsync(cfs::forceMajorCompaction);
        waitForAssert(() -> assertThat(holdCompaction.getCount()).isEqualTo(1), 60, TimeUnit.SECONDS);

        String index = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "w"));
        // the initial build of the new index stops the compaction, and waits for it
        waitForAssert(() -> assertThat(CompactionManager.instance.active.getTableOperations()).anyMatch(TableOperation::isStopRequested), 60, TimeUnit.SECONDS);
        holdCompaction.countDown();
        holdCompaction.disable();
        try
        {
            compaction.get(60, TimeUnit.SECONDS);
        }
        catch (Throwable t)
        {
            // The compaction may be stopped by the initial build of the new index: its sources then remain.
        }
        waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse(), 60, TimeUnit.SECONDS);

        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
        assertAllRowsFound("v", SSTABLES * ROWS_PER_SSTABLE);
        assertAllRowsFound("w", SSTABLES * ROWS_PER_SSTABLE);
    }

    /**
     * The version check of the vector indexes of a table (when the table definition is validated, e.g. for ALTER TABLE
     * ADD or CREATE INDEX) reads the descriptors of the sstables of the table: while a compaction result is opened
     * early, that must not leave the index group with the result's incomplete descriptor.
     */
    @Test
    public void testTableWithVectorIndexAlteredWhileCompactionResultIsOpenedEarly() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int, vec vector<float, 3>)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String index = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v"));
        String vectorIndex = createIndex(String.format(CREATE_INDEX_TEMPLATE, "vec"));
        waitForTableIndexesQueryable();
        for (int s = 0; s < SSTABLES; s++)
        {
            for (int k = s * ROWS_PER_SSTABLE; k < (s + 1) * ROWS_PER_SSTABLE; k++)
                execute("INSERT INTO %s (k, v, w, vec) VALUES (?, ?, ?, ?)", k, k % 2, k % 2, vector(1.0f + k, 2.0f, 3.0f));
            flush();
        }

        Injections.Barrier holdCompaction = holdCompactionBeforeItCompletesItsResult();
        CompletableFuture<Void> compaction = CompletableFuture.runAsync(cfs::forceMajorCompaction);
        waitForAssert(() -> assertThat(holdCompaction.getCount()).isEqualTo(1), 60, TimeUnit.SECONDS);
        assertThat(cfs.getLiveSSTables()).anyMatch(sstable -> sstable.openReason == SSTableReader.OpenReason.EARLY);

        alterTable("ALTER TABLE %s ADD extra int");

        holdCompaction.countDown();
        holdCompaction.disable();
        compaction.get(60, TimeUnit.SECONDS);

        assertThat(cfs.getLiveSSTables()).hasSize(1);
        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
        assertThat(cfs.indexManager.isIndexQueryable(vectorIndex)).isTrue();
        assertAllRowsFound("v", SSTABLES * ROWS_PER_SSTABLE);
        assertThat(execute("SELECT k FROM %s ORDER BY vec ANN OF [1.0, 2.0, 3.0] LIMIT 100")).hasSize(SSTABLES * ROWS_PER_SSTABLE);
    }

    /**
     * The initial build of an index selects its sstables while a compaction that started after its wait for compactions
     * has moved the starts of its sources: it builds the sources as they are in the canonical set.
     */
    @Test
    public void testInitialBuildWhileCompactionHasMovedStarts() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int, pad blob)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForTableIndexesQueryable();
        writeLargeSSTables();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(1);

        // Hold the initial build of a new index after its wait for compactions, before it selects its sstables
        Injections.Barrier holdSelection = Injections.newBarrier("hold_initial_build_selection", 2, false)
                                                     .add(InvokePointBuilder.newInvokePoint()
                                                                            .onClass(StorageAttachedIndex.class)
                                                                            .onMethod("findNonIndexedSSTables"))
                                                     .build();
        Injections.inject(holdSelection);
        String index = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "w"));
        waitForAssert(() -> assertThat(holdSelection.getCount()).isEqualTo(1), 60, TimeUnit.SECONDS);

        // Start a compaction, and hold it once it has moved the starts of its sources
        CompletableFuture<Void> compaction = startCompactionHeldOnceStartsAreMoved(cfs);

        // Let the initial build go on, then stop the compaction
        holdSelection.countDown();
        holdSelection.disable();
        waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse(), 120, TimeUnit.SECONDS);
        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();

        stopCompactions(cfs, compaction);
        assertAllRowsFound("v", SSTABLES * LARGE_ROWS_PER_SSTABLE);
        assertAllRowsFound("w", SSTABLES * LARGE_ROWS_PER_SSTABLE);
    }

    private CompletableFuture<Void> startCompactionHeldOnceStartsAreMoved(ColumnFamilyStore cfs) throws Throwable
    {
        holdTable = cfs;
        hold = true;
        Injections.inject(Injections.newCustom("hold_compaction_once_starts_are_moved")
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(SSTableRewriter.class)
                                                           .onMethod("append")
                                                           .atExit())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(Expression.expr(IndexBuildDuringCompactionTest.class.getName())
                                                                          .method("holdOnceStartsAreMoved")
                                                                          .args()))
                                    .build());
        CompletableFuture<Void> compaction = CompletableFuture.runAsync(cfs::forceMajorCompaction);
        waitForAssert(() -> assertThat(holding).describedAs("compaction held once it has moved starts").isTrue(), 120, TimeUnit.SECONDS);
        return compaction;
    }

    /**
     * Stops the held compaction, as a cleanup or an anticompaction would: it aborts and restores its sources.
     */
    private static void stopCompactions(ColumnFamilyStore cfs, CompletableFuture<Void> compaction)
    {
        CompactionManager.instance.interruptCompactionFor(Collections.singleton(cfs.metadata()), Predicates.alwaysTrue(), Predicates.alwaysTrue(),
                                                          false, TableOperation.StopTrigger.CLEANUP);
        hold = false;
        assertThatThrownBy(() -> compaction.get(120, TimeUnit.SECONDS)).hasRootCauseInstanceOf(CompactionInterruptedException.class);
    }

    private void writeLargeSSTables()
    {
        Random random = new Random(1);
        byte[] pad = new byte[LARGE_ROW_PAD];
        for (int s = 0; s < SSTABLES; s++)
        {
            for (int k = s * LARGE_ROWS_PER_SSTABLE; k < (s + 1) * LARGE_ROWS_PER_SSTABLE; k++)
            {
                random.nextBytes(pad);
                execute("INSERT INTO %s (k, v, w, pad) VALUES (?, ?, ?, ?)", k, k % 2, k % 2, ByteBuffer.wrap(pad));
            }
            flush();
        }
    }

    /**
     * Holds compactions once their final result is opened early (and in the live set), before its writer completes
     * the result, with its index components.
     */
    private static Injections.Barrier holdCompactionBeforeItCompletesItsResult() throws Throwable
    {
        Injections.Barrier barrier = Injections.newBarrier("hold_compaction_result", 2, false)
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(SSTableRewriter.class)
                                                                      .onMethod("doPrepare")
                                                                      .atInvoke("prepareToCommit"))
                                               .build();
        Injections.inject(barrier);
        return barrier;
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

    private void assertAllRowsFound(String column, int expected)
    {
        int found = execute("SELECT k FROM %s WHERE " + column + " = 0").size() + execute("SELECT k FROM %s WHERE " + column + " = 1").size();
        assertThat(found).describedAs("rows found through the index on %s", column).isEqualTo(expected);
    }
}
