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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexStatusManager;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A stopped index build must fail, whatever point the stop reaches it at: in particular before the build of an SSTable
 * has a writer (before its first key, between two SSTables, or while it waits for the other builds of the SSTable).
 * A full rebuild has removed its SSTables from the index's view, so a stopped rebuild that ended as a success would
 * leave the index queryable without the SSTables it hadn't rebuilt yet.
 */
public class StoppedIndexBuildTest extends SAITester
{
    private static final int SSTABLES = 8;
    private static final int ROWS_PER_SSTABLE = 100;
    /**
     * A build splits its SSTables by size between at most concurrent_compactors builders. The tests that hold a builder
     * between two SSTables need two or more SSTables per builder.
     */
    private static final int CONCURRENT_COMPACTORS = 2;

    private static int concurrentCompactors;

    private static volatile boolean holdBuilders;
    private static volatile boolean holdFirstBuilderOnly;
    private static final AtomicInteger buildersHeld = new AtomicInteger();
    private static volatile boolean holdCheck;
    private static volatile boolean checkHeld;
    private static volatile boolean holdSecondLook;
    private static volatile boolean secondLookHeld;
    private static volatile boolean holdNotifications;
    private static final AtomicInteger notificationsHeld = new AtomicInteger();

    /** called by the injected code: holds a builder there while {@link #holdBuilders} is set */
    public static void holdBuilder() throws InterruptedException
    {
        if (!holdBuilders)
            return;
        if (buildersHeld.getAndIncrement() > 0 && holdFirstBuilderOnly)
            return;
        while (holdBuilders)
            Thread.sleep(20);
    }

    /** called by the injected code at the start of the end check of a full rebuild */
    public static void holdCheck() throws InterruptedException
    {
        if (!holdCheck)
            return;
        checkHeld = true;
        while (holdCheck)
            Thread.sleep(10);
    }

    /** called by the injected code in the end check of a full rebuild, after it has loaded what was missing */
    public static void holdSecondLook() throws InterruptedException
    {
        if (!holdSecondLook)
            return;
        secondLookHeld = true;
        while (holdSecondLook)
            Thread.sleep(10);
    }

    /** called by the injected code when the group of the table is notified of SSTables added */
    public static void holdNotification(Object notification) throws InterruptedException
    {
        if (!holdNotifications || !(notification instanceof SSTableAddedNotification))
            return;
        notificationsHeld.incrementAndGet();
        while (holdNotifications)
            Thread.sleep(10);
    }

    @BeforeClass
    public static void setConcurrentCompactors()
    {
        concurrentCompactors = DatabaseDescriptor.getConcurrentCompactors();
        DatabaseDescriptor.setConcurrentCompactors(CONCURRENT_COMPACTORS);
    }

    @AfterClass
    public static void restoreConcurrentCompactors()
    {
        if (concurrentCompactors > 0)
            DatabaseDescriptor.setConcurrentCompactors(concurrentCompactors);
    }

    @After
    public void release() throws Throwable
    {
        holdCheck = false;
        holdSecondLook = false;
        holdNotifications = false;
        holdBuilders = false;
        holdFirstBuilderOnly = false;
        Injections.deleteAll();
    }

    @Test
    public void testFullRebuildStoppedBeforeTheWriterOfItsFirstSSTable() throws Throwable
    {
        testFullRebuildStopped("indexSSTable", true);
        testFullRebuildStopped("indexSSTable", false);
    }

    @Test
    public void testFullRebuildStoppedBetweenTwoSSTables() throws Throwable
    {
        // the stop is seen at the start of the next SSTable
        testFullRebuildStopped("completeSSTable", true);
        testFullRebuildStopped("completeSSTable", false);
    }

    /**
     * @param holdAt the method of StorageAttachedIndexBuilder at the start of which the builders are held when the stop
     *               comes
     * @param nodetoolStop nodetool stop INDEX_BUILD, or the interrupt of the operations of the table that a cleanup (or
     *                     a scrub, nodetool compact...) issues
     */
    private void testFullRebuildStopped(String holdAt, boolean nodetoolStop) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        writeSSTables();
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));

        holdBuilders(holdAt);
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForBuildersHeld();
        stop(cfs, nodetoolStop);
        holdBuilders = false;

        Throwable failure = rebuild.get(180, TimeUnit.SECONDS);
        String label = "held at " + holdAt + ", " + (nodetoolStop ? "nodetool stop" : "interrupted");
        assertNotNull(label + ": the stopped rebuild must fail", failure);
        assertFalse(label + ": the index must not be queryable without the SSTables the rebuild didn't reach",
                    cfs.indexManager.isIndexQueryable(v));
        Injections.deleteAll();

        // a rebuild brings it back, once the other builders of the stopped one are done
        waitForIndexBuildsToSettle();
        cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(v));
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
    }

    /**
     * A full rebuild stopped while it looks for an empty static row, to find out how the per-SSTable components it
     * replaces number the rows, fails without deleting them: when every partition has a static row, the scan reads
     * the whole SSTable. The stop comes once the scan has read its first partition, which is also its last one with a
     * single partition.
     */
    @Test
    public void testFullRebuildStoppedWhileLookingForAnEmptyStaticRow() throws Throwable
    {
        for (int partitions : new int[]{ ROWS_PER_SSTABLE, 1 })
            for (boolean nodetoolStop : new boolean[]{ true, false })
                testFullRebuildStoppedWhileLookingForAnEmptyStaticRow(partitions, nodetoolStop);
    }

    private void testFullRebuildStoppedWhileLookingForAnEmptyStaticRow(int partitions, boolean nodetoolStop) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s int STATIC, v int, w int, PRIMARY KEY (k, c))");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        for (int k = 0; k < partitions; k++)
            execute("INSERT INTO %s (k, c, s, v, w) VALUES (?, 0, 0, ?, ?)", k, k % 2, k % 2);
        flush();
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        Set<File> perSSTableFiles = loadDescriptor(sstable, cfs).perSSTableComponents()
                                                                .all()
                                                                .stream()
                                                                .map(IndexComponent::file)
                                                                .collect(Collectors.toSet());

        holdBuilders(InvokePointBuilder.newInvokePoint()
                                       .onClass(StorageAttachedIndexBuilder.class)
                                       .onMethod("numbersEmptyStaticRows")
                                       .atInvoke("advance"));
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForBuildersHeld();
        stop(cfs, nodetoolStop);
        holdBuilders = false;

        Throwable failure = rebuild.get(180, TimeUnit.SECONDS);
        String label = partitions + " partitions, " + (nodetoolStop ? "nodetool stop" : "interrupted");
        assertTrue(label + ": the rebuild must fail with the stop, but got " + failure,
                   failure != null && Throwables.anyCauseMatches(failure, t -> t instanceof CompactionInterruptedException));
        assertThat(perSSTableFiles).as(label).isNotEmpty().allMatch(File::exists);
        Injections.deleteAll();
        waitForIndexBuildsToSettle();
    }

    /**
     * A full rebuild that waits for the build lock of an SSTable (here held by the initial build of another index) and
     * is stopped meanwhile must fail too.
     */
    @Test
    public void testFullRebuildStoppedWhileWaitingForTheBuildLock() throws Throwable
    {
        for (boolean nodetoolStop : new boolean[]{ true, false })
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
            disableCompaction();
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            writeSSTables();

            // the initial build of w holds the (shared) build lock of an SSTable (only one builder of it is held, so
            // that the index build executor has threads left for the rebuild)
            holdFirstBuilderOnly = true;
            holdBuilders("completeSSTable");
            String w = createIndexAsync("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
            waitForBuildersHeld();

            // the full rebuild of v waits for it (it takes the lock exclusively)
            CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
            Thread.sleep(3000);
            stop(cfs, nodetoolStop);
            Throwable failure = rebuild.get(180, TimeUnit.SECONDS);
            holdBuilders = false;
            waitForIndexBuilds(KEYSPACE, w);

            String label = nodetoolStop ? "nodetool stop" : "interrupted";
            assertNotNull(label + ": the stopped rebuild must fail", failure);
            assertFalse(label + ": v must not be queryable without the SSTables its rebuild didn't reach",
                        cfs.indexManager.isIndexQueryable(v));
            Injections.deleteAll();
            holdFirstBuilderOnly = false;

            waitForIndexBuildsToSettle();
            cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(v));
            assertTrue(cfs.indexManager.isIndexQueryable(v));
            assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
        }
    }

    /**
     * A truncate stops a full rebuild, and removes the SSTables the rebuild hadn't built: nothing is missing from the
     * index, so the rebuild completes, and the index stays queryable, and right.
     */
    @Test
    public void testFullRebuildStoppedByATruncate() throws Throwable
    {
        // before the writer of an SSTable, between two SSTables, and while writing (the writer aborts)
        for (String holdAt : new String[]{ "indexSSTable", "completeSSTable", "indexPartitions" })
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
            disableCompaction();
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
            writeSSTables();

            holdBuilders(holdAt);
            CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
            waitForBuildersHeld();
            truncateWhileBuildersAreHeld(cfs);

            assertNull("held at " + holdAt + ": the rebuild stopped by the truncate must complete", rebuild.get(180, TimeUnit.SECONDS));
            Injections.deleteAll();
            waitForIndexBuildsToSettle();
            assertTrue("held at " + holdAt + ": v must stay queryable", cfs.indexManager.isIndexQueryable(v));
            assertEquals(0, rows("v"));
            assertIndexesWorkAfterTheTruncate(cfs);
        }
    }

    /**
     * The same for the initial build of an index (held where it holds its SSTable: a build that starts after the truncate
     * has removed its SSTable just skips it).
     */
    @Test
    public void testInitialBuildStoppedByATruncate() throws Throwable
    {
        // while writing (the writer aborts), and between two SSTables
        for (String holdAt : new String[]{ "indexPartitions", "completeSSTable" })
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
            disableCompaction();
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            writeSSTables();

            holdBuilders(holdAt);
            String w = createIndexAsync("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
            waitForBuildersHeld();
            truncateWhileBuildersAreHeld(cfs);

            // (the truncate makes the indexes queryable: wait for the end of the build, which could undo it)
            waitForAssert(() -> assertFalse(cfs.indexManager.isIndexBuilding(w)), 180, TimeUnit.SECONDS);
            waitForIndexBuildsToSettle();
            Injections.deleteAll();
            assertTrue("held at " + holdAt + ": w must be queryable", cfs.indexManager.isIndexQueryable(w));
            assertEquals(0, rows("w"));
            assertIndexesWorkAfterTheTruncate(cfs);
        }
    }

    /**
     * A stop that a truncate issues, but the SSTables the rebuild hadn't built stay (here, there is no truncate; it
     * could also fail, or give up because other operations of the table don't stop): the rebuild fails, as any stop.
     */
    @Test
    public void testFullRebuildStoppedByATruncateThatRemovesNothing() throws Throwable
    {
        long wait = StorageAttachedIndexBuilder.TRUNCATED_SSTABLES_WAIT_MILLIS;
        StorageAttachedIndexBuilder.TRUNCATED_SSTABLES_WAIT_MILLIS = 2000;
        try
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
            disableCompaction();
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
            writeSSTables();

            holdBuilders("indexSSTable");
            CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
            waitForBuildersHeld();
            CompactionManager.instance.interruptCompactionFor(Collections.singleton(cfs.metadata()),
                                                              Predicates.alwaysTrue(),
                                                              Predicates.alwaysTrue(),
                                                              false,
                                                              TableOperation.StopTrigger.TRUNCATE);
            holdBuilders = false;

            assertNotNull("the stopped rebuild must fail", rebuild.get(180, TimeUnit.SECONDS));
            assertFalse("v must not be queryable without the SSTables its rebuild didn't reach", cfs.indexManager.isIndexQueryable(v));
            Injections.deleteAll();

            waitForIndexBuildsToSettle();
            cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(v));
            assertTrue(cfs.indexManager.isIndexQueryable(v));
            assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
        }
        finally
        {
            StorageAttachedIndexBuilder.TRUNCATED_SSTABLES_WAIT_MILLIS = wait;
        }
    }

    /**
     * A stop that a truncate issues while the SSTables the rebuild hadn't built are out of the live set (an operation
     * that opens its results early has read them all) without having been removed: the rebuild fails, as any stop.
     */
    @Test
    public void testFullRebuildStoppedByATruncateWhileItsSSTablesAreOutOfTheLiveSet() throws Throwable
    {
        long wait = StorageAttachedIndexBuilder.TRUNCATED_SSTABLES_WAIT_MILLIS;
        StorageAttachedIndexBuilder.TRUNCATED_SSTABLES_WAIT_MILLIS = 2000;
        try
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
            disableCompaction();
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
            writeSSTables();

            holdBuilders("indexSSTable");
            CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
            waitForBuildersHeld();

            // what an operation that opens its results early does with the SSTables it has read all of
            LifecycleTransaction compaction = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
            assertNotNull(compaction);
            try
            {
                compaction.originals().forEach(compaction::obsolete);
                compaction.checkpoint();
                assertThat(cfs.getLiveSSTables()).isEmpty();

                CompactionManager.instance.interruptCompactionFor(Collections.singleton(cfs.metadata()),
                                                                  Predicates.alwaysTrue(),
                                                                  Predicates.alwaysTrue(),
                                                                  false,
                                                                  TableOperation.StopTrigger.TRUNCATE);
                holdBuilders = false;
                assertNotNull("the stopped rebuild must fail", rebuild.get(180, TimeUnit.SECONDS));
            }
            finally
            {
                // it aborts, and its SSTables are live again
                compaction.abort();
            }
            assertThat(cfs.getLiveSSTables()).hasSize(SSTABLES);
            assertFalse("v must not be queryable without the SSTables its rebuild didn't reach", cfs.indexManager.isIndexQueryable(v));
            Injections.deleteAll();

            waitForIndexBuildsToSettle();
            cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(v));
            assertTrue(cfs.indexManager.isIndexQueryable(v));
            assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
        }
        finally
        {
            StorageAttachedIndexBuilder.TRUNCATED_SSTABLES_WAIT_MILLIS = wait;
        }
    }

    /**
     * A build stopped by a truncate waits for the truncate to be over (here, an operation that issues the stop with the
     * TRUNCATE trigger), and no longer: it then fails if the truncate removed nothing.
     */
    @Test
    public void testFullRebuildStoppedByATruncateWaitsForItToBeOver() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        writeSSTables();

        holdBuilders("indexSSTable");
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForBuildersHeld();

        // a "truncate" that stops the builds, and then takes a while and removes nothing
        CountDownLatch truncating = new CountDownLatch(1);
        CountDownLatch over = new CountDownLatch(1);
        CompletableFuture<Object> truncate = CompletableFuture.supplyAsync(() -> cfs.runWithCompactionsDisabled(() -> {
            truncating.countDown();
            over.await();
            return null;
        }, OperationType.P0, true, true, TableOperation.StopTrigger.TRUNCATE));
        assertTrue(truncating.await(60, TimeUnit.SECONDS));
        holdBuilders = false;

        // the rebuild waits for it...
        Thread.sleep(3000);
        assertFalse("the rebuild must wait for the truncate", rebuild.isDone());
        over.countDown();
        truncate.get(60, TimeUnit.SECONDS);

        // ...and fails once it is over, long before the timeout
        assertNotNull("the stopped rebuild must fail", rebuild.get(30, TimeUnit.SECONDS));
        assertFalse(cfs.indexManager.isIndexQueryable(v));
        Injections.deleteAll();

        waitForIndexBuildsToSettle();
        cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(v));
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
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

        holdBuilders("indexPartitions");
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForBuildersHeld();
        stop(cfs, true);
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

        holdBuilders("indexPartitions");
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

    /**
     * SSTables restored by an aborted operation while a full rebuild of v writes them are loaded again without the
     * components being written; once the rebuild completes them, v includes them and is queryable.
     */
    @Test
    public void testSSTablesLoadedAgainDuringAFullRebuild() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        writeSSTables();
        StorageAttachedIndex index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(v);

        holdBuilders("indexPartitions");
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForBuildersHeld();

        // an operation that has read all of the SSTables (and so taken them out of the live set) aborts
        LifecycleTransaction compaction = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        assertNotNull(compaction);
        compaction.originals().forEach(compaction::obsolete);
        compaction.checkpoint();
        compaction.abort();
        assertThat(cfs.getLiveSSTables()).hasSize(SSTABLES);

        holdBuilders = false;
        assertThat(rebuild.get(180, TimeUnit.SECONDS)).isNull();
        Injections.deleteAll();
        waitForIndexBuildsToSettle();

        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertThat(index.getIndexContext().getView().getIndexes()).hasSize(SSTABLES);
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
        // the SSTables are served from the components on disk, which are those the rebuild wrote
        for (SSTableIndex sstableIndex : index.getIndexContext().getView().getIndexes())
        {
            IndexDescriptor onDisk = IndexDescriptor.load(sstableIndex.getSSTable(), Collections.singleton(index.getIndexContext()));
            assertEquals(onDisk.perIndexComponents(index.getIndexContext()).buildId(), sstableIndex.usedPerIndexComponents().buildId());
        }
        // w didn't serve the SSTables the rebuild was writing when they were loaded again (without immutable
        // components): it is not queryable, or it has them
        assertTrue("w must not be queryable without some of its SSTables",
                   !cfs.indexManager.isIndexQueryable(w) || rows("w") == SSTABLES * ROWS_PER_SSTABLE);
    }

    /**
     * A full rebuild checks that its index has every live SSTable in its view when it is over: an SSTable it built
     * that is loaded again without per-SSTable components fails the rebuild.
     */
    @Test
    public void testFullRebuildChecksTheViewOfItsIndexWhenItIsOver() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        writeSSTables();
        StorageAttachedIndex index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(v);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);

        // one builder of the rebuild is held, the others build their SSTables
        holdFirstBuilderOnly = true;
        holdBuilders("indexSSTable");
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForBuildersHeld();
        // (the rebuild took the SSTables out of v's view: those in it now are built)
        waitForAssert(() -> assertThat(index.getIndexContext().getView().getIndexes()).isNotEmpty(), 120, TimeUnit.SECONDS);
        SSTableReader built = index.getIndexContext().getView().getIndexes().iterator().next().getSSTable();

        // that SSTable loses its per-SSTable components, and is loaded again
        group.descriptorFor(built).perSSTableComponents().invalidate(built, cfs.getTracker());
        group.onSSTableChanged(Collections.singleton(built), Collections.singleton(built), group.getIndexes(), false);
        assertFalse(index.getIndexContext().getView().containsSSTableIndex(built.descriptor));

        holdBuilders = false;
        Throwable failure = rebuild.get(180, TimeUnit.SECONDS);
        Injections.deleteAll();
        holdFirstBuilderOnly = false;
        waitForIndexBuildsToSettle();
        assertNotNull("the rebuild must fail", failure);
        assertThat(failure).hasStackTraceContaining("aren't in its view");
        assertFalse("v must not be queryable without the SSTable", cfs.indexManager.isIndexQueryable(v));

        // a rebuild brings it back
        cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(v));
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
    }

    /**
     * While a full rebuild of v is between deleting the per-SSTable components of an SSTable and those of v, a load of
     * the SSTables doesn't flag v as unable to serve it: the rebuild is building it and checks it when it is over.
     */
    @Test
    public void testIndexUnderFullRebuildIsNotFlaggedWhenItsSSTablesAreLoaded() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        writeSSTables();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);

        // a builder of the rebuild is held once it has deleted the per-SSTable components of its first SSTable
        holdFirstBuilderOnly = true;
        buildersHeld.set(0);
        holdBuilders = true;
        Injections.inject(Injections.newCustom("hold_after_preparing_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                           .onMethod("prepareForRebuild")
                                                           .atExit())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(Expression.expr(StoppedIndexBuildTest.class.getName())
                                                                          .method("holdBuilder")
                                                                          .args()))
                                    .build());
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForBuildersHeld();

        // the SSTables of the table are loaded (as for the initial build of a new index, whose view they are not in;
        // the indexes that serve them don't load them again)
        group.onSSTableChanged(Collections.emptyList(), cfs.getLiveSSTables(), Collections.emptySet(), false);
        assertEquals("v is being rebuilt, and must not be published as failed",
                     Index.Status.FULL_REBUILD_STARTED,
                     IndexStatusManager.instance.getIndexStatus(FBUtilities.getBroadcastAddressAndPort(), KEYSPACE, v));
        assertTrue(cfs.indexManager.isIndexQueryable(w));

        holdBuilders = false;
        assertThat(rebuild.get(180, TimeUnit.SECONDS)).isNull();
        Injections.deleteAll();
        holdFirstBuilderOnly = false;
        waitForIndexBuildsToSettle();
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertTrue(cfs.indexManager.isIndexQueryable(w));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("w"));
    }

    /**
     * The second look of the end check of a full rebuild is only at the SSTables missing at the first; an SSTable
     * flushed in between is added to the view by its own notification, and the rebuild succeeds.
     */
    @Test
    public void testEndCheckOfAFullRebuildIgnoresSSTablesAddedSinceItsFirstLook() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        writeSSTables();

        checkHeld = false;
        secondLookHeld = false;
        notificationsHeld.set(0);
        holdCheck = true;
        Injections.inject(Injections.newCustom("hold_check_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.FullRebuild.class).onMethod("checkViews"))
                                    .add(ActionBuilder.newActionBuilder().actions()
                                                      .doAction(Expression.expr(StoppedIndexBuildTest.class.getName()).method("holdCheck").args()))
                                    .build());
        Injections.inject(Injections.newCustom("hold_second_look_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.FullRebuild.class).onMethod("checkViews")
                                                           .after("INVOKE onSSTableChanged"))
                                    .add(ActionBuilder.newActionBuilder().actions()
                                                      .doAction(Expression.expr(StoppedIndexBuildTest.class.getName()).method("holdSecondLook").args()))
                                    .build());
        Injections.inject(Injections.newCustom("hold_notification_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexGroup.class).onMethod("handleNotification"))
                                    .add(ActionBuilder.newActionBuilder().actions()
                                                      .doAction(Expression.expr(StoppedIndexBuildTest.class.getName()).method("holdNotification").args("$1")))
                                    .build());
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForAssert(() -> assertTrue(checkHeld), 120, TimeUnit.SECONDS);

        // a flush adds an SSTable; the group isn't told yet
        holdNotifications = true;
        CompletableFuture<Void> flush1 = flushAsync(SSTABLES * ROWS_PER_SSTABLE);
        waitForAssert(() -> assertThat(notificationsHeld.get()).isEqualTo(1), 120, TimeUnit.SECONDS);
        // the check looks, loads it, and is about to look again
        holdSecondLook = true;
        holdCheck = false;
        waitForAssert(() -> assertTrue(secondLookHeld), 120, TimeUnit.SECONDS);
        // the group is told of the first flush; a second flush adds another SSTable, and the group isn't told yet
        holdNotifications = false;
        flush1.get(120, TimeUnit.SECONDS);
        holdNotifications = true;
        CompletableFuture<Void> flush2 = flushAsync((SSTABLES + 1) * ROWS_PER_SSTABLE);
        waitForAssert(() -> assertThat(notificationsHeld.get()).isEqualTo(2), 120, TimeUnit.SECONDS);

        holdSecondLook = false;
        Throwable failure = rebuild.get(120, TimeUnit.SECONDS);
        holdNotifications = false;
        flush2.get(120, TimeUnit.SECONDS);
        Injections.deleteAll();
        assertNull("the rebuild must succeed", failure);
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertEquals((SSTABLES + 2) * ROWS_PER_SSTABLE, rows("v"));
    }

    private CompletableFuture<Void> flushAsync(int firstKey)
    {
        for (int k = firstKey; k < firstKey + ROWS_PER_SSTABLE; k++)
            execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 2, k % 2);
        return CompletableFuture.runAsync(this::flush);
    }

    /**
     * A partial build writes an SSTable that is loaded again meanwhile, from the disk as it was: the SSTable is not in
     * the index's view after the build, so the build fails and leaves the index non-queryable.
     */
    @Test
    public void testPartialBuildOfAnSSTableLoadedAgainMeanwhileFails() throws Throwable
    {
        partialBuildOfAnSSTableLoadedAgainMeanwhile(false);
    }

    /**
     * The same, with the abort of the build's writer skipped: the check that finds the SSTable out of the index's view
     * makes the index non-queryable itself, as it must for an index the build added by itself.
     */
    @Test
    public void testIndexOfAPartialBuildThatFindsItsSSTableOutOfTheViewIsMadeNonQueryable() throws Throwable
    {
        partialBuildOfAnSSTableLoadedAgainMeanwhile(true);
    }

    private void partialBuildOfAnSSTableLoadedAgainMeanwhile(boolean skipTheWritersAbort) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        String w = createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        for (int k = 0; k < ROWS_PER_SSTABLE; k++)
            execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 2, k % 2);
        flush();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        StorageAttachedIndex wIndex = (StorageAttachedIndex) cfs.indexManager.getIndexByName(w);
        // w is missing on the SSTable, and a partial build builds it there
        group.descriptorFor(sstable).perIndexComponents(wIndex.getIndexContext()).invalidate(sstable, cfs.getTracker());

        holdBuilders("indexPartitions");
        CompletableFuture<Throwable> build = CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, w));
        waitForBuildersHeld();

        // the SSTable is loaded again
        LifecycleTransaction compaction = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        assertNotNull(compaction);
        compaction.originals().forEach(compaction::obsolete);
        compaction.checkpoint();
        compaction.abort();

        if (skipTheWritersAbort)
        {
            ActionBuilder skip = ActionBuilder.newActionBuilder();
            skip.actions().doReturn("");
            Injections.inject(Injections.newCustom("skip_writer_abort_" + System.nanoTime())
                                        .add(InvokePointBuilder.newInvokePoint()
                                                               .onClass(StorageAttachedIndexWriter.class)
                                                               .onMethod("abort"))
                                        .add(skip)
                                        .build());
        }
        holdBuilders = false;
        Throwable failure = build.get(120, TimeUnit.SECONDS);
        Injections.deleteAll();
        assertNotNull("the build must fail", failure);
        assertThat(failure).hasStackTraceContaining("Failed to update views");
        assertFalse("w must not be queryable without the SSTable", cfs.indexManager.isIndexQueryable(w));
    }

    /**
     * The record the group keeps of a full rebuild is cleared by its builders: one whose builders never ran (e.g.
     * queued when their executor was shut down) is never cleared. It must not hide, for good, an SSTable the index
     * lost: the group skips an index under a full rebuild only while the manager counts it as building.
     */
    @Test
    public void testRecordOfAFullRebuildThatNeverRanHidesNothing() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        for (int k = 0; k < ROWS_PER_SSTABLE; k++)
            execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 2, k % 2);
        flush();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        Set<StorageAttachedIndex> vIndex = Set.of((StorageAttachedIndex) cfs.indexManager.getIndexByName(v));

        group.fullRebuildStarted(vIndex);
        try
        {
            assertFalse(cfs.indexManager.isIndexBuilding(v));
            // the SSTable loses its per-SSTable components, and is loaded again
            group.descriptorFor(sstable).perSSTableComponents().invalidate(sstable, cfs.getTracker());
            group.onSSTableChanged(Collections.singleton(sstable), Collections.singleton(sstable), group.getIndexes(), false);
            assertFalse("v can't serve the SSTable, and isn't being built: it must not stay queryable without it",
                        cfs.indexManager.isIndexQueryable(v));
        }
        finally
        {
            group.fullRebuildEnded(vIndex);
        }
    }

    private void truncateWhileBuildersAreHeld(ColumnFamilyStore cfs) throws Exception
    {
        CompletableFuture<Void> truncate = CompletableFuture.runAsync(cfs::truncateBlockingWithoutSnapshot);
        // (the truncate doesn't wait for the index builds it stops)
        truncate.get(180, TimeUnit.SECONDS);
        assertThat(cfs.getLiveSSTables()).isEmpty();
        holdBuilders = false;
    }

    private void assertIndexesWorkAfterTheTruncate(ColumnFamilyStore cfs)
    {
        for (int k = 0; k < ROWS_PER_SSTABLE; k++)
            execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 2, k % 2);
        flush();
        assertEquals(ROWS_PER_SSTABLE, rows("v"));
        assertEquals(ROWS_PER_SSTABLE, rows("w"));
    }

    /**
     * A thread interrupt of a builder of a full rebuild that waits for the build lock of an SSTable: the rebuild fails
     * and returns, leaving the index failed rather than "building", so that a rebuild can then fix it.
     */
    @Test
    public void testFullRebuildInterruptedWhileWaitingForTheBuildLock() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        writeSSTables();

        holdFirstBuilderOnly = true;
        holdBuilders("completeSSTable");
        String w = createIndexAsync("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        waitForBuildersHeld();

        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        // interrupt the builder of the rebuild once it waits for the lock
        waitForAssert(() -> assertThat(interruptBuildersIn("lockForBuild")).isGreaterThan(0), 120, TimeUnit.SECONDS);
        Throwable failure = rebuild.get(120, TimeUnit.SECONDS);
        holdBuilders = false;
        waitForIndexBuilds(KEYSPACE, w);

        assertNotNull("the interrupted rebuild must fail", failure);
        assertFalse("v must not be building anymore", cfs.indexManager.isIndexBuilding(v));
        assertFalse("v must not be queryable without the SSTables its rebuild didn't reach", cfs.indexManager.isIndexQueryable(v));
        Injections.deleteAll();
        holdFirstBuilderOnly = false;

        waitForIndexBuildsToSettle();
        cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(v));
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertEquals(SSTABLES * ROWS_PER_SSTABLE, rows("v"));
    }

    private static int interruptBuildersIn(String method)
    {
        int interrupted = 0;
        for (Map.Entry<Thread, StackTraceElement[]> thread : Thread.getAllStackTraces().entrySet())
        {
            boolean inBuilder = false, inMethod = false;
            for (StackTraceElement frame : thread.getValue())
            {
                inBuilder |= frame.getClassName().equals(StorageAttachedIndexBuilder.class.getName()) && frame.getMethodName().equals("indexSSTable");
                inMethod |= frame.getMethodName().equals(method);
            }
            if (inBuilder && inMethod)
            {
                thread.getKey().interrupt();
                interrupted++;
            }
        }
        return interrupted;
    }

    private void waitForIndexBuildsToSettle()
    {
        waitForAssert(() -> assertThat(CompactionManager.instance.active.getTableOperations()
                                                                       .stream()
                                                                       .filter(op -> op.getProgress().operationType() == OperationType.INDEX_BUILD)
                                                                       .count()).isZero(),
                      120, TimeUnit.SECONDS);
    }

    private void holdBuilders(String method) throws Throwable
    {
        holdBuilders(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod(method));
    }

    private void holdBuilders(InvokePointBuilder at) throws Throwable
    {
        buildersHeld.set(0);
        holdBuilders = true;
        Injections.inject(Injections.newCustom("hold_builders_" + System.nanoTime())
                                    .add(at)
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

    private static void stop(ColumnFamilyStore cfs, boolean nodetoolStop)
    {
        if (nodetoolStop)
            CompactionManager.instance.stopCompaction("INDEX_BUILD");
        else
            CompactionManager.instance.interruptCompactionFor(Collections.singleton(cfs.metadata()),
                                                              Predicates.alwaysTrue(),
                                                              Predicates.alwaysTrue(),
                                                              false,
                                                              TableOperation.StopTrigger.CLEANUP);
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
