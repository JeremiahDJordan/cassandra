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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SSTables that are added to a table without the components of one of its indexes, like the SSTables of a stream whose
 * writers were created before the index, must not be silently skipped by that index's queries.
 */
public class MissingIndexOnAddedSSTablesTest extends SAITester
{
    private static final int ROWS = 10;

    private boolean incrementalBackups;

    // the thread whose check after adding is held once it has found the SSTables missing (see holdCheck)
    private static volatile Thread heldCheck;
    private static volatile CountDownLatch checkHeld;
    private static volatile CountDownLatch releaseCheck;

    // called by Byteman, when StorageAttachedIndexGroup#unindexedLiveSSTables returns
    public static void holdCheck(List<?> missing) throws InterruptedException
    {
        if (Thread.currentThread() != heldCheck || missing.isEmpty())
            return;
        checkHeld.countDown();
        assertThat(releaseCheck.await(60, TimeUnit.SECONDS)).isTrue();
    }

    @Before
    public void disableIncrementalBackups()
    {
        // The SSTables are added to the table twice, which incremental backups don't support.
        incrementalBackups = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
    }

    @After
    public void cleanUp()
    {
        Injections.deleteAll();
        DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackups);
    }

    @Test
    public void testBuildMissingIndexBeforeAdding()
    {
        testBuildMissingIndexBeforeAdding(false);
    }

    @Test
    public void testBuildMissingIndexBeforeAddingWithExistingIndex()
    {
        testBuildMissingIndexBeforeAdding(true);
    }

    private void testBuildMissingIndexBeforeAdding(boolean existingIndex)
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        if (existingIndex)
            createIndex(String.format(CREATE_INDEX_TEMPLATE, "w"));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = writeSSTablesNotAdded(0);

        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForTableIndexesQueryable();
        IndexContext context = getIndexContexts(KEYSPACE, currentTable()).stream()
                                                                         .filter(c -> c.getColumnName().equals("v"))
                                                                         .findFirst()
                                                                         .orElseThrow();
        for (SSTableReader sstable : sstables)
            assertThat(IndexDescriptor.isIndexBuildCompleteOnDisk(sstable, context)).isFalse();

        cfs.indexManager.buildMissingSSTableAttachedIndexes(sstables);

        // The index is built on the SSTables, but they don't become visible to it before they are added...
        for (SSTableReader sstable : sstables)
        {
            assertThat(IndexDescriptor.isIndexBuildCompleteOnDisk(sstable, context)).isTrue();
            assertThat(context.getView().containsSSTableIndex(sstable.descriptor)).isFalse();
        }
        assertThat(execute("SELECT k FROM %s WHERE v = 0")).isEmpty();

        // ...and then they do.
        cfs.addSSTables(sstables, OperationType.STREAM);
        cfs.indexManager.checkSSTableAttachedIndexesAfterAdding(sstables);
        for (SSTableReader sstable : sstables)
            assertThat(context.getView().containsSSTableIndex(sstable.descriptor)).isTrue();
        assertThat(cfs.indexManager.isIndexQueryable(context.getIndexName())).isTrue();
        assertThat(execute("SELECT k FROM %s WHERE v = 0")).hasSize(ROWS / 2);
        assertThat(execute("SELECT k FROM %s WHERE v = 1")).hasSize(ROWS / 2);
        if (existingIndex)
            assertThat(execute("SELECT k FROM %s WHERE w = 0")).hasSize(ROWS / 2);
    }

    /**
     * SSTables that are added without the components of an index that isn't being built make that index
     * non-queryable, rather than letting it silently miss their rows.
     */
    @Test
    public void testSSTablesAddedWithoutIndexMakeItNonQueryable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = writeSSTablesNotAdded(0);

        String index = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForTableIndexesQueryable();
        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();

        cfs.addSSTables(sstables, OperationType.STREAM);
        cfs.indexManager.checkSSTableAttachedIndexesAfterAdding(sstables);

        assertThat(cfs.indexManager.isIndexQueryable(index)).isFalse();
        assertThatThrownBy(() -> execute("SELECT k FROM %s WHERE v = 0")).hasMessageContaining("INDEX_NOT_AVAILABLE");
    }

    /**
     * SSTables that are added without an index while its initial build is running, after it has selected the SSTables
     * to build, are built by that initial build before the index becomes queryable.
     */
    @Test
    public void testSSTablesAddedDuringInitialBuildAreBuilt() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> streamed = writeSSTablesNotAdded(0);
        // a live SSTable for the initial build to build
        writeRows(100);

        Injections.Barrier holdBuild = barrier("hold_initial_build",
                                               InvokePointBuilder.newInvokePoint()
                                                                 .onClass(StorageAttachedIndexBuilder.class)
                                                                 .onMethod("build"));
        Injections.inject(holdBuild);

        // The index is created, and its initial build has selected the live SSTables to build...
        String index = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForAssert(() -> assertThat(holdBuild.getCount()).isEqualTo(1));

        // ...when the SSTables are added without it.
        cfs.addSSTables(streamed, OperationType.STREAM);
        cfs.indexManager.checkSSTableAttachedIndexesAfterAdding(streamed);
        assertThat(cfs.indexManager.isIndexQueryable(index)).isFalse();

        holdBuild.countDown();
        holdBuild.disable();
        waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse(), 60, TimeUnit.SECONDS);

        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
        assertThat(execute("SELECT k FROM %s")).hasSize(2 * ROWS);
        assertThat(rowsThroughV()).isEqualTo(2 * ROWS);
    }

    /**
     * SSTables that are added without an index while its initial build is running, before it selects the SSTables to
     * build, are just built by it.
     */
    @Test
    public void testSSTablesAddedBeforeInitialBuildSelectsSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> streamed = writeSSTablesNotAdded(0);

        Injections.Barrier holdInitialBuild = barrier("hold_initial_build_start",
                                                      InvokePointBuilder.newInvokePoint()
                                                                        .onClass(StorageAttachedIndex.class)
                                                                        .onMethod("startInitialBuild"));
        Injections.inject(holdInitialBuild);
        String index = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForAssert(() -> assertThat(holdInitialBuild.getCount()).isEqualTo(1));

        cfs.addSSTables(streamed, OperationType.STREAM);
        cfs.indexManager.checkSSTableAttachedIndexesAfterAdding(streamed);

        holdInitialBuild.countDown();
        holdInitialBuild.disable();
        waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse(), 60, TimeUnit.SECONDS);
        // let the check that follows the build run
        Thread.sleep(500);

        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
        assertThat(rowsThroughV()).isEqualTo(ROWS);
    }

    /**
     * SSTables that are added without an index while its initial build is running, before it selects the SSTables to
     * build, and that the check after adding finds missing while the build runs, but looks at again only once the build
     * is over: the build built them, so the index stays queryable.
     */
    @Test
    public void testSSTablesBuiltByInitialBuildDuringCheckAfterAdding() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> streamed = writeSSTablesNotAdded(0);

        Injections.Hold holdInitialBuild = Injections.newHold("initial_build_start")
                                                     .add(InvokePointBuilder.newInvokePoint()
                                                                            .onClass(StorageAttachedIndex.class)
                                                                            .onMethod("startInitialBuild"))
                                                     .build();
        Injection holdCheck = Injections.newCustom("hold_check_after_adding")
                                        .add(InvokePointBuilder.newInvokePoint()
                                                               .onClass(StorageAttachedIndexGroup.class)
                                                               .onMethod("unindexedLiveSSTables")
                                                               .atExit())
                                        .add(ActionBuilder.newActionBuilder()
                                                          .actions()
                                                          .doAction(Expression.expr(MissingIndexOnAddedSSTablesTest.class.getName())
                                                                              .method("holdCheck")
                                                                              .args("$!")))
                                        .build();
        checkHeld = new CountDownLatch(1);
        releaseCheck = new CountDownLatch(1);
        Injections.inject(holdInitialBuild, holdCheck);
        try
        {
            String index = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v"));
            holdInitialBuild.awaitHeld();

            cfs.addSSTables(streamed, OperationType.STREAM);
            CompletableFuture<Void> check = new CompletableFuture<>();
            heldCheck = new Thread(() -> {
                try
                {
                    cfs.indexManager.checkSSTableAttachedIndexesAfterAdding(streamed);
                    check.complete(null);
                }
                catch (Throwable t)
                {
                    check.completeExceptionally(t);
                }
            });
            heldCheck.start();
            assertThat(checkHeld.await(60, TimeUnit.SECONDS)).isTrue();

            // the initial build is still held, so the check found the SSTables missing before the build selected them
            holdInitialBuild.assertHeld();
            assertThat(cfs.indexManager.isIndexBuilding(index)).isTrue();
            holdInitialBuild.release();
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse(), 60, TimeUnit.SECONDS);
            assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();

            assertThat(check).isNotDone();
            releaseCheck.countDown();
            check.get(60, TimeUnit.SECONDS);

            assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
            assertThat(rowsThroughV()).isEqualTo(ROWS);
        }
        finally
        {
            Injections.Hold.releaseAll();
            releaseCheck.countDown();
            heldCheck = null;
        }
    }

    /**
     * SSTables that are added without an index while its initial build is running, after it has built the SSTables
     * handed to it, are built by its last look for SSTables still without the index.
     */
    @Test
    public void testSSTablesAddedAfterHandOffAreBuiltByInitialBuild() throws Throwable
    {
        ColumnFamilyStore cfs = addSSTablesAtEndOfInitialBuild("buildSSTablesAddedDuringInitialBuild");

        assertThat(cfs.indexManager.isIndexQueryable(getIndexContexts(KEYSPACE, currentTable()).iterator().next().getIndexName())).isTrue();
        assertThat(rowsThroughV()).isEqualTo(ROWS);
    }

    /**
     * SSTables that are added without an index while its initial build is running, but too late for it to build them,
     * make the index non-queryable once the build is over, although the build makes it queryable.
     */
    @Test
    public void testSSTablesAddedAtEndOfInitialBuildMakeIndexNonQueryable() throws Throwable
    {
        ColumnFamilyStore cfs = addSSTablesAtEndOfInitialBuild("buildSSTablesMissedByInitialBuild");
        String index = getIndexContexts(KEYSPACE, currentTable()).iterator().next().getIndexName();

        waitForAssert(() -> assertThat(cfs.indexManager.isIndexQueryable(index)).isFalse());
        assertThatThrownBy(() -> execute("SELECT k FROM %s WHERE v = 0")).hasMessageContaining("INDEX_NOT_AVAILABLE");
    }

    /**
     * Adds SSTables without an index while its initial build is held at the exit of the specified method, and returns
     * once the build is over.
     */
    private ColumnFamilyStore addSSTablesAtEndOfInitialBuild(String method) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> streamed = writeSSTablesNotAdded(0);

        Injections.Barrier holdEndOfBuild = barrier("hold_end_of_initial_build",
                                                    InvokePointBuilder.newInvokePoint()
                                                                      .onClass(StorageAttachedIndex.class)
                                                                      .onMethod(method)
                                                                      .atExit());
        Injections.inject(holdEndOfBuild);
        String index = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForAssert(() -> assertThat(holdEndOfBuild.getCount()).isEqualTo(1));

        cfs.addSSTables(streamed, OperationType.STREAM);
        cfs.indexManager.checkSSTableAttachedIndexesAfterAdding(streamed);

        holdEndOfBuild.countDown();
        holdEndOfBuild.disable();
        waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(index)).isFalse(), 60, TimeUnit.SECONDS);
        return cfs;
    }

    /**
     * A build that is stopped (nodetool stop INDEX_BUILD) fails, so that the SSTables aren't added without the index,
     * and doesn't affect the index, which doesn't need them.
     */
    @Test
    public void testStoppedBuildFailsAndKeepsIndexQueryable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = writeSSTablesNotAdded(0);
        writeRows(100);
        String index = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForTableIndexesQueryable();
        int descriptors = StorageAttachedIndexGroup.getIndexGroup(cfs).sstableContextManager().getDescriptorCount();

        Injections.Barrier holdBuild = barrier("hold_build",
                                               InvokePointBuilder.newInvokePoint()
                                                                 .onClass(StorageAttachedIndexBuilder.class)
                                                                 .onMethod("indexSSTable"));
        Injections.inject(holdBuild);
        CompletableFuture<Void> build = CompletableFuture.runAsync(() -> cfs.indexManager.buildMissingSSTableAttachedIndexes(sstables));
        waitForAssert(() -> assertThat(holdBuild.getCount()).isEqualTo(1));
        CompactionManager.instance.stopCompaction("INDEX_BUILD");
        holdBuild.countDown();
        holdBuild.disable();

        assertThatThrownBy(() -> build.get(60, TimeUnit.SECONDS)).hasRootCauseInstanceOf(CompactionInterruptedException.class);
        assertBuildLeftIndexAlone(cfs, index, sstables, descriptors);
        sstables.forEach(sstable -> sstable.selfRef().release());
    }

    /**
     * A build that fails fails the operation that was going to add the SSTables, and doesn't affect the index.
     */
    @Test
    public void testFailedBuildKeepsIndexQueryable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = writeSSTablesNotAdded(0);
        writeRows(100);
        String index = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForTableIndexesQueryable();
        int descriptors = StorageAttachedIndexGroup.getIndexGroup(cfs).sstableContextManager().getDescriptorCount();

        Injection failBuild = Injections.newCustom("fail_build")
                                        .add(InvokePointBuilder.newInvokePoint()
                                                               .onClass(StorageAttachedIndexWriter.class)
                                                               .onMethod("complete"))
                                        .add(ActionBuilder.newActionBuilder()
                                                          .actions()
                                                          .doThrow(RuntimeException.class, Expression.quote("Injected failure!")))
                                        .build();
        Injections.inject(failBuild);
        try
        {
            assertThatThrownBy(() -> cfs.indexManager.buildMissingSSTableAttachedIndexes(sstables)).hasStackTraceContaining("Injected failure!");
        }
        finally
        {
            failBuild.disable();
        }
        assertBuildLeftIndexAlone(cfs, index, sstables, descriptors);
        sstables.forEach(sstable -> sstable.selfRef().release());
    }

    /**
     * The same for a failure inside the writer (e.g. a full disk): it fails the operation that was going to add the
     * SSTables, and doesn't affect the index.
     */
    @Test
    public void testBuildFailedInsideTheWriterKeepsIndexQueryable() throws Throwable
    {
        for (String method : new String[]{ "addRow", "complete" })
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
            disableCompaction();
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            Set<SSTableReader> sstables = writeSSTablesNotAdded(0);
            writeRows(100);
            String index = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v"));
            waitForTableIndexesQueryable();
            int descriptors = StorageAttachedIndexGroup.getIndexGroup(cfs).sstableContextManager().getDescriptorCount();

            Injection failBuild = Injections.newCustom("fail_build_in_" + method)
                                            .add(InvokePointBuilder.newInvokePoint()
                                                                   .onClass(SSTableIndexWriter.class)
                                                                   .onMethod(method))
                                            .add(ActionBuilder.newActionBuilder()
                                                              .actions()
                                                              .doThrow(RuntimeException.class, Expression.quote("Injected failure!")))
                                            .build();
            Injections.inject(failBuild);
            try
            {
                assertThatThrownBy(() -> cfs.indexManager.buildMissingSSTableAttachedIndexes(sstables)).hasStackTraceContaining("Injected failure!");
            }
            finally
            {
                failBuild.disable();
            }
            assertBuildLeftIndexAlone(cfs, index, sstables, descriptors);
            sstables.forEach(sstable -> sstable.selfRef().release());
        }
    }

    private void assertBuildLeftIndexAlone(ColumnFamilyStore cfs, String index, Set<SSTableReader> sstables, int descriptors)
    {
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertThat(cfs.indexManager.isIndexQueryable(index)).isTrue();
        assertThat(rowsThroughV()).isEqualTo(ROWS);
        // nothing is cached for the SSTables that weren't added
        for (SSTableReader sstable : sstables)
            assertThat(group.sstableContextManager().contains(sstable)).isFalse();
        assertThat(group.sstableContextManager().getDescriptorCount()).isEqualTo(descriptors);
    }

    /**
     * Building SSTables that aren't live leaves nothing in the descriptor cache of the index group, whether they are
     * added afterwards or not.
     */
    @Test
    public void testBuildDoesNotCacheDescriptors()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "w"));
        Set<SSTableReader> added = writeSSTablesNotAdded(0);
        Set<SSTableReader> abandoned = writeSSTablesNotAdded(100);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForTableIndexesQueryable();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        int before = group.sstableContextManager().getDescriptorCount();

        cfs.indexManager.buildMissingSSTableAttachedIndexes(abandoned);
        cfs.indexManager.buildMissingSSTableAttachedIndexes(added);
        assertThat(group.sstableContextManager().getDescriptorCount()).isEqualTo(before);

        // the SSTables that are added are loaded with the components that have been built
        cfs.addSSTables(added, OperationType.STREAM);
        cfs.indexManager.checkSSTableAttachedIndexesAfterAdding(added);
        assertThat(group.sstableContextManager().getDescriptorCount()).isEqualTo(before + added.size());
        assertThat(execute("SELECT k FROM %s WHERE v = 0")).hasSize(ROWS / 2);
        assertThat(execute("SELECT k FROM %s WHERE w = 0")).hasSize(ROWS / 2);

        abandoned.forEach(sstable -> sstable.selfRef().release());
    }

    /**
     * Building SSTables that aren't live doesn't count the size of their components in the disk space used by the table
     * before they are added, when the whole SSTables are.
     */
    @Test
    public void testBuildDoesNotCountDiskSpaceTwice()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (int k = 0; k < 1000; k++)
            execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k, k);
        flush();
        Set<SSTableReader> sstables = removeFromTable();
        // removeUnsafe leaves the metrics alone: take the SSTables out of them, as if they had never been added
        long dataBytes = sstables.stream().mapToLong(SSTableReader::bytesOnDisk).sum();
        cfs.metric.liveDiskSpaceUsed.dec(dataBytes);
        cfs.metric.totalDiskSpaceUsed.dec(dataBytes);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v"));
        waitForTableIndexesQueryable();

        long before = cfs.metric.liveDiskSpaceUsed.getCount();
        cfs.indexManager.buildMissingSSTableAttachedIndexes(sstables);
        assertThat(cfs.metric.liveDiskSpaceUsed.getCount()).isEqualTo(before);

        cfs.addSSTables(sstables, OperationType.STREAM);
        long actual = cfs.getLiveSSTables().stream().mapToLong(SSTableReader::bytesOnDisk).sum();
        assertThat(actual).isGreaterThan(dataBytes);
        assertThat(cfs.metric.liveDiskSpaceUsed.getCount() - before).isEqualTo(actual);
    }

    private void writeRows(int firstKey)
    {
        for (int k = firstKey; k < firstKey + ROWS; k++)
            execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 2, k % 2);
        flush();
    }

    /**
     * Writes {@link #ROWS} rows to SSTables, and removes those from the table without releasing them, as if they had
     * been received by a stream and not added yet.
     */
    private Set<SSTableReader> writeSSTablesNotAdded(int firstKey)
    {
        writeRows(firstKey);
        return removeFromTable();
    }

    private Set<SSTableReader> removeFromTable()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        assertThat(sstables).isNotEmpty();
        cfs.getTracker().removeUnsafe(sstables);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        if (group != null)
            group.onSSTableChanged(sstables, Collections.emptySet(), group.getIndexes(), false);
        return sstables;
    }

    private int rowsThroughV()
    {
        return execute("SELECT k FROM %s WHERE v = 0").size() + execute("SELECT k FROM %s WHERE v = 1").size();
    }

    private static Injections.Barrier barrier(String name, InvokePointBuilder at)
    {
        return Injections.newBarrier(name, 2, false).add(at).build();
    }
}
