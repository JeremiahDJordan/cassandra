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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The end check of a full rebuild, with the default decider, for SSTables the rebuild built that are out of the index's
 * view without components when it ends: one another build in progress is building doesn't fail the rebuild; one that
 * nothing is building does, and so does the SSTable of a build that has just failed.
 */
public class FullRebuildEndCheckPendingBuildsTest extends SAITester
{
    private static final int SSTABLES = 4;
    private static final int ROWS_PER_SSTABLE = 100;

    /** the builds of these SSTables are held at the start of StorageAttachedIndexBuilder#indexSSTable */
    private static final Set<Descriptor> hold = ConcurrentHashMap.newKeySet();
    private static final Set<Descriptor> held = ConcurrentHashMap.newKeySet();
    private static volatile boolean holdingCheck;
    private static volatile boolean checkHeld;
    private static volatile boolean holdingMarked;
    private static volatile boolean markedHeld;
    private static volatile boolean holdingLook;
    private static volatile boolean lookHeld;

    /** called by the injected code at the start of StorageAttachedIndexBuilder#indexSSTable */
    public static void holdBuildOf(Object sstable) throws InterruptedException
    {
        Descriptor descriptor = ((SSTableReader) sstable).descriptor;
        if (!hold.contains(descriptor))
            return;
        held.add(descriptor);
        while (hold.contains(descriptor))
            Thread.sleep(20);
    }

    /** called by the injected code at the start of the end check of a full rebuild */
    public static void holdCheck() throws InterruptedException
    {
        if (!holdingCheck)
            return;
        checkHeld = true;
        while (holdingCheck)
            Thread.sleep(20);
    }

    /** called by the injected code at the start of FullRebuild#hasCompleteComponents */
    public static void holdLook() throws InterruptedException
    {
        if (!holdingLook)
            return;
        lookHeld = true;
        while (holdingLook)
            Thread.sleep(20);
    }

    /** called by the injected code at the end of SecondaryIndexManager#logAndMarkIndexesFailed: holds the first failure */
    public static void holdMarked() throws InterruptedException
    {
        if (!holdingMarked || markedHeld)
            return;
        markedHeld = true;
        while (holdingMarked)
            Thread.sleep(20);
    }

    @After
    public void release() throws Throwable
    {
        hold.clear();
        held.clear();
        holdingCheck = false;
        checkHeld = false;
        holdingMarked = false;
        markedHeld = false;
        holdingLook = false;
        lookHeld = false;
        Injections.deleteAll();
    }

    /**
     * An SSTable the rebuild built is replaced, when the rebuild is at its end check, by a copy without index components
     * reloaded from remote storage, which is built asynchronously (see IndexBuildDecider#onSSTableListChanged): while
     * that build is in progress it must not fail the rebuild, and it adds the SSTable to the view.
     */
    @Test
    public void testRebuiltSSTableReplacedByARemoteCopyBeingBuiltDoesNotFailTheRebuild() throws Throwable
    {
        ColumnFamilyStore cfs = prepare();
        String v = indexOn(cfs, "v");
        StorageAttachedIndex index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(v);
        SSTableReader built = firstSSTable(cfs);

        CompletableFuture<Throwable> rebuild = rebuildHeldAtItsEndCheck(cfs, v);
        assertTrue(index.getIndexContext().getView().containsSSTableIndex(built.descriptor));

        // the copy has no index components, and its asynchronous build starts (and is held)
        removeIndexComponents(cfs, built, true, true);
        hold.add(built.descriptor);
        cfs.getTracker().notifySSTablesChanged(Collections.singleton(built), Collections.singleton(built), OperationType.REMOTE_RELOAD,
                                               Optional.empty(), null);
        waitForAssert(() -> assertTrue(held.contains(built.descriptor)), 120, TimeUnit.SECONDS);
        assertFalse(index.getIndexContext().getView().containsSSTableIndex(built.descriptor));

        holdingCheck = false;
        assertThat(rebuild.get(180, TimeUnit.SECONDS)).isNull();
        assertTrue(cfs.indexManager.isIndexQueryable(v));

        hold.clear();
        waitForAssert(() -> assertTrue(index.getIndexContext().getView().containsSSTableIndex(built.descriptor)), 120, TimeUnit.SECONDS);
        Injections.deleteAll();
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertThat(rowsThrough("v")).isEqualTo(SSTABLES * ROWS_PER_SSTABLE);
    }

    /**
     * An SSTable the rebuild built has lost its components of the index and is out of the index's view when the rebuild
     * ends, and nothing is building it: the rebuild must fail rather than make the index queryable without it, also
     * while a build of the index on another SSTable (one reloaded from remote storage) is in progress.
     */
    @Test
    public void testRebuiltSSTableWithoutComponentsThatNothingBuildsFailsTheRebuild() throws Throwable
    {
        ColumnFamilyStore cfs = prepare();
        String v = indexOn(cfs, "v");
        StorageAttachedIndex index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(v);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        SSTableReader remote = anSSTableWithoutIndexComponentsOutOfTheTable(cfs);
        SSTableReader built = firstSSTable(cfs);

        CompletableFuture<Throwable> rebuild = rebuildHeldAtItsEndCheck(cfs, v);

        // another SSTable is reloaded from remote storage, and its asynchronous build starts (and is held)
        hold.add(remote.descriptor);
        cfs.getTracker().addInitialSSTables(Collections.singleton(remote));
        cfs.getTracker().notifySSTablesChanged(Collections.emptySet(), Collections.singleton(remote), OperationType.REMOTE_RELOAD,
                                               Optional.empty(), null);
        waitForAssert(() -> assertTrue(held.contains(remote.descriptor)), 120, TimeUnit.SECONDS);

        // an SSTable the rebuild built loses its components of v, and is loaded again
        removeIndexComponents(cfs, built, false, false);
        group.onSSTableChanged(Collections.singleton(built), Collections.singleton(built), group.getIndexes(), false);
        assertFalse(index.getIndexContext().getView().containsSSTableIndex(built.descriptor));

        holdingCheck = false;
        Throwable failure = rebuild.get(180, TimeUnit.SECONDS);
        hold.clear();
        waitForAssert(() -> assertTrue(index.getIndexContext().getView().containsSSTableIndex(remote.descriptor)), 120, TimeUnit.SECONDS);
        Injections.deleteAll();
        assertNotNull("the rebuild must fail", failure);
        assertThat(failure).hasStackTraceContaining("aren't in its view");
        assertFalse("v must not be queryable without the SSTable", cfs.indexManager.isIndexQueryable(v));
    }

    /**
     * The asynchronous build of an SSTable reloaded from remote storage fails right before the end check of the rebuild
     * looks at the builds in progress: its indexes are marked failed, and nothing is going to build the SSTable. The
     * rebuild must fail rather than make the index queryable again without it.
     */
    @Test
    public void testSSTableOfABuildThatJustFailedFailsTheRebuild() throws Throwable
    {
        ColumnFamilyStore cfs = prepare();
        String v = indexOn(cfs, "v");
        SSTableReader remote = anSSTableWithoutIndexComponentsOutOfTheTable(cfs);

        // (injected before anything fails: a method that is already running isn't changed)
        Injections.inject(Injections.newCustom("hold_marked_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint().onClass(SecondaryIndexManager.class)
                                                           .onMethod("logAndMarkIndexesFailed").atExit())
                                    .add(ActionBuilder.newActionBuilder().actions()
                                                      .doAction(Expression.expr(FullRebuildEndCheckPendingBuildsTest.class.getName())
                                                                          .method("holdMarked").args()))
                                    .build());
        CompletableFuture<Throwable> rebuild = rebuildHeldAtItsEndCheck(cfs, v);

        // an SSTable is reloaded from remote storage, and its asynchronous build starts (and is held)
        hold.add(remote.descriptor);
        cfs.getTracker().addInitialSSTables(Collections.singleton(remote));
        cfs.getTracker().notifySSTablesChanged(Collections.emptySet(), Collections.singleton(remote), OperationType.REMOTE_RELOAD,
                                               Optional.empty(), null);
        waitForAssert(() -> assertTrue(held.contains(remote.descriptor)), 120, TimeUnit.SECONDS);

        // that build is stopped: it fails, and is held once its indexes are marked failed
        holdingMarked = true;
        CompactionManager.instance.stopCompaction("INDEX_BUILD");
        hold.clear();
        waitForAssert(() -> assertTrue(markedHeld), 120, TimeUnit.SECONDS);
        assertFalse(cfs.indexManager.isIndexQueryable(v));

        holdingCheck = false;
        Throwable failure = rebuild.get(180, TimeUnit.SECONDS);
        holdingMarked = false;
        Injections.deleteAll();
        assertNotNull("the rebuild must fail", failure);
        assertThat(failure).hasStackTraceContaining("aren't in its view");
        assertFalse("v must not be queryable without the SSTable", cfs.indexManager.isIndexQueryable(v));
    }

    /**
     * The asynchronous build of an SSTable reloaded from remote storage ends while the end check of the rebuild looks
     * at that SSTable's components on disk: the check still sees the SSTable that build added, and the rebuild doesn't
     * fail.
     */
    @Test
    public void testBuildThatEndsWhileTheCheckLooksAtItsSSTableDoesNotFailTheRebuild() throws Throwable
    {
        ColumnFamilyStore cfs = prepare();
        String v = indexOn(cfs, "v");
        StorageAttachedIndex index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(v);
        SSTableReader remote = anSSTableWithoutIndexComponentsOutOfTheTable(cfs);

        Injections.inject(Injections.newCustom("hold_look_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.FullRebuild.class)
                                                           .onMethod("hasCompleteComponents"))
                                    .add(ActionBuilder.newActionBuilder().actions()
                                                      .doAction(Expression.expr(FullRebuildEndCheckPendingBuildsTest.class.getName())
                                                                          .method("holdLook").args()))
                                    .build());
        CompletableFuture<Throwable> rebuild = rebuildHeldAtItsEndCheck(cfs, v);

        // an SSTable is reloaded from remote storage, and its asynchronous build starts (and is held)
        hold.add(remote.descriptor);
        cfs.getTracker().addInitialSSTables(Collections.singleton(remote));
        cfs.getTracker().notifySSTablesChanged(Collections.emptySet(), Collections.singleton(remote), OperationType.REMOTE_RELOAD,
                                               Optional.empty(), null);
        waitForAssert(() -> assertTrue(held.contains(remote.descriptor)), 120, TimeUnit.SECONDS);

        // the check is held as it looks at the components of that SSTable, and its build ends meanwhile
        holdingLook = true;
        holdingCheck = false;
        waitForAssert(() -> assertTrue(lookHeld), 120, TimeUnit.SECONDS);
        hold.clear();
        waitForAssert(() -> assertTrue(index.getIndexContext().getView().containsSSTableIndex(remote.descriptor)), 120, TimeUnit.SECONDS);
        waitForAssert(() -> assertTrue(cfs.indexManager.sstablesOfBuildsInProgress(v).isEmpty()), 120, TimeUnit.SECONDS);

        holdingLook = false;
        assertThat(rebuild.get(180, TimeUnit.SECONDS)).isNull();
        Injections.deleteAll();
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertThat(rowsThrough("v")).isEqualTo((SSTABLES + 1) * ROWS_PER_SSTABLE);
    }

    private ColumnFamilyStore prepare()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        for (int s = 0; s < SSTABLES; s++)
        {
            for (int k = s * ROWS_PER_SSTABLE; k < (s + 1) * ROWS_PER_SSTABLE; k++)
                execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 2, k % 2);
            flush();
        }
        return getCurrentColumnFamilyStore();
    }

    private static String indexOn(ColumnFamilyStore cfs, String column)
    {
        return cfs.indexManager.listIndexes().stream()
                               .map(i -> i.getIndexMetadata().name)
                               .filter(name -> name.endsWith('_' + column + "_idx"))
                               .findFirst().get();
    }

    private static SSTableReader firstSSTable(ColumnFamilyStore cfs)
    {
        List<SSTableReader> live = new ArrayList<>(cfs.getLiveSSTables());
        live.sort(SSTableReader.idComparator);
        return live.get(0);
    }

    private int rowsThrough(String column)
    {
        return execute("SELECT k FROM %s WHERE " + column + " = 0").size() + execute("SELECT k FROM %s WHERE " + column + " = 1").size();
    }

    /** starts a full rebuild of the index, and waits until it is held at the start of its end check */
    private CompletableFuture<Throwable> rebuildHeldAtItsEndCheck(ColumnFamilyStore cfs, String index) throws Throwable
    {
        checkHeld = false;
        holdingCheck = true;
        Injections.inject(Injections.newCustom("hold_check_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.FullRebuild.class)
                                                           .onMethod("checkViews"))
                                    .add(ActionBuilder.newActionBuilder().actions()
                                                      .doAction(Expression.expr(FullRebuildEndCheckPendingBuildsTest.class.getName())
                                                                          .method("holdCheck").args()))
                                    .build());
        Injections.inject(Injections.newCustom("hold_build_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("indexSSTable"))
                                    .add(ActionBuilder.newActionBuilder().actions()
                                                      .doAction(Expression.expr(FullRebuildEndCheckPendingBuildsTest.class.getName())
                                                                          .method("holdBuildOf").args("$1")))
                                    .build());
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, index);
        waitForAssert(() -> assertTrue(checkHeld), 120, TimeUnit.SECONDS);
        return rebuild;
    }

    /** removes from the disk the components of the index on v of the SSTable, and optionally the others */
    private static void removeIndexComponents(ColumnFamilyStore cfs, SSTableReader sstable, boolean otherIndexes, boolean perSSTable)
    {
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        IndexDescriptor descriptor = group.descriptorFor(sstable);
        for (StorageAttachedIndex index : group.getIndexes())
        {
            if (otherIndexes || index.getIndexContext().getIndexName().endsWith("_v_idx"))
                descriptor.perIndexComponents(index.getIndexContext()).invalidate(sstable, cfs.getTracker());
        }
        if (perSSTable)
            descriptor.perSSTableComponents().invalidate(sstable, cfs.getTracker());
    }

    /** an SSTable out of the table, without index components (as one in remote storage, whose index is built elsewhere) */
    private SSTableReader anSSTableWithoutIndexComponentsOutOfTheTable(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> before = new HashSet<>(cfs.getLiveSSTables());
        for (int k = SSTABLES * ROWS_PER_SSTABLE; k < (SSTABLES + 1) * ROWS_PER_SSTABLE; k++)
            execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 2, k % 2);
        flush();
        Set<SSTableReader> added = new HashSet<>(cfs.getLiveSSTables());
        added.removeAll(before);
        SSTableReader remote = added.iterator().next();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        cfs.getTracker().removeUnsafe(Collections.singleton(remote));
        cfs.getTracker().notifySSTablesChanged(Collections.singleton(remote), Collections.emptySet(), OperationType.UNKNOWN, Optional.empty(), null);
        Set<IndexContext> contexts = group.getIndexes().stream().map(StorageAttachedIndex::getIndexContext).collect(Collectors.toSet());
        IndexDescriptor descriptor = IndexDescriptor.load(remote, contexts);
        for (IndexContext context : contexts)
            descriptor.perIndexComponents(context).invalidate(remote, cfs.getTracker());
        descriptor.perSSTableComponents().invalidate(remote, cfs.getTracker());
        return remote;
    }
}
