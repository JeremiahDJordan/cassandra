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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The submission of the builders of a build that isn't a full rebuild throws (CompactionManager#submitIndexBuild), here
 * for an SSTable reloaded from remote storage: its indexes aren't left queryable or building without it, and a full
 * rebuild running then doesn't leave the SSTable to that build.
 */
public class IndexBuildSubmissionFailureTest extends SAITester
{
    private static final int SSTABLES = 4;
    private static final int ROWS_PER_SSTABLE = 100;

    private static volatile boolean failingSubmission;
    private static volatile boolean holdingCheck;
    private static volatile boolean checkHeld;

    /** condition of the failure injected at the entry of CompactionManager#submitIndexBuild */
    public static boolean failSubmission()
    {
        return failingSubmission;
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

    @After
    public void release() throws Throwable
    {
        failingSubmission = false;
        holdingCheck = false;
        checkHeld = false;
        Injections.deleteAll();
    }

    /**
     * No rebuild runs: the indexes of the build are marked failed (non-queryable, no longer building), the builder
     * releases the SSTable's rebuild reservation, and a full rebuild then brings them back.
     */
    @Test
    public void testFailedSubmissionMakesIndexesNonQueryable() throws Throwable
    {
        ColumnFamilyStore cfs = prepare();
        String v = indexOn(cfs, "v");
        String w = indexOn(cfs, "w");
        SSTableReader remote = anSSTableWithoutIndexComponentsOutOfTheTable(cfs);
        injectSubmissionFailure();

        failingSubmission = true;
        reload(cfs, remote);
        failingSubmission = false;

        assertFalse("v must not be queryable without the SSTable", cfs.indexManager.isIndexQueryable(v));
        assertFalse("w must not be queryable without the SSTable", cfs.indexManager.isIndexQueryable(w));
        assertFalse(cfs.indexManager.isIndexBuilding(v));
        assertFalse(cfs.indexManager.isIndexBuilding(w));
        assertTrue(cfs.indexManager.sstablesOfBuildsInProgress(v).isEmpty());
        assertTrue("the builder must release the SSTable's rebuild reservation", remote.streamRebuildState().tryBeginRebuild());
        remote.streamRebuildState().endRebuild();

        cfs.indexManager.rebuildIndexesBlocking(new HashSet<>(Arrays.asList(v, w)));
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertTrue(cfs.indexManager.isIndexQueryable(w));
        assertThat(rowsThrough("v")).isEqualTo((SSTABLES + 1) * ROWS_PER_SSTABLE);
        assertThat(rowsThrough("w")).isEqualTo((SSTABLES + 1) * ROWS_PER_SSTABLE);
    }

    /**
     * A full rebuild of v is at its end check: the SSTable of the build whose submission threw counts, so the rebuild
     * fails rather than make v queryable again without it.
     */
    @Test
    public void testSSTableOfABuildWhoseSubmissionThrowsFailsTheRebuild() throws Throwable
    {
        ColumnFamilyStore cfs = prepare();
        String v = indexOn(cfs, "v");
        SSTableReader remote = anSSTableWithoutIndexComponentsOutOfTheTable(cfs);
        injectSubmissionFailure();
        CompletableFuture<Throwable> rebuild = rebuildHeldAtItsEndCheck(cfs, v);

        failingSubmission = true;
        reload(cfs, remote);
        failingSubmission = false;
        assertFalse(cfs.indexManager.isIndexQueryable(v));

        holdingCheck = false;
        Throwable failure = rebuild.get(180, TimeUnit.SECONDS);
        Injections.deleteAll();
        assertNotNull("the rebuild must fail", failure);
        assertThat(failure).hasStackTraceContaining("aren't in its view");
        assertFalse("v must not be queryable without the SSTable", cfs.indexManager.isIndexQueryable(v));
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

    private int rowsThrough(String column)
    {
        return execute("SELECT k FROM %s WHERE " + column + " = 0").size() + execute("SELECT k FROM %s WHERE " + column + " = 1").size();
    }

    /** The SSTable is reloaded from remote storage: its asynchronous build is submitted on this thread. */
    private static void reload(ColumnFamilyStore cfs, SSTableReader sstable)
    {
        cfs.getTracker().addInitialSSTables(Collections.singleton(sstable));
        try
        {
            cfs.getTracker().notifySSTablesChanged(Collections.emptySet(), Collections.singleton(sstable), OperationType.REMOTE_RELOAD, Optional.empty(), null);
        }
        catch (RuntimeException e)
        {
            logger.info("The reload threw", e);
        }
    }

    private static void injectSubmissionFailure() throws Throwable
    {
        Injections.inject(Injections.newCustom("fail_submission_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint().onClass(CompactionManager.class).onMethod("submitIndexBuild").atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(Expression.expr(IndexBuildSubmissionFailureTest.class.getName()).method("failSubmission").args())
                                                      .toActionBuilder()
                                                      .actions()
                                                      .doThrow(RuntimeException.class, Expression.quote("Injected submission failure")))
                                    .build());
    }

    /** starts a full rebuild of the index, and waits until it is held at the start of its end check */
    private CompletableFuture<Throwable> rebuildHeldAtItsEndCheck(ColumnFamilyStore cfs, String index) throws Throwable
    {
        checkHeld = false;
        holdingCheck = true;
        Injections.inject(Injections.newCustom("hold_check_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.FullRebuild.class).onMethod("checkViews"))
                                    .add(ActionBuilder.newActionBuilder().actions()
                                                      .doAction(Expression.expr(IndexBuildSubmissionFailureTest.class.getName()).method("holdCheck").args()))
                                    .build());
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, index);
        waitForAssert(() -> assertTrue(checkHeld), 120, TimeUnit.SECONDS);
        return rebuild;
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
