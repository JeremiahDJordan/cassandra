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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.index.IndexBuildDecider;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_INDEX_BUILD_DECIDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The end check of a full rebuild with the answers of the decider: whether it checks the SSTables the rebuild didn't
 * build ({@link IndexBuildDecider#checkSSTablesNotBuiltByFullRebuild}), those it built, and an SSTable another
 * build of the index is building. The decider is chosen when {@link IndexBuildDecider} is loaded, so the property
 * is set first.
 */
public class FullRebuildEndCheckDeciderTest extends SAITester
{
    private static final int ROWS_PER_SSTABLE = 100;

    private static volatile boolean holdingBuilders;
    private static volatile boolean holdFirstBuilderOnly;
    private static volatile Descriptor holdOnly;
    private static final AtomicInteger buildersHeld = new AtomicInteger();
    private static volatile boolean holdingCheck;
    private static volatile boolean checkHeld;

    /** called by the injected code: holds a builder there while {@link #holdingBuilders} is set */
    public static void holdBuilder() throws InterruptedException
    {
        if (!holdingBuilders)
            return;
        if (buildersHeld.getAndIncrement() > 0 && holdFirstBuilderOnly)
            return;
        while (holdingBuilders)
            Thread.sleep(20);
    }

    /** called by the injected code at the start of StorageAttachedIndexBuilder#indexSSTable: holds the build of one SSTable */
    public static void holdBuildOf(Object sstable) throws InterruptedException
    {
        if (!holdingBuilders || !((SSTableReader) sstable).descriptor.equals(holdOnly))
            return;
        buildersHeld.incrementAndGet();
        while (holdingBuilders)
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

    public static class Decider implements IndexBuildDecider
    {
        static volatile Decision initialBuild = Decision.NONE;
        // the answer of checkSSTablesNotBuiltByFullRebuild, or null for the default one
        static volatile Boolean checkNotBuilt;

        @Override
        public Decision onInitialBuild()
        {
            return initialBuild;
        }

        @Override
        public boolean checkSSTablesNotBuiltByFullRebuild(ColumnFamilyStore cfs)
        {
            Boolean check = checkNotBuilt;
            return check == null ? IndexBuildDecider.super.checkSSTablesNotBuiltByFullRebuild(cfs) : check;
        }
    }

    @BeforeClass
    public static void setUpClass()
    {
        CUSTOM_INDEX_BUILD_DECIDER.setString(Decider.class.getName());
        CQLTester.setUpClass();
    }

    @AfterClass
    public static void teardown()
    {
        CUSTOM_INDEX_BUILD_DECIDER.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'
    }

    @After
    public void release() throws Throwable
    {
        holdingBuilders = false;
        holdFirstBuilderOnly = false;
        holdOnly = null;
        holdingCheck = false;
        Decider.initialBuild = IndexBuildDecider.Decision.NONE;
        Decider.checkNotBuilt = null;
        Injections.deleteAll();
    }

    /**
     * Where the decider leaves the builds of SSTables to something else, a live SSTable can be out of the view of an
     * index when its full rebuild ends (here one added without index components while the rebuild runs): the rebuild
     * must not fail.
     */
    @Test
    public void testEndCheckIsSkippedWhereInitialBuildsAre() throws Throwable
    {
        assertThat(IndexBuildDecider.instance).isInstanceOf(Decider.class);
        Decider.initialBuild = IndexBuildDecider.Decision.NONE;
        ColumnFamilyStore cfs = prepare();
        String v = cfs.indexManager.listIndexes().iterator().next().getIndexMetadata().name;

        Throwable failure = rebuildWhileAnSSTableIsAddedWithoutIndexComponents(cfs, v);
        assertThat(failure).isNull();
        assertTrue(cfs.indexManager.isIndexQueryable(v));
    }

    /** With the same decider building as the default one does, the same rebuild fails its end check. */
    @Test
    public void testEndCheckRunsWhereInitialBuildsDo() throws Throwable
    {
        Decider.initialBuild = IndexBuildDecider.Decision.SYNC;
        ColumnFamilyStore cfs = prepare();
        String v = cfs.indexManager.listIndexes().iterator().next().getIndexMetadata().name;

        Throwable failure = rebuildWhileAnSSTableIsAddedWithoutIndexComponents(cfs, v);
        assertNotNull("the rebuild must fail", failure);
        assertThat(failure).hasStackTraceContaining("aren't in its view");
        assertFalse(cfs.indexManager.isIndexQueryable(v));
    }

    /**
     * The dedicated hook decides, whatever onInitialBuild() says: an SSTable the rebuild didn't build, out of the view,
     * fails the rebuild only if the hook asks for the SSTables a full rebuild didn't build to be checked.
     */
    @Test
    public void testTheHookDecidesWhetherSSTablesTheRebuildDidNotBuildAreChecked() throws Throwable
    {
        Decider.initialBuild = IndexBuildDecider.Decision.SYNC;
        Decider.checkNotBuilt = false;
        ColumnFamilyStore cfs = prepare();
        String v = cfs.indexManager.listIndexes().iterator().next().getIndexMetadata().name;
        assertThat(rebuildWhileAnSSTableIsAddedWithoutIndexComponents(cfs, v)).isNull();
        assertTrue(cfs.indexManager.isIndexQueryable(v));

        Decider.initialBuild = IndexBuildDecider.Decision.NONE;
        Decider.checkNotBuilt = true;
        cfs = prepare();
        v = cfs.indexManager.listIndexes().iterator().next().getIndexMetadata().name;
        Throwable failure = rebuildWhileAnSSTableIsAddedWithoutIndexComponents(cfs, v);
        assertNotNull("the rebuild must fail", failure);
        assertThat(failure).hasStackTraceContaining("aren't in its view");
        assertFalse(cfs.indexManager.isIndexQueryable(v));
    }

    /**
     * Where SSTables the rebuild didn't build aren't checked, those it built still are: an SSTable the rebuild built
     * that loses its per-SSTable components and is loaded again before the rebuild ends fails the rebuild.
     */
    @Test
    public void testTheSSTablesTheRebuildBuiltAreCheckedWhereTheOthersAreNot() throws Throwable
    {
        Decider.initialBuild = IndexBuildDecider.Decision.NONE;
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        String v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        // (no initial build here, but the indexes are written by the flushes)
        for (int s = 0; s < 8; s++)
        {
            for (int k = s * ROWS_PER_SSTABLE; k < (s + 1) * ROWS_PER_SSTABLE; k++)
                execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 2, k % 2);
            flush();
        }
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndex index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(v);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);

        // one builder of the rebuild is held, the others build their SSTables
        holdFirstBuilderOnly = true;
        holdBuilders("indexSSTable", "holdBuilder", false);
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForAssert(() -> assertThat(buildersHeld.get()).isGreaterThan(0), 120, TimeUnit.SECONDS);
        waitForAssert(() -> assertThat(index.getIndexContext().getView().getIndexes()).isNotEmpty(), 120, TimeUnit.SECONDS);
        SSTableReader built = index.getIndexContext().getView().getIndexes().iterator().next().getSSTable();

        // that SSTable loses its per-SSTable components, and is loaded again
        group.descriptorFor(built).perSSTableComponents().invalidate(built, cfs.getTracker());
        group.onSSTableChanged(Collections.singleton(built), Collections.singleton(built), group.getIndexes(), false);
        assertFalse(index.getIndexContext().getView().containsSSTableIndex(built.descriptor));

        holdingBuilders = false;
        Throwable failure = rebuild.get(180, TimeUnit.SECONDS);
        Injections.deleteAll();
        assertNotNull("the rebuild must fail", failure);
        assertThat(failure).hasStackTraceContaining("aren't in its view");
        assertFalse("v must not be queryable without the SSTable", cfs.indexManager.isIndexQueryable(v));
    }

    /**
     * With the default answers (initial builds built, so SSTables the rebuild didn't build are checked), an SSTable
     * reloaded from remote storage without index components when the rebuild ends is built asynchronously (see
     * IndexBuildDecider#onSSTableListChanged): while that build is in progress, it must not fail the rebuild, and it
     * adds the SSTable to the view.
     */
    @Test
    public void testAnSSTableAnotherBuildIsBuildingDoesNotFailTheRebuild() throws Throwable
    {
        Decider.initialBuild = IndexBuildDecider.Decision.SYNC;
        ColumnFamilyStore cfs = prepare();
        String v = cfs.indexManager.listIndexes().iterator().next().getIndexMetadata().name;
        StorageAttachedIndex index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(v);
        SSTableReader remote = anSSTableWithoutIndexComponentsOutOfTheTable(cfs);

        // the rebuild is at its end check...
        checkHeld = false;
        holdingCheck = true;
        Expression checkAction = Expression.expr(FullRebuildEndCheckDeciderTest.class.getName()).method("holdCheck");
        Injections.inject(Injections.newCustom("hold_check_" + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexBuilder.FullRebuild.class)
                                                           .onMethod("checkViews"))
                                    .add(ActionBuilder.newActionBuilder().actions()
                                                      .doAction(checkAction.args()))
                                    .build());
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForAssert(() -> assertTrue(checkHeld), 120, TimeUnit.SECONDS);

        // ...when the SSTable is reloaded from remote storage, and its asynchronous build starts (and is held)
        buildersHeld.set(0);
        holdOnly = remote.descriptor;
        holdingBuilders = true;
        holdBuilders("indexSSTable", "holdBuildOf", true);
        cfs.getTracker().addInitialSSTables(Collections.singleton(remote));
        cfs.getTracker().notifySSTablesChanged(Collections.emptySet(), Collections.singleton(remote),
                                               OperationType.REMOTE_RELOAD, Optional.empty(), null);
        waitForAssert(() -> assertThat(buildersHeld.get()).isGreaterThan(0), 120, TimeUnit.SECONDS);

        holdingCheck = false;
        Throwable failure = rebuild.get(180, TimeUnit.SECONDS);
        assertThat(failure).isNull();
        assertTrue(cfs.indexManager.isIndexQueryable(v));

        // the build of the reloaded SSTable adds it to the view
        holdingBuilders = false;
        waitForAssert(() -> assertTrue(index.getIndexContext().getView().containsSSTableIndex(remote.descriptor)), 120, TimeUnit.SECONDS);
        Injections.deleteAll();
        assertTrue(cfs.indexManager.isIndexQueryable(v));
        assertThat(execute("SELECT k FROM %s WHERE v = 0").size() + execute("SELECT k FROM %s WHERE v = 1").size()).isEqualTo(3 * ROWS_PER_SSTABLE);
    }

    private static void holdBuilders(String method, String helper, boolean withSSTable) throws Throwable
    {
        if (!withSSTable)
        {
            buildersHeld.set(0);
            holdingBuilders = true;
        }
        Expression action = Expression.expr(FullRebuildEndCheckDeciderTest.class.getName()).method(helper);
        Injections.inject(Injections.newCustom("hold_" + method + '_' + System.nanoTime())
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                           .onMethod(method))
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(withSSTable ? action.args("$1") : action.args()))
                                    .build());
    }

    private ColumnFamilyStore prepare()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        for (int s = 0; s < 2; s++)
        {
            for (int k = s * ROWS_PER_SSTABLE; k < (s + 1) * ROWS_PER_SSTABLE; k++)
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, k % 2);
            flush();
        }
        return getCurrentColumnFamilyStore();
    }

    private Throwable rebuildWhileAnSSTableIsAddedWithoutIndexComponents(ColumnFamilyStore cfs, String v) throws Throwable
    {
        SSTableReader remote = anSSTableWithoutIndexComponentsOutOfTheTable(cfs);

        buildersHeld.set(0);
        holdingBuilders = true;
        holdBuilders("indexPartitions", "holdBuilder", false);
        CompletableFuture<Throwable> rebuild = rebuildAsync(cfs, v);
        waitForAssert(() -> assertThat(buildersHeld.get()).isGreaterThan(0), 120, TimeUnit.SECONDS);

        // the SSTable is added to the table while the rebuild runs: it isn't one of those the rebuild builds
        cfs.getTracker().addInitialSSTables(Collections.singleton(remote));

        holdingBuilders = false;
        Throwable failure = rebuild.get(180, TimeUnit.SECONDS);
        Injections.deleteAll();
        StorageAttachedIndex index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(v);
        assertFalse(index.getIndexContext().getView().containsSSTableIndex(remote.descriptor));
        return failure;
    }

    /** an SSTable out of the table, without index components (as one in remote storage, whose index is built elsewhere) */
    private SSTableReader anSSTableWithoutIndexComponentsOutOfTheTable(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> before = new HashSet<>(cfs.getLiveSSTables());
        for (int k = 2 * ROWS_PER_SSTABLE; k < 3 * ROWS_PER_SSTABLE; k++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, k % 2);
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
