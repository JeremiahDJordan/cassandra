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

package org.apache.cassandra.index.sai.functional;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.inject.Expression.expr;
import static org.apache.cassandra.inject.Expression.quote;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A failure to load an SSTable's index that isn't corruption (e.g. too many open files) must neither delete the
 * index components, which are fine, nor leave the SSTable out of the index once the index is queryable. A failure to
 * load an SSTable's per-SSTable components must only get that SSTable rebuilt.
 */
public class IndexLoadFailureTest extends SAITester
{
    // The SSTable for which loading the index fails, and how many more times it fails
    private static volatile String failingSSTable;
    private static final AtomicInteger failuresLeft = new AtomicInteger();

    // Counts the SSTables the index builder builds
    private static final Injections.Counter SSTABLE_BUILDS =
        Injections.newCounter("SSTableIndexBuilds")
                  .add(InvokePointBuilder.newInvokePoint()
                                         .onClass(StorageAttachedIndexBuilder.class)
                                         .onMethod("indexSSTable"))
                  .build();

    @BeforeClass
    public static void setNormalMode()
    {
        // These tests are about the initial build, out of STARTING mode
        StorageService.instance.setNormalModeUnsafe();
    }

    @After
    public void removeInjections()
    {
        Injections.deleteAll();
        failuresLeft.set(0);
    }

    /** Called by the injection: whether creating the per-SSTable context of the given SSTable should fail. */
    public static boolean shouldFailContext(SSTableReader sstable)
    {
        return sstable.descriptor.id.toString().equals(failingSSTable)
               && failuresLeft.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0;
    }

    /** Called by the injection: whether loading the index of the SSTable of the given context should fail. */
    public static boolean shouldFailLoad(SSTableContext context)
    {
        return context != null
               && context.sstable.descriptor.id.toString().equals(failingSSTable)
               && failuresLeft.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0;
    }

    @Test
    public void transientLoadFailureOnFlushKeepsComponents() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);
        StorageAttachedIndex index = index(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")));
        IndexContext context = index.getIndexContext();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('1', 1, '1')");
        flush();

        // Fail loading the index of every new SSTable once
        failingSSTable = null;
        Injections.inject(loadFailure(true));
        execute("INSERT INTO %s (id1, v1, v2) VALUES ('2', 2, '2')");
        flush();

        // The index can't use the new SSTable, but its components are fine, so they must still be there
        assertFalse(cfs.indexManager.isIndexQueryable(index));
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue(sstable.descriptor.toString(), IndexDescriptor.isIndexBuildCompleteOnDisk(sstable, context));

        Injections.deleteAll();
        restart(cfs, index);
        assertEquals(2, context.getView().getIndexes().size());
        assertEquals(2, execute("SELECT id1 FROM %s WHERE v1 >= 0").size());
    }

    @Test
    public void loadFailureOnStartupFailsBuildAndKeepsComponents() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);
        StorageAttachedIndex index = index(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")));
        IndexContext context = index.getIndexContext();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        for (int i = 0; i < 2; i++)
        {
            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, ?, '0')", Integer.toString(i), i);
            flush();
        }

        // Loading the index of one of the SSTables fails twice, so on both loads of the initial build
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals(2, sstables.size());
        SSTableReader failing = sstables.get(0);
        failingSSTable = failing.descriptor.id.toString();
        failuresLeft.set(2);
        Injections.inject(loadFailure(false), SSTABLE_BUILDS);
        SSTABLE_BUILDS.reset();

        // The SSTable's components are complete, and the failure isn't corruption, so the build must fail rather
        // than rebuild the SSTable (and replace its components), and the index must not be queryable
        assertThatThrownBy(() -> restart(cfs, index))
                .hasMessageContaining("Failed to load the index of 1 SSTable(s) whose index components are complete");
        assertEquals(0, failuresLeft.get());
        assertFalse(cfs.indexManager.isIndexQueryable(index));
        assertTrue(IndexDescriptor.isIndexBuildCompleteOnDisk(failing, context));
        assertEquals(0, SSTABLE_BUILDS.get());

        // Once the cause is gone, a restart loads the components without rebuilding anything
        restart(cfs, index);
        assertTrue(cfs.indexManager.isIndexQueryable(index));
        assertEquals(2, context.getView().getIndexes().size());
        assertEquals(2, execute("SELECT id1 FROM %s WHERE v1 >= 0").size());
        assertEquals(0, SSTABLE_BUILDS.get());
    }

    @Test
    public void perSSTableLoadFailureOnStartupRebuildsOnlyThatSSTable() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        disableCompaction(KEYSPACE);
        StorageAttachedIndex index = index(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")));
        IndexContext context = index.getIndexContext();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, ?, '0')", Integer.toString(i), i);
            flush();
        }

        // Creating the per-SSTable context of one SSTable fails once. That invalidates its per-SSTable components,
        // and fails the whole batch, so none of the SSTables is added to the view by the first load
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals(3, sstables.size());
        failingSSTable = sstables.get(1).descriptor.id.toString();
        failuresLeft.set(1);
        Injections.inject(contextFailure(), SSTABLE_BUILDS);
        SSTABLE_BUILDS.reset();

        restart(cfs, index);

        // Only the SSTable whose components were invalidated must be rebuilt: the others are loaded by the second load
        assertEquals(0, failuresLeft.get());
        assertEquals(1, SSTABLE_BUILDS.get());
        assertTrue(cfs.indexManager.isIndexQueryable(index));
        assertEquals(3, context.getView().getIndexes().size());
        assertEquals(3, execute("SELECT id1 FROM %s WHERE v1 >= 0").size());
    }

    private StorageAttachedIndex index(String name)
    {
        return (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(name);
    }

    /** Like a restart: forget everything the index group has loaded, and run the initial build. */
    private static void restart(ColumnFamilyStore cfs, StorageAttachedIndex index) throws Exception
    {
        StorageAttachedIndexGroup.getIndexGroup(cfs).reset();
        cfs.indexManager.buildIndex(index).get();
    }

    /**
     * Fails creating the per-SSTable context of {@link #failingSSTable}, while {@link #failuresLeft} is positive.
     */
    private static Injection contextFailure()
    {
        ActionBuilder action = ActionBuilder.newActionBuilder();
        action.conditions().when(expr(IndexLoadFailureTest.class.getName()).method("shouldFailContext").args("$1"));
        action.actions().doThrow(RuntimeException.class, quote("Injected context failure"));
        return Injections.newCustom("sstable_context_failure")
                         .add(InvokePointBuilder.newInvokePoint()
                                                .onClass(SSTableContext.class)
                                                .onMethod("create", SSTableReader.class, IndexComponents.ForRead.class))
                         .add(action)
                         .build();
    }

    /**
     * @param always whether loading fails for every SSTable, every time, or only for {@link #failingSSTable}, while
     * {@link #failuresLeft} is positive
     */
    private static Injection loadFailure(boolean always)
    {
        ActionBuilder action = ActionBuilder.newActionBuilder();
        if (!always)
            action.conditions().when(expr(IndexLoadFailureTest.class.getName()).method("shouldFailLoad").args("$3"));
        action.actions().doThrow(RuntimeException.class, quote("Injected load failure"));
        return Injections.newCustom("index_load_failure_" + always)
                         .add(InvokePointBuilder.newInvokePoint()
                                                .onClass(SegmentMetadata.class)
                                                .onMethod("load", MetadataSource.class, IndexContext.class, SSTableContext.class))
                         .add(action)
                         .build();
    }
}
