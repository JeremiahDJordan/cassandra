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

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexStatusManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.disk.PerIndexFilesOwner;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An index created on an existing table is built from the data, never from components of an index of the same name
 * already on disk. The stale components are those of an index "stale_idx" on column {@code t}; the new index of that
 * name is on column {@code a}, where 14 of the 100 rows have {@code a = 'a3'}.
 */
public class StaleSameNameIndexComponentsTest extends SAITester
{
    private static final String SCHEMA = "CREATE TABLE %s (pk int PRIMARY KEY, t text, a ascii, o int, n text)";
    private static final String INDEX = "stale_idx";

    private static volatile CountDownLatch paused;
    private static volatile CountDownLatch release;
    private static final AtomicInteger pauses = new AtomicInteger();

    private static volatile CountDownLatch compactionPaused;
    private static volatile CountDownLatch compactionRelease;
    private static volatile boolean compactionArmed;
    private static volatile CountDownLatch buildPaused;
    private static volatile CountDownLatch buildRelease;
    private static volatile boolean buildArmed;

    private static volatile CountDownLatch flushPaused;
    private static volatile CountDownLatch flushRelease;
    private static final AtomicInteger flushPauses = new AtomicInteger();

    // the SSTable whose compaction is held once it has written all of it, and taken it out of the live set
    private static volatile ColumnFamilyStore writtenOutTable;
    private static volatile SSTableReader writtenOut;
    private static volatile boolean holdWrittenOut;
    private static volatile boolean holdingWrittenOut;

    private boolean defaultImmutableSetting;

    // called by Byteman
    public static void pauseBuild() throws InterruptedException
    {
        if (pauses.incrementAndGet() == 1)
        {
            paused.countDown();
            release.await(60, TimeUnit.SECONDS);
        }
    }

    // called by Byteman, in SSTableIndexWriter#writeSegmentsMetadata
    public static void pauseCompactionInIndexWriter(Object writer) throws Exception
    {
        if (!compactionArmed)
            return;
        Field field = SSTableIndexWriter.class.getDeclaredField("indexContext");
        field.setAccessible(true);
        if (!INDEX.equals(((IndexContext) field.get(writer)).getIndexName()))
            return;
        compactionArmed = false;
        compactionPaused.countDown();
        compactionRelease.await(60, TimeUnit.SECONDS);
    }

    // called by Byteman, in StorageAttachedIndex#startInitialBuild
    public static void pauseInitialBuild() throws InterruptedException
    {
        if (!buildArmed)
            return;
        buildArmed = false;
        buildPaused.countDown();
        buildRelease.await(60, TimeUnit.SECONDS);
    }

    // called by Byteman
    public static void pauseFlush() throws InterruptedException
    {
        if (flushPauses.incrementAndGet() == 1)
        {
            flushPaused.countDown();
            flushRelease.await(60, TimeUnit.SECONDS);
        }
    }

    // called by Byteman, after each partition a compaction writes
    public static void holdOnceWrittenOut() throws InterruptedException
    {
        ColumnFamilyStore cfs = writtenOutTable;
        if (cfs == null || !holdWrittenOut || cfs.getLiveSSTables().stream().anyMatch(s -> s.descriptor.equals(writtenOut.descriptor)))
            return;
        holdingWrittenOut = true;
        while (holdWrittenOut)
            Thread.sleep(20);
        holdingWrittenOut = false;
    }

    @Before
    public void saveImmutableSetting()
    {
        defaultImmutableSetting = IMMUTABLE_SAI_COMPONENTS.getBoolean();
    }

    @After
    public void restoreImmutableSettingAndInjections()
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(defaultImmutableSetting);
        Injections.deleteAll();
    }

    /**
     * SSTables carrying the components of an index of the same name (in another table) are imported into a table
     * without indexes, then the index is created.
     */
    @Test
    public void testImportedComponents() throws Throwable
    {
        testImportedComponents(false);
    }

    @Test
    public void testImportedComponentsWithImmutableComponents() throws Throwable
    {
        testImportedComponents(true);
    }

    private void testImportedComponents(boolean immutable) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(immutable);
        createTableWithStaleIndex();
        importInto(saveSSTableFiles());

        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    /**
     * A DROP INDEX whose removal of the components didn't reach the disk (e.g. the node stopped right after the schema
     * change): the TOC still lists them, and their files are there, when an index of the same name is created.
     */
    @Test
    public void testComponentsOfAnInterruptedDrop() throws Throwable
    {
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        Set<Component> components = indexComponents(sstable);
        Path saved = saveSSTableFiles();
        dropIndex("DROP INDEX %s." + INDEX);
        awaitDeleted(sstable, components);
        // as if the drop hadn't removed them
        restoreFiles(sstable, saved, components);
        sstable.registerComponents(components, getCurrentColumnFamilyStore().getTracker());

        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    /**
     * With immutable components, the index is dropped while it is rebuilt (so the drop doesn't remove the components
     * of the previous generation), then an index of the same name is created.
     */
    @Test
    public void testDropDuringRebuildWithImmutableComponents() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(true);
        createTableWithStaleIndex();
        paused = new CountDownLatch(1);
        release = new CountDownLatch(1);
        pauses.set(0);
        Injections.inject(Injections.newCustom("pause_build")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete").atEntry())
                                    .add(ActionBuilder.newActionBuilder().actions().doAction(StaleSameNameIndexComponentsTest.class.getName() + ".pauseBuild()"))
                                    .build());
        try
        {
            CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(INDEX));
            assertThat(paused.await(30, TimeUnit.SECONDS)).isTrue();
            dropIndex("DROP INDEX %s." + INDEX);
            release.countDown();
            try
            {
                rebuild.get(60, TimeUnit.SECONDS);
            }
            catch (Throwable t)
            {
                // the rebuild of a dropped index stops
            }
        }
        finally
        {
            release.countDown();
            Injections.deleteAll();
        }

        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    /**
     * The stale files are deleted on CREATE INDEX also while an entire-sstable stream holds the sstable, so the build
     * of the new index writes files of the same names anew rather than append to them.
     */
    @Test
    public void testComponentsDeletedWhileStreamed() throws Throwable
    {
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        Path saved = saveSSTableFiles();
        dropIndex("DROP INDEX %s." + INDEX);
        awaitDeleted(sstable, indexComponents(sstable, saved));
        restoreFiles(sstable, saved, indexComponents(sstable, saved));
        sstable.registerComponents(indexComponents(sstable, saved), getCurrentColumnFamilyStore().getTracker());

        assertThat(sstable.streamRebuildState().tryBeginStreaming()).isTrue();
        try
        {
            createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForTableIndexesQueryable();
        }
        finally
        {
            sstable.streamRebuildState().endStreaming();
        }
        assertBuiltFromData();
        assertSameFilesAsAfterARebuild(sstable);
    }

    /**
     * A numeric stale index (on o) and a literal new one (on a), created while a stream holds the sstable: the stale
     * files of the types the new index doesn't write are deleted too.
     */
    @Test
    public void testStaleComponentsOfOtherTypesWhileStreamed() throws Throwable
    {
        createTableWithStaleIndex("o", "t");
        importInto(saveSSTableFiles());
        SSTableReader sstable = sstable();
        assertThat(indexFilesOnDisk(sstable)).anyMatch(n -> n.endsWith("KDTree.db"));

        assertThat(sstable.streamRebuildState().tryBeginStreaming()).isTrue();
        try
        {
            createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForTableIndexesQueryable();
        }
        finally
        {
            sstable.streamRebuildState().endStreaming();
        }
        assertBuiltFromData();
        assertThat(indexFilesOnDisk(sstable)).noneMatch(n -> n.endsWith("KDTree.db") || n.endsWith("KDTreePostingLists.db"));

        dropIndex("DROP INDEX %s." + INDEX);
        waitForAssert(() -> assertThat(indexFilesOnDisk(sstable)).isEmpty(), 30, TimeUnit.SECONDS);
    }

    /**
     * Without immutable components, a file under the name of a component of the new build, that no TOC lists (e.g. left
     * by a crash), is deleted before the build: the build must not append to it.
     */
    @Test
    public void testUnlistedFileUnderTheNameOfTheNewBuild() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        Path saved = saveSSTableFiles();
        dropIndex("DROP INDEX %s." + INDEX);
        awaitDeleted(sstable, indexComponents(sstable, saved));
        int copied = 0;
        for (Component component : indexComponents(sstable, saved))
        {
            if (component.name.endsWith("TermsData.db"))
            {
                Files.copy(saved.resolve(sstable.descriptor.fileFor(component).name()), sstable.descriptor.fileFor(component).toPath());
                copied++;
            }
        }
        assertThat(copied).isEqualTo(1);

        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        assertSameFilesAsAfterARebuild(sstable);
    }

    /**
     * An SSTable written by DSE 6.8 (format bb) carries the files of an index "text_index" on whole values, found by
     * listing the directory: they are deleted, also while a stream holds the SSTable, for the ngram index of the name.
     */
    @Test
    public void testDSE68SSTable() throws Throwable
    {
        testDSE68SSTable(false);
    }

    @Test
    public void testDSE68SSTableWhileStreamed() throws Throwable
    {
        testDSE68SSTable(true);
    }

    private void testDSE68SSTable(boolean streamed) throws Throwable
    {
        SSTableReader sstable = loadDse68SSTable();
        int matching = 0;
        for (UntypedResultSet.Row row : execute("SELECT text_value FROM %s"))
            if (row.getString("text_value").contains("1"))
                matching++;
        assertThat(matching).isEqualTo(19);

        if (streamed)
            assertThat(sstable.streamRebuildState().tryBeginStreaming()).isTrue();
        try
        {
            createNgramIndex();
            waitForTableIndexesQueryable();
            assertThat(execute("SELECT pk FROM %s WHERE text_value : '1'")).hasSize(matching);
        }
        finally
        {
            if (streamed)
                sstable.streamRebuildState().endStreaming();
        }
    }

    /**
     * A query holds the reader of the DSE 6.8 index "text_index" (next to another index, which keeps the per-SSTable
     * components) across DROP INDEX and the CREATE of the ngram index of the same name: CREATE finds the files by
     * listing the directory and deletes them.
     */
    @Test
    public void testDSE68ReaderHeldAcrossDropAndCreate() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        SSTableReader sstable = loadDse68SSTable();
        createIndex("CREATE CUSTOM INDEX int_index ON %s(int_value) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        SSTableIndex held = getIndexContext("text_index").getView().getIndexes().iterator().next();
        assertThat(held.reference()).isTrue();
        try
        {
            dropIndex("DROP INDEX %s.text_index");
            assertThat(sstable.components()).noneMatch(c -> c.name.contains("text_index"));
            createNgramIndex();
            waitForTableIndexesQueryable();
            assertThat(execute("SELECT pk FROM %s WHERE text_value : '1'")).hasSize(19);
        }
        finally
        {
            held.release();
        }
        assertThat(execute("SELECT pk FROM %s WHERE text_value : '1'")).hasSize(19);
    }

    /**
     * With immutable components, a query holds the reader of the stale index across its drop and the creation of an
     * index of the same name: the files of the dropped index are deleted on CREATE.
     */
    @Test
    public void testReaderHeldAcrossDropAndCreateWithImmutableComponents() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(true);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        Set<String> stale = indexFilesOnDisk(sstable);
        SSTableIndex held = getIndexContext(INDEX).getView().getIndexes().iterator().next();
        assertThat(held.reference()).isTrue();
        try
        {
            dropIndex("DROP INDEX %s." + INDEX);
            createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForTableIndexesQueryable();
        }
        finally
        {
            held.release();
        }
        assertBuiltFromData();
        Set<String> files = indexFilesOnDisk(sstable);
        Set<String> listed = indexComponents(sstable).stream().map(c -> sstable.descriptor.fileFor(c).name()).collect(Collectors.toSet());
        assertThat(files).as("the index's files on disk, of which %s were the dropped index's", stale).isEqualTo(listed);
    }

    /**
     * A query holds the reader of the stale index across its drop and the creation of an index of the same definition:
     * when the initial build of the new index starts, the descriptor cached for the SSTable, which the other index
     * keeps, still has the components of the dropped one, which were built for the same definition.
     */
    @Test
    public void testReaderHeldAcrossDropAndCreateWithTheSameDefinition() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex("a", "o");
        SSTableReader sstable = sstable();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
        assertThat(group.descriptorFor(sstable).perIndexComponents(getIndexContext(INDEX)).isComplete()).isTrue();
        SSTableIndex held = getIndexContext(INDEX).getView().getIndexes().iterator().next();
        assertThat(held.reference()).isTrue();

        buildPaused = new CountDownLatch(1);
        buildRelease = new CountDownLatch(1);
        Injections.inject(Injections.newCustom("pause_initial_build")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild").atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".pauseInitialBuild()"))
                                    .build());
        try
        {
            dropIndex("DROP INDEX %s." + INDEX);
            buildArmed = true;
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            assertThat(buildPaused.await(60, TimeUnit.SECONDS)).isTrue();
            assertThat(group.descriptorFor(sstable).perIndexComponents(getIndexContext(INDEX)).isComplete()).isTrue();
        }
        finally
        {
            buildArmed = false;
            buildRelease.countDown();
            held.release();
            Injections.deleteAll();
        }
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    /**
     * The only SAI index is dropped while a flush is in flight, so no index group takes the record of which index the
     * flushed SSTable's writer wrote for; the index is created again, then an aborted compaction adds the SSTable
     * again: the index keeps its components.
     */
    @Test
    public void testLastIndexDroppedDuringFlushThenAbortedCompaction() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows();

        injectFlushPause(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class)
                                           .onMethod("complete").atEntry());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        try
        {
            assertThat(flushPaused.await(30, TimeUnit.SECONDS)).isTrue();
            dropIndex("DROP INDEX %s." + INDEX);
            assertThat(StorageAttachedIndexGroup.getIndexGroup(cfs)).isNull();
            flushRelease.countDown();
            flush.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            flushRelease.countDown();
            Injections.deleteAll();
        }

        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        SSTableReader sstable = sstable();
        Map<String, Long> built = indexFileSizes(sstable);

        // what SSTableRewriter does when it opens its output early, then what an aborted compaction does
        DecoratedKey second;
        try (KeyIterator keys = sstable.keyIterator())
        {
            keys.next();
            second = keys.next();
        }
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.COMPACTION))
        {
            assertThat(txn).isNotNull();
            txn.update(sstable.cloneWithNewStart(second), true);
            txn.checkpoint();
            txn.abort();
        }
        assertThat(indexFileSizes(sstable())).isEqualTo(built);
        assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isTrue();
        assertBuiltFromData();
    }

    /**
     * When the files of the table's SSTables can't be listed (injected), CREATE INDEX succeeds and the index isn't
     * built until it is rebuilt, since files of its name that the TOC doesn't list may be left.
     */
    @Test
    public void testListingFails() throws Throwable
    {
        createTableWithStaleIndex();
        dropIndex("DROP INDEX %s." + INDEX);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Injections.inject(Injections.newCustom("listing_returns_null")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(File.class).onMethod("tryListNames").atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(StaleSameNameIndexComponentsTest.class.getName() + ".listingInHook()")
                                                      .actions()
                                                      .doReturn("null"))
                                    .build(),
                          Injections.newCustom("listing_throws")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(File.class).onMethod("listNames").atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(StaleSameNameIndexComponentsTest.class.getName() + ".listingInHook()")
                                                      .actions()
                                                      .doThrow(IOException.class, Expression.quote("Injected listing failure")))
                                    .build());
        try
        {
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            assertThat(cfs.indexManager.getIndexByName(INDEX)).isNotNull();
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 30, TimeUnit.SECONDS);
            assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isFalse();
        }
        finally
        {
            Injections.deleteAll();
        }
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 'a3'")).isInstanceOf(ReadFailureException.class);
        rebuildIndexes(INDEX);
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    // called by Byteman
    public static boolean listingInHook()
    {
        for (StackTraceElement element : Thread.currentThread().getStackTrace())
            if (element.getMethodName().equals("onAddedToExistingTable"))
                return true;
        return false;
    }

    /**
     * A query holds the reader of the stale index across its drop and the creation of a legacy index of the same name:
     * the release of the reader deletes the SAI files.
     */
    @Test
    public void testReaderHeldAcrossDropAndCreateOfLegacyIndex() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        SSTableIndex held = getIndexContext(INDEX).getView().getIndexes().iterator().next();
        assertThat(held.reference()).isTrue();
        try
        {
            dropIndex("DROP INDEX %s." + INDEX);
            createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'legacy_local_table'");
            assertThat(indexFilesOnDisk(sstable)).isNotEmpty();
        }
        finally
        {
            held.release();
        }
        assertThat(indexFilesOnDisk(sstable)).isEmpty();
    }

    /**
     * A compaction's output is live (opened early) before its index writers complete. The compaction is paused before
     * it writes the completion marker of the stale index, which is dropped and created again on a, its initial build
     * held until the compaction commits: the marker isn't taken for the new index's. The index may end up
     * non-queryable.
     */
    @Test
    public void testCompactionOutputOpenedEarlyAcrossDropAndCreate() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE INDEX " + INDEX + " ON %s(n) USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        waitForTableIndexesQueryable();
        String padding = "x".repeat(300);
        for (int pk = 0; pk < 200; pk++)
        {
            execute("INSERT INTO %s (pk, t, a, o) VALUES (?, ?, ?, ?)", pk, "t" + pk + padding, "a" + pk % 7, pk);
            if (pk == 99)
                flush();
        }
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(2);

        compactionPaused = new CountDownLatch(1);
        compactionRelease = new CountDownLatch(1);
        buildPaused = new CountDownLatch(1);
        buildRelease = new CountDownLatch(1);
        Injections.inject(Injections.newCustom("pause_compaction")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(SSTableIndexWriter.class).onMethod("writeSegmentsMetadata").atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".pauseCompactionInIndexWriter($0)"))
                                    .build(),
                          Injections.newCustom("pause_initial_build")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild").atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".pauseInitialBuild()"))
                                    .build());
        compactionArmed = true;
        CompletableFuture<Void> compaction = CompletableFuture.runAsync(cfs::forceMajorCompaction);
        try
        {
            assertThat(compactionPaused.await(60, TimeUnit.SECONDS)).isTrue();
            buildArmed = true;
            dropIndex("DROP INDEX %s." + INDEX);
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            compactionRelease.countDown();
            assertThat(buildPaused.await(60, TimeUnit.SECONDS)).isTrue();
            compaction.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            compactionArmed = false;
            buildArmed = false;
            compactionRelease.countDown();
            buildRelease.countDown();
            Injections.deleteAll();
        }
        waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 60, TimeUnit.SECONDS);
        if (cfs.indexManager.isIndexQueryable(INDEX))
            assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(29);
        rebuildIndexes(INDEX);
        waitForTableIndexesQueryable();
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(29);
    }

    /**
     * The SSTable with the components of an interrupted drop isn't live when the index is created: a compaction that
     * opens its result early has written all of it out. The compaction is then stopped, which restores the SSTable:
     * the stale components were discarded all the same.
     */
    @Test
    public void testComponentsOfSSTableWrittenOutByCompaction() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        int preemptiveOpenInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE INDEX " + INDEX + " ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        waitForTableIndexesQueryable();

        // two SSTables of disjoint token ranges, each large enough for a compaction to open its result early
        Random random = new Random(1);
        int a3 = 0;
        for (boolean first : new boolean[]{ true, false })
        {
            int rows = 0;
            for (int pk = 0; rows < 1500; pk++)
            {
                if ((Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk)).getLongValue() < 0) != first)
                    continue;
                StringBuilder pad = new StringBuilder();
                for (int i = 0; i < 1000; i++)
                    pad.append(Integer.toHexString(random.nextInt(256)));
                execute("INSERT INTO %s (pk, t, a, o, n) VALUES (?, ?, ?, ?, ?)", pk, "t" + pk, "a" + pk % 7, pk, pad.toString());
                if (pk % 7 == 3)
                    a3++;
                rows++;
            }
            flush();
        }
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader first = cfs.getLiveSSTables().stream().min(Comparator.comparing(SSTableReader::getFirst)).orElseThrow();

        // the drop doesn't remove the components of the first SSTable (see testComponentsOfAnInterruptedDrop)
        Set<Component> components = indexComponents(first);
        Path saved = Files.createTempDirectory("stale-components");
        for (Component component : components)
            Files.copy(first.descriptor.fileFor(component).toPath(), saved.resolve(first.descriptor.fileFor(component).name()));
        dropIndex("DROP INDEX %s." + INDEX);
        awaitDeleted(first, components);
        restoreFiles(first, saved, components);
        first.registerComponents(components, cfs.getTracker());

        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(1);
        writtenOutTable = cfs;
        writtenOut = first;
        holdWrittenOut = true;
        Injections.inject(Injections.newCustom("hold_compaction_once_first_sstable_written_out")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(SSTableRewriter.class).onMethod("append").atExit())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".holdOnceWrittenOut()"))
                                    .build());
        CompletableFuture<Void> compaction = CompletableFuture.runAsync(cfs::forceMajorCompaction);
        try
        {
            waitForAssert(() -> assertThat(holdingWrittenOut).isTrue(), 60, TimeUnit.SECONDS);
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            CompactionManager.instance.interruptCompactionFor(Collections.singleton(cfs.metadata()), Predicates.alwaysTrue(), Predicates.alwaysTrue(),
                                                              false, TableOperation.StopTrigger.CLEANUP);
        }
        finally
        {
            holdWrittenOut = false;
            writtenOutTable = null;
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(preemptiveOpenInterval);
        }
        assertThatThrownBy(() -> compaction.get(60, TimeUnit.SECONDS)).hasRootCauseInstanceOf(CompactionInterruptedException.class);
        waitForTableIndexesQueryable();
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(a3);
    }

    /**
     * The only SAI index is dropped while a flush is in flight, created again and built, then the SSTable is removed
     * from the tracker and added again with the same descriptor: the index keeps its components.
     */
    @Test
    public void testStaleRecordThenAddedAgain() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        flushWhile(cfs, () -> dropIndex("DROP INDEX %s." + INDEX));

        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        SSTableReader sstable = sstable();
        Map<String, Long> built = indexFileSizes(sstable);

        addAgain(cfs, sstable);
        assertThat(indexFileSizes(sstable)).isEqualTo(built);
        assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isTrue();
        assertBuiltFromData();
    }

    /**
     * A table unloaded without being dropped while a flush is in flight is loaded again with new index instances and
     * the SSTable added again: the components on it are the reloaded index's, and are kept.
     */
    @Test
    public void testTableLoadedAgainThenSSTableAddedAgain() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        flushWhile(cfs, () -> cfs.getTracker().unsubscribe(group));
        cfs.getTracker().subscribe(group);
        SSTableReader sstable = sstable();
        Map<String, Long> written = indexFileSizes(sstable);
        assertThat(written).isNotEmpty();

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        TableMetadataRef metadata = Schema.instance.getTableMetadataRef(KEYSPACE, currentTable());
        ColumnFamilyStore loaded = ColumnFamilyStore.createColumnFamilyStore(keyspace, currentTable(), metadata,
                                                                             new Directories(metadata.get()),
                                                                             true, false, true);
        try
        {
            loaded.awaitInitialIndexBuilds(60, TimeUnit.SECONDS);
            assertThat(StorageAttachedIndexGroup.getIndexGroup(loaded).getIndexes())
            .noneMatch(i -> StorageAttachedIndexGroup.getIndexGroup(cfs).getIndexes().contains(i));
            assertThat(loaded.getLiveSSTables()).hasSize(1);
            SSTableReader reloaded = loaded.getLiveSSTables().iterator().next();
            addAgain(loaded, reloaded);
            assertThat(indexFileSizes(reloaded)).isEqualTo(written);
            assertThat(loaded.indexManager.isIndexQueryable(INDEX)).isTrue();
        }
        finally
        {
            loaded.unloadCf();
        }
    }

    /** Flushes the current memtable, running the provided action while the flush has its index writers. */
    private void flushWhile(ColumnFamilyStore cfs, Runnable action) throws Throwable
    {
        injectFlushPause(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class)
                                           .onMethod("complete").atEntry());
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        try
        {
            assertThat(flushPaused.await(30, TimeUnit.SECONDS)).isTrue();
            action.run();
            flushRelease.countDown();
            flush.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            flushRelease.countDown();
            Injections.deleteAll();
        }
    }

    private static void injectFlushPause(InvokePointBuilder at) throws Throwable
    {
        flushPaused = new CountDownLatch(1);
        flushRelease = new CountDownLatch(1);
        flushPauses.set(0);
        Injections.inject(Injections.newCustom("pause_flush")
                                    .add(at)
                                    .add(ActionBuilder.newActionBuilder().actions().doAction(StaleSameNameIndexComponentsTest.class.getName() + ".pauseFlush()"))
                                    .build());
    }

    /** Removes the SSTable from the tracker and adds it again, as a storage handler loading it again would. */
    private static void addAgain(ColumnFamilyStore cfs, SSTableReader sstable)
    {
        boolean backups = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
        try
        {
            cfs.getTracker().removeUnsafe(Collections.singleton(sstable));
            cfs.getTracker().addSSTables(Collections.singleton(sstable), OperationType.UNKNOWN);
        }
        finally
        {
            DatabaseDescriptor.setIncrementalBackupsEnabled(backups);
        }
    }

    /**
     * A flush has its index writers, not completed, when the index is dropped and one of the same name is created on
     * another column: the flush writes nothing for the dropped index.
     */
    @Test
    public void testFlushInFlightAcrossDropAndCreate() throws Throwable
    {
        testFlushInFlightAcrossDrop(false, true);
    }

    /**
     * Same, without the CREATE INDEX: nothing of the dropped index is left on the flushed SSTable.
     */
    @Test
    public void testFlushInFlightAcrossDrop() throws Throwable
    {
        testFlushInFlightAcrossDrop(false, false);
    }

    /**
     * Same, when the flush has completed its index writers, and the SSTable is not yet added: the components of the
     * dropped index are discarded when it is.
     */
    @Test
    public void testFlushedSSTableAddedAfterDropAndCreate() throws Throwable
    {
        testFlushInFlightAcrossDrop(true, true);
    }

    private void testFlushInFlightAcrossDrop(boolean indexWritersCompleted, boolean create) throws Throwable
    {
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE INDEX " + INDEX + " ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows();

        InvokePointBuilder complete = InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete");
        injectFlushPause(indexWritersCompleted ? complete.atExit() : complete.atEntry());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        try
        {
            assertThat(flushPaused.await(30, TimeUnit.SECONDS)).isTrue();
            dropIndex("DROP INDEX %s." + INDEX);
            if (create)
                createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            assertThat(flush).isNotDone();
            flushRelease.countDown();
            flush.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            flushRelease.countDown();
            Injections.deleteAll();
        }
        if (create)
        {
            waitForTableIndexesQueryable();
            assertBuiltFromData();
        }
        else
        {
            SSTableReader sstable = sstable();
            assertThat(indexComponents(sstable)).isEmpty();
            assertThat(indexFilesOnDisk(sstable)).isEmpty();
        }
    }

    /**
     * When the TOC can't be rewritten on CREATE INDEX (e.g. the disk is full), the files of the stale components are
     * deleted all the same.
     */
    @Test
    public void testHookFailsToUnregister() throws Throwable
    {
        createTableWithStaleIndex();
        importInto(saveSSTableFiles());
        Injections.inject(failUnregister());
        try
        {
            createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForTableIndexesQueryable();
        }
        finally
        {
            Injections.deleteAll();
        }
        assertBuiltFromData();
    }

    /**
     * Neither the TOC can be rewritten, nor the completion marker of the stale components deleted: the index fails its
     * initial build, rather than use them.
     */
    @Test
    public void testHookFailsToUnregisterAndDeleteCompletionMarker() throws Throwable
    {
        createTableWithStaleIndex();
        importInto(saveSSTableFiles());
        Injections.inject(failUnregister());
        Injections.inject(Injections.newCustom("keep_marker")
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass(StorageAttachedIndex.class)
                                                           .onMethod("completionMarkerDeleted")
                                                           .atExit())
                                    .add(ActionBuilder.newActionBuilder().actions().doReturn(false))
                                    .build());
        try
        {
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 30, TimeUnit.SECONDS);
            assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isFalse();
        }
        finally
        {
            Injections.deleteAll();
        }
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 'a3'")).isInstanceOf(ReadFailureException.class);
    }

    private static Injection failUnregister()
    {
        return Injections.newCustom("fail_unregister")
                         .add(InvokePointBuilder.newInvokePoint().onClass(SSTable.class).onMethod("unregisterComponents").atEntry())
                         .add(ActionBuilder.newActionBuilder()
                                           .conditions()
                                           .when(StaleSameNameIndexComponentsTest.class.getName() + ".calledBy(\"onAddedToExistingTable\")")
                                           .actions()
                                           .doThrow(RuntimeException.class, Expression.quote("Failed to write the TOC (injected)")))
                         .build();
    }

    // called by Byteman
    public static boolean calledBy(String method)
    {
        for (StackTraceElement element : Thread.currentThread().getStackTrace())
            if (element.getMethodName().equals(method))
                return true;
        return false;
    }

    /**
     * A query holds an SSTable index of the stale index across its drop, the creation of an index of the same name
     * and its build: the release of the dropped index's reader must not delete the files of the new one, which have
     * the same names.
     */
    @Test
    public void testReaderOfDroppedIndexReleasedAfterCreate() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        SSTableIndex held = getIndexContext(INDEX).getView().getIndexes().iterator().next();
        assertThat(held.reference()).isTrue();
        Set<String> filesOfNewIndex;
        try
        {
            dropIndex("DROP INDEX %s." + INDEX);
            createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForTableIndexesQueryable();
            filesOfNewIndex = indexFilesOnDisk(sstable);
            assertThat(filesOfNewIndex).isNotEmpty();
        }
        finally
        {
            held.release();
        }
        assertThat(indexFilesOnDisk(sstable)).isEqualTo(filesOfNewIndex);
        assertBuiltFromData();
        simulateNodeRestart();
        assertBuiltFromData();
    }

    /**
     * Table with an index {@link #INDEX} on column t, another one on column o, and one sstable of 100 rows with long
     * values of t.
     */
    private void createTableWithStaleIndex() throws Throwable
    {
        createTableWithStaleIndex("t", "o");
        String padding = "x".repeat(300);
        assertThat(execute("SELECT pk FROM %s WHERE t = ?", "t3" + padding)).hasSize(1);
    }

    /**
     * Table with an index {@link #INDEX} on the provided column, another index on the other provided column (so that
     * the per-SSTable components stay when the stale index is dropped), and one sstable of 100 rows.
     */
    private void createTableWithStaleIndex(String staleColumn, String otherColumn) throws Throwable
    {
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE INDEX " + INDEX + " ON %s(" + staleColumn + ") USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(" + otherColumn + ") USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows();
        flush();
    }

    private void insertRows() throws Throwable
    {
        String padding = "x".repeat(300);
        for (int pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, t, a, o) VALUES (?, ?, ?, ?)", pk, "t" + pk + padding, "a" + pk % 7, pk);
    }

    /**
     * Creates a new table without indexes, and imports the sstable saved from another table into it, with the components
     * of its indexes.
     */
    private void importInto(Path saved) throws Throwable
    {
        // index names are unique in a keyspace
        dropIndex("DROP INDEX %s." + INDEX);
        dropIndex("DROP INDEX %s.other_idx");
        createTable(SCHEMA);
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        for (String name : saved.toFile().list())
            Files.copy(saved.resolve(name), dir.toPath().resolve(name));
        cfs.loadNewSSTables();
        assertThat(sstable().components()).anyMatch(c -> c.name.contains('+' + INDEX + '+'));
    }

    private void assertSameFilesAsAfterARebuild(SSTableReader sstable)
    {
        Map<String, Long> built = indexFileSizes(sstable);
        rebuildIndexes(INDEX);
        waitForTableIndexesQueryable();
        assertThat(built).isEqualTo(indexFileSizes(sstable));
    }

    private static Set<String> indexFilesOnDisk(SSTableReader sstable)
    {
        return indexFileSizes(sstable).keySet();
    }

    private static Map<String, Long> indexFileSizes(SSTableReader sstable)
    {
        String prefix = sstable.descriptor.baseFile().name() + '-';
        Map<String, Long> sizes = new TreeMap<>();
        for (File file : sstable.descriptor.directory.tryList())
            if (file.isFile() && file.name().startsWith(prefix) && file.name().contains('+' + INDEX + '+'))
                sizes.put(file.name(), file.length());
        return sizes;
    }

    private void assertBuiltFromData()
    {
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(14);
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a0'")).hasSize(15);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexComponents.ForRead components = group.descriptorFor(sstable).perIndexComponents(getIndexContext(INDEX));
            assertThat(components.isComplete()).isTrue();
            assertThat(components.isValid(true)).isTrue();
        }
    }

    private void awaitDeleted(SSTableReader sstable, Set<Component> components)
    {
        waitForAssert(() -> assertThat(components).noneMatch(c -> sstable.descriptor.fileFor(c).exists()), 30, TimeUnit.SECONDS);
    }

    private SSTableReader sstable()
    {
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertThat(sstables).hasSize(1);
        return sstables.iterator().next();
    }

    private static Set<Component> indexComponents(SSTableReader sstable)
    {
        return sstable.components().stream().filter(c -> c.name.contains('+' + INDEX + '+')).collect(Collectors.toSet());
    }

    private static Set<Component> indexComponents(SSTableReader sstable, Path saved)
    {
        String prefix = sstable.descriptor.baseFile().name() + '-';
        return Arrays.stream(saved.toFile().list())
                               .filter(n -> n.startsWith(prefix) && n.contains('+' + INDEX + '+'))
                               .map(n -> new Component(SSTableFormat.Components.Types.CUSTOM, n.substring(prefix.length())))
                               .collect(Collectors.toSet());
    }

    private Path saveSSTableFiles() throws Exception
    {
        SSTableReader sstable = sstable();
        Path saved = Files.createTempDirectory("stale-components");
        for (File file : sstable.descriptor.directory.tryList())
            if (file.isFile() && file.name().startsWith(sstable.descriptor.baseFile().name() + '-'))
                Files.copy(file.toPath(), saved.resolve(file.name()));
        return saved;
    }

    private static void restoreFiles(SSTableReader sstable, Path saved, Set<Component> components) throws IOException
    {
        for (Component component : components)
            Files.copy(saved.resolve(sstable.descriptor.fileFor(component).name()),
                       sstable.descriptor.fileFor(component).toPath(),
                       StandardCopyOption.REPLACE_EXISTING);
    }

    private SSTableReader loadDse68SSTable() throws IOException
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, int_value int, text_value text)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        FileUtils.copySSTablesAndIndexes(cfs.getDirectories().getDirectoryForNewSSTables().toJavaIOFile(), "aa");
        cfs.loadNewSSTables();
        SSTableReader sstable = sstable();
        assertThat(sstable.descriptor.version.version).isEqualTo("bb");
        return sstable;
    }

    private void createNgramIndex()
    {
        createIndex("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                    "{'index_analyzer': '{\"tokenizer\":{\"name\":\"ngram\", " +
                    "\"args\":{\"minGramSize\":\"1\", \"maxGramSize\":\"1\"}}}'}");
    }

    /**
     * A build of the stale index owns its files on the SSTable when the index is dropped and created again; a partial
     * build of the other index then finds the per-SSTable components corrupt, and its rewrite fails once it has deleted
     * them: the indexes it rebuilt become non-queryable, but not the new index, which it left out before deleting
     * anything, and whose own build goes on once the owner is done.
     */
    @Test
    public void testFailedRewriteAsItDeletesCorruptComponentsWhileBuildOfDroppedIndexOwnsItsFiles() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        createIndex("CREATE INDEX sibling_idx ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = sstable();
        StorageAttachedIndex dropped = (StorageAttachedIndex) cfs.indexManager.getIndexByName(INDEX);
        PerIndexFilesOwner owner = PerIndexFilesOwner.tryOwn(sstable.descriptor, INDEX, dropped.isDropped());
        assertThat(owner).isNotNull();
        boolean validate = SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD.getBoolean();
        try
        {
            dropIndex("DROP INDEX %s." + INDEX);
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(t) USING 'sai'");
            IndexComponent.ForRead component = loadDescriptor(sstable, cfs).perSSTableComponents()
                                                                           .all()
                                                                           .stream()
                                                                           .filter(c -> c.componentType() != IndexComponentType.GROUP_COMPLETION_MARKER)
                                                                           .max(Comparator.comparingLong(c -> c.file().length()))
                                                                           .orElseThrow();
            flipMiddleByte(component.file());
            SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD.setBoolean(true);
            Injections.inject(Injections.newCustom("fail_rewrite")
                                        .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("prepareForRebuild").atExit())
                                        .add(ActionBuilder.newActionBuilder()
                                                          .actions()
                                                          .doThrow(RuntimeException.class, Expression.quote("Injected rewrite failure")))
                                        .build());

            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
            assertThat(CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, "other_idx")).get(60, TimeUnit.SECONDS))
            .hasStackTraceContaining("Injected rewrite failure");
            Injections.deleteAll();
            assertThat(cfs.indexManager.isIndexQueryable(cfs.indexManager.getIndexByName("other_idx"))).isFalse();
            assertThat(cfs.indexManager.isIndexQueryable(cfs.indexManager.getIndexByName("sibling_idx"))).isFalse();
            assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isTrue();
            assertThat(IndexStatusManager.instance.getIndexStatus(FBUtilities.getBroadcastAddressAndPort(), KEYSPACE, INDEX))
            .isNotEqualTo(Index.Status.BUILD_FAILED);
        }
        finally
        {
            SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD.setBoolean(validate);
            owner.close();
        }
        waitForAssert(() -> assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isTrue(), 60, TimeUnit.SECONDS);
        assertThat(execute("SELECT pk FROM %s WHERE t = ?", "t3" + "x".repeat(300))).hasSize(1);
    }
}
