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
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDefinitionDigest;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;

import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
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
    private static final String NGRAM_INDEX = "CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                                              "{'index_analyzer': '{\"tokenizer\":{\"name\":\"ngram\", " +
                                              "\"args\":{\"minGramSize\":\"1\", \"maxGramSize\":\"1\"}}}'}";

    private static volatile CountDownLatch paused;
    private static volatile CountDownLatch release;
    private static final AtomicInteger pauses = new AtomicInteger();

    private static volatile CountDownLatch writerPaused;
    private static volatile CountDownLatch writerRelease;
    private static volatile boolean writerArmed;
    private static volatile CountDownLatch buildPaused;
    private static volatile CountDownLatch buildRelease;
    private static volatile boolean buildArmed;

    private static volatile CountDownLatch flushPaused;
    private static volatile CountDownLatch flushRelease;
    private static final AtomicInteger flushPauses = new AtomicInteger();

    private static volatile CountDownLatch hookPaused;
    private static volatile CountDownLatch hookRelease;

    private static volatile CountDownLatch buildDone;

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
    public static void pauseIndexWriter(Object writer) throws Exception
    {
        if (!writerArmed)
            return;
        Field field = SSTableIndexWriter.class.getDeclaredField("indexContext");
        field.setAccessible(true);
        if (!INDEX.equals(((IndexContext) field.get(writer)).getIndexName()))
            return;
        writerArmed = false;
        writerPaused.countDown();
        writerRelease.await(60, TimeUnit.SECONDS);
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

        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    /**
     * SSTables carrying the components of an index of the same definition (in another table) are imported into a table
     * without indexes, then the index is created: it uses them, without building the SSTables again.
     */
    @Test
    public void testImportedComponentsOfSameDefinition() throws Throwable
    {
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(o) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        insertRows();
        flush();
        importInto(saveSSTableFiles());
        SSTableReader sstable = sstable();
        Map<String, Object> imported = indexFileKeys(sstable);
        assertThat(imported).isNotEmpty();

        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        assertThat(indexFileKeys(sstable)).isEqualTo(imported);
        assertBuiltFromData();
    }

    /**
     * A flush has written the files of the index when it is dropped and created again with the same definition: the new
     * index uses them and returns every row.
     */
    @Test
    public void testIdenticalIndexCreatedAcrossFlush() throws Throwable
    {
        testIdenticalIndexCreatedAcrossFlush(true);
    }

    /**
     * Same, when the flush hasn't written the files of the index yet: it writes none, and the new index builds the
     * SSTable.
     */
    @Test
    public void testIdenticalIndexCreatedAcrossFlushBeforeIndexWritersComplete() throws Throwable
    {
        testIdenticalIndexCreatedAcrossFlush(false);
    }

    private void testIdenticalIndexCreatedAcrossFlush(boolean indexWritersCompleted) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(o) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        insertRows(0, 100);
        flush();
        insertRows(100, 200);

        InvokePointBuilder complete = InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete");
        injectFlushPause(indexWritersCompleted ? complete.atExit() : complete.atEntry());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        try
        {
            assertThat(flushPaused.await(30, TimeUnit.SECONDS)).isTrue();
            dropIndex("DROP INDEX %s." + INDEX);
            createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            flushRelease.countDown();
            flush.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            flushRelease.countDown();
            Injections.deleteAll();
        }
        waitForTableIndexesQueryable();
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(29);
        simulateNodeRestart();
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(29);
    }

    /**
     * A rebuild of the index is held after it wrote the files of an SSTable; the index is dropped and created again with
     * the same definition, and the initial build of the new index is held until the rebuild has stopped (and, as a
     * rebuild of a dropped index, deleted the files it wrote): the new index builds the SSTable and returns every row,
     * also after a restart.
     */
    @Test
    public void testIdenticalIndexCreatedWhileRebuildRuns() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(o) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        insertRows();
        flush();

        paused = new CountDownLatch(1);
        release = new CountDownLatch(1);
        pauses.set(0);
        Injections.inject(Injections.newCustom("pause_rebuild")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("validateIndexes").atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(StaleSameNameIndexComponentsTest.class.getName() + ".calledBy(\"completeSSTable\")")
                                                      .actions()
                                                      .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".pauseBuild()"))
                                    .build(),
                          initialBuildPause(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild").atEntry()));
        try
        {
            CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(INDEX));
            assertThat(paused.await(30, TimeUnit.SECONDS)).isTrue();
            dropIndex("DROP INDEX %s." + INDEX);
            buildArmed = true;
            createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            assertThat(buildPaused.await(60, TimeUnit.SECONDS)).isTrue();
            release.countDown();
            // the rebuild of a dropped index stops
            assertThatThrownBy(() -> rebuild.get(60, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
        }
        finally
        {
            buildArmed = false;
            release.countDown();
            buildRelease.countDown();
            Injections.deleteAll();
        }
        waitForTableIndexesQueryable();
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(14);
        simulateNodeRestart();
        assertBuiltFromData();
    }

    /**
     * The initial build of the index is held after it checked for a drop and wrote some of the files of an SSTable; the
     * index is dropped and created again with the same definition, which deletes those files, then the build goes on:
     * it writes no completion marker, and the new index builds the SSTable.
     */
    @Test
    public void testIdenticalIndexCreatedWhileInitialBuildCompletes() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(o) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        insertRows();
        flush();
        SSTableReader sstable = sstable();

        buildDone = new CountDownLatch(1);
        Injections.inject(indexWriterPause(),
                          initialBuildPause(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild").atEntry()),
                          Injections.newCustom("build_done")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("build").atExit())
                                    .add(ActionBuilder.newActionBuilder().actions().doAction(StaleSameNameIndexComponentsTest.class.getName() + ".buildDone()"))
                                    .build());
        try
        {
            writerArmed = true;
            createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            assertThat(writerPaused.await(60, TimeUnit.SECONDS)).isTrue();
            assertThat(indexFilesOnDisk(sstable)).isNotEmpty();
            dropIndex("DROP INDEX %s." + INDEX);
            buildArmed = true;
            createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            assertThat(buildPaused.await(60, TimeUnit.SECONDS)).isTrue();
            writerRelease.countDown();
            assertThat(buildDone.await(60, TimeUnit.SECONDS)).isTrue();
            assertThat(indexFilesOnDisk(sstable)).noneMatch(n -> n.endsWith("ColumnComplete.db"));
        }
        finally
        {
            writerArmed = false;
            buildArmed = false;
            writerRelease.countDown();
            buildRelease.countDown();
            Injections.deleteAll();
        }
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        simulateNodeRestart();
        assertBuiltFromData();
    }

    // called by Byteman
    public static void buildDone()
    {
        buildDone.countDown();
    }

    /** The file keys (inodes) of the files of {@link #INDEX} on the provided SSTable, by name. */
    private static Map<String, Object> indexFileKeys(SSTableReader sstable) throws IOException
    {
        Map<String, Object> keys = new TreeMap<>();
        for (String name : indexFileSizes(sstable).keySet())
            keys.put(name, Files.readAttributes(sstable.descriptor.directory.toPath().resolve(name), BasicFileAttributes.class).fileKey());
        return keys;
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

        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
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

        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    /**
     * Without immutable components, the stale files listed in the TOC are deleted on CREATE INDEX, so the build of the
     * new index writes files of the same names anew rather than append to them.
     */
    @Test
    public void testListedComponentsDeleted() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        Path saved = saveSSTableFiles();
        dropIndex("DROP INDEX %s." + INDEX);
        awaitDeleted(sstable, indexComponents(sstable, saved));
        restoreFiles(sstable, saved, indexComponents(sstable, saved));
        sstable.registerComponents(indexComponents(sstable, saved), getCurrentColumnFamilyStore().getTracker());

        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        assertSameFilesAsAfterARebuild(sstable);
    }

    /**
     * A numeric stale index (on o) and a literal new one (on a): the stale files of the types the new index doesn't
     * write are deleted too.
     */
    @Test
    public void testStaleComponentsOfOtherTypes() throws Throwable
    {
        createTableWithStaleIndex("o", "t");
        importInto(saveSSTableFiles());
        SSTableReader sstable = sstable();
        assertThat(indexFilesOnDisk(sstable)).anyMatch(n -> n.endsWith("KDTree.db"));

        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        assertThat(indexFilesOnDisk(sstable)).noneMatch(n -> n.endsWith("KDTree.db") || n.endsWith("KDTreePostingLists.db"));

        dropIndex("DROP INDEX %s." + INDEX);
        waitForAssert(() -> assertThat(indexFilesOnDisk(sstable)).isEmpty(), 30, TimeUnit.SECONDS);
    }

    /**
     * SSTables carrying the components of a numeric index of the same name (on o) are imported once the index exists on
     * a: they are built for the index, the TOC lists only the files of that build, and restarts keep them.
     */
    @Test
    public void testImportedComponentsOfOtherTypesOnceIndexExists() throws Throwable
    {
        testImportedComponentsOfOtherTypesOnceIndexExists(false);
    }

    @Test
    public void testImportedComponentsOfOtherTypesOnceIndexExistsWithImmutableComponents() throws Throwable
    {
        testImportedComponentsOfOtherTypesOnceIndexExists(true);
    }

    private void testImportedComponentsOfOtherTypesOnceIndexExists(boolean immutable) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(immutable);
        createTableWithStaleIndex("o", "t");
        importInto(saveSSTableFiles(), "CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        SSTableReader sstable = sstable();
        Set<Component> listed = assertTocListsOneBuild(sstable, immutable);

        simulateNodeRestart();
        simulateNodeRestart();
        assertBuiltFromData();
        assertThat(assertTocListsOneBuild(sstable, immutable)).isEqualTo(listed);
    }

    /**
     * The TOC of the SSTable lists the files of one build of {@link #INDEX}, which exist and are complete; without
     * immutable components, they are all the files of the index on disk.
     *
     * @return the listed components
     */
    private Set<Component> assertTocListsOneBuild(SSTableReader sstable, boolean immutable) throws IOException
    {
        Set<Component> listed = SSTable.readTOC(sstable.descriptor, false)
                                       .stream()
                                       .filter(c -> c.name.contains('+' + INDEX + '+'))
                                       .collect(Collectors.toSet());
        assertThat(listed).isNotEmpty();
        assertThat(listed).allMatch(c -> sstable.descriptor.fileFor(c).exists());
        assertThat(listed.stream().map(c -> Version.tryParseFileName(c.name).get().buildId).distinct()).hasSize(1);
        assertThat(loadDescriptor(sstable, getCurrentColumnFamilyStore()).perIndexComponents(getIndexContext(INDEX)).isComplete()).isTrue();
        if (!immutable)
            assertThat(indexFilesOnDisk(sstable)).isEqualTo(listed.stream().map(c -> sstable.descriptor.fileFor(c).name()).collect(Collectors.toSet()));
        return listed;
    }

    /**
     * Files of the stale index that the TOC doesn't list, with an empty completion marker (as written before the marker
     * carried the definition of the index), on an SSTable whose TOC lists a missing SAI file, so that its components
     * are found by scanning the directory: CREATE INDEX finds them by listing the directory, and the index is built from
     * the data.
     */
    @Test
    public void testUnlistedLegacyComponentsFoundByScanningTheDirectory() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        Path saved = saveSSTableFiles();
        Set<Component> stale = indexComponents(sstable, saved);
        dropIndex("DROP INDEX %s." + INDEX);
        awaitDeleted(sstable, stale);
        restoreFiles(sstable, saved, stale);
        Component missing = null;
        for (Component component : stale)
        {
            if (component.name.endsWith("ColumnComplete.db"))
            {
                Files.write(sstable.descriptor.fileFor(component).toPath(), new byte[0]);
                missing = new Component(Component.Type.CUSTOM, component.name.replace('+' + INDEX + '+', "+missing_idx+"));
            }
        }
        assertThat(missing).isNotNull();
        Tracker tracker = getCurrentColumnFamilyStore().getTracker();
        sstable.registerComponents(Collections.singleton(missing), tracker);
        try
        {
            createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            waitForTableIndexesQueryable();
            assertBuiltFromData();
        }
        finally
        {
            sstable.unregisterComponents(Collections.singleton(missing), tracker);
        }
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

        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        assertSameFilesAsAfterARebuild(sstable);
    }

    /**
     * An SSTable written by DSE 6.8 (format bb), whose TOC doesn't list its SAI files, carries the files of an index
     * "text_index" on whole values: the ngram index of the name is built from the data.
     */
    @Test
    public void testDSE68SSTable() throws Throwable
    {
        SSTableReader sstable = loadDse68SSTable();
        int matching = 0;
        for (UntypedResultSet.Row row : execute("SELECT text_value FROM %s"))
            if (row.getString("text_value").contains("1"))
                matching++;
        assertThat(matching).isEqualTo(19);

        createNgramIndex();
        waitForTableIndexesQueryable();
        assertThat(execute("SELECT pk FROM %s WHERE text_value : '1'")).hasSize(matching);
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
            createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
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
     * The only SAI index is dropped while a flush is in flight; the index is created again, then an aborted compaction
     * adds the SSTable again: the index keeps its components.
     */
    @Test
    public void testLastIndexDroppedDuringFlushThenAbortedCompaction() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
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

        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        SSTableReader sstable = sstable();
        Map<String, Long> built = indexFileSizes(sstable);

        // what SSTableRewriter does when it opens its output early, then what an aborted compaction does
        DecoratedKey second;
        try (KeyIterator keys = KeyIterator.forSSTable(sstable))
        {
            keys.next();
            second = keys.next();
        }
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.COMPACTION))
        {
            assertThat(txn).isNotNull();
            txn.update(sstable.cloneWithNewStart(second, null), true);
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
        Injections.inject(listingFails());
        try
        {
            createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
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

    private static Injection listingFails()
    {
        return Injections.newCustom("listing_returns_null")
                         .add(InvokePointBuilder.newInvokePoint().onClass(File.class).onMethod("tryListNames").atEntry())
                         .add(ActionBuilder.newActionBuilder()
                                           .conditions()
                                           .when(StaleSameNameIndexComponentsTest.class.getName() + ".listingInHook()")
                                           .actions()
                                           .doReturn("null"))
                         .build();
    }

    // called by Byteman
    public static boolean listingInHook()
    {
        return calledBy("onAddedToExistingTable");
    }

    /**
     * A flush has written the files of the stale index when it is dropped; CREATE INDEX of the same name lists the
     * table's SSTables while the flush adds its SSTable, before the new index is registered: the new index is built
     * from the data of that SSTable too.
     */
    @Test
    public void testFlushAddedWhileCreateListsSSTables() throws Throwable
    {
        testFlushAddedWhileCreateListsSSTables(true);
    }

    /**
     * Same, without another SAI index on the table, so that no index group sees the flushed SSTable added.
     */
    @Test
    public void testFlushAddedWhileCreateListsSSTablesWithoutOtherIndex() throws Throwable
    {
        testFlushAddedWhileCreateListsSSTables(false);
    }

    private void testFlushAddedWhileCreateListsSSTables(boolean otherIndex) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(t) USING 'StorageAttachedIndex'");
        if (otherIndex)
            createIndex("CREATE CUSTOM INDEX other_idx ON %s(o) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        insertRows(0, 100);
        // a live SSTable, so that CREATE INDEX lists files
        flush();
        insertRows(100, 200);

        injectFlushPause(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete").atExit());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        hookPaused = new CountDownLatch(1);
        hookRelease = new CountDownLatch(1);
        try
        {
            assertThat(flushPaused.await(30, TimeUnit.SECONDS)).isTrue();
            dropIndex("DROP INDEX %s." + INDEX);
            Injections.inject(Injections.newCustom("pause_hook")
                                        .add(InvokePointBuilder.newInvokePoint().onClass(File.class).onMethod("tryListNames").atEntry())
                                        .add(ActionBuilder.newActionBuilder()
                                                          .conditions()
                                                          .when(StaleSameNameIndexComponentsTest.class.getName() + ".listingInHook()")
                                                          .actions()
                                                          .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".pauseHook()"))
                                        .build());
            CompletableFuture<Void> create = CompletableFuture.runAsync(() -> createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'"));
            assertThat(hookPaused.await(30, TimeUnit.SECONDS)).isTrue();
            flushRelease.countDown();
            flush.get(60, TimeUnit.SECONDS);
            assertThat(cfs.getLiveSSTables()).hasSize(2);
            hookRelease.countDown();
            create.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            flushRelease.countDown();
            hookRelease.countDown();
            Injections.deleteAll();
        }
        waitForTableIndexesQueryable();
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(29);
    }

    /**
     * A flush is about to add its SSTable, written for the stale index while it existed, when the index is dropped and
     * one of the same name is created on another column: the new index is built from the data of that SSTable.
     */
    @Test
    public void testIndexReplacedWhileFlushAddsSSTable() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(t) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(o) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        insertRows();

        injectFlushPause(InvokePointBuilder.newInvokePoint().onClass(Tracker.class).onMethod("notifyAdding").atExit());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        try
        {
            assertThat(flushPaused.await(30, TimeUnit.SECONDS)).isTrue();
            dropIndex("DROP INDEX %s." + INDEX);
            createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            flushRelease.countDown();
            flush.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            flushRelease.countDown();
            Injections.deleteAll();
        }
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    // called by Byteman
    public static void pauseHook() throws InterruptedException
    {
        if (hookPaused.getCount() > 0)
        {
            hookPaused.countDown();
            hookRelease.await(60, TimeUnit.SECONDS);
        }
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
            createIndex("CREATE INDEX " + INDEX + " ON %s(a)");
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
     * held until the compaction commits: the new index builds the compacted SSTable.
     */
    @Test
    public void testCompactionOutputOpenedEarlyAcrossDropAndCreate() throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithTwoSSTablesOfStaleIndexOnN();
        Injections.inject(indexWriterPause(),
                          initialBuildPause(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild").atEntry()));
        writerArmed = true;
        CompletableFuture<Void> compaction = CompletableFuture.runAsync(cfs::forceMajorCompaction);
        try
        {
            assertThat(writerPaused.await(60, TimeUnit.SECONDS)).isTrue();
            buildArmed = true;
            dropIndex("DROP INDEX %s." + INDEX);
            createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            writerRelease.countDown();
            assertThat(buildPaused.await(60, TimeUnit.SECONDS)).isTrue();
            compaction.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            writerArmed = false;
            buildArmed = false;
            writerRelease.countDown();
            buildRelease.countDown();
            Injections.deleteAll();
        }
        waitForTableIndexesQueryable();
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(29);
    }

    /**
     * Table with an index {@link #INDEX} on column n, which has no values, another one on column o, and two SSTables of
     * 100 rows each.
     */
    private ColumnFamilyStore createTableWithTwoSSTablesOfStaleIndexOnN() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(o) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        insertRows(0, 100);
        flush();
        insertRows(100, 200);
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        return cfs;
    }

    /** Pauses an SSTable index writer of {@link #INDEX} before it writes its metadata, once armed. */
    private static Injection indexWriterPause()
    {
        writerPaused = new CountDownLatch(1);
        writerRelease = new CountDownLatch(1);
        return Injections.newCustom("pause_index_writer")
                         .add(InvokePointBuilder.newInvokePoint().onClass(SSTableIndexWriter.class).onMethod("writeSegmentsMetadata").atEntry())
                         .add(ActionBuilder.newActionBuilder()
                                           .actions()
                                           .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".pauseIndexWriter($0)"))
                         .build();
    }

    /** Pauses the initial build at the provided point, once armed. */
    private static Injection initialBuildPause(InvokePointBuilder at)
    {
        buildPaused = new CountDownLatch(1);
        buildRelease = new CountDownLatch(1);
        return Injections.newCustom("pause_initial_build")
                         .add(at)
                         .add(ActionBuilder.newActionBuilder()
                                           .actions()
                                           .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".pauseInitialBuild()"))
                         .build();
    }

    /**
     * The only SAI index is dropped while a flush is in flight, created again and built, then the SSTable is removed
     * from the tracker and added again with the same descriptor: the index keeps its components.
     */
    @Test
    public void testSSTableAddedAgainAfterDropAndCreate() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        insertRows();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        flushWhile(cfs, () -> dropIndex("DROP INDEX %s." + INDEX));

        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
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
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
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
     * Same, when the flush has completed its index writers, and the SSTable is not yet added: their completion markers
     * hold the definition of the dropped index, and the new index builds the SSTable once it is added.
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
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(t) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(o) USING 'StorageAttachedIndex'");
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
                createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
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
     * deleted all the same; the build of the index, under the names the TOC still lists, is listed after it and after a
     * restart.
     */
    @Test
    public void testHookFailsToUnregister() throws Throwable
    {
        testHookFailsToUnregister(false);
    }

    @Test
    public void testHookFailsToUnregisterWithImmutableComponents() throws Throwable
    {
        testHookFailsToUnregister(true);
    }

    private void testHookFailsToUnregister(boolean immutable) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(immutable);
        createTableWithStaleIndex();
        importInto(saveSSTableFiles());
        Injections.inject(failUnregister());
        try
        {
            createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            waitForTableIndexesQueryable();
        }
        finally
        {
            Injections.deleteAll();
        }
        assertBuiltFromData();
        SSTableReader sstable = sstable();
        Set<Component> listed = assertTocListsOneBuild(sstable, immutable);

        simulateNodeRestart();
        assertBuiltFromData();
        assertThat(assertTocListsOneBuild(sstable, immutable)).isEqualTo(listed);
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
        SSTableReader sstable = sstable();
        Set<String> markers = indexFilesOnDisk(sstable).stream().filter(n -> n.endsWith("ColumnComplete.db")).collect(Collectors.toSet());
        assertThat(markers).hasSize(1);
        Injections.inject(failUnregister(),
                          keepMarker("keep_marker_component", componentDelete(), "$0.file()", "onAddedToExistingTable"),
                          keepMarker("keep_marker_file", fileDelete(), "$0", "onAddedToExistingTable"));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        try
        {
            createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 30, TimeUnit.SECONDS);
        }
        finally
        {
            Injections.deleteAll();
        }
        assertThat(indexFilesOnDisk(sstable)).containsAll(markers);
        assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isFalse();
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 'a3'")).isInstanceOf(ReadFailureException.class);
    }

    /** Skips the deletion of a completion marker, at the provided method taking the provided file, by the caller. */
    private static Injection keepMarker(String name, InvokePointBuilder delete, String file, String caller)
    {
        return Injections.newCustom(name)
                         .add(delete.atEntry())
                         .add(ActionBuilder.newActionBuilder()
                                           .conditions()
                                           .when(StaleSameNameIndexComponentsTest.class.getName() + ".isMarkerDeletedBy(" + file + ", \"" + caller + "\")")
                                           .actions()
                                           .doAction("return"))
                         .build();
    }

    private static InvokePointBuilder componentDelete()
    {
        return InvokePointBuilder.newInvokePoint()
                                 .onClass("org.apache.cassandra.index.sai.disk.format.IndexDescriptor$IndexComponentsImpl$IndexComponentImpl")
                                 .onMethod("delete");
    }

    private static InvokePointBuilder fileDelete()
    {
        return InvokePointBuilder.newInvokePoint().onClass(File.class).onMethod("delete", "");
    }

    // called by Byteman
    public static boolean isMarkerDeletedBy(File file, String caller)
    {
        return file.name().endsWith("ColumnComplete.db") && calledBy(caller);
    }

    private static Injection failUnregister()
    {
        return failUnregister("onAddedToExistingTable");
    }

    /** Fails to unregister components from the TOC, when called by the provided method. */
    private static Injection failUnregister(String caller)
    {
        return Injections.newCustom("fail_unregister")
                         .add(InvokePointBuilder.newInvokePoint().onClass(SSTable.class).onMethod("unregisterComponents").atEntry())
                         .add(ActionBuilder.newActionBuilder()
                                           .conditions()
                                           .when(StaleSameNameIndexComponentsTest.class.getName() + ".calledBy(\"" + caller + "\")")
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
            createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
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
     * The completion marker of the per-index components written by a flush, a compaction, an initial build and a
     * rebuild, also of an index without values and of a vector index, carries the definition of its index.
     */
    @Test
    public void testCompletionMarkerOfEachWriter() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, t text, a ascii, o int, n text, v vector<float, 2>)");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(o) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX empty_idx ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX vector_idx ON %s(v) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        for (int pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, t, a, o, v) VALUES (?, ?, ?, ?, [" + pk + ", 1])", pk, "t" + pk, "a" + pk % 7, pk);
        flush();
        assertCompletionMarkersCarryDefinitions();

        for (int pk = 100; pk < 200; pk++)
            execute("INSERT INTO %s (pk, t, a, o, v) VALUES (?, ?, ?, ?, [" + pk + ", 1])", pk, "t" + pk, "a" + pk % 7, pk);
        flush();
        compact();
        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1);
        assertCompletionMarkersCarryDefinitions();

        createIndex("CREATE CUSTOM INDEX built_idx ON %s(t) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        assertCompletionMarkersCarryDefinitions();

        rebuildIndexes("built_idx", "empty_idx");
        waitForTableIndexesQueryable();
        assertCompletionMarkersCarryDefinitions();
    }

    /**
     * The files of the stale index (on t, with values) are put back on disk, without the TOC listing them, under the
     * names of the new index of the same name (on n, without values), which is then rebuilt: the rebuilt index has only
     * its completion marker, and doesn't use the stale files.
     */
    @Test
    public void testIndexWithoutValuesRebuiltOverStaleComponents() throws Throwable
    {
        testIndexWithoutValuesRebuiltOverStaleComponents(false);
    }

    @Test
    public void testIndexWithoutValuesRebuiltOverStaleComponentsWithImmutableComponents() throws Throwable
    {
        testIndexWithoutValuesRebuiltOverStaleComponents(true);
    }

    private void testIndexWithoutValuesRebuiltOverStaleComponents(boolean immutable) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(immutable);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        Path saved = saveSSTableFiles();
        Set<Component> stale = indexComponents(sstable, saved);
        dropIndex("DROP INDEX %s." + INDEX);
        awaitDeleted(sstable, stale);
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(n) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        restoreFiles(sstable, saved, stale);
        rebuildIndexes(INDEX);
        waitForTableIndexesQueryable();
        assertBuiltWithoutValues(sstable);
        if (!immutable)
            assertThat(indexFilesOnDisk(sstable)).allMatch(n -> n.endsWith("ColumnComplete.db"));

        simulateNodeRestart();
        assertBuiltWithoutValues(sstable);
    }

    private void assertBuiltWithoutValues(SSTableReader sstable) throws IOException
    {
        String padding = "x".repeat(300);
        assertThat(execute("SELECT pk FROM %s WHERE n = ?", "t3" + padding)).isEmpty();
        IndexComponents.ForRead components = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore())
                                                                      .descriptorFor(sstable)
                                                                      .perIndexComponents(getIndexContext(INDEX));
        assertThat(components.isEmpty()).isTrue();
        assertThat(components.all()).allMatch(IndexComponent::isCompletionMarker);
        assertCompletionMarkersCarryDefinitions();
    }

    /**
     * A flush has written the files of the stale index when it is dropped; CREATE INDEX of the same name lists the
     * table's SSTables while the flush adds its SSTable, and the node restarts before the initial build of the new
     * index: the new index is built from the data of that SSTable when the node starts.
     */
    @Test
    public void testRestartBeforeInitialBuild() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(t) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(o) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        insertRows(0, 100);
        flush();
        insertRows(100, 200);

        injectFlushPause(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete").atExit());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        hookPaused = new CountDownLatch(1);
        hookRelease = new CountDownLatch(1);
        try
        {
            assertThat(flushPaused.await(30, TimeUnit.SECONDS)).isTrue();
            dropIndex("DROP INDEX %s." + INDEX);
            Injections.inject(Injections.newCustom("pause_hook")
                                        .add(InvokePointBuilder.newInvokePoint().onClass(File.class).onMethod("tryListNames").atEntry())
                                        .add(ActionBuilder.newActionBuilder()
                                                          .conditions()
                                                          .when(StaleSameNameIndexComponentsTest.class.getName() + ".listingInHook()")
                                                          .actions()
                                                          .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".pauseHook()"))
                                        .build(),
                              // the node stops before the initial build
                              Injections.newCustom("no_initial_build")
                                        .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild").atEntry())
                                        .add(ActionBuilder.newActionBuilder()
                                                          .actions()
                                                          .doThrow(RuntimeException.class, Expression.quote("Stopped before the initial build (injected)")))
                                        .build());
            CompletableFuture<Void> create = CompletableFuture.runAsync(() -> createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'"));
            assertThat(hookPaused.await(30, TimeUnit.SECONDS)).isTrue();
            flushRelease.countDown();
            flush.get(60, TimeUnit.SECONDS);
            assertThat(cfs.getLiveSSTables()).hasSize(2);
            hookRelease.countDown();
            create.get(60, TimeUnit.SECONDS);
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 30, TimeUnit.SECONDS);
            assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isFalse();
        }
        finally
        {
            flushRelease.countDown();
            hookRelease.countDown();
            Injections.deleteAll();
        }
        simulateNodeRestart();
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(29);
        assertCompletionMarkersCarryDefinitions();
    }

    /**
     * Components whose completion marker is empty, as written before the marker carried the definition of the index,
     * are used after a restart.
     */
    @Test
    public void testEmptyCompletionMarkerUsed() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        insertRows();
        flush();
        SSTableReader sstable = sstable();
        File marker = completionMarker(sstable, INDEX);
        Files.write(marker.toPath(), new byte[0]);
        Map<String, Long> legacy = indexFileSizes(sstable);

        simulateNodeRestart();
        assertBuiltFromData();
        assertThat(indexFileSizes(sstable)).isEqualTo(legacy);
    }

    /** The completion marker of the per-index components of each index on each live SSTable has its definition. */
    private void assertCompletionMarkersCarryDefinitions() throws IOException
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            for (IndexMetadata index : cfs.metadata().indexes)
                assertThat(Files.readAllBytes(completionMarker(sstable, index.name).toPath()))
                .as("completion marker of %s on %s", index.name, sstable.descriptor)
                .isEqualTo(IndexDefinitionDigest.markerContent(index));
        }
    }

    private File completionMarker(SSTableReader sstable, String index)
    {
        return StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore())
                                        .descriptorFor(sstable)
                                        .perIndexComponents(getIndexContext(index))
                                        .get(IndexComponentType.COLUMN_COMPLETION_MARKER)
                                        .file();
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
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(" + staleColumn + ") USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX other_idx ON %s(" + otherColumn + ") USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        insertRows();
        flush();
    }

    private void insertRows() throws Throwable
    {
        insertRows(0, 100);
    }

    private void insertRows(int from, int to) throws Throwable
    {
        String padding = "x".repeat(300);
        for (int pk = from; pk < to; pk++)
            execute("INSERT INTO %s (pk, t, a, o) VALUES (?, ?, ?, ?)", pk, "t" + pk + padding, "a" + pk % 7, pk);
    }

    /**
     * Creates a new table without indexes, and imports the sstable saved from another table into it, with the components
     * of its indexes.
     */
    private void importInto(Path saved) throws Throwable
    {
        importInto(saved, null);
    }

    /**
     * Same, with the provided index created on the new table before the import.
     */
    private void importInto(Path saved, @Nullable String index) throws Throwable
    {
        // index names are unique in a keyspace
        dropIndex("DROP INDEX %s." + INDEX);
        dropIndex("DROP INDEX %s.other_idx");
        createTable(SCHEMA);
        disableCompaction();
        if (index != null)
        {
            createIndex(index);
            waitForTableIndexesQueryable();
        }
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
        String prefix = sstable.descriptor.filenamePart() + '-';
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
        String prefix = sstable.descriptor.filenamePart() + '-';
        return Arrays.stream(saved.toFile().list())
                               .filter(n -> n.startsWith(prefix) && n.contains('+' + INDEX + '+'))
                               .map(n -> new Component(Component.Type.CUSTOM, n.substring(prefix.length())))
                               .collect(Collectors.toSet());
    }

    private Path saveSSTableFiles() throws Exception
    {
        SSTableReader sstable = sstable();
        Path saved = Files.createTempDirectory("stale-components");
        for (File file : sstable.descriptor.directory.tryList())
            if (file.isFile() && file.name().startsWith(sstable.descriptor.filenamePart() + '-'))
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
        assertThat(sstable.descriptor.version.getVersion()).isEqualTo("bb");
        return sstable;
    }

    private void createNgramIndex()
    {
        createIndex(NGRAM_INDEX);
    }
}
