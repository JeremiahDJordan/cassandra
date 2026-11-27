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
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.base.Predicates;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
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
import org.apache.cassandra.index.sai.disk.format.IndexDefinitionDigest;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mockito;

import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * An index created on an existing table is built from the data, never from components of an index of the same name
 * already on disk. The stale components are those of an index "stale_idx" on column {@code t}; the new index of that
 * name is on column {@code a}, where 14 of the 100 rows have {@code a = 'a3'}.
 */
public class StaleSameNameIndexComponentsTest extends SAITester
{
    private static final String SCHEMA = "CREATE TABLE %s (pk int PRIMARY KEY, t text, a ascii, o int, n text)";
    private static final String INDEX = "stale_idx";
    private static final String NGRAM_INDEX = "CREATE INDEX text_index ON %s(text_value) USING 'sai' WITH OPTIONS = " +
                                              "{'index_analyzer': '{\"tokenizer\":{\"name\":\"ngram\", " +
                                              "\"args\":{\"minGramSize\":\"1\", \"maxGramSize\":\"1\"}}}'}";

    private static final AtomicInteger writerPauses = new AtomicInteger();

    private static volatile boolean writerArmed;
    private static volatile String pausedIndex = INDEX;
    private static volatile boolean buildArmed;

    private static volatile CountDownLatch buildDone;

    // the builds whose first ownership of files is held (see holdFirstOwner)
    private static final ThreadLocal<Boolean> holdsFirstOwner = ThreadLocal.withInitial(() -> false);
    private static volatile CountDownLatch firstOwnersHeld;
    private static volatile CountDownLatch firstOwnersRelease;

    private static final AtomicInteger linkFailures = new AtomicInteger();

    // the SSTable whose compaction is held once it has written all of it, and taken it out of the live set
    private static volatile ColumnFamilyStore writtenOutTable;
    private static volatile SSTableReader writtenOut;
    private static volatile boolean holdWrittenOut;
    private static volatile boolean holdingWrittenOut;

    // the armed holds, which pauseIndexWriter and pauseInitialBuild take
    private static volatile Injections.Hold armedWriterHold;
    private static volatile Injections.Hold armedInitialBuildHold;

    private static volatile boolean failAbortArmed;
    // the hold of the rebuild whose abort fails (see failsAbort)
    private static volatile Injections.Hold abortedRebuildHold;
    private static final AtomicInteger abortFailures = new AtomicInteger();
    private static final AtomicInteger tocFailures = new AtomicInteger();
    private static volatile Thread waiter;
    // the SSTable whose build waiterRecording(Descriptor) records
    private static volatile Descriptor waitedSSTable;
    private static final List<String> awaitedNames = new CopyOnWriteArrayList<>();

    private boolean defaultImmutableSetting;

    // called by Byteman, in a method of SSTableIndexWriter
    public static void pauseIndexWriter(Object writer) throws Exception
    {
        if (!writerArmed)
            return;
        Field field = SSTableIndexWriter.class.getDeclaredField("indexContext");
        field.setAccessible(true);
        if (!pausedIndex.equals(((IndexContext) field.get(writer)).getIndexName()))
            return;
        writerArmed = false;
        armedWriterHold.arrive();
    }

    // called by Byteman, in StorageAttachedIndex#startInitialBuild
    public static void pauseInitialBuild() throws InterruptedException
    {
        if (!buildArmed)
            return;
        buildArmed = false;
        armedInitialBuildHold.arrive();
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
        Injections.Hold.releaseAll();
        failAbortArmed = false;
        abortedRebuildHold = null;
        abortFailures.set(0);
        tocFailures.set(0);
        waiter = null;
        waitedSSTable = null;
        awaitedNames.clear();
        pausedIndex = INDEX;
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
     * SSTables carrying the components of an index of the same definition (in another table) are imported into a table
     * without indexes, then the index is created: it uses them, without building the SSTables again.
     */
    @Test
    public void testImportedComponentsOfSameDefinition() throws Throwable
    {
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows();
        flush();
        importInto(saveSSTableFiles());
        SSTableReader sstable = sstable();
        Map<String, Object> imported = indexFileKeys(sstable);
        assertThat(imported).isNotEmpty();

        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertThat(indexFileKeys(sstable)).isEqualTo(imported);
        assertBuiltFromData();
    }

    /**
     * A flush has written the files of the index when it is dropped and created again with the same definition: the new
     * index uses them, and builds only the SSTable flushed before, whose files the drop deleted; it returns every row.
     * When the flush hasn't written the files of the index yet, it writes none, and the new index builds both SSTables.
     */
    @Test
    public void testIdenticalIndexCreatedAcrossFlush() throws Throwable
    {
        testIdenticalIndexCreatedAcrossFlush(true);
        // index names are unique in the keyspace
        schemaChange("DROP TABLE " + KEYSPACE + '.' + currentTable());
        testIdenticalIndexCreatedAcrossFlush(false);
    }

    private void testIdenticalIndexCreatedAcrossFlush(boolean indexWritersCompleted) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows(0, 100);
        flush();
        insertRows(100, 200);

        InvokePointBuilder complete = InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete");
        Injections.Hold flushHold = injectFlushPause(indexWritersCompleted ? complete.atExit() : complete.atEntry());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        Injections.Counter built;
        try
        {
            flushHold.awaitHeld();
            dropIndex("DROP INDEX %s." + INDEX);
            built = countBuiltSSTables();
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            flushHold.release();
            flush.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            Injections.Hold.releaseAll();
        }
        waitForTableIndexesQueryable();
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(29);
        assertThat(built.get()).isEqualTo(indexWritersCompleted ? 1 : 2);
        simulateNodeRestart();
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(29);
        assertThat(built.get()).isEqualTo(indexWritersCompleted ? 1 : 2);
        Injections.deleteAll();
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
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
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
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            armedWriterHold.awaitHeld();
            assertThat(indexFilesOnDisk(sstable)).isNotEmpty();
            dropIndex("DROP INDEX %s." + INDEX);
            buildArmed = true;
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            // the initial build of the new index runs once the held one is over
            armedWriterHold.release();
            assertThat(buildDone.await(60, TimeUnit.SECONDS)).isTrue();
            assertThat(indexFilesOnDisk(sstable)).noneMatch(n -> n.endsWith("ColumnComplete.db"));
            armedInitialBuildHold.awaitHeld();
            // the failed build of the dropped index leaves the new one building
            assertThat(getCurrentColumnFamilyStore().indexManager.isIndexBuilding(INDEX)).isTrue();
            assertThat(SystemKeyspace.isIndexBuilt(KEYSPACE, INDEX)).isFalse();
            armedInitialBuildHold.release();
        }
        finally
        {
            Injections.Hold.releaseAll();
            writerArmed = false;
            buildArmed = false;
            Injections.deleteAll();
        }
        waitForTableIndexesQueryable();
        assertBuildingState(getCurrentColumnFamilyStore(), false);
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
     * change): the TOC still lists them, and their files are there, when an index of the same name is created. CREATE
     * INDEX unlists and deletes them before the initial build.
     */
    @Test
    public void testComponentsOfAnInterruptedDrop() throws Throwable
    {
        testComponentsOfAnInterruptedDrop(false);
    }

    @Test
    public void testComponentsOfAnInterruptedDropWithImmutableComponents() throws Throwable
    {
        testComponentsOfAnInterruptedDrop(true);
    }

    private void testComponentsOfAnInterruptedDrop(boolean immutable) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(immutable);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        Set<Component> components = indexComponents(sstable);
        Path saved = saveSSTableFiles();
        dropIndex("DROP INDEX %s." + INDEX);
        awaitDeleted(sstable, components);
        // as if the drop hadn't removed them
        restoreFiles(sstable, saved, components);
        sstable.registerComponents(components, getCurrentColumnFamilyStore().getTracker());
        assertThat(TOCComponent.loadTOC(sstable.descriptor, false)).containsAll(components);
        assertThat(indexFilesOnDisk(sstable)).isNotEmpty();

        Injections.Hold buildHold = Injections.newHold("build")
                                              .add(InvokePointBuilder.newInvokePoint()
                                                                     .onClass(StorageAttachedIndex.class)
                                                                     .onMethod("startInitialBuild")
                                                                     .atEntry())
                                              .build();
        Injections.inject(buildHold);
        try
        {
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            buildHold.awaitHeld();
            assertThat(TOCComponent.loadTOC(sstable.descriptor, false)).doesNotContainAnyElementsOf(components);
            assertThat(indexFilesOnDisk(sstable)).isEmpty();
            buildHold.release();
        }
        finally
        {
            Injections.Hold.releaseAll();
        }
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
        Injections.Hold buildHold = Injections.newHold("build")
                                              .add(InvokePointBuilder.newInvokePoint()
                                                                     .onClass(StorageAttachedIndexWriter.class)
                                                                     .onMethod("complete")
                                                                     .atEntry())
                                              .build();
        Injections.inject(buildHold);
        try
        {
            CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(INDEX));
            buildHold.awaitHeld();
            dropIndex("DROP INDEX %s." + INDEX);
            buildHold.release();
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
            Injections.Hold.releaseAll();
            Injections.deleteAll();
        }

        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
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
        importInto(saveSSTableFiles(), "CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        SSTableReader sstable = sstable();
        Set<Component> listed = assertTocListsOneBuild(sstable, immutable);
        assertThat(totalDiskSpaceUsed()).isEqualTo(sstable.bytesOnDisk());

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
        Set<Component> listed = TOCComponent.loadTOC(sstable.descriptor, false)
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
                missing = new Component(SSTableFormat.Components.Types.CUSTOM, component.name.replace('+' + INDEX + '+', "+missing_idx+"));
            }
        }
        assertThat(missing).isNotNull();
        Tracker tracker = getCurrentColumnFamilyStore().getTracker();
        sstable.registerComponents(Collections.singleton(missing), tracker);
        try
        {
            createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForTableIndexesQueryable();
            assertBuiltFromData();
        }
        finally
        {
            sstable.unregisterComponents(Collections.singleton(missing), tracker);
        }
    }

    /**
     * The stale files are deleted on CREATE INDEX also while an entire-sstable stream holds the sstable, so the build
     * of the new index writes files of the same names anew rather than append to them.
     */
    @Test
    public void testComponentsDeletedWhileStreamed() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
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
     * A numeric stale index (on o) and a literal new one (on a): CREATE INDEX itself unlists and deletes the stale
     * components of every type, before the initial build of the new index starts.
     */
    @Test
    public void testHookDiscardsStaleComponentsOfEveryType() throws Throwable
    {
        createTableWithStaleIndex("o", "t");
        importInto(saveSSTableFiles());
        SSTableReader sstable = sstable();
        assertThat(indexFilesOnDisk(sstable)).anyMatch(n -> n.endsWith("KDTree.db"));
        assertThat(indexComponents(sstable)).isNotEmpty();

        Injections.Hold buildHold = Injections.newHold("build")
                                              .add(InvokePointBuilder.newInvokePoint()
                                                                     .onClass(StorageAttachedIndex.class)
                                                                     .onMethod("startInitialBuild")
                                                                     .atEntry())
                                              .build();
        Injections.inject(buildHold);
        createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        buildHold.awaitHeld();
        assertThat(indexFilesOnDisk(sstable)).isEmpty();
        assertThat(indexComponents(sstable)).isEmpty();
        assertThat(TOCComponent.loadTOC(sstable.descriptor, false)).noneMatch(c -> c.name.contains('+' + INDEX + '+'));
        buildHold.release();
        waitForTableIndexesQueryable();
        assertBuiltFromData();
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
     * An SSTable written by DSE 6.8 (format bb), whose TOC doesn't list its SAI files, carries the files of an index
     * "text_index" on whole values: the ngram index of the name is built from the data, also while a stream holds the
     * SSTable.
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
        createIndex("CREATE INDEX int_index ON %s(int_value) USING 'sai'");
        createIndex("CREATE INDEX text_index ON %s(text_value) USING 'sai'");
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
     * The only SAI index is dropped while a flush is in flight; the index is created again, then an aborted compaction
     * adds the SSTable again: the index keeps its components.
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

        Injections.Hold flushHold = injectFlushPause(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(StorageAttachedIndexWriter.class)
                                                                       .onMethod("complete")
                                                                       .atEntry());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        try
        {
            flushHold.awaitHeld();
            dropIndex("DROP INDEX %s." + INDEX);
            assertThat(StorageAttachedIndexGroup.getIndexGroup(cfs)).isNull();
            flushHold.release();
            flush.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            Injections.Hold.releaseAll();
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
        Injections.inject(listingFails());
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
        createIndex("CREATE INDEX " + INDEX + " ON %s(t) USING 'sai'");
        if (otherIndex)
            createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows(0, 100);
        // a live SSTable, so that CREATE INDEX lists files
        flush();
        insertRows(100, 200);

        Injections.Hold flushHold = injectFlushPause(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(StorageAttachedIndexWriter.class)
                                                                       .onMethod("complete")
                                                                       .atExit());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        try
        {
            flushHold.awaitHeld();
            dropIndex("DROP INDEX %s." + INDEX);
            String condition = StaleSameNameIndexComponentsTest.class.getName() + ".listingInHook()";
            Injections.Hold hookHold = Injections.newHold("hook")
                                                 .add(InvokePointBuilder.newInvokePoint()
                                                                        .onClass(File.class)
                                                                        .onMethod("tryListNames")
                                                                        .atEntry())
                                                 .withLastActionBuilder(action -> action.conditions().when(condition))
                                                 .build();
            Injections.inject(hookHold);
            CompletableFuture<Void> create = CompletableFuture.runAsync(() -> createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'"));
            hookHold.awaitHeld();
            flushHold.release();
            flush.get(60, TimeUnit.SECONDS);
            assertThat(cfs.getLiveSSTables()).hasSize(2);
            hookHold.release();
            create.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            Injections.Hold.releaseAll();
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
        createIndex("CREATE INDEX " + INDEX + " ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows();

        Injections.Hold flushHold = injectFlushPause(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(Tracker.class)
                                                                       .onMethod("notifyAdding")
                                                                       .atExit());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        try
        {
            flushHold.awaitHeld();
            dropIndex("DROP INDEX %s." + INDEX);
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            flushHold.release();
            flush.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            Injections.Hold.releaseAll();
            Injections.deleteAll();
        }
        waitForTableIndexesQueryable();
        assertBuiltFromData();
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
     * Same, with the class of the index of the name failing to link: the release of the reader deletes the SAI files.
     */
    @Test
    public void testReaderHeldAcrossDropAndCreateOfLegacyIndexWhoseClassFailsToLink() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        SSTableIndex held = getIndexContext(INDEX).getView().getIndexes().iterator().next();
        assertThat(held.reference()).isTrue();
        linkFailures.set(0);
        Injections.inject(Injections.newCustom("fail_link")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(FBUtilities.class).onMethod("classForNameWithoutInitialization").atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(StaleSameNameIndexComponentsTest.class.getName() + ".failsLink()")
                                                      .actions()
                                                      .doThrow(UnsupportedClassVersionError.class, Expression.quote("Fails to link (injected)")))
                                    .build());
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
        assertThat(linkFailures.get()).isPositive();
        assertThat(indexFilesOnDisk(sstable)).isEmpty();
    }

    // called by Byteman
    public static boolean failsLink()
    {
        if (!calledBy("isStorageAttachedIndexClass"))
            return false;
        linkFailures.incrementAndGet();
        return true;
    }

    /**
     * A compaction of two SSTables has completed its index writers, which wrote the completion marker of the stale
     * index, when the index is dropped and created again on a, its initial build held until the compaction has added
     * its output: the output has the marker of the dropped index, and the new index builds it.
     */
    @Test
    public void testCompactionOutputWithCompleteMarkerAcrossDropAndCreate() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE INDEX " + INDEX + " ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows(0, 50);
        flush();
        insertRows(50, 100);
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        byte[] droppedMarker = IndexDefinitionDigest.markerContent(cfs.metadata().indexes.get(INDEX).orElseThrow());

        int preemptiveOpenInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        Injections.Hold compactionHold = Injections.newHold("compaction")
                                                   .add(InvokePointBuilder.newInvokePoint()
                                                                          .onClass(StorageAttachedIndexWriter.class)
                                                                          .onMethod("complete")
                                                                          .atExit())
                                                   .build();
        Injections.inject(compactionHold);
        CompletableFuture<Void> compaction = CompletableFuture.runAsync(cfs::forceMajorCompaction);
        try
        {
            compactionHold.awaitHeld();
            Injections.Hold initialBuildHold = Injections.newHold("initial_build")
                                                         .add(InvokePointBuilder.newInvokePoint()
                                                                                .onClass(StorageAttachedIndex.class)
                                                                                .onMethod("startInitialBuild")
                                                                                .atEntry())
                                                         .build();
            Injections.inject(initialBuildHold);
            dropIndex("DROP INDEX %s." + INDEX);
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            initialBuildHold.awaitHeld();
            compactionHold.release();
            compaction.get(60, TimeUnit.SECONDS);

            SSTableReader output = sstable();
            String marker = indexFilesOnDisk(output).stream().filter(n -> n.endsWith("ColumnComplete.db")).findFirst().orElseThrow();
            assertThat(Files.readAllBytes(output.descriptor.directory.resolve(marker).toPath())).isEqualTo(droppedMarker);
            initialBuildHold.release();
        }
        finally
        {
            Injections.Hold.releaseAll();
            Injections.deleteAll();
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(preemptiveOpenInterval);
        }
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    /**
     * The completion marker of the index on the SSTable is larger than any valid one (sparse, larger than an array can
     * be): a restart builds the SSTable again rather than read it whole.
     */
    @Test
    public void testOversizedCompletionMarkerOnRestart() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex("a", "o");
        File marker = completionMarker(sstable(), INDEX);
        makeOversized(marker);
        Injections.Counter builds = countBuilds();
        simulateNodeRestart();
        assertBuiltFromData();
        assertThat(builds.get()).isPositive();
        assertThat(marker.length()).isEqualTo(IndexDefinitionDigest.markerContent(getCurrentColumnFamilyStore().metadata().indexes.get(INDEX).orElseThrow()).length);
    }

    /**
     * Same, with the SSTable imported into a table without indexes, then the index of that definition created: CREATE
     * discards the components, whose marker holds the digest of that definition followed by more bytes, before the
     * initial build, and the index is built.
     */
    @Test
    public void testOversizedCompletionMarkerOnCreate() throws Throwable
    {
        testOversizedCompletionMarkerOnCreate(3L << 30);
    }

    /**
     * Same, with a marker of 1 MiB.
     */
    @Test
    public void testPaddedCompletionMarkerOnCreate() throws Throwable
    {
        testOversizedCompletionMarkerOnCreate(1 << 20);
    }

    private void testOversizedCompletionMarkerOnCreate(long size) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex("a", "o");
        importInto(saveSSTableFiles());
        SSTableReader sstable = sstable();
        File marker = sstable.descriptor.directory.resolve(indexFilesOnDisk(sstable).stream()
                                                                                    .filter(n -> n.endsWith("ColumnComplete.db"))
                                                                                    .findFirst()
                                                                                    .orElseThrow());
        makeOversized(marker, size);
        Map<String, Object> imported = indexFileKeys(sstable);
        assertThat(imported).isNotEmpty();
        Injections.Hold buildHold = Injections.newHold("build")
                                              .add(InvokePointBuilder.newInvokePoint()
                                                                     .onClass(StorageAttachedIndex.class)
                                                                     .onMethod("startInitialBuild")
                                                                     .atEntry())
                                              .build();
        Injections.inject(buildHold);
        try
        {
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            buildHold.awaitHeld();
            assertThat(marker.exists()).isFalse();
            assertThat(TOCComponent.loadTOC(sstable.descriptor, false)).noneMatch(c -> c.name.contains('+' + INDEX + '+'));
            buildHold.release();
        }
        finally
        {
            Injections.Hold.releaseAll();
        }
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        assertThat(indexFileKeys(sstable)).isNotEqualTo(imported);
        assertThat(Files.readAllBytes(marker.toPath())).isEqualTo(IndexDefinitionDigest.markerContent(getCurrentColumnFamilyStore().metadata().indexes.get(INDEX).orElseThrow()));
    }

    private static void makeOversized(File marker) throws IOException
    {
        makeOversized(marker, 3L << 30);
    }

    /** Extends the marker to the provided size, the added bytes zeros. */
    private static void makeOversized(File marker, long size) throws IOException
    {
        try (RandomAccessFile file = new RandomAccessFile(marker.toJavaIOFile(), "rw"))
        {
            file.setLength(size);
        }
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
            armedWriterHold.awaitHeld();
            buildArmed = true;
            dropIndex("DROP INDEX %s." + INDEX);
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            armedWriterHold.release();
            armedInitialBuildHold.awaitHeld();
            compaction.get(60, TimeUnit.SECONDS);
            armedInitialBuildHold.release();
        }
        finally
        {
            Injections.Hold.releaseAll();
            writerArmed = false;
            buildArmed = false;
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
        createIndex("CREATE INDEX " + INDEX + " ON %s(n) USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows(0, 100);
        flush();
        insertRows(100, 200);
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        return cfs;
    }

    /** Pauses an SSTable index writer of {@link #pausedIndex} before it writes its metadata, once armed. */
    private static Injection indexWriterPause()
    {
        return indexWriterPause(writerPoint("writeSegmentsMetadata").atEntry());
    }

    /** Pauses an SSTable index writer of {@link #pausedIndex} at the provided point, once armed. */
    private static Injection indexWriterPause(InvokePointBuilder at)
    {
        armedWriterHold = Injections.newHold("writer").build();
        return Injections.newCustom("pause_index_writer_" + writerPauses.incrementAndGet())
                         .add(at)
                         .add(ActionBuilder.newActionBuilder()
                                           .actions()
                                           .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".pauseIndexWriter($0)"))
                         .build();
    }

    /** Pauses the initial build at the provided point, once armed. */
    private static Injection initialBuildPause(InvokePointBuilder at)
    {
        armedInitialBuildHold = Injections.newHold("initial_build").build();
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
        Injections.Hold flushHold = injectFlushPause(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(StorageAttachedIndexWriter.class)
                                                                       .onMethod("complete")
                                                                       .atEntry());
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        try
        {
            flushHold.awaitHeld();
            action.run();
            flushHold.release();
            flush.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            Injections.Hold.releaseAll();
            Injections.deleteAll();
        }
    }

    private static Injections.Hold injectFlushPause(InvokePointBuilder at) throws Throwable
    {
        Injections.Hold flush = Injections.newHold("flush")
                                          .add(at)
                                          .build();
        Injections.inject(flush);
        return flush;
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
     * The SSTable with the components of an interrupted drop, with an empty completion marker, isn't live when the index
     * is created: a compaction that opens its result early has written all of it out. The compaction is then stopped,
     * which restores the SSTable: the stale components were discarded all the same.
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
        for (Component component : components)
            if (component.name.endsWith("ColumnComplete.db"))
                Files.write(first.descriptor.fileFor(component).toPath(), new byte[0]);
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
        createIndex("CREATE INDEX " + INDEX + " ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows();

        InvokePointBuilder complete = InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete");
        Injections.Hold flushHold = injectFlushPause(indexWritersCompleted ? complete.atExit() : complete.atEntry());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        try
        {
            flushHold.awaitHeld();
            dropIndex("DROP INDEX %s." + INDEX);
            if (create)
                createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            assertThat(flush).isNotDone();
            flushHold.release();
            flush.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            Injections.Hold.releaseAll();
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
     * deleted all the same, before the initial build; the build of the index, under the names the TOC still lists, is
     * listed after it and after a restart.
     */
    @Test
    public void testHookFailsToUnregister() throws Throwable
    {
        testHookFailsToUnregister(false);
    }

    /**
     * Same, with immutable components: the build of the index takes the first generation, whose files are gone, under
     * the names the TOC still lists.
     */
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
        SSTableReader sstable = sstable();
        Set<Component> stale = indexComponents(sstable);
        assertThat(stale).isNotEmpty();
        Injections.inject(failUnregister());
        Injections.Hold buildHold = Injections.newHold("build")
                                              .add(InvokePointBuilder.newInvokePoint()
                                                                     .onClass(StorageAttachedIndex.class)
                                                                     .onMethod("startInitialBuild")
                                                                     .atEntry())
                                              .build();
        Injections.inject(buildHold);
        try
        {
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            buildHold.awaitHeld();
            assertThat(tocFailures.get()).isPositive();
            assertThat(indexComponents(sstable)).isEqualTo(stale);
            assertThat(indexFilesOnDisk(sstable)).isEmpty();
            buildHold.release();
            waitForTableIndexesQueryable();
        }
        finally
        {
            Injections.Hold.releaseAll();
            Injections.deleteAll();
        }
        assertBuiltFromData();
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
        Set<Component> listedMarkers = indexComponents(sstable).stream().filter(c -> c.name.endsWith("ColumnComplete.db")).collect(Collectors.toSet());
        assertThat(listedMarkers).hasSize(1);
        Injections.inject(failUnregister(),
                          keepMarker("keep_marker_component", componentDelete(), "$0.file()", "onAddedToExistingTable"),
                          keepMarker("keep_marker_file", fileDelete(), "$0", "onAddedToExistingTable"));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        try
        {
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 30, TimeUnit.SECONDS);
        }
        finally
        {
            Injections.deleteAll();
        }
        assertThat(tocFailures.get()).isPositive();
        assertThat(TOCComponent.loadTOC(sstable.descriptor, false)).containsAll(listedMarkers);
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

    // called by Byteman
    public static void recordTocFailure()
    {
        tocFailures.incrementAndGet();
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
                                           .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".recordTocFailure()")
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
     * The completion marker of the per-index components written by a flush, a compaction, an initial build and a
     * rebuild, also of an index without values and of a vector index, carries the definition of its index.
     */
    @Test
    public void testCompletionMarkerOfEachWriter() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, t text, a ascii, o int, n text, v vector<float, 2>)");
        disableCompaction();
        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        createIndex("CREATE INDEX empty_idx ON %s(n) USING 'sai'");
        createIndex("CREATE INDEX vector_idx ON %s(v) USING 'sai'");
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

        createIndex("CREATE INDEX built_idx ON %s(t) USING 'sai'");
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
        createIndex("CREATE INDEX " + INDEX + " ON %s(n) USING 'sai'");
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
        createIndex("CREATE INDEX " + INDEX + " ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(o) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows(0, 100);
        flush();
        insertRows(100, 200);

        Injections.Hold flushHold = injectFlushPause(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(StorageAttachedIndexWriter.class)
                                                                       .onMethod("complete")
                                                                       .atExit());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        try
        {
            flushHold.awaitHeld();
            dropIndex("DROP INDEX %s." + INDEX);
            String condition = StaleSameNameIndexComponentsTest.class.getName() + ".listingInHook()";
            Injections.Hold hookHold = Injections.newHold("hook")
                                                 .add(InvokePointBuilder.newInvokePoint()
                                                                        .onClass(File.class)
                                                                        .onMethod("tryListNames")
                                                                        .atEntry())
                                                 .withLastActionBuilder(action -> action.conditions().when(condition))
                                                 .build();
            Injections.inject(hookHold);
            Injections.inject(
                              // the node stops before the initial build
                              Injections.newCustom("no_initial_build")
                                        .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild").atEntry())
                                        .add(ActionBuilder.newActionBuilder()
                                                          .actions()
                                                          .doThrow(RuntimeException.class, Expression.quote("Stopped before the initial build (injected)")))
                                        .build());
            CompletableFuture<Void> create = CompletableFuture.runAsync(() -> createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'"));
            hookHold.awaitHeld();
            flushHold.release();
            flush.get(60, TimeUnit.SECONDS);
            assertThat(cfs.getLiveSSTables()).hasSize(2);
            hookHold.release();
            create.get(60, TimeUnit.SECONDS);
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 30, TimeUnit.SECONDS);
            assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isFalse();
        }
        finally
        {
            Injections.Hold.releaseAll();
            Injections.deleteAll();
        }
        // as on a restart, where the index is created anew and marked building
        cfs.indexManager.markIndexesBuilding(Collections.singleton(cfs.indexManager.getIndexByName(INDEX)), true, true);
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
        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
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

    /**
     * A query holds the reader of the index across its drop and the creation of an index of the same definition, which
     * uses its files: the TOC lists them again, and a restart doesn't build the SSTable again.
     */
    @Test
    public void testReaderHeldAcrossDropAndCreateOfIdenticalIndex() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex("a", "o");
        SSTableReader sstable = sstable();
        Set<Component> listed = indexComponents(sstable);
        SSTableIndex held = getIndexContext(INDEX).getView().getIndexes().iterator().next();
        assertThat(held.reference()).isTrue();
        Injections.Counter builds = countBuilds();
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
        assertThat(builds.get()).isZero();
        assertBuiltFromData();
        assertThat(indexComponents(sstable)).isEqualTo(listed);

        simulateNodeRestart();
        assertBuiltFromData();
        assertThat(builds.get()).isZero();
    }

    /**
     * Same, with an empty completion marker, as written before it held the definition of its index: CREATE deletes the
     * files, which the descriptor that the other index keeps for the SSTable still has, and the new index is built.
     */
    @Test
    public void testReaderHeldAcrossDropAndCreateOfIdenticalIndexWithEmptyCompletionMarker() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex("a", "o");
        SSTableReader sstable = sstable();
        File marker = completionMarker(sstable, INDEX);
        Files.write(marker.toPath(), new byte[0]);
        SSTableIndex held = getIndexContext(INDEX).getView().getIndexes().iterator().next();
        assertThat(held.reference()).isTrue();
        try
        {
            dropIndex("DROP INDEX %s." + INDEX);
            createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForTableIndexesQueryable();
            assertBuiltFromData();
        }
        finally
        {
            held.release();
        }
        assertBuiltFromData();
        assertThat(Files.readAllBytes(marker.toPath()))
        .isEqualTo(IndexDefinitionDigest.markerContent(getCurrentColumnFamilyStore().metadata().indexes.get(INDEX).orElseThrow()));
    }

    /**
     * Same, on a column without values, so that the components of the dropped index on the SSTable are only a
     * completion marker: CREATE deletes it, and the new index writes its own, which the TOC lists.
     */
    @Test
    public void testReaderHeldAcrossDropAndCreateOfIdenticalIndexWithoutValuesWithEmptyCompletionMarker() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex("n", "o");
        SSTableReader sstable = sstable();
        File marker = completionMarker(sstable, INDEX);
        Files.write(marker.toPath(), new byte[0]);
        SSTableIndex held = getIndexContext(INDEX).getView().getIndexes().iterator().next();
        assertThat(held.reference()).isTrue();
        try
        {
            dropIndex("DROP INDEX %s." + INDEX);
            createIndex("CREATE INDEX " + INDEX + " ON %s(n) USING 'sai'");
            waitForTableIndexesQueryable();
        }
        finally
        {
            held.release();
        }
        assertThat(Files.readAllBytes(marker.toPath()))
        .isEqualTo(IndexDefinitionDigest.markerContent(getCurrentColumnFamilyStore().metadata().indexes.get(INDEX).orElseThrow()));
        assertThat(indexComponents(sstable)).anyMatch(c -> c.name.endsWith("ColumnComplete.db"));
    }

    /**
     * A rebuild of the stale index is held before it writes its metadata, with some of the files of the SSTable written;
     * the index is dropped and created again on a, and the build of the new index reaches the SSTable before the
     * rebuild goes on: the new index builds the SSTable once the rebuild of the dropped index has stopped.
     */
    @Test
    public void testNewIndexBuiltWhileRebuildOfDroppedIndexWritesMetadata() throws Throwable
    {
        testNewIndexBuiltAcrossRebuildOfDroppedIndex(writerPoint("writeSegmentsMetadata").atEntry(), "a", true, false);
    }

    /**
     * Same, with the rebuild held before it deletes the files of the previous build of the SSTable: the new index stays
     * building while the rebuild of the dropped index fails.
     */
    @Test
    public void testNewIndexBuiltWhileRebuildOfDroppedIndexPreparesItsFiles() throws Throwable
    {
        testNewIndexBuiltAcrossRebuildOfDroppedIndex(builderPoint("listedComponentsOfName").atEntry(), "a", true, true);
    }

    /**
     * Same, with the rebuild held after its writer of the stale index completed, before the rebuild stops for the drop,
     * and the new index of the definition of the dropped one: it can't take the files the rebuild wrote, which the
     * rebuild deletes.
     */
    @Test
    public void testIdenticalIndexBuiltWhileRebuildOfDroppedIndexCompletes() throws Throwable
    {
        testNewIndexBuiltAcrossRebuildOfDroppedIndex(writerPoint("complete").atExit(), "t", true, false);
    }

    /**
     * Same, with the rebuild held before it takes the build lock and the files of the SSTable: the new index is built
     * first, and the rebuild of the dropped index then stops without touching them.
     */
    @Test
    public void testNewIndexBuiltBeforeRebuildOfDroppedIndexTakesItsFiles() throws Throwable
    {
        testNewIndexBuiltAcrossRebuildOfDroppedIndex(builderPoint("lockForBuild").atEntry(), "a", false, false);
    }

    private static InvokePointBuilder writerPoint(String method)
    {
        return InvokePointBuilder.newInvokePoint().onClass(SSTableIndexWriter.class).onMethod(method);
    }

    private static InvokePointBuilder builderPoint(String method)
    {
        return InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod(method);
    }

    /**
     * A full rebuild of the stale index is held at the provided point; the index is dropped and created again on the
     * provided column, and the rebuild is released once the build of the new index waits for it, the new index not
     * queryable, or, when it doesn't wait, once the new index is built: the rebuild stops, the new index is built, and a
     * restart builds nothing.
     *
     * @param newBuildWaits whether the build of the new index waits for the rebuild
     * @param holdNewBuild whether to check the state of the new index while its build is held after the rebuild stopped
     */
    private void testNewIndexBuiltAcrossRebuildOfDroppedIndex(InvokePointBuilder heldAt,
                                                              String newColumn,
                                                              boolean newBuildWaits,
                                                              boolean holdNewBuild) throws Throwable
    {
        // with immutable components, the rebuild and the new build write files of different generations
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        Injections.Hold rebuildHold = Injections.newHold("rebuild")
                                                .add(heldAt)
                                                .build();
        Injections.inject(rebuildHold);
        CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(INDEX));
        rebuildHold.awaitHeld();
        // a full rebuild holds the build lock of the SSTable exclusively: the new build may wait for it there
        Injections.inject(waiterRecording(sstable.descriptor));
        Injections.Hold newBuildHold = Injections.newHold("new_build")
                                                 .add(builderPoint("shouldWritePerSSTableFiles").atEntry())
                                                 .build();
        if (holdNewBuild)
            Injections.inject(newBuildHold);
        dropIndex("DROP INDEX %s." + INDEX);
        createIndexAsync("CREATE INDEX " + INDEX + " ON %s(" + newColumn + ") USING 'sai'");
        // when the new build doesn't wait, it completes while the rebuild is held, and the rebuild leaves its files
        Map<String, Object> newFiles = null;
        if (newBuildWaits)
        {
            waitForAssert(() -> assertThat(waiter != null && waiter.getState() == Thread.State.TIMED_WAITING).isTrue(), 60, TimeUnit.SECONDS);
            assertThat(cfs.indexManager.getIndexByName(INDEX)).isNotNull();
            assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isFalse();
        }
        else
        {
            waitForTableIndexesQueryable();
            newFiles = indexFileKeys(sstable);
        }
        rebuildHold.release();
        assertThatThrownBy(() -> rebuild.get(60, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                                                                  .hasMessageContaining("are dropped, will stop index build");
        if (newFiles != null)
            assertThat(indexFileKeys(sstable)).isEqualTo(newFiles);
        if (holdNewBuild)
        {
            newBuildHold.awaitHeld();
            assertBuildingState(cfs, true);
            newBuildHold.release();
        }
        Injections.deleteAll();

        waitForTableIndexesQueryable();
        assertBuildingState(cfs, false);
        assertBuiltFromData(newColumn);
        Set<Component> listed = assertTocListsOneBuild(sstable, false);

        Injections.Counter builds = countBuilds();
        simulateNodeRestart();
        assertBuiltFromData(newColumn);
        assertThat(builds.get()).isZero();
        assertThat(assertTocListsOneBuild(sstable, false)).isEqualTo(listed);
    }

    /** The index {@link #INDEX} is writable, building or built, and not left for a full rebuild. */
    private static void assertBuildingState(ColumnFamilyStore cfs, boolean building)
    {
        assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isEqualTo(building);
        assertThat(SystemKeyspace.isIndexBuilt(KEYSPACE, INDEX)).isEqualTo(!building);
        assertThat(cfs.indexManager.needsFullRebuild(INDEX)).isFalse();
        assertThat(cfs.indexManager.isIndexWritable(cfs.indexManager.getIndexByName(INDEX))).isTrue();
    }

    private void assertBuiltFromData(String column)
    {
        if (column.equals("a"))
            assertBuiltFromData();
        else
            assertThat(execute("SELECT pk FROM %s WHERE t = ?", "t3" + "x".repeat(300))).hasSize(1);
    }

    /**
     * The writer of a rebuild of the stale index fails to delete its files when the rebuild stops for the drop: the
     * build of the new index, which waits for it, goes on.
     */
    @Test
    public void testNewIndexBuiltAfterAbortOfDroppedIndexWriterFails() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        Injections.Hold rebuildHold = Injections.newHold("rebuild")
                                                .add(writerPoint("writeSegmentsMetadata").atEntry())
                                                .build();
        Injections.inject(rebuildHold);
        abortedRebuildHold = rebuildHold;
        Injections.inject(Injections.newCustom("fail_abort")
                                    .add(InvokePointBuilder.newInvokePoint()
                                                           .onClass("org.apache.cassandra.index.sai.disk.format.IndexDescriptor$IndexComponentsImpl")
                                                           .onMethod("forceDeleteAllComponents")
                                                           .atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(StaleSameNameIndexComponentsTest.class.getName() + ".failsAbort()")
                                                      .actions()
                                                      .doThrow(RuntimeException.class, Expression.quote("Failed to delete (injected)")))
                                    .build());
        CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(INDEX));
        rebuildHold.awaitHeld();
        dropIndex("DROP INDEX %s." + INDEX);
        Injections.inject(waiterRecording());
        createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        // the build of the new index waits for the rebuild, which owns the files of the dropped index
        waitForAssert(() -> assertThat(waiter != null && waiter.getState() == Thread.State.TIMED_WAITING).isTrue(), 60, TimeUnit.SECONDS);
        assertThat(getCurrentColumnFamilyStore().indexManager.isIndexQueryable(INDEX)).isFalse();
        failAbortArmed = true;
        rebuildHold.release();
        assertThatThrownBy(() -> rebuild.get(60, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
        failAbortArmed = false;
        assertThat(abortFailures.get()).isPositive();
        Injections.deleteAll();
        waitForAssert(() -> assertThat(getCurrentColumnFamilyStore().indexManager.isIndexQueryable(INDEX)).isTrue(), 60, TimeUnit.SECONDS);
        assertBuiltFromData();
    }

    // called by Byteman
    public static boolean failsAbort()
    {
        Injections.Hold rebuild = abortedRebuildHold;
        boolean fails = failAbortArmed && rebuild != null && Thread.currentThread() == rebuild.heldThread() && calledBy("abort");
        if (fails)
            abortFailures.incrementAndGet();
        return fails;
    }

    /**
     * A writer of a new SSTable that got rows is closed without being finished or aborted: its observers are aborted,
     * and its SAI index writer releases the files of the dropped index.
     */
    @Test
    public void testWriterClosedWithoutFinish() throws Throwable
    {
        createTableWithStaleIndex("a", "o");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = sstable();
        Injections.Counter aborts = Injections.newCounter("aborts")
                                              .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("abort"))
                                              .build();
        Injections.inject(aborts);
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.SCRUB, cfs.metadata);
             ISSTableScanner scanner = sstable.getScanner())
        {
            SSTableWriter writer = CompactionManager.createWriter(cfs, cfs.getDirectories().getDirectoryForNewSSTables(), 100, 0, null, false, sstable, txn);
            writer.append(scanner.next());
            dropIndex("DROP INDEX %s." + INDEX);
            assertThat(PerIndexFilesOwner.hasOwnerOfDroppedIndex(writer.descriptor, INDEX)).isTrue();
            writer.close();
            assertThat(PerIndexFilesOwner.hasOwnerOfDroppedIndex(writer.descriptor, INDEX)).isFalse();
        }
        assertThat(aborts.get()).isPositive();
    }

    /**
     * A writer of a new SSTable has completed the components of the stale index, which is then dropped and created again,
     * and the write is aborted: the SAI index writer, which owns the files no more and whose index is dropped, leaves
     * them, which could be those of the new index, to the transaction of the SSTable.
     */
    @Test
    public void testCompletedWriterOfDroppedIndexAbortedKeepsItsFiles() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex("a", "o");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = sstable();
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.SCRUB, cfs.metadata);
             ISSTableScanner scanner = sstable.getScanner())
        {
            SSTableWriter writer = CompactionManager.createWriter(cfs, cfs.getDirectories().getDirectoryForNewSSTables(), 100, 0, null, false, sstable, txn);
            while (scanner.hasNext())
                writer.append(scanner.next());
            writer.prepareToCommit();
            Map<String, Long> written = indexFileSizes(writer.descriptor);
            assertThat(written.keySet()).anyMatch(n -> n.endsWith("ColumnComplete.db"));
            dropIndex("DROP INDEX %s." + INDEX);
            createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            writer.abort();
            assertThat(indexFileSizes(writer.descriptor)).isEqualTo(written);
        }
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    /**
     * Same, with the writer of the new SSTable aborted, or closed, after the observer of another index that fails to
     * abort: the SAI index writers are aborted all the same, and release the files of the SSTable.
     */
    @Test
    public void testWriterAbortedAfterObserverFailsToAbort() throws Throwable
    {
        testWriterEndedAfterObserverFailsToAbort(List.of(failingObserver(new RuntimeException("Failed to abort (injected)"))),
                                                 writer -> assertThatThrownBy(writer::abort).hasMessage("Failed to abort (injected)"));
    }

    @Test
    public void testWriterAbortedWithCauseAfterObserverFailsToAbort() throws Throwable
    {
        RuntimeException cause = new RuntimeException("Failed to write (injected)");
        testWriterEndedAfterObserverFailsToAbort(List.of(failingObserver(new RuntimeException("Failed to abort (injected)"))),
                                                 writer -> assertThat(writer.abort(cause)).isSameAs(cause)
                                                                                          .hasSuppressedException(new RuntimeException("Failed to abort (injected)")));
    }

    /**
     * Same, with the observer of the other index throwing the cause of the abort again.
     */
    @Test
    public void testWriterAbortedWithCauseAfterObserverRethrowsIt() throws Throwable
    {
        RuntimeException cause = new RuntimeException("Failed to write (injected)");
        SSTableFlushObserver rethrowing = Mockito.mock(SSTableFlushObserver.class);
        Mockito.doAnswer(invocation -> {
            throw invocation.<Throwable>getArgument(0);
        }).when(rethrowing).abort(Mockito.any());
        testWriterEndedAfterObserverFailsToAbort(List.of(rethrowing), writer -> assertThat(writer.abort(cause)).isSameAs(cause));
    }

    /**
     * Same, with the observers of two other indexes throwing one error.
     */
    @Test
    public void testWriterAbortedAfterObserversFailWithOneError() throws Throwable
    {
        RuntimeException error = new RuntimeException("Failed to abort (injected)");
        testWriterEndedAfterObserverFailsToAbort(List.of(failingObserver(error), failingObserver(error)),
                                                 writer -> assertThatThrownBy(writer::abort).isSameAs(error));
    }

    /**
     * Same, with the writer closed after the observer of another index that fails to abort.
     */
    @Test
    public void testWriterClosedAfterObserverFailsToAbort() throws Throwable
    {
        testWriterEndedAfterObserverFailsToAbort(List.of(failingObserver(new RuntimeException("Failed to abort (injected)"))),
                                                 writer -> assertThatThrownBy(writer::close).hasMessage("Failed to abort (injected)"));
    }

    private static SSTableFlushObserver failingObserver(RuntimeException error)
    {
        SSTableFlushObserver observer = Mockito.mock(SSTableFlushObserver.class);
        Mockito.doThrow(error).when(observer).abort(Mockito.any());
        return observer;
    }

    /**
     * Writes a partition with the provided observers ahead of those of the table's indexes, drops {@link #INDEX} and
     * ends the write as provided: no owner of the files of the dropped index is left.
     */
    private void testWriterEndedAfterObserverFailsToAbort(List<SSTableFlushObserver> failing, Consumer<SSTableWriter> end) throws Throwable
    {
        createTableWithStaleIndex("a", "o");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = sstable();
        List<Index.Group> groups = new ArrayList<>();
        for (SSTableFlushObserver observer : failing)
        {
            Index.Group group = Mockito.mock(Index.Group.class);
            Mockito.when(group.getFlushObserver(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong())).thenReturn(observer);
            groups.add(group);
        }
        groups.addAll(cfs.indexManager.listIndexGroups());
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.SCRUB, cfs.metadata);
             ISSTableScanner scanner = sstable.getScanner())
        {
            Descriptor descriptor = cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables());
            SSTableWriter writer = descriptor.getFormat().getWriterFactory().builder(descriptor)
                                             .setKeyCount(100)
                                             .setTableMetadataRef(cfs.metadataRef())
                                             .setMetadataCollector(new MetadataCollector(cfs.metadata().comparator))
                                             .setSerializationHeader(sstable.header)
                                             .addDefaultComponents(cfs.indexManager.listIndexGroups())
                                             .setSecondaryIndexGroups(groups)
                                             .build(txn, cfs);
            writer.append(scanner.next());
            dropIndex("DROP INDEX %s." + INDEX);
            assertThat(PerIndexFilesOwner.hasOwnerOfDroppedIndex(writer.descriptor, INDEX)).isTrue();
            end.accept(writer);
            assertThat(PerIndexFilesOwner.hasOwnerOfDroppedIndex(writer.descriptor, INDEX)).isFalse();
        }
    }

    /**
     * A rebuild of the stale index fails on a row once its writer has taken the files of the SSTable; the index is dropped
     * and created again on a: the new index is built.
     */
    @Test
    public void testNewIndexBuiltAfterRebuildOfDroppedIndexFailsOnRow() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        Injections.inject(Injections.newCustom("fail_row")
                                    .add(writerPoint("addRow").atExit())
                                    .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, Expression.quote("Failed row (injected)")))
                                    .build());
        assertThatThrownBy(() -> rebuildIndexes(INDEX)).hasStackTraceContaining("Failed row (injected)");
        Injections.deleteAll();

        dropIndex("DROP INDEX %s." + INDEX);
        assertThat(PerIndexFilesOwner.hasOwnerOfDroppedIndex(sstable.descriptor, INDEX)).isFalse();
        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        // the failed rebuild deleted the per-SSTable components, so the other index waits for a rebuild
        waitForIndexQueryable(INDEX);
        assertBuiltFromData();
    }

    /**
     * A rebuild of the stale index, on an int column, is held once it has collected the components it built and before
     * it lists them; the index is dropped and created again on a (ascii), and the rebuild then lists the components of
     * the dropped index: the new index waits for the rebuild, and its build replaces them, so that the TOC lists only
     * the components of the new index.
     */
    @Test
    public void testNewIndexBuiltWhileRebuildOfDroppedIndexListsItsComponents() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex("o", "t");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = sstable();
        // in completeSSTable, once it has collected the components it built and before it registers them
        String condition = StaleSameNameIndexComponentsTest.class.getName() + ".calledBy(\"completeSSTable\")";
        Injections.Hold registerHold = Injections.newHold("register")
                                                 .add(InvokePointBuilder.newInvokePoint()
                                                                        .onClass(SSTable.class)
                                                                        .onMethod("registerComponents")
                                                                        .atEntry())
                                                 .withLastActionBuilder(action -> action.conditions().when(condition))
                                                 .build();
        Injections.inject(registerHold);
        CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(INDEX));
        registerHold.awaitHeld();
        // the rebuild unlisted the components it replaces before it built the SSTable
        assertThat(indexComponents(sstable)).isEmpty();
        Injections.inject(waiterRecording());
        dropIndex("DROP INDEX %s." + INDEX);
        createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        // the build of the new index waits for the rebuild (for the build lock of the SSTable, which the rebuild holds
        // exclusively), or is done
        waitForAssert(() -> assertThat((waiter != null && waiter.getState() == Thread.State.TIMED_WAITING) || cfs.indexManager.isIndexQueryable(INDEX)).isTrue(),
                      60, TimeUnit.SECONDS);
        registerHold.release();
        rebuild.handle((result, error) -> null).get(60, TimeUnit.SECONDS);
        Injections.deleteAll();

        waitForTableIndexesQueryable();
        assertBuiltFromData();
        assertTocListsOneBuild(sstable, false);
    }

    /**
     * A rebuild of the stale index waits for an owner of the files of a dropped index of its name, which is released; the
     * rebuild is interrupted as the wait returns: it fails, and changes no file of the index or the TOC.
     */
    @Test
    public void testRebuildInterruptedAsItStopsWaiting() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        Map<String, Object> files = indexFileKeys(sstable);
        Set<Component> listed = TOCComponent.loadTOC(sstable.descriptor, false);
        AtomicBoolean dropped = new AtomicBoolean();
        PerIndexFilesOwner owner = PerIndexFilesOwner.tryOwn(sstable.descriptor, INDEX, dropped::get);
        dropped.set(true);
        Injections.inject(waiterRecording(),
                          Injections.newCustom("interrupt_on_wakeup")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(PerIndexFilesOwner.class).onMethod("awaitOwnersOfDroppedIndex").atExit())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when("$! == true")
                                                      .actions()
                                                      .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".interruptOnWakeup()"))
                                    .build());
        CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(INDEX));
        Throwable failure;
        try
        {
            waitForAssert(() -> assertThat(waiter != null && waiter.getState() == Thread.State.TIMED_WAITING).isTrue(), 60, TimeUnit.SECONDS);
            owner.close();
            failure = catchThrowable(() -> rebuild.get(60, TimeUnit.SECONDS));
        }
        finally
        {
            owner.close();
        }
        Injections.deleteAll();
        assertThat(failure).isInstanceOf(ExecutionException.class).hasRootCauseInstanceOf(CompactionInterruptedException.class);
        assertThat(indexFileKeys(sstable)).isEqualTo(files);
        assertThat(TOCComponent.loadTOC(sstable.descriptor, false)).isEqualTo(listed);
    }

    // called by Byteman, as the wait for the owners of the files of a dropped index returns true
    public static void interruptOnWakeup()
    {
        Thread.currentThread().interrupt();
    }

    /**
     * A partial build of the SSTable for the stale index and other_idx waits for an owner of the files of a dropped index
     * of each name; the first one it waits for is released, and that index is dropped while the build waits for the
     * second: a wait for the owners of a dropped index of the first name doesn't wait for the build, which holds no files
     * while it waits, and the build then ends.
     */
    @Test
    public void testBuildHoldsNoFilesWhileItWaits() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = sstable();
        Injections.inject(Injections.newCustom("record_awaited")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(PerIndexFilesOwner.class).onMethod("awaitOwnersOfDroppedIndex").atEntry())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".recordAwaited($1, $2)"))
                                    .build());

        Map<String, PerIndexFilesOwner> owners = new HashMap<>();
        for (String name : List.of(INDEX, "other_idx"))
        {
            AtomicBoolean dropped = new AtomicBoolean();
            owners.put(name, PerIndexFilesOwner.tryOwn(sstable.descriptor, name, dropped::get));
            dropped.set(true);
        }
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        CompletableFuture<Throwable> build = CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, "other_idx", INDEX));
        try
        {
            waitForAssert(() -> assertThat(awaitedNames).isNotEmpty(), 60, TimeUnit.SECONDS);
            String first = awaitedNames.get(0);
            String second = first.equals(INDEX) ? "other_idx" : INDEX;
            owners.get(first).close();
            waitForAssert(() -> assertThat(awaitedNames).contains(second), 60, TimeUnit.SECONDS);
            dropIndex("DROP INDEX %s." + first);
            assertThat(PerIndexFilesOwner.awaitOwnersOfDroppedIndex(sstable.descriptor, first, 10, TimeUnit.SECONDS)).isTrue();
        }
        finally
        {
            owners.values().forEach(PerIndexFilesOwner::close);
        }
        assertThat(build.get(60, TimeUnit.SECONDS)).isNull();
        assertThat(PerIndexFilesOwner.hasOwnerOfDroppedIndex(sstable.descriptor, INDEX)).isFalse();
        assertThat(PerIndexFilesOwner.hasOwnerOfDroppedIndex(sstable.descriptor, "other_idx")).isFalse();
    }

    // called by Byteman
    public static void recordAwaited(Object descriptor, String indexName)
    {
        if (PerIndexFilesOwner.hasOwnerOfDroppedIndex((Descriptor) descriptor, indexName))
            awaitedNames.add(indexName);
    }

    /**
     * The initial build of the new index is interrupted while it waits for a rebuild of the dropped index: it fails,
     * rather than leave the SSTable out of the index. A rebuild of the new index is then asked for while the rebuild of
     * the dropped index holds the SSTable: it fails, and the new index stays non-queryable and not built.
     */
    @Test
    public void testRebuildOfNewIndexWhileRebuildOfDroppedIndexHoldsSSTable() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Injections.Hold rebuildHold = Injections.newHold("rebuild")
                                                .add(writerPoint("writeSegmentsMetadata").atEntry())
                                                .build();
        Injections.inject(rebuildHold);
        CompletableFuture<Void> rebuild = CompletableFuture.runAsync(() -> rebuildIndexes(INDEX));
        rebuildHold.awaitHeld();
        Injections.inject(waiterRecording());
        dropIndex("DROP INDEX %s." + INDEX);
        createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForAssert(() -> assertThat(waiter).isNotNull(), 60, TimeUnit.SECONDS);
        waiter.interrupt();
        waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 60, TimeUnit.SECONDS);
        assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isFalse();

        assertThatThrownBy(() -> rebuildIndexes(INDEX)).hasStackTraceContaining("another index build holds some of its SSTables");
        assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isFalse();
        assertThat(SystemKeyspace.isIndexBuilt(KEYSPACE, INDEX)).isFalse();
        rebuildHold.release();
        assertThatThrownBy(() -> rebuild.get(60, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
        Injections.deleteAll();
        assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isFalse();

        rebuildIndexes(INDEX);
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    /**
     * A writer of the stale index owns its files on the SSTable when the index is dropped and created again on a: the
     * initial build of the new index, which no build of the SSTable holds back, waits for it, and builds the SSTable
     * once the writer releases them.
     */
    @Test
    public void testInitialBuildWaitsForWriterOfDroppedIndex() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = sstable();
        StorageAttachedIndex dropped = (StorageAttachedIndex) cfs.indexManager.getIndexByName(INDEX);
        PerIndexFilesOwner owner = PerIndexFilesOwner.tryOwn(sstable.descriptor, INDEX, dropped.isDropped());
        assertThat(owner).isNotNull();
        Injections.inject(waiterRecording());
        try
        {
            dropIndex("DROP INDEX %s." + INDEX);
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForAssert(() -> assertThat(waiter).isNotNull(), 60, TimeUnit.SECONDS);
            waitForAssert(() -> assertThat(waiter.getState()).isEqualTo(Thread.State.TIMED_WAITING), 60, TimeUnit.SECONDS);
            assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isTrue();
        }
        finally
        {
            owner.close();
            Injections.deleteAll();
        }
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    /**
     * Same, with the waiting initial build of the new index interrupted: it fails, and a rebuild once the writer is done
     * builds the index.
     */
    @Test
    public void testInitialBuildInterruptedWhileWaitingForWriterOfDroppedIndex() throws Throwable
    {
        testInitialBuildEndedWhileWaitingForWriterOfDroppedIndex(false);
    }

    /**
     * Same, with the waiting initial build stopped.
     */
    @Test
    public void testInitialBuildStoppedWhileWaitingForWriterOfDroppedIndex() throws Throwable
    {
        testInitialBuildEndedWhileWaitingForWriterOfDroppedIndex(true);
    }

    private void testInitialBuildEndedWhileWaitingForWriterOfDroppedIndex(boolean stop) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = sstable();
        StorageAttachedIndex dropped = (StorageAttachedIndex) cfs.indexManager.getIndexByName(INDEX);
        PerIndexFilesOwner owner = PerIndexFilesOwner.tryOwn(sstable.descriptor, INDEX, dropped.isDropped());
        assertThat(owner).isNotNull();
        Injections.inject(waiterRecording());
        try
        {
            dropIndex("DROP INDEX %s." + INDEX);
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForAssert(() -> assertThat(waiter).isNotNull(), 60, TimeUnit.SECONDS);
            waitForAssert(() -> assertThat(waiter.getState()).isEqualTo(Thread.State.TIMED_WAITING), 60, TimeUnit.SECONDS);
            if (stop)
                CompactionManager.instance.stopCompaction(OperationType.INDEX_BUILD.name());
            else
                waiter.interrupt();
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 60, TimeUnit.SECONDS);
            assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isFalse();
        }
        finally
        {
            owner.close();
            Injections.deleteAll();
        }
        rebuildIndexes(INDEX);
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    /**
     * Two partial builds of the SSTable, of indexes x_idx and y_idx, are each held once they own the files of their
     * index, and both indexes are dropped: both builds end, and an index created again with either name is built.
     */
    @Test
    public void testPartialBuildsOfIndexesDroppedOnceTheyOwnTheirFiles() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE INDEX x_idx ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX y_idx ON %s(o) USING 'sai'");
        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        insertRows();
        flush();
        SSTableReader sstable = sstable();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());

        firstOwnersHeld = new CountDownLatch(2);
        firstOwnersRelease = new CountDownLatch(1);
        Injections.inject(Injections.newCustom("hold_first_owner")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(PerIndexFilesOwner.class).onMethod("tryOwn").atExit())
                                    .add(ActionBuilder.newActionBuilder()
                                                      .actions()
                                                      .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".holdFirstOwner($1)"))
                                    .build());
        CompletableFuture<Throwable> buildX = CompletableFuture.supplyAsync(() -> partialBuildHoldingFirstOwner(group, sstable, "x_idx"));
        CompletableFuture<Throwable> buildY = CompletableFuture.supplyAsync(() -> partialBuildHoldingFirstOwner(group, sstable, "y_idx"));
        try
        {
            assertThat(firstOwnersHeld.await(60, TimeUnit.SECONDS)).isTrue();
            dropIndex("DROP INDEX %s.x_idx");
            dropIndex("DROP INDEX %s.y_idx");
        }
        finally
        {
            firstOwnersRelease.countDown();
        }
        buildX.get(60, TimeUnit.SECONDS);
        buildY.get(60, TimeUnit.SECONDS);
        Injections.deleteAll();

        createIndex("CREATE INDEX x_idx ON %s(t) USING 'sai'");
        waitForTableIndexesQueryable();
        assertThat(execute("SELECT pk FROM %s WHERE t = ?", "t3" + "x".repeat(300))).hasSize(1);
    }

    /**
     * The files of an SSTable whose same-name files lock is held are owned once the lock is released, those of an SSTable
     * mapped to another lock right away, and an index created on an existing table waits for every lock.
     */
    @Test
    public void testSameNameFilesLocks() throws Throwable
    {
        createTable(SCHEMA);
        insertRows();
        flush();
        Descriptor descriptor = sstable().descriptor;
        Descriptor locked = descriptor;
        for (int id = 1; id < 1000 && StorageAttachedIndex.sameNameFilesLock(locked) == StorageAttachedIndex.sameNameFilesLock(descriptor); id++)
            locked = new Descriptor(descriptor.version, descriptor.directory, descriptor.ksname, descriptor.cfname, new SequenceBasedSSTableId(id));
        Object lock = StorageAttachedIndex.sameNameFilesLock(locked);
        assertThat(lock).isNotSameAs(StorageAttachedIndex.sameNameFilesLock(descriptor));

        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Void> holder = CompletableFuture.runAsync(() -> {
            synchronized (lock)
            {
                held.countDown();
                Uninterruptibles.awaitUninterruptibly(release);
            }
        });
        try
        {
            assertThat(held.await(60, TimeUnit.SECONDS)).isTrue();
            CompletableFuture.supplyAsync(() -> PerIndexFilesOwner.tryOwn(descriptor, INDEX, () -> false)).get(10, TimeUnit.SECONDS).close();
            Descriptor lockedDescriptor = locked;
            CompletableFuture<PerIndexFilesOwner> owner = CompletableFuture.supplyAsync(() -> PerIndexFilesOwner.tryOwn(lockedDescriptor, INDEX, () -> false));
            CompletableFuture<Void> created = CompletableFuture.runAsync(() -> createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'"));
            assertThatThrownBy(() -> created.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
            assertThat(owner).isNotDone();
            release.countDown();
            owner.get(60, TimeUnit.SECONDS).close();
            created.get(60, TimeUnit.SECONDS);
        }
        finally
        {
            release.countDown();
        }
        holder.get(60, TimeUnit.SECONDS);
        waitForTableIndexesQueryable();
        assertBuiltFromData();
    }

    private static Throwable partialBuildHoldingFirstOwner(StorageAttachedIndexGroup group, SSTableReader sstable, String index)
    {
        holdsFirstOwner.set(true);
        try
        {
            return partialBuild(group, sstable, index);
        }
        finally
        {
            holdsFirstOwner.remove();
        }
    }

    // called by Byteman, when PerIndexFilesOwner#tryOwn returns: holds the first ownership a marked build takes, without
    // the lock of the owners
    public static void holdFirstOwner(Descriptor descriptor) throws InterruptedException
    {
        if (!holdsFirstOwner.get())
            return;
        holdsFirstOwner.set(false);
        Object lock = StorageAttachedIndex.sameNameFilesLock(descriptor);
        synchronized (lock)
        {
            firstOwnersHeld.countDown();
            while (firstOwnersRelease.getCount() > 0)
                lock.wait(100);
        }
    }

    /**
     * A build of the stale index owns its files on the SSTable when the index is dropped and created again; a partial
     * build of the other index then finds the per-SSTable components corrupt, and rewrites them, rebuilding every index
     * of the SSTable but the new one, whose own build waits for the owner: the build succeeds, and a third index stays
     * queryable. The new index is built once the owner is done.
     */
    @Test
    public void testCorruptComponentsRewrittenWhileBuildOfDroppedIndexOwnsItsFiles() throws Throwable
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

            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
            assertThat(CompletableFuture.supplyAsync(() -> partialBuild(group, sstable, "other_idx")).get(60, TimeUnit.SECONDS)).isNull();
            assertThat(indexFilesOnDisk(sstable)).isEmpty();
            assertThat(cfs.indexManager.isIndexQueryable(cfs.indexManager.getIndexByName("sibling_idx"))).isTrue();
            assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isTrue();
            assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(14);
            assertThat(execute("SELECT pk FROM %s WHERE o = 3")).hasSize(1);
        }
        finally
        {
            SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD.setBoolean(validate);
            owner.close();
        }
        waitForTableIndexesQueryable();
        assertThat(execute("SELECT pk FROM %s WHERE t = ?", "t3" + "x".repeat(300))).hasSize(1);
    }

    /**
     * The same, when the rewrite fails as it creates its writer: the indexes it rebuilt, which used the deleted
     * per-SSTable components, become non-queryable, but not the new index it left out, whose own build goes on once the
     * owner is done.
     */
    @Test
    public void testFailedRewriteOfCorruptComponentsWhileBuildOfDroppedIndexOwnsItsFiles() throws Throwable
    {
        testFailedRewriteOfCorruptComponentsWhileBuildOfDroppedIndexOwnsItsFiles(false, writerConstruction());
    }

    /**
     * The same, when the rewrite fails once it has deleted the per-SSTable components: the new index was already left
     * out, and isn't made non-queryable either.
     */
    @Test
    public void testFailedRewriteAsItDeletesCorruptComponentsWhileBuildOfDroppedIndexOwnsItsFiles() throws Throwable
    {
        testFailedRewriteOfCorruptComponentsWhileBuildOfDroppedIndexOwnsItsFiles(false, builderPoint("prepareForRebuild").atExit());
    }

    /**
     * The same with immutable components: the rewrite deletes nothing, but the indexes it rebuilt used the corrupt
     * components, and become non-queryable all the same.
     */
    @Test
    public void testFailedRewriteOfCorruptImmutableComponentsWhileBuildOfDroppedIndexOwnsItsFiles() throws Throwable
    {
        testFailedRewriteOfCorruptComponentsWhileBuildOfDroppedIndexOwnsItsFiles(true, writerConstruction());
    }

    private static InvokePointBuilder writerConstruction()
    {
        return InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("<init>").atEntry();
    }

    private void testFailedRewriteOfCorruptComponentsWhileBuildOfDroppedIndexOwnsItsFiles(boolean immutable, InvokePointBuilder failurePoint) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(immutable);
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
                                        .add(failurePoint)
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

    /**
     * Records the first thread that waits for the owners of the files of a dropped index, or for the build lock of the
     * SSTable, which a full rebuild of the dropped index holds exclusively.
     */
    private static Injection waiterRecording()
    {
        return Injections.newCustom("record_waiter")
                         .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("ownFiles").atEntry())
                         .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("lockForBuild").atEntry())
                         .add(ActionBuilder.newActionBuilder()
                                           .actions()
                                           .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".recordWaiter()"))
                         .build();
    }

    /**
     * Same, for the build of the provided SSTable by the index registered as {@link #INDEX}, not a dropped one of that
     * name.
     */
    private static Injection waiterRecording(Descriptor sstable)
    {
        waitedSSTable = sstable;
        return Injections.newCustom("record_waiter_of_sstable")
                         .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("ownFiles").atEntry())
                         .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("lockForBuild").atEntry())
                         .add(ActionBuilder.newActionBuilder()
                                           .actions()
                                           .doAction(StaleSameNameIndexComponentsTest.class.getName() + ".recordWaiterOf($1, $0.sstables.get($1))"))
                         .build();
    }

    // called by Byteman, with the indexes the builder builds on the SSTable
    public static void recordWaiterOf(SSTableReader sstable, Object indexes)
    {
        if (waiter == null && sstable.descriptor.equals(waitedSSTable)
            && ((Set<?>) indexes).stream().map(StorageAttachedIndex.class::cast).anyMatch(index -> index.getIndexMetadata().name.equals(INDEX) && !index.isDropped().getAsBoolean()))
            waiter = Thread.currentThread();
    }

    // called by Byteman
    public static void recordWaiter()
    {
        if (waiter == null)
            waiter = Thread.currentThread();
    }

    /**
     * The TOC of the SSTable lists a file named after the index with an SAI version this node doesn't know: a rebuild
     * of the index ignores it, and leaves it on disk and in the TOC.
     */
    @Test
    public void testRebuildWithListedFileOfUnknownVersion() throws Throwable
    {
        createTableWithStaleIndex("a", "o");
        SSTableReader sstable = sstable();
        Component unknown = new Component(SSTableFormat.Components.Types.CUSTOM, "SAI+zz+" + INDEX + "+ColumnComplete.db");
        Files.write(sstable.descriptor.fileFor(unknown).toPath(), new byte[0]);
        Tracker tracker = getCurrentColumnFamilyStore().getTracker();
        sstable.registerComponents(Collections.singleton(unknown), tracker);
        try
        {
            rebuildIndexes(INDEX);
            waitForTableIndexesQueryable();
            assertBuiltFromData();
            assertThat(sstable.descriptor.fileFor(unknown).exists()).isTrue();
            assertThat(sstable.components()).contains(unknown);
            assertThat(TOCComponent.loadTOC(sstable.descriptor, false)).contains(unknown);
        }
        finally
        {
            sstable.unregisterComponents(Collections.singleton(unknown), tracker);
            sstable.descriptor.fileFor(unknown).deleteIfExists();
        }
    }

    /**
     * With immutable components, the index is rebuilt, which leaves the files of the previous generation on disk, then
     * dropped and created again with the same definition: the new index is built, only the files of its build are left
     * on disk and listed, and a restart builds nothing.
     */
    @Test
    public void testIdenticalIndexCreatedAfterRebuildWithImmutableComponents() throws Throwable
    {
        testIdenticalIndexCreatedAfterRebuildWithImmutableComponents(false);
    }

    /**
     * Same, with a query holding the reader of the rebuilt generation across DROP and CREATE: the new index uses it
     * without a build, and the files of the previous generation are deleted.
     */
    @Test
    public void testReaderHeldAcrossDropAndCreateOfIdenticalIndexAfterRebuildWithImmutableComponents() throws Throwable
    {
        testIdenticalIndexCreatedAfterRebuildWithImmutableComponents(true);
    }

    private void testIdenticalIndexCreatedAfterRebuildWithImmutableComponents(boolean holdReader) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(true);
        createTableWithStaleIndex("a", "o");
        SSTableReader sstable = sstable();
        rebuildIndexes(INDEX);
        waitForTableIndexesQueryable();
        SSTableIndex held = holdReader ? getIndexContext(INDEX).getView().getIndexes().iterator().next() : null;
        if (held != null)
            assertThat(held.reference()).isTrue();
        Injections.Counter builds = countBuilds();
        try
        {
            dropIndex("DROP INDEX %s." + INDEX);
            createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForTableIndexesQueryable();
        }
        finally
        {
            if (held != null)
                held.release();
        }
        if (holdReader)
            assertThat(builds.get()).isZero();
        else
            assertThat(builds.get()).isPositive();
        assertBuiltFromData();
        Set<Component> listed = assertTocListsOneBuild(sstable, true);
        assertThat(indexFilesOnDisk(sstable)).isEqualTo(listed.stream().map(c -> sstable.descriptor.fileFor(c).name()).collect(Collectors.toSet()));

        builds.reset();
        simulateNodeRestart();
        assertBuiltFromData();
        assertThat(builds.get()).isZero();
        assertThat(assertTocListsOneBuild(sstable, true)).isEqualTo(listed);
    }

    /**
     * The stale index's files on the SSTable are put back, with their completion marker, under the names of the new
     * index of the same name on a, and the node restarts: the initialization task at startup doesn't take them as
     * built, and the index is built from the data.
     */
    @Test
    public void testStartupOverComponentsOfAnotherDefinition() throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTableWithStaleIndex();
        SSTableReader sstable = sstable();
        Path saved = saveSSTableFiles();
        Set<Component> stale = indexComponents(sstable, saved);
        dropIndex("DROP INDEX %s." + INDEX);
        awaitDeleted(sstable, stale);
        createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
        waitForTableIndexesQueryable();
        assertThat(indexComponents(sstable)).isEqualTo(stale);
        restoreFiles(sstable, saved, stale);

        StorageService.instance.setStartingModeUnsafe();
        try
        {
            simulateNodeRestart();
        }
        finally
        {
            StorageService.instance.setNormalModeUnsafe();
        }
        waitForTableIndexesQueryable();
        assertBuiltFromData();
        assertCompletionMarkersCarryDefinitions();
    }

    /**
     * The TOC can't be rewritten, nor the completion marker of the stale components deleted, which is empty, as written
     * before the marker held the definition of its index: the pre-join task doesn't make the index queryable.
     */
    @Test
    public void testPreJoinTaskAfterHookFailsToDeleteEmptyCompletionMarker() throws Throwable
    {
        createIndexOverUndeletableEmptyCompletionMarker(false);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.indexManager.executePreJoinTasksBlocking(false);
        assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isFalse();
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 'a3'")).isInstanceOf(ReadFailureException.class);
    }

    /**
     * Same, with the index created while the node starts: its initialization task doesn't make it queryable.
     */
    @Test
    public void testCreatedAtStartupAfterHookFailsToDeleteEmptyCompletionMarker() throws Throwable
    {
        createIndexOverUndeletableEmptyCompletionMarker(true);
        assertThat(getCurrentColumnFamilyStore().indexManager.isIndexQueryable(INDEX)).isFalse();
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 'a3'")).isInstanceOf(ReadFailureException.class);
    }

    private void createIndexOverUndeletableEmptyCompletionMarker(boolean starting) throws Throwable
    {
        createTableWithStaleIndex();
        importInto(saveSSTableFiles());
        SSTableReader sstable = sstable();
        Set<Component> markers = indexComponents(sstable).stream().filter(c -> c.name.endsWith("ColumnComplete.db")).collect(Collectors.toSet());
        assertThat(markers).hasSize(1);
        for (Component marker : markers)
            Files.write(sstable.descriptor.fileFor(marker).toPath(), new byte[0]);

        Injections.inject(failUnregister(),
                          keepMarker("keep_marker_component", componentDelete(), "$0.file()", "onAddedToExistingTable"),
                          keepMarker("keep_marker_file", fileDelete(), "$0", "onAddedToExistingTable"));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        if (starting)
            StorageService.instance.setStartingModeUnsafe();
        try
        {
            createIndexAsync("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 30, TimeUnit.SECONDS);
        }
        finally
        {
            StorageService.instance.setNormalModeUnsafe();
            Injections.deleteAll();
        }
        assertThat(indexComponents(sstable)).containsAll(markers);
    }

    /**
     * The SSTable has a file named after the index with an SAI version this node doesn't know (e.g. left by a newer
     * version): the index created with that name is built from the data.
     */
    @Test
    public void testFileOfUnknownVersionNamedAfterTheIndex() throws Throwable
    {
        createTableWithStaleIndex();
        dropIndex("DROP INDEX %s." + INDEX);
        SSTableReader sstable = sstable();
        File unknown = new File(sstable.descriptor.directory, sstable.descriptor.filenamePart() + "-SAI+zz+" + INDEX + "+ColumnComplete.db");
        Files.write(unknown.toPath(), new byte[0]);
        try
        {
            createIndex("CREATE INDEX " + INDEX + " ON %s(a) USING 'sai'");
            waitForTableIndexesQueryable();
            assertBuiltFromData();
        }
        finally
        {
            unknown.deleteIfExists();
        }
    }

    /** Counts SSTables built by SAI. */
    private static Injections.Counter countBuiltSSTables() throws Throwable
    {
        Injections.Counter built = Injections.newCounter("built_sstables")
                                             .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("indexSSTable"))
                                             .build();
        Injections.inject(built);
        return built;
    }

    /** Counts the index builds of SSTables from now on. */
    private static Injections.Counter countBuilds() throws Throwable
    {
        Injections.Counter builds = Injections.newCounter("builds")
                                              .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("build"))
                                              .build();
        Injections.inject(builds);
        return builds;
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
        createIndex("CREATE INDEX " + INDEX + " ON %s(" + staleColumn + ") USING 'sai'");
        createIndex("CREATE INDEX other_idx ON %s(" + otherColumn + ") USING 'sai'");
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
        return indexFileSizes(sstable.descriptor);
    }

    private static Map<String, Long> indexFileSizes(Descriptor descriptor)
    {
        String prefix = descriptor.filenamePart() + '-';
        Map<String, Long> sizes = new TreeMap<>();
        for (File file : descriptor.directory.tryList())
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
                               .map(n -> new Component(SSTableFormat.Components.Types.CUSTOM, n.substring(prefix.length())))
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
        assertThat(sstable.descriptor.version.version).isEqualTo("bb");
        return sstable;
    }

    private void createNgramIndex()
    {
        createIndex(NGRAM_INDEX);
    }
}
