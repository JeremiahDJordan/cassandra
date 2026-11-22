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

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.db.streaming.CassandraStreamReceiver;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexDefinitionDigest;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * SSTables streamed (not whole) while an index is dropped and created again with the same name. The node streams half
 * of an SSTable of the table to itself.
 */
public class StaleSameNameIndexComponentsStreamingTest extends SAITester
{
    private static final String SCHEMA = "CREATE TABLE %s (pk int PRIMARY KEY, t text, a ascii, o int)";
    private static final String INDEX = "stale_idx";

    private static volatile boolean hookPauseArmed;
    // the tracker whose addition of streamed SSTables the adding hold takes
    private static volatile Tracker addingTo;

    private boolean defaultImmutableSetting;

    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
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
        hookPauseArmed = false;
        addingTo = null;
    }

    // called by Byteman
    public static boolean listingInHook()
    {
        return hookPauseArmed && calledBy("onAddedToExistingTable");
    }

    // called by Byteman
    public static boolean isStreamAdding(Tracker tracker, OperationType operationType)
    {
        return tracker == addingTo && operationType == OperationType.STREAM;
    }

    private static boolean calledBy(String method)
    {
        for (StackTraceElement element : Thread.currentThread().getStackTrace())
            if (element.getMethodName().equals(method))
                return true;
        return false;
    }

    /**
     * A stream written for the stale index adds its SSTable after the initial build of the new index of the same name
     * selected the SSTables to build, and before it is made queryable: the new index includes the streamed SSTable.
     */
    @Test
    public void testStreamAddedAfterInitialBuildChecks() throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithStaleIndex();
        Injections.Hold streamHold = Injections.newHold("stream")
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(CassandraStreamReceiver.class)
                                                                      .onMethod("finished")
                                                                      .atEntry())
                                               .build();
        Injections.inject(streamHold);
        Future<StreamState> stream = streamHalfToSelf(cfs);
        try
        {
            streamHold.awaitHeld();
            dropIndex("DROP INDEX %s." + INDEX);

            Injections.Hold buildHold = Injections.newHold("build")
                                                  .add(InvokePointBuilder.newInvokePoint()
                                                                         .onClass(StorageAttachedIndex.class)
                                                                         .onMethod("startInitialBuild")
                                                                         .atExit())
                                                  .build();
            Injections.inject(buildHold);
            createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            buildHold.awaitHeld();
            streamHold.release();
            stream.get(60, TimeUnit.SECONDS);
            assertThat(cfs.getLiveSSTables()).hasSize(2);
            buildHold.release();
        }
        finally
        {
            Injections.deleteAll();
            Injections.Hold.releaseAll();
        }
        waitForTableIndexesQueryable();
        assertIndexCoversLiveSSTables(cfs);
    }

    /**
     * A stream written for the stale index adds its SSTable while CREATE INDEX of the same name lists the table's
     * SSTables, before the new index is registered; the stream completes before the initial build: the new index
     * doesn't use the stale files, and its initial build builds the SSTable.
     */
    @Test
    public void testStreamAddedWhileCreateListsSSTables() throws Throwable
    {
        testStreamAddedWhileCreateListsSSTables(false);
    }

    /**
     * Same, with another SAI index on the table, so that the index manager holds the stream until CREATE returns.
     */
    @Test
    public void testStreamAddedWhileCreateListsSSTablesWithOtherIndex() throws Throwable
    {
        testStreamAddedWhileCreateListsSSTables(true);
    }

    private void testStreamAddedWhileCreateListsSSTables(boolean otherIndex) throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithStaleIndex(otherIndex);
        Injections.Hold streamHold = Injections.newHold("stream")
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(CassandraStreamReceiver.class)
                                                                      .onMethod("finished")
                                                                      .atEntry())
                                               .build();
        Injections.inject(streamHold);
        Injections.Hold cleanupHold = Injections.newHold("cleanup")
                                                .add(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(CassandraStreamReceiver.class)
                                                                       .onMethod("cleanup")
                                                                       .atExit())
                                                .build();
        Injections.inject(cleanupHold);
        Injections.Hold hookHold = Injections.newHold("hook")
                                             .add(InvokePointBuilder.newInvokePoint()
                                                                    .onClass(File.class)
                                                                    .onMethod("tryListNames")
                                                                    .atEntry())
                                             .withLastActionBuilder(action -> action.conditions()
                                                                                    .when(StaleSameNameIndexComponentsStreamingTest.class.getName() + ".listingInHook()"))
                                             .build();
        Injections.inject(hookHold);
        Future<StreamState> stream = streamHalfToSelf(cfs);
        try
        {
            streamHold.awaitHeld();
            dropIndex("DROP INDEX %s." + INDEX);
            hookPauseArmed = true;
            CompletableFuture<Void> create = CompletableFuture.runAsync(() -> createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'"));
            hookHold.awaitHeld();
            Injections.Hold buildHold = Injections.newHold("build")
                                                  .add(InvokePointBuilder.newInvokePoint()
                                                                         .onClass(StorageAttachedIndex.class)
                                                                         .onMethod("startInitialBuild")
                                                                         .atEntry())
                                                  .build();
            Injections.inject(buildHold);
            streamHold.release();
            waitForAssert(() -> assertThat(cfs.getLiveSSTables()).hasSize(2), 30, TimeUnit.SECONDS);
            hookPauseArmed = false;
            hookHold.release();
            create.get(60, TimeUnit.SECONDS);
            buildHold.awaitHeld();
            cleanupHold.awaitHeld();
            cleanupHold.release();
            stream.get(60, TimeUnit.SECONDS);
            buildHold.release();
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 60, TimeUnit.SECONDS);
        }
        finally
        {
            Injections.deleteAll();
            Injections.Hold.releaseAll();
        }
        assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isTrue();
        assertIndexCoversLiveSSTables(cfs);
    }

    /**
     * A stream written for the stale index is about to add its SSTable when the table has no index of that name; the
     * new index of the same name is created and built before the SSTable is added: the new index doesn't use the stale
     * files, and builds the SSTable once it is added.
     */
    @Test
    public void testStreamAddingBeforeCreateAddedAfterBuild() throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithStaleIndex();
        Injections.Hold streamHold = Injections.newHold("stream")
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(CassandraStreamReceiver.class)
                                                                      .onMethod("finished")
                                                                      .atEntry())
                                               .build();
        Injections.inject(streamHold);
        Future<StreamState> stream = streamHalfToSelf(cfs);
        try
        {
            streamHold.awaitHeld();
            dropIndex("DROP INDEX %s." + INDEX);
            addingTo = cfs.getTracker();
            Injections.Hold addingHold = Injections.newHold("adding")
                                                   .add(InvokePointBuilder.newInvokePoint()
                                                                          .onClass(Tracker.class)
                                                                          .onMethod("addSSTablesInternal")
                                                                          .atInvoke("setupOnline"))
                                                   .withLastActionBuilder(action -> action.conditions()
                                                                                          .when(StaleSameNameIndexComponentsStreamingTest.class.getName() + ".isStreamAdding($0, $2)"))
                                                   .build();
            Injections.inject(addingHold);
            streamHold.release();
            addingHold.awaitHeld();
            createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            waitForTableIndexesQueryable();
            addingHold.release();
            stream.get(60, TimeUnit.SECONDS);
            assertThat(cfs.getLiveSSTables()).hasSize(2);
        }
        finally
        {
            Injections.deleteAll();
            Injections.Hold.releaseAll();
        }
        assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isTrue();
        assertIndexCoversLiveSSTables(cfs);
    }

    /**
     * The stale index is dropped while a stream writes its SSTable, so that the stream writes no files for it, and the
     * new index of the same name is built before the stream adds the SSTable: the new index includes it, and the TOC of
     * the SSTable lists files of the new index.
     */
    @Test
    public void testStreamWithoutFilesOfDroppedIndexAddedAfterCreate() throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithStaleIndex();
        Injections.Hold writerHold = Injections.newHold("writer")
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(StorageAttachedIndexWriter.class)
                                                                      .onMethod("complete")
                                                                      .atEntry())
                                               .build();
        Injections.inject(writerHold);
        Injections.Hold streamHold = Injections.newHold("stream")
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(CassandraStreamReceiver.class)
                                                                      .onMethod("finished")
                                                                      .atEntry())
                                               .build();
        Injections.inject(streamHold);
        Future<StreamState> stream = streamHalfToSelf(cfs);
        Set<Descriptor> received;
        try
        {
            writerHold.awaitHeld();
            dropIndex("DROP INDEX %s." + INDEX);
            writerHold.release();
            streamHold.awaitHeld();
            received = sstablesNotLive(cfs);
            assertThat(received).hasSize(1);
            for (Descriptor descriptor : received)
            {
                assertThat(descriptor.fileFor(Component.DATA).exists()).isTrue();
                assertThat(descriptor.directory.tryListNames()).noneMatch(n -> n.startsWith(descriptor.filenamePart() + '-') && n.contains('+' + INDEX + '+'));
            }

            createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            waitForTableIndexesQueryable();
            streamHold.release();
            stream.get(60, TimeUnit.SECONDS);
            assertThat(cfs.getLiveSSTables()).hasSize(2);
        }
        finally
        {
            Injections.deleteAll();
            Injections.Hold.releaseAll();
        }
        assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isTrue();
        assertIndexCoversLiveSSTables(cfs);
        for (Descriptor descriptor : received)
        {
            assertThat(SSTable.readTOC(descriptor, false)).filteredOn(c -> c.name.contains('+' + INDEX + '+'))
                                                          .isNotEmpty()
                                                          .allMatch(c -> descriptor.fileFor(c).exists());
        }
    }

    /** The SSTables of the table on disk that aren't live, such as those a stream received and hasn't added yet. */
    private static Set<Descriptor> sstablesNotLive(ColumnFamilyStore cfs)
    {
        Set<Descriptor> live = cfs.getLiveSSTables().stream().map(s -> s.descriptor).collect(Collectors.toSet());
        Set<Descriptor> notLive = new HashSet<>();
        for (File directory : cfs.getDirectories().getCFDirectories())
        {
            for (File file : directory.tryList())
            {
                if (!file.name().endsWith('-' + Component.DATA.name()))
                    continue;
                Descriptor descriptor = Descriptor.fromFilename(file);
                if (!live.contains(descriptor))
                    notLive.add(descriptor);
            }
        }
        return notLive;
    }

    /**
     * The completion marker of the per-index components of a streamed SSTable carries the definition of its index.
     */
    @Test
    public void testCompletionMarkerOfStreamedSSTable() throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithStaleIndex();
        streamHalfToSelf(cfs).get(60, TimeUnit.SECONDS);
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            for (IndexMetadata index : cfs.metadata().indexes)
            {
                File marker = group.descriptorFor(sstable)
                                   .perIndexComponents(getIndexContext(index.name))
                                   .get(IndexComponentType.COLUMN_COMPLETION_MARKER)
                                   .file();
                assertThat(Files.readAllBytes(marker.toPath())).isEqualTo(IndexDefinitionDigest.markerContent(index));
            }
        }
    }

    /** Table with an index {@link #INDEX} on column t, another one on column o, and one SSTable of 100 rows. */
    private ColumnFamilyStore createTableWithStaleIndex() throws Throwable
    {
        return createTableWithStaleIndex(true);
    }

    /** Table with an index {@link #INDEX} on column t, possibly another one on column o, and one SSTable of 100 rows. */
    private ColumnFamilyStore createTableWithStaleIndex(boolean otherIndex) throws Throwable
    {
        IMMUTABLE_SAI_COMPONENTS.setBoolean(false);
        createTable(SCHEMA);
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(t) USING 'StorageAttachedIndex'");
        if (otherIndex)
            createIndex("CREATE CUSTOM INDEX other_idx ON %s(o) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        for (int pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, t, a, o) VALUES (?, ?, ?, ?)", pk, "t" + pk, "a" + pk % 7, pk);
        flush();
        return getCurrentColumnFamilyStore();
    }

    /** Streams the partitions of the first half of the token range of the table's SSTable to this node. */
    private static Future<StreamState> streamHalfToSelf(ColumnFamilyStore cfs) throws Exception
    {
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        List<Token> tokens = new ArrayList<>();
        try (KeyIterator keys = KeyIterator.forSSTable(sstable))
        {
            while (keys.hasNext())
                tokens.add(keys.next().getToken());
        }
        Collections.sort(tokens);
        List<Range<Token>> ranges = Range.normalize(Collections.singletonList(new Range<>(sstable.getPartitioner().getMinimumToken(),
                                                                                          tokens.get(tokens.size() / 2))));
        CassandraOutgoingFile stream = new CassandraOutgoingFile(StreamOperation.OTHER,
                                                                 sstable.ref(),
                                                                 sstable.getPositionsForRanges(ranges),
                                                                 ranges,
                                                                 sstable.estimatedKeysForRanges(ranges));
        return new StreamPlan(StreamOperation.OTHER).transferStreams(FBUtilities.getBroadcastAddressAndPort(), Collections.singletonList(stream))
                                                    .execute();
    }

    /** The index on a has the terms of a ("a0" to "a6"), never those of the stale index on t ("t0" to "t99"), if any. */
    private void assertNoStaleTerms() throws Exception
    {
        for (SSTableIndex index : getIndexContext(INDEX).getView().getIndexes())
        {
            if (index.minTerm() == null)
                continue;
            assertThat(ByteBufferUtil.string(index.minTerm())).startsWith("a");
            assertThat(ByteBufferUtil.string(index.maxTerm())).startsWith("a");
        }
    }

    private void assertIndexCoversLiveSSTables(ColumnFamilyStore cfs) throws Exception
    {
        assertNoStaleTerms();
        Set<SSTableReader> indexed = getIndexContext(INDEX).getView().getIndexes().stream().map(SSTableIndex::getSSTable).collect(Collectors.toSet());
        assertThat(indexed).containsExactlyInAnyOrderElementsOf(cfs.getLiveSSTables());
        assertThat(execute("SELECT pk FROM %s WHERE a = 'a3'")).hasSize(14);
    }
}
