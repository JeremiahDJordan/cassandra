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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
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
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
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

    private static final Map<String, AtomicInteger> pauseAt = new ConcurrentHashMap<>();
    private static final Map<String, CountDownLatch> paused = new ConcurrentHashMap<>();
    private static final Map<String, CountDownLatch> released = new ConcurrentHashMap<>();

    private static volatile boolean hookPauseArmed;

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
        released.values().forEach(CountDownLatch::countDown);
        pauseAt.clear();
        paused.clear();
        released.clear();
        hookPauseArmed = false;
    }

    // called by Byteman
    public static void pause(String name) throws InterruptedException
    {
        if (pauseAt.get(name).decrementAndGet() == 0)
        {
            paused.get(name).countDown();
            released.get(name).await(60, TimeUnit.SECONDS);
        }
    }

    // called by Byteman
    public static boolean listingInHook()
    {
        return hookPauseArmed && calledBy("onAddedToExistingTable");
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
        injectPause("stream", InvokePointBuilder.newInvokePoint().onClass(CassandraStreamReceiver.class).onMethod("finished").atEntry(), 1);
        Future<StreamState> stream = streamHalfToSelf(cfs);
        try
        {
            awaitPause("stream");
            dropIndex("DROP INDEX %s." + INDEX);

            injectPause("build", InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild").atExit(), 1);
            createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            awaitPause("build");
            released.get("stream").countDown();
            stream.get(60, TimeUnit.SECONDS);
            assertThat(cfs.getLiveSSTables()).hasSize(2);
            released.get("build").countDown();
        }
        finally
        {
            Injections.deleteAll();
            released.values().forEach(CountDownLatch::countDown);
        }
        waitForTableIndexesQueryable();
        assertIndexCoversLiveSSTables(cfs);
    }

    /**
     * A stream written for the stale index adds its SSTable while CREATE INDEX of the same name lists the table's
     * SSTables, before the new index is registered; the stream completes before the initial build: the new index
     * doesn't use the stale files.
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
        injectPause("stream", InvokePointBuilder.newInvokePoint().onClass(CassandraStreamReceiver.class).onMethod("finished").atEntry(), 1);
        injectPause("cleanup", InvokePointBuilder.newInvokePoint().onClass(CassandraStreamReceiver.class).onMethod("cleanup").atExit(), 1);
        injectPause("hook", InvokePointBuilder.newInvokePoint().onClass(File.class).onMethod("tryListNames").atEntry(), 1,
                    StaleSameNameIndexComponentsStreamingTest.class.getName() + ".listingInHook()");
        Future<StreamState> stream = streamHalfToSelf(cfs);
        try
        {
            awaitPause("stream");
            dropIndex("DROP INDEX %s." + INDEX);
            hookPauseArmed = true;
            CompletableFuture<Void> create = CompletableFuture.runAsync(() -> createIndexAsync("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'"));
            awaitPause("hook");
            injectPause("build", InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndex.class).onMethod("startInitialBuild").atEntry(), 1);
            released.get("stream").countDown();
            waitForAssert(() -> assertThat(cfs.getLiveSSTables()).hasSize(2), 30, TimeUnit.SECONDS);
            hookPauseArmed = false;
            released.get("hook").countDown();
            create.get(60, TimeUnit.SECONDS);
            awaitPause("build");
            awaitPause("cleanup");
            released.get("cleanup").countDown();
            stream.get(60, TimeUnit.SECONDS);
            released.get("build").countDown();
            waitForAssert(() -> assertThat(cfs.indexManager.isIndexBuilding(INDEX)).isFalse(), 60, TimeUnit.SECONDS);
        }
        finally
        {
            Injections.deleteAll();
            released.values().forEach(CountDownLatch::countDown);
        }
        assertNoStaleComponentsUsed(cfs);
    }

    /**
     * A stream written for the stale index is about to add its SSTable when the table has no index of that name; the
     * new index of the same name is created and built before the SSTable is added: the new index doesn't use the stale
     * files.
     */
    @Test
    public void testStreamAddingBeforeCreateAddedAfterBuild() throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithStaleIndex();
        injectPause("stream", InvokePointBuilder.newInvokePoint().onClass(CassandraStreamReceiver.class).onMethod("finished").atEntry(), 1);
        Future<StreamState> stream = streamHalfToSelf(cfs);
        try
        {
            awaitPause("stream");
            dropIndex("DROP INDEX %s." + INDEX);
            injectPause("adding", InvokePointBuilder.newInvokePoint().onClass(Tracker.class).onMethod("addSSTablesInternal").atInvoke("setupOnline"), 1);
            released.get("stream").countDown();
            awaitPause("adding");
            createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            waitForTableIndexesQueryable();
            released.get("adding").countDown();
            stream.get(60, TimeUnit.SECONDS);
            assertThat(cfs.getLiveSSTables()).hasSize(2);
        }
        finally
        {
            Injections.deleteAll();
            released.values().forEach(CountDownLatch::countDown);
        }
        assertNoStaleComponentsUsed(cfs);
    }

    /**
     * The stale index is dropped while a stream writes its SSTable, so that the stream writes no files for it, and the
     * new index of the same name is built before the stream adds the SSTable: the new index includes it.
     */
    @Test
    public void testStreamWithoutFilesOfDroppedIndexAddedAfterCreate() throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithStaleIndex();
        injectPause("writer", InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexWriter.class).onMethod("complete").atEntry(), 1);
        injectPause("stream", InvokePointBuilder.newInvokePoint().onClass(CassandraStreamReceiver.class).onMethod("finished").atEntry(), 1);
        Future<StreamState> stream = streamHalfToSelf(cfs);
        try
        {
            awaitPause("writer");
            dropIndex("DROP INDEX %s." + INDEX);
            released.get("writer").countDown();
            awaitPause("stream");

            createIndex("CREATE CUSTOM INDEX " + INDEX + " ON %s(a) USING 'StorageAttachedIndex'");
            waitForTableIndexesQueryable();
            released.get("stream").countDown();
            stream.get(60, TimeUnit.SECONDS);
            assertThat(cfs.getLiveSSTables()).hasSize(2);
        }
        finally
        {
            Injections.deleteAll();
            released.values().forEach(CountDownLatch::countDown);
        }
        assertThat(cfs.indexManager.isIndexQueryable(INDEX)).isTrue();
        assertIndexCoversLiveSSTables(cfs);
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

    /** Pauses the provided call (1 for the next one) at the provided point, until released. */
    private static void injectPause(String name, InvokePointBuilder at, int call) throws Throwable
    {
        injectPause(name, at, call, "true");
    }

    /** Pauses the provided call (1 for the next one) at the provided point when the condition holds, until released. */
    private static void injectPause(String name, InvokePointBuilder at, int call, String condition) throws Throwable
    {
        pauseAt.put(name, new AtomicInteger(call));
        paused.put(name, new CountDownLatch(1));
        released.put(name, new CountDownLatch(1));
        Injections.inject(Injections.newCustom("pause_" + name)
                                    .add(at)
                                    .add(ActionBuilder.newActionBuilder()
                                                      .conditions()
                                                      .when(condition)
                                                      .actions()
                                                      .doAction(StaleSameNameIndexComponentsStreamingTest.class.getName() + ".pause(\"" + name + "\")"))
                                    .build());
    }

    private static void awaitPause(String name) throws InterruptedException
    {
        assertThat(paused.get(name).await(60, TimeUnit.SECONDS)).isTrue();
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

    /** The index either includes every live SSTable, with the terms of its own column, or isn't queryable. */
    private void assertNoStaleComponentsUsed(ColumnFamilyStore cfs) throws Exception
    {
        assertNoStaleTerms();
        if (cfs.indexManager.isIndexQueryable(INDEX))
            assertIndexCoversLiveSSTables(cfs);
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
