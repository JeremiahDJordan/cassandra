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

package org.apache.cassandra.db.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.Throwables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Streaming to nodes that read only what Apache Cassandra 5.0 reads, and in storage_compatibility_mode CASSANDRA_5.
 */
public class Cassandra5ModeStreamingTest extends SAITester
{
    private InetAddressAndPort peer;
    private StorageCompatibilityMode mode;
    private SSTableFormat<?, ?> format;

    @Before
    public void setUpPeer() throws Exception
    {
        mode = DatabaseDescriptor.getStorageCompatibilityMode();
        // big sstables, which Apache Cassandra 5.0 reads whatever the bti version written in tests
        format = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(DatabaseDescriptor.getSSTableFormats().get(BigFormat.NAME));
        // a peer at messaging version 13 with this node's schema version
        peer = InetAddressAndPort.getByName("127.0.0.2");
        MessagingService.instance().versions.set(peer, MessagingService.VERSION_50);
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().versions.reset(peer);
        DatabaseDescriptor.setStorageCompatibilityMode(mode);
        DatabaseDescriptor.setSelectedSSTableFormat(format);
    }

    @Test
    public void testWholeOnlyWithAgreedSchemaAndAaGroups() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (pk, v) VALUES (0, 0)");
        flush();
        SSTableReader sstable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();

        setPeerSchema(Schema.instance.getVersion());
        assertThat(entire(sstable)).isTrue();
        setPeerSchema(UUID.randomUUID());
        assertThat(entire(sstable)).isFalse();

        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        setPeerSchema(Schema.instance.getVersion());
        assertThat(entire(sstable)).isFalse();
    }

    @Test
    public void testWholeWithApacheGroups() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, s text, n bigint, PRIMARY KEY (pk, ck))");
        for (String column : new String[]{ "s", "n", "st" })
            createIndex("CREATE INDEX wide_" + column + " ON %s(" + column + ") USING 'sai'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        FileUtils.copySSTablesAndIndexes(cfs.getDirectories().getDirectoryForNewSSTables().toJavaIOFile(), "apache-aa/wide");
        cfs.loadNewSSTables();
        assertThat(cfs.getLiveSSTables()).hasSize(2);

        setPeerSchema(Schema.instance.getVersion());
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertThat(entire(sstable)).isTrue();

        // the components of an index the table no longer has, which Apache didn't write
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        sstable.addComponents(Collections.singleton(new Component(SSTableFormat.Components.Types.CUSTOM, "SAI+" + Version.current(KEYSPACE) + "+dropped+ColumnComplete.db")));
        assertThat(entire(sstable)).isFalse();
    }

    @Test
    public void testModeReceivesWholeSSTablesWithApacheGroups() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, s text, n bigint, PRIMARY KEY (pk, ck))");
        for (String column : new String[]{ "s", "n", "st" })
            createIndex("CREATE INDEX wide_" + column + " ON %s(" + column + ") USING 'sai'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        FileUtils.copySSTablesAndIndexes(cfs.getDirectories().getDirectoryForNewSSTables().toJavaIOFile(), "apache-aa/wide");
        cfs.loadNewSSTables();
        Set<SSTableReader> originals = cfs.getLiveSSTables();
        disableCompaction();

        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_5);
        List<OutgoingStream> streams = new ArrayList<>();
        for (SSTableReader sstable : originals)
        {
            CassandraOutgoingFile stream = outgoingFile(sstable, fullRange(sstable), CassandraOutgoingFile.BtiDaReceiver.ENTIRE);
            assertThat(stream.computeShouldStreamEntireSSTables()).isTrue();
            streams.add(stream);
        }
        new StreamPlan(StreamOperation.OTHER).transferStreams(FBUtilities.getBroadcastAddressAndPort(), streams).execute().get(1, TimeUnit.MINUTES);

        // the received sstables alone answer the query, with Apache's indexes
        Throwables.maybeFail(cfs.getTracker().dropSSTables(originals::contains, OperationType.UNKNOWN, null));
        assertThat(cfs.getLiveSSTables()).hasSize(2).doesNotContainAnyElementsOf(originals);
        assertThat(execute("SELECT pk, ck FROM %s WHERE s = 'w1'")).hasSize(100);
    }

    @Test
    public void testRefusesWhatApacheCantRead() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH compression = {'class': 'AdaptiveCompressor'}");
        execute("INSERT INTO %s (pk, v) VALUES (0, 0)");
        // a flush writes with a fast compressor, a compaction with the table's
        flush();
        compact();
        SSTableReader sstable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();

        assertThat(canSendFailure(sstable)).contains("Cannot stream " + sstable.descriptor + " to " + peer + ", a node at messaging version 13, which reads only what Apache Cassandra 5.0 reads")
                                           .contains("compressor org.apache.cassandra.io.compress.AdaptiveCompressor");

        // a peer of unknown version on the path may be an Apache Cassandra node
        MessagingService.instance().versions.reset(peer);
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_5);
        assertThat(canSendFailure(sstable)).contains("Cannot stream " + sstable.descriptor + " to " + peer + ", whose messaging version isn't known yet, which reads only what Apache Cassandra 5.0 reads");
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        assertThat(canSendFailure(sstable)).isNull();
    }

    @Test
    public void testModeRefusesWholeSSTableApacheCantRead() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int) WITH compression = {'class': 'AdaptiveCompressor'}");
        execute("INSERT INTO %s (pk, v) VALUES (0, 0)");
        // a flush writes with a fast compressor, a compaction with the table's
        flush();
        compact();
        SSTableReader sstable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();

        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_5);
        CassandraOutgoingFile whole = outgoingFile(sstable, fullRange(sstable), CassandraOutgoingFile.BtiDaReceiver.ENTIRE);
        assertThat(whole.computeShouldStreamEntireSSTables()).isTrue();
        assertThat(streamToSelfFailure(Collections.singletonList(whole)))
        .contains("storage_compatibility_mode CASSANDRA_5 doesn't take from /127.0.0.1:7012 a whole sstable Apache Cassandra 5.0 can't read")
        .contains("compressor org.apache.cassandra.io.compress.AdaptiveCompressor");
    }

    /**
     * One session with an entire sstable first and a partial one last: the entire sstable's index groups are
     * validated, so an incomplete one fails the session.
     */
    @Test
    public void testEntireSSTableValidatedBeforePartialLastOne() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        for (int pk = 0; pk < 10; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, pk);
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader first = cfs.getLiveSSTables().iterator().next();
        for (int pk = 10; pk < 20; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, pk);
        flush();
        SSTableReader second = cfs.getLiveSSTables().stream().filter(s -> s != first).findFirst().get();

        StorageAttachedIndex sai = (StorageAttachedIndex) cfs.indexManager.getIndexByName(index);
        IndexDescriptor.load(first, Collections.singleton(sai.getIndexContext()))
                       .perIndexComponents(sai.getIndexContext())
                       .get(IndexComponentType.COLUMN_COMPLETION_MARKER)
                       .file()
                       .delete();

        CassandraOutgoingFile whole = outgoingFile(first, fullRange(first), CassandraOutgoingFile.BtiDaReceiver.ENTIRE);
        CassandraOutgoingFile partial = outgoingFile(second, halfRange(second), CassandraOutgoingFile.BtiDaReceiver.ENTIRE);
        assertThat(whole.computeShouldStreamEntireSSTables()).isTrue();
        assertThat(partial.computeShouldStreamEntireSSTables()).isFalse();
        assertThat(streamToSelfFailure(Arrays.asList(whole, partial))).contains("Incomplete or obsolete per-column index build");
    }

    @Test
    public void testModeRefusesWholeSSTableWithDataStaxSAIComponents() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, v) VALUES (0, 0)");
        flush();
        SSTableReader sstable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();

        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_5);
        CassandraOutgoingFile whole = outgoingFile(sstable, fullRange(sstable), CassandraOutgoingFile.BtiDaReceiver.ENTIRE);
        assertThat(whole.computeShouldStreamEntireSSTables()).isTrue();
        assertThat(streamToSelfFailure(Collections.singletonList(whole)))
        .contains("storage_compatibility_mode CASSANDRA_5 doesn't take from /127.0.0.1:7012 a whole sstable Apache Cassandra 5.0 can't read")
        .contains("SAI component SAI+" + Version.current(KEYSPACE) + "+GroupMeta.db");
    }

    private void setPeerSchema(UUID version)
    {
        Gossiper.instance.initializeNodeUnsafe(peer, UUID.randomUUID(), MessagingService.VERSION_50, 1);
        Gossiper.instance.injectApplicationState(peer, ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(version));
    }

    private boolean entire(SSTableReader sstable)
    {
        CassandraOutgoingFile stream = outgoingFile(sstable, fullRange(sstable), CassandraOutgoingFile.BtiDaReceiver.of(peer));
        try
        {
            return stream.computeShouldStreamEntireSSTables();
        }
        finally
        {
            release(stream);
        }
    }

    /**
     * Why the sstable can't be streamed to the peer, or null if it can.
     */
    private String canSendFailure(SSTableReader sstable)
    {
        CassandraOutgoingFile stream = outgoingFile(sstable, fullRange(sstable), CassandraOutgoingFile.BtiDaReceiver.of(peer));
        try
        {
            stream.checkCanSend();
            return null;
        }
        catch (IllegalStateException e)
        {
            return e.getMessage();
        }
        finally
        {
            release(stream);
        }
    }

    private static List<Range<Token>> fullRange(SSTableReader sstable)
    {
        Token minimum = sstable.getPartitioner().getMinimumToken();
        return Range.normalize(Collections.singletonList(new Range<>(minimum, minimum)));
    }

    private static List<Range<Token>> halfRange(SSTableReader sstable)
    {
        List<Token> tokens = new ArrayList<>();
        try (KeyIterator keys = sstable.keyIterator())
        {
            while (keys.hasNext())
                tokens.add(keys.next().getToken());
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
        Collections.sort(tokens);
        return Range.normalize(Collections.singletonList(new Range<>(sstable.getPartitioner().getMinimumToken(), tokens.get(tokens.size() / 2))));
    }

    private static CassandraOutgoingFile outgoingFile(SSTableReader sstable, List<Range<Token>> ranges, CassandraOutgoingFile.BtiDaReceiver receiver)
    {
        return new CassandraOutgoingFile(StreamOperation.OTHER, sstable.ref(), sstable.getPositionsForRanges(ranges), ranges,
                                         sstable.estimatedKeysForRanges(ranges), receiver);
    }

    private static void release(CassandraOutgoingFile stream)
    {
        stream.releaseStreamRebuildStatus();
        stream.getRef().release();
    }

    /**
     * Streams to this node a session that fails, and returns the stack traces logged by streaming.
     */
    private static String streamToSelfFailure(List<OutgoingStream> streams)
    {
        Logger logger = (Logger) LoggerFactory.getLogger("org.apache.cassandra.streaming");
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
        try
        {
            assertThatThrownBy(() -> new StreamPlan(StreamOperation.OTHER).transferStreams(FBUtilities.getBroadcastAddressAndPort(), streams)
                                                                         .execute()
                                                                         .get(1, TimeUnit.MINUTES))
            .isInstanceOf(ExecutionException.class);
        }
        finally
        {
            logger.detachAppender(logs);
        }
        return logs.list.stream()
                        .filter(event -> event.getThrowableProxy() != null)
                        .map(event -> ThrowableProxyUtil.asString(event.getThrowableProxy()))
                        .collect(Collectors.joining("\n"));
    }
}
