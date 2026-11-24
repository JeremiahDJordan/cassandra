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

package org.apache.cassandra.streaming;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.util.concurrent.Future;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.LegacySSTableTest;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StreamSessionRefusedTransferTest
{
    private static final String KEYSPACE = "legacy_tables";

    private InetAddressAndPort peer;
    private ColumnFamilyStore cc;
    private ColumnFamilyStore da;
    private SSTableReader ccSSTable;
    private SSTableReader daSSTable;
    private final List<StreamMessage> sent = new CopyOnWriteArrayList<>();

    @BeforeClass
    public static void defineSchema()
    {
        LegacySSTableTest.defineSchema();
    }

    @Before
    public void setUp() throws Exception
    {
        LegacySSTableTest.loadLegacyTables("cc");
        LegacySSTableTest.loadLegacyTables("da");
        cc = Keyspace.open(KEYSPACE).getColumnFamilyStore("legacy_cc_simple");
        da = Keyspace.open(KEYSPACE).getColumnFamilyStore("legacy_da_simple");
        ccSSTable = cc.getLiveSSTables().iterator().next();
        daSSTable = da.getLiveSSTables().iterator().next();
        peer = InetAddressAndPort.getByName("127.0.0.2");
        MessagingService.instance().versions.set(peer, MessagingService.VERSION_DS_12);
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().versions.reset(peer);
        LegacySSTableTest.truncateLegacyTables("cc");
        LegacySSTableTest.truncateLegacyTables("da");
    }

    /**
     * An initiator with a bti "da" sstable for a peer that can't receive it fails without sending a prepare message,
     * and releases its other transfers too.
     */
    @Test
    public void testRefusedTransferFailsInitiator() throws Exception
    {
        StreamResultFuture result = new StreamResultFuture(nextTimeUUID(), StreamOperation.OTHER, NO_PENDING_REPAIR, PreviewKind.NONE);
        StreamSession session = session(result, false, PreviewKind.NONE);
        List<OutgoingStream> streams = new ArrayList<>(cc.getStreamManager().createOutgoingStreams(session, fullRing(), NO_PENDING_REPAIR, PreviewKind.NONE));
        boolean ccEntire = ((CassandraOutgoingFile) streams.get(0)).computeShouldStreamEntireSSTables();
        streams.addAll(da.getStreamManager().createOutgoingStreams(session, fullRing(), NO_PENDING_REPAIR, PreviewKind.NONE));
        assertThat(streams).hasSize(2);
        session.addTransferStreams(streams);
        assertThat(ccSSTable.streamRebuildState().zcsStreamCount()).isEqualTo(ccEntire ? 1 : 0);

        session.onInitializationComplete();

        assertRefused(session, result);
    }

    /**
     * A follower asked for a bti "da" sstable by a peer that can't receive it fails without answering the prepare
     * message, and releases its other transfers too.
     */
    @Test
    public void testRefusedTransferFailsFollower() throws Exception
    {
        StreamResultFuture result = new StreamResultFuture(nextTimeUUID(), StreamOperation.OTHER, NO_PENDING_REPAIR, PreviewKind.NONE);
        StreamSession session = session(result, true, PreviewKind.NONE);
        RangesAtEndpoint ranges = fullRing();
        StreamRequest request = new StreamRequest(KEYSPACE, ranges, RangesAtEndpoint.empty(ranges.endpoint()),
                                                  List.of(cc.getTableName(), da.getTableName()));

        session.prepare(Collections.singletonList(request), Collections.emptyList());

        assertRefused(session, result);
    }

    /**
     * A preview sends no sstables, so it isn't refused.
     */
    @Test
    public void testPreviewIsNotRefused() throws Exception
    {
        StreamResultFuture result = new StreamResultFuture(nextTimeUUID(), StreamOperation.OTHER, NO_PENDING_REPAIR, PreviewKind.ALL);
        StreamSession session = session(result, false, PreviewKind.ALL);
        List<OutgoingStream> streams = new ArrayList<>(da.getStreamManager().createOutgoingStreams(session, fullRing(), NO_PENDING_REPAIR, PreviewKind.ALL));
        assertThat(streams).hasSize(1);
        session.addTransferStreams(streams);

        session.onInitializationComplete();

        assertThat(session.state()).isEqualTo(StreamSession.State.PREPARING);
        assertThat(sent).extracting(m -> m.type).containsExactly(StreamMessage.Type.PREPARE_SYN);
        session.abort();
        Util.spinAssertEquals(1, () -> daSSTable.selfRef().globalCount(), 10);
    }

    private void assertRefused(StreamSession session, StreamResultFuture result)
    {
        String diagnostic = "Cannot stream " + daSSTable.descriptor + " to " + peer + ", whose messaging version "
                            + MessagingService.VERSION_DS_12 + " doesn't read it: make the sending node write bti cc";
        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS)).hasMessageContaining(diagnostic);
        assertThat(session.state()).isEqualTo(StreamSession.State.FAILED);
        assertThat(sent).extracting(m -> m.type).containsExactly(StreamMessage.Type.SESSION_FAILED);
        Util.spinAssertEquals(0, () -> ccSSTable.streamRebuildState().zcsStreamCount(), 10);
        Util.spinAssertEquals(1, () -> ccSSTable.selfRef().globalCount(), 10);
        Util.spinAssertEquals(1, () -> daSSTable.selfRef().globalCount(), 10);
    }

    private RangesAtEndpoint fullRing()
    {
        IPartitioner p = cc.getPartitioner();
        return RangesAtEndpoint.toDummyList(Collections.singletonList(new Range<>(p.getMinimumToken(), p.getMinimumToken())));
    }

    private StreamSession session(StreamResultFuture result, boolean follower, PreviewKind previewKind)
    {
        StreamSession session = new StreamSession(StreamOperation.OTHER, peer, new NettyStreamingConnectionFactory(), null,
                                                  MessagingService.current_version, follower, 0, NO_PENDING_REPAIR, previewKind)
        {
            @Override
            protected Future<?> sendControlMessage(StreamMessage message)
            {
                sent.add(message);
                AsyncPromise<Object> promise = new AsyncPromise<>(null);
                promise.setSuccess(null);
                return promise;
            }
        };
        session.init(result);
        return session;
    }
}
