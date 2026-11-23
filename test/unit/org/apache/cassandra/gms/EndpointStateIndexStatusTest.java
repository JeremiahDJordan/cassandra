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

package org.apache.cassandra.gms;

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexStatusFormat;
import org.apache.cassandra.index.IndexStatusManager;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;
import org.json.simple.JSONValue;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * HCD-623: HCD 1.2 nodes (messaging version up to {@link MessagingService#VERSION_DS_12}) can only parse the legacy
 * index status format, so the compact one must be rewritten when gossiping with them.
 */
public class EndpointStateIndexStatusTest
{
    private static final Map<String, Index.Status> STATUSES = Map.of("ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                                                     "ks1.idx2", Index.Status.FULL_REBUILD_STARTED,
                                                                     "ks2.idx3", Index.Status.BUILD_FAILED);

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Test
    public void testLegacyConversionOfCompactFormat() throws Exception
    {
        String compact = IndexStatusFormat.toSerializedFormat(STATUSES);
        String legacy = IndexStatusFormat.toLegacySerializedFormat(compact);

        assertEquals(STATUSES, parseAsHcd12(legacy));
        assertEquals(STATUSES, IndexStatusFormat.fromSerializedFormat(legacy));
    }

    @Test
    public void testLegacyConversionKeepsLegacyAndUnparseableValues()
    {
        String legacy = IndexStatusFormat.toLegacySerializedFormat(STATUSES);
        assertSame(legacy, IndexStatusFormat.toLegacySerializedFormat(legacy));

        String empty = IndexStatusFormat.toSerializedFormat(Map.of());
        assertSame(empty, IndexStatusFormat.toLegacySerializedFormat(empty));

        String garbage = "not json";
        assertSame(garbage, IndexStatusFormat.toLegacySerializedFormat(garbage));

        String malformed = "{\"ks1\": {\"idx1\": 3}";
        assertSame(malformed, IndexStatusFormat.toLegacySerializedFormat(malformed));

        // a code this version does not know: not sent to peers that need the legacy format
        String unknownCode = "{\"ks1\": {\"idx1\": 99}}";
        assertNull(IndexStatusFormat.toLegacySerializedFormat(unknownCode));
    }

    @Test
    public void testLocalIndexStatusGossipedInCompactFormat() throws Exception
    {
        // enough indexes for the legacy format to exceed the 64KiB limit of a gossiped value, but not the compact one
        Map<String, Index.Status> statuses = new HashMap<>();
        for (int i = 0; i < 2000; i++)
            statuses.put("a_rather_long_keyspace_name.idx" + i, Index.Status.BUILD_SUCCEEDED);
        assertTrue(IndexStatusFormat.toLegacySerializedFormat(statuses).length() > 0xFFFF);

        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.43");
        try
        {
            setLocalIndexStatus(statuses);
            Gossiper.instance.start(0);

            // a peer whose release version isn't known yet, as during an upgrade
            Gossiper.instance.endpointStateMap.put(peer, new EndpointState(new HeartBeatState(1, 2)));
            Gossiper.instance.clusterVersionProvider.reset();
            assertEquals(CassandraVersion.NULL_VERSION, Gossiper.instance.getMinVersion());

            IndexStatusManager.instance.propagateLocalIndexStatus("a_rather_long_keyspace_name", "idx0", Index.Status.BUILD_SUCCEEDED);
            await().atMost(10, TimeUnit.SECONDS).until(() -> {
                VersionedValue status = Gossiper.instance.getEndpointStateForEndpoint(local).getApplicationState(ApplicationState.INDEX_STATUS);
                return status != null && statuses.equals(IndexStatusFormat.fromSerializedFormat(status.value));
            });

            EndpointState localState = Gossiper.instance.getEndpointStateForEndpoint(local);
            String gossiped = localState.getApplicationState(ApplicationState.INDEX_STATUS).value;
            assertEquals(gossiped, roundTrip(localState, MessagingService.current_version).getApplicationState(ApplicationState.INDEX_STATUS).value);
            assertTrue(JsonUtils.fromJsonMap(gossiped).containsKey("a_rather_long_keyspace_name"));

            // too long in the legacy format: left out for peers that need it, the other states are sent
            EndpointState received = roundTrip(localState, MessagingService.VERSION_50);
            assertNull(received.getApplicationState(ApplicationState.INDEX_STATUS));
            assertEquals(localState.states().size() - 1, received.states().size());
        }
        finally
        {
            Gossiper.instance.stop();
            Gossiper.instance.endpointStateMap.remove(peer);
            Gossiper.instance.clusterVersionProvider.reset();
            setLocalIndexStatus(Maps.transformValues(statuses, s -> Index.Status.DROPPED));
        }
    }

    private static void setLocalIndexStatus(Map<String, Index.Status> statuses)
    {
        for (Map.Entry<String, Index.Status> e : statuses.entrySet())
        {
            String[] keyspaceAndIndex = e.getKey().split("\\.");
            IndexStatusManager.instance.propagateLocalIndexStatus(keyspaceAndIndex[0], keyspaceAndIndex[1], e.getValue());
        }
    }

    @Test
    public void testLegacyValueRelayedBackIsParsed() throws Exception
    {
        // an HCD 1.2 node relays our legacy rewrite of a peer's status to another HCD 2.0 node
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.42");
        String legacy = IndexStatusFormat.toLegacySerializedFormat(IndexStatusFormat.toSerializedFormat(STATUSES));
        IndexStatusManager.instance.receivePeerIndexStatus(peer, VersionedValue.unsafeMakeVersionedValue(legacy, 1));

        for (Map.Entry<String, Index.Status> e : STATUSES.entrySet())
        {
            String[] keyspaceAndIndex = e.getKey().split("\\.");
            assertEquals(e.getValue(), IndexStatusManager.instance.getIndexStatus(peer, keyspaceAndIndex[0], keyspaceAndIndex[1]));
        }
    }

    @Test
    public void testIndexStatusRewrittenForHcd12Peers() throws Exception
    {
        String compact = IndexStatusFormat.toSerializedFormat(STATUSES);

        // HCD 1.2 (up to VERSION_DS_12), and Apache Cassandra 5.0, of which 5.0.0-5.0.2 only parse the legacy format
        for (int version : new int[]{ MessagingService.VERSION_40, MessagingService.VERSION_50,
                                      MessagingService.VERSION_DS_10, MessagingService.VERSION_DS_11,
                                      MessagingService.VERSION_DS_12 })
        {
            VersionedValue received = roundTrip(compact, version).getApplicationState(ApplicationState.INDEX_STATUS);
            assertEquals(STATUSES, parseAsHcd12(received.value));
            assertEquals(42, received.version);
        }
    }

    @Test
    public void testIndexStatusUnchangedForHcd20Peers() throws Exception
    {
        String compact = IndexStatusFormat.toSerializedFormat(STATUSES);

        for (int version : new int[]{ MessagingService.VERSION_DS_20, MessagingService.VERSION_DSE_68 })
        {
            EndpointState received = roundTrip(compact, version);
            VersionedValue status = received.getApplicationState(ApplicationState.INDEX_STATUS);
            assertEquals(compact, status.value);
            assertEquals(42, status.version);
            assertEquals("other", received.getApplicationState(ApplicationState.HOST_ID).value);
        }
    }

    @Test
    public void testIndexStatusTooLongInLegacyFormatNotSentToLegacyPeers() throws Exception
    {
        // enough indexes for the legacy format to exceed the 64KiB limit of a gossiped value, but not the compact one
        Map<String, Index.Status> statuses = new HashMap<>();
        for (int i = 0; i < 2000; i++)
            statuses.put("a_rather_long_keyspace_name.idx" + i, Index.Status.FULL_REBUILD_STARTED);
        String compact = IndexStatusFormat.toSerializedFormat(statuses);
        assertTrue(IndexStatusFormat.toLegacySerializedFormat(statuses).length() > 0xFFFF);
        assertTrue(compact.length() <= 0xFFFF);

        assertNull(IndexStatusFormat.toLegacySerializedFormat(compact));

        // gossip with HCD 1.2 and Apache Cassandra 5.0 peers keeps working, without the index status
        for (int version : new int[]{ MessagingService.VERSION_50, MessagingService.VERSION_DS_12 })
        {
            EndpointState received = roundTrip(compact, version);
            assertNull(received.getApplicationState(ApplicationState.INDEX_STATUS));
            assertEquals("other", received.getApplicationState(ApplicationState.HOST_ID).value);
        }
    }

    @Test
    public void testIndexStatusDroppedForPre40Peers() throws Exception
    {
        String compact = IndexStatusFormat.toSerializedFormat(STATUSES);
        assertNull(roundTrip(compact, MessagingService.VERSION_30).getApplicationState(ApplicationState.INDEX_STATUS));
    }

    @Test
    public void testOtherStatesUnchangedForHcd12Peers() throws Exception
    {
        String compact = IndexStatusFormat.toSerializedFormat(STATUSES);

        EndpointState received = roundTrip(compact, MessagingService.VERSION_DS_12);
        assertEquals(2, received.states().size());
        assertEquals("other", received.getApplicationState(ApplicationState.HOST_ID).value);
        assertEquals(7, received.getApplicationState(ApplicationState.HOST_ID).version);
    }

    private static EndpointState roundTrip(String indexStatus, int version) throws IOException
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.INDEX_STATUS, VersionedValue.unsafeMakeVersionedValue(indexStatus, 42));
        states.put(ApplicationState.HOST_ID, VersionedValue.unsafeMakeVersionedValue("other", 7));
        return roundTrip(new EndpointState(new HeartBeatState(1, 2), states), version);
    }

    private static EndpointState roundTrip(EndpointState state, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            EndpointState.serializer.serialize(state, out, version);
            assertEquals(EndpointState.serializer.serializedSize(state, version), out.getLength());
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                return EndpointState.serializer.deserialize(in, version);
            }
        }
    }

    /**
     * Parses the index status like HCD 1.2 does in {@code SecondaryIndexManager.receivePeerIndexStatus}.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Index.Status> parseAsHcd12(String value) throws Exception
    {
        Map<String, String> peerStatus = (Map<String, String>) JSONValue.parseWithException(value);
        Map<String, Index.Status> indexStatus = new HashMap<>();
        for (Map.Entry<String, String> e : peerStatus.entrySet())
            indexStatus.put(e.getKey(), Index.Status.valueOf(e.getValue()));
        return indexStatus;
    }
}
