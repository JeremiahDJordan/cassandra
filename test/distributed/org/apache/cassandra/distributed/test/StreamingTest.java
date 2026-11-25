/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.StreamMessage;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRow;
import static org.apache.cassandra.streaming.StreamSession.State.PREPARING;
import static org.apache.cassandra.streaming.StreamSession.State.STREAMING;
import static org.apache.cassandra.streaming.StreamSession.State.WAIT_COMPLETE;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.PREPARE_ACK;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.PREPARE_SYN;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.PREPARE_SYNACK;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.RECEIVED;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.STREAM;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.STREAM_INIT;
import static org.awaitility.Awaitility.await;

public class StreamingTest extends TestBaseImpl
{

    private void testStreaming(int nodes, int replicationFactor, int rowCount, String compactionStrategy) throws Throwable
    {
        try (Cluster cluster = builder().withNodes(nodes)
                                        .withDataDirCount(1) // this test expects there to only be a single sstable to stream (with ddirs = 3, we get 3 sstables)
                                        .withConfig(config -> config.with(NETWORK)).start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + "};");
            cluster.schemaChange(String.format("CREATE TABLE %s.cf (k text, c1 text, c2 text, PRIMARY KEY (k)) WITH compaction = {'class': '%s', 'enabled': 'true'}", KEYSPACE, compactionStrategy));

            for (int i = 0 ; i < rowCount ; ++i)
            {
                for (int n = 1 ; n < nodes ; ++n)
                    cluster.get(n).executeInternal(String.format("INSERT INTO %s.cf (k, c1, c2) VALUES (?, 'value1', 'value2');", KEYSPACE), Integer.toString(i));
            }

            cluster.get(nodes).executeInternal("TRUNCATE system.available_ranges;");
            {
                Object[][] results = cluster.get(nodes).executeInternal(String.format("SELECT k, c1, c2 FROM %s.cf;", KEYSPACE));
                Assert.assertEquals(0, results.length);
            }

            // collect message and state
            registerSink(cluster, nodes);

            cluster.get(nodes).runOnInstance(() -> StorageService.instance.rebuild(null, KEYSPACE, null, null));
            verifySinks(cluster);
            {
                Object[][] results = cluster.get(nodes).executeInternal(String.format("SELECT k, c1, c2 FROM %s.cf;", KEYSPACE));
                Assert.assertEquals(rowCount, results.length);
                Arrays.sort(results, Comparator.comparingInt(a -> Integer.parseInt((String) a[0])));
                for (int i = 0 ; i < results.length ; ++i)
                {
                    Assert.assertEquals(Integer.toString(i), results[i][0]);
                    Assert.assertEquals("value1", results[i][1]);
                    Assert.assertEquals("value2", results[i][2]);
                }
            }
        }
    }

    @Test
    public void test() throws Throwable
    {
        testStreaming(2, 2, 1000, "LeveledCompactionStrategy");
    }

    @Test
    public void testMixedMessagingVersionStreamingDs11ToDs12() throws Throwable
    {
        testMixedMessagingVersionStreaming(MessagingService.VERSION_DS_11, MessagingService.VERSION_DS_12, false);
    }

    @Test
    public void testMixedMessagingVersionStreamingDs12ToDs11() throws Throwable
    {
        testMixedMessagingVersionStreaming(MessagingService.VERSION_DS_12, MessagingService.VERSION_DS_11, false);
    }

    @Test
    public void testMixedMessagingVersionUncompressedStreamingDs11ToDs12() throws Throwable
    {
        testMixedMessagingVersionStreaming(MessagingService.VERSION_DS_11, MessagingService.VERSION_DS_12, true);
    }

    @Test
    public void testMixedMessagingVersionUncompressedStreamingDs12ToDs11() throws Throwable
    {
        testMixedMessagingVersionStreaming(MessagingService.VERSION_DS_12, MessagingService.VERSION_DS_11, true);
    }

    private void testMixedMessagingVersionStreaming(int sourceVersion, int targetVersion, boolean uncompressed) throws Throwable
    {
        int rowCount = 1000;
        String keyspace = KEYSPACE + '_' + sourceVersion + '_' + targetVersion + (uncompressed ? "_uncompressed" : "");
        try (Cluster cluster = builder().withNodes(2)
                                       .withDataDirCount(1)
                                       // disable entire sstable streaming so the per-section CassandraStreamReader/Writer path is used
                                       .withConfig(config -> config.with(NETWORK).set("stream_entire_sstables", !uncompressed))
                                       .withInstanceInitializer((classLoader, node) -> initializeMessagingVersion(classLoader, node == 1 ? sourceVersion : targetVersion))
                                       .start())
        {
            Assert.assertEquals(sourceVersion, (int) cluster.get(1).callOnInstance(() -> MessagingService.current_version));
            Assert.assertEquals(targetVersion, (int) cluster.get(2).callOnInstance(() -> MessagingService.current_version));

            // disable sstable compression so the uncompressed stream path is exercised
            String compression = uncompressed ? " AND compression = {'enabled': 'false'}" : "";
            int schemaCoordinator = sourceVersion <= targetVersion ? 1 : 2;
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};", false, cluster.get(schemaCoordinator));
            cluster.schemaChange(String.format("CREATE TABLE %s.cf (k text, c1 text, c2 text, PRIMARY KEY (k)) WITH compaction = {'class': 'LeveledCompactionStrategy', 'enabled': 'true'}%s", keyspace, compression), false, cluster.get(schemaCoordinator));

            for (int i = 0 ; i < rowCount ; ++i)
                cluster.get(1).executeInternal(String.format("INSERT INTO %s.cf (k, c1, c2) VALUES (?, 'value1', 'value2');", keyspace), Integer.toString(i));

            cluster.get(2).executeInternal("TRUNCATE system.available_ranges;");
            Assert.assertEquals(0, cluster.get(2).executeInternal(String.format("SELECT k, c1, c2 FROM %s.cf;", keyspace)).length);

            registerSink(cluster, 2);
            cluster.get(2).runOnInstance(() -> StorageService.instance.rebuild(null, keyspace, null, null));
            verifySinks(cluster);

            Object[][] results = cluster.get(2).executeInternal(String.format("SELECT k, c1, c2 FROM %s.cf;", keyspace));
            Assert.assertEquals(rowCount, results.length);
            Arrays.sort(results, Comparator.comparingInt(a -> Integer.parseInt((String) a[0])));
            for (int i = 0 ; i < results.length ; ++i)
            {
                assertRow(results[i], Integer.toString(i), "value1", "value2");
            }
        }
    }

    private static void initializeMessagingVersion(ClassLoader classLoader, int version)
    {
        String key = CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getKey();
        String previous = System.getProperty(key);
        try
        {
            System.setProperty(key, Integer.toString(version));
            Class.forName(MessagingService.class.getName(), true, classLoader).getField("current_version").getInt(null);
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (previous == null)
                System.clearProperty(key);
            else
                System.setProperty(key, previous);
        }
    }

    public static void registerSink(Cluster cluster, int initiatorNodeId)
    {
        IInvokableInstance initiatorNode = cluster.get(initiatorNodeId);
        InetSocketAddress initiator = initiatorNode.broadcastAddress();
        MessageStateSinkImpl initiatorSink = new MessageStateSinkImpl();

        for (int node = 1; node <= cluster.size(); node++)
        {
            if (initiatorNodeId == node)
                continue;

            IInvokableInstance followerNode = cluster.get(node);
            InetSocketAddress follower = followerNode.broadcastAddress();

            // verify on initiator's stream session
            initiatorSink.messages(follower, Arrays.asList(PREPARE_SYNACK, STREAM, StreamMessage.Type.COMPLETE));
            initiatorSink.states(follower, Arrays.asList(PREPARING, STREAMING, WAIT_COMPLETE, StreamSession.State.COMPLETE));

            // verify on follower's stream session
            MessageStateSinkImpl followerSink = new MessageStateSinkImpl();
            followerSink.messages(initiator, Arrays.asList(STREAM_INIT, PREPARE_SYN, PREPARE_ACK, RECEIVED));
            followerSink.states(initiator,  Arrays.asList(PREPARING, STREAMING, StreamSession.State.COMPLETE));
            followerNode.runOnInstance(() -> StreamSession.sink = followerSink);
        }

        cluster.get(initiatorNodeId).runOnInstance(() -> StreamSession.sink = initiatorSink);
    }

    public static void verifySinks(Cluster cluster)
    {
        for (int node = 1; node <= cluster.size(); node++)
        {
            IInvokableInstance instance = cluster.get(node);
            await().atMost(1, MINUTES).until(() -> instance.callOnInstance(() -> ((MessageStateSinkImpl) StreamSession.sink).allClosed()));
            List<String> failures = instance.callOnInstance(() -> new ArrayList<>(((MessageStateSinkImpl) StreamSession.sink).failures));
            Assert.assertTrue("Node " + node + ": " + failures, failures.isEmpty());
        }
    }

    /**
     * Records mismatches instead of throwing them, as a throw from inside the stream session leaves it unfinished.
     */
    @VisibleForTesting
    public static class MessageStateSinkImpl implements StreamSession.MessageStateSink, Serializable
    {
        // use enum ordinal instead of enum to walk around inter-jvm class loader issue, only classes defined in
        // InstanceClassLoader#sharedClassNames are shareable between server jvm and test jvm
        public final Map<InetAddress, Queue<Integer>> messageSink = new ConcurrentHashMap<>();
        public final Map<InetAddress, Queue<Integer>> stateTransitions = new ConcurrentHashMap<>();
        public final List<String> failures = new CopyOnWriteArrayList<>();
        private final Set<InetAddress> closed = ConcurrentHashMap.newKeySet();

        public void messages(InetSocketAddress peer, List<StreamMessage.Type> messages)
        {
            messageSink.put(peer.getAddress(), messages.stream().map(Enum::ordinal).collect(Collectors.toCollection(ConcurrentLinkedQueue::new)));
        }

        public void states(InetSocketAddress peer, List<StreamSession.State> states)
        {
            stateTransitions.put(peer.getAddress(), states.stream().map(Enum::ordinal).collect(Collectors.toCollection(ConcurrentLinkedQueue::new)));
        }

        @Override
        public void recordState(InetAddressAndPort from, StreamSession.State state)
        {
            Integer expected = stateTransitions.get(from.address).poll();
            if (expected == null)
                failures.add("Unexpected state " + state + " from " + from);
            else if (expected != state.ordinal())
                failures.add("Expected state " + StreamSession.State.values()[expected] + " but was " + state + " from " + from);
        }

        @Override
        public void recordMessage(InetAddressAndPort from, StreamMessage.Type message)
        {
            if (message == StreamMessage.Type.KEEP_ALIVE)
                return;

            Integer expected = messageSink.get(from.address).poll();
            if (expected == null)
                failures.add("Unexpected message " + message + " from " + from);
            else if (expected != message.ordinal())
                failures.add("Expected message " + StreamMessage.Type.values()[expected] + " but was " + message + " from " + from);
        }

        @Override
        public void onClose(InetAddressAndPort from)
        {
            Queue<Integer> states = stateTransitions.get(from.address);
            if (!states.isEmpty())
                failures.add("Missing states " + states + " from " + from);

            Queue<Integer> messages = messageSink.get(from.address);
            if (!messages.isEmpty())
                failures.add("Missing messages " + messages + " from " + from);

            closed.add(from.address);
        }

        public boolean allClosed()
        {
            return closed.containsAll(stateTransitions.keySet());
        }
    }
}
