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

package org.apache.cassandra.distributed.test.sai;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FixedValue;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.reads.range.EndpointGroupingRangeCommandIterator;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.MAX_CONCURRENT_RANGE_REQUESTS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.awaitility.Awaitility.await;

/**
 * Tests that SAI range reads group their ranges by endpoint ({@link Verb#MULTI_RANGE_REQ}) only for the replicas on
 * DS 10 or later, as nodes on an earlier messaging version, such as Apache Cassandra 5.0 nodes, don't know that verb.
 * Unless said otherwise, each batch of the range reads of node 1 queries a single token range, and with one token per
 * node and RF 2 on 3 nodes, some ranges have nodes 1 and 2 as replicas, and the others have node 3.
 */
public class EndpointGroupingMessagingVersionTest extends TestBaseImpl
{
    private static final int ROWS = 30;
    private static final String QUERY = "SELECT k FROM %s.t WHERE v = 0";

    @Test
    public void testAllDS20() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(MAX_CONCURRENT_RANGE_REQUESTS, 1);
             Cluster cluster = init(Cluster.build(3)
                                           .withTokenCount(1)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            Set<String> requests = recordRangeRequests(cluster);

            assertQuery(cluster);

            Assertions.assertThat(requests).contains("MULTI_RANGE_REQ to 1", "MULTI_RANGE_REQ to 2", "MULTI_RANGE_REQ to 3")
                      .noneMatch(request -> request.startsWith("RANGE_REQ"));
        }
    }

    @Test
    public void testReplicaBelowDS10() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(MAX_CONCURRENT_RANGE_REQUESTS, 1);
             Cluster cluster = init(Cluster.build(3)
                                           .withTokenCount(1)
                                           .withInstanceInitializer(SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 3))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            Set<String> requests = recordRangeRequests(cluster);

            assertQuery(cluster);

            Assertions.assertThat(requests).contains("RANGE_REQ to 3", "MULTI_RANGE_REQ to 1", "MULTI_RANGE_REQ to 2")
                      .doesNotContain("MULTI_RANGE_REQ to 3");
        }
    }

    @Test
    public void testReplicaOfUnknownVersion() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(MAX_CONCURRENT_RANGE_REQUESTS, 1);
             Cluster cluster = init(Cluster.build(3)
                                           .withTokenCount(1)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            InetSocketAddress node2 = cluster.get(2).broadcastAddress();
            cluster.get(1).runOnInstance(() -> MessagingService.instance().versions.reset(InetAddressAndPort.getByAddress(node2)));
            Set<String> requests = recordRangeRequests(cluster);

            assertQuery(cluster);

            Assertions.assertThat(requests).contains("RANGE_REQ to 2", "MULTI_RANGE_REQ to 1", "MULTI_RANGE_REQ to 3")
                      .doesNotContain("MULTI_RANGE_REQ to 2");
        }
    }

    @Test
    public void testReplicaRestartedBelowDS10() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(MAX_CONCURRENT_RANGE_REQUESTS, 1);
             Cluster cluster = init(Cluster.build(3)
                                           .withTokenCount(1)
                                           .withInstanceInitializer((classLoader, threadGroup, node, generation) -> {
                                               if (generation > 0)
                                                   SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 3).accept(classLoader, node);
                                           })
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            Set<String> requests = recordRangeRequests(cluster);
            assertQuery(cluster);
            Assertions.assertThat(requests).contains("MULTI_RANGE_REQ to 3");

            cluster.get(3).shutdown().get();
            cluster.get(3).startup();
            InetSocketAddress node3 = cluster.get(3).broadcastAddress();
            await().atMost(1, TimeUnit.MINUTES).until(() -> cluster.get(1).callOnInstance(() -> {
                InetAddressAndPort endpoint = InetAddressAndPort.getByAddress(node3);
                Integer version = MessagingService.instance().versions.getRawIfKnown(endpoint);
                return version != null && version == MessagingService.VERSION_50 && FailureDetector.instance.isAlive(endpoint);
            }));
            SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE);
            requests.clear();

            assertQuery(cluster);

            Assertions.assertThat(requests).contains("RANGE_REQ to 3", "MULTI_RANGE_REQ to 1", "MULTI_RANGE_REQ to 2")
                      .doesNotContain("MULTI_RANGE_REQ to 3");
        }
    }

    /**
     * A batch of several ranges, one of which has node 3 as a replica, isn't grouped, and limits and paging still work.
     */
    @Test
    public void testBatchOfSeveralRangesWithReplicaBelowDS10() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(MAX_CONCURRENT_RANGE_REQUESTS, 100);
             Cluster cluster = init(Cluster.build(3)
                                           .withTokenCount(1)
                                           .withInstanceInitializer(SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 3))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            Set<String> requests = recordRangeRequests(cluster);

            assertQuery(cluster);
            Assertions.assertThat(cluster.coordinator(1).execute(withKeyspace(QUERY + " LIMIT 4"), ConsistencyLevel.ALL).length)
                      .isEqualTo(4);
            List<Integer> keys = new ArrayList<>();
            cluster.coordinator(1).executeWithPaging(withKeyspace(QUERY), ConsistencyLevel.ALL, 3)
                   .forEachRemaining(row -> keys.add((Integer) row[0]));
            Assertions.assertThat(keys).containsExactlyInAnyOrderElementsOf(expectedKeys());

            Assertions.assertThat(requests).contains("RANGE_REQ to 2", "RANGE_REQ to 3")
                      .noneMatch(request -> request.startsWith("MULTI_RANGE_REQ"));
        }
    }

    /**
     * A connection on a version below DS 10 counts, even if node 1 has a later version for node 3.
     */
    @Test
    public void testConnectionBelowDS10() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(MAX_CONCURRENT_RANGE_REQUESTS, 1);
             Cluster cluster = init(Cluster.build(3)
                                           .withTokenCount(1)
                                           .withInstanceInitializer(SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 3))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            InetSocketAddress node3 = cluster.get(3).broadcastAddress();
            cluster.get(1).runOnInstance(() -> MessagingService.instance().versions.set(InetAddressAndPort.getByAddress(node3),
                                                                                       MessagingService.current_version));
            Set<String> requests = recordRangeRequests(cluster);

            assertQuery(cluster);

            Assertions.assertThat(requests).contains("RANGE_REQ to 3", "MULTI_RANGE_REQ to 1", "MULTI_RANGE_REQ to 2")
                      .doesNotContain("MULTI_RANGE_REQ to 3");
        }
    }

    /**
     * DS Converged Cassandra 4.0 nodes (DS 10 to DS 12) know multi-range reads.
     */
    @Test
    public void testReplicaOnDS12() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(MAX_CONCURRENT_RANGE_REQUESTS, 1);
             Cluster cluster = init(Cluster.build(3)
                                           .withTokenCount(1)
                                           .withInstanceInitializer(SAIUtil.pinMessagingVersion(MessagingService.VERSION_DS_12, 3))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            Set<String> requests = recordRangeRequests(cluster);

            assertQuery(cluster);

            Assertions.assertThat(requests).contains("MULTI_RANGE_REQ to 1", "MULTI_RANGE_REQ to 2", "MULTI_RANGE_REQ to 3")
                      .noneMatch(request -> request.startsWith("RANGE_REQ"));
        }
    }

    @Test
    public void testCoordinatorBelowDS10() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(MAX_CONCURRENT_RANGE_REQUESTS, 1);
             Cluster cluster = init(Cluster.build(3)
                                           .withTokenCount(1)
                                           .withInstanceInitializer(SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 1))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            Set<String> requests = recordRangeRequests(cluster);

            assertQuery(cluster);

            Assertions.assertThat(requests).contains("RANGE_REQ to 2", "RANGE_REQ to 3")
                      .noneMatch(request -> request.startsWith("MULTI_RANGE_REQ"));
        }
    }

    /**
     * If node 1 doesn't know yet that node 3 is below DS 10, the serialization of the multi-range read refuses it,
     * before writing anything, also for a read larger than the large message threshold. The read times out, with the
     * reason in the log, and node 1 doesn't reconnect to node 3.
     */
    @Test
    public void testSerializationRefusal() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(MAX_CONCURRENT_RANGE_REQUESTS, 1);
             Cluster cluster = init(Cluster.build(3)
                                           .withTokenCount(1)
                                           .withInstanceInitializer(BB::installHidingThirdNode)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                                       .set("read_request_timeout", "60s")
                                                                       .set("range_request_timeout", "5s"))
                                           .start(), 2))
        {
            createSchema(cluster);

            // The in-JVM dtest outbound sink serializes every message on the caller thread, which production doesn't:
            // remove it, so that the real outbound connection of node 1 to node 3 serializes the read command.
            cluster.get(1).runOnInstance(() -> MessagingService.instance().outboundSink.clear());

            // a range read without index of more than the large message threshold, for the large message connection
            String value = "x".repeat(40 * 1024);
            String unindexed = "SELECT k FROM %s.t WHERE w = ? AND w2 = ? ALLOW FILTERING";
            cluster.coordinator(1).execute(withKeyspace(unindexed), ConsistencyLevel.ALL, value, value);
            long connections = successfulConnections(cluster, 3);

            String large = "SELECT k FROM %s.t WHERE s = ? AND u = ?";
            for (Object[] query : new Object[][]{ { QUERY }, { large, value, value } })
            {
                Object[] values = Arrays.copyOfRange(query, 1, query.length);
                Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(withKeyspace((String) query[0]), ConsistencyLevel.ALL, values))
                          .hasMessageContaining("Operation timed out");
            }

            long start = System.nanoTime();
            Assertions.assertThat(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.t"), ConsistencyLevel.ALL).length)
                      .isEqualTo(ROWS);
            Assertions.assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)).isLessThan(10);
            cluster.coordinator(1).execute(withKeyspace(unindexed), ConsistencyLevel.ALL, value, value);
            Assertions.assertThat(successfulConnections(cluster, 3)).isEqualTo(connections);

            Assertions.assertThat(cluster.get(1).logs().grep("Cannot serialize a multi-range read command for messaging version " +
                                                             MessagingService.VERSION_50).getResult()).isNotEmpty();
        }
    }

    private static void createSchema(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v int, s text, u text, w text, w2 text)"));
        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX v_idx ON %s.t(v) USING 'StorageAttachedIndex'"));
        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX s_idx ON %s.t(s) USING 'StorageAttachedIndex'"));
        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX u_idx ON %s.t(u) USING 'StorageAttachedIndex'"));
        SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE);

        for (int k = 0; k < ROWS; k++)
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), ConsistencyLevel.ALL, k, k % 3);

        // a read of every range at ALL, so that node 1 has a connection to every node and knows its version
        cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.t"), ConsistencyLevel.ALL);
    }

    /**
     * Records the range read requests of node 1, as "verb to node".
     */
    private static Set<String> recordRangeRequests(Cluster cluster)
    {
        Set<String> requests = ConcurrentHashMap.newKeySet();
        cluster.filters()
               .verbs(Verb.RANGE_REQ.id, Verb.MULTI_RANGE_REQ.id)
               .from(1)
               .messagesMatching((from, to, message) -> {
                   requests.add((message.verb() == Verb.RANGE_REQ.id ? "RANGE_REQ" : "MULTI_RANGE_REQ") + " to " + to);
                   return false;
               })
               .drop();
        return requests;
    }

    private static void assertQuery(Cluster cluster)
    {
        List<Integer> keys = new ArrayList<>();
        for (Object[] row : cluster.coordinator(1).execute(withKeyspace(QUERY), ConsistencyLevel.ALL))
            keys.add((Integer) row[0]);
        Assertions.assertThat(keys).containsExactlyInAnyOrderElementsOf(expectedKeys());
    }

    private static List<Integer> expectedKeys()
    {
        List<Integer> expected = new ArrayList<>();
        for (int k = 0; k < ROWS; k += 3)
            expected.add(k);
        return expected;
    }

    /**
     * The connections that node 1 has established to the given node, over all connection types.
     */
    private static long successfulConnections(Cluster cluster, int node)
    {
        InetAddress address = cluster.get(node).broadcastAddress().getAddress();
        long connections = 0;
        for (Object[] row : cluster.get(1).executeInternal("SELECT address, successful_connection_attempts FROM system_views.internode_outbound"))
        {
            if (address.equals(row[0]))
                connections += (Long) row[1];
        }
        return connections;
    }

    public static class BB
    {
        /**
         * Pins node 3 to the Apache Cassandra 5.0 messaging version, and makes node 1 send it multi-range reads, as
         * before node 1 learns its version.
         */
        public static void installHidingThirdNode(ClassLoader classLoader, int node)
        {
            SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 3).accept(classLoader, node);
            if (node == 1)
            {
                new ByteBuddy().rebase(EndpointGroupingRangeCommandIterator.class)
                               .method(named("canSendMultiRangeRead"))
                               .intercept(FixedValue.value(true))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }
    }
}
