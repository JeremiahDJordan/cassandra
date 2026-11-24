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

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Tests that ANN queries using synthetic score columns (see {@code Ordering.Ann}) return the right rows in the right
 * order when a replica is on a messaging version below DS 10, such as an Apache Cassandra 5.0 node, which can't
 * deserialize those columns: that replica is sent the vector column instead, and the coordinator computes the scores
 * of its rows.
 */
public class ANNSyntheticScoreDistributedTest extends TestBaseImpl
{
    private static final int ROWS = 10;
    // the rows nearest to [1, 0] first, which isn't their token or clustering order
    private static final List<Integer> EXPECTED = Arrays.asList(9, 8, 7, 6, 5);

    private static final List<String> QUERIES = Arrays.asList("SELECT k FROM %s.t ORDER BY v ANN OF [1, 0] LIMIT 5",
                                                              "SELECT k, n FROM %s.t ORDER BY v ANN OF [1, 0] LIMIT 5",
                                                              "SELECT * FROM %s.t ORDER BY v ANN OF [1, 0] LIMIT 5",
                                                              "SELECT c FROM %s.wide WHERE k = ? ORDER BY v ANN OF [1, 0] LIMIT 5",
                                                              "SELECT * FROM %s.wide WHERE k = ? ORDER BY v ANN OF [1, 0] LIMIT 5");

    @Before
    public void enableSyntheticScores()
    {
        CassandraRelevantProperties.SAI_ANN_USE_SYNTHETIC_SCORE.setBoolean(true);
    }

    @After
    public void resetSyntheticScores()
    {
        CassandraRelevantProperties.SAI_ANN_USE_SYNTHETIC_SCORE.reset();
    }

    /**
     * Node 2 is on the Apache Cassandra 5.0 messaging version.
     */
    @Test
    public void testMixedApache50AndDS() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 2))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 1))
        {
            createSchema(cluster);
            insert(cluster);

            for (int coordinator = 1; coordinator <= 2; coordinator++)
            {
                List<Boolean> sent = new CopyOnWriteArrayList<>();
                assertQueries(cluster, coordinator, sent);
                Assertions.assertThat(sent).hasSize(QUERIES.size()).containsOnly(false);
            }
        }
    }

    /**
     * Node 2 restarts on the Apache Cassandra 5.0 messaging version.
     */
    @Test
    public void testPeerRestartsOnApache50() throws Throwable
    {
        // with several data directories, the flush at the end of the commit log replay fails
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .withInstanceInitializer((classLoader, threadGroup, node, generation) -> {
                                               if (generation > 0)
                                                   SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 2).accept(classLoader, node);
                                           })
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 1))
        {
            createSchema(cluster);
            insert(cluster);

            List<Boolean> sent = new CopyOnWriteArrayList<>();
            assertQueries(cluster, 1, sent);
            Assertions.assertThat(sent).hasSize(QUERIES.size()).containsOnly(true);

            cluster.get(2).shutdown().get();
            cluster.get(2).startup();
            Assertions.assertThat(cluster.get(2).getMessagingVersion()).isEqualTo(MessagingService.VERSION_50);
            for (String index : Arrays.asList("t_idx", "wide_idx"))
                SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, index, 2);

            sent.clear();
            assertQueries(cluster, 1, sent);
            Assertions.assertThat(sent).hasSize(QUERIES.size()).containsOnly(false);
        }
    }

    /**
     * Node 1 reads from node 2, on the Apache Cassandra 5.0 messaging version, over a new connection, without knowing
     * the version of node 2 before the handshake of that connection. Node 2 drops the read commands with synthetic
     * columns.
     */
    @Test
    public void testNewConnectionToApache50() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 2))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                                       .set("read_request_timeout", "10s")
                                                                       .set("range_request_timeout", "10s"))
                                           .start(), 1))
        {
            createSchema(cluster);
            insert(cluster);
            int partition = partitionOf(cluster, 2);

            cluster.get(2).runOnInstance(() -> MessagingService.instance().inboundSink.add(message -> !hasSyntheticColumns(message)));
            InetSocketAddress address = cluster.get(2).broadcastAddress();
            cluster.get(1).runOnInstance(() -> {
                // The in-JVM dtest outbound sink serializes every message with the version that node 1 knows: remove
                // it, so that the real outbound connection serializes the read commands.
                MessagingService.instance().outboundSink.clear();
                InetAddressAndPort node2 = InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
                MessagingService.instance().closeOutboundNow(node2);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (MessagingService.instance().channelManagers.containsKey(node2) && System.nanoTime() < deadline)
                    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                Assertions.assertThat(MessagingService.instance().channelManagers).doesNotContainKey(node2);
                MessagingService.instance().versions.reset(node2);
            });

            for (String query : QUERIES)
                Assertions.assertThat(execute(cluster, 1, query, partition)).as(query).isEqualTo(EXPECTED);
        }
    }

    /**
     * Nodes 2 and 3 are on the Apache Cassandra 5.0 messaging version, and have a newer vector of row 0 than node 1.
     * The range read of node 1 gets row 0 from node 1, with the score of its vector, and from node 2 or 3, with the
     * newer vector and no score: the row is scored by the newer vector.
     */
    @Test
    public void testNewerVectorOnApache50() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withInstanceInitializer(SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 2, 3))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            for (int node = 1; node <= cluster.size(); node++)
            {
                String vector = node == 1 ? "[1, 0]" : "[0, 1]";
                cluster.get(node).executeInternal(withKeyspace("INSERT INTO %s.t (k, n, v) VALUES (0, 0, " + vector + ") USING TIMESTAMP " + (node == 1 ? 100 : 200)));
                cluster.get(node).executeInternal(withKeyspace("INSERT INTO %s.t (k, n, v) VALUES (1, 1, [1, 1]) USING TIMESTAMP 100"));
            }

            String query = "SELECT k FROM %s.t ORDER BY v ANN OF [1, 0] LIMIT 2";
            Assertions.assertThat(execute(cluster, 1, query, 0)).isEqualTo(Arrays.asList(1, 0));
        }
    }

    /**
     * Runs the queries on the given coordinator, reading the partition of the other node for the single-partition
     * ones, and adds to {@code sent} whether each read command that it sends to the other node has synthetic columns,
     * as that node deserializes it.
     */
    private static void assertQueries(Cluster cluster, int coordinator, List<Boolean> sent)
    {
        int replica = 3 - coordinator;
        IMessageFilters.Filter filter = cluster.filters()
                                               .outbound()
                                               .verbs(Verb.READ_REQ.id, Verb.RANGE_REQ.id)
                                               .from(coordinator)
                                               .to(replica)
                                               .messagesMatching((from, to, message) -> {
                                                   sent.add(cluster.get(to).callsOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>) () -> hasSyntheticColumns(Instance.deserializeMessage(message))).call());
                                                   return false;
                                               })
                                               .drop();
        try
        {
            int partition = partitionOf(cluster, replica);
            for (String query : QUERIES)
                Assertions.assertThat(execute(cluster, coordinator, query, partition)).as(query + " on node " + coordinator).isEqualTo(EXPECTED);
        }
        finally
        {
            filter.off();
        }
    }

    /**
     * Executes the query at ONE on the given coordinator, binding the partition key if it has a marker, and returns the
     * value of the key or clustering column of each row.
     */
    private static List<Integer> execute(Cluster cluster, int coordinator, String query, int partition)
    {
        Object[] values = query.contains("?") ? new Object[]{ partition } : new Object[0];
        List<Integer> rows = new ArrayList<>();
        for (Object[] row : cluster.coordinator(coordinator).execute(withKeyspace(query), ConsistencyLevel.ONE, values))
            rows.add((Integer) row[query.startsWith("SELECT * FROM %s.wide") ? 1 : 0]);
        return rows;
    }

    /**
     * Whether the message is a read command with synthetic columns.
     */
    @SuppressWarnings("unchecked")
    private static boolean hasSyntheticColumns(Message<?> message)
    {
        if (message.verb() != Verb.READ_REQ && message.verb() != Verb.RANGE_REQ)
            return false;

        ReadCommand command = ((Message<ReadCommand>) message).payload;
        for (ColumnMetadata column : command.columnFilter().fetchedColumns())
        {
            if (column.isSynthetic())
                return true;
        }
        for (ColumnMetadata column : command.columnFilter().queriedColumns())
        {
            if (column.isSynthetic())
                return true;
        }
        return false;
    }

    /**
     * Creates the schema locally on each node, with fixed ids, because schema isn't propagated between nodes on
     * different messaging versions (see StreamingTest).
     */
    private static void createSchema(Cluster cluster)
    {
        for (int node = 1; node <= cluster.size(); node++)
        {
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE TABLE IF NOT EXISTS %s.t (k int PRIMARY KEY, n int, v vector<float, 2>) " +
                                                                "WITH ID = " + tableId("t")));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS t_idx ON %s.t(v) USING 'StorageAttachedIndex'"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE TABLE IF NOT EXISTS %s.wide (k int, c int, n int, v vector<float, 2>, PRIMARY KEY (k, c)) " +
                                                                "WITH ID = " + tableId("wide")));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS wide_idx ON %s.wide(v) USING 'StorageAttachedIndex'"));
        }
        for (int node = 1; node <= cluster.size(); node++)
            for (String index : Arrays.asList("t_idx", "wide_idx"))
                SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, index, node);
    }

    private static String tableId(String table)
    {
        return UUID.nameUUIDFromBytes((KEYSPACE + '.' + table).getBytes(StandardCharsets.UTF_8)).toString();
    }

    /**
     * Inserts rows whose vectors get nearer to [1, 0] as their key grows, in table {@code t}, and in a partition of
     * each node in table {@code wide}.
     */
    private static void insert(Cluster cluster)
    {
        int[] partitions = { partitionOf(cluster, 1), partitionOf(cluster, 2) };
        for (int i = 0; i < ROWS; i++)
        {
            String vector = "[1, " + (ROWS - 1 - i) + "]";
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, n, v) VALUES (?, ?, " + vector + ")"), ConsistencyLevel.ONE, i, i);
            for (int partition : partitions)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.wide (k, c, n, v) VALUES (?, ?, ?, " + vector + ")"),
                                               ConsistencyLevel.ONE, partition, i, i);
        }
    }

    /**
     * Returns the first partition key that the given node has.
     */
    private static int partitionOf(Cluster cluster, int node)
    {
        String address = cluster.get(node).broadcastAddress().getAddress().getHostAddress();
        for (int k = 0; ; k++)
        {
            if (SAIUtil.endpoints(cluster, KEYSPACE, "wide", k).contains(address))
                return k;
        }
    }
}
