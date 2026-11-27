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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.StubMethod;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.awaitility.Awaitility.await;

/**
 * Tests that queries using parts of {@link RowFilter} that only exist in DataStax's Cassandra (see
 * {@link RowFilter#dataStaxOnlyFeature()}) are rejected while the query can contact nodes on a messaging version below
 * DS 10, such as Apache Cassandra 4.0 and 4.1 nodes, which would fail to deserialize them or drop them, and that the
 * queries Apache Cassandra supports still work.
 */
public class DataStaxOnlyRowFilterDistributedTest extends TestBaseImpl
{
    private static final int NUM_REPLICAS = 2;
    private static final int RF = 2;

    /** Queries using DataStax-only operators, disjunctions and expressions, with the feature named by the error. */
    private static final List<String[]> DATASTAX_ONLY_QUERIES = Arrays.asList(
        new String[]{ "SELECT k FROM %s.t WHERE s NOT CONTAINS 1 ALLOW FILTERING", "The NOT CONTAINS operator" },
        new String[]{ "SELECT k FROM %s.t WHERE n IN (1, 2)", "The IN operator" },
        new String[]{ "SELECT k FROM %s.t WHERE am CONTAINS KEY 'apple'", "The CONTAINS KEY operator on an analyzed index" },
        new String[]{ "SELECT k FROM %s.t WHERE v : 'apple'", "The : operator (and = on an analyzed index)" },
        new String[]{ "SELECT k FROM %s.t WHERE v = 'apple'", "The : operator (and = on an analyzed index)" },
        new String[]{ "SELECT k FROM %s.t WHERE a = 'APPLE'", "The : operator (and = on an analyzed index)" },
        new String[]{ "SELECT k FROM %s.t WHERE m['a'] > 0", "The > operator on a map entry" },
        new String[]{ "SELECT k FROM %s.t WHERE n = 1 OR n = 2", "The OR operator" },
        new String[]{ "SELECT k FROM %s.t ORDER BY vec ANN OF [1, 1] LIMIT 1", "The ANN operator" },
        new String[]{ "SELECT k FROM %s.t WHERE GEO_DISTANCE(vec, [1, 1]) < 1000", "GEO_DISTANCE" },
        new String[]{ "SELECT k FROM %s.t ORDER BY n ASC LIMIT 3", "ORDER BY on a non-clustering column" },
        new String[]{ "SELECT k FROM %s.t ORDER BY v BM25 OF 'apple' LIMIT 3", "ORDER BY BM25" });

    /** Queries that Apache Cassandra 4.0 and 4.1 nodes can deserialize, with the number of rows they find. */
    private static final Map<String, Integer> APACHE_QUERIES = new LinkedHashMap<>();
    static
    {
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE n = 1", 1);
        // NOT IN is sent as != expressions
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE n NOT IN (1, 2) ALLOW FILTERING", 1);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE n > 0 AND n < 2", 1);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE n != 2 ALLOW FILTERING", 2);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE s CONTAINS 1 ALLOW FILTERING", 3);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE m['a'] = 1", 3);
    }

    @After
    public void resetMessagingVersion()
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.reset();
    }

    /**
     * Without nodes below DS 10, the DataStax-only parts of the filter are accepted.
     */
    @Test
    public void testAllDS12() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            createSchema(cluster);

            for (String[] query : DATASTAX_ONLY_QUERIES)
            {
                // BM25 needs a later SAI version than the default one of this cluster, and isn't about messaging
                if (query[1].contains("BM25"))
                    continue;
                assertAccepted(cluster, query[0]);
            }
            assertRowCounts(cluster, APACHE_QUERIES);
        }
    }

    /**
     * A node on the Apache Cassandra 4.0 messaging version rejects the DataStax-only parts of the filter.
     */
    @Test
    public void testAllApache40Messaging() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_40);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            createSchema(cluster);

            assertRejected(cluster, DATASTAX_ONLY_QUERIES);
            assertRowCounts(cluster, APACHE_QUERIES);
        }
    }

    /**
     * With one node on the Apache Cassandra 4.0 messaging version, every coordinator rejects the DataStax-only parts
     * of the filter, and still accepts the queries that node can deserialize.
     */
    @Test
    public void testMixedApache40AndDS12() throws Throwable
    {
        assert CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getInt() >= MessagingService.VERSION_DS_10;

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer(BB::install)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            createSchema(cluster);

            // read from every node at ALL, so that every coordinator has a connection to the other node
            assertRowCounts(cluster, APACHE_QUERIES);

            assertRejected(cluster, DATASTAX_ONLY_QUERIES);
        }
    }

    /**
     * If a coordinator doesn't know yet that a replica is below DS 10, the serialization of the read command for that
     * replica refuses the DataStax-only parts of the filter. The read then fails right away for that replica, with
     * the reason in the log, and the connection to it keeps working for other queries.
     */
    @Test
    public void testSerializationBackstop() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer(BB::installWithoutCoordinatorCheck)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                                       .set("read_request_timeout_in_ms", 60000L)
                                                                       .set("range_request_timeout_in_ms", 60000L))
                                           .start(), RF))
        {
            createSchema(cluster);
            assertRowCounts(cluster, APACHE_QUERIES);

            // The in-JVM dtest outbound sink serializes every message on the caller thread, which production doesn't:
            // remove it, so that the real outbound connection of node 2 to node 1 serializes the read command.
            cluster.get(2).runOnInstance(() -> MessagingService.instance().outboundSink.clear());

            for (String query : Arrays.asList("SELECT k FROM %s.t WHERE n = 1 OR n = 2",
                                              "SELECT k FROM %s.t WHERE s NOT CONTAINS 7 ALLOW FILTERING",
                                              "SELECT k FROM %s.t WHERE k = 1 AND s NOT CONTAINS 7 ALLOW FILTERING",
                                              "SELECT k FROM %s.t WHERE m['a'] > 0"))
            {
                long start = System.nanoTime();
                Assertions.assertThatThrownBy(() -> cluster.coordinator(2).execute(withKeyspace(query), ConsistencyLevel.ALL))
                          .as(query)
                          .hasMessageContaining("Operation failed");
                Assertions.assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)).as(query).isLessThan(10);

                // the other queries to that replica keep working
                start = System.nanoTime();
                Assertions.assertThat(cluster.coordinator(2).execute(withKeyspace("SELECT k FROM %s.t WHERE n = 1"), ConsistencyLevel.ALL).length)
                          .as(query)
                          .isEqualTo(1);
                Assertions.assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)).as(query).isLessThan(10);
            }

            Assertions.assertThat(cluster.get(2).logs().grep("Cannot serialize a row filter for messaging version " +
                                                             MessagingService.VERSION_40).getResult()).isNotEmpty();
        }
    }

    /**
     * After a rolling upgrade from the Apache Cassandra 4.0 messaging version, every coordinator accepts OR without
     * waiting for its connections to the other nodes to connect again on the new version.
     */
    @Test
    public void testRollingUpgradeFromApache40Messaging() throws Throwable
    {
        AtomicBoolean startOnApache40 = new AtomicBoolean(true);
        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer((classLoader, node) -> {
                                               if (startOnApache40.get())
                                                   SAIUtil.pinMessagingVersion(MessagingService.VERSION_40, 1, 2).accept(classLoader, node);
                                           })
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            createSchema(cluster);
            String largeId = UUID.nameUUIDFromBytes("large".getBytes(StandardCharsets.UTF_8)).toString();
            for (int node = 1; node <= cluster.size(); node++)
                cluster.get(node).schemaChangeInternal(withKeyspace("CREATE TABLE %s.large (k int PRIMARY KEY, b blob) WITH ID = " + largeId));
            String orQuery = "SELECT k FROM %s.t WHERE n = 1 OR n = 2";

            startOnApache40.set(false);
            restart(cluster, 1);
            // a write large enough for the large message connection, which connects on the Apache Cassandra version
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.large (k, b) VALUES (0, ?)"),
                                           ConsistencyLevel.ALL, ByteBuffer.allocate(100_000));
            assertRejected(cluster, Collections.singletonList(new String[]{ orQuery, "The OR operator" }));

            restart(cluster, 2);
            assertAccepted(cluster, orQuery);
        }
    }

    /**
     * Restarts the given node, and waits until it and the other nodes see each other alive with their versions, and its
     * indexes are queryable.
     */
    private static void restart(Cluster cluster, int node) throws Exception
    {
        cluster.get(node).shutdown().get();
        cluster.get(node).startup();
        for (IInvokableInstance other : cluster)
        {
            if (other.config().num() != node)
            {
                awaitKnown(other, cluster.get(node));
                awaitKnown(cluster.get(node), other);
            }
        }
        waitForIndexes(cluster, node);
    }

    /** Waits until the given observer sees the given node alive, with its messaging version capped to the observer's. */
    private static void awaitKnown(IInvokableInstance observer, IInvokableInstance node)
    {
        InetSocketAddress address = node.broadcastAddress();
        int version = node.callOnInstance(() -> MessagingService.current_version);
        await().atMost(1, TimeUnit.MINUTES).until(() -> observer.callOnInstance(() -> {
            InetAddressAndPort endpoint = InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
            Integer known = MessagingService.instance().versions.getRawIfKnown(endpoint);
            int current = MessagingService.current_version;
            return known != null && Math.min(known, current) == Math.min(version, current) && FailureDetector.instance.isAlive(endpoint);
        }));
    }

    private static void waitForIndexes(Cluster cluster, int node)
    {
        for (String index : Arrays.asList("n_idx", "v_idx", "a_idx", "m_idx", "am_idx", "vec_idx"))
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, index, node);
    }

    /**
     * Creates the schema locally on each node, with fixed ids, because schema isn't propagated between nodes on
     * different messaging versions (see StreamingTest), and inserts some data.
     */
    private static void createSchema(Cluster cluster)
    {
        String tableId = UUID.nameUUIDFromBytes(KEYSPACE.getBytes(StandardCharsets.UTF_8)).toString();
        for (int node = 1; node <= cluster.size(); node++)
        {
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE TABLE IF NOT EXISTS %s.t " +
                                                                "(k int PRIMARY KEY, n int, s set<int>, v text, a text, " +
                                                                "m map<text, int>, am map<text, text>, vec vector<float, 2>) " +
                                                                "WITH ID = " + tableId));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS n_idx ON %s.t(n) USING 'StorageAttachedIndex'"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS v_idx ON %s.t(v) USING 'StorageAttachedIndex' " +
                                                                "WITH OPTIONS = { 'index_analyzer': 'standard' }"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS a_idx ON %s.t(a) USING 'StorageAttachedIndex' " +
                                                                "WITH OPTIONS = { 'case_sensitive': 'false' }"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS m_idx ON %s.t(entries(m)) USING 'StorageAttachedIndex'"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS am_idx ON %s.t(keys(am)) USING 'StorageAttachedIndex' " +
                                                                "WITH OPTIONS = { 'index_analyzer': 'standard' }"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS vec_idx ON %s.t(vec) USING 'StorageAttachedIndex' " +
                                                                "WITH OPTIONS = { 'similarity_function': 'euclidean' }"));
        }
        // the local schema changes don't reach schema agreement, so wait for each index on each node without it
        for (int node = 1; node <= cluster.size(); node++)
            waitForIndexes(cluster, node);

        for (int k = 0; k < 3; k++)
        {
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, n, s, v, a, m, am, vec) " +
                                                        "VALUES (?, ?, {1, 2}, 'apple pie', 'Apple', {'a': 1}, {'Golden Apple': 'Green Pear'}, [1, 1])"),
                                           ConsistencyLevel.ALL, k, k);
        }
    }

    private static void assertAccepted(Cluster cluster, String query)
    {
        for (int i = 1; i <= cluster.size(); i++)
            cluster.coordinator(i).execute(withKeyspace(query), ConsistencyLevel.ALL);
    }

    /**
     * Checks the number of rows each query finds from every coordinator, at ALL, so that the node below DS 10 is a
     * replica of the query.
     */
    private static void assertRowCounts(Cluster cluster, Map<String, Integer> queries)
    {
        for (Map.Entry<String, Integer> query : queries.entrySet())
        {
            for (int i = 1; i <= cluster.size(); i++)
            {
                Object[][] rows = cluster.coordinator(i).execute(withKeyspace(query.getKey()), ConsistencyLevel.ALL);
                Assertions.assertThat(rows.length).as("node" + i + ": " + query.getKey()).isEqualTo(query.getValue());
            }
        }
    }

    private static void assertRejected(Cluster cluster, List<String[]> queries)
    {
        List<String> notRejected = new ArrayList<>();
        for (String[] query : queries)
        {
            String expectedMessage = String.format(RowFilter.DATASTAX_ONLY_FEATURE_ERROR, query[1]);
            for (int i = 1; i <= cluster.size(); i++)
            {
                ICoordinator coordinator = cluster.coordinator(i);
                try
                {
                    coordinator.execute(withKeyspace(query[0]), ConsistencyLevel.ONE);
                    notRejected.add("node" + i + ": " + query[0]);
                }
                catch (Throwable t)
                {
                    Assertions.assertThat(t).hasMessageContaining(expectedMessage);
                }
            }
        }
        Assertions.assertThat(notRejected).isEmpty();
    }

    /**
     * Injection to set the current messaging version of the first cluster node to the Apache Cassandra 4.0 one.
     */
    public static class BB
    {
        public static void install(ClassLoader classLoader, int node)
        {
            SAIUtil.pinMessagingVersion(MessagingService.VERSION_40, 1).accept(classLoader, node);
        }

        /**
         * Same as {@link #install}, and disables the coordinator check on node 2, as when it doesn't know yet the
         * version of node 1, so that the serialization is what refuses the filter.
         */
        public static void installWithoutCoordinatorCheck(ClassLoader classLoader, int node)
        {
            install(classLoader, node);
            if (node == 2)
            {
                new ByteBuddy().rebase(RowFilter.class)
                               .method(named("validateForPeers"))
                               .intercept(StubMethod.INSTANCE)
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }
    }
}
