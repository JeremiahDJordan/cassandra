/*
 * Copyright DataStax, Inc.
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
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.awaitility.Awaitility.await;

/**
 * Distributed tests for ANN options.
 */
public class ANNOptionsDistributedTest extends TestBaseImpl
{
    private static final int NUM_REPLICAS = 2;
    private static final int RF = 2;

    /**
     * Test that ANN options are accepted in clusters with all nodes in DS 11 (although SAI will reject them for now).
     */
    @Test
    public void testANNOptionsWithAllDS11() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION,
                                                                  MessagingService.VERSION_DS_11);
             Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            // null indicates that the query should succeed
            testSelectWithAnnOptions(cluster, null);
        }
    }

    /**
     * Test that ANN options are rejected in clusters with all nodes below DS 11.
     */
    @Test
    public void testANNOptionsWithAllDS10() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION,
                                                                  MessagingService.VERSION_DS_10);
             Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            testSelectWithAnnOptions(cluster, "ANN options are not supported in clusters below DS 11.");
        }
    }

    /**
     * Test that ANN options are rejected in clusters with some nodes below DS 11.
     */
    @Test
    public void testANNOptionsWithMixedDS10AndDS11() throws Throwable
    {
        assert CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getInt() >= MessagingService.VERSION_DS_11;

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer(BB::install)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL))
                                           .start(), RF))
        {
            testSelectWithAnnOptions(cluster, "ANN options are not supported in clusters below DS 11.");
        }
    }

    /**
     * Test that ANN options are accepted after a rolling upgrade from DS 10, without waiting for the connections between
     * the nodes to connect again on the new version.
     */
    @Test
    public void testANNOptionsAfterRollingUpgradeFromDS10() throws Throwable
    {
        assert CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.getInt() >= MessagingService.VERSION_DS_11;

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer((classLoader, threadGroup, node, generation) -> {
                                               if (generation == 0)
                                                   BB.pin(classLoader);
                                           })
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.large (k int PRIMARY KEY, b blob)"));
            testSelectWithAnnOptions(cluster, "ANN options are not supported in clusters below DS 11.");

            restart(cluster, 1);
            // a write large enough for the large message connection, which connects on DS 10
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.large (k, b) VALUES (0, ?)"),
                                           ConsistencyLevel.ALL, ByteBuffer.allocate(100_000));

            restart(cluster, 2);
            assertSelectWithAnnOptions(cluster, null);
        }
    }

    /**
     * Restarts the given node, without its pinned messaging version, and waits until it and the other nodes see each
     * other alive with their versions, and the indexes are queryable.
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
        SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);
    }

    /** Waits until the given observer sees the given node alive, with its messaging version capped to the observer's. */
    private static void awaitKnown(IInvokableInstance observer, IInvokableInstance node)
    {
        InetSocketAddress address = node.broadcastAddress();
        int version = node.callOnInstance(() -> MessagingService.current_version);
        await().atMost(1, TimeUnit.MINUTES).until(() -> observer.callOnInstance(() -> {
            InetAddressAndPort endpoint = InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
            return MessagingService.instance().versions.knows(endpoint)
                   && MessagingService.instance().versions.get(endpoint) == Math.min(version, MessagingService.current_version)
                   && FailureDetector.instance.isAlive(endpoint);
        }));
    }

    private static void testSelectWithAnnOptions(Cluster cluster, String expectedErrorMessage)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, n int, v vector<float, 2>)"));
        cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t(v) USING 'StorageAttachedIndex'"));
        // Wait for index to be queryable on all nodes since test queries from all coordinators
        SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);
        assertSelectWithAnnOptions(cluster, expectedErrorMessage);
    }

    private static void assertSelectWithAnnOptions(Cluster cluster, String expectedErrorMessage)
    {
        String selectRerankk = withKeyspace("SELECT * FROM %s.t ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'rerank_k': 10}");
        String selectUsePruning = withKeyspace("SELECT * FROM %s.t ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'use_pruning': false}");

        for (int i = 1; i <= cluster.size(); i++)
        {
            ICoordinator coordinator = cluster.coordinator(i);
            if (expectedErrorMessage == null)
            {
                coordinator.execute(selectRerankk, ConsistencyLevel.ONE);
                coordinator.execute(selectUsePruning, ConsistencyLevel.ONE);
            }
            else
            {
                Assertions.assertThatThrownBy(() -> coordinator.execute(selectRerankk, ConsistencyLevel.ONE))
                          .hasMessageContaining(expectedErrorMessage);
                Assertions.assertThatThrownBy(() -> coordinator.execute(selectUsePruning, ConsistencyLevel.ONE))
                          .hasMessageContaining(expectedErrorMessage);
            }
        }
    }

    /**
     * Injection to set the current version of the first cluster node, or of the node of a given class loader, to DS 10.
     */
    public static class BB
    {
        public static void install(ClassLoader classLoader, int node)
        {
            if (node == 1)
                pin(classLoader);
        }

        static void pin(ClassLoader classLoader)
        {
            new ByteBuddy().rebase(MessagingService.class)
                           .method(named("currentVersion"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static int currentVersion()
        {
            return MessagingService.VERSION_DS_10;
        }
    }
}
