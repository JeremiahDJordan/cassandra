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

package org.apache.cassandra.distributed.test.sai;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.StubMethod;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexStatusManager;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.virtual.ColumnIndexesSystemView;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ApacheUpgradePath;
import org.apache.cassandra.service.ClientState;
import org.assertj.core.util.Streams;
import org.awaitility.core.ConditionTimeoutException;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.awaitility.Awaitility.await;

public class SAIUtil
{
    private static final long INDEX_BUILD_TIMEOUT_SECONDS = 60;

    /**
     * Sets the SAI non-strict replica filtering option in a node configuration.
     */
    public static IInstanceConfig withNonStrictReplicaFiltering(IInstanceConfig config, boolean enabled)
    {
        if (!enabled)
            return config;
        return config.set("sai_options", Collections.singletonMap("non_strict_replica_filtering_enabled", true));
    }

    /**
     * Enables or disables non-strict replica filtering on every node of a running cluster.
     */
    public static void setNonStrictReplicaFiltering(Cluster cluster, boolean enabled)
    {
        cluster.forEach(node -> setNonStrictReplicaFiltering(node, enabled));
    }

    /**
     * Enables or disables non-strict replica filtering on a running node.
     */
    public static void setNonStrictReplicaFiltering(IInvokableInstance node, boolean enabled)
    {
        node.runOnInstance(() -> DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(enabled));
    }

    /** The parameters of a test that runs with non-strict replica filtering disabled and enabled. */
    public static Collection<Object[]> nonStrictReplicaFilteringModes()
    {
        return List.of(new Object[]{ false }, new Object[]{ true });
    }

    /**
     * Waits until all indexes in the given keyspace become queryable.
     * This checks from node1's perspective. If your test queries from multiple nodes,
     * use {@link #waitForIndexQueryableOnAllNodes(Cluster, String)} instead.
     *
     * @param cluster the cluster
     * @param keyspace the keyspace name
     */
    public static void waitForIndexQueryableOnFirstNode(Cluster cluster, String keyspace)
    {
        waitForIndexQueryable(cluster, keyspace, 1);
    }

    /**
     * Waits until given index becomes queryable.
     * This checks from node1's perspective. If your test queries from multiple nodes,
     * use {@link #waitForIndexQueryable(Cluster, String, String, int)} instead.
     *
     * @param cluster the cluster
     * @param keyspace the keyspace name
     * @param index the index name
     */
    public static void waitForIndexQueryableOnFirstNode(Cluster cluster, String keyspace, String index)
    {
        waitForIndexQueryable(cluster, keyspace, index, 1);
    }

    /**
     * Waits until all indexes in the given keyspace become queryable, as verified by the specified coordinator node.
     * This method checks index status from the coordinator's perspective, ensuring that when the coordinator
     * executes queries, it will see the indexes as queryable.
     * <p>
     * Use this method when your test will query from a specific node, to ensure that the coordinator node
     * has received gossip updates about index status.
     *
     * @param cluster the cluster
     * @param keyspace the keyspace name
     * @param coordinatorNode the node number (1-based) that will act as coordinator for queries
     */
    public static void waitForIndexQueryable(Cluster cluster, String keyspace, int coordinatorNode)
    {
        assertGossipEnabled(cluster);
        final List<String> indexes = getIndexes(cluster, keyspace);
        await().atMost(INDEX_BUILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)
               .untilAsserted(() -> assertIndexesQueryable(cluster, keyspace, indexes, coordinatorNode));
    }

    /**
     * Waits until the given index becomes queryable, as verified by the specified coordinator node.
     *
     * @param cluster the cluster
     * @param keyspace the keyspace name
     * @param index the index name
     * @param coordinatorNode the node number (1-based) that will act as coordinator for queries
     */
    public static void waitForIndexQueryable(Cluster cluster, String keyspace, String index, int coordinatorNode)
    {
        assertGossipEnabled(cluster);
        await().atMost(INDEX_BUILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)
               .untilAsserted(() -> assertIndexQueryable(cluster, keyspace, index, coordinatorNode));
    }

    /**
     * Waits until all indexes in the given keyspace become queryable on ALL cluster nodes.
     * This is useful when tests query from multiple coordinators, ensuring that each node
     * has received gossip updates about index status before queries are executed.
     *
     * @param cluster the cluster
     * @param keyspace the keyspace name
     */
    public static void waitForIndexQueryableOnAllNodes(Cluster cluster, String keyspace)
    {
        assertGossipEnabled(cluster);
        final List<String> indexes = getIndexes(cluster, keyspace);

        for (int nodeNum = 1; nodeNum <= cluster.size(); nodeNum++)
        {
            final int node = nodeNum;
            await().atMost(INDEX_BUILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                   .untilAsserted(() -> assertIndexesQueryable(cluster, keyspace, indexes, node));
        }
    }

    /**
     * Returns whether the index is queryable on the node, as its {@link SecondaryIndexManager} sees it.
     */
    public static boolean isIndexQueryable(IInvokableInstance node, String keyspace, String table, String index)
    {
        return node.callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            return cfs.indexManager.isIndexQueryable(cfs.indexManager.getIndexByName(index));
        });
    }

    /**
     * Waits up to 10 seconds for the index to become queryable on the node, without failing if it doesn't, for tests
     * that check that an index doesn't become queryable too early.
     */
    public static void awaitIndexQueryableQuietly(IInvokableInstance node, String keyspace, String table, String index)
    {
        try
        {
            await().atMost(10, TimeUnit.SECONDS).until(() -> isIndexQueryable(node, keyspace, table, index));
        }
        catch (ConditionTimeoutException e)
        {
            // the index is still not queryable
        }
    }

    /**
     * Returns the number of SSTables in the view of the SAI index on the node.
     */
    public static int indexedSSTables(IInvokableInstance node, String keyspace, String table, String index)
    {
        return node.callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            return ((StorageAttachedIndex) cfs.indexManager.getIndexByName(index)).getIndexContext().getView().getIndexes().size();
        });
    }

    /**
     * Waits until all the SSTables of the table are repaired on all the nodes: incremental repair finalizes its
     * sessions asynchronously, so they can still be pending after {@code nodetool repair} returns.
     */
    public static void waitForRepairedSSTables(Cluster cluster, String keyspace, String table)
    {
        await().atMost(INDEX_BUILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)
               .until(() -> cluster.stream().allMatch(node -> node.callOnInstance(() -> {
                   Set<SSTableReader> sstables = Keyspace.open(keyspace).getColumnFamilyStore(table).getLiveSSTables();
                   return !sstables.isEmpty() && sstables.stream().allMatch(SSTableReader::isRepaired);
               })));
    }

    private static void assertGossipEnabled(Cluster cluster)
    {
        cluster.stream().forEach(node -> {
            assert node.config().has(Feature.NETWORK) : "Network not enabled on this cluster";
            assert node.config().has(Feature.GOSSIP) : "Gossip not enabled on this cluster";
        });
    }

    /**
     * Checks if index is known to be queryable, by pulling index state from {@link SecondaryIndexManager}
     * on the specified coordinator node. Requires gossip.
     *
     * @param coordinatorNode the node number (1-based) that checks index status
     */
    public static void assertIndexQueryable(Cluster cluster, String keyspace, String index, int coordinatorNode)
    {
        assertIndexesQueryable(cluster, keyspace, Collections.singleton(index), coordinatorNode);
    }

    /**
     * Checks if all indexes are known to be queryable, by pulling index state from {@link SecondaryIndexManager}
     * on the specified coordinator node. Requires gossip.
     *
     * @param coordinatorNode the node number (1-based) that checks index status from its perspective
     */
    private static void assertIndexesQueryable(Cluster cluster, String keyspace, final Iterable<String> indexes, int coordinatorNode)
    {
        IInvokableInstance localNode = cluster.get(coordinatorNode);
        final List<InetAddressAndPort> nodes =
            cluster.stream()
                   .map(node -> nodeAddress(node.broadcastAddress()))
                   .collect(Collectors.toList());

        localNode.runOnInstance(() -> {
            for (String index : indexes)
            {
                for (InetAddressAndPort node : nodes)
                {
                    Index.Status status = IndexStatusManager.instance.getIndexStatus(node, keyspace, index);
                    assert status == Index.Status.BUILD_SUCCEEDED
                        : "Index " + index + " not queryable on node " + node + " (status = " + status + ')';
                }
            }
        });
    }

    private static InetAddressAndPort nodeAddress(InetSocketAddress address)
    {
        return InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
    }

    /**
     * Returns names of the indexes in the keyspace, found on the first node of the cluster.
     */
    public static List<String> getIndexes(Cluster cluster, String keyspace)
    {
        waitForSchemaAgreement(cluster);
        String query = String.format("SELECT index_name FROM system_views.%s WHERE keyspace_name = '%s' ALLOW FILTERING",
                                     ColumnIndexesSystemView.NAME, keyspace);
        SimpleQueryResult result = cluster.get(1).executeInternalWithResult(query);
        return Streams.stream(result)
                      .map(row -> (String) row.get("index_name"))
                      .collect(Collectors.toList());
    }

    public static void waitForSchemaAgreement(Cluster cluster)
    {
        await().atMost(INDEX_BUILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)
               .until(() -> schemaAgrees(cluster));
    }

    /**
     * Returns true if schema agrees on all nodes of the cluster
     */
    public static boolean schemaAgrees(Cluster cluster)
    {
        Set<UUID> versions = cluster.stream()
                                    .map(IInstance::schemaVersion)
                                    .collect(Collectors.toSet());
        return versions.size() == 1;
    }

    /**
     * Checks if an index is known to have failed its build, by pulling index state from {@link SecondaryIndexManager}.
     * Requires gossip.
     */
    public static void assertIndexBuildFailed(IInvokableInstance coordinator, IInvokableInstance replica, String keyspace, String index)
    {
        InetAddressAndPort nodeAddress = nodeAddress(replica.broadcastAddress());

        coordinator.runsOnInstance(() -> {
            Index.Status status = IndexStatusManager.instance.getIndexStatus(nodeAddress, keyspace, index);
            assert status == Index.Status.BUILD_FAILED
                : "Index " + index + " not failed on node " + nodeAddress + " (status = " + status + ')';
        });
    }

    /**
     * Returns an instance initializer that makes the given nodes speak the given messaging version. A node pinned to
     * messaging version 13 isn't taken for an Apache Cassandra 5.0 node (see {@link #ignoreApachePeers}).
     */
    public static BiConsumer<ClassLoader, Integer> pinMessagingVersion(int version, int... nodes)
    {
        return (classLoader, node) -> {
            if (Arrays.stream(nodes).anyMatch(n -> n == node))
            {
                new ByteBuddy().rebase(MessagingService.class)
                               .method(named("currentVersion"))
                               .intercept(FixedValue.value(version))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
            if (version == MessagingService.VERSION_50)
                ignoreApachePeers(classLoader, node);
        };
    }

    /**
     * An instance initializer that keeps the Apache upgrade path from taking the nodes at messaging version 13 of this
     * build for Apache Cassandra 5.0 nodes, which would make the node check schema changes against Apache Cassandra 5.0
     * and use its schema columns. Its schema handling is tested with Apache nodes (see
     * {@code ApacheMixedClusterTestBase}).
     */
    public static void ignoreApachePeers(ClassLoader classLoader, int node)
    {
        new ByteBuddy().rebase(ApacheUpgradePath.class)
                       .method(named("decide"))
                       .intercept(StubMethod.INSTANCE)
                       .make()
                       .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
    }

    public static List<String> toStrings(Object[][] rows)
    {
        return toStrings(Arrays.asList(rows).iterator());
    }

    public static List<String> toStrings(Iterator<Object[]> rows)
    {
        List<String> result = new ArrayList<>();
        while (rows.hasNext())
            result.add(Arrays.toString(rows.next()));
        return result;
    }

    /**
     * Adds a mismatch if the query doesn't return the expected rows; an error counts as the row "ERROR" and the error.
     */
    public static void check(String query, List<String> expected, Supplier<List<String>> actual,
                             List<String> mismatches)
    {
        List<String> rows;
        try
        {
            rows = actual.get();
        }
        catch (Throwable t)
        {
            rows = List.of("ERROR " + t);
        }
        if (!expected.equals(rows))
            mismatches.add(query + ": expected " + expected + " but got " + rows);
    }

    /**
     * Whether the given node makes the row filter of the given SELECT strict at the given consistency level.
     */
    public static boolean isStrict(Cluster cluster, int node, String query, ConsistencyLevel consistency)
    {
        String level = consistency.name();
        return cluster.get(node).callOnInstance(() -> {
            ClientState state = ClientState.forInternalCalls();
            SelectStatement select = (SelectStatement) QueryProcessor.getStatement(query, state);
            org.apache.cassandra.db.ConsistencyLevel cl = org.apache.cassandra.db.ConsistencyLevel.valueOf(level);
            QueryOptions options = QueryOptions.forInternalCalls(cl, Collections.emptyList());
            return select.getRowFilter(options, state).isStrict();
        });
    }

    /**
     * Returns the addresses of the replicas of the given key of a table with an int partition key, as node 1 sees them.
     */
    public static Set<String> endpoints(Cluster cluster, String keyspace, String table, int key)
    {
        Set<String> endpoints = new TreeSet<>();
        String[] lines = cluster.get(1).nodetoolResult("getendpoints", keyspace, table, String.valueOf(key))
                                .getStdout().trim().split("\\s+");
        for (String endpoint : lines)
            endpoints.add(endpoint.replaceFirst("^/", "").replaceFirst(":\\d+$", ""));
        return endpoints;
    }
}
