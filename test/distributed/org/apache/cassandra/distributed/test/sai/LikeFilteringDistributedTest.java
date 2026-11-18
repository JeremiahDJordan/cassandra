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
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.VersionGenerator;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.net.MessagingService.VERSION_50;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_20;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Tests that LIKE restrictions, filtered (CASSANDRA-17198) or served by an index, are only accepted when every replica
 * runs a DataStax version with the same LIKE semantics. An Apache Cassandra 5.0 replica, whose SAI doesn't serve LIKE,
 * scans the rows and evaluates a wildcard-free pattern and the patterns with inner wildcards as a plain "contains":
 * the data below would make it return the wrong rows.
 */
public class LikeFilteringDistributedTest extends TestBaseImpl
{
    static final String REJECTION = "isn't supported while the query can contact nodes on a messaging version " +
                                    "below DS 21";

    /**
     * The queries and the keys they select with the DataStax semantics. With the Apache Cassandra 5.0 semantics, the
     * first one would also return 2 ('xabcx' contains 'abc'), and the others would only return 3 (the value containing
     * the literal 'a%c').
     */
    private static final String[] QUERIES = { "SELECT k FROM %s.t WHERE %s LIKE 'abc'",
                                              "SELECT k FROM %s.t WHERE %s LIKE 'a%%c'",
                                              "SELECT k FROM %s.t WHERE i = 0 AND %s LIKE 'a%%c'" };
    private static final Integer[][] EXPECTED = { { 1 }, { 1, 3, 4 }, { 1, 3 } };

    @Test
    public void testLikeWithAllDataStaxNodes() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);

            for (int i = 1; i <= cluster.size(); i++)
            {
                ICoordinator coordinator = cluster.coordinator(i);
                for (int q = 0; q < QUERIES.length; q++)
                {
                    // u has no index: filtering
                    assertThat(keys(coordinator, QUERIES[q], "u", " ALLOW FILTERING")).containsExactly(EXPECTED[q]);
                    // v has an SAI index serving LIKE
                    assertThat(keys(coordinator, QUERIES[q], "v", "")).containsExactly(EXPECTED[q]);
                }
            }
        }
    }

    @Test
    public void testLikeWithApacheNode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(SAIUtil.pinMessagingVersion(VERSION_50, 1))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            assertRejected(cluster, 1, 2);
        }
    }

    /**
     * A DataStax 5.0 node before 5.0.10, or a node of this release pinned to its messaging version, DS 20.
     */
    @Test
    public void testLikeWithOlderDataStaxNode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(SAIUtil.pinMessagingVersion(VERSION_DS_20, 1))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            assertRejected(cluster, 1, 2);
        }
    }

    /**
     * Only the messaging version tells what a node supports: a node on DS 21 that gossips the release version of an
     * earlier DataStax 5.0 build doesn't block LIKE.
     */
    @Test
    public void testLikeWithOlderReleaseVersion() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(OlderReleaseVersion::install)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);
            for (int i = 1; i <= cluster.size(); i++)
                for (int q = 0; q < QUERIES.length; q++)
                    assertThat(keys(cluster.coordinator(i), QUERIES[q], "v", "")).containsExactly(EXPECTED[q]);
        }
    }

    /**
     * After a full cluster restart with a node still down, the node's messaging version, kept in memory, is unknown:
     * LIKE isn't blocked by the down node.
     */
    @Test
    public void testLikeAfterFullRestartWithNodeDown() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            createSchema(cluster);

            cluster.get(2).shutdown().get();
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();

            // node 1 knows the release version of node 2 from system.peers (Nodes), but not its messaging version
            InetSocketAddress down = cluster.get(2).broadcastAddress();
            byte[] address = down.getAddress().getAddress();
            int port = down.getPort();
            String state = cluster.get(1).callOnInstance(() -> {
                InetAddressAndPort node;
                try
                {
                    node = InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(address), port);
                }
                catch (UnknownHostException e)
                {
                    throw new AssertionError(e);
                }
                return Nodes.getReleaseVersion(node, null) + " " + Gossiper.instance.getReleaseVersion(node) + ' ' +
                       MessagingService.instance().versions.knows(node) + ' ' + IFailureDetector.instance.isAlive(node);
            });
            // gossip itself has an empty state for the saved endpoint
            assertThat(state).isEqualTo(new CassandraVersion(FBUtilities.getReleaseVersionString()) + " null false false");

            // wait for the indexes with a query that isn't a LIKE
            ICoordinator coordinator = cluster.coordinator(1);
            await().atMost(2, TimeUnit.MINUTES)
                   .ignoreExceptions()
                   .until(() -> coordinator.execute(withKeyspace("SELECT k FROM %s.t WHERE i = 0"), ConsistencyLevel.ONE).length == 2);

            for (int q = 0; q < QUERIES.length; q++)
            {
                assertThat(keys(coordinator, QUERIES[q], "u", " ALLOW FILTERING", ConsistencyLevel.ONE)).containsExactly(EXPECTED[q]);
                assertThat(keys(coordinator, QUERIES[q], "v", "", ConsistencyLevel.ONE)).containsExactly(EXPECTED[q]);
            }
        }
    }

    private static void assertRejected(Cluster cluster, int... coordinators)
    {
        for (int i : coordinators)
        {
            ICoordinator coordinator = cluster.coordinator(i);
            for (String query : QUERIES)
            {
                assertThatThrownBy(() -> keys(coordinator, query, "u", " ALLOW FILTERING")).hasMessageContaining(REJECTION);
                assertThatThrownBy(() -> keys(coordinator, query, "v", "")).hasMessageContaining(REJECTION);
            }
        }
    }

    private static List<Integer> keys(ICoordinator coordinator, String query, String column, String suffix)
    {
        return keys(coordinator, query, column, suffix, ConsistencyLevel.ALL);
    }

    static List<Integer> keys(ICoordinator coordinator, String query, String column, String suffix, ConsistencyLevel cl)
    {
        Object[][] rows = coordinator.execute(String.format(query, KEYSPACE, column) + suffix, cl);
        return Arrays.stream(rows).map(row -> (Integer) row[0]).sorted().collect(Collectors.toList());
    }

    private static void createSchema(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v text, u text, i int)"));
        cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(v) USING 'sai'"));
        cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(i) USING 'sai'"));
        SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);

        String[] values = { "abc", "xabcx", "a%c", "axxc" };
        int[] is = { 0, 1, 0, 1 };
        for (int k = 1; k <= values.length; k++)
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v, u, i) VALUES (?, ?, ?, ?)"), ConsistencyLevel.ALL,
                                           k, values[k - 1], values[k - 1], is[k - 1]);
    }

    /**
     * Makes the first node gossip the release version of a DataStax 5.0 build older than LIKE filtering.
     */
    public static class OlderReleaseVersion
    {
        public static void install(ClassLoader classLoader, int node)
        {
            if (node == 1)
            {
                new ByteBuddy().rebase(VersionedValue.VersionedValueFactory.class)
                               .method(named("releaseVersion").and(takesArguments(0)))
                               .intercept(MethodDelegation.to(OlderReleaseVersion.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static VersionedValue releaseVersion()
        {
            return VersionedValue.unsafeMakeVersionedValue("5.0.7.0", VersionGenerator.getNextVersion());
        }
    }
}
