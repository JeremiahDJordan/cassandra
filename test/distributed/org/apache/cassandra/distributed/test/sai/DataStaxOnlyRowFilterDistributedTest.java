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

import org.junit.After;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.StubMethod;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ClientRequestsMetricsProvider;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundConnections;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.awaitility.Awaitility.await;

/**
 * Tests that queries using parts of {@link RowFilter} that only exist in DataStax's Cassandra (see
 * {@link RowFilter#dataStaxOnlyFeature()}) are rejected while the query can contact nodes on a messaging version below
 * DS 10, such as Apache Cassandra 5.0 nodes, which would fail to deserialize them or drop them, and that the queries
 * Apache Cassandra supports still work.
 */
public class DataStaxOnlyRowFilterDistributedTest extends TestBaseImpl
{
    private static final int NUM_REPLICAS = 2;
    private static final int RF = 2;

    /** Queries using DataStax-only operators, disjunctions and expressions, with the feature named by the error. */
    private static final List<String[]> DATASTAX_ONLY_QUERIES = Arrays.asList(
        new String[]{ "SELECT k FROM %s.t WHERE s NOT CONTAINS 1 ALLOW FILTERING", "The NOT CONTAINS operator" },
        new String[]{ "SELECT k FROM %s.t WHERE v : 'apple'", "The : operator (and = on an analyzed index)" },
        new String[]{ "SELECT k FROM %s.t WHERE v = 'apple'", "The : operator (and = on an analyzed index)" },
        // the index of u has equals_behaviour_when_analyzed: unsupported, so it can't serve = in place of :
        new String[]{ "SELECT k FROM %s.t WHERE u : 'APPLE'", "The : operator (and = on an analyzed index)" },
        new String[]{ "SELECT k FROM %s.t WHERE v IN ('apple')", "The : operator (and = on an analyzed index)" },
        // an IN on an analyzed index is the disjunction of the : of its values
        new String[]{ "SELECT k FROM %s.t WHERE v IN ('apple', 'pear')", "The OR operator" },
        new String[]{ "SELECT k FROM %s.t WHERE a IN ('APPLE', 'pear')", "The OR operator" },
        new String[]{ "SELECT k FROM %s.t WHERE m['a'] > 0", "The > operator on a map entry" },
        new String[]{ "SELECT k FROM %s.t WHERE n = 1 OR n = 2", "The OR operator" },
        new String[]{ "SELECT k FROM %s.t WHERE GEO_DISTANCE(vec, [1, 1]) < 1000", "GEO_DISTANCE" },
        new String[]{ "SELECT k FROM %s.t ORDER BY n ASC LIMIT 3", "ORDER BY on a non-clustering column" },
        new String[]{ "SELECT k FROM %s.t ORDER BY v BM25 OF 'apple' LIMIT 3", "ORDER BY BM25" },
        // the SAI filtering of Apache Cassandra 5.0 nodes ignores !=, which NOT IN is sent as
        new String[]{ "SELECT k FROM %s.t WHERE n >= 0 AND n != 1 ALLOW FILTERING", RowFilter.SAI_EXCLUSION },
        new String[]{ "SELECT k FROM %s.t WHERE a = 'APPLE' AND n NOT IN (1, 2) ALLOW FILTERING", RowFilter.SAI_EXCLUSION },
        // the SAI filtering of Apache Cassandra 5.0 nodes compares the whole frozen collection with the element
        new String[]{ "SELECT k FROM %s.t WHERE n = 1 AND fs CONTAINS 1 ALLOW FILTERING", RowFilter.FROZEN_COLLECTION_ELEMENT_RESTRICTION });

    /** A query whose IN the SAI of Apache Cassandra 5.0 nodes refuses at a consistency level that needs reconciliation. */
    private static final String NON_STRICT_IN_QUERY = "SELECT k FROM %s.t WHERE n = 1 AND u IN ('Apple', 'pear') ALLOW FILTERING";

    /** Queries that Apache Cassandra 5.0 nodes can deserialize, with the number of rows they find. */
    private static final Map<String, Integer> APACHE_QUERIES = new LinkedHashMap<>();
    static
    {
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE n = 1", 1);
        // NOT IN is sent as != expressions
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE n NOT IN (1, 2) ALLOW FILTERING", 1);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE n > 0 AND n < 2", 1);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE n != 2 ALLOW FILTERING", 2);
        // a read of a single row doesn't use the index
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE k = 1 AND n >= 0 AND n != 1 ALLOW FILTERING", 0);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE s CONTAINS 1 ALLOW FILTERING", 3);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE fs CONTAINS 1 ALLOW FILTERING", 3);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE n = 1 AND fs = {1, 2} ALLOW FILTERING", 1);
        // sent as the analyzed equality it is on a non-tokenizing index
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE a = 'APPLE'", 3);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE a : 'APPLE'", 3);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE a = 'pear'", 0);
        // the index of u can't serve IN, which is filtered on the raw values
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE u IN ('Apple', 'pear') ALLOW FILTERING", 3);
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE m['a'] = 1", 3);
        APACHE_QUERIES.put("SELECT k FROM %s.t ORDER BY vec ANN OF [1, 1] LIMIT 1", 1);
        // top-k reads are read at ONE, which needs no reconciliation
        APACHE_QUERIES.put("SELECT k FROM %s.t WHERE m['a'] = 1 AND n IN (0, 1) ORDER BY vec ANN OF [1, 1] LIMIT 1", 1);
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
    public void testAllDS20() throws Throwable
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
            assertRowCounts(cluster, Collections.singletonMap(NON_STRICT_IN_QUERY, 1));
        }
    }

    /**
     * A node on the Apache Cassandra 5.0 messaging version rejects the DataStax-only parts of the filter.
     */
    @Test
    public void testAllApache50Messaging() throws Throwable
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_50);

        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer(SAIUtil::ignoreApachePeers)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            createSchema(cluster);

            assertRejected(cluster, DATASTAX_ONLY_QUERIES);
            assertRowCounts(cluster, APACHE_QUERIES);
            assertNonStrictInRejected(cluster);
        }
    }

    /**
     * With one node on the Apache Cassandra 5.0 messaging version, every coordinator rejects the DataStax-only parts
     * of the filter, and still accepts the queries that node can deserialize.
     */
    @Test
    public void testMixedApache50AndDS20() throws Throwable
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
            assertNonStrictInRejected(cluster);
        }
    }

    /**
     * After a rolling upgrade from the Apache Cassandra 5.0 messaging version, every coordinator accepts OR and LIKE
     * without waiting for its connections to the other nodes to connect again on the new version.
     */
    @Test
    public void testRollingUpgradeFromApache50Messaging() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer((classLoader, threadGroup, node, generation) -> {
                                               if (generation == 0)
                                                   SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 1, 2).accept(classLoader, node);
                                           })
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            createSchema(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.large (k int PRIMARY KEY, b blob)"));
            String orQuery = "SELECT k FROM %s.t WHERE n = 1 OR n = 2";
            String likeQuery = "SELECT k FROM %s.t WHERE a LIKE 'app%%' ALLOW FILTERING";

            restart(cluster, 1);
            // a write large enough for the large message connection, which connects on the Apache Cassandra version
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.large (k, b) VALUES (0, ?)"),
                                           ConsistencyLevel.ALL, ByteBuffer.allocate(100_000));
            assertRejected(cluster, Collections.singletonList(new String[]{ orQuery, "The OR operator" }));
            for (int i = 1; i <= cluster.size(); i++)
            {
                ICoordinator coordinator = cluster.coordinator(i);
                Assertions.assertThatThrownBy(() -> coordinator.execute(withKeyspace(likeQuery), ConsistencyLevel.ALL))
                          .hasMessageContaining(LikeFilteringDistributedTest.REJECTION);
            }

            restart(cluster, 2);
            assertAccepted(cluster, orQuery);
            assertAccepted(cluster, likeQuery);
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
                                                                       .set("read_request_timeout", "60s")
                                                                       .set("range_request_timeout", "60s"))
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
                                              "SELECT k FROM %s.t WHERE m['a'] > 0",
                                              "SELECT k FROM %s.t WHERE n >= 0 AND n != 1 ALLOW FILTERING",
                                              "SELECT k FROM %s.t WHERE n = 1 AND fs CONTAINS 1 ALLOW FILTERING",
                                              NON_STRICT_IN_QUERY))
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
                                                             MessagingService.VERSION_50).getResult()).isNotEmpty();
        }
    }

    /**
     * A coordinator that doesn't know yet that a replica is below DS 10 makes a filter of several regular columns
     * strict; the serialization for that replica refuses it, and the coordinator reads again with a filter that
     * replicas can relax, which it builds for the reads of the keyspace for a second.
     */
    @Test
    public void testStrictFilterRefusedByNodeBelowDS10() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withInstanceInitializer(BB::installHidingPeersBelowDS10)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                                       .set("paxos_variant", "v2")
                                                                       .set("read_request_timeout", "60s")
                                                                       .set("range_request_timeout", "60s"))
                                           .start(), RF))
        {
            createSchema(cluster);
            String query = "SELECT k FROM %s.t WHERE n = 1 AND a = 'APPLE'";

            // node 1 is below DS 10, so it lets the replicas relax the filter
            Assertions.assertThat(cluster.coordinator(1).execute(withKeyspace(query), ConsistencyLevel.ALL).length).isEqualTo(1);

            cluster.get(2).runOnInstance(() -> MessagingService.instance().outboundSink.clear());
            String singlePartitionQuery = "SELECT k FROM %s.t WHERE k = 1 AND n = 1 AND a = 'APPLE'";
            for (String strictQuery : Arrays.asList(query, singlePartitionQuery))
            {
                for (ConsistencyLevel consistency : Arrays.asList(ConsistencyLevel.ALL, ConsistencyLevel.SERIAL))
                {
                    for (boolean paged : new boolean[]{ false, true })
                    {
                        if (consistency == ConsistencyLevel.SERIAL && strictQuery.equals(query))
                            continue;

                        String what = strictQuery + " at " + consistency + (paged ? ", paged" : "");
                        waitForMarkExpiry();
                        Assertions.assertThat(SAIUtil.isStrict(cluster, 2, withKeyspace(strictQuery),
                                                               ConsistencyLevel.ALL)).as(what).isTrue();

                        long refusals = refusals(cluster);
                        long paxosReadFailures = paxosReadFailures(cluster);
                        Assertions.assertThat(execute(cluster, strictQuery, consistency, paged)).as(what).containsExactly(1);
                        Assertions.assertThat(refusals(cluster)).as(what).isGreaterThan(refusals);
                        // at SERIAL, the prepare carries the read
                        if (consistency == ConsistencyLevel.SERIAL)
                            Assertions.assertThat(paxosReadFailures(cluster)).as(what).isGreaterThan(paxosReadFailures);

                        refusals = refusals(cluster);
                        Assertions.assertThat(SAIUtil.isStrict(cluster, 2, withKeyspace(strictQuery),
                                                               ConsistencyLevel.ALL)).as(what).isFalse();
                        Assertions.assertThat(execute(cluster, strictQuery, consistency, paged)).as(what).containsExactly(1);
                        Assertions.assertThat(refusals(cluster)).as(what).isEqualTo(refusals);
                    }
                }
            }

            Assertions.assertThat(cluster.get(2).logs().grep("Cannot serialize a row filter for messaging version " +
                                                             MessagingService.VERSION_50 + ": nodes on that version don't apply it strictly")
                                         .getResult()).isNotEmpty();

            // the read of a single row doesn't use the index, but the read again with a filter that replicas can relax
            // does, and its filter is checked again
            waitForMarkExpiry();
            String notEqualQuery = "SELECT k FROM %s.t WHERE k = 1 AND n = 1 AND a = 'APPLE' AND u != 'x' ALLOW FILTERING";
            Assertions.assertThatThrownBy(() -> execute(cluster, notEqualQuery, ConsistencyLevel.ALL, false))
                      .hasMessageContaining(String.format(RowFilter.DATASTAX_ONLY_FEATURE_ERROR, RowFilter.SAI_EXCLUSION));
        }
    }

    /**
     * Aggregates and GROUP BY keep their results when a node below DS 10 refuses a strict filter after the coordinator
     * has processed rows: with RF 2 on 3 nodes and node 3 below DS 10, the range read of node 1 gets the rows of the
     * first token range from nodes 1 and 2 before node 3 refuses the next one.
     */
    @Test
    public void testAggregatesReadAgainAfterRefusal() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withInstanceInitializer(BB::installThirdNodeBelowDS10)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                                       .set("read_request_timeout", "60s")
                                                                       .set("range_request_timeout", "60s"))
                                           .start(), RF))
        {
            int partitions = 30;
            createGroups(cluster, partitions);

            String where = " FROM %s.groups WHERE n >= 0 AND a = 'APPLE'";
            List<String> groups = new ArrayList<>();
            for (int k = 0; k < partitions; k++)
                groups.add("[" + k + ", 3]");
            Map<String, List<String>> queries = new LinkedHashMap<>();
            queries.put("SELECT count(*)" + where, Collections.singletonList("[90]"));
            queries.put("SELECT sum(n)" + where, Collections.singletonList("[4005]"));
            queries.put("SELECT k, count(*)" + where + " GROUP BY k", groups);
            for (Map.Entry<String, List<String>> query : queries.entrySet())
            {
                for (int pageSize : new int[]{ 0, 7, 5000 })
                {
                    String what = query.getKey() + (pageSize > 0 ? ", paged by " + pageSize : "");
                    waitForMarkExpiry();
                    long refusals = refusals(cluster, 1, 3);
                    List<String> rows = new ArrayList<>();
                    if (pageSize == 0)
                    {
                        SimpleQueryResult result = cluster.coordinator(1).executeWithResult(withKeyspace(query.getKey()), ConsistencyLevel.ALL);
                        for (Object[] row : result.toObjectArrays())
                            rows.add(Arrays.toString(row));
                        Assertions.assertThat(result.warnings()).as(what).containsOnlyOnce("Aggregation query used without partition key");
                    }
                    else
                    {
                        cluster.coordinator(1).executeWithPaging(withKeyspace(query.getKey()), ConsistencyLevel.ALL, pageSize)
                               .forEachRemaining(row -> rows.add(Arrays.toString(row)));
                    }
                    Assertions.assertThat(rows).as(what).containsExactlyInAnyOrderElementsOf(query.getValue());
                    Assertions.assertThat(refusals(cluster, 1, 3)).as(what).isGreaterThan(refusals);
                }
            }
        }
    }

    /**
     * When node 3, below DS 10, refuses a read, the client gets only the read size warnings of the read that follows,
     * one per token range, each from both replicas.
     */
    @Test
    public void testReadSizeWarningsOfRefusedRead() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withInstanceInitializer(BB::installThirdNodeBelowDS10)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                                       .set("read_request_timeout", "60s")
                                                                       .set("range_request_timeout", "60s")
                                                                       .set("read_thresholds_enabled", true)
                                                                       .set("local_read_size_warn_threshold", "1KiB"))
                                           .start(), RF))
        {
            createGroups(cluster, 30);
            for (String query : Arrays.asList("SELECT count(*) FROM %s.groups WHERE n >= 0 AND a = 'APPLE'",
                                              "SELECT k FROM %s.groups WHERE n >= 0 AND a = 'APPLE'"))
            {
                waitForMarkExpiry();
                long refusals = refusals(cluster, 1, 3);
                SimpleQueryResult result = cluster.coordinator(1).executeWithResult(withKeyspace(query), ConsistencyLevel.ALL);
                Assertions.assertThat(refusals(cluster, 1, 3)).as(query).isGreaterThan(refusals);

                List<String> ranges = new ArrayList<>();
                for (String warning : result.warnings())
                {
                    if (!warning.contains("local read size warnings"))
                        continue;
                    Assertions.assertThat(warning).as(query).startsWith("2 nodes loaded");
                    ranges.add(warning.substring(warning.indexOf("with token range: ")));
                }
                Assertions.assertThat(ranges).as(query).isNotEmpty().doesNotHaveDuplicates();
            }
        }
    }

    private static void createGroups(Cluster cluster, int partitions)
    {
        String tableId = UUID.nameUUIDFromBytes((KEYSPACE + ".groups").getBytes(StandardCharsets.UTF_8)).toString();
        for (int node = 1; node <= cluster.size(); node++)
        {
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE TABLE %s.groups (k int, c int, n int, a text, PRIMARY KEY (k, c)) WITH ID = " + tableId));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX n_idx ON %s.groups(n) USING 'StorageAttachedIndex'"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX a_idx ON %s.groups(a) USING 'StorageAttachedIndex' " +
                                                                "WITH OPTIONS = { 'case_sensitive': 'false' }"));
        }
        for (int node = 1; node <= cluster.size(); node++)
            for (String index : Arrays.asList("n_idx", "a_idx"))
                SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, index, node);

        for (int k = 0; k < partitions; k++)
            for (int c = 0; c < 3; c++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.groups (k, c, n, a) VALUES (?, ?, ?, 'Apple')"), ConsistencyLevel.ALL, k, c, k * 3 + c);
        cluster.get(1).runOnInstance(() -> MessagingService.instance().outboundSink.clear());
    }

    private static List<Integer> execute(Cluster cluster, String query, ConsistencyLevel consistency, boolean paged)
    {
        List<Integer> keys = new ArrayList<>();
        if (paged)
            cluster.coordinator(2).executeWithPaging(withKeyspace(query), consistency, 1).forEachRemaining(row -> keys.add((Integer) row[0]));
        else
            for (Object[] row : cluster.coordinator(2).execute(withKeyspace(query), consistency))
                keys.add((Integer) row[0]);
        return keys;
    }

    /** Waits past the mark of the last refusal, so that the coordinator makes filters strict again. */
    private static void waitForMarkExpiry() throws InterruptedException
    {
        TimeUnit.MILLISECONDS.sleep(1100);
    }

    private static long paxosReadFailures(Cluster cluster)
    {
        return cluster.get(2).callOnInstance(() -> ClientRequestsMetricsProvider.instance.metrics(KEYSPACE).casReadMetrics.failures.getCount());
    }

    /**
     * The messages that node 2 couldn't serialize for node 1.
     */
    private static long refusals(Cluster cluster)
    {
        return refusals(cluster, 2, 1);
    }

    /**
     * The messages that a coordinator couldn't serialize for a replica.
     */
    private static long refusals(Cluster cluster, int coordinator, int replica)
    {
        InetSocketAddress address = cluster.get(replica).broadcastAddress();
        return cluster.get(coordinator).callOnInstance(() -> {
            InetAddressAndPort node = InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
            OutboundConnections connections = MessagingService.instance().channelManagers.get(node);
            return connections == null ? 0L : connections.small.errorCount() + connections.large.errorCount() + connections.urgent.errorCount();
        });
    }

    /**
     * Restarts the given node, without its pinned messaging version, and waits until it and the other nodes see each
     * other alive with their versions, and its indexes are queryable.
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
        for (String index : Arrays.asList("n_idx", "v_idx", "a_idx", "u_idx", "m_idx", "vec_idx"))
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, index, node);
    }

    /** Waits until the given observer sees the given node alive, with its messaging version capped to the observer's. */
    private static void awaitKnown(IInvokableInstance observer, IInvokableInstance node)
    {
        InetSocketAddress address = node.broadcastAddress();
        int version = node.callOnInstance(() -> MessagingService.current_version);
        await().atMost(1, TimeUnit.MINUTES).until(() -> observer.callOnInstance(() -> {
            InetAddressAndPort endpoint = InetAddressAndPort.getByAddress(address);
            Integer known = MessagingService.instance().versions.getRawIfKnown(endpoint);
            int current = MessagingService.current_version;
            return known != null && Math.min(known, current) == Math.min(version, current) && FailureDetector.instance.isAlive(endpoint);
        }));
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
                                                                "(k int PRIMARY KEY, n int, s set<int>, fs frozen<set<int>>, v text, a text, " +
                                                                "u text, m map<text, int>, vec vector<float, 2>) " +
                                                                "WITH ID = " + tableId));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS n_idx ON %s.t(n) USING 'StorageAttachedIndex'"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS v_idx ON %s.t(v) USING 'StorageAttachedIndex' " +
                                                                "WITH OPTIONS = { 'index_analyzer': 'standard' }"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS a_idx ON %s.t(a) USING 'StorageAttachedIndex' " +
                                                                "WITH OPTIONS = { 'case_sensitive': 'false' }"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS u_idx ON %s.t(u) USING 'StorageAttachedIndex' " +
                                                                "WITH OPTIONS = { 'case_sensitive': 'false', 'equals_behaviour_when_analyzed': 'unsupported' }"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS m_idx ON %s.t(entries(m)) USING 'StorageAttachedIndex'"));
            cluster.get(node).schemaChangeInternal(withKeyspace("CREATE CUSTOM INDEX IF NOT EXISTS vec_idx ON %s.t(vec) USING 'StorageAttachedIndex' " +
                                                                "WITH OPTIONS = { 'similarity_function': 'euclidean' }"));
        }
        // the local schema changes don't reach schema agreement, so wait for each index on each node without it
        for (int node = 1; node <= cluster.size(); node++)
            for (String index : Arrays.asList("n_idx", "v_idx", "a_idx", "u_idx", "m_idx", "vec_idx"))
                SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, index, node);

        for (int k = 0; k < 3; k++)
        {
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, n, s, fs, v, a, u, m, vec) " +
                                                        "VALUES (?, ?, {1, 2}, {1, 2}, 'apple pie', 'Apple', 'Apple', {'a': 1}, [1, 1])"),
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

    /**
     * The IN of {@link #NON_STRICT_IN_QUERY} is rejected at ALL, and accepted at ONE, which needs no reconciliation.
     */
    private static void assertNonStrictInRejected(Cluster cluster)
    {
        String feature = String.format(RowFilter.SAI_NON_STRICT_OPERATOR, "IN");
        assertRejected(cluster, Collections.singletonList(new String[]{ NON_STRICT_IN_QUERY, feature }), ConsistencyLevel.ALL);
        for (int i = 1; i <= cluster.size(); i++)
        {
            Object[][] rows = cluster.coordinator(i).execute(withKeyspace(NON_STRICT_IN_QUERY), ConsistencyLevel.ONE);
            Assertions.assertThat(rows.length).as("node" + i).isEqualTo(1);
        }
    }

    private static void assertRejected(Cluster cluster, List<String[]> queries)
    {
        assertRejected(cluster, queries, ConsistencyLevel.ONE);
    }

    private static void assertRejected(Cluster cluster, List<String[]> queries, ConsistencyLevel consistency)
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
                    coordinator.execute(withKeyspace(query[0]), consistency);
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
     * Injection to set the current messaging version of the first cluster node to the Apache Cassandra 5.0 one.
     */
    public static class BB
    {
        public static void install(ClassLoader classLoader, int node)
        {
            SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 1).accept(classLoader, node);
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

        /**
         * Same as {@link #install}, and hides node 1 from the checks of node 2, as before node 2 learns its version.
         */
        public static void installHidingPeersBelowDS10(ClassLoader classLoader, int node)
        {
            install(classLoader, node);
            if (node == 2)
            {
                new ByteBuddy().rebase(RowFilter.class)
                               .method(named("computeMayHavePeersBelowDS10"))
                               .intercept(FixedValue.value(false))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        /**
         * Pins node 3 to the Apache Cassandra 5.0 messaging version, and hides it from the checks of node 1.
         */
        public static void installThirdNodeBelowDS10(ClassLoader classLoader, int node)
        {
            SAIUtil.pinMessagingVersion(MessagingService.VERSION_50, 3).accept(classLoader, node);
            if (node == 1)
            {
                new ByteBuddy().rebase(RowFilter.class)
                               .method(named("computeMayHavePeersBelowDS10"))
                               .intercept(FixedValue.value(false))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }
    }
}
