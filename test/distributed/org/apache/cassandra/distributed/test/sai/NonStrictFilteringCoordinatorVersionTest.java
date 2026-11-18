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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Replicas relax the SAI filtering of reads that need reconciliation and intersect several mutable columns
 * (CASSANDRA-19018) only for coordinators that send the custom message parameter
 * {@link ReadCommand#NON_STRICT_FILTERING_PARAM}, which this version adds to its non-strict reads. Replicas filter the
 * reads of other coordinators strictly, whatever their versions: earlier DataStax releases send non-strict row filters
 * without the parameter, in single-partition reads that return a replica's rows unfiltered when the digests of the
 * replica responses match.
 * <p>
 * The coordinator of these tests isn't a replica of the queried partition, so all the replica reads are remote. The
 * tests emulate other coordinators by making the coordinator omit the parameter, and set the release and messaging
 * versions that the replicas see for it.
 */
public class NonStrictFilteringCoordinatorVersionTest extends TestBaseImpl
{
    private static final String RF2_KEYSPACE = "rf2_keyspace";
    private static final String TABLE = RF2_KEYSPACE + ".t";

    /** Release versions of DataStax nodes before and after 5.0.10 */
    private static final String[] DATASTAX_VERSIONS = { "5.0.7.0", "5.0.10.0-SNAPSHOT", "5.0.10.0" };

    private static final String[] RELEASE_VERSIONS = { "5.0.7.0", "5.0.10.0-SNAPSHOT", "5.0.10.0", "5.0.4", "5.0-rc1",
                                                       "5.1.0", null };

    /** Messaging versions of DataStax 5.0 nodes, and of Apache Cassandra 5.0 nodes or DataStax nodes pinned to it */
    private static final int[] MESSAGING_VERSIONS = { MessagingService.current_version, MessagingService.VERSION_50 };

    /**
     * A non-strict filter whose index restrictions are nested in a disjunction, which partition-restricted queries
     * can read as single-partition reads of several partitions with "IN".
     */
    private static final String NESTED = "((a = 1 AND b = 2) OR (a = 7 AND b = 7))";

    private static Cluster CLUSTER;
    private static int coordinator;
    private static final List<Integer> replicas = new ArrayList<>();

    /** A partition with the same replicas as partition 0, which has a row split across them */
    private static int splitKey;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(3)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false).with(GOSSIP).with(NETWORK))
                              .withInstanceInitializer(BBHelper::install)
                              .start());
        CLUSTER.schemaChange("CREATE KEYSPACE " + RF2_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
        CLUSTER.schemaChange("CREATE TABLE " + TABLE + " (k int, c int, a int, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'");
        CLUSTER.schemaChange("CREATE INDEX ON " + TABLE + "(a) USING 'sai'");
        CLUSTER.schemaChange("CREATE INDEX ON " + TABLE + "(b) USING 'sai'");
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, RF2_KEYSPACE);

        // find the replicas of partition 0, and another partition with the same replicas
        Set<String> endpoints = SAIUtil.endpoints(CLUSTER, RF2_KEYSPACE, "t", 0);
        splitKey = 1;
        while (!SAIUtil.endpoints(CLUSTER, RF2_KEYSPACE, "t", splitKey).equals(endpoints))
            splitKey++;
        for (int node = 1; node <= CLUSTER.size(); node++)
        {
            if (endpoints.contains(CLUSTER.get(node).broadcastAddress().getAddress().getHostAddress()))
                replicas.add(node);
            else
                coordinator = node;
        }
        assertThat(replicas).hasSize(2);
        assertThat(coordinator).isPositive();

        // rows on all the replicas, only (0, 1) matches both restrictions
        CLUSTER.coordinator(1).execute("INSERT INTO " + TABLE + " (k, c, a, b) VALUES (0, 0, 1, 5)", org.apache.cassandra.distributed.api.ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute("INSERT INTO " + TABLE + " (k, c, a, b) VALUES (0, 1, 1, 2)", org.apache.cassandra.distributed.api.ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute("INSERT INTO " + TABLE + " (k, c, a, b) VALUES (0, 2, 3, 2)", org.apache.cassandra.distributed.api.ConsistencyLevel.ALL);

        // a row split across the replicas, which only matches both restrictions once reconciled
        CLUSTER.get(replicas.get(0)).executeInternal("INSERT INTO " + TABLE + " (k, c, a) VALUES (?, 0, 1) USING TIMESTAMP 1", splitKey);
        CLUSTER.get(replicas.get(1)).executeInternal("INSERT INTO " + TABLE + " (k, c, b) VALUES (?, 0, 2) USING TIMESTAMP 2", splitKey);
    }

    @Before
    public void enableNonStrictFiltering()
    {
        SAIUtil.setNonStrictReplicaFiltering(CLUSTER, true);
        omitParameter(false);
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /**
     * Replicas relax their filtering for coordinators that send the parameter, whatever their versions, and the
     * coordinator finds the split row.
     */
    @Test
    public void testCoordinatorsSendingTheParameter()
    {
        forEachCoordinatorMessagingVersion(() -> {
            for (String version : RELEASE_VERSIONS)
            {
                setCoordinatorVersion(version);
                assertRelaxed();
            }
        });
    }

    /**
     * Replicas filter strictly for coordinators that don't send the parameter, whatever their versions: DataStax
     * releases before 5.0.10, and Apache Cassandra releases, which filter the reconciled rows but don't ask. Those
     * coordinators can't find the split row.
     */
    @Test
    public void testCoordinatorsOmittingTheParameter()
    {
        omitParameter(true);
        forEachCoordinatorMessagingVersion(() -> {
            for (String version : RELEASE_VERSIONS)
            {
                setCoordinatorVersion(version);
                assertStrict();
            }
        });
    }

    /**
     * A coordinator with non-strict replica filtering disabled reads these queries as single-partition reads that
     * compare digests, so its replicas must filter strictly whatever versions they see for it.
     */
    @Test
    public void testStrictCoordinator()
    {
        SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(coordinator), false);
        forEachCoordinatorMessagingVersion(() -> {
            for (String version : new String[]{ "5.0.4", "5.0.10", "5.0.10.0" })
            {
                setCoordinatorVersion(version);
                for (String consistency : new String[]{ "QUORUM", "ALL" })
                {
                    assertRows(execute("SELECT k, c FROM " + TABLE + " WHERE k = 0 AND a = 1 AND b = 2", consistency),
                               row(0, 1));
                    assertRows(execute("SELECT k, c FROM " + TABLE + " WHERE k = 0 AND " + NESTED, consistency),
                               row(0, 1));
                    assertRows(execute("SELECT k, c FROM " + TABLE + " WHERE k IN (0, " + splitKey + ") AND " + NESTED,
                                       consistency),
                               row(0, 1));
                }
            }
        });
    }

    /**
     * Older DataStax coordinators read partition-restricted queries as single-partition reads, with the non-strict row
     * filter, and without the parameter. When the digests of the replica responses match, they return a replica's rows
     * without filtering them, so the replicas must filter strictly.
     */
    @Test
    public void testOlderDataStaxCoordinatorSinglePartitionRead()
    {
        omitParameter(true);
        for (String version : DATASTAX_VERSIONS)
        {
            setCoordinatorVersion(version);
            for (String consistency : new String[]{ "QUORUM", "ALL" })
            {
                assertThat(executeAsOlderCoordinator("SELECT * FROM " + TABLE + " WHERE k = 0 AND a = 1 AND b = 2", consistency)).containsExactly("0:1");
                assertThat(executeAsOlderCoordinator("SELECT * FROM " + TABLE + " WHERE k = 0 AND " + NESTED, consistency)).containsExactly("0:1");
            }
        }
    }

    /**
     * The setting of the coordinator decides, whatever the setting on the replicas.
     */
    @Test
    public void testNonStrictFilteringSettings()
    {
        for (boolean onReplicas : new boolean[]{ false, true })
        {
            for (int replica : replicas)
                SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(replica), onReplicas);

            SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(coordinator), false);
            assertStrict();

            SAIUtil.setNonStrictReplicaFiltering(CLUSTER.get(coordinator), true);
            assertRelaxed();
        }
    }

    private static void assertRelaxed()
    {
        assertRows(execute("SELECT k, c FROM " + TABLE + " WHERE k = " + splitKey + " AND a = 1 AND b = 2"), row(splitKey, 0));
        assertThat(keys(execute("SELECT k, c FROM " + TABLE + " WHERE a = 1 AND b = 2"))).containsExactlyInAnyOrder(splitKey + ":0", "0:1");
        assertRows(execute("SELECT k, c FROM " + TABLE + " WHERE k = 0 AND a = 1 AND b = 2"), row(0, 1));
        assertRows(execute("SELECT k, c FROM " + TABLE + " WHERE k = " + splitKey + " AND " + NESTED), row(splitKey, 0));
        assertThat(keys(execute("SELECT k, c FROM " + TABLE + " WHERE k IN (0, " + splitKey + ") AND " + NESTED))).containsExactlyInAnyOrder(splitKey + ":0", "0:1");
    }

    private static void assertStrict()
    {
        assertRows(execute("SELECT k, c FROM " + TABLE + " WHERE k = " + splitKey + " AND a = 1 AND b = 2"));
        assertRows(execute("SELECT k, c FROM " + TABLE + " WHERE a = 1 AND b = 2"), row(0, 1));
        assertRows(execute("SELECT k, c FROM " + TABLE + " WHERE k = 0 AND a = 1 AND b = 2"), row(0, 1));
        assertRows(execute("SELECT k, c FROM " + TABLE + " WHERE k = " + splitKey + " AND " + NESTED));
        assertRows(execute("SELECT k, c FROM " + TABLE + " WHERE k IN (0, " + splitKey + ") AND " + NESTED), row(0, 1));
    }

    /** Makes the coordinator omit the parameter from its non-strict reads, as DataStax releases before 5.0.10 do. */
    private static void omitParameter(boolean omit)
    {
        CLUSTER.get(coordinator).runOnInstance(() -> BBHelper.omitParameter.set(omit));
    }

    private static List<String> keys(Object[][] rows)
    {
        List<String> keys = new ArrayList<>();
        for (Object[] row : rows)
            keys.add(row[0] + ":" + row[1]);
        return keys;
    }

    private static Object[][] execute(String query)
    {
        return execute(query, "ALL");
    }

    private static Object[][] execute(String query, String consistency)
    {
        return CLUSTER.coordinator(coordinator).execute(query, org.apache.cassandra.distributed.api.ConsistencyLevel.valueOf(consistency));
    }

    /** Runs the given assertions with each of {@link #MESSAGING_VERSIONS} as the coordinator's messaging version. */
    private static void forEachCoordinatorMessagingVersion(Runnable assertions)
    {
        for (int messagingVersion : MESSAGING_VERSIONS)
        {
            setCoordinatorMessagingVersion(messagingVersion);
            try
            {
                assertions.run();
            }
            finally
            {
                setCoordinatorMessagingVersion(MessagingService.current_version);
            }
        }
    }

    private static void setCoordinatorMessagingVersion(int version)
    {
        InetSocketAddress address = CLUSTER.get(coordinator).broadcastAddress();
        String host = address.getAddress().getHostAddress();
        int port = address.getPort();
        for (int replica : replicas)
        {
            CLUSTER.get(replica).runOnInstance(() -> {
                try
                {
                    MessagingService.instance().versions.set(InetAddressAndPort.getByNameOverrideDefaults(host, port), version);
                }
                catch (UnknownHostException e)
                {
                    throw new AssertionError(e);
                }
            });
        }
    }

    /** Sets the release version the replicas see for the coordinator, or makes it unknown if it's {@code null}. */
    private static void setCoordinatorVersion(@Nullable String version)
    {
        InetSocketAddress address = CLUSTER.get(coordinator).broadcastAddress();
        String host = address.getAddress().getHostAddress();
        int port = address.getPort();
        for (int replica : replicas)
        {
            CLUSTER.get(replica).runOnInstance(() -> {
                InetAddressAndPort peer;
                try
                {
                    peer = InetAddressAndPort.getByNameOverrideDefaults(host, port);
                }
                catch (UnknownHostException e)
                {
                    throw new AssertionError(e);
                }
                CassandraVersion releaseVersion = version == null ? null : new CassandraVersion(version);
                Nodes.peers().update(peer, p -> p.setReleaseVersion(releaseVersion), true);
                assertThat(Nodes.getReleaseVersion(peer, null)).isEqualTo(releaseVersion);
            });
        }
    }

    /**
     * Executes the given partition-restricted query on the coordinator as DataStax releases before 5.0.10 do: as a
     * single-partition read with the non-strict row filter, which compares the digests of the replica responses.
     *
     * @return the clusterings of the returned rows, as "k:c"
     */
    private static List<String> executeAsOlderCoordinator(String query, String consistency)
    {
        return CLUSTER.get(coordinator).callOnInstance(() -> {
            ConsistencyLevel cl = ConsistencyLevel.valueOf(consistency);
            SelectStatement statement = (SelectStatement) QueryProcessor.prepareInternal(query).statement;
            QueryOptions options = QueryOptions.forInternalCalls(cl, Collections.emptyList());
            long nowInSec = FBUtilities.nowInSeconds();

            RowFilter rowFilter = statement.getRowFilter(options, ClientState.forInternalCalls());
            assertThat(rowFilter.isStrict()).isFalse();

            PartitionRangeReadCommand range = (PartitionRangeReadCommand) statement.getQuery(options, nowInSec);
            DecoratedKey key = (DecoratedKey) range.dataRange().startKey();
            SinglePartitionReadCommand single = SinglePartitionReadCommand.create(range.metadata(),
                                                                                  nowInSec,
                                                                                  range.columnFilter(),
                                                                                  rowFilter,
                                                                                  range.limits(),
                                                                                  key,
                                                                                  range.clusteringIndexFilter(key));
            List<String> rows = new ArrayList<>();
            try (PartitionIterator partitions = StorageProxy.read(SinglePartitionReadCommand.Group.one(single),
                                                                  cl,
                                                                  ClientState.forInternalCalls(),
                                                                  Dispatcher.RequestTime.forImmediateExecution()))
            {
                while (partitions.hasNext())
                {
                    try (RowIterator partition = partitions.next())
                    {
                        while (partition.hasNext())
                        {
                            Row row = partition.next();
                            rows.add(partition.partitionKey().getKey().getInt(0) + ":" + row.clustering().bufferAt(0).getInt(0));
                        }
                    }
                }
            }
            return rows;
        });
    }

    /**
     * Lets the coordinator omit {@link ReadCommand#NON_STRICT_FILTERING_PARAM} from its read messages.
     */
    public static class BBHelper
    {
        static final AtomicBoolean omitParameter = new AtomicBoolean();

        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, Integer node)
        {
            new ByteBuddy().rebase(ReadCommand.class)
                           .method(named("withNonStrictFilteringParam"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static Message<ReadCommand> withNonStrictFilteringParam(Message<ReadCommand> message,
                                                                       @SuperCall Callable<Message<ReadCommand>> zuperCall) throws Exception
        {
            return omitParameter.get() ? message : zuperCall.call();
        }
    }
}
