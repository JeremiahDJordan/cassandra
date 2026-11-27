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
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Filtering reads that need reconciliation and restrict a static column and a regular column get, from each replica,
 * the partitions without matching rows whose static row satisfies a static restriction, as their static row alone,
 * counted in the replica's limits (see {@link ReadCommand#returnsStaticCandidates()}). Node 1 is in its own
 * datacenter, and nodes 2 and 3 in another.
 */
public class StaticCandidatesTest extends TestBaseImpl
{
    /** One replica per node */
    private static final String ALL_NODES = "all_nodes";
    /** Node 1 is the only replica in its datacenter */
    private static final String ONE_PER_DC = "one_per_dc";

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = Cluster.build(3)
                         .withNodeIdTopology(networkTopology(3, node -> dcAndRack(node == 1 ? "dc1" : "dc2", "rack1")))
                         .withConfig(config -> config.set("hinted_handoff_enabled", false).with(GOSSIP).with(NETWORK))
                         .withInstanceInitializer(BBHelper::install)
                         .start();
        CLUSTER.schemaChange("CREATE KEYSPACE " + ALL_NODES + " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 2}");
        CLUSTER.schemaChange("CREATE KEYSPACE " + ONE_PER_DC + " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}");
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testReplicaCountsStaticCandidatesInItsLimits()
    {
        String table = ALL_NODES + ".counted";
        CLUSTER.schemaChange("CREATE TABLE " + table + " (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'");
        for (int pk = 0; pk < 20; pk++)
            CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (pk, ck, s, v) VALUES (?, 1, 5, 0)", ALL, pk);

        String select = "SELECT pk FROM " + table + " WHERE s = 5 AND v = 1 LIMIT 2 ALLOW FILTERING";
        assertEquals(2, partitionsReadOnNode1(select));
        assertRows(CLUSTER.coordinator(1).execute(select, ALL));
    }

    @Test
    public void testReplicaSkipsStaticCandidatesOfOtherPartitionKeys()
    {
        String table = ALL_NODES + ".partition_key";
        CLUSTER.schemaChange("CREATE TABLE " + table + " (pk0 int, pk1 int, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'");
        for (int pk = 0; pk < 20; pk++)
            CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (pk0, pk1, ck, s, v) VALUES (?, 1, 1, 5, 0)", ALL, pk);

        String select = "SELECT pk0 FROM " + table + " WHERE pk1 = 2 AND s = 5 AND v = 1 LIMIT 2 ALLOW FILTERING";
        assertEquals(0, partitionsReadOnNode1(select));
    }

    @Test
    public void testStaticColumnsNewerOnDifferentReplicas()
    {
        String table = ALL_NODES + ".two_statics";
        CLUSTER.schemaChange("CREATE TABLE " + table + " (pk int, ck int, s int static, sx int static, v int, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'");
        CLUSTER.get(1).executeInternal("INSERT INTO " + table + " (pk, s, sx) VALUES (0, 0, 5) USING TIMESTAMP 2");
        CLUSTER.get(2).executeInternal("INSERT INTO " + table + " (pk, ck, s, sx, v) VALUES (0, 1, 5, 0, 1) USING TIMESTAMP 1");
        CLUSTER.get(2).executeInternal("UPDATE " + table + " USING TIMESTAMP 3 SET s = 5 WHERE pk = 0");

        String select = "SELECT pk, ck FROM " + table + " WHERE s = 5 AND sx = 5 AND v = 1 ALLOW FILTERING";
        assertRows(CLUSTER.coordinator(1).execute(select, ALL), row(0, 1));
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ALL, 1), row(0, 1));
    }

    @Test
    public void testShortReadProtectionGrowsItsRequests()
    {
        String table = ALL_NODES + ".grows";
        CLUSTER.schemaChange("CREATE TABLE " + table + " (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'");
        for (int pk = 0; pk < 1000; pk++)
            CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (pk, ck, s, v) VALUES (?, 1, 5, 0)", ALL, pk);

        long before = shortReadProtectionRequestsOnNode1("grows");
        assertRows(CLUSTER.coordinator(1).execute("SELECT pk FROM " + table + " WHERE s = 5 AND v = 1 LIMIT 1 ALLOW FILTERING", ALL));
        long requests = shortReadProtectionRequestsOnNode1("grows") - before;
        assertTrue("short read protection requests: " + requests, requests < 100);
    }

    @Test
    public void testSingleReplicaReadsPastStaticCandidates()
    {
        String table = ONE_PER_DC + ".single_replica";
        CLUSTER.schemaChange("CREATE TABLE " + table + " (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'");
        for (int pk = 0; pk < 20; pk++)
            CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (pk, ck, s, v) VALUES (?, 1, 5, ?)", ALL, pk, pk == 7 ? 1 : 0);

        String select = "SELECT pk, ck FROM " + table + " WHERE s = 5 AND v = 1 LIMIT 1 ALLOW FILTERING";
        assertRows(CLUSTER.coordinator(1).execute(select, LOCAL_QUORUM), row(7, 1));
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, LOCAL_QUORUM, 1), row(7, 1));
    }

    @Test
    public void testCoordinatorWithoutStaticCandidates()
    {
        String table = ALL_NODES + ".older_coordinator";
        CLUSTER.schemaChange("CREATE TABLE " + table + " (pk int, ck int, s int static, v0 int, v1 int, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'");

        // Every node has the static value. Row 1 only matches with the columns of nodes 1 and 2, and row 2 is on
        // node 3 alone.
        for (int node = 1; node <= 3; node++)
            CLUSTER.get(node).executeInternal("INSERT INTO " + table + " (pk, s) VALUES (0, 1) USING TIMESTAMP 1");
        CLUSTER.get(1).executeInternal("INSERT INTO " + table + " (pk, ck, v0) VALUES (0, 1, 1) USING TIMESTAMP 1");
        CLUSTER.get(2).executeInternal("INSERT INTO " + table + " (pk, ck, v1) VALUES (0, 1, 1) USING TIMESTAMP 1");
        CLUSTER.get(3).executeInternal("INSERT INTO " + table + " (pk, ck, v0, v1) VALUES (0, 2, 1, 1) USING TIMESTAMP 1");

        // Node 1 runs a release without static candidates: it doesn't ask the other replicas for them.
        CLUSTER.get(1).runOnInstance(() -> BBHelper.withoutStaticCandidates.set(true));
        try
        {
            String select = "SELECT pk, ck FROM " + table + " WHERE s = 1 AND v0 = 1 AND v1 = 1 ALLOW FILTERING";
            assertRows(CLUSTER.coordinator(1).execute(select, ALL), row(0, 1), row(0, 2));
        }
        finally
        {
            CLUSTER.get(1).runOnInstance(() -> BBHelper.withoutStaticCandidates.set(false));
        }
    }

    private static long shortReadProtectionRequestsOnNode1(String table)
    {
        return CLUSTER.get(1).callOnInstance(() -> Keyspace.open(ALL_NODES).getColumnFamilyStore(table).metric.shortReadProtectionRequests.getCount());
    }

    @Test
    public void testSerialReadsWithPaxosV2()
    {
        // The static value is on node 1 alone, the row on nodes 2 and 3. SERIAL reads with Paxos v2 read without static
        // candidates from every coordinator, also when they complete an earlier round that only node 1 accepted.
        CLUSTER.forEach(instance -> instance.runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v2)));
        try
        {
            String table = paxosTable("paxos_v2");
            for (int node = 1; node <= 3; node++)
                assertRows(CLUSTER.coordinator(node).execute(paxosSelect(table), SERIAL));

            for (int node : new int[]{ 1, 3 })
            {
                table = paxosTable("paxos_v2_incomplete_" + node);
                CLUSTER.filters().verbs(Verb.PAXOS2_PROPOSE_REQ.id).to(2, 3).drop();
                try
                {
                    CLUSTER.coordinator(1).execute("INSERT INTO " + table + " (pk, ck, v) VALUES (0, 9, 0) IF NOT EXISTS", QUORUM);
                    fail("The proposal reached a quorum");
                }
                catch (RuntimeException e)
                {
                    // nodes 2 and 3 don't get the proposal
                }
                CLUSTER.filters().reset();
                CLUSTER.filters().allVerbs().to(2).drop();
                CLUSTER.filters().allVerbs().from(2).drop();
                assertRows(CLUSTER.coordinator(node).execute(paxosSelect(table), SERIAL));
                CLUSTER.filters().reset();
            }
        }
        finally
        {
            CLUSTER.filters().reset();
            CLUSTER.forEach(instance -> instance.runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v1)));
        }
    }

    private static String paxosTable(String name)
    {
        String table = ALL_NODES + '.' + name;
        CLUSTER.schemaChange("CREATE TABLE " + table + " (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'");
        CLUSTER.get(1).executeInternal("INSERT INTO " + table + " (pk, s) VALUES (0, 5) USING TIMESTAMP 1");
        for (int node = 2; node <= 3; node++)
            CLUSTER.get(node).executeInternal("INSERT INTO " + table + " (pk, ck, v) VALUES (0, 1, 1) USING TIMESTAMP 1");
        return table;
    }

    private static String paxosSelect(String table)
    {
        return "SELECT pk, ck FROM " + table + " WHERE pk = 0 AND s = 5 AND v = 1 ALLOW FILTERING";
    }

    /**
     * The number of partitions that node 1 returns as a replica for the given query at ALL.
     */
    private static int partitionsReadOnNode1(String select)
    {
        return CLUSTER.get(1).callOnInstance(() -> {
            SelectStatement statement = (SelectStatement) QueryProcessor.getStatement(select, ClientState.forInternalCalls());
            QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.ALL, Collections.emptyList());
            ReadCommand command = (ReadCommand) statement.getQuery(options, FBUtilities.nowInSeconds());
            int count = 0;
            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator iterator = command.executeLocally(controller))
            {
                while (iterator.hasNext())
                {
                    iterator.next().close();
                    count++;
                }
            }
            return count;
        });
    }

    /**
     * Lets a node act as a release without static candidates.
     */
    public static class BBHelper
    {
        static final AtomicBoolean withoutStaticCandidates = new AtomicBoolean();

        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, Integer node)
        {
            new ByteBuddy().rebase(ReadCommand.class)
                           .method(named("returnsStaticCandidates"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static boolean returnsStaticCandidates(@SuperCall Callable<Boolean> zuperCall) throws Exception
        {
            return !withoutStaticCandidates.get() && zuperCall.call();
        }
    }
}
