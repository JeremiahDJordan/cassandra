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
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.memory.TrieMemoryIndex;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.ReplicaWarnings;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.paxos.PaxosCommit;
import org.apache.cassandra.service.paxos.PaxosCommitAndPrepare;
import org.apache.cassandra.service.paxos.PaxosPrepareRefresh;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.ConditionAsConsumer;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_FAIL_MV_LOCKS_COUNT;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.index.sai.NotIndexedValues.NGRAM_ANALYZER;
import static org.apache.cassandra.index.sai.NotIndexedValues.NGRAM_VALUE;
import static org.apache.cassandra.index.sai.NotIndexedValues.letters;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that the client warnings that replicas raise while applying a write, LWT commits included, reach the client,
 * here those of SAI values that aren't indexed. In-JVM dtests have no authentication, so the coordinator doesn't check
 * the values.
 * <p>
 * The cluster has two datacenters: nodes 1 and 2 in datacenter1, nodes 3 and 4 in datacenter2.
 */
public class ReplicaWriteWarningsTest extends TestBaseImpl
{
    private static final String KS_ONE = "ks_one";
    private static final String KS_THREE = "ks_three";
    private static final String KS_FOUR = "ks_four";

    private static final AtomicInteger seq = new AtomicInteger();

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        // the first view write of each node fails to get its lock and is retried
        try (WithProperties properties = new WithProperties().set(TEST_FAIL_MV_LOCKS_COUNT, 1))
        {
            cluster = Cluster.build(4)
                             .withDCs(2)
                             .withConfig(config -> config.with(GOSSIP, NETWORK)
                                                         .set("materialized_views_enabled", true)
                                                         .set("hinted_handoff_enabled", false))
                             .withInstanceInitializer(BBHelper::install)
                             .start();
        }

        cluster.schemaChange("CREATE KEYSPACE " + KS_ONE + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}");
        cluster.schemaChange("CREATE KEYSPACE " + KS_THREE + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2': 1}");
        cluster.schemaChange("CREATE KEYSPACE " + KS_FOUR + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2': 2}");
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @After
    public void after()
    {
        cluster.filters().reset();
        cluster.forEach(node -> node.runOnInstance(() -> {
            BBHelper.warnings = null;
            BBHelper.timeOutCommitWaitUntilReplyFrom = null;
            BBHelper.timedOutThread = null;
            BBHelper.timedOutWaitFor = null;
            BBHelper.commitRepliesFrom.clear();
        }));
    }

    @Test
    public void testRemoteAndLocalReplica()
    {
        String table = createTable(KS_ONE);
        List<Integer> keys = keys(KS_ONE, table, 2, 2);
        String warning = notIndexedWarning(KS_ONE, table);

        // through a coordinator that isn't a replica
        assertThat(write(1, KS_ONE, table, keys.get(0), NGRAM_VALUE, ConsistencyLevel.ALL)).containsExactly(warning);

        // through the replica, which gives its warnings to the response handler
        int localHookCalls = cluster.get(2).callOnInstance(() -> BBHelper.localHookCalls.get());
        assertThat(write(2, KS_ONE, table, keys.get(1), NGRAM_VALUE, ConsistencyLevel.ALL)).containsExactly(warning);
        assertEquals(localHookCalls + 1, (int) cluster.get(2).callOnInstance(() -> BBHelper.localHookCalls.get()));

        for (int key : keys)
            assertEquals(1, cluster.get(2).executeInternal(String.format("SELECT k FROM %s.%s WHERE k = ?", KS_ONE, table), key).length);
        assertEquals(0, cluster.coordinator(1).execute(String.format("SELECT k FROM %s.%s WHERE v : 'ab'", KS_ONE, table), ConsistencyLevel.ALL).length);
    }

    @Test
    public void testSeveralReplicas()
    {
        String table = createTable(KS_THREE);
        String warning = notIndexedWarning(KS_THREE, table);

        assertThat(write(1, KS_THREE, table, 0, NGRAM_VALUE, ConsistencyLevel.ALL)).containsExactly(warning);
        assertThat(write(4, KS_THREE, table, 1, NGRAM_VALUE, ConsistencyLevel.ALL)).containsExactly(warning);
    }

    @Test
    public void testNoWarning()
    {
        String table = createTable(KS_THREE);

        assertThat(write(1, KS_THREE, table, 0, "abc", ConsistencyLevel.ALL)).isEmpty();

        // a write after one that warned
        assertThat(write(1, KS_THREE, table, 1, NGRAM_VALUE, ConsistencyLevel.ALL)).hasSize(1);
        assertThat(write(1, KS_THREE, table, 2, "abc", ConsistencyLevel.ALL)).isEmpty();
    }

    @Test
    public void testBatches()
    {
        String table1 = createTable(KS_ONE);
        String table2 = createTable(KS_ONE);
        List<Integer> keysOn1 = keys(KS_ONE, table1, 2, 1);
        List<Integer> keysOn2 = keys(KS_ONE, table1, 4, 2);
        String warning1 = notIndexedWarning(KS_ONE, table1);
        String warning2 = notIndexedWarning(KS_ONE, table2);

        int i = 0;
        for (String type : new String[]{ "", "UNLOGGED " })
        {
            // two partitions of one index, both remote
            String batch = "BEGIN " + type + "BATCH " +
                           insert(KS_ONE, table1, keysOn2.get(2 * i)) +
                           insert(KS_ONE, table1, keysOn2.get(2 * i + 1)) +
                           "APPLY BATCH";
            assertThat(execute(1, batch, ConsistencyLevel.ALL)).as("BEGIN " + type + "BATCH").containsExactly(warning1);

            // a partition of each table, one local and one remote
            batch = "BEGIN " + type + "BATCH " +
                    insert(KS_ONE, table1, keysOn1.get(i)) +
                    insert(KS_ONE, table2, keysOn2.get(i)) +
                    "APPLY BATCH";
            assertThat(execute(1, batch, ConsistencyLevel.ALL)).as("BEGIN " + type + "BATCH").containsExactlyInAnyOrder(warning1, warning2);
            i++;
        }
    }

    @Test
    public void testConsistencyLevel()
    {
        String table = createTable(KS_FOUR);
        cluster.get(3).runOnInstance(() -> BBHelper.warnings = List.of("datacenter2"));
        cluster.get(4).runOnInstance(() -> BBHelper.warnings = List.of("datacenter2"));

        assertThat(write(1, KS_FOUR, table, 0, "abc", ConsistencyLevel.ALL)).containsExactly("datacenter2");

        // the datacenter1 replica answers once a datacenter2 reply was processed, which doesn't count
        int remoteHookCalls = cluster.get(1).callOnInstance(() -> BBHelper.remoteDcHookCalls.get());
        hold(Verb.MUTATION_RSP, 2, 1, () -> cluster.get(1).callOnInstance(() -> BBHelper.remoteDcHookCalls.get()) > remoteHookCalls);
        assertThat(write(1, KS_FOUR, table, 1, "abc", ConsistencyLevel.LOCAL_QUORUM)).isEmpty();
    }

    @Test
    public void testLateReply()
    {
        String table = createTable(KS_THREE);
        List<Integer> keys = keys(KS_THREE, table, 2, 1, 2, 3);
        cluster.get(1).runOnInstance(() -> BBHelper.warnings = List.of("node1"));

        // node 1's reply completes the write
        CountDownLatch written = new CountDownLatch(1);
        hold(Verb.MUTATION_RSP, 2, 4, () -> written.getCount() == 0);
        hold(Verb.MUTATION_RSP, 3, 4, () -> written.getCount() == 0);
        assertThat(write(4, KS_THREE, table, keys.get(0), "abc", ConsistencyLevel.ONE)).containsExactly("node1");
        written.countDown();
        cluster.filters().reset();

        // node 1's reply comes after the write completed
        CountDownLatch written2 = new CountDownLatch(1);
        hold(Verb.MUTATION_RSP, 1, 4, () -> written2.getCount() == 0);
        assertThat(write(4, KS_THREE, table, keys.get(1), "abc", ConsistencyLevel.ONE)).isEmpty();
        written2.countDown();
    }

    @Test
    public void testWarningsOverSizeLimit()
    {
        String table = createTable(KS_ONE);
        int key = keys(KS_ONE, table, 1, 2).get(0);
        List<String> warnings = new ArrayList<>();
        for (int i = 0; i < ReplicaWarnings.MAX_WARNINGS; i++)
            warnings.add(String.format("%02d", i) + "x".repeat(ReplicaWarnings.MAX_WARNING_BYTES - 2));
        cluster.get(2).runOnInstance(() -> BBHelper.warnings = warnings);

        // as many as fit the size limit of the warnings of a response, which are 3 of the max size
        assertThat(write(1, KS_ONE, table, key, "abc", ConsistencyLevel.ALL)).containsExactlyElementsOf(warnings.subList(0, 3));
    }

    @Test
    public void testWarningsOverMaxMessageSize()
    {
        String table = createTable(KS_ONE);
        List<Integer> keys = keys(KS_ONE, table, 2, 2);
        List<String> warnings = new ArrayList<>();
        for (int i = 0; i < ReplicaWarnings.MAX_WARNINGS; i++)
            warnings.add(String.format("%02d", i) + "x".repeat(1018));

        int maxMessageSize = cluster.get(2).callOnInstance(DatabaseDescriptor::getInternodeMaxMessageSizeInBytes);
        cluster.get(2).runOnInstance(() -> DatabaseDescriptor.setInternodeMaxMessageSizeInBytes(16 * 1024));
        try
        {
            assertThat(write(1, KS_ONE, table, keys.get(0), "abc", ConsistencyLevel.ALL)).isEmpty();

            // 16 warnings of 1020 bytes fit the size limit of the warnings, but not a response of 16KiB at most
            cluster.get(2).runOnInstance(() -> BBHelper.warnings = warnings);
            List<String> received = write(1, KS_ONE, table, keys.get(1), "abc", ConsistencyLevel.ALL);
            assertThat(received).isNotEmpty().hasSizeLessThan(warnings.size());
            assertThat(received).isEqualTo(warnings.subList(0, received.size()));
        }
        finally
        {
            cluster.get(2).runOnInstance(() -> DatabaseDescriptor.setInternodeMaxMessageSizeInBytes(maxMessageSize));
        }
    }

    @Test
    public void testDeferredViewLockRetry()
    {
        String table = createTable(KS_ONE);
        cluster.schemaChange(String.format("CREATE MATERIALIZED VIEW %s.%s_view AS SELECT * FROM %s.%s " +
                                           "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)",
                                           KS_ONE, table, KS_ONE, table));
        int key = keys(KS_ONE, table, 1, 2).get(0);

        assertThat(write(1, KS_ONE, table, key, NGRAM_VALUE, ConsistencyLevel.ALL)).containsExactly(notIndexedWarning(KS_ONE, table));
        assertEquals(0, (int) cluster.get(2).callOnInstance(() -> {
            try
            {
                Field count = Keyspace.class.getDeclaredField("TEST_FAIL_MV_LOCKS_COUNT");
                count.setAccessible(true);
                return count.getInt(null);
            }
            catch (ReflectiveOperationException e)
            {
                throw new AssertionError(e);
            }
        }));
    }

    @Test
    public void testOversizedTerm()
    {
        String table = "t_" + seq.getAndIncrement();
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (k int PRIMARY KEY, v text)", KS_ONE, table));
        cluster.schemaChange(String.format("CREATE INDEX %s_idx ON %s.%s (v) USING 'sai'", table, KS_ONE, table));
        SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KS_ONE);
        int key = keys(KS_ONE, table, 1, 2).get(0);

        // over the default max term size of 8KiB
        assertThat(write(1, KS_ONE, table, key, "b".repeat(9000), ConsistencyLevel.ALL))
        .containsExactly(String.format(IndexContext.TERM_NOT_INDEXED_WARNING, "v", KS_ONE, table + "_idx"));
    }

    @Test
    public void testLwt()
    {
        for (String variant : new String[]{ "v1", "v2" })
        {
            setPaxosVariant(variant);
            String table = createTable(KS_ONE);
            List<Integer> keys = keys(KS_ONE, table, 2, 2);
            String warning = notIndexedWarning(KS_ONE, table);

            // through a coordinator that isn't a replica, then through the replica, whose commit gives its warnings to
            // the response handler
            for (int i = 0; i < 2; i++)
            {
                int coordinator = i + 1;
                int localHookCalls = cluster.get(2).callOnInstance(() -> BBHelper.localHookCalls.get());
                String insert = String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?) IF NOT EXISTS", KS_ONE, table);
                assertThat(cas(coordinator, insert, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL, keys.get(i), NGRAM_VALUE))
                .as(variant + " insert through node " + coordinator).containsExactly(warning);

                String update = String.format("UPDATE %s.%s SET v = ? WHERE k = ? IF EXISTS", KS_ONE, table);
                assertThat(cas(coordinator, update, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL, letters(512, 1), keys.get(i)))
                .as(variant + " update through node " + coordinator).containsExactly(warning);
                assertEquals(variant + " through node " + coordinator, localHookCalls + 2 * i,
                             (int) cluster.get(2).callOnInstance(() -> BBHelper.localHookCalls.get()));
            }
        }
    }

    @Test
    public void testLwtCommitTimeoutRace()
    {
        setPaxosVariant("v2");
        String table = createTable(KS_THREE);
        int key = keys(KS_THREE, table, 1, 1, 2, 3).get(0);
        cluster.get(1).runOnInstance(() -> BBHelper.warnings = List.of("node1"));

        // node 4's wait for the commit times out before node 1's reply, the last the commit needs, which is counted
        // before the outcome is read
        String node1 = host(1);
        String node2 = host(2);
        String node3 = host(3);
        int timeouts = cluster.get(4).callOnInstance(() -> BBHelper.commitWaitTimeouts.get());
        cluster.get(4).runOnInstance(() -> BBHelper.timeOutCommitWaitUntilReplyFrom = node1);
        hold(Verb.PAXOS_COMMIT_RSP, 1, 4, () -> cluster.get(4).callOnInstance(() -> BBHelper.commitWaitTimeouts.get() > timeouts
                                                                                    && BBHelper.commitRepliesFrom.contains(node2)
                                                                                    && BBHelper.commitRepliesFrom.contains(node3)));

        String insert = String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?) IF NOT EXISTS", KS_THREE, table);
        SimpleQueryResult result = cluster.coordinator(4).executeWithResult(insert, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL, key, "abc");
        assertThat(result.warnings()).containsExactly("node1");
        assertTrue(result.hasNext());
        assertEquals(true, result.next().get("[applied]"));
        assertEquals(timeouts + 1, (int) cluster.get(4).callOnInstance(() -> BBHelper.commitWaitTimeouts.get()));
    }

    @Test
    public void testLwtWarningsOverSizeLimit()
    {
        List<String> warnings = new ArrayList<>();
        for (int i = 0; i < ReplicaWarnings.MAX_WARNINGS; i++)
            warnings.add(String.format("%02d", i) + "x".repeat(ReplicaWarnings.MAX_WARNING_BYTES - 2));
        cluster.get(2).runOnInstance(() -> BBHelper.warnings = warnings);

        for (String variant : new String[]{ "v1", "v2" })
        {
            setPaxosVariant(variant);
            String table = createTable(KS_ONE);
            int key = keys(KS_ONE, table, 1, 2).get(0);
            String insert = String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?) IF NOT EXISTS", KS_ONE, table);
            assertThat(cas(1, insert, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL, key, "abc"))
            .as(variant).containsExactlyElementsOf(warnings.subList(0, 3));
        }
    }

    @Test
    public void testLwtRecoveredProposal()
    {
        for (String variant : new String[]{ "v1", "v2" })
        {
            setPaxosVariant(variant);
            String table = createTable(KS_THREE);
            int key = keys(KS_THREE, table, 1, 1, 2, 3).get(0);

            // client A's commit doesn't reach the replicas
            cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id, Verb.PAXOS2_COMMIT_REMOTE_REQ.id).from(4).to(1, 2, 3).drop();
            String insert = String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?) IF NOT EXISTS", KS_THREE, table);
            assertThatThrownBy(() -> cas(4, insert, ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, key, NGRAM_VALUE))
            .as(variant).satisfies(e -> assertThat(e.getClass().getSimpleName()).contains("Timeout"));
            cluster.filters().reset();
            String select = String.format("SELECT k FROM %s.%s WHERE k = ?", KS_THREE, table);
            for (int node = 1; node <= 3; node++)
                assertEquals(variant, 0, cluster.get(node).executeInternal(select, key).length);

            // client B's read finishes A's proposal, whose warnings aren't B's
            int captured = capturedNotIndexedWarnings();
            int discards = cluster.get(1).callOnInstance(() -> BBHelper.discardCalls.get());
            int commitAndPrepares = cluster.get(1).callOnInstance(() -> BBHelper.commitAndPrepareCalls.get());
            SimpleQueryResult result = cluster.coordinator(1).executeWithResult(select, ConsistencyLevel.SERIAL, key);
            assertThat(result.warnings()).as(variant).isEmpty();
            assertTrue(variant, result.hasNext());
            assertThat(capturedNotIndexedWarnings()).as(variant).isGreaterThan(captured);
            if (variant.equals("v1"))
                assertThat((int) cluster.get(1).callOnInstance(() -> BBHelper.discardCalls.get())).isGreaterThan(discards);
            else
                assertThat((int) cluster.get(1).callOnInstance(() -> BBHelper.commitAndPrepareCalls.get())).isGreaterThan(commitAndPrepares);
        }
    }

    @Test
    public void testLwtRefresh()
    {
        setPaxosVariant("v2");
        String table = createTable(KS_THREE);
        int key = keys(KS_THREE, table, 1, 1, 2, 3).get(0);

        // only node 2 gets the commit
        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).from(2).to(1, 3).drop();
        String insert = String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?) IF NOT EXISTS", KS_THREE, table);
        cas(2, insert, ConsistencyLevel.SERIAL, ConsistencyLevel.ONE, key, NGRAM_VALUE);
        cluster.filters().reset();

        // a CAS that applies nothing, whose prepare quorum is nodes 1 and 2, so node 1 refreshes itself
        cluster.filters().verbs(Verb.PAXOS2_PREPARE_RSP.id).from(3).to(1).drop();
        int refreshes = cluster.get(1).callOnInstance(() -> BBHelper.refreshCalls.get());
        int warnings = cluster.get(1).callOnInstance(() -> BBHelper.notIndexedWarnings.get());
        String update = String.format("UPDATE %s.%s SET v = 'abc' WHERE k = ? IF v = 'other'", KS_THREE, table);
        assertThat(cas(1, update, ConsistencyLevel.SERIAL, ConsistencyLevel.ONE, key)).isEmpty();

        assertThat((int) cluster.get(1).callOnInstance(() -> BBHelper.refreshCalls.get())).isGreaterThan(refreshes);
        assertThat((int) cluster.get(1).callOnInstance(() -> BBHelper.notIndexedWarnings.get())).isGreaterThan(warnings);
        assertEquals(1, cluster.get(1).executeInternal(String.format("SELECT k FROM %s.%s WHERE k = ?", KS_THREE, table), key).length);
    }

    @Test
    public void testLwtCommitInRemoteDatacenter()
    {
        setPaxosVariant("v2");
        String table = createTable(KS_FOUR);
        List<Integer> keys = keys(KS_FOUR, table, 2, 1, 2, 3, 4);
        cluster.get(3).runOnInstance(() -> BBHelper.warnings = List.of("datacenter2"));
        cluster.get(4).runOnInstance(() -> BBHelper.warnings = List.of("datacenter2"));
        String insert = String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?) IF NOT EXISTS", KS_FOUR, table);

        assertThat(cas(1, insert, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.ALL, keys.get(0), "abc")).containsExactly("datacenter2");

        // node 2 answers once a datacenter2 reply was processed, which doesn't count
        int remoteHookCalls = cluster.get(1).callOnInstance(() -> BBHelper.remoteDcHookCalls.get());
        hold(Verb.PAXOS_COMMIT_RSP, 2, 1, () -> cluster.get(1).callOnInstance(() -> BBHelper.remoteDcHookCalls.get()) > remoteHookCalls);
        assertThat(cas(1, insert, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_QUORUM, keys.get(1), "abc")).isEmpty();
    }

    // creates a table with an n-gram index on v, named <table>_idx
    private static String createTable(String keyspace)
    {
        String table = "t_" + seq.getAndIncrement();
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (k int PRIMARY KEY, v text)", keyspace, table));
        cluster.schemaChange(String.format("CREATE CUSTOM INDEX %s_idx ON %s.%s (v) USING 'StorageAttachedIndex' WITH OPTIONS = %s",
                                           table, keyspace, table, NGRAM_ANALYZER));
        SAIUtil.waitForIndexQueryableOnAllNodes(cluster, keyspace);
        return table;
    }

    private static String notIndexedWarning(String keyspace, String table)
    {
        return String.format(IndexContext.ANALYZED_VALUE_NOT_INDEXED_WARNING, "v", keyspace, table + "_idx");
    }

    private static List<String> write(int coordinator, String keyspace, String table, int key, String value, ConsistencyLevel cl)
    {
        return cluster.coordinator(coordinator)
                      .executeWithResult(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", keyspace, table), cl, key, value)
                      .warnings();
    }

    private static List<String> cas(int coordinator, String query, ConsistencyLevel serial, ConsistencyLevel commit, Object... values)
    {
        return cluster.coordinator(coordinator).executeWithResult(query, serial, commit, values).warnings();
    }

    private static String host(int node)
    {
        return cluster.get(node).broadcastAddress().getAddress().getHostAddress();
    }

    private static void setPaxosVariant(String variant)
    {
        cluster.forEach(node -> node.runOnInstance(() -> Paxos.setPaxosVariant(Config.PaxosVariant.valueOf(variant))));
    }

    private static int capturedNotIndexedWarnings()
    {
        int captured = 0;
        for (int node = 1; node <= 4; node++)
            captured += cluster.get(node).callOnInstance(() -> BBHelper.capturedNotIndexedWarnings.get());
        return captured;
    }

    private static List<String> execute(int coordinator, String query, ConsistencyLevel cl)
    {
        return cluster.coordinator(coordinator).executeWithResult(query, cl).warnings();
    }

    private static String insert(String keyspace, String table, int key)
    {
        return String.format("INSERT INTO %s.%s (k, v) VALUES (%d, '%s'); ", keyspace, table, key, NGRAM_VALUE);
    }

    // the first keys whose replicas are exactly the given nodes
    private static List<Integer> keys(String keyspace, String table, int count, int... nodes)
    {
        List<InetSocketAddress> addresses = Arrays.stream(nodes).mapToObj(n -> cluster.get(n).broadcastAddress()).collect(Collectors.toList());
        List<Integer> keys = cluster.get(1).callOnInstance(() -> {
            Set<InetAddressAndPort> replicas = addresses.stream()
                                                        .map(a -> InetAddressAndPort.getByAddressOverrideDefaults(a.getAddress(), a.getPort()))
                                                        .collect(Collectors.toSet());
            List<Integer> found = new ArrayList<>();
            for (int key = 0; found.size() < count; key++)
            {
                if (StorageService.instance.getNaturalReplicasForToken(keyspace, table, String.valueOf(key)).endpoints().equals(replicas))
                    found.add(key);
            }
            return found;
        });
        return keys;
    }

    // holds the messages of the verb from a node to another until the condition holds, for 10 seconds at most
    private static void hold(Verb verb, int from, int to, BooleanSupplier release)
    {
        cluster.filters().verbs(verb.id).from(from).to(to).messagesMatching((f, t, message) -> {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!release.getAsBoolean())
            {
                assertTrue("message held for too long", System.nanoTime() < deadline);
                FBUtilities.sleepQuietly(10);
            }
            return false;
        }).drop();
    }

    public static class BBHelper
    {
        // the warnings this node raises whenever it adds a value to an SAI memtable index
        public static volatile List<String> warnings;
        public static final AtomicInteger localHookCalls = new AtomicInteger();
        public static final AtomicInteger remoteDcHookCalls = new AtomicInteger();
        public static final AtomicInteger discardCalls = new AtomicInteger();
        public static final AtomicInteger commitAndPrepareCalls = new AtomicInteger();
        public static final AtomicInteger refreshCalls = new AtomicInteger();
        public static final AtomicInteger notIndexedWarnings = new AtomicInteger();
        public static final AtomicInteger capturedNotIndexedWarnings = new AtomicInteger();

        // the next wait for a Paxos commit times out, and its outcome is read once this node processed a commit reply
        // from that host
        public static volatile String timeOutCommitWaitUntilReplyFrom;
        public static final AtomicInteger commitWaitTimeouts = new AtomicInteger();
        public static final Set<String> commitRepliesFrom = ConcurrentHashMap.newKeySet();
        static volatile Thread timedOutThread;
        static volatile String timedOutWaitFor;

        @SuppressWarnings("resource")
        public static void install(ClassLoader classLoader, Integer node)
        {
            new ByteBuddy().rebase(TrieMemoryIndex.class)
                           .method(named("add"))
                           .intercept(MethodDelegation.to(AddWarning.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(AbstractWriteResponseHandler.class)
                           .method(named("addReplicaWarnings").and(takesArguments(List.class)))
                           .intercept(MethodDelegation.to(LocalHook.class))
                           .method(named("addReplicaWarnings").and(takesArguments(Message.class)))
                           .intercept(MethodDelegation.to(RemoteHook.class))
                           .method(named("discardReplicaWarnings"))
                           .intercept(MethodDelegation.to(DiscardHook.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(IndexContext.class)
                           .method(named("warnNotIndexed"))
                           .intercept(MethodDelegation.to(NotIndexedHook.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(PaxosCommitAndPrepare.RequestHandler.class)
                           .method(named("execute"))
                           .intercept(MethodDelegation.to(CommitAndPrepareHook.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(PaxosPrepareRefresh.RequestHandler.class)
                           .method(named("execute"))
                           .intercept(MethodDelegation.to(RefreshHook.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(PaxosCommit.class)
                           .method(named("onResponse").and(takesArguments(NoPayload.class, InetAddressAndPort.class)))
                           .intercept(MethodDelegation.to(CommitReplyHook.class))
                           .method(named("status"))
                           .intercept(MethodDelegation.to(CommitStatusHook.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(ConditionAsConsumer.Async.class)
                           .method(named("awaitUntil"))
                           .intercept(MethodDelegation.to(CommitWaitHook.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            TypePool.Resolution async = TypePool.Default.of(classLoader).describe(PaxosCommit.class.getName() + "$Async");
            if (async.isResolved())
            {
                new ByteBuddy().rebase(async.resolve(), ClassFileLocator.ForClassLoader.of(classLoader))
                               .method(named("addReplicaWarnings").and(takesArguments(List.class)))
                               .intercept(MethodDelegation.to(LocalHook.class))
                               .method(named("addReplicaWarnings").and(takesArguments(Message.class)))
                               .intercept(MethodDelegation.to(RemoteHook.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }
    }

    public static class AddWarning
    {
        @SuppressWarnings("unused")
        public static void add(@SuperCall Callable<?> zuper) throws Exception
        {
            zuper.call();
            List<String> warnings = BBHelper.warnings;
            if (warnings != null)
                warnings.forEach(ClientWarn.instance::warn);
        }
    }

    public static class LocalHook
    {
        @SuppressWarnings("unused")
        public static void addReplicaWarnings(@SuperCall Callable<?> zuper) throws Exception
        {
            zuper.call();
            BBHelper.localHookCalls.incrementAndGet();
        }
    }

    public static class RemoteHook
    {
        // counts the replies from another datacenter
        @SuppressWarnings("unused")
        public static void addReplicaWarnings(@Argument(0) Message<?> response, @SuperCall Callable<?> zuper) throws Exception
        {
            zuper.call();
            String datacenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(response.from());
            if (!datacenter.equals(DatabaseDescriptor.getLocalDataCenter()))
                BBHelper.remoteDcHookCalls.incrementAndGet();
        }
    }

    public static class DiscardHook
    {
        @SuppressWarnings("unused")
        public static void discardReplicaWarnings(@SuperCall Callable<?> zuper) throws Exception
        {
            zuper.call();
            BBHelper.discardCalls.incrementAndGet();
        }
    }

    public static class NotIndexedHook
    {
        @SuppressWarnings("unused")
        public static void warnNotIndexed(@SuperCall Callable<?> zuper) throws Exception
        {
            BBHelper.notIndexedWarnings.incrementAndGet();
            if (ReplicaWarnings.isCapturing())
                BBHelper.capturedNotIndexedWarnings.incrementAndGet();
            zuper.call();
        }
    }

    public static class CommitAndPrepareHook
    {
        @RuntimeType
        @SuppressWarnings("unused")
        public static Object execute(@SuperCall Callable<?> zuper) throws Exception
        {
            BBHelper.commitAndPrepareCalls.incrementAndGet();
            return zuper.call();
        }
    }

    public static class RefreshHook
    {
        @RuntimeType
        @SuppressWarnings("unused")
        public static Object execute(@SuperCall Callable<?> zuper) throws Exception
        {
            BBHelper.refreshCalls.incrementAndGet();
            return zuper.call();
        }
    }

    public static class CommitReplyHook
    {
        @SuppressWarnings("unused")
        public static void onResponse(@Argument(1) InetAddressAndPort from, @SuperCall Callable<?> zuper) throws Exception
        {
            zuper.call();
            BBHelper.commitRepliesFrom.add(from.getAddress().getHostAddress());
        }
    }

    public static class CommitWaitHook
    {
        // the wait for a Paxos commit returns as timed out, when armed
        @RuntimeType
        @SuppressWarnings("unused")
        public static Object awaitUntil(@SuperCall Callable<?> zuper) throws Exception
        {
            String waitFor = BBHelper.timeOutCommitWaitUntilReplyFrom;
            if (waitFor == null || !inPaxosCommit())
                return zuper.call();

            BBHelper.timeOutCommitWaitUntilReplyFrom = null;
            BBHelper.timedOutWaitFor = waitFor;
            BBHelper.timedOutThread = Thread.currentThread();
            BBHelper.commitWaitTimeouts.incrementAndGet();
            return false;
        }

        private static boolean inPaxosCommit()
        {
            return StackWalker.getInstance().walk(frames -> frames.anyMatch(f -> f.getClassName().startsWith(PaxosCommit.class.getName() + '$')));
        }
    }

    public static class CommitStatusHook
    {
        // after a timed out wait, the outcome is read once the awaited reply was processed
        @RuntimeType
        @SuppressWarnings("unused")
        public static Object status(@SuperCall Callable<?> zuper) throws Exception
        {
            if (Thread.currentThread() == BBHelper.timedOutThread)
            {
                BBHelper.timedOutThread = null;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!BBHelper.commitRepliesFrom.contains(BBHelper.timedOutWaitFor) && System.nanoTime() < deadline)
                    FBUtilities.sleepQuietly(10);
            }
            return zuper.call();
        }
    }
}
