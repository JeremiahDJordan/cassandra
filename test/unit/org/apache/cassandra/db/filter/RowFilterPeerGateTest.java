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

package org.apache.cassandra.db.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.IncompatibleMessagingVersionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.PeerCheckCache;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.EndpointMessagingVersions;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceAccessor;
import org.apache.cassandra.utils.CassandraVersion;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/**
 * Tests which nodes that a query can contact make {@link RowFilter#validateForPeers} reject a DataStax-only query (an
 * OR), and make the coordinator let replicas relax its filters (see {@link RowFilter#withReplicaFilteringStrictness}),
 * depending on what is known of them. The result of the check is reused until the nodes change.
 */
public class RowFilterPeerGateTest extends CQLTester
{
    private static final String TABLE = "CREATE TABLE %s (k int PRIMARY KEY, a int, b int)";
    private static final String OR = "SELECT k FROM %s WHERE a = 1 OR b = 2 ALLOW FILTERING";

    private String replicatedTable;
    private boolean previousNonStrict;

    @Before
    public void setup() throws Throwable
    {
        previousNonStrict = DatabaseDescriptor.getSAINonStrictReplicaFilteringEnabled();
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(false);
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                         "{'class': 'SimpleStrategy', 'replication_factor': '2'}");
        replicatedTable = keyspace + '.' + createTable(keyspace, TABLE);
        createTable(TABLE);
        execute("INSERT INTO %s (k, a, b) VALUES (1, 1, 2)");
    }

    @After
    public void restoreNonStrictFiltering()
    {
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(previousNonStrict);
    }

    /**
     * The nodes a query can contact are checked again as soon as a node is first seen below DS 10, even if that
     * doesn't change the minimum known messaging version.
     */
    @Test
    public void testNodeNewlySeenBelowDS10() throws Throwable
    {
        // a node below DS 10 that isn't in the ring sets the minimum known version
        InetAddressAndPort outsider = InetAddressAndPort.getByName("127.0.0.9");
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.8");
        MessagingService.instance().versions.set(outsider, MessagingService.VERSION_50);
        try
        {
            // a ring member whose version isn't known, and that isn't alive, isn't counted
            addToRing(peer);
            assertAccepted();

            MessagingService.instance().versions.set(peer, MessagingService.VERSION_50);
            assertRejected();
        }
        finally
        {
            StorageService.instance.getTokenMetadata().removeEndpoint(peer);
            MessagingService.instance().versions.reset(peer);
            MessagingService.instance().versions.reset(outsider);
        }
    }

    /**
     * A node below DS 10 that refuses a filter while the coordinator checks the nodes a query can contact counts,
     * although the check, which started before, doesn't see it.
     */
    @Test
    public void testRefusalDuringCheck() throws Throwable
    {
        InetAddressAndPort outsider = InetAddressAndPort.getByName("127.0.0.9");
        Injections.Barrier holdCheck = Injections.newBarrier("hold_peer_check", 2, false)
                                                 .add(InvokePointBuilder.newInvokePoint()
                                                                        .onClass(RowFilter.class)
                                                                        .onMethod("computeMayHavePeersBelowDS10")
                                                                        .atExit())
                                                 .build();
        try
        {
            RowFilter strict = rowFilter();
            assertThat(strict.isStrict()).isTrue();
            // a node outside the ring seen below DS 10 makes the next query check the nodes
            MessagingService.instance().versions.set(outsider, MessagingService.VERSION_50);
            Injections.inject(holdCheck);

            CompletableFuture<RowFilter> checked = CompletableFuture.supplyAsync(this::rowFilter);
            Util.spinAssertEquals(1L, holdCheck::getCount, 60);
            assertThatThrownBy(() -> RowFilter.serializer.serialize(strict, new DataOutputBuffer(),
                                                                     MessagingService.VERSION_50))
                .isInstanceOf(IncompatibleMessagingVersionException.class);
            holdCheck.countDown();

            assertThat(checked.get(60, TimeUnit.SECONDS).isStrict()).isFalse();
            assertStrictness(false);
        }
        finally
        {
            holdCheck.disable();
            MessagingService.instance().versions.reset(outsider);
        }
    }

    /**
     * A node below DS 10 that refuses a filter counts for a second, after which the nodes a query can contact decide
     * again.
     */
    @Test
    public void testRefusalMarkExpires() throws Throwable
    {
        RowFilter strict = rowFilter();
        assertThat(strict.isStrict()).isTrue();
        assertThatThrownBy(() -> RowFilter.serializer.serialize(strict, new DataOutputBuffer(), MessagingService.VERSION_50))
            .isInstanceOf(IncompatibleMessagingVersionException.class);
        assertStrictness(false);

        TimeUnit.MILLISECONDS.sleep(1100);
        assertStrictness(true);
    }

    @Test
    public void testPeers() throws Throwable
    {
        // with gossip enabled, the failure detector logs an error when asked about an endpoint gossip doesn't know
        ListAppender<ILoggingEvent> failureDetectorLog = new ListAppender<>();
        failureDetectorLog.start();
        ((Logger) LoggerFactory.getLogger(FailureDetector.class)).addAppender(failureDetectorLog);
        Gossiper.instance.start(1);
        try
        {
            testPeers(failureDetectorLog);
        }
        finally
        {
            Gossiper.instance.stop();
            ((Logger) LoggerFactory.getLogger(FailureDetector.class)).detachAppender(failureDetectorLog);
        }
    }

    private void testPeers(ListAppender<ILoggingEvent> failureDetectorLog) throws Throwable
    {
        assertAccepted();

        EndpointMessagingVersions versions = MessagingService.instance().versions;

        // A ring member that isn't a known peer (see Nodes), of unknown version, isn't counted: only known peers are
        // asked about to the failure detector, which would log an error for this one, unknown to gossip too.
        InetAddressAndPort unknown = InetAddressAndPort.getByName("127.0.0.3");
        addToRing(unknown);
        assertAccepted();
        assertThat(failureDetectorLog.list).noneMatch(e -> e.getLevel() == Level.ERROR);

        // a known peer, alive, of unknown version, is counted as below DS 10
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.2");
        UUID hostId = addToRing(peer);
        Nodes.peers().update(peer, info -> info.setHostId(hostId), true);
        Gossiper.instance.initializeNodeUnsafe(peer, hostId, 1);
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(peer);
        assertRejected();

        // The same, removed from the known peers (soft removal, as when a node leaves the cluster): it isn't counted, as
        // Nodes is what says which peers there are, even though gossip still has it alive here.
        Nodes.peers().remove(peer, true, false);
        assertAccepted();
        Nodes.peers().update(peer, info -> info.setHostId(hostId), true);
        assertRejected();

        // the same, down: it can't be sent a read anyway
        state.markDead();
        assertAccepted();

        // a known version below DS 10 is counted, alive or not; a DataStax version isn't
        versions.set(peer, MessagingService.VERSION_50);
        assertRejected();
        versions.set(peer, MessagingService.current_version);
        assertAccepted();
        state.markAlive();
        assertAccepted();
        versions.set(peer, MessagingService.VERSION_50);
        assertRejected();
        versions.set(peer, MessagingService.current_version);
        assertAccepted();
    }

    /**
     * The nodes a query can contact aren't checked again until they change, however long it takes.
     */
    @Test
    public void testNoCheckWithoutChange() throws Throwable
    {
        createTable(TABLE);
        validateOr(ConsistencyLevel.QUORUM);
        validateOr(ConsistencyLevel.LOCAL_QUORUM);
        Injections.Counter checks = newCheckCounter();
        Injections.inject(checks);
        try
        {
            TimeUnit.MILLISECONDS.sleep(1100);
            for (int i = 0; i < 10; i++)
            {
                assertThat(validateOr(ConsistencyLevel.QUORUM)).isTrue();
                assertThat(validateOr(ConsistencyLevel.LOCAL_QUORUM)).isTrue();
            }
            assertEquals(0, checks.get());
        }
        finally
        {
            checks.disable();
        }
    }

    /**
     * A query checks the nodes again after a messaging version is learnt, changes or is forgotten, whatever the
     * version, including the versions that the connections to a node keep apart.
     */
    @Test
    public void testVersionChangeChecksAgain() throws Throwable
    {
        createTable(TABLE);
        InetAddressAndPort outsider = InetAddressAndPort.getByName("127.0.0.10");
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        try
        {
            assertChecksAgain(() -> versions.set(outsider, MessagingService.current_version));
            assertNotCheckedAgain(() -> versions.set(outsider, MessagingService.current_version));
            assertChecksAgain(() -> versions.set(outsider, MessagingService.VERSION_50));
            assertChecksAgain(() -> versions.reset(outsider));
            assertNotCheckedAgain(() -> versions.reset(outsider));
            assertChecksAgain(() -> versions.copy().set(outsider, MessagingService.VERSION_50));
        }
        finally
        {
            versions.reset(outsider);
        }
    }

    /**
     * A query checks the nodes again after a known peer gets a gossip state, is marked down or is marked up.
     */
    @Test
    public void testLivenessChangeChecksAgain() throws Throwable
    {
        createTable(TABLE);
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.11");
        UUID hostId = UUID.randomUUID();
        try
        {
            assertChecksAgain(() -> Nodes.peers().update(peer, info -> info.setHostId(hostId), true));
            assertChecksAgain(() -> Gossiper.instance.initializeNodeUnsafe(peer, hostId, 1));
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(peer);
            assertChecksAgain(state::markDead);
            assertNotCheckedAgain(state::markDead);
            assertChecksAgain(state::markAlive);
            assertNotCheckedAgain(state::markAlive);
        }
        finally
        {
            removePeer(peer);
        }
    }

    /**
     * A query checks the nodes again after a node joins: its host ID, then its entry in the known peers. A change of a
     * peer's datacenter counts, other changes of the peer don't.
     */
    @Test
    public void testJoinChecksAgain() throws Throwable
    {
        createTable(TABLE);
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.12");
        UUID hostId = UUID.randomUUID();
        try
        {
            assertChecksAgain(() -> StorageService.instance.getTokenMetadata().updateHostId(hostId, peer));
            assertNotCheckedAgain(() -> StorageService.instance.getTokenMetadata().updateHostId(hostId, peer));
            assertChecksAgain(() -> Nodes.peers().update(peer, info -> info.setHostId(hostId), true));
            assertNotCheckedAgain(() -> Nodes.peers().update(peer, info -> info.setReleaseVersion(new CassandraVersion("5.0.10")), true));
            assertChecksAgain(() -> Nodes.peers().update(peer, info -> info.setDataCenter("dc2"), true));
        }
        finally
        {
            StorageService.instance.getTokenMetadata().removeEndpoint(peer);
            Nodes.peers().remove(peer, true, true);
        }
    }

    /**
     * A query checks the nodes again after a node leaves the ring, is removed from the known peers, and loses its gossip
     * state.
     */
    @Test
    public void testRemovalChecksAgain() throws Throwable
    {
        createTable(TABLE);
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.13");
        UUID hostId = addToRing(peer);
        Nodes.peers().update(peer, info -> info.setHostId(hostId), true);
        Gossiper.instance.initializeNodeUnsafe(peer, hostId, 1);
        try
        {
            assertChecksAgain(() -> StorageService.instance.getTokenMetadata().removeEndpoint(peer));
            assertChecksAgain(() -> Nodes.peers().remove(peer, true, false));
            assertChecksAgain(() -> Nodes.peers().remove(peer, true, true));
            assertNotCheckedAgain(() -> Nodes.peers().remove(peer, true, true));
            assertChecksAgain(() -> Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.replacedEndpoint(peer)));
        }
        finally
        {
            StorageService.instance.getTokenMetadata().removeEndpoint(peer);
            removePeer(peer);
        }
    }

    /**
     * A query checks the nodes again after a change of the tokens or of the topology of the ring, or after the token
     * metadata is replaced.
     */
    @Test
    public void testRingChangeChecksAgain() throws Throwable
    {
        createTable(TABLE);
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.14");
        addToRing(peer);
        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
        try
        {
            assertChecksAgain(() -> tokenMetadata.updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), peer));
            assertChecksAgain(() -> tokenMetadata.updateTopology(peer));
            assertChecksAgain(() -> StorageServiceAccessor.setTokenMetadata(tokenMetadata.cloneOnlyTokenMap()));
        }
        finally
        {
            StorageServiceAccessor.setTokenMetadata(tokenMetadata);
            tokenMetadata.removeEndpoint(peer);
        }
    }

    /**
     * A query checks the nodes again after a keyspace is created, after a change of its replication but not after other
     * changes of it, and after it is dropped.
     */
    @Test
    public void testKeyspaceChangeChecksAgain() throws Throwable
    {
        createTable(TABLE);
        String keyspace = createKeyspaceName();
        assertChecksAgain(() -> schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = " +
                                             "{'class': 'SimpleStrategy', 'replication_factor': '1'}"));
        assertChecksAgain(() -> schemaChange("ALTER KEYSPACE " + keyspace + " WITH replication = " +
                                             "{'class': 'SimpleStrategy', 'replication_factor': '2'}"));
        assertNotCheckedAgain(() -> schemaChange("ALTER KEYSPACE " + keyspace + " WITH durable_writes = false"));
        assertChecksAgain(() -> schemaChange("DROP KEYSPACE " + keyspace));
    }

    /**
     * The queries that find the nodes changed check them once: one query checks them while the others wait for its
     * result.
     */
    @Test
    public void testConcurrentQueriesCheckOnce() throws Throwable
    {
        createTable(TABLE);
        InetAddressAndPort outsider = InetAddressAndPort.getByName("127.0.0.15");
        Injections.Counter checks = newCheckCounter();
        Injections.Barrier holdCheck = Injections.newBarrierAwait("hold_concurrent_peer_check", 1, false)
                                                 .add(InvokePointBuilder.newInvokePoint()
                                                                        .onClass(RowFilter.class)
                                                                        .onMethod("computeMayHavePeersBelowDS10"))
                                                 .build();
        List<Thread> queries = new ArrayList<>();
        List<Boolean> accepted = new CopyOnWriteArrayList<>();
        try
        {
            validateOr(ConsistencyLevel.QUORUM);
            Injections.inject(checks, holdCheck);
            MessagingService.instance().versions.set(outsider, MessagingService.VERSION_50);
            for (int i = 0; i < 16; i++)
                queries.add(new Thread(() -> accepted.add(validateOr(ConsistencyLevel.QUORUM))));
            queries.forEach(Thread::start);

            Util.spinAssertEquals(1L, checks::get, 60);
            Util.spinAssertEquals(true, () -> queries.stream().noneMatch(t -> t.getState() == Thread.State.RUNNABLE), 60);
            holdCheck.countDown();
            for (Thread query : queries)
                query.join(TimeUnit.SECONDS.toMillis(60));

            assertEquals(1, checks.get());
            assertThat(accepted).hasSize(16).containsOnly(true);
        }
        finally
        {
            holdCheck.countDown();
            checks.disable();
            holdCheck.disable();
            MessagingService.instance().versions.reset(outsider);
        }
    }

    /**
     * The queries that wait for a check use its result if the check started after they found the nodes changed, even if
     * the nodes changed again since: they don't each check the nodes again.
     */
    @Test
    public void testQueuedQueriesUseNewerCheck() throws Throwable
    {
        createTable(TABLE);
        validateOr(ConsistencyLevel.QUORUM);
        Injections.Counter checks = newCheckCounter();
        Injection changeDuringCheck = Injections.newCustom("change_during_queued_peer_check")
                                                .add(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(RowFilter.class)
                                                                       .onMethod("computeMayHavePeersBelowDS10"))
                                                .add(ActionBuilder.newActionBuilder().actions()
                                                                  .doAction(Expression.expr(PeerCheckCache.class.getName())
                                                                                      .method("peersChanged").args()))
                                                .build();
        Injections.Barrier holdCheck = Injections.newBarrier("hold_first_queued_peer_check", 2, false)
                                                 .add(InvokePointBuilder.newInvokePoint()
                                                                        .onClass(RowFilter.class)
                                                                        .onMethod("computeMayHavePeersBelowDS10")
                                                                        .atExit())
                                                 .build();
        List<Thread> waiting = new ArrayList<>();
        try
        {
            Injections.inject(checks, changeDuringCheck, holdCheck);
            PeerCheckCache.peersChanged();
            CompletableFuture<Boolean> first = CompletableFuture.supplyAsync(() -> validateOr(ConsistencyLevel.QUORUM));
            Util.spinAssertEquals(1L, holdCheck::getCount, 60);

            // these queries find the nodes changed by the first check, and wait for it
            for (int i = 0; i < 4; i++)
                waiting.add(new Thread(() -> validateOr(ConsistencyLevel.QUORUM)));
            waiting.forEach(Thread::start);
            Util.spinAssertEquals(true, () -> waiting.stream().allMatch(t -> t.getState() == Thread.State.BLOCKED), 60);
            holdCheck.countDown();

            assertThat(first.get(60, TimeUnit.SECONDS)).isTrue();
            for (Thread query : waiting)
                query.join(TimeUnit.SECONDS.toMillis(60));
            assertEquals(2, checks.get());
        }
        finally
        {
            holdCheck.countDown();
            changeDuringCheck.disable();
            checks.disable();
            holdCheck.disable();
        }
    }

    /**
     * A query whose check of the nodes overlaps a change checks them once more, then returns even if they keep
     * changing; the next query checks them again.
     */
    @Test
    public void testChangesDuringCheck() throws Throwable
    {
        createTable(TABLE);
        validateOr(ConsistencyLevel.QUORUM);
        Injections.Counter checks = newCheckCounter();
        Injection changeDuringCheck = Injections.newCustom("change_during_peer_check")
                                                .add(InvokePointBuilder.newInvokePoint()
                                                                       .onClass(RowFilter.class)
                                                                       .onMethod("computeMayHavePeersBelowDS10"))
                                                .add(ActionBuilder.newActionBuilder().actions()
                                                                  .doAction(Expression.expr(PeerCheckCache.class.getName())
                                                                                      .method("peersChanged").args()))
                                                .build();
        try
        {
            Injections.inject(checks, changeDuringCheck);
            PeerCheckCache.peersChanged();
            CompletableFuture<Boolean> query = CompletableFuture.supplyAsync(() -> validateOr(ConsistencyLevel.QUORUM));
            assertThat(query.get(60, TimeUnit.SECONDS)).isTrue();
            assertEquals(2, checks.get());

            changeDuringCheck.disable();
            validateOr(ConsistencyLevel.QUORUM);
            validateOr(ConsistencyLevel.QUORUM);
            assertEquals(3, checks.get());
        }
        finally
        {
            changeDuringCheck.disable();
            checks.disable();
        }
    }

    /**
     * A query without a row filter doesn't check the nodes.
     */
    @Test
    public void testEmptyFilterChecksNoPeers() throws Throwable
    {
        createTable(TABLE);
        execute("INSERT INTO %s (k, a, b) VALUES (1, 1, 2)");
        InetAddressAndPort outsider = InetAddressAndPort.getByName("127.0.0.16");
        Injections.Counter checks = newCheckCounter();
        Injections.inject(checks);
        try
        {
            MessagingService.instance().versions.set(outsider, MessagingService.VERSION_50);
            assertRows(execute("SELECT k FROM %s WHERE k = 1"), row(1));
            assertRows(execute("SELECT k FROM %s"), row(1));
            assertEquals(0, checks.get());
        }
        finally
        {
            checks.disable();
            MessagingService.instance().versions.reset(outsider);
        }
    }

    /**
     * A query whose restrictions match no row, such as a contradictory range, reads nothing, so it isn't rejected while
     * a node it can contact is below DS 10.
     */
    @Test
    public void testUnsatisfiableFilterBelowDS10() throws Throwable
    {
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.8");
        addToRing(peer);
        MessagingService.instance().versions.set(peer, MessagingService.VERSION_50);
        try
        {
            assertRejected();
            assertEmpty(execute("SELECT k FROM %s WHERE a > 2 AND a < 1 ALLOW FILTERING"));
            assertEmpty(execute("SELECT k FROM %s WHERE (a > 2 AND a < 1) OR (b > 3 AND b < 2) ALLOW FILTERING"));
        }
        finally
        {
            StorageService.instance.getTokenMetadata().removeEndpoint(peer);
            MessagingService.instance().versions.reset(peer);
        }
    }

    /**
     * Asserts that the given change makes the next queries check the nodes again, once for every node and once for the
     * local datacenter.
     */
    private void assertChecksAgain(CheckedFunction change) throws Throwable
    {
        assertChecks(change, 1);
    }

    /** Asserts that the given change doesn't make the next queries check the nodes again. */
    private void assertNotCheckedAgain(CheckedFunction change) throws Throwable
    {
        assertChecks(change, 0);
    }

    private void assertChecks(CheckedFunction change, int checksPerScope) throws Throwable
    {
        validateOr(ConsistencyLevel.QUORUM);
        validateOr(ConsistencyLevel.LOCAL_QUORUM);
        Injections.Counter checks = newCheckCounter();
        Injections.inject(checks);
        try
        {
            change.apply();
            for (int i = 0; i < 2; i++)
            {
                validateOr(ConsistencyLevel.QUORUM);
                validateOr(ConsistencyLevel.LOCAL_QUORUM);
            }
            assertEquals(2 * checksPerScope, checks.get());
        }
        finally
        {
            checks.disable();
        }
    }

    private static Injections.Counter newCheckCounter()
    {
        return Injections.newCounter("peer_checks")
                         .add(InvokePointBuilder.newInvokePoint()
                                                .onClass(RowFilter.class)
                                                .onMethod("computeMayHavePeersBelowDS10"))
                         .build();
    }

    /** Whether the nodes that a query at the given consistency level can contact accept an OR on the current table. */
    private boolean validateOr(ConsistencyLevel consistency)
    {
        SelectStatement select = (SelectStatement) QueryProcessor.parseStatement(formatQuery(OR), ClientState.forInternalCalls());
        RowFilter filter = select.getRowFilter(QueryOptions.forInternalCalls(consistency, Collections.emptyList()),
                                               ClientState.forInternalCalls());
        try
        {
            filter.validateForPeers(KEYSPACE, consistency, false);
            return true;
        }
        catch (InvalidRequestException e)
        {
            return false;
        }
    }

    /** Removes the given peer from the known peers and from gossip. */
    private static void removePeer(InetAddressAndPort peer)
    {
        Nodes.peers().remove(peer, true, true);
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.replacedEndpoint(peer));
    }

    private static UUID addToRing(InetAddressAndPort endpoint)
    {
        UUID hostId = UUID.randomUUID();
        StorageService.instance.getTokenMetadata().updateHostId(hostId, endpoint);
        StorageService.instance.getTokenMetadata().updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), endpoint);
        return hostId;
    }

    private void assertAccepted()
    {
        assertEquals(1, execute(OR).size());
        assertStrictness(true);
    }

    private void assertRejected()
    {
        assertThatThrownBy(() -> execute(OR)).isInstanceOf(InvalidRequestException.class)
                                             .hasMessageContaining("The OR operator is not supported");
        assertStrictness(false);
    }

    /** Asserts whether the coordinator makes a QUORUM filter of several mutable columns strict. */
    private void assertStrictness(boolean strict)
    {
        RowFilter filter = rowFilter();
        assertThat(filter.needsReconciliation()).isTrue();
        assertThat(filter.isStrict()).isEqualTo(strict);
    }

    private RowFilter rowFilter()
    {
        String query = "SELECT * FROM " + replicatedTable + " WHERE a = 1 AND b = 2 ALLOW FILTERING";
        SelectStatement select = (SelectStatement) QueryProcessor.parseStatement(query, ClientState.forInternalCalls());
        return select.getRowFilter(QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM, Collections.emptyList()),
                                   ClientState.forInternalCalls());
    }
}
