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

package org.apache.cassandra.nodes;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaConstants;

import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.NODES_PERSISTENCE_CLASS;
import static org.apache.cassandra.config.CassandraRelevantProperties.UNSAFE_SYSTEM;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.LOCAL;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the startup steps that read or write the local node information against an alternate
 * {@link INodesPersistence}, plugged in through {@code cassandra.nodes.persistence_class}, that keeps everything in
 * memory. None of them may fall back to {@code system.local}.
 */
public class AlternateNodesPersistenceTest
{
    /**
     * Keeps the node information in memory. {@link #syncedGossipGeneration} is the gossip generation that was stored
     * when {@link #syncLocal()} last ran.
     */
    public static class InMemoryNodesPersistence implements INodesPersistence
    {
        public static final InMemoryNodesPersistence instance = new InMemoryNodesPersistence();

        private final Map<InetAddressAndPort, PeerInfo> peers = new ConcurrentHashMap<>();
        volatile LocalInfo local;
        volatile Integer syncedGossipGeneration;
        volatile boolean storesGossipGeneration = true;

        void reset()
        {
            local = null;
            syncedGossipGeneration = null;
            storesGossipGeneration = true;
            peers.clear();
        }

        @Override
        public LocalInfo loadLocal()
        {
            LocalInfo info = local;
            return info != null ? info.duplicate() : null;
        }

        @Override
        public void saveLocal(LocalInfo info)
        {
            LocalInfo stored = info.duplicate();
            if (!storesGossipGeneration)
                stored.setGossipGeneration(null);
            local = stored;
        }

        @Override
        public void syncLocal()
        {
            LocalInfo info = local;
            syncedGossipGeneration = info != null ? info.getGossipGeneration() : null;
        }

        @Override
        public Stream<PeerInfo> loadPeers()
        {
            return peers.values().stream().map(PeerInfo::duplicate);
        }

        @Override
        public void savePeer(PeerInfo info)
        {
            peers.put(info.getPeerAddressAndPort(), info.duplicate());
        }

        @Override
        public void deletePeer(InetAddressAndPort endpoint)
        {
            peers.remove(endpoint);
        }

        @Override
        public void syncPeers()
        {
            // nothing to sync
        }
    }

    static
    {
        NODES_PERSISTENCE_CLASS.setString(InMemoryNodesPersistence.class.getName());
        // flush system tables as a real node does, so that writes to system.local leave SSTables behind
        UNSAFE_SYSTEM.setBoolean(false);
    }

    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();
        ServerTestUtils.prepareServer();
    }

    @Before
    public void setUp()
    {
        Nodes.getInstance().awaitInflightUpdateCompletion();
        InMemoryNodesPersistence.instance.reset();
        Nodes.getInstance().reload();
        SystemKeyspace.persistLocalMetadata();
    }

    @Test
    public void testGossipGenerationLeavesSystemLocalAlone()
    {
        int liveSSTables = systemLocal().getLiveSSTables().size();

        SystemKeyspace.incrementAndGetGeneration();
        SystemKeyspace.incrementAndGetGeneration();

        UntypedResultSet rows = executeInternal(format("SELECT gossip_generation FROM system.%s WHERE key = '%s'", LOCAL, LOCAL));
        assertThat(rows.isEmpty() || !rows.one().has("gossip_generation")).isTrue();
        assertThat(systemLocal().getLiveSSTables()).hasSize(liveSSTables);
    }

    @Test
    public void testGossipGenerationGoesThroughPersistence()
    {
        int first = SystemKeyspace.incrementAndGetGeneration();
        // stored and synced before gossip can start with it
        assertThat(InMemoryNodesPersistence.instance.local.getGossipGeneration()).isEqualTo(first);
        assertThat(InMemoryNodesPersistence.instance.syncedGossipGeneration).isEqualTo(first);

        // a restart reads it back from the persistence
        Nodes.getInstance().reload();
        assertThat(Nodes.local().get().getGossipGeneration()).isEqualTo(first);
        int second = SystemKeyspace.incrementAndGetGeneration();
        assertThat(second).isGreaterThan(first);
        assertThat(InMemoryNodesPersistence.instance.syncedGossipGeneration).isEqualTo(second);
    }

    @Test
    public void testGossipGenerationNeverGoesBackwards()
    {
        // the stored generation is ahead of this node's clock
        int ahead = (int) (currentTimeMillis() / 1000) + 3600;
        Nodes.local().update(info -> info.setGossipGeneration(ahead), true);
        Nodes.getInstance().reload();

        assertThat(SystemKeyspace.incrementAndGetGeneration()).isEqualTo(ahead + 1);
        assertThat(SystemKeyspace.incrementAndGetGeneration()).isEqualTo(ahead + 2);
    }

    @Test
    public void testGossipGenerationFallsBackToCurrentTimeWhenNotStored()
    {
        // an implementation that doesn't store the gossip generation yet
        InMemoryNodesPersistence.instance.storesGossipGeneration = false;
        SystemKeyspace.incrementAndGetGeneration();
        Nodes.getInstance().reload();
        assertThat(Nodes.local().get().getGossipGeneration()).isNull();

        int now = (int) (currentTimeMillis() / 1000);
        assertThat(SystemKeyspace.incrementAndGetGeneration()).isBetween(now, now + 1);
    }

    /**
     * The node starts once, then restarts with no local information in the alternate store, for example because the
     * store was reset or the node got a new identity.
     */
    @Test
    public void testRestartWithEmptyStore()
    {
        SystemKeyspace.incrementAndGetGeneration();
        SystemKeyspace.checkHealth();

        InMemoryNodesPersistence.instance.reset();
        Nodes.getInstance().reload();
        assertThat(Nodes.local().get().getClusterName()).isNull();

        SystemKeyspace.checkHealth();
    }

    /**
     * system.local has SSTables, for example because this data directory was used with the default persistence, but
     * the alternate store has no local information. The node starts as a new one.
     */
    @Test
    public void testCheckHealthIgnoresSystemLocalFiles()
    {
        executeInternal(format("INSERT INTO system.%s (key, cluster_name) VALUES ('%s', 'Some Cluster')", LOCAL, LOCAL));
        systemLocal().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        try
        {
            assertThat(systemLocal().getLiveSSTables()).isNotEmpty();

            InMemoryNodesPersistence.instance.reset();
            Nodes.getInstance().reload();
            assertThat(Nodes.local().get().getClusterName()).isNull();

            SystemKeyspace.checkHealth();
        }
        finally
        {
            systemLocal().clearUnsafe();
        }
    }

    private static ColumnFamilyStore systemLocal()
    {
        return Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(LOCAL);
    }
}
