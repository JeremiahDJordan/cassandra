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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.StorageCompatibilityMode;

/**
 * A {@link NodesPersistence} subclass that migrates CC4 file-based node metadata
 * to CC5 system tables on first load.
 *
 * This class is instantiated by {@link Nodes} when a CC4 nodes directory is detected.
 * It reads CC4's MessagePack metadata via {@link CC4NodesFileReader}, persists the
 * migrated data to the system tables, and returns it.
 *
 * The nodes directory is archived only in a storage compatibility mode that doesn't allow going back to CC4. Until
 * then CC4 starts from it again, and it is migrated again if CC4 ran since this node last ran CC5, which CC4 shows by
 * writing a higher gossip generation at every start. CC4 doesn't see the node metadata changed while on CC5, such as
 * truncation records, so drain a node before going back to CC4.
 */
public class CC4UpgradeNodesPersistence extends NodesPersistence
{
    private static final Logger logger = LoggerFactory.getLogger(CC4UpgradeNodesPersistence.class);

    // decided by loadLocal, which Nodes calls before loadPeers
    private boolean migrate = true;

    @Override
    public LocalInfo loadLocal()
    {
        LocalInfo migrated = CC4NodesFileReader.tryReadLocalInfo();
        Integer generation = loadGossipGeneration();
        migrate = migrated != null && (generation == null || (migrated.getGossipGeneration() != null && migrated.getGossipGeneration() > generation));
        if (migrate)
        {
            logger.info("Migrating local node metadata from CC4 file-based store (host_id={})", migrated.getHostId());
            saveLocal(migrated);
            return migrated;
        }
        if (migrated != null)
            logger.info("Not migrating CC4 node metadata, which isn't newer than the system tables (gossip generation {}, {} in system.local)",
                        migrated.getGossipGeneration(), generation);
        else
            logger.debug("No CC4 local metadata found to migrate, falling back to system tables");
        return super.loadLocal();
    }

    @Override
    public Stream<PeerInfo> loadPeers()
    {
        if (migrate && CC4NodesFileReader.hasCC4NodesDirectory())
        {
            List<PeerInfo> migrated = CC4NodesFileReader.tryReadPeers().collect(Collectors.toList());
            logger.info("Migrating {} peer entries from CC4 file-based store", migrated.size());
            Set<InetAddressAndPort> addresses = migrated.stream().map(PeerInfo::getPeerAddressAndPort).collect(Collectors.toSet());
            super.loadPeers().map(PeerInfo::getPeerAddressAndPort).filter(peer -> !addresses.contains(peer)).forEach(this::deletePeer);
            for (PeerInfo peer : migrated)
                savePeer(peer);
        }

        if (!StorageCompatibilityMode.current().isBefore(CassandraVersion.CASSANDRA_5_0.major))
            CC4NodesFileReader.archiveCC4NodesDirectory();

        return super.loadPeers();
    }
}
