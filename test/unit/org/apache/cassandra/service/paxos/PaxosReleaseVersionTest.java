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

package org.apache.cassandra.service.paxos;

import java.net.UnknownHostException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Paxos reads peer release versions from {@link Nodes}, not from gossip.
 */
public class PaxosReleaseVersionTest
{
    private static Replica pre41Peer;
    private static Replica modernPeer;
    private static Replica unknownPeer;

    @BeforeClass
    public static void setUpClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();

        // Only Nodes knows these peers' versions; gossip has no state for them
        pre41Peer = peer("127.0.0.10", "4.0.0");
        modernPeer = peer("127.0.0.11", "4.1.0");
        unknownPeer = full(InetAddressAndPort.getByName("127.0.0.12"));
    }

    @Test
    public void testIsOldParticipant()
    {
        assertTrue(Paxos.isOldParticipant(pre41Peer));
        assertFalse(Paxos.isOldParticipant(modernPeer));
        assertFalse(Paxos.isOldParticipant(unknownPeer));
    }

    @Test
    public void testValidatePeerCompatibility()
    {
        assertFalse(PaxosRepair.validatePeerCompatibility(pre41Peer));
        assertTrue(PaxosRepair.validatePeerCompatibility(modernPeer));
        assertFalse(PaxosRepair.validatePeerCompatibility(unknownPeer));
    }

    private static Replica peer(String address, String releaseVersion) throws UnknownHostException
    {
        InetAddressAndPort endpoint = InetAddressAndPort.getByName(address);
        Nodes.peers().update(endpoint, info -> info.setReleaseVersion(new CassandraVersion(releaseVersion)));
        assertNull(Gossiper.instance.getEndpointStateForEndpoint(endpoint));
        return full(endpoint);
    }
}
