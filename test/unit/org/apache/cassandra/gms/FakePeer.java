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
package org.apache.cassandra.gms;

import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * A fake peer in the ring of a single-node test, to test the checks of {@link DataStaxFeatureSupport}: its messaging
 * version is the one {@link MessagingService} knows, its release version the one {@link Nodes} knows, and the failure
 * detector reads its liveness from its gossip state. The peer can't be sent any message. It starts with unknown
 * versions, and with no gossip state, so it isn't alive. Closing it removes it.
 */
public class FakePeer implements AutoCloseable
{
    public final InetAddressAndPort address;

    public FakePeer(String address) throws UnknownHostException
    {
        this(address, true);
    }

    /**
     * @param knownToNodes whether {@link Nodes} knows the peer; a peer of a custom token metadata provider may not be
     */
    public FakePeer(String address, boolean knownToNodes) throws UnknownHostException
    {
        this.address = InetAddressAndPort.getByName(address);
        UUID hostId = UUID.randomUUID();
        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
        tokenMetadata.updateHostId(hostId, this.address);
        tokenMetadata.updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), this.address);
        if (knownToNodes)
            Nodes.peers().update(this.address, info -> info.setHostId(hostId), true);
    }

    public FakePeer messagingVersion(int version)
    {
        MessagingService.instance().versions.set(address, version);
        return this;
    }

    /**
     * Forgets the messaging version of the peer, as a restart of this node does: messaging versions are only kept in
     * memory.
     */
    public FakePeer forgetMessagingVersion()
    {
        MessagingService.instance().versions.reset(address);
        return this;
    }

    public FakePeer releaseVersion(String version)
    {
        Nodes.peers().update(address, info -> info.setReleaseVersion(new CassandraVersion(version)), true);
        return this;
    }

    public FakePeer alive(boolean alive)
    {
        if (alive)
            state().markAlive();
        else
            state().markDead();
        return this;
    }

    private EndpointState state()
    {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(address);
        if (state == null)
        {
            state = new EndpointState(new HeartBeatState(1));
            state.markDead();
            Gossiper.instance.endpointStateMap.put(address, state);
        }
        return state;
    }

    @Override
    public void close()
    {
        StorageService.instance.getTokenMetadata().removeEndpoint(address);
        MessagingService.instance().versions.reset(address);
        Gossiper.instance.endpointStateMap.remove(address);
        Nodes.peers().remove(address, true, true);
    }

    @Override
    public String toString()
    {
        return address.toString();
    }
}
