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

import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.IPeerInfo;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Tells which nodes can't be relied on to support the features that need every node of a request on DataStax 5.0.10
 * or later, such as a query or a schema change that older nodes would reject or mis-evaluate. Such a request is only
 * accepted when no node is returned.
 * <p>
 * A node supports the features if it speaks {@link MessagingService#VERSION_DS_21} or a later DataStax version, as it
 * announced it and on every connection we have to it, whatever it runs. A node whose messaging version isn't known
 * counts as not supporting them while it's alive, or always if the caller asks (a schema change also reaches the nodes
 * that are down); a node that {@link Nodes} doesn't know can't be alive in gossip.
 */
public final class DataStaxFeatureSupport
{
    /** The results of {@link #checkRead} per keyspace and scope. */
    private static final PeerCheckCache<Set<InetAddressAndPort>> readChecks = new PeerCheckCache<>(DataStaxFeatureSupport::checkRead);

    private DataStaxFeatureSupport()
    {
    }

    /**
     * Returns the nodes that a read of the given keyspace at the given consistency level can contact, plus this node,
     * that can't be relied on to support the features: those of this node's datacenter at LOCAL_ONE, LOCAL_QUORUM and
     * LOCAL_SERIAL, every node otherwise. The result is reused until the nodes change (see {@link PeerCheckCache}) or a
     * node refuses a read (see {@link #refreshReadChecks}).
     */
    public static Set<InetAddressAndPort> nodesNotSupportingRead(String keyspace, ConsistencyLevel consistency)
    {
        return readChecks.get(keyspace, consistency);
    }

    private static Set<InetAddressAndPort> checkRead(String keyspace, @Nullable String datacenter)
    {
        // the endpoints with a host ID include the ring members, whose host ID is set before their tokens
        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadataForKeyspace(keyspace);
        Set<InetAddressAndPort> scoped = new HashSet<>();
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        for (InetAddressAndPort endpoint : tokenMetadata.getAllEndpoints())
        {
            if (datacenter == null || datacenter.equals(snitch.getDatacenter(endpoint)))
                scoped.add(endpoint);
        }
        return nodesNotSupporting(scoped, false);
    }

    /** Makes the reads of the given keyspace check their nodes again, as a node refused one of them. */
    public static void refreshReadChecks(String keyspace)
    {
        readChecks.invalidate(keyspace);
    }

    /**
     * Returns the nodes among the given endpoints, plus this node, that can't be relied on to support the features.
     *
     * @param endpoints the nodes involved, which may include this node
     * @param unknownIsUnsupported whether a node with an unknown messaging version counts as not supporting the
     * features even when it isn't alive
     * @return the nodes that can't be relied on to support the features, empty if there are none
     */
    public static Set<InetAddressAndPort> nodesNotSupporting(Iterable<InetAddressAndPort> endpoints,
                                                             boolean unknownIsUnsupported)
    {
        Set<InetAddressAndPort> nodes = new HashSet<>();

        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        if (!MessagingService.isDataStax21OrLater(MessagingService.current_version))
            nodes.add(local);

        MessagingService messaging = MessagingService.instance();
        for (InetAddressAndPort node : endpoints)
        {
            if (node.equals(local) || nodes.contains(node))
                continue;

            if (!supports(messaging, node, unknownIsUnsupported))
                nodes.add(node);
        }
        return nodes;
    }

    private static boolean supports(MessagingService messaging, InetAddressAndPort node, boolean unknownIsUnsupported)
    {
        Integer messagingVersion = messaging.versions.getRawIfKnown(node);
        if (messagingVersion != null && !MessagingService.isDataStax21OrLater(messagingVersion))
            return false;

        if (messaging.hasConnectionWithVersionNotMatching(node, MessagingService::isDataStax21OrLater))
            return false;

        if (messagingVersion != null)
            return true;

        // A node whose version isn't known counts only while it's alive, as it's about to announce it, unless every
        // node must be shown to support the features (a schema change also reaches the nodes that are down). Versions
        // are only kept in memory: after a restart of this node, a node that is still down is unknown until it's back.
        if (unknownIsUnsupported)
            return false;

        // A ring member that Nodes doesn't know (such as a peer of a custom token metadata provider) has never had its
        // gossip state installed here: gossip records a node in Nodes when it installs its state
        // (Gossiper#putEndpointState), before the node can be marked alive. So the failure detector shouldn't be asked
        // about it (it would log an "Unknown endpoint" error).
        IPeerInfo peer = Nodes.peers().get(node);
        if (peer == null || peer.isRemoved())
            return true;

        return !IFailureDetector.instance.isAlive(node);
    }
}
