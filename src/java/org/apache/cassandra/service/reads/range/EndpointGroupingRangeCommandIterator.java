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

package org.apache.cassandra.service.reads.range;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.QueryInfoTracker;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A range command iterator that executes requests by endpoints and then merges responses in token order. It's designed to
 * reduce the number of range requests when scanning the whole token ring (eg. rows per range is low) for all range
 * reads that don't use digests and also to reduce the amount of disk-access for storage-attached indexes, as they will
 * be able to read index content for all required ranges at once.
 *
 * <ul>
 *     <li> With the non-grouping range command iterator, scanning the entire ring requires "num_of_nodes * num_of_tokens * consistency"
 *     range requests (assuming no ranges are merged by {@link ReplicaPlanMerger}) to their respective replicas.
 *
 *     <li> With the endpoint grouping range command iterator, scanning the entire ring only requires at most "num_of_nodes" multi-range
 *     requests to their respective replicas. So coordinator will cache up to "num_of_nodes" responses.
 * </ul>
 *
 * Nodes on a messaging version below {@link MessagingService#VERSION_DS_10}, such as Apache Cassandra nodes, don't know
 * the multi-range requests, so a batch of ranges that has such a replica is queried as {@link
 * NonGroupingRangeCommandIterator} does.
 */
public class EndpointGroupingRangeCommandIterator extends NonGroupingRangeCommandIterator
{
    EndpointGroupingRangeCommandIterator(CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans,
                                         PartitionRangeReadCommand command,
                                         int concurrencyFactor,
                                         int maxConcurrencyFactor,
                                         int totalRangeCount,
                                         Dispatcher.RequestTime requestTime,
                                         QueryInfoTracker.ReadTracker readTracker)
    {
        super(replicaPlans, command, concurrencyFactor, maxConcurrencyFactor, totalRangeCount, requestTime, readTracker);
    }

    @Override
    protected PartitionIterator sendNextRequests()
    {
        // the plans of one batch, as both EndpointGroupingCoordinator and sendNextRangeRequests take them
        List<ReplicaPlan.ForRangeRead> plans = new ArrayList<>();
        Set<InetAddressAndPort> endpoints = new HashSet<>();
        for (int vnodeRanges = 0; vnodeRanges < concurrencyFactor() && replicaPlans.hasNext(); )
        {
            ReplicaPlan.ForRangeRead plan = replicaPlans.next();
            plans.add(plan);
            vnodeRanges += plan.vnodeCount();
            for (Replica replica : plan.contacts())
                endpoints.add(replica.endpoint());
        }

        if (!endpoints.stream().allMatch(EndpointGroupingRangeCommandIterator::canSendMultiRangeRead))
        {
            Tracing.trace("Not grouping range requests by endpoint, as a replica may be on a messaging version without multi-range reads");
            return sendNextRangeRequests(plans.iterator());
        }

        counter = command.createUnlimitedCounter(true);

        EndpointGroupingCoordinator coordinator = new EndpointGroupingCoordinator(command,
                                                                                  counter,
                                                                                  plans.iterator(),
                                                                                  concurrencyFactor(),
                                                                                  requestTime,
                                                                                  readTracker);
        PartitionIterator partitions = coordinator.execute();

        rangesQueried += coordinator.vnodeRanges();
        batchesRequested++;
        Tracing.trace("Submitted concurrent grouped range read requests to {} endpoints", coordinator.endpoints());
        return partitions;
    }

    /**
     * Whether the given node can be sent a multi-range read: this node and the given one are on DS 10 or later, as is
     * every messaging connection to it. A node whose version isn't known yet may be below DS 10.
     */
    @VisibleForTesting
    static boolean canSendMultiRangeRead(InetAddressAndPort endpoint)
    {
        if (MessagingService.current_version < MessagingService.VERSION_DS_10)
            return false;

        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return true;

        MessagingService messaging = MessagingService.instance();
        Integer version = messaging.versions.getRawIfKnown(endpoint);
        return version != null
               && version >= MessagingService.VERSION_DS_10
               && !messaging.hasConnectionWithVersionNotMatching(endpoint, v -> v >= MessagingService.VERSION_DS_10);
    }
}
