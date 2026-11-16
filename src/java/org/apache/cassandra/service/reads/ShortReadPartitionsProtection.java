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

package org.apache.cassandra.service.reads;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.db.transform.MoreRows;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.QueryInfoTracker;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.NoSpamLogger;

public class ShortReadPartitionsProtection extends Transformation<UnfilteredRowIterator> implements MorePartitions<UnfilteredPartitionIterator>
{
    private static final Logger logger = LoggerFactory.getLogger(ShortReadPartitionsProtection.class);
    private static final NoSpamLogger oneMinuteLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private final ReadCommand command;
    private final Replica source;

    private UnfilteredPartitionIterator firstResponse; // the replica's response to the command, until moreContents() replaces it
    private ShortReadRowsProtection lastRowsProtection; // the protection of the last partition of the replica's response

    private final Runnable preFetchCallback; // called immediately before fetching more contents

    protected final DataLimits.Counter singleResultCounter; // unmerged per-source counter
    private final DataLimits.Counter mergedResultCounter; // merged end-result counter

    private DecoratedKey lastPartitionKey; // key of the last observed partition

    private boolean partitionsFetched; // whether we've seen any new partitions since iteration start or last moreContents() call
    protected boolean rangeFetched = false; // fetched by original read request or SRP request

    private final Dispatcher.RequestTime requestTime;

    public ShortReadPartitionsProtection(ReadCommand command,
                                         Replica source,
                                         UnfilteredPartitionIterator firstResponse,
                                         Runnable preFetchCallback,
                                         DataLimits.Counter singleResultCounter,
                                         DataLimits.Counter mergedResultCounter,
                                         Dispatcher.RequestTime requestTime)
    {
        this.command = command;
        this.source = source;
        this.firstResponse = firstResponse;
        this.preFetchCallback = preFetchCallback;
        this.singleResultCounter = singleResultCounter;
        this.mergedResultCounter = mergedResultCounter;
        this.requestTime = requestTime;
    }

    @Override
    public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        partitionsFetched = true;
        rangeFetched = true;

        /*
         * A replica whose GROUP BY counter reaches the limit as a new partition starts still returns that partition,
         * without its static row and rows, which belong to the next page (see GroupByAwareCounter#applyToStatic).
         * Drop it and leave lastPartitionKey alone, so that moreContents() asks for the partitions after the previous
         * one with row limits (see DataLimits#forShortReadRetry) and gets this partition whole.
         */
        if (isCutByGroupByLimit(partition))
        {
            partition.close();
            return null;
        }

        lastPartitionKey = partition.partitionKey();

        /*
         * Extend for moreContents() then apply protection to track lastClustering by applyToRow().
         *
         * If we don't apply the transformation *after* extending the partition with MoreRows,
         * applyToRow() method of protection will not be called on the first row of the new extension iterator.
         */
        ReplicaPlan.ForTokenRead replicaPlan = ReplicaPlans.forSingleReplicaRead(Keyspace.open(command.metadata().keyspace), partition.partitionKey().getToken(), source);
        ReplicaPlan.SharedForTokenRead sharedReplicaPlan = ReplicaPlan.shared(replicaPlan);
        ShortReadRowsProtection protection = new ShortReadRowsProtection(partition.partitionKey(),
                                                                         command, source,
                                                                         (cmd) -> executeReadCommand(cmd, sharedReplicaPlan),
                                                                         singleResultCounter,
                                                                         mergedResultCounter,
                                                                         () -> firstResponse != null && firstResponse.hasNext(),
                                                                         firstResponse != null);
        if (firstResponse != null)
            lastRowsProtection = protection;
        return Transformation.apply(MoreRows.extend(partition, protection), protection);
    }

    /**
     * Whether the given partition is the empty last partition of the response of a replica that reached a GROUP BY limit.
     * The response can only tell whether it has more partitions once the current one is consumed, hence the order.
     */
    private boolean isCutByGroupByLimit(UnfilteredRowIterator partition)
    {
        return firstResponse != null
               && command.limits().isGroupByLimit()
               && !command.isLimitedToOnePartition() // moreContents() only runs for range reads
               && !command.limits().isReplicaExhausted(singleResultCounter)
               // the start of the next partition completes the merged result, which then needs nothing of this one
               && command.limits().isReplicaExhausted(mergedResultCounter)
               && partition.staticRow().isEmpty()
               && !partition.hasNext()
               && !firstResponse.hasNext();
    }

    /*
     * We only get here once all the rows and partitions in this iterator have been iterated over, and so
     * if the node had returned the requested number of rows but we still get here, then some results were
     * skipped during reconciliation.
     */
    public UnfilteredPartitionIterator moreContents()
    {
        // never try to request additional partitions from replicas if our reconciled partitions are already filled to the limit
        // (the first phase of replica filtering protection doesn't stop at the limit, see DataResolver)
        if (mergedResultCounter.isDone())
            return null;

        // we do not apply short read protection when we have no limits at all
        assert !command.limits().isUnlimited();

        /*
         * If this is a single partition read command or an (indexed) partition range read command with
         * a partition key specified, then we can't and shouldn't try fetch more partitions.
         */
        assert !command.isLimitedToOnePartition();

        boolean inFirstResponse = firstResponse != null;
        firstResponse = null;

        /*
         * If the returned result doesn't have enough rows/partitions to satisfy even the original limit, don't ask for more.
         *
         * Can only take the short cut if there is no per partition limit set. Otherwise it's possible to hit false
         * positives due to some rows being uncounted for in certain scenarios (see CASSANDRA-13911).
         */
        if (command.limits().isReplicaExhausted(singleResultCounter) && command.limits().perPartitionCount() == DataLimits.NO_LIMIT)
            return null;

        // the merged result needs no more rows from the replica (GROUP BY, see DataLimits#isCompleteWithLastRowOf)
        if (inFirstResponse && (lastRowsProtection != null
                                ? lastRowsProtection.isCompleteWithLastRow()
                                : command.limits().isCompleteWithLastRowOf(mergedResultCounter, singleResultCounter, null, null,
                                                                           command.metadata().comparator, command.isReversed())))
            return null;

        /*
         * Either we had an empty iterator as the initial response, or our moreContents() call got us an empty iterator.
         * There is no point to ask the replica for more rows - it has no more in the requested range.
         */
        if (rangeExhausted())
            return null;
        partitionsFetched = false;
        rangeFetched = true;

        /*
         * Request one row per remaining result, or use the per-partition limit if there is no total limit.
         * For GROUP BY, count() and counted() measure groups. ShortReadRowsProtection fetches any additional
         * rows needed to complete those groups.
         */
        int toQuery = command.limits().count() != DataLimits.NO_LIMIT
                      ? command.limits().count() - mergedResultCounter.counted()
                      : command.limits().perPartitionCount();

        ColumnFamilyStore.metricsFor(command.metadata().id).shortReadProtectionRequests.mark();
        Tracing.trace("Requesting {} extra rows from {} for short read protection", toQuery, source);
        // This is a NoSpamLogger because, in the event of unrepaired data or missing data on nodes in
        // a cluster, we can end up spamming the logs with this message
        oneMinuteLogger.info("Requesting {} extra rows from {} for short read protection", toQuery, source);

        // If we've arrived here, all responses have been consumed, and we're about to request more.
        preFetchCallback.run();

        return makeAndExecuteFetchAdditionalPartitionReadCommand(toQuery);
    }

    public boolean rangeExhausted()
    {
        return !partitionsFetched;
    }

    private UnfilteredPartitionIterator makeAndExecuteFetchAdditionalPartitionReadCommand(int toQuery)
    {
        PartitionRangeReadCommand cmd = (PartitionRangeReadCommand) command;

        DataLimits newLimits = cmd.limits().forShortReadRetry(toQuery);

        // lastPartitionKey is null when the replica hasn't returned any partition of the range, as when the only one was
        // cut by a GROUP BY limit: read the range again
        DataRange newDataRange = cmd.dataRange();
        if (lastPartitionKey != null)
        {
            AbstractBounds<PartitionPosition> bounds = newDataRange.keyRange();
            AbstractBounds<PartitionPosition> newBounds = bounds.inclusiveRight()
                                                          ? new Range<>(lastPartitionKey, bounds.right)
                                                          : new ExcludingBounds<>(lastPartitionKey, bounds.right);
            newDataRange = newDataRange.forSubRange(newBounds);
        }

        ReplicaPlan.ForRangeRead replicaPlan = ReplicaPlans.forSingleReplicaRead(Keyspace.open(command.metadata().keyspace), cmd.dataRange().keyRange(), source, 1);
        return executeReadCommand(cmd.withUpdatedLimitsAndDataRange(newLimits, newDataRange), ReplicaPlan.shared(replicaPlan));
    }

    private <E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
    UnfilteredPartitionIterator executeReadCommand(ReadCommand cmd, ReplicaPlan.Shared<E, P> replicaPlan)
    {
        DataResolver<E, P> resolver = new DataResolver<>(cmd,
                                                         replicaPlan,
                                                         (NoopReadRepair<E, P>)NoopReadRepair.instance,
                                                         requestTime,
                                                         QueryInfoTracker.ReadTracker.NOOP);
        ReadCallback<E, P> handler = new ReadCallback<>(resolver, cmd, replicaPlan, requestTime);

        if (source.isSelf())
        {
            Stage.READ.maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(cmd, handler, requestTime));
        }
        else
        {
            if (source.isTransient())
                cmd = cmd.copyAsTransientQuery(source);
            MessagingService.instance().sendWithCallback(cmd.createMessage(false, requestTime), source.endpoint(), handler);
        }

        // We don't call handler.get() because we want to preserve tombstones since we're still in the middle of merging node results.
        handler.awaitResults();
        assert resolver.getMessages().size() == 1;
        return resolver.getMessages().get(0).payload.makeIterator(command);
    }
}
