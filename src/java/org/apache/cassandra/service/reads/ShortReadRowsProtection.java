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

import java.util.NoSuchElementException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.db.transform.MoreRows;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;

class ShortReadRowsProtection extends Transformation implements MoreRows<UnfilteredRowIterator>
{
    private final ReadCommand command;
    private final Replica source;
    private final DataLimits.Counter singleResultCounter; // unmerged per-source counter
    private final DataLimits.Counter mergedResultCounter; // merged end-result counter
    private final Function<ReadCommand, UnfilteredPartitionIterator> commandExecutor;
    private final TableMetadata metadata;
    private final DecoratedKey partitionKey;
    private final BooleanSupplier hasNextPartition; // whether the replica's response has a partition after this one
    private final boolean inFirstResponse; // whether the partition is from the replica's response to the command

    private Clustering<?> lastClustering; // clustering of the last observed row
    private boolean hasObservedMarkers; // whether any range tombstone marker was observed
    private ClusteringBound<?> lastClosingBound; // bound of the last observed item, if it is a marker that closes an RT

    private int lastCounted = 0; // last seen recorded # before attempting to fetch more rows
    private int lastFetched = 0; // # rows returned by last attempt to get more (or by the original read command)
    private int lastQueried = 0; // # extra rows requested from the replica last time
    private int lastBytesCounted = 0; // last seen recorded # of bytes before attempting to fetch more rows

    ShortReadRowsProtection(DecoratedKey partitionKey, ReadCommand command, Replica source,
                            Function<ReadCommand, UnfilteredPartitionIterator> commandExecutor,
                            DataLimits.Counter singleResultCounter, DataLimits.Counter mergedResultCounter,
                            BooleanSupplier hasNextPartition, boolean inFirstResponse)
    {
        this.command = command;
        this.source = source;
        this.commandExecutor = commandExecutor;
        this.singleResultCounter = singleResultCounter;
        this.mergedResultCounter = mergedResultCounter;
        this.metadata = command.metadata();
        this.partitionKey = partitionKey;
        this.hasNextPartition = hasNextPartition;
        this.inFirstResponse = inFirstResponse;
    }

    @Override
    public Row applyToRow(Row row)
    {
        lastClustering = row.clustering();
        lastClosingBound = null;
        return row;
    }

    @Override
    public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        hasObservedMarkers = true;
        lastClosingBound = marker.isClose(command.isReversed()) ? marker.closeBound(command.isReversed()) : null;
        return marker;
    }

    /*
     * We only get here once all the rows in this iterator have been iterated over, and so if the node
     * had returned the requested number of rows but we still get here, then some results were skipped
     * during reconciliation.
     */
    public UnfilteredRowIterator moreContents()
    {
        // never try to request additional rows from replicas if our reconciled partition is already filled to the limit
        // (the first phase of replica filtering protection doesn't stop at the limit, see DataResolver)
        if (mergedResultCounter.isDoneForPartition())
            return null;

        // we do not apply short read protection when we have no limits at all
        assert !command.limits().isUnlimited();

        /*
         * If the returned partition doesn't have enough rows to satisfy even the original limit, don't ask for more.
         *
         * Can only take the short cut if there is no per partition limit set. Otherwise it's possible to hit false
         * positives due to some rows being uncounted for in certain scenarios (see CASSANDRA-13911).
         */
        if (command.limits().isReplicaExhausted(singleResultCounter) && command.limits().perPartitionCount() == DataLimits.NO_LIMIT)
            return null;

        /*
         * A replica without a per partition limit only goes on to another partition once this one has no more rows. A
         * GROUP BY response needs this: the replica that returned as many groups as the limit isn't exhausted, as far
         * as the counter can tell, even when its last group is followed by the partition that closed it.
         */
        if (command.limits().perPartitionCount() == DataLimits.NO_LIMIT && hasNextPartition.getAsBoolean())
            return null;

        if (isCompleteWithLastRow())
            return null;

        /*
         * If the replica has no live rows in the partition, don't try to fetch more.
         *
         * Note that the previous branch [if (!singleResultCounter.isDoneForPartition()) return null] doesn't
         * always cover this scenario:
         * isDoneForPartition() is defined as [isDone() || rowInCurrentPartition >= perPartitionLimit],
         * and will return true if isDone() returns true, even if there are 0 rows counted in the current partition.
         *
         * This can happen with a range read if after 1+ rounds of short read protection requests we managed to fetch
         * enough extra rows for other partitions to satisfy the singleResultCounter's total row limit, but only
         * have tombstones in the current partition.
         *
         * One other way we can hit this condition is when the partition only has a live static row and no regular
         * rows. In that scenario the counter will remain at 0 until the partition is closed - which happens after
         * the moreContents() call.
         */
        if (singleResultCounter.rowsCountedInCurrentPartition() == 0)
            return null;

        /*
         * If the replica returned range tombstone markers but no rows in the partition, the merged result needs nothing
         * more of it: either the replica read the whole partition, or its limits stopped as the partition started, or,
         * in the partition a page continues, at the first row of the next group, where the merged result's open group,
         * the page's continued group, ends as well.
         */
        if (lastClustering == null && hasObservedMarkers)
            return null;

        /*
         * This is a table with no clustering columns, and has at most one row per partition - with EMPTY clustering.
         * We already have the row, so there is no point in asking for more from the partition.
         */
        if (lastClustering != null && lastClustering.isEmpty())
            return null;

        lastFetched = singleResultCounter.rowsCountedInCurrentPartition() - lastCounted;
        lastCounted = singleResultCounter.rowsCountedInCurrentPartition();
        int lastFetchedBytes = singleResultCounter.bytesCounted() - lastBytesCounted;
        lastBytesCounted = singleResultCounter.bytesCounted();

        // getting back fewer rows than we asked for means the partition on the replica has been fully consumed, unless
        // the replica stopped at the byte limit of the request (byte-sized pages or the page weight guardrail)
        if (lastQueried > 0 && lastFetched < lastQueried && !reachedBytesLimit(lastFetchedBytes))
            return null;

        /*
         * At this point we know that:
         *     1. the replica returned [repeatedly?] as many rows as we asked for and potentially has more
         *        rows in the partition
         *     2. at least one of those returned rows was shadowed by a tombstone returned from another
         *        replica
         *     3. we haven't satisfied the client's limits yet, and should attempt to query for more rows to
         *        avoid a short read
         *
         * In the ideal scenario, we would get exactly min(a, b) or fewer rows from the next request, where a and b
         * are defined as follows:
         *     [a] limits.count() - mergedResultCounter.counted()
         *     [b] limits.perPartitionCount() - mergedResultCounter.countedInCurrentPartition()
         *
         * It would be naive to query for exactly that many rows, as it's possible and not unlikely
         * that some of the returned rows would also be shadowed by tombstones from other hosts.
         *
         * Note: we don't know, nor do we care, how many rows from the replica made it into the reconciled result;
         * we can only tell how many in total we queried for, and that [0, mrc.countedInCurrentPartition()) made it.
         *
         * In general, our goal should be to minimise the number of extra requests - *not* to minimise the number
         * of rows fetched: there is a high transactional cost for every individual request, but a relatively low
         * marginal cost for each extra row requested.
         *
         * As such it's better to overfetch than to underfetch extra rows from a host; but at the same
         * time we want to respect paging limits and not blow up spectacularly.
         *
         * Note: it's ok to retrieve more rows that necessary since singleResultCounter is not stopping and only
         * counts.
         *
         * With that in mind, we'll just request the minimum of (count(), perPartitionCount()) limits.
         *
         * See CASSANDRA-13794 for more details.
         */
        int missing = Math.min(command.limits().count(), command.limits().perPartitionCount());
        lastQueried = ShortReadProtection.fetchSize(command, missing, lastQueried > 0 ? lastQueried : missing);
        // The request keeps the per-partition limit (see DataLimits#forShortReadRetry), so the replica never returns more
        // rows than that. Asking for more would make the check above take a full response for the end of the partition.
        lastQueried = Math.min(lastQueried, command.limits().perPartitionCount());

        ColumnFamilyStore.metricsFor(metadata.id).shortReadProtectionRequests.mark();
        Tracing.trace("Requesting {} extra rows from {} for short read protection", lastQueried, source);

        SinglePartitionReadCommand cmd = makeFetchAdditionalRowsReadCommand(lastQueried);
        UnfilteredRowIterator rows = UnfilteredPartitionIterators.getOnlyElement(commandExecutor.apply(cmd), cmd);
        return null == lastClosingBound ? rows : new RowsAfterClosingBound(rows, lastClosingBound);
    }

    /**
     * Whether the merged result needs no more rows from the replica, whose last row so far is the last row of this
     * partition (see DataLimits#isCompleteWithLastRowOf). GROUP BY responses need this: a replica that returned as many
     * groups as the limit isn't exhausted, as far as its counter can tell, also when the replicas agree.
     */
    boolean isCompleteWithLastRow()
    {
        return command.limits().isCompleteWithLastRowOf(mergedResultCounter,
                                                        inFirstResponse && lastQueried == 0 ? singleResultCounter : null,
                                                        lastClustering == null ? null : partitionKey,
                                                        lastClustering,
                                                        metadata.comparator,
                                                        command.isReversed());
    }

    /**
     * Skips the items of a short read protection response up to the RT closing bound that the replica returned last.
     * The response starts after the last row that the replica returned, so it repeats the RT bound markers between that
     * row and that closing bound: the bound where the RT ends, or the bound that RTBoundCloser adds where the replica
     * stopped in the RT, which can be right before a row that the replica read without returning it. An RT that is
     * still open at that closing bound is opened again at the same position.
     */
    private static final class RowsAfterClosingBound implements WrappingUnfilteredRowIterator
    {
        private final UnfilteredRowIterator wrapped;
        private final ClusteringBound<?> closingBound;
        private boolean isPastClosingBound;
        private Unfiltered next;
        private Unfiltered afterNext;

        private RowsAfterClosingBound(UnfilteredRowIterator wrapped, ClusteringBound<?> closingBound)
        {
            this.wrapped = wrapped;
            this.closingBound = closingBound;
        }

        @Override
        public UnfilteredRowIterator wrapped()
        {
            return wrapped;
        }

        @Override
        public boolean hasNext()
        {
            if (null != next)
                return true;

            if (null != afterNext)
            {
                next = afterNext;
                afterNext = null;
            }
            else if (!isPastClosingBound)
            {
                skipToClosingBound();
            }
            else if (wrapped.hasNext())
            {
                next = wrapped.next();
            }
            return null != next;
        }

        private void skipToClosingBound()
        {
            isPastClosingBound = true;
            DeletionTime openDeletion = null;
            while (wrapped.hasNext())
            {
                Unfiltered unfiltered = wrapped.next();
                int cmp = metadata().comparator.compare(unfiltered.clustering(), closingBound);
                if (isReverseOrder() ? cmp < 0 : cmp > 0)
                {
                    if (null == openDeletion)
                    {
                        next = unfiltered;
                    }
                    else
                    {
                        next = new RangeTombstoneBoundMarker(closingBound.invert(), openDeletion);
                        afterNext = unfiltered;
                    }
                    return;
                }

                if (unfiltered.isRangeTombstoneMarker())
                {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
                    openDeletion = marker.isOpen(isReverseOrder()) ? marker.openDeletionTime(isReverseOrder()) : null;
                }
            }
        }

        @Override
        public Unfiltered next()
        {
            if (!hasNext())
                throw new NoSuchElementException();

            Unfiltered unfiltered = next;
            next = null;
            return unfiltered;
        }
    }

    /**
     * Whether a replica response of the given size reached the byte limit of the read. The requests of short read
     * protection keep that limit (see {@link DataLimits#forShortReadRetry}), and the replica stops after the row that
     * reaches it. The unmerged counter of the replica's responses counts the same rows, at no fewer bytes (see
     * {@link DataLimits.Counter#countingFullRowSize()}).
     */
    private boolean reachedBytesLimit(int fetchedBytes)
    {
        int bytesLimit = command.limits().bytes();
        return bytesLimit != DataLimits.NO_LIMIT && fetchedBytes >= bytesLimit;
    }

    private SinglePartitionReadCommand makeFetchAdditionalRowsReadCommand(int toQuery)
    {
        ClusteringIndexFilter filter = command.clusteringIndexFilter(partitionKey);
        if (null != lastClustering)
            filter = filter.forPaging(metadata.comparator, lastClustering, false);

        return SinglePartitionReadCommand.create(command.metadata(),
                                                 command.nowInSec(),
                                                 command.columnFilter(),
                                                 command.rowFilter(),
                                                 command.limits().forShortReadRetry(toQuery),
                                                 partitionKey,
                                                 filter,
                                                 command.indexQueryPlan());
    }
}
