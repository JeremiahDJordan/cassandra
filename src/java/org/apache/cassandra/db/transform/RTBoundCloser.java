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
package org.apache.cassandra.db.transform;

import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;

/**
 * A transformation that appends an RT bound marker to row iterators in case they don't have one.
 *
 * This used to happen, for example, in {@link org.apache.cassandra.db.ReadCommand#executeLocally(ReadExecutionController)}
 * if {@link org.apache.cassandra.db.filter.DataLimits} stopped the iterator on a live row that was enclosed in an
 * older RT.
 *
 * If we don't do this, and send a response without the closing bound, we can break read/short read protection read
 * isolation, and potentially cause data loss.
 *
 * See CASSANDRA-14515 for context.
 */
public final class RTBoundCloser extends Transformation<UnfilteredRowIterator>
{
    @Nullable
    private final LastRowTracker tracker;

    private RTBoundCloser(@Nullable LastRowTracker tracker)
    {
        this.tracker = tracker;
    }

    public static UnfilteredPartitionIterator close(UnfilteredPartitionIterator partitions)
    {
        return Transformation.apply(partitions, new RTBoundCloser(null));
    }

    /**
     * Applies {@code limits} to {@code partitions}, and appends an RT bound marker to the resulting row iterators in
     * case they don't have one. GROUP BY limits read one row past the last one they return; an RT opened right before
     * that row is closed at it.
     */
    public static UnfilteredPartitionIterator close(UnfilteredPartitionIterator partitions,
                                                    Function<UnfilteredPartitionIterator, UnfilteredPartitionIterator> limits)
    {
        LastRowTracker tracker = new LastRowTracker();
        return Transformation.apply(limits.apply(Transformation.apply(partitions, tracker)), new RTBoundCloser(tracker));
    }

    public static UnfilteredRowIterator close(UnfilteredRowIterator partition)
    {
        RowsTransformation transformation = new RowsTransformation(partition, null);
        return Transformation.apply(MoreRows.extend(partition, transformation, partition.columns()), transformation);
    }

    @Override
    public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        RowsTransformation transformation = new RowsTransformation(partition, tracker);
        return Transformation.apply(MoreRows.extend(partition, transformation, partition.columns()), transformation);
    }

    /**
     * Tracks the clustering of the last row of the current partition that follows the last RT bound marker, before the
     * limits are applied.
     */
    private final static class LastRowTracker extends Transformation<UnfilteredRowIterator>
    {
        private Clustering<?> lastRowClustering;

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            lastRowClustering = null;
            return Transformation.apply(partition, this);
        }

        @Override
        protected Row applyToRow(Row row)
        {
            lastRowClustering = row.clustering();
            return row;
        }

        @Override
        protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            lastRowClustering = null;
            return marker;
        }
    }

    private final static class RowsTransformation extends Transformation implements MoreRows<UnfilteredRowIterator>
    {
        private final UnfilteredRowIterator partition;
        @Nullable
        private final LastRowTracker tracker;

        private Clustering<?> lastRowClustering;
        private DeletionTime openMarkerDeletionTime;

        private RowsTransformation(UnfilteredRowIterator partition, @Nullable LastRowTracker tracker)
        {
            this.partition = partition;
            this.tracker = tracker;
        }

        @Override
        public Row applyToRow(Row row)
        {
            lastRowClustering = row.clustering();
            return row;
        }

        @Override
        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            openMarkerDeletionTime =
                marker.isOpen(partition.isReverseOrder()) ? marker.openDeletionTime(partition.isReverseOrder()) : null;
            lastRowClustering = null;
            return marker;
        }

        @Override
        public UnfilteredRowIterator moreContents()
        {
            // there is no open RT in the stream - nothing for us to do
            if (null == openMarkerDeletionTime)
                return null;

            /*
             * there *is* an open RT in the stream, but there have been no rows after the opening bound - this must
             * never happen in scenarios where RTBoundCloser is meant to be used; the last encountered clustering
             * should be either a closing bound marker - if the iterator was exhausted fully - or a live row - if
             * DataLimits stopped it short in the middle of an RT. The exception is a row that the limits read but
             * did not return, which follows the opening bound and so is covered by the RT.
             */
            if (null == lastRowClustering && null != tracker)
                lastRowClustering = tracker.lastRowClustering;

            if (null == lastRowClustering)
            {
                String message = String.format("UnfilteredRowIterator for %s has an open RT bound as its last item", partition.metadata());
                throw new IllegalStateException(message);
            }

            // create an artificial inclusive closing RT bound with bound matching last seen row's clustering
            RangeTombstoneBoundMarker closingBound =
                RangeTombstoneBoundMarker.inclusiveClose(partition.isReverseOrder(), lastRowClustering, openMarkerDeletionTime);

            return UnfilteredRowIterators.singleton(closingBound,
                                                    partition.metadata(),
                                                    partition.partitionKey(),
                                                    partition.partitionLevelDeletion(),
                                                    partition.columns(),
                                                    partition.staticRow(),
                                                    partition.isReverseOrder(),
                                                    partition.stats());
        }
    }
}
