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

import java.util.NoSuchElementException;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;

/**
 * A transformation that appends an RT bound marker to row iterators in case they don't have one, and removes an RT
 * bound marker that opens an RT as their last item when the limits read no row after it.
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
    private final LastReadRow lastReadRow;

    private RTBoundCloser(LastReadRow lastReadRow)
    {
        this.lastReadRow = lastReadRow;
    }

    public static UnfilteredPartitionIterator close(UnfilteredPartitionIterator partitions)
    {
        return close(partitions, null);
    }

    /**
     * @param lastReadRow the {@link LastReadRow} applied to the iterator right before the limits that stop it, or null
     */
    public static UnfilteredPartitionIterator close(UnfilteredPartitionIterator partitions, LastReadRow lastReadRow)
    {
        return Transformation.apply(partitions, new RTBoundCloser(lastReadRow));
    }

    public static UnfilteredRowIterator close(UnfilteredRowIterator partition)
    {
        return close(partition, null);
    }

    private static UnfilteredRowIterator close(UnfilteredRowIterator partition, LastReadRow lastReadRow)
    {
        if (null != lastReadRow)
            partition = new TrailingOpenMarkerRemover(partition, lastReadRow);
        RowsTransformation transformation = new RowsTransformation(partition, lastReadRow);
        return Transformation.apply(MoreRows.extend(partition, transformation, partition.columns()), transformation);
    }

    @Override
    public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        return close(partition, lastReadRow);
    }

    /**
     * Tracks the last row that the limits read in the current partition after its last RT bound marker. GROUP BY limits
     * only know that a group is complete when they read a row of the next group, which they don't return.
     */
    public final static class LastReadRow extends Transformation<UnfilteredRowIterator>
    {
        private Clustering<?> clustering;

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            clustering = null;
            return Transformation.apply(partition, this);
        }

        @Override
        protected Row applyToRow(Row row)
        {
            clustering = row.clustering();
            return row;
        }

        @Override
        protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            clustering = null;
            return marker;
        }
    }

    /**
     * Removes an RT bound marker that opens an RT as the last item of a partition when the limits read no row after it,
     * or replaces such a boundary marker with its closing bound. This happens when GROUP BY limits stop as a partition
     * starts, as that partition still returns its first item. The groups that the limits counted all belong to the
     * previous partitions, so that RT covers none of their rows.
     */
    private final static class TrailingOpenMarkerRemover extends WrappingUnfilteredRowIterator
    {
        private final LastReadRow lastReadRow;
        private Unfiltered next;

        private TrailingOpenMarkerRemover(UnfilteredRowIterator wrapped, LastReadRow lastReadRow)
        {
            super(wrapped);
            this.lastReadRow = lastReadRow;
        }

        @Override
        public boolean hasNext()
        {
            if (null == next && wrapped.hasNext())
            {
                next = wrapped.next();
                if (next.isRangeTombstoneMarker()
                    && ((RangeTombstoneMarker) next).isOpen(isReverseOrder())
                    && !wrapped.hasNext()
                    && null == lastReadRow.clustering)
                {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker) next;
                    next = marker.isBoundary()
                           ? ((RangeTombstoneBoundaryMarker) marker).createCorrespondingCloseMarker(isReverseOrder())
                           : null;
                }
            }
            return null != next;
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

    private final static class RowsTransformation extends Transformation implements MoreRows<UnfilteredRowIterator>
    {
        private final UnfilteredRowIterator partition;
        private final LastReadRow lastReadRow;

        private Clustering<?> lastRowClustering;
        private DeletionTime openMarkerDeletionTime;

        private RowsTransformation(UnfilteredRowIterator partition, LastReadRow lastReadRow)
        {
            this.partition = partition;
            this.lastReadRow = lastReadRow;
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
             * there *is* an open RT in the stream, so DataLimits stopped the iterator short in the middle of an RT. If
             * they read a row that they didn't return, the RT covers the rows of the other replicas up to that row,
             * which can belong to the group being counted: create an artificial exclusive closing RT bound right before
             * that row. Otherwise, create an artificial inclusive closing RT bound at the last returned row. With a
             * LastReadRow, an opening bound followed by neither row has been removed by TrailingOpenMarkerRemover.
             */
            boolean reversed = partition.isReverseOrder();
            Clustering<?> lastReadClustering = null == lastReadRow ? null : lastReadRow.clustering;
            if (null == lastReadClustering && null == lastRowClustering)
            {
                String message = String.format("UnfilteredRowIterator for %s has an open RT bound as its last item", partition.metadata());
                throw new IllegalStateException(message);
            }

            RangeTombstoneBoundMarker closingBound;
            if (null == lastReadClustering
                || (null != lastRowClustering && partition.metadata().comparator.compare(lastReadClustering, lastRowClustering) == 0))
                closingBound = RangeTombstoneBoundMarker.inclusiveClose(reversed, lastRowClustering, openMarkerDeletionTime);
            else
                closingBound = RangeTombstoneBoundMarker.exclusiveClose(reversed, lastReadClustering, openMarkerDeletionTime);

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
