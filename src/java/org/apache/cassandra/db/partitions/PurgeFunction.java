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
package org.apache.cassandra.db.partitions;

import java.util.function.LongPredicate;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;

public abstract class PurgeFunction extends Transformation<UnfilteredRowIterator>
{
    private final DeletionPurger purger;
    private final long nowInSec;

    private final boolean enforceStrictLiveness;
    private boolean isReverseOrder;

    private boolean ignoreGcGraceSeconds;
    private final long oldestUnrepairedTombstone;
    // Whether a range tombstone is open in the current partition, and whether it is purged. Its close bound is purged
    // with it: reads lower the oldest unrepaired tombstone when they open sources, which the index searchers do between
    // partitions or key batches, not between the bounds of a range tombstone, but a searcher that does keeps them paired
    private boolean rangeTombstoneOpen;
    private boolean openRangeTombstonePurged;

    public PurgeFunction(long nowInSec, long gcBefore, long oldestUnrepairedTombstone, boolean onlyPurgeRepairedTombstones,
                         boolean enforceStrictLiveness)
    {
        this.nowInSec = nowInSec;
        this.oldestUnrepairedTombstone = oldestUnrepairedTombstone;
        this.purger = (timestamp, localDeletionTime) ->
                      (localDeletionTime < gcBefore || ignoreGcGraceSeconds)
                      && !(onlyPurgeRepairedTombstones && localDeletionTime >= oldestUnrepairedTombstone())
                      && getPurgeEvaluator().test(timestamp);
        this.enforceStrictLiveness = enforceStrictLiveness;
    }

    // Called for each gc-able tombstone of a table that only purges repaired tombstones
    // Return the oldest unrepaired tombstone of the data read, no higher than the value given to the constructor
    protected long oldestUnrepairedTombstone()
    {
        return oldestUnrepairedTombstone;
    }

    protected abstract LongPredicate getPurgeEvaluator();

    // Called at the beginning of each new partition
    protected void onNewPartition(DecoratedKey partitionKey)
    {
    }

    // Called for each partition that had only purged infos and are empty post-purge.
    protected void onEmptyPartitionPostPurge(DecoratedKey partitionKey)
    {
    }

    // Called for every unfiltered. Meant for CompactionIterator to update progress
    protected void updateProgress()
    {
    }

    // Called at the beginning of each new partition
    // Return true if the current partitionKey ignores the gc_grace_seconds during compaction.
    protected boolean shouldIgnoreGcGrace()
    {
        return false;
    }

    protected void setReverseOrder(boolean isReverseOrder)
    {
        this.isReverseOrder = isReverseOrder;
    }

    @Override
    protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        onNewPartition(partition.partitionKey());

        ignoreGcGraceSeconds = shouldIgnoreGcGrace();
        rangeTombstoneOpen = false;

        setReverseOrder(partition.isReverseOrder());
        UnfilteredRowIterator purged = Transformation.apply(partition, this);
        if (purged.isEmpty())
        {
            onEmptyPartitionPostPurge(purged.partitionKey());
            purged.close();
            return null;
        }

        return purged;
    }

    @Override
    protected DeletionTime applyToDeletion(DeletionTime deletionTime)
    {
        return purger.shouldPurge(deletionTime) ? DeletionTime.LIVE : deletionTime;
    }

    @Override
    protected Row applyToStatic(Row row)
    {
        updateProgress();
        return row.purge(purger, nowInSec, enforceStrictLiveness);
    }

    @Override
    protected Row applyToRow(Row row)
    {
        updateProgress();
        return row.purge(purger, nowInSec, enforceStrictLiveness);
    }

    @Override
    protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        updateProgress();
        boolean reversed = isReverseOrder;
        if (marker.isBoundary())
        {
            // We can only skip the whole marker if both deletion time are purgeable.
            // If only one of them is, filterTombstoneMarker will deal with it.
            RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker)marker;
            boolean shouldPurgeClose = rangeTombstoneOpen ? openRangeTombstonePurged : purger.shouldPurge(boundary.closeDeletionTime(reversed));
            boolean shouldPurgeOpen = purger.shouldPurge(boundary.openDeletionTime(reversed));
            rangeTombstoneOpen = true;
            openRangeTombstonePurged = shouldPurgeOpen;

            if (shouldPurgeClose)
            {
                if (shouldPurgeOpen)
                    return null;

                return boundary.createCorrespondingOpenMarker(reversed);
            }

            return shouldPurgeOpen
                   ? boundary.createCorrespondingCloseMarker(reversed)
                   : marker;
        }
        else
        {
            boolean shouldPurge = rangeTombstoneOpen && marker.isClose(reversed)
                                  ? openRangeTombstonePurged
                                  : purger.shouldPurge(((RangeTombstoneBoundMarker)marker).deletionTime());
            rangeTombstoneOpen = marker.isOpen(reversed);
            openRangeTombstonePurged = shouldPurge;
            return shouldPurge ? null : marker;
        }
    }
}
