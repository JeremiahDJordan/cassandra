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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.github.jamm.Unmetered;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * A thread-safe and atomic Partition implementation.
 *
 * Operations (in particular addAll) on this implementation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all rows have
 * been added.
 */
public final class AtomicBTreePartition extends AbstractBTreePartition
{
    private static final Logger logger = LoggerFactory.getLogger(AtomicBTreePartition.class);

    public static final long EMPTY_SIZE = ObjectSizes.measure(new AtomicBTreePartition(null,
                                                                                       DatabaseDescriptor.getPartitioner().decorateKey(ByteBuffer.allocate(1)),
                                                                                       null));

    // Reserved values for wasteTracker field. These values must not be consecutive (see avoidReservedValues)
    private static final int TRACKER_NEVER_WASTED = 0;
    private static final int TRACKER_PESSIMISTIC_LOCKING = Integer.MAX_VALUE;

    // The granularity with which we track wasted allocation/work; we round up
    private static final int ALLOCATION_GRANULARITY_BYTES = 1024;
    // The number of bytes we have to waste in excess of our acceptable realtime rate of waste (defined below)
    private static final long EXCESS_WASTE_BYTES = 10 * 1024 * 1024L;
    private static final int EXCESS_WASTE_OFFSET = (int) (EXCESS_WASTE_BYTES / ALLOCATION_GRANULARITY_BYTES);
    // Note this is a shift, because dividing a long time and then picking the low 32 bits doesn't give correct rollover behavior
    private static final int CLOCK_SHIFT = 17;
    // CLOCK_GRANULARITY = 1^9ns >> CLOCK_SHIFT == 132us == (1/7.63)ms

    private static final AtomicIntegerFieldUpdater<AtomicBTreePartition> wasteTrackerUpdater = AtomicIntegerFieldUpdater.newUpdater(AtomicBTreePartition.class, "wasteTracker");
    private static final AtomicReferenceFieldUpdater<AtomicBTreePartition, BTreePartitionData> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicBTreePartition.class, BTreePartitionData.class, "ref");

    /**
     * (clock + allocation) granularity are combined to give us an acceptable (waste) allocation rate that is defined by
     * the passage of real time of ALLOCATION_GRANULARITY_BYTES/CLOCK_GRANULARITY, or in this case 7.63KiB/ms, or 7.45Mb/s
     *
     * in wasteTracker we maintain within EXCESS_WASTE_OFFSET before the current time; whenever we waste bytes
     * we increment the current value if it is within this window, and set it to the min of the window plus our waste
     * otherwise.
     */
    private volatile int wasteTracker = TRACKER_NEVER_WASTED;

    @Unmetered
    private final MemtableAllocator allocator;

    private volatile BTreePartitionData ref;

    @Unmetered
    private final TableMetadataRef metadata;

    public AtomicBTreePartition(TableMetadataRef metadata, DecoratedKey partitionKey, MemtableAllocator allocator)
    {
        // involved in potential bug? partition columns may be a subset if we alter columns while it's in memtable
        super(partitionKey);
        this.metadata = metadata;
        this.allocator = allocator;
        this.ref = BTreePartitionData.EMPTY;
    }

    protected BTreePartitionData holder()
    {
        return ref;
    }

    public TableMetadata metadata()
    {
        return metadata.get();
    }

    protected boolean canHaveShadowedData()
    {
        return true;
    }

    /**
     * Adds a given update to this in-memtable partition.
     * <p>
     * Once the update is applied, {@code accountForApplied} is called with the updater, for the caller's bookkeeping of
     * an applied update (the memtable's columns, minimum timestamp, size...). Then, if the index transaction failed
     * after the update was applied (when it delivers held events, or completes), the failure is thrown: the update
     * stays applied, and is accounted for, but the write fails. If the update was not applied, the failure is thrown
     * and {@code accountForApplied} is not called.
     *
     * @return the updater, with the difference in size seen after merging the updates and the minimum time delta
     * between updates.
     */
    public BTreePartitionUpdater addAll(final BTreePartitionUpdate update,
                                        Cloner cloner,
                                        OpOrder.Group writeOp,
                                        UpdateTransaction indexer,
                                        Consumer<? super BTreePartitionUpdater> accountForApplied)
    {
        // Updater.addAll returns only once the update is applied.
        Updater updater = new Updater(allocator, cloner, writeOp, indexer).addAll(update);
        try
        {
            accountForApplied.accept(updater);
        }
        finally
        {
            updater.throwIndexFailure();
        }
        return updater;
    }

    /**
     * Adds a given update to this in-memtable partition, without a callback for the bookkeeping of an applied update.
     * When the index transaction fails after the update was applied, this throws that failure, so the caller skips its
     * bookkeeping for an update that stays in the memtable (an error is logged): a flush can then drop the update's
     * cells, and compaction can purge a tombstone that shadows the update, which then comes back.
     *
     * @return the updater, with the difference in size seen after merging the updates and the minimum time delta
     * between updates.
     * @deprecated use {@link #addAll(BTreePartitionUpdate, Cloner, OpOrder.Group, UpdateTransaction, Consumer)}, and do
     * the bookkeeping of the applied update in its callback.
     */
    @Deprecated(since = "5.0")
    public BTreePartitionUpdater addAll(final BTreePartitionUpdate update,
                                        Cloner cloner,
                                        OpOrder.Group writeOp,
                                        UpdateTransaction indexer)
    {
        boolean[] applied = new boolean[1];
        try
        {
            return addAll(update, cloner, writeOp, indexer, updater -> applied[0] = true);
        }
        catch (Throwable t)
        {
            if (applied[0])
                NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, TimeUnit.MINUTES,
                                 "An index failed on a write to {} after the update was applied: the update stays in " +
                                 "the memtable without the memtable's bookkeeping, as the memtable adds updates " +
                                 "without a callback for it", metadata());
            throw t;
        }
    }

    @VisibleForTesting
    public void unsafeSetHolder(BTreePartitionData holder)
    {
        ref = holder;
    }

    @VisibleForTesting
    public BTreePartitionData unsafeGetHolder()
    {
        return ref;
    }

    class Updater extends BTreePartitionUpdater
    {
        BTreePartitionData current;
        /// Whether the update was applied.
        private boolean applied;
        /// The failure of the index transaction after the update was applied, or null.
        private Throwable indexFailure;

        public Updater(MemtableAllocator allocator, Cloner cloner, OpOrder.Group writeOp, UpdateTransaction indexer)
        {
            super(allocator, cloner, writeOp, indexer);
        }

        Updater addAll(final BTreePartitionUpdate update)
        {
            try
            {
                boolean atomically = indexer.requiresAtomicApply();
                boolean shouldLock = !atomically && shouldLock(writeOp);
                indexer.start();

                if (atomically)
                {
                    addAllAtomically(update);
                    return this;
                }

                while (true)
                {
                    if (shouldLock)
                    {
                        synchronized (this)
                        {
                            if (tryUpdateData(update))
                                return this;
                        }
                    }
                    else
                    {
                        if (tryUpdateData(update))
                            return this;

                        shouldLock = shouldLock(heapSize, writeOp);
                    }
                }
            }
            finally
            {
                try
                {
                    indexer.commit();
                }
                catch (Throwable t)
                {
                    // An applied update stays applied: let the caller account for it before the write fails.
                    if (!applied)
                        throw t;
                    indexFailure = t;
                }
                finally
                {
                    reportAllocatedMemory();
                }
            }
        }

        /**
         * Throws the failure of the index transaction of an update that was applied, if any. The update is in the
         * memtable, so the caller has done the bookkeeping of an applied update first.
         */
        private void throwIndexFailure()
        {
            if (indexFailure instanceof Error)
                throw (Error) indexFailure;
            if (indexFailure != null)
                throw Throwables.unchecked(indexFailure);
        }

        /**
         * Merges and applies the update under the partition's lock, which every update whose index transaction
         * {@linkplain UpdateTransaction#requiresAtomicApply requires it} takes, and tells the transaction which
         * merge it applied before releasing the lock. Its indexes then see the updates of the partition once each,
         * in the order they were applied (see {@link org.apache.cassandra.db.memtable.Memtable#appliesUpdatesAtomicallyOnRequest}).
         * A writer that doesn't take the lock (an update with no such index transaction) can still change the
         * partition first; the merge is then retried, still under the lock.
         * <p>
         * Allocations never wait for room in the memtable pool (that is done before the write starts, see
         * {@link org.apache.cassandra.db.memtable.Memtable#checkSpaceAndPut}), so holding the lock can't stop a flush
         * from completing, whatever the write's op order group.
         */
        private void addAllAtomically(BTreePartitionUpdate update)
        {
            synchronized (AtomicBTreePartition.this)
            {
                while (!tryUpdateData(update))
                    indexer.onAttemptLost();
                indexer.onApplied();
            }
        }

        private boolean tryUpdateData(BTreePartitionUpdate update)
        {
            current = ref;
            this.dataSize = 0;
            this.heapSize = 0;
            BTreePartitionData result = makeMergedPartition(current, update);
            applied = refUpdater.compareAndSet(AtomicBTreePartition.this, current, result);
            return applied;
        }
    }

    @Override
    public DeletionInfo deletionInfo()
    {
        return allocator.ensureOnHeap().applyToDeletionInfo(super.deletionInfo());
    }

    @Override
    public Row staticRow()
    {
        return allocator.ensureOnHeap().applyToStatic(super.staticRow());
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return allocator.ensureOnHeap().applyToPartitionKey(super.partitionKey());
    }

    @Override
    public Row getRow(Clustering<?> clustering)
    {
        return allocator.ensureOnHeap().applyToRow(super.getRow(clustering));
    }

    @Override
    public Row lastRow()
    {
        return allocator.ensureOnHeap().applyToRow(super.lastRow());
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(super.unfilteredIterator(selection, slices, reversed));
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(super.unfilteredIterator(selection, clusteringsInQueryOrder, reversed));
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator()
    {
        return allocator.ensureOnHeap().applyToPartition(super.unfilteredIterator());
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(BTreePartitionData current, ColumnFilter selection, Slices slices, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(super.unfilteredIterator(current, selection, slices, reversed));
    }

    @Override
    public Iterator<Row> rowIterator()
    {
        return allocator.ensureOnHeap().applyToPartition(super.rowIterator());
    }

    private boolean shouldLock(OpOrder.Group writeOp)
    {
        if (!useLock())
            return false;

        return lockIfOldest(writeOp);
    }

    private boolean shouldLock(long addWaste, OpOrder.Group writeOp)
    {
        if (!updateWastedAllocationTracker(addWaste))
            return false;

        return lockIfOldest(writeOp);
    }

    private boolean lockIfOldest(OpOrder.Group writeOp)
    {
        if (!writeOp.isOldestLiveGroup())
        {
            Thread.yield();
            return writeOp.isOldestLiveGroup();
        }

        return true;
    }

    public boolean useLock()
    {
        return wasteTracker == TRACKER_PESSIMISTIC_LOCKING;
    }

    /**
     * Update the wasted allocation tracker state based on newly wasted allocation information
     *
     * @param wastedBytes the number of bytes wasted by this thread
     * @return true if the caller should now proceed with pessimistic locking because the waste limit has been reached
     */
    private boolean updateWastedAllocationTracker(long wastedBytes)
    {
        // Early check for huge allocation that exceeds the limit
        if (wastedBytes < EXCESS_WASTE_BYTES)
        {
            // We round up to ensure work < granularity are still accounted for
            int wastedAllocation = ((int) (wastedBytes + ALLOCATION_GRANULARITY_BYTES - 1)) / ALLOCATION_GRANULARITY_BYTES;

            int oldTrackerValue;
            while (TRACKER_PESSIMISTIC_LOCKING != (oldTrackerValue = wasteTracker))
            {
                // Note this time value has an arbitrary offset, but is a constant rate 32 bit counter (that may wrap)
                int time = (int) (nanoTime() >>> CLOCK_SHIFT);
                int delta = oldTrackerValue - time;
                if (oldTrackerValue == TRACKER_NEVER_WASTED || delta >= 0 || delta < -EXCESS_WASTE_OFFSET)
                    delta = -EXCESS_WASTE_OFFSET;
                delta += wastedAllocation;
                if (delta >= 0)
                    break;
                if (wasteTrackerUpdater.compareAndSet(this, oldTrackerValue, avoidReservedValues(time + delta)))
                    return false;
            }
        }
        // We have definitely reached our waste limit so set the state if it isn't already
        wasteTrackerUpdater.set(this, TRACKER_PESSIMISTIC_LOCKING);
        // And tell the caller to proceed with pessimistic locking
        return true;
    }

    private static int avoidReservedValues(int wasteTracker)
    {
        if (wasteTracker == TRACKER_NEVER_WASTED || wasteTracker == TRACKER_PESSIMISTIC_LOCKING)
            return wasteTracker + 1;
        return wasteTracker;
    }
}
