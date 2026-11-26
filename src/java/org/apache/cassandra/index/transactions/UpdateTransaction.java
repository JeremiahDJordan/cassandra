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

package org.apache.cassandra.index.transactions;

import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;

/// Handling of index updates on the write path.
///
/// Instances of an `UpdateTransaction` are scoped to a single partition update
/// A new instance is used for every write, obtained from the
/// `newUpdateTransaction(PartitionUpdate)` method. Likewise, a single
/// `CleanupTransaction` instance is used for each partition processed during a
/// compaction or cleanup.
///
/// We make certain guarantees about the lifecycle of each UpdateTransaction
/// instance. Namely that `start()` will be called before any other method, and
/// `commit()` will be called at the end of the update.
/// Each instance is initialized with 1..many `Index.Indexer` instances, one per
/// registered `Index`. As with the transaction itself, these are scoped to a
/// specific partition update, so implementations can be assured that all indexing
/// events they receive relate to the same logical operation.
///
/// `onPartitionDelete()`, `onRangeTombstone()`, `onInserted()` and `onUpdated()`
/// calls may arrive in any order, but this should have no impact for the
/// Indexers being notified as any events delivered to a single instance
/// necessarily relate to a single partition.
///
/// The typical sequence of events during a Memtable update would be:
/// - `start()`                       -- no-op, used to notify Indexers of the start of the transaction
/// - `onPartitionDeletion(dt)`      -- if the PartitionUpdate implies one
/// - `onRangeTombstone(rt)`*         -- for each in the PartitionUpdate, if any
/// then:
/// - `onInserted(row)`*              -- called for each Row not already present in the Memtable
/// - `onUpdated(existing, updated)`* -- called for any Row in the update for where a version was already present
///                                  in the Memtable. It's important to note here that existing is the previous
///                                  row from the Memtable and updated is the final version replacing it. It is
///                                  *not* the incoming row, but the result of merging the incoming and existing
///                                  rows.
/// - `commit()`                      -- finally, finish is called when the new Partition is swapped into the Memtable
///
/// A Memtable that applies updates atomically (see `Memtable#appliesUpdatesAtomically()`) calls `abort()` instead of
/// `commit()` when it fails to apply the update. It then holds none of the update's rows and deletions, so
/// implementations should not deliver any event they still hold. Other Memtables always call `commit()`.
///
/// A Memtable that merges updates in a CAS loop calls the methods above again for each merge attempt, including the
/// attempts that lose the race against another writer. If the transaction [requires it][#requiresAtomicApply], it
/// also tells the transaction which attempts it applied ([#onApplied]) and which it did not ([#onAttemptLost]).
///
/// If the memtable prefers to supply individual cells rather than rows, the `onInserted`/`onUpdated` calls above
/// are replaced with a sequence of `startRow` plus zero or more calls to `onCellUpdate`.
public interface UpdateTransaction extends IndexTransaction
{
    void onPartitionDeletion(DeletionTime deletionTime);
    void onRangeTombstone(RangeTombstone rangeTombstone);

    void onInserted(Row row);

    /// @param existing the existing row from the Memtable
    /// @param updated the updated version, which includes the update merged with the existing version
    void onUpdated(Row existing, Row updated);

    /// Start a new row, whose content will be given through [#onCellUpdate].
    /// The existing liveness may be null, in which case no existing row was present in the data.
    void startRow(Clustering clustering,
                  @Nullable LivenessInfo existingLiveness,
                  @Nullable Row.Deletion existingDeletion,
                  LivenessInfo updatedLiveness,
                  Row.Deletion updatedDeletion);

    void onCellUpdate(@Nullable Cell<?> original, @Nullable Cell<?> merged);
    void onComplexColumnDeletion(ColumnMetadata column, DeletionTime deletionTime);

    /// Ends the transaction because the memtable failed to apply the update, and holds none of its rows and
    /// deletions. Called instead of [#commit], and only by memtables that apply updates atomically.
    ///
    /// Implementations that hold events until [#commit] should drop them, and end the transaction for the indexers.
    /// The default calls [#commit]: implementations that deliver events as they arrive cannot take them back.
    default void abort()
    {
        commit();
    }

    /// Whether the memtable should apply this update atomically for some of its indexers: merge it and make it
    /// visible under a lock of the partition that every update of the partition that asks for this takes, and call
    /// [#onApplied] before releasing that lock. Only asked by memtables that merge updates in a CAS loop and support
    /// this (see `Memtable#appliesUpdatesAtomicallyOnRequest()`), and only true for transactions made for such a
    /// memtable. A memtable that honours it calls [#onAttemptLost] for every merge attempt it does not apply, and
    /// [#onApplied] for the one it applies, then [#commit] as usual. The memtable asks before the first event, and
    /// asking commits it to that protocol: the transaction then holds the events of those indexers until [#onApplied],
    /// and drops them at [#commit] if it was never called.
    default boolean requiresAtomicApply()
    {
        return false;
    }

    /// The memtable merged the update but did not apply that merge, because another writer changed the partition
    /// first, and will merge the update again. Only called if [#requiresAtomicApply] is true.
    default void onAttemptLost()
    {
    }

    /// The memtable applied the last merge of the update, and still holds the lock that serializes the updates of the
    /// partition that [require it][#requiresAtomicApply]. Only called if [#requiresAtomicApply] is true, at most once,
    /// and before [#commit].
    default void onApplied()
    {
    }

    UpdateTransaction NO_OP = new UpdateTransaction()
    {
        public void start(){}
        public void onPartitionDeletion(DeletionTime deletionTime){}
        public void onRangeTombstone(RangeTombstone rangeTombstone){}
        public void onInserted(Row row){}
        public void onUpdated(Row existing, Row updated){}
        public void startRow(Clustering clustering, LivenessInfo existingLiveness, Row.Deletion existingDeletion, LivenessInfo updatedLiveness, Row.Deletion updatedDeletion){}
        public void onCellUpdate(Cell<?> original, Cell<?> merged){}
        public void onComplexColumnDeletion(ColumnMetadata column, DeletionTime deletionTime){}
        public void commit(){}
    };
}
