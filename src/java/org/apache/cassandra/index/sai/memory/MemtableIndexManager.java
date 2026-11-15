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

package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

public class MemtableIndexManager
{
    private final StorageAttachedIndex index;
    private final ConcurrentMap<Memtable, MemtableIndex> liveMemtableIndexMap;

    public MemtableIndexManager(StorageAttachedIndex index)
    {
        this.index = index;
        this.liveMemtableIndexMap = new ConcurrentHashMap<>();
    }

    public void maybeInitializeMemtableIndex(Memtable memtable)
    {
        if (index.termType().isVector())
            initializeMemtableIndex(memtable);
    }

    private MemtableIndex initializeMemtableIndex(Memtable mt)
    {
        MemtableIndex current = liveMemtableIndexMap.get(mt);

        // We expect the relevant IndexMemtable to be present most of the time, so only make the
        // call to computeIfAbsent() if it's not. (see https://bugs.openjdk.java.net/browse/JDK-8161372)
        return current != null ? current
                               : liveMemtableIndexMap.computeIfAbsent(mt, memtable -> new MemtableIndex(index, memtable));
    }

    public long index(DecoratedKey key, Row row, Memtable mt)
    {
        MemtableIndex target = initializeMemtableIndex(mt);

        long start = Clock.Global.nanoTime();

        long bytes = 0;

        if (indexesVectorCells())
        {
            Cell<?> cell = vectorCell(row);
            if (cell != null)
                bytes += target.update(key, row.clustering(), cell);
        }
        else if (index.termType().isNonFrozenCollection())
        {
            Iterator<ByteBuffer> bufferIterator = index.termType().valuesOf(row, FBUtilities.nowInSeconds());
            if (bufferIterator != null)
            {
                while (bufferIterator.hasNext())
                {
                    ByteBuffer value = bufferIterator.next();
                    bytes += target.index(key, row.clustering(), value);
                }
            }
        }
        else
        {
            ByteBuffer value = index.termType().valueOf(key, row, FBUtilities.nowInSeconds());
            bytes += target.index(key, row.clustering(), value);
        }
        index.indexMetrics().memtableIndexWriteLatency.update(Clock.Global.nanoTime() - start, TimeUnit.NANOSECONDS);
        return bytes;
    }

    public long update(DecoratedKey key, Row oldRow, Row newRow, Memtable memtable)
    {
        if (!index.termType().isVector())
        {
            return index(key, newRow, memtable);
        }

        // A vector that is part of the primary key never changes for a row.
        if (!indexesVectorCells())
            return 0;

        // Updates should only be able to happen on memtables that were already created and that are still live.
        MemtableIndex target = liveMemtableIndexMap.get(memtable);
        assert target != null : "Memtable for " + memtable.metadata().getTableName() + " not found";

        Cell<?> cell = vectorCell(newRow);
        return cell == null ? 0 : target.update(key, newRow.clustering(), cell);
    }

    /**
     * @return the cell of the indexed vector column of the row or, if the row has none but a row deletion, a tombstone
     * at the time of that deletion, which may have shadowed the vector; {@code null} if the row has neither.
     * <p>
     * The rows passed to {@link #update} only hold the cells that the write changed. A write of a row that is merged
     * again after losing the race to publish its partition may have indexed a vector that the other write deleted, and
     * then change nothing, so the deletion must be applied whenever the index sees it, even if the row has no vector
     * at that time.
     */
    private Cell<?> vectorCell(Row row)
    {
        ColumnMetadata column = index.termType().columnMetadata();
        Cell<?> cell = row.getCell(column);
        if (cell != null || row.deletion().isLive())
            return cell;

        DeletionTime deletion = row.deletion().time();
        return BufferCell.tombstone(column, deletion.markedForDeleteAt(), deletion.localDeletionTime());
    }

    /**
     * @return whether this is a vector index on a regular or static column, whose memtable index is updated with the
     * cells of the column, see {@link VectorMemoryIndex#update}
     */
    private boolean indexesVectorCells()
    {
        return index.termType().isVector() && !index.termType().columnMetadata().isPrimaryKeyColumn();
    }

    public void renewMemtable(Memtable renewed)
    {
        for (Memtable memtable : liveMemtableIndexMap.keySet())
        {
            // remove every index but the one that corresponds to the post-truncate Memtable
            if (renewed != memtable)
            {
                liveMemtableIndexMap.remove(memtable);
            }
        }
    }

    public void discardMemtable(Memtable discarded)
    {
        liveMemtableIndexMap.remove(discarded);
    }

    @Nullable
    public MemtableIndex getPendingMemtableIndex(LifecycleNewTracker tracker)
    {
        return liveMemtableIndexMap.keySet().stream()
                                   .filter(m -> tracker.equals(m.getFlushTransaction()))
                                   .findFirst()
                                   .map(liveMemtableIndexMap::get)
                                   .orElse(null);
    }

    public long liveMemtableWriteCount()
    {
        return liveMemtableIndexMap.values().stream().mapToLong(MemtableIndex::writeCount).sum();
    }

    public Collection<MemtableIndex> getLiveMemtableIndexesSnapshot()
    {
        Collection<MemtableIndex> memtableIndexes = liveMemtableIndexMap.values();
        if (memtableIndexes.isEmpty())
            return Collections.emptyList();

        // Copy the values. Otherwise, we'll only have a view of the map's values which is subject to change.
        return new ArrayList<>(memtableIndexes);
    }

    public long estimatedMemIndexMemoryUsed()
    {
        return liveMemtableIndexMap.values().stream().mapToLong(MemtableIndex::estimatedMemoryUsed).sum();
    }

    @VisibleForTesting
    public int size()
    {
        return liveMemtableIndexMap.size();
    }

    public void invalidate()
    {
        liveMemtableIndexMap.clear();
    }
}
