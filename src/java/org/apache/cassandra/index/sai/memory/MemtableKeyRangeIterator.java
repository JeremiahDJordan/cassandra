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

import java.io.IOException;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Iterates over the primary keys of the rows of a memtable or, for an index on a static column, over the keys of the
 * static rows of its partitions.
 */
public class MemtableKeyRangeIterator extends KeyRangeIterator
{
    private final Memtable memtable;
    private final PrimaryKey.Factory pkFactory;
    private final AbstractBounds<PartitionPosition> keyRange;
    private final ColumnFilter columns;
    private final boolean staticKeys;
    private UnfilteredPartitionIterator partitionIterator;
    // the partition we are positioned on; with staticKeys, one whose key we haven't returned yet
    private UnfilteredRowIterator rowIterator;
    private PrimaryKey lastKey;

    public MemtableKeyRangeIterator(Memtable memtable,
                                    PrimaryKey.Factory pkFactory,
                                    AbstractBounds<PartitionPosition> keyRange)
    {
        this(memtable, pkFactory, keyRange, false);
    }

    /**
     * @param staticKeys whether to return the static row key of each partition instead of the keys of its rows, for an
     *                   index on a static column (a partition with only a static row has no row key)
     */
    public MemtableKeyRangeIterator(Memtable memtable,
                                    PrimaryKey.Factory pkFactory,
                                    AbstractBounds<PartitionPosition> keyRange,
                                    boolean staticKeys)
    {
        super(minKey(memtable, pkFactory),
              maxKey(memtable, pkFactory),
              memtable.getOperations());

        TableMetadata metadata = memtable.metadata();
        this.memtable = memtable;
        this.pkFactory = pkFactory;
        this.keyRange = keyRange;
        this.staticKeys = staticKeys;
        this.columns = ColumnFilter.selectionBuilder()
                                           .addAll(metadata.partitionKeyColumns())
                                           .addAll(metadata.clusteringColumns())
                                           .addAll(metadata.regularColumns())
                                           .build();

        DataRange dataRange = new DataRange(keyRange, new ClusteringIndexSliceFilter(Slices.ALL, false));
        this.partitionIterator = memtable.makePartitionIterator(columns, dataRange);
        this.rowIterator = null;
    }

    private static PrimaryKey minKey(Memtable memtable, PrimaryKey.Factory factory)
    {
        DecoratedKey pk = memtable.minPartitionKey();
        return pk != null ? factory.createPartitionKeyOnly(pk) : null;
    }

    private static PrimaryKey maxKey(Memtable memtable, PrimaryKey.Factory factory)
    {
        DecoratedKey pk = memtable.maxPartitionKey();
        return pk != null ? factory.createPartitionKeyOnly(pk) : null;
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        // Never go back: a key without clustering (e.g. a static key) compares equal to every key of its partition.
        if (lastKey != null && lastKey.compareTo(nextKey) >= 0)
            return;

        PartitionPosition start = nextKey.isTokenOnly()
                                  ? nextKey.token().minKeyBound()
                                  : nextKey.partitionKey();

        // Drop the row iterator of the partition we are currently positioned on (possibly by a previous skipTo),
        // otherwise its rows would be returned if there are no more partitions after the skip target.
        FileUtils.closeQuietly(rowIterator);
        rowIterator = null;
        FileUtils.closeQuietly(partitionIterator);

        if (!keyRange.right.isMinimum() && start.compareTo(keyRange.right) > 0)
        {
            partitionIterator = EmptyIterators.unfilteredPartition(memtable.metadata());
            return;
        }

        AbstractBounds<PartitionPosition> partitionBounds = AbstractBounds.bounds(start, true, keyRange.right, true);
        DataRange dataRange = new DataRange(partitionBounds, new ClusteringIndexSliceFilter(Slices.ALL, false));
        partitionIterator = memtable.makePartitionIterator(columns, dataRange);
        if (partitionIterator.hasNext())
        {
            this.rowIterator = partitionIterator.next();
            if (!staticKeys && nextKey.hasClustering() && rowIterator.partitionKey().equals(nextKey.partitionKey()))
            {
                Slice slice = Slice.make(nextKey.clustering(), Clustering.EMPTY);
                Slices slices = Slices.with(memtable.metadata().comparator, slice);
                FileUtils.closeQuietly(rowIterator);
                rowIterator = memtable.getPartition(nextKey.partitionKey()).unfilteredIterator(columns, slices, false);
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(partitionIterator, rowIterator);
    }

    @Override
    protected PrimaryKey computeNext()
    {
        if (staticKeys)
            return computeNextStaticKey();

        while (hasNextRow(rowIterator) || partitionIterator.hasNext())
        {
            if (!hasNextRow(rowIterator))
            {
                FileUtils.closeQuietly(rowIterator);
                rowIterator = partitionIterator.next();
                continue;
            }

            Unfiltered unfiltered = rowIterator.next();
            if (unfiltered.isRow())
            {
                Row row = (Row) unfiltered;
                lastKey = pkFactory.create(rowIterator.partitionKey(), row.clustering());
                return lastKey;
            }
        }
        return endOfData();
    }

    private PrimaryKey computeNextStaticKey()
    {
        // the partition a skip positioned us on, or the next one
        if (rowIterator == null)
        {
            if (!partitionIterator.hasNext())
                return endOfData();
            rowIterator = partitionIterator.next();
        }
        lastKey = pkFactory.create(rowIterator.partitionKey(), Clustering.STATIC_CLUSTERING);
        FileUtils.closeQuietly(rowIterator);
        rowIterator = null;
        return lastKey;
    }

    private static boolean hasNextRow(UnfilteredRowIterator rowIterator)
    {
        return rowIterator != null && rowIterator.hasNext();
    }
}
