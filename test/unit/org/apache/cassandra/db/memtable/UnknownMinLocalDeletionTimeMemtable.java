/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.memtable;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * A skip-list memtable that doesn't keep its min local deletion time, as memtables outside the tree may not, whose
 * partitions report the stats of their current content, and which counts the partition iterators it opens.
 */
public class UnknownMinLocalDeletionTimeMemtable extends SkipListMemtable
{
    public static final Factory FACTORY = UnknownMinLocalDeletionTimeMemtable::new;
    public static final AtomicInteger partitionIterators = new AtomicInteger();

    UnknownMinLocalDeletionTimeMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        super(commitLogLowerBound, metadataRef, owner);
    }

    @Override
    protected Factory factory()
    {
        return FACTORY;
    }

    @Override
    public int getMinLocalDeletionTime()
    {
        return UNKNOWN_MIN_LOCAL_DELETION_TIME;
    }

    @Override
    public Partition getPartition(DecoratedKey key)
    {
        Partition partition = super.getPartition(key);
        return partition == null ? null : new CurrentStatsPartition((AtomicBTreePartition) partition);
    }

    @Override
    public MemtableUnfilteredPartitionIterator makePartitionIterator(ColumnFilter columnFilter, DataRange dataRange)
    {
        partitionIterators.incrementAndGet();
        return super.makePartitionIterator(columnFilter, dataRange);
    }

    private static class CurrentStatsPartition implements Partition
    {
        private final AtomicBTreePartition partition;

        CurrentStatsPartition(AtomicBTreePartition partition)
        {
            this.partition = partition;
        }

        public TableMetadata metadata()
        {
            return partition.metadata();
        }

        public DecoratedKey partitionKey()
        {
            return partition.partitionKey();
        }

        public DeletionTime partitionLevelDeletion()
        {
            return partition.partitionLevelDeletion();
        }

        public RegularAndStaticColumns columns()
        {
            return partition.columns();
        }

        public EncodingStats stats()
        {
            return EncodingStats.Collector.collect(partition.staticRow(), partition.rowIterator(), partition.deletionInfo());
        }

        public boolean isEmpty()
        {
            return partition.isEmpty();
        }

        public boolean hasRows()
        {
            return partition.hasRows();
        }

        public int rowCount()
        {
            return partition.rowCount();
        }

        public Iterator<Row> rowIterator()
        {
            return partition.rowIterator();
        }

        public Row staticRow()
        {
            return partition.staticRow();
        }

        public Row lastRow()
        {
            return partition.lastRow();
        }

        public Row getRow(Clustering<?> clustering)
        {
            return partition.getRow(clustering);
        }

        public UnfilteredRowIterator unfilteredIterator()
        {
            return partition.unfilteredIterator();
        }

        public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed)
        {
            return partition.unfilteredIterator(columns, slices, reversed);
        }

        public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
        {
            return partition.unfilteredIterator(columns, clusteringsInQueryOrder, reversed);
        }
    }
}
