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

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.tuple.Triple;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.InMemoryPartitionIterator;
import org.apache.cassandra.index.sai.utils.InMemoryUnfilteredPartitionIterator;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.PartitionInfo;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Processor that scans all rows from given partitions and selects rows with top-k scores based on vector indexes.
 * <p>
 * This processor performs the following steps:
 * - collect rows with score into {@link PriorityQueue} that sorts rows based on score. If there are multiple vector indexes,
 *   the final score is the sum of all vector index scores.
 * - remove rows with the lowest scores from PQ if PQ size exceeds limit
 * - return rows from PQ in primary key order to client
 */
public class VectorTopKProcessor
{
    private final ReadCommand command;
    private final StorageAttachedIndex index;
    private final IndexTermType indexTermType;
    private final float[] queryVector;

    private final int limit;
    private final boolean enforceStrictLiveness;

    public VectorTopKProcessor(ReadCommand command)
    {
        this.command = command;

        Pair<StorageAttachedIndex, float[]> annIndexAndExpression = findTopKIndex();
        Preconditions.checkNotNull(annIndexAndExpression);

        this.index = annIndexAndExpression.left;
        this.indexTermType = annIndexAndExpression.left().termType();
        this.queryVector = annIndexAndExpression.right;
        this.limit = command.limits().count();
        this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();
    }

    /**
     * Filter given partitions and keep the rows with the highest scores. In case of {@link UnfilteredPartitionIterator},
     * all tombstones will be kept.
     */
    public <U extends Unfiltered, R extends BaseRowIterator<U>, P extends BasePartitionIterator<R>> BasePartitionIterator<?> consumeSortByScoreAndTakeTopK(P partitions)
    {
        // priority queue ordered by score in ascending order
        PriorityQueue<Triple<PartitionInfo, Row, Float>> topK = new PriorityQueue<>(limit + 1, Comparator.comparing(Triple::getRight));
        // to store top-k results in primary key order
        TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition = new TreeMap<>(Comparator.comparing(p -> p.key));

        while (partitions.hasNext())
        {
            try (R partition = partitions.next())
            {
                DecoratedKey key = partition.partitionKey();
                Row staticRow = partition.staticRow();
                PartitionInfo partitionInfo = PartitionInfo.create(partition);
                // compute key and static row score once per partition
                float keyAndStaticScore = getScoreForRow(key, staticRow);

                // a partition without rows takes part through its static row if the query returns its static content
                if (!partition.hasNext() && command.selectsFullPartition())
                {
                    topK.add(Triple.of(partitionInfo, staticRow, keyAndStaticScore));
                    if (topK.size() > limit)
                        topK.poll();
                }

                while (partition.hasNext())
                {
                    Unfiltered unfiltered = partition.next();
                    // Always include tombstones for coordinator. It relies on ReadCommand#withMetricsRecording to throw
                    // TombstoneOverwhelmingException to prevent OOM.
                    if (!unfiltered.isRow())
                    {
                        unfilteredByPartition.computeIfAbsent(partitionInfo, k -> new TreeSet<>(command.metadata().comparator))
                                             .add(unfiltered);
                        continue;
                    }

                    Row row = (Row) unfiltered;
                    float rowScore = getScoreForRow(null, row);
                    topK.add(Triple.of(partitionInfo, row, keyAndStaticScore + rowScore));

                    // when exceeding limit, remove row with low score
                    while (topK.size() > limit)
                        topK.poll();
                }
            }
        }
        partitions.close();

        // reorder rows in partition/clustering order
        for (Triple<PartitionInfo, Row, Float> triple : topK)
        {
            TreeSet<Unfiltered> unfiltereds = unfilteredByPartition.computeIfAbsent(triple.getLeft(), k -> new TreeSet<>(command.metadata().comparator));
            if (!triple.getMiddle().isStatic())
                unfiltereds.add(triple.getMiddle());
        }

        if (partitions instanceof PartitionIterator)
            return new InMemoryPartitionIterator(command, unfilteredByPartition);
        return new InMemoryUnfilteredPartitionIterator(command, unfilteredByPartition);
    }

    /**
     * Sum the scores from different vector indexes for the row
     */
    private float getScoreForRow(DecoratedKey key, Row row)
    {
        ColumnMetadata column = indexTermType.columnMetadata();

        if (column.isPrimaryKeyColumn() && key == null)
            return 0;

        if (column.isStatic() && !row.isStatic())
            return 0;

        if ((column.isClusteringColumn() || column.isRegular()) && row.isStatic())
            return 0;

        ByteBuffer value = indexTermType.valueOf(key, row, FBUtilities.nowInSeconds());
        if (value != null)
        {
            float[] vector = indexTermType.decomposeVector(value);
            return index.indexWriterConfig().getSimilarityFunction().compare(vector, queryVector);
        }
        return 0;
    }

    /**
     * Filter given partitions and keep the rows with the highest scores. Rows without live data are left out.
     */
    public <U extends Unfiltered, R extends BaseRowIterator<U>, P extends BasePartitionIterator<R>> BasePartitionIterator<?> takeTopKThenSortByPrimaryKey(P partitions)
    {
        try (partitions)
        {
            TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition = new TreeMap<>(Comparator.comparing(pi -> pi.key));

            int rowsMatched = 0;
            while (rowsMatched < limit && partitions.hasNext())
            {
                try (BaseRowIterator<?> partitionRowIterator = partitions.next())
                {
                    rowsMatched += processSingleRowPartition(unfilteredByPartition, partitionRowIterator, limit - rowsMatched);
                }
            }

            return new InMemoryUnfilteredPartitionIterator(command, unfilteredByPartition);
        }
    }

    /**
     * Processes a single partition, without scoring it. Only live rows are kept, and a partition without them is kept
     * and counts as one row if the query returns its static content.
     */
    private int processSingleRowPartition(TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition,
                                          BaseRowIterator<?> partitionRowIterator,
                                          int reamining)
    {
        PartitionInfo partitionInfo = PartitionInfo.create(partitionRowIterator);
        TreeSet<Unfiltered> rows = new TreeSet<>(command.metadata().comparator);
        while (partitionRowIterator.hasNext() && rows.size() < reamining)
        {
            Unfiltered unfiltered = partitionRowIterator.next();
            if (unfiltered.isRow() && ((Row) unfiltered).hasLiveData(command.nowInSec(), enforceStrictLiveness))
                rows.add(unfiltered);
        }

        if (rows.isEmpty())
        {
            if (!command.selectsFullPartition() || !partitionInfo.staticRow.hasLiveData(command.nowInSec(), enforceStrictLiveness))
                return 0;

            unfilteredByPartition.computeIfAbsent(partitionInfo, k -> new TreeSet<>(command.metadata().comparator));
            return 1;
        }

        unfilteredByPartition.computeIfAbsent(partitionInfo, k -> new TreeSet<>(command.metadata().comparator)).addAll(rows);
        return rows.size();
    }

    private Pair<StorageAttachedIndex, float[]> findTopKIndex()
    {
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(command.metadata());

        for (RowFilter.Expression expression : command.rowFilter().getExpressions())
        {
            StorageAttachedIndex sai = findVectorIndexFor(cfs.indexManager, expression);
            if (sai != null)
            {
                float[] qv = sai.termType().decomposeVector(expression.getIndexValue().duplicate());
                return Pair.create(sai, qv);
            }
        }

        return null;
    }

    @Nullable
    private StorageAttachedIndex findVectorIndexFor(SecondaryIndexManager sim, RowFilter.Expression e)
    {
        if (e.operator() != Operator.ANN)
            return null;

        IndexHints hints = command.rowFilter().indexHints;
        return sim.getBestIndexFor(e, StorageAttachedIndex.class, hints).orElse(null);
    }
}
