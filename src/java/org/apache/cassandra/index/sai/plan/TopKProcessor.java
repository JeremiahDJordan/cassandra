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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Ordering;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher.ScoreOrderedResultRetriever;
import org.apache.cassandra.index.sai.utils.InMemoryPartitionIterator;
import org.apache.cassandra.index.sai.utils.InMemoryUnfilteredPartitionIterator;
import org.apache.cassandra.index.sai.utils.PartitionInfo;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TopKSelector;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Processor applied to SAI based ORDER BY queries.
 * </p>
 * On a replica:
 * <ul>
 *     <li>filter(ScoreOrderedResultRetriever) is used to collect up to the top-K rows.</li>
 *     <li>We store any tombstones as well, to avoid losing them during coordinator reconciliation.</li>
 *     <li>The result is returned in PK order so that coordinator can merge from multiple replicas.</li>
 * </ul>
 * On a coordinator:
 * <ul>
 *     <li>reorder(PartitionIterator) is used to consume all rows from the provided partitions,
 *     compute the order based on either a column ordering or a similarity score, and keep top-K.</li>
 *     <li>The result is returned in score/sortkey order.</li>
 * </ul>
 */
public class TopKProcessor
{
    public static final String INDEX_MAY_HAVE_BEEN_DROPPED = "An index may have been dropped. Ordering on non-clustering " +
                                                             "column requires the column to be indexed";
    protected static final Logger logger = LoggerFactory.getLogger(TopKProcessor.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final ReadCommand command;
    private final IndexContext indexContext;
    private final RowFilter.Expression expression;
    private final ColumnMetadata scoreColumn;
    // Lazily compute this value, if needed.
    private VectorFloat<?> queryVector = null;

    private final int limit;
    // The most rows of a partition that the query returns: PER PARTITION LIMIT, or 1 for SELECT DISTINCT
    private final int perPartitionLimit;

    public TopKProcessor(ReadCommand command)
    {
        this.command = command;

        Pair<IndexContext, RowFilter.Expression> indexAndExpression = findTopKIndexContext();
        // this can happen in case an index was dropped after the query was initiated
        if (indexAndExpression == null)
            throw invalidRequest(INDEX_MAY_HAVE_BEEN_DROPPED);

        this.indexContext = indexAndExpression.left;
        this.expression = indexAndExpression.right;
        this.limit = command.limits().count();
        this.perPartitionLimit = command.limits().perPartitionCount();
        this.scoreColumn = ColumnMetadata.syntheticScoreColumn(expression.column(), FloatType.instance);
    }

    /**
     * Sort the specified filtered rows according to the {@code ORDER BY} clause and keep the first {@link #limit} rows.
     * Called on the coordinator side.
     *
     * @param partitions the partitions collected by the coordinator. It will be closed as a side-effect.
     * @return the provided rows, sorted and trimmed to {@link #limit} rows
     */
    public PartitionIterator reorder(PartitionIterator partitions)
    {
        // We consume the partitions iterator and create a new one. Use a try-with-resources block to ensure the
        // original iterator is closed. We do not expect exceptions here, but if they happen, we want to make sure the
        // original iterator is closed to prevent leaking resources, which could compound the effect of an exception.
        try (partitions)
        {
            Comparator<Triple<PartitionInfo, Row, ?>> comparator = comparator()
                   .thenComparing(Triple::getLeft, Comparator.comparing(pi -> pi.key))
                   .thenComparing(Triple::getMiddle, command.metadata().comparator);

            TopKSelector<Triple<PartitionInfo, Row, ?>> topK = new TopKSelector<>(comparator, limit);
            while (partitions.hasNext())
            {
                try (BaseRowIterator<?> partitionRowIterator = partitions.next())
                {
                    if (expression.operator() == Operator.ANN || expression.operator() == Operator.BM25)
                    {
                        PartitionResults pr = processScoredPartition(partitionRowIterator);
                        addPartitionRows(topK, pr.rows, comparator);
                    }
                    else
                    {
                        List<Triple<PartitionInfo, Row, ?>> rows = new ArrayList<>();
                        while (partitionRowIterator.hasNext())
                        {
                            Row row = (Row) partitionRowIterator.next();
                            ByteBuffer value = row.getCell(expression.column()).buffer();
                            rows.add(Triple.of(PartitionInfo.create(partitionRowIterator), row, value));
                        }
                        addPartitionRows(topK, rows, comparator);
                    }
                }
            }

            // Convert the topK results to a PartitionIterator
            List<Pair<PartitionInfo, Row>> sortedRows = new ArrayList<>(topK.size());
            for (Triple<PartitionInfo, Row, ?> triple : topK.getShared())
                sortedRows.add(Pair.create(triple.getLeft(), triple.getMiddle()));
            return InMemoryPartitionIterator.create(command, sortedRows);
        }
    }

    /**
     * Adds the rows of a partition to the top-k selection. Only the first {@link #perPartitionLimit} rows of the
     * partition can be in the results, so we don't let the others take the place of rows of other partitions: the
     * per-partition limit is only applied after this processor (see {@code RangeCommands#partitions}).
     */
    private <T extends Triple<PartitionInfo, Row, ?>> void addPartitionRows(TopKSelector<Triple<PartitionInfo, Row, ?>> topK,
                                                                            List<T> rows,
                                                                            Comparator<Triple<PartitionInfo, Row, ?>> comparator)
    {
        if (rows.size() > perPartitionLimit)
        {
            rows.sort(comparator);
            rows = rows.subList(0, perPartitionLimit);
        }
        topK.addAll(rows);
    }

    /**
     * Sort the specified unfiltered rows according to the {@code ORDER BY} clause, keep the first {@link #limit} rows,
     * and then order them again by primary key.
     * </p>
     * This is meant to be used on the replica-side, before reconciliation. We need to order the rows by primary key
     * after the top-k selection to avoid confusing reconciliation later, on the coordinator. Note that due to sstable
     * overlap and how the full data set of each node is queried for top-k queries we can have multiple versions of the
     * same row in the coordinator even with CL=ONE. Reconciliation should remove those duplicates, but it needs the
     * rows to be ordered by primary key to do so. See CNDB-12308 for details.
     * </p>
     * All tombstones will be kept. Caller must close the supplied iterator.
     *
     * @param partitions the partitions collected in the replica side of a query. It will be closed as a side-effect.
     * @return the provided rows, sorted by the requested {@code ORDER BY} chriteria, trimmed to {@link #limit} rows,
     * and the sorted again by primary key.
     */
    public UnfilteredPartitionIterator filter(ScoreOrderedResultRetriever partitions)
    {
        try (partitions)
        {
            TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition = new TreeMap<>(Comparator.comparing(pi -> pi.key));

            // The rows taken from each partition, to apply the per-partition limit, if any
            Map<DecoratedKey, Integer> rowsByPartition = perPartitionLimit < limit ? new HashMap<>() : null;

            int rowsMatched = 0;
            // Because each “partition” from ScoreOrderedResultRetriever is actually a single row or tombstone, or
            // the rows of a partition that share the score of its static row, we can simply read them until we have
            // enough.
            while (rowsMatched < limit && partitions.hasNext())
            {
                try (BaseRowIterator<?> partitionRowIterator = partitions.next())
                {
                    rowsMatched += processSortedPartition(unfilteredByPartition, partitionRowIterator, limit - rowsMatched, rowsByPartition);
                }
            }

            return new InMemoryUnfilteredPartitionIterator(command, unfilteredByPartition);
        }
    }

    /**
     * Constructs a comparator for triple (PartitionInfo, Row, X) used for top-K ranking.
     * For ANN/BM25 we compare descending by X (float score). For ORDER_BY_ASC or DESC,
     * we compare ascending/descending by the row’s relevant ByteBuffer data.
     */
    private Comparator<Triple<PartitionInfo, Row, ?>> comparator()
    {
        if (expression.operator() == Operator.ANN || expression.operator() == Operator.BM25)
        {
            // For similarity, higher is better, so reversed
            return Comparator.comparing((Triple<PartitionInfo, Row, ?> t) -> (Float) t.getRight()).reversed();
        }

        Comparator<Triple<PartitionInfo, Row, ?>> comparator = Comparator.comparing(t -> (ByteBuffer) t.getRight(), indexContext.getValidator());
        if (expression.operator() == Operator.ORDER_BY_DESC)
            comparator = comparator.reversed();
        return comparator;
    }

    /**
     * Simple holder for partial results of a single partition (score-based path).
     */
    private class PartitionResults
    {
        final PartitionInfo partitionInfo;
        final SortedSet<Unfiltered> tombstones = new TreeSet<>(command.metadata().comparator);
        final List<Triple<PartitionInfo, Row, Float>> rows = new ArrayList<>();

        PartitionResults(PartitionInfo partitionInfo)
        {
            this.partitionInfo = partitionInfo;
        }

        void addTombstone(Unfiltered uf)
        {
            tombstones.add(uf);
        }

        void addRow(Triple<PartitionInfo, Row, Float> triple)
        {
            rows.add(triple);
        }
    }

    /**
     * Processes all rows in a single partition to compute scores (for ANN or BM25)
     */
    private PartitionResults processScoredPartition(BaseRowIterator<?> partitionRowIterator)
    {
        // Compute key and static row score once per partition
        DecoratedKey key = partitionRowIterator.partitionKey();
        Row staticRow = partitionRowIterator.staticRow();
        PartitionInfo partitionInfo = PartitionInfo.create(partitionRowIterator);
        float keyAndStaticScore = getScoreForRow(key, staticRow);
        var pr = new PartitionResults(partitionInfo);

        // A partition with only static content takes part in the top-k selection through an empty static row, which
        // InMemoryPartitionIterator removes, so the partition is returned with its static content and no rows.
        if (!partitionRowIterator.hasNext())
            pr.addRow(Triple.of(partitionInfo, BTreeRow.emptyRow(Clustering.STATIC_CLUSTERING), keyAndStaticScore));

        while (partitionRowIterator.hasNext())
        {
            Unfiltered unfiltered = partitionRowIterator.next();
            // This runs on the coordinator (see #reorder), whose partitions are filtered, so they hold no range
            // tombstone markers: one would be set apart, and not returned. Nothing here counts tombstones.
            if (unfiltered.isRangeTombstoneMarker())
            {
                pr.addTombstone(unfiltered);
                continue;
            }

            Row row = (Row) unfiltered;
            float rowScore = getScoreForRow(null, row);
            pr.addRow(Triple.of(partitionInfo, row, keyAndStaticScore + rowScore));
        }

        return pr;
    }

    /**
     * Processes a partition of {@link StorageAttachedIndexSearcher.ScoreOrderedResultRetriever}, without scoring it. It
     * has a single row, or none if only its static row matches, unless the ordered column is static: then its rows are
     * tied, and the first ones in clustering order are kept, as the coordinator does. Rows beyond the per-partition
     * limit aren't added nor counted ({@code rowsByPartition}, {@code null} without such a limit, holds the rows taken
     * so far).
     *
     * @return the number of rows added, counting a partition with only a static row as one row
     */
    private int processSortedPartition(TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition,
                                       BaseRowIterator<?> partitionRowIterator,
                                       int maxRows,
                                       @Nullable Map<DecoratedKey, Integer> rowsByPartition)
    {
        if (rowsByPartition != null)
        {
            int taken = rowsByPartition.getOrDefault(partitionRowIterator.partitionKey(), 0);
            maxRows = Math.min(maxRows, perPartitionLimit - taken);
            if (maxRows <= 0)
                return 0;
        }

        PartitionInfo partitionInfo = PartitionInfo.create(partitionRowIterator);
        if (!partitionRowIterator.hasNext())
        {
            addUnfiltered(unfilteredByPartition, partitionInfo, null);
            if (rowsByPartition != null)
                rowsByPartition.merge(partitionRowIterator.partitionKey(), 1, Integer::sum);
            return 1;
        }

        int rows = 0;
        while (rows < maxRows && partitionRowIterator.hasNext())
        {
            Unfiltered unfiltered = partitionRowIterator.next();
            // Keep tombstones for the coordinator's reconciliation. ReadCommand#withMetricsRecording counts those
            // returned; those the retriever skipped earlier are counted nowhere.
            addUnfiltered(unfilteredByPartition, partitionInfo, unfiltered);
            if (!unfiltered.isRangeTombstoneMarker())
                rows++;
        }
        if (rowsByPartition != null && rows > 0)
            rowsByPartition.merge(partitionRowIterator.partitionKey(), rows, Integer::sum);
        return rows;
    }

    private void addUnfiltered(SortedMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition,
                               PartitionInfo partitionInfo,
                               Unfiltered unfiltered)
    {
        var map = unfilteredByPartition.computeIfAbsent(partitionInfo, k -> new TreeSet<>(command.metadata().comparator));
        if (unfiltered != null)
            map.add(unfiltered);
    }

    private float getScoreForRow(DecoratedKey key, Row row)
    {
        ColumnMetadata column = indexContext.getDefinition();

        if (column.isPartitionKey() && key == null)
            return 0;

        if (column.isStatic() && !row.isStatic())
            return 0;

        if ((column.isClusteringColumn() || column.isRegular()) && row.isStatic())
            return 0;

        // If we have a synthetic score column, use it
        var scoreData = row.getColumnData(scoreColumn);
        if (scoreData != null)
        {
            var cell = (Cell<?>) scoreData;
            return FloatType.instance.compose(cell.buffer());
        }

        // TODO remove this once we enable Ordering.Ann.USE_SYNTHETIC_SCORE
        ByteBuffer value = indexContext.getValueOf(key, row, FBUtilities.nowInSeconds());
        if (value != null)
        {
            // The replica sends the BM25 score of a row with the row: there is nothing to compute it from here.
            if (expression.operator() == Operator.BM25)
                throw new IllegalStateException(indexContext.logMessage("Got row " + row.clustering().toString(command.metadata()) +
                                                                        " without its BM25 score from the replica"));

            if (queryVector == null)
                queryVector = vts.createFloatVector(TypeUtil.decomposeVector(indexContext, expression.getIndexValue().duplicate()));
            var vector = vts.createFloatVector(TypeUtil.decomposeVector(indexContext, value));
            return indexContext.getIndexWriterConfig().getSimilarityFunction().compare(vector, queryVector);
        }
        return 0;
    }

    private Pair<IndexContext, RowFilter.Expression> findTopKIndexContext()
    {
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(command.metadata());

        for (RowFilter.Expression expression : command.rowFilter().expressions())
        {
            StorageAttachedIndex sai = findOrderingIndexFor(cfs.indexManager, expression);
            if (sai != null)
                return Pair.create(sai.getIndexContext(), expression);
        }

        return null;
    }

    @Nullable
    private StorageAttachedIndex findOrderingIndexFor(SecondaryIndexManager sim, RowFilter.Expression e)
    {
        if (e.operator() != Operator.ANN
            && e.operator() != Operator.BM25
            && e.operator() != Operator.ORDER_BY_ASC
            && e.operator() != Operator.ORDER_BY_DESC)
        {
            return null;
        }

        IndexHints hints = command.rowFilter().indexHints;
        return sim.getBestIndexFor(e, hints, StorageAttachedIndex.class).orElse(null);
    }
}
