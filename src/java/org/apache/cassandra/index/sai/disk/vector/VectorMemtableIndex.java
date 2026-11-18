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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToIntFunction;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Runnables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.agrona.collections.IntHashSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.memory.MemoryIndex;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyListUtil;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SortingIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;

public class VectorMemtableIndex extends AbstractMemtableIndex
{
    private static final Logger logger = LoggerFactory.getLogger(VectorMemtableIndex.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    public static int GLOBAL_BRUTE_FORCE_ROWS = Integer.MAX_VALUE; // not final so test can inject its own setting
    // an IndexedCell and its entry in indexedCells (a ConcurrentHashMap node and its slot in the table)
    private static final long INDEXED_CELL_BYTES = ObjectSizes.measure(new IndexedCell())
                                                   + RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                                                                                       + 3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
                                                                                       + Integer.BYTES)
                                                   + RamUsageEstimator.NUM_BYTES_OBJECT_REF;

    private final ColumnQueryMetrics.VectorIndexMetrics columnQueryMetrics;
    private final CassandraOnHeapGraph<PrimaryKey> graph;
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder overwriteCount = new LongAdder();
    private final LongAdder removedCount = new LongAdder();

    // Written only under updateKeyBounds()'s lock. A single volatile immutable holder, so the lock-free readers always
    // see a consistent (minimum, maximum) pair; null until the first key is indexed (as upstream's VectorMemoryIndex,
    // CASSANDRA-21160). The minimum only ever decreases and the maximum only ever increases.
    private volatile KeyBounds keyBounds;

    private final NavigableSet<PrimaryKey> primaryKeys = new ConcurrentSkipListSet<>();

    // the cell each row is indexed for, see update(DecoratedKey, Clustering, Cell, Memtable, OpOrder.Group)
    private final ConcurrentMap<PrimaryKey, IndexedCell> indexedCells = new ConcurrentHashMap<>();
    private final LongAdder indexedCellsBytes = new LongAdder();
    private final VectorType.VectorSerializer serializer;

    public VectorMemtableIndex(IndexContext indexContext, Memtable mt)
    {
        super(indexContext, mt);
        this.columnQueryMetrics = (ColumnQueryMetrics.VectorIndexMetrics) indexContext.getColumnQueryMetrics();
        this.graph = new CassandraOnHeapGraph<>(indexContext, true, mt);
        this.serializer = (VectorType.VectorSerializer) indexContext.getValidator().getSerializer();
    }

    @Override
    public Memtable getMemtable()
    {
        return memtable;
    }

    @VisibleForTesting
    public CassandraOnHeapGraph<PrimaryKey> getGraph()
    {
        return graph;
    }

    @Override
    public void index(DecoratedKey key, Clustering clustering, ByteBuffer value, Memtable memtable, OpOrder.Group opGroup)
    {
        if (value == null || value.remaining() == 0)
            return;

        var primaryKey = indexContext.keyFactory().create(key, clustering);
        long allocatedBytes = index(primaryKey, value);
        memtable.markExtraOnHeapUsed(allocatedBytes, opGroup);
        onIndexUpdated();
    }

    private long index(PrimaryKey primaryKey, ByteBuffer value)
    {
        if (value == null || value.remaining() == 0)
            return 0;

        updateKeyBounds(primaryKey);

        writeCount.increment();
        primaryKeys.add(primaryKey);
        return graph.add(value, primaryKey);
    }

    /**
     * Indexes the row of a vector on a regular or static column under the vector of the given cell, if the cell
     * supersedes the cell indexed for that row so far (see {@link Cells#reconcile}); a tombstone or an expired cell
     * leaves the row indexed under no vector. The cells of a row can arrive in any order and more than once, as an
     * {@link org.apache.cassandra.db.partitions.AtomicBTreePartition} write passes the row of each merge attempt.
     *
     * @param cell the cell of the indexed column of the row, or a tombstone at the time of a deletion of the row
     */
    public void update(DecoratedKey key, Clustering clustering, Cell<?> cell, Memtable memtable, OpOrder.Group opGroup)
    {
        PrimaryKey primaryKey = indexContext.keyFactory().create(key, clustering);

        long bytesUsed = 0;
        IndexedCell indexed = indexedCells.get(primaryKey);
        if (indexed == null)
        {
            IndexedCell created = new IndexedCell();
            indexed = indexedCells.putIfAbsent(primaryKey, created);
            if (indexed == null)
            {
                indexed = created;
                bytesUsed += INDEXED_CELL_BYTES;
                indexedCellsBytes.add(INDEXED_CELL_BYTES);
            }
        }

        boolean changed = false;
        synchronized (indexed)
        {
            if (indexed.isSupersededBy(cell, serializer))
            {
                ByteBuffer value = cell.isLive(FBUtilities.nowInSeconds()) ? cell.buffer() : null;
                if (value != null && value.remaining() == 0)
                    value = null;

                VectorFloat<?> oldVector = indexed.vector;
                VectorFloat<?> newVector = null;
                if (value != null)
                {
                    if (oldVector != null && oldVector.equals(vts.createFloatVector(serializer.deserializeFloatArray(value))))
                    {
                        newVector = oldVector;
                    }
                    else
                    {
                        // add the new vector before removing the old one, so we don't have a window where the row is
                        // not in the index at all
                        updateKeyBounds(primaryKey);
                        primaryKeys.add(primaryKey);
                        bytesUsed += graph.add(value, primaryKey);
                        if (oldVector == null)
                            writeCount.increment();
                        else
                            overwriteCount.increment();

                        // the graph ignores an invalid vector during commit log replay, and then keeps the old one
                        VectorFloat<?> added = graph.vectorForKey(primaryKey);
                        newVector = added == oldVector ? null : added;
                        changed = true;
                    }
                }

                if (oldVector != null && newVector != oldVector)
                {
                    graph.remove(oldVector, primaryKey);
                    changed = true;
                }

                // remove primary key if it's no longer indexed
                if (newVector == null && changed)
                {
                    primaryKeys.remove(primaryKey);
                    if (oldVector != null)
                        removedCount.increment();
                }

                indexed.set(cell, newVector);
            }
        }

        memtable.markExtraOnHeapUsed(bytesUsed, opGroup);
        if (changed)
            onIndexUpdated();
    }

    @Override
    public void update(DecoratedKey key, Clustering clustering, ByteBuffer oldValue, ByteBuffer newValue, Memtable memtable, OpOrder.Group opGroup)
    {
        int oldRemaining = oldValue == null ? 0 : oldValue.remaining();
        int newRemaining = newValue == null ? 0 : newValue.remaining();
        if (oldRemaining == 0 && newRemaining == 0)
            return;

        boolean different;
        if (oldRemaining != newRemaining)
        {
            assert oldRemaining == 0 || newRemaining == 0; // one of them is null
            different = true;
        }
        else
        {
            different = indexContext.getValidator().compare(oldValue, newValue) != 0;
        }

        if (different)
        {
            var primaryKey = indexContext.keyFactory().create(key, clustering);
            // update bounds because only rows with vectors are included in the key bounds,
            // so if the vector was null before, we won't have included it
            updateKeyBounds(primaryKey);

            // make the changes in this order so we don't have a window where the row is not in the index at all
            if (newRemaining > 0)
            {
                // the row may not have had a vector before, so its key may not be in primaryKeys yet, which key
                // restricted searches use
                primaryKeys.add(primaryKey);
                graph.add(newValue, primaryKey);
                overwriteCount.increment();
            }
            if (oldRemaining > 0)
                graph.remove(oldValue, primaryKey);

            // remove primary key if it's no longer indexed
            if (newRemaining <= 0 && oldRemaining > 0)
            {
                primaryKeys.remove(primaryKey);
                removedCount.increment();
            }

            onIndexUpdated();
        }
    }

    @Override
    public void update(DecoratedKey key, Clustering clustering, Iterator<ByteBuffer> oldValues, Iterator<ByteBuffer> newValues, Memtable memtable, OpOrder.Group opGroup)
    {
        throw new UnsupportedOperationException("Vector index does not support multi-value updates");
    }

    // Synchronized because index()/update() are called concurrently by writers (they are not serialized per
    // memtable), and an unsynchronized read-compare-write of the bounds can lose an update, which would make
    // orderResultsBy() drop keys that are in this memtable (CASSANDRA-21160).
    private synchronized void updateKeyBounds(PrimaryKey primaryKey) {
        KeyBounds bounds = keyBounds;
        keyBounds = bounds == null ? new KeyBounds(primaryKey, primaryKey) : bounds.withUpdated(primaryKey);
    }

    @Override
    public KeyRangeIterator search(QueryContext context, Expression expr, AbstractBounds<PartitionPosition> keyRange)
    {
        if (expr.getOp() != Expression.Op.BOUNDED_ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Only BOUNDED_ANN is supported, received: " + expr));
        var qv = vts.createFloatVector(expr.lower.value.vector);
        float threshold = expr.getEuclideanSearchThreshold();

        SortingIterator.Builder<PrimaryKey> keyQueue;
        // Threshold queries do not use pruning.
        try (var pkIterator = searchInternal(context, qv, keyRange, graph.size(), graph.size(), threshold, false))
        {
            keyQueue = new SortingIterator.Builder<>();
            while (pkIterator.hasNext())
                keyQueue.add(pkIterator.next().primaryKey());
        }

        if (keyQueue.size() == 0)
            return KeyRangeIterator.empty();
        return new ReorderingKeyRangeIterator(keyBounds, keyQueue.build(Comparator.naturalOrder()), keyQueue.size());
    }

    @Override
    public long estimateMatchingRowsCount(Expression expression)
    {
        // For BOUNDED_ANN we use the old way of estimating cardinality - by running the search.
        throw new UnsupportedOperationException("Cardinality estimation not supported by vector indexes");
    }

    @Override
    public List<CloseableIterator<PrimaryKeyWithSortKey>> orderBy(QueryContext context,
                                                                  Orderer orderer,
                                                                  Expression slice,
                                                                  AbstractBounds<PartitionPosition> keyRange,
                                                                  int limit)
    {
        assert slice == null : "ANN does not support index slicing";
        assert orderer.isANN() : "Only ANN is supported for vector search, received " + orderer.operator;

        var qv = orderer.getVectorTerm();
        var rerankK = orderer.rerankKFor(limit, VectorCompression.NO_COMPRESSION);

        return List.of(searchInternal(context, qv, keyRange, limit, rerankK, 0, orderer.usePruning()));
    }

    private CloseableIterator<PrimaryKeyWithSortKey> searchInternal(QueryContext context,
                                                                    VectorFloat<?> queryVector,
                                                                    AbstractBounds<PartitionPosition> keyRange,
                                                                    int limit,
                                                                    int rerankK,
                                                                    float threshold,
                                                                    boolean usePruning)
    {
        Bits bits;
        if (RangeUtil.coversFullRing(keyRange))
        {
            bits = Bits.ALL;
        }
        else
        {
            // if left bound is MIN_BOUND or KEY_BOUND, we need to include all token-only PrimaryKeys with same token
            boolean leftInclusive = keyRange.left.kind() != PartitionPosition.Kind.MAX_BOUND;
            // if right bound is MAX_BOUND or KEY_BOUND, we need to include all token-only PrimaryKeys with same token
            boolean rightInclusive = keyRange.right.kind() != PartitionPosition.Kind.MIN_BOUND;
            // if right token is MAX (Long.MIN_VALUE), there is no upper bound
            boolean isMaxToken = keyRange.right.getToken().isMinimum(); // max token

            PrimaryKey left = indexContext.keyFactory().createTokenOnly(keyRange.left.getToken()); // lower bound
            PrimaryKey right = isMaxToken ? null : indexContext.keyFactory().createTokenOnly(keyRange.right.getToken()); // upper bound

            NavigableSet<PrimaryKey> resultKeys = isMaxToken ? primaryKeys.tailSet(left, leftInclusive)
                                                             : primaryKeys.subSet(left, leftInclusive, right, rightInclusive);

            if (resultKeys.isEmpty())
                return CloseableIterator.emptyIterator();

            // The size of a view of a skip list is counted by walking it, so count it once
            int rangeSize = resultKeys.size();
            int bruteForceRows = maxBruteForceRows(rerankK, rangeSize, graph.size());
            logger.trace("Search range covers {} rows; max brute force rows is {} for memtable index with {} nodes, rerankK {}, LIMIT {}",
                         rangeSize, bruteForceRows, graph.size(), rerankK, limit);
            Tracing.trace("Search range covers {} rows; max brute force rows is {} for memtable index with {} nodes, rerankK {}, LIMIT {}",
                          rangeSize, bruteForceRows, graph.size(), rerankK, limit);
            if (rangeSize <= bruteForceRows)
            {
                // A copy: resultKeys is a view of primaryKeys, which writes change concurrently, and the brute
                // force sizes its arrays from the number of keys.
                List<PrimaryKey> keys = new ArrayList<>(resultKeys);
                // When we have a threshold, we only need to filter the results, not order them, because it means we're
                // evaluating a boolean predicate in the SAI pipeline that wants to collate by PK
                if (threshold > 0)
                    return filterByBruteForce(queryVector, threshold, keys);
                else
                    return orderByBruteForce(queryVector, keys);
            }
            else
            {
                bits = new KeyRangeFilteringBits(keyRange);
            }
        }

        var nodeScoreIterator = graph.search(context, queryVector, limit, rerankK, threshold, usePruning, bits);
        return new NodeScoreToScoredPrimaryKeyIterator(nodeScoreIterator);
    }


    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderResultsBy(QueryContext context, List<PrimaryKey> keys, Orderer orderer, int limit)
    {
        KeyBounds bounds = keyBounds;
        if (bounds == null)
            return CloseableIterator.emptyIterator();

        assert orderer.isANN() : "Only ANN is supported for vector search, received " + orderer;
        // Compute the keys that exist in the current memtable and their corresponding graph ordinals
        var keysInGraph = new HashSet<PrimaryKey>();
        var relevantOrdinals = new IntHashSet();

        var keysInRange = PrimaryKeyListUtil.getKeysInRange(keys, bounds.minimum, bounds.maximum);
        boolean isStatic = indexContext.getDefinition().isStatic();

        keysInRange.forEach(k ->
        {
            // if the indexed column is static, we need to get the static row associated with the non-static row that
            // might be referenced by the key
            if (isStatic)
                k = k.forStaticRow();

            var v = graph.vectorForKey(k);
            if (v == null)
                return;
            var i = graph.getOrdinal(v);
            if (i < 0)
                // might happen if the vector and/or its postings have been removed in the meantime between getting the
                // vector and getting the ordinal (graph#vectorForKey and graph#getOrdinal are not synchronized)
                return;
            keysInGraph.add(k);
            relevantOrdinals.add(i);
        });

        int rerankK = orderer.rerankKFor(limit, VectorCompression.NO_COMPRESSION);
        int maxBruteForceRows = maxBruteForceRows(rerankK, relevantOrdinals.size(), graph.size());
        Tracing.logAndTrace(logger, "{} rows relevant to current memtable out of {} materialized by SAI; max brute force rows is {} for memtable index with {} nodes, rerankK {}",
                            relevantOrdinals.size(), keys.size(), maxBruteForceRows, graph.size(), rerankK);

        // convert the expression value to query vector
        var qv = orderer.getVectorTerm();
        // brute force path
        if (keysInGraph.size() <= maxBruteForceRows)
        {
            if (keysInGraph.isEmpty())
                return CloseableIterator.emptyIterator();
            return orderByBruteForce(qv, keysInGraph);
        }
        // indexed path
        var nodeScoreIterator = graph.search(context, qv, limit, rerankK, 0, orderer.usePruning(), relevantOrdinals::contains);
        return new NodeScoreToScoredPrimaryKeyIterator(nodeScoreIterator);
    }

    /**
     * Filter the keys in the provided set by comparing their vectors to the query vector and returning only those
     * that have a similarity score >= the provided threshold.
     * NOTE: because the threshold is not used for ordering, the result is returned in PK order, not score order.
     * @param queryVector the query vector
     * @param threshold the minimum similarity score to accept
     * @param keys the keys to filter
     * @return an iterator over the keys that pass the filter in PK order
     */
    private CloseableIterator<PrimaryKeyWithSortKey> filterByBruteForce(VectorFloat<?> queryVector, float threshold, Collection<PrimaryKey> keys)
    {
        columnQueryMetrics.onBruteForceNodesReranked(keys.size());
        // Keys are already ordered in ascending PK order, so just use an ArrayList to collect the results.
        var results = new ArrayList<PrimaryKeyWithSortKey>(keys.size());
        scoreKeysAndAddToCollector(queryVector, keys, threshold, results);
        return CloseableIterator.wrap(results.iterator());
    }

    private CloseableIterator<PrimaryKeyWithSortKey> orderByBruteForce(VectorFloat<?> queryVector, Collection<PrimaryKey> keys)
    {
        columnQueryMetrics.onBruteForceNodesReranked(keys.size());
        // Use a sorting iterator because we often don't need to consume the entire iterator
        var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        return SortingIterator.createCloseable(Comparator.naturalOrder(),
                                               keys,
                                               key -> scoreKey(similarityFunction, queryVector, key, 0),
                                               Runnables.doNothing());
    }

    private void scoreKeysAndAddToCollector(VectorFloat<?> queryVector,
                                            Collection<PrimaryKey> keys,
                                            float threshold,
                                            Collection<PrimaryKeyWithSortKey> collector)
    {
        var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        for (var key : keys)
        {
            var scored = scoreKey(similarityFunction, queryVector, key, threshold);
            if (scored != null)
                collector.add(scored);
        }
    }

    private PrimaryKeyWithScore scoreKey(VectorSimilarityFunction similarityFunction, VectorFloat<?> queryVector, PrimaryKey key, float threshold)
    {
        var vector = graph.vectorForKey(key);
        if (vector == null)
            return null;
        var score = similarityFunction.compare(queryVector, vector);
        if (score < threshold)
            return null;
        return new PrimaryKeyWithScore(indexContext, memtable, key, score);
    }

    private int maxBruteForceRows(int rerankK, int nPermittedOrdinals, int graphSize)
    {
        int expectedNodesVisited = expectedNodesVisited(rerankK, nPermittedOrdinals, graphSize);
        return min(max(rerankK, expectedNodesVisited), GLOBAL_BRUTE_FORCE_ROWS);
    }

    public int estimateAnnNodesVisited(int rerankK, int nPermittedOrdinals)
    {
        return expectedNodesVisited(rerankK, nPermittedOrdinals, graph.size());
    }

    /**
     * All parameters must be greater than zero.  nPermittedOrdinals may be larger than graphSize.
     * <p>
     * Returns the expected number of nodes visited by an ANN search.
     * !!!
     * !!! "Visted" means we compute the coarse similarity with the query vector.  This is
     * !!! roughly `degree` times larger than the number of nodes whose edge lists we load!
     * !!!
     */
    public static int expectedNodesVisited(int rerankK, int nPermittedOrdinals, int graphSize)
    {
        var K = rerankK;
        var B = min(nPermittedOrdinals, graphSize);
        var N = graphSize;
        // These constants come from running many searches on a variety of datasets and graph sizes.
        // * It is very consistent that the visited count is slightly less than linear wrt K, for both
        //   unconstrained (B = N) and constrained (B < N) searches.
        // * The behavior wrt B is hard to characterize.  Graphing the result F vs N/B shows ranges of
        //   growth very close to linear, interspersed with sharp jumps up to a higher visit count.  Overall,
        //   approximating it as linear is in the right ballpark.
        // * For unconstrained searches, the visited count is closest to log(N) but for constrained searches
        //   it is closer to log(N)**2 (or a higher exponent), perhaps as a result of N/B being too small.
        //
        // If we need to make this even more accurate, the relationship to B and to log(N) may be the best
        // places to start.
        var raw = (int) (100 + 0.025 * pow(log(N), 2) * pow(K, 0.95) * ((double) N / B));
        return ensureSaneEstimate(raw, rerankK, graphSize);
    }

    public static int ensureSaneEstimate(int rawEstimate, int rerankK, int graphSize)
    {
        // we will always visit at least min(rerankK, graphSize) nodes, and we can't visit more nodes than exist in the graph
        return min(max(rawEstimate, min(rerankK, graphSize)), graphSize);
    }

    @Override
    public Iterator<Pair<ByteComparable.Preencoded, List<MemoryIndex.PkWithFrequency>>> iterator(DecoratedKey min, DecoratedKey max)
    {
        // This method is only used when merging an in-memory index with a RowMapping. This is done a different
        // way with the graph using the writeData method below.
        throw new UnsupportedOperationException();
    }

    /** returns true if the index is non-empty and should be flushed */
    public boolean preFlush(ToIntFunction<PrimaryKey> ordinalMapper)
    {
        return graph.preFlush(ordinalMapper, this::flushedValue);
    }

    // The indexed value of the key's row in this memtable, or null. An expired value counts (as in
    // IndexContext#update); shadowing deletions are not applied, so queries discard the posting when they read the row.
    private ByteBuffer flushedValue(PrimaryKey key)
    {
        Partition data = memtable.getPartition(key.partitionKey());
        if (data == null)
            return null;

        ColumnMetadata column = indexContext.getDefinition();
        Slices slices = column.isStatic() ? Slices.NONE : Slices.with(indexContext.comparator(), Slice.make(key.clustering()));
        ColumnFilter columnFilter = ColumnFilter.selection(RegularAndStaticColumns.of(column));
        try (UnfilteredRowIterator partition = data.unfilteredIterator(columnFilter, slices, false))
        {
            if (column.isStatic())
                return indexContext.getValueOf(key.partitionKey(), partition.staticRow(), 0);
            while (partition.hasNext())
            {
                Unfiltered unfiltered = partition.next();
                if (unfiltered.isRow())
                    return indexContext.getValueOf(key.partitionKey(), (Row) unfiltered, 0);
            }
            return null;
        }
    }

    @Override
    public int getRowCount()
    {
        return graph.size();
    }

    @Override
    public long getApproximateTermCount()
    {
        throw new UnsupportedOperationException("Getting number of terms not supported by vector indexes");
    }

    public SegmentMetadata.ComponentMetadataMap writeData(IndexComponents.ForWrite perIndexComponents) throws IOException
    {
        // Note that range deletions won't show up in the removed count, which is why it's just named removedCount and
        // not deleted count.
        logger.debug("Writing {} nodes to disk after {} inserts, {} overwrites, and {} removals for {}", graph.size(),
                     writeCount.longValue(), overwriteCount.longValue(), removedCount.longValue(), perIndexComponents.descriptor());
        return graph.flush(perIndexComponents);
    }

    @Override
    public long writeCount()
    {
        return writeCount.longValue() + overwriteCount.longValue();
    }

    @Override
    public long estimatedOnHeapMemoryUsed()
    {
        return graph.ramBytesUsed() + indexedCellsBytes.sum();
    }

    @Override
    public long estimatedOffHeapMemoryUsed()
    {
        return 0;
    }

    @Override
    public boolean isEmpty()
    {
        return graph.isEmpty();
    }

    @Nullable
    @Override
    public ByteBuffer getMinTerm()
    {
        return null;
    }

    @Nullable
    @Override
    public ByteBuffer getMaxTerm()
    {
        return null;
    }

    /**
     * A {@link Bits} implementation that filters out all ordinals that do not correspond to a {@link PrimaryKey}
     * in the provided {@link AbstractBounds<PartitionPosition>}.
     */
    private class KeyRangeFilteringBits implements Bits
    {
        private final AbstractBounds<PartitionPosition> keyRange;

        public KeyRangeFilteringBits(AbstractBounds<PartitionPosition> keyRange)
        {
            this.keyRange = keyRange;
        }

        @Override
        public boolean get(int ordinal)
        {
            var keys = graph.keysFromOrdinal(ordinal);
            return keys.stream().anyMatch(k -> keyRange.contains(k.partitionKey()));
        }
    }

    private class ReorderingKeyRangeIterator extends KeyRangeIterator
    {
        private final SortingIterator<PrimaryKey> keyQueue;

        ReorderingKeyRangeIterator(KeyBounds bounds, SortingIterator<PrimaryKey> keyQueue, int expectedSize)
        {
            super(bounds.minimum, bounds.maximum, expectedSize);
            this.keyQueue = keyQueue;
        }

        @Override
        protected void performSkipTo(PrimaryKey nextKey)
        {
            keyQueue.skipTo(nextKey);
        }

        @Override
        public void close() {}

        @Override
        protected PrimaryKey computeNext()
        {
            if (!keyQueue.hasNext())
                return endOfData();
            return keyQueue.next();
        }
    }

    /**
     * An iterator over {@link PrimaryKeyWithSortKey} sorted by score descending. The iterator converts ordinals (node ids)
     * to {@link PrimaryKey}s and pairs them with the score given by the index.
     */
    private class NodeScoreToScoredPrimaryKeyIterator extends AbstractIterator<PrimaryKeyWithSortKey>
    {
        private final CloseableIterator<SearchResult.NodeScore> nodeScores;
        private Iterator<PrimaryKeyWithScore> primaryKeysForNode = Collections.emptyIterator();

        NodeScoreToScoredPrimaryKeyIterator(CloseableIterator<SearchResult.NodeScore> nodeScores)
        {
            this.nodeScores = nodeScores;
        }

        @Override
        protected PrimaryKeyWithSortKey computeNext()
        {
            if (primaryKeysForNode.hasNext())
                return primaryKeysForNode.next();

            while (nodeScores.hasNext())
            {
                SearchResult.NodeScore nodeScore = nodeScores.next();
                primaryKeysForNode = graph.keysFromOrdinal(nodeScore.node)
                                          .stream()
                                          .map(pk -> new PrimaryKeyWithScore(indexContext, memtable, pk, nodeScore.score))
                                          .iterator();
                if (primaryKeysForNode.hasNext())
                    return primaryKeysForNode.next();
            }

            return endOfData();
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(nodeScores);
        }
    }

    /** ensures that the graph is connected -- normally not necessary but it can help tests reason about the state */
    public void cleanup()
    {
        graph.cleanup();
    }

    private static final class KeyBounds
    {
        final PrimaryKey minimum;
        final PrimaryKey maximum;

        KeyBounds(PrimaryKey minimum, PrimaryKey maximum)
        {
            this.minimum = minimum;
            this.maximum = maximum;
        }

        KeyBounds withUpdated(PrimaryKey key)
        {
            PrimaryKey newMin = minimum.compareTo(key) > 0 ? key : minimum;
            PrimaryKey newMax = maximum.compareTo(key) < 0 ? key : maximum;

            // Avoid allocation if nothing changed
            return newMin == minimum && newMax == maximum ? this : new KeyBounds(newMin, newMax);
        }
    }

    /**
     * What the index knows of the cell a row is indexed for: enough to reconcile it with another cell of that row.
     * Guarded by its own monitor.
     */
    private static final class IndexedCell
    {
        private boolean isSet;
        private long timestamp;
        private int ttl;
        private int localDeletionTime;
        // the vector the row is indexed under, the instance held by the graph for the row, or null if none
        private VectorFloat<?> vector;

        boolean isSupersededBy(Cell<?> cell, VectorType.VectorSerializer serializer)
        {
            if (!isSet)
                return true;
            if (cell.timestamp() != timestamp)
                return cell.timestamp() > timestamp;

            // Same timestamp: reconcile as the memtable does, with the vector the row is indexed under as the value.
            // Only a cell that is not indexed under any vector has a value other than its own here, an empty one.
            ByteBuffer value = vector == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : serializer.serializeFloatArray(toFloatArray(vector));
            Cell<?> current = new BufferCell(cell.column(), timestamp, ttl, localDeletionTime, value, null);
            return Cells.reconcile(current, cell) != current;
        }

        void set(Cell<?> cell, VectorFloat<?> vector)
        {
            isSet = true;
            timestamp = cell.timestamp();
            ttl = cell.ttl();
            localDeletionTime = cell.localDeletionTime();
            this.vector = vector;
        }

        private static float[] toFloatArray(VectorFloat<?> vector)
        {
            float[] values = new float[vector.length()];
            for (int i = 0; i < values.length; i++)
                values[i] = vector.get(i);
            return values;
        }
    }
}
