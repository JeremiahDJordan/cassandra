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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.vector.OnHeapGraph;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.PriorityQueueIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.RamUsageEstimator;

import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;

public class VectorMemoryIndex extends MemoryIndex
{
    // an IndexedCell and its entry in indexedCells (a ConcurrentHashMap node and its slot in the table)
    private static final long INDEXED_CELL_BYTES = ObjectSizes.measure(new IndexedCell())
                                                   + RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                                                                                       + 3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
                                                                                       + Integer.BYTES)
                                                   + RamUsageEstimator.NUM_BYTES_OBJECT_REF;

    private final OnHeapGraph<PrimaryKey> graph;
    private final Memtable memtable;
    private final LongAdder writeCount = new LongAdder();

    private volatile KeyBounds keyBounds;

    private final NavigableSet<PrimaryKey> primaryKeys = new ConcurrentSkipListSet<>();

    // the cell each row is indexed for, see update()
    private final ConcurrentMap<PrimaryKey, IndexedCell> indexedCells = new ConcurrentHashMap<>();

    public VectorMemoryIndex(StorageAttachedIndex index, Memtable memtable)
    {
        super(index);
        this.graph = new OnHeapGraph<>(index.termType().indexType(), index.indexWriterConfig(), memtable);
        this.memtable = memtable;
    }

    /**
     * Indexes the row under the given vector. Only used for a vector that is part of the primary key, which never
     * changes for a row; the vector of a regular or static column is indexed by {@link #update}.
     */
    @Override
    public long add(DecoratedKey key, Clustering<?> clustering, ByteBuffer value)
    {
        if (value == null || value.remaining() == 0 || !index.validateTermSize(key, value, false, null))
            return 0;

        PrimaryKey primaryKey = index.hasClustering() ? index.keyFactory().create(key, clustering)
                                                      : index.keyFactory().create(key);
        return index(primaryKey, value);
    }

    private long index(PrimaryKey primaryKey, ByteBuffer value)
    {
        long bytesUsed = graph.add(value, primaryKey, OnHeapGraph.InvalidVectorBehavior.FAIL);
        writeCount.increment();
        primaryKeys.add(primaryKey);
        updateKeyBounds(primaryKey);
        return bytesUsed;
    }

    /**
     * Indexes the row under the vector of the given cell, in place of the vector it is indexed under, if the cell
     * supersedes the cell indexed for that row so far (see {@link Cells#reconcile}); the row is not indexed under any
     * vector if the cell is a tombstone or has expired.
     * <p>
     * The cells of concurrent writes of the same row can arrive in any order, and more than once: a write into an
     * {@link org.apache.cassandra.db.partitions.AtomicBTreePartition} retries its merge until it is the one that
     * publishes the partition, and every attempt passes its merged row to the index. Comparing each cell with the
     * cell indexed for the row so far, under a lock of that row, leaves the row indexed under the vector of its
     * reconciled cell whatever the order of the calls.
     *
     * @param cell the cell of the indexed column of the row
     */
    @Override
    public long update(DecoratedKey key, Clustering<?> clustering, Cell<?> cell)
    {
        PrimaryKey primaryKey = index.hasClustering() ? index.keyFactory().create(key, clustering)
                                                      : index.keyFactory().create(key);

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
            }
        }

        synchronized (indexed)
        {
            if (!indexed.isSupersededBy(cell, (VectorType<?>) index.termType().indexType()))
                return bytesUsed;

            ByteBuffer value = cell.isLive(FBUtilities.nowInSeconds()) ? cell.buffer() : null;
            if (value != null && (value.remaining() == 0 || !index.validateTermSize(key, value, false, null)))
                value = null;

            float[] oldVector = indexed.vector;
            float[] newVector = null;
            if (value != null)
            {
                if (oldVector != null && Arrays.equals(oldVector, index.termType().decomposeVector(value)))
                {
                    newVector = oldVector;
                }
                else
                {
                    // add the new vector before removing the old one, so we don't have a window where the row is not
                    // in the index at all
                    bytesUsed += index(primaryKey, value);
                    newVector = graph.vectorForKey(primaryKey);
                }
            }

            if (oldVector != null && newVector != oldVector)
            {
                bytesUsed -= graph.remove(oldVector, primaryKey);
                // remove primary key if it's no longer indexed
                if (newVector == null)
                    primaryKeys.remove(primaryKey);
            }

            indexed.set(cell, newVector);
        }
        return bytesUsed;
    }

    private synchronized void updateKeyBounds(PrimaryKey primaryKey)
    {
        KeyBounds current = keyBounds;
        keyBounds = current == null ? new KeyBounds(primaryKey, primaryKey) : current.withUpdated(primaryKey);
    }

    @Override
    public KeyRangeIterator search(QueryContext queryContext, Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CloseableIterator<PrimaryKeyWithScore> orderBy(QueryContext queryContext, Expression expr, AbstractBounds<PartitionPosition> keyRange)
    {
        assert expr.getIndexOperator() == Expression.IndexOperator.ANN : "Only ANN is supported for vector search, received " + expr.getIndexOperator();

        ByteBuffer buffer = expr.lower().value.raw;
        float[] qv = index.termType().decomposeVector(buffer);

        Bits bits;
        if (!RangeUtil.coversFullRing(keyRange))
        {
            // if left bound is MIN_BOUND or KEY_BOUND, we need to include all token-only PrimaryKeys with same token
            boolean leftInclusive = keyRange.left.kind() != PartitionPosition.Kind.MAX_BOUND;
            // if right bound is MAX_BOUND or KEY_BOUND, we need to include all token-only PrimaryKeys with same token
            boolean rightInclusive = keyRange.right.kind() != PartitionPosition.Kind.MIN_BOUND;
            // if right token is MAX (Long.MIN_VALUE), there is no upper bound
            boolean isMaxToken = keyRange.right.getToken().isMinimum(); // max token

            PrimaryKey left = index.keyFactory().create(keyRange.left.getToken()); // lower bound
            PrimaryKey right = isMaxToken ? null : index.keyFactory().create(keyRange.right.getToken()); // upper bound

            Set<PrimaryKey> resultKeys = isMaxToken ? primaryKeys.tailSet(left, leftInclusive) : primaryKeys.subSet(left, leftInclusive, right, rightInclusive);

            if (resultKeys.isEmpty())
                return CloseableIterator.empty();

            int bruteForceRows = maxBruteForceRows(queryContext.limit(), resultKeys.size(), graph.size());
            Tracing.trace("Search range covers {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                          resultKeys.size(), bruteForceRows, graph.size(), queryContext.limit());
            if (resultKeys.size() < Math.max(queryContext.limit(), bruteForceRows))
                return orderByBruteForce(qv, resultKeys);
            else
                bits = new KeyRangeFilteringBits(keyRange, null);
        }
        else
        {
            // Accept all bits
            bits = new Bits.MatchAllBits(Integer.MAX_VALUE);
        }

        CloseableIterator<SearchResult.NodeScore> iterator = graph.search(qv, queryContext.limit(), bits);
        return new NodeScoreToScoredPrimaryKeyIterator(iterator);
    }

    @Override
    public CloseableIterator<PrimaryKeyWithScore> orderResultsBy(QueryContext queryContext, List<PrimaryKey> results, Expression orderer)
    {
        KeyBounds bounds = keyBounds;
        if (bounds == null)
            return CloseableIterator.empty();

        int limit = queryContext.limit();

        List<PrimaryKey> resultsInRange = results.stream()
                                                 .dropWhile(k -> k.compareTo(bounds.minimum) < 0)
                                                 .takeWhile(k -> k.compareTo(bounds.maximum) <= 0)
                                                 .collect(Collectors.toList());

        int maxBruteForceRows = maxBruteForceRows(limit, resultsInRange.size(), graph.size());
        Tracing.trace("SAI materialized {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                      resultsInRange.size(), maxBruteForceRows, graph.size(), limit);

        if (resultsInRange.isEmpty())
            return CloseableIterator.empty();

        ByteBuffer buffer = orderer.lower().value.raw;
        float[] qv = index.termType().decomposeVector(buffer);

        if (resultsInRange.size() <= maxBruteForceRows)
            return orderByBruteForce(qv, resultsInRange);

        // Search the graph for the topK vectors near the query
        KeyFilteringBits bits = new KeyFilteringBits(resultsInRange);
        CloseableIterator<SearchResult.NodeScore> nodeScores = graph.search(qv, limit, bits);
        return new NodeScoreToScoredPrimaryKeyIterator(nodeScores);
    }

    private int maxBruteForceRows(int limit, int nPermittedOrdinals, int graphSize)
    {
        int expectedNodesVisited = expectedNodesVisited(limit, nPermittedOrdinals, graphSize);
        // ANN index will do a bunch of extra work besides the full comparisons
        // VSTODO I'm not sure which one is more expensive, but since the graph is in memory and the vectors are
        // full precision, the goal here is simple: minimize the number of vector comparisons (aka nodes visited).
        // As such, the cost function weights them at a 1:1 ratio for now.
        return max(limit, expectedNodesVisited);
    }

    private CloseableIterator<PrimaryKeyWithScore> orderByBruteForce(float[] queryVector, Collection<PrimaryKey> keys)
    {
        VectorSimilarityFunction similarityFunction = index.indexWriterConfig().getSimilarityFunction();
        List<PrimaryKeyWithScore> scoredKeys = new ArrayList<>(keys.size());
        for (PrimaryKey key : keys)
        {
            PrimaryKeyWithScore scoredKey = scoreKey(similarityFunction, queryVector, key);
            if (scoredKey != null)
                scoredKeys.add(scoredKey);
        }
        // Because we merge iterators from all sstables and memtables, we do not need a complete sort of these
        // elements, so a priority queue provides good performance.
        return new PriorityQueueIterator<>(new PriorityQueue<>(scoredKeys));
    }

    private PrimaryKeyWithScore scoreKey(VectorSimilarityFunction similarityFunction, float[] queryVector, PrimaryKey key)
    {
        float[] vector = graph.vectorForKey(key);
        if (vector == null)
            return null;
        float score = similarityFunction.compare(queryVector, vector);
        return new PrimaryKeyWithScore(index.termType().columnMetadata(), memtable, key, score);
    }

    /**
     * All parameters must be greater than zero.  nPermittedOrdinals may be larger than graphSize.
     */
    public static int expectedNodesVisited(int limit, int nPermittedOrdinals, int graphSize)
    {
        // constants are computed by Code Interpreter based on observed comparison counts in tests
        // https://chat.openai.com/share/2b1d7195-b4cf-4a45-8dce-1b9b2f893c75
        int sizeRestriction = min(nPermittedOrdinals, graphSize);
        int raw = (int) (0.7 * pow(log(graphSize), 2) *
                         pow(graphSize, 0.33) *
                         pow(log(limit), 2) *
                         pow(log((double) graphSize / sizeRestriction), 2) / pow(sizeRestriction, 0.13));
        // we will always visit at least min(limit, graphSize) nodes, and we can't visit more nodes than exist in the graph
        return min(max(raw, min(limit, graphSize)), graphSize);
    }

    @Override
    public Iterator<Pair<ByteComparable, PrimaryKeys>> iterator()
    {
        // This method is only used when merging an in-memory index with a RowMapping. This is done a different
        // way with the graph using the writeData method below.
        throw new UnsupportedOperationException();
    }

    public SegmentMetadata.ComponentMetadataMap writeDirect(IndexDescriptor indexDescriptor,
                                                            IndexIdentifier indexIdentifier,
                                                            Function<PrimaryKey, Integer> postingTransformer) throws IOException
    {
        return graph.writeData(indexDescriptor, indexIdentifier, postingTransformer);
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

    private class KeyRangeFilteringBits implements Bits
    {
        private final AbstractBounds<PartitionPosition> keyRange;
        @Nullable
        private final Bits bits;

        public KeyRangeFilteringBits(AbstractBounds<PartitionPosition> keyRange, @Nullable Bits bits)
        {
            this.keyRange = keyRange;
            this.bits = bits;
        }

        @Override
        public boolean get(int ordinal)
        {
            if (bits != null && !bits.get(ordinal))
                return false;

            Collection<PrimaryKey> keys = graph.keysFromOrdinal(ordinal);
            return keys.stream().anyMatch(k -> keyRange.contains(k.partitionKey()));
        }

        @Override
        public int length()
        {
            return graph.size();
        }
    }

    private class KeyFilteringBits implements Bits
    {
        private final List<PrimaryKey> results;

        public KeyFilteringBits(List<PrimaryKey> results)
        {
            this.results = results;
        }

        @Override
        public boolean get(int i)
        {
            Collection<PrimaryKey> pk = graph.keysFromOrdinal(i);
            return results.stream().anyMatch(pk::contains);
        }

        @Override
        public int length()
        {
            return results.size();
        }
    }

    /**
     * An iterator over {@link PrimaryKeyWithScore} sorted by score descending. The iterator converts ordinals (node ids)
     * to {@link PrimaryKey}s and pairs them with the score given by the index.
     */
    private class NodeScoreToScoredPrimaryKeyIterator extends AbstractIterator<PrimaryKeyWithScore>
    {
        private final CloseableIterator<SearchResult.NodeScore> nodeScores;
        private Iterator<PrimaryKeyWithScore> primaryKeysForNode = Collections.emptyIterator();

        NodeScoreToScoredPrimaryKeyIterator(CloseableIterator<SearchResult.NodeScore> nodeScores)
        {
            this.nodeScores = nodeScores;
        }

        @Override
        protected PrimaryKeyWithScore computeNext()
        {
            if (primaryKeysForNode.hasNext())
                return primaryKeysForNode.next();

            while (nodeScores.hasNext())
            {
                SearchResult.NodeScore nodeScore = nodeScores.next();
                primaryKeysForNode = graph.keysFromOrdinal(nodeScore.node)
                                          .stream()
                                          .map(pk -> new PrimaryKeyWithScore(index.termType().columnMetadata(), memtable, pk, nodeScore.score))
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

    /**
     * What the index knows of the cell a row is indexed for: enough to reconcile it with another cell of that row.
     * Guarded by its own monitor.
     */
    private static final class IndexedCell
    {
        private boolean isSet;
        private long timestamp;
        private int ttl;
        private long localDeletionTime;
        // the vector the row is indexed under, the instance held by the graph for the row, or null if none
        private float[] vector;

        /**
         * @return whether {@code cell} wins the reconciliation with the cell this was set from
         */
        boolean isSupersededBy(Cell<?> cell, VectorType<?> type)
        {
            if (!isSet)
                return true;
            if (cell.timestamp() != timestamp)
                return cell.timestamp() > timestamp;

            // Same timestamp: reconcile as the memtable does, with the vector the row is indexed under as the value.
            // Only a cell that is not indexed under any vector has a value other than its own here, an empty one.
            ByteBuffer value = vector == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : type.decomposeAsFloat(vector);
            Cell<?> current = new BufferCell(cell.column(), timestamp, ttl, localDeletionTime, value, null);
            return Cells.reconcile(current, cell) != current;
        }

        void set(Cell<?> cell, float[] vector)
        {
            isSet = true;
            timestamp = cell.timestamp();
            ttl = cell.ttl();
            localDeletionTime = cell.localDeletionTime();
            this.vector = vector;
        }
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
}
