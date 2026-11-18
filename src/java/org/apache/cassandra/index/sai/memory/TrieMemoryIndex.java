/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.ByteLimitedMaterializer;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.disk.v6.TermsDistribution;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.AutomatonQueries;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithByteComparable;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.BinaryHeap;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.automaton.CompiledAutomaton;

public class TrieMemoryIndex extends MemoryIndex
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemoryIndex.class);
    private static final int MINIMUM_QUEUE_SIZE = 128;
    private static final int MAX_RECURSIVE_KEY_LENGTH = 128;

    private final InMemoryTrie<PrimaryKeys> data;
    private final LongAdder primaryKeysHeapAllocations;
    private final PrimaryKeysAccumulator primaryKeysAccumulator;
    private final PrimaryKeysRemover primaryKeysRemover;
    private final Map<PrimaryKey, Integer> docLengths = new HashMap<>();
    private volatile int indexedRows = 0;
    private volatile long totalTermCount = 0;

    private final Memtable memtable;
    private AbstractBounds<PartitionPosition> keyBounds;

    /**
     * The maximum number of terms whose rows are counted when estimating a range
     * (see {@link #estimateNumRowsInTermRange}).
     */
    private static final int MAX_COUNTED_TERMS = 256;
    /**
     * The maximum number of rows counted when estimating a range (see {@link #estimateNumRowsInTermRange}): the keys
     * of a term are a skip list whose size is linear to compute, so counting stops after this many keys.
     */
    private static final int MAX_COUNTED_ROWS = 4096;

    private ByteBuffer minTerm;
    private ByteBuffer maxTerm;
    private final Version version;

    private static final FastThreadLocal<Integer> lastQueueSize = new FastThreadLocal<Integer>()
    {
        protected Integer initialValue()
        {
            return MINIMUM_QUEUE_SIZE;
        }
    };

    @VisibleForTesting
    public TrieMemoryIndex(IndexContext indexContext)
    {
        this(indexContext, null, AbstractBounds.unbounded(indexContext.getPartitioner()));
    }

    public TrieMemoryIndex(IndexContext indexContext, Memtable memtable, AbstractBounds<PartitionPosition> keyBounds)
    {
        super(indexContext);
        this.version = indexContext.version();
        this.keyBounds = keyBounds;
        this.primaryKeysHeapAllocations = new LongAdder();
        this.primaryKeysAccumulator = new PrimaryKeysAccumulator(primaryKeysHeapAllocations);
        this.primaryKeysRemover = new PrimaryKeysRemover(primaryKeysHeapAllocations);
        this.data = InMemoryTrie.longLivedOrdered(TypeUtil.byteComparableVersionForTermsData(indexContext.version()), TrieMemtable.BUFFER_TYPE, indexContext.columnFamilyStore().readOrdering());
        this.memtable = memtable;
    }

    public synchronized Map<PrimaryKey, Integer> getDocLengths()
    {
        return docLengths;
    }

    @Override
    public int indexedRows()
    {
        return indexedRows;
    }

    /**
     * The count of terms for indexed rows is maintained during insertions and updates.
     * Deletes are not accounted for. Thus, the count is approximated.
     *
     * @return the total number of terms in the indexed rows
     */
    public long approximateTotalTermCount()
    {
        return totalTermCount;
    }

    public synchronized void add(DecoratedKey key,
                                 Clustering clustering,
                                 ByteBuffer value,
                                 LongConsumer onHeapAllocationsTracker,
                                 LongConsumer offHeapAllocationsTracker)
    {
        final PrimaryKey primaryKey = indexContext.keyFactory().create(key, clustering);
        int tokenCount = applyTransformer(primaryKey, value, onHeapAllocationsTracker, offHeapAllocationsTracker, primaryKeysAccumulator);
        recordAddedDocLength(primaryKey, tokenCount, onHeapAllocationsTracker);
    }

    public synchronized void update(DecoratedKey key,
                                    Clustering clustering,
                                    ByteBuffer oldValue,
                                    ByteBuffer newValue,
                                    LongConsumer onHeapAllocationsTracker,
                                    LongConsumer offHeapAllocationsTracker)
    {
        final PrimaryKey primaryKey = indexContext.keyFactory().create(key, clustering);

        // A row has one value here: add the terms of the new value, resetting the frequency of each the first time it
        // is seen, then remove the row from the old value's terms that the new value doesn't hold. Add before removing
        // so the row is never missing from the index.
        Set<PrimaryKeys> seenPrimaryKeys = new HashSet<>();
        primaryKeysAccumulator.seenPrimaryKeys = seenPrimaryKeys;
        primaryKeysRemover.seenPrimaryKeys = seenPrimaryKeys;
        try
        {
            boolean indexesNewValue = TypeUtil.indexesValue(newValue, indexContext.getValidator());
            int newTokenCount = indexesNewValue
                                ? applyTransformer(primaryKey, newValue, onHeapAllocationsTracker, offHeapAllocationsTracker, primaryKeysAccumulator)
                                : 0;
            if (TypeUtil.indexesValue(oldValue, indexContext.getValidator()))
                applyTransformer(primaryKey, oldValue, onHeapAllocationsTracker, offHeapAllocationsTracker, primaryKeysRemover);

            // The document length is that of the new value, or none if the update indexes no new value
            if (indexesNewValue)
                recordUpdatedDocLength(primaryKey, newTokenCount, onHeapAllocationsTracker);
            else
                forgetDocLength(primaryKey);
        }
        finally
        {
            primaryKeysAccumulator.seenPrimaryKeys = null;
            primaryKeysRemover.seenPrimaryKeys = null;
        }
    }

    public synchronized void update(DecoratedKey key,
                                    Clustering clustering,
                                    Iterator<ByteBuffer> oldValues,
                                    Iterator<ByteBuffer> newValues,
                                    LongConsumer onHeapAllocationsTracker,
                                    LongConsumer offHeapAllocationsTracker)
    {
        final PrimaryKey primaryKey = indexContext.keyFactory().create(key, clustering);

        // Add before removing so the row is never missing. Each term counts the occurrences of the row, so removing an
        // old element releases only its own occurrence.
        while (newValues != null && newValues.hasNext())
        {
            ByteBuffer newValue = newValues.next();
            if (TypeUtil.indexesValue(newValue, indexContext.getValidator()))
            {
                int tokenCount = applyTransformer(primaryKey, newValue, onHeapAllocationsTracker, offHeapAllocationsTracker, primaryKeysAccumulator);
                recordAddedDocLength(primaryKey, tokenCount, onHeapAllocationsTracker);
            }
        }

        while (oldValues != null && oldValues.hasNext())
        {
            ByteBuffer oldValue = oldValues.next();
            if (TypeUtil.indexesValue(oldValue, indexContext.getValidator()))
            {
                int tokenCount = applyTransformer(primaryKey, oldValue, onHeapAllocationsTracker, offHeapAllocationsTracker, primaryKeysRemover);
                if (docLengths.containsKey(primaryKey))
                    totalTermCount -= tokenCount;
            }
        }
    }

    // Adds or removes the terms of a value of the row with the transformer, and returns the number of terms
    private int applyTransformer(PrimaryKey primaryKey,
                                 ByteBuffer value,
                                 LongConsumer onHeapAllocationsTracker,
                                 LongConsumer offHeapAllocationsTracker,
                                 InMemoryTrie.UpsertTransformer<PrimaryKeys, PrimaryKey> transformer)
    {
        // The terms the sstable writers produce (see SegmentBuilder#analyzeAndAdd), so a value is indexed the same way
        // in memtables and sstables.
        value = TypeUtil.asIndexBytes(value, indexContext.getValidator());
        List<ByteBuffer> terms = ByteLimitedMaterializer.materializeTokens(indexContext.getAnalyzerFactory().create(), value, indexContext, primaryKey);
        final long initialSizeOnHeap = data.usedSizeOnHeap();
        final long initialSizeOffHeap = data.usedSizeOffHeap();
        final long initialPrimaryKeysHeapAllocations = primaryKeysHeapAllocations.longValue();

        int tokenCount = 0;
        for (ByteBuffer term : terms)
        {
            if (!indexContext.validateMaxTermSize(primaryKey.partitionKey(), term))
                continue;

            tokenCount++;

            // Note that this term is already encoded once by the TypeUtil.encode call above.
            setMinMaxTerm(term.duplicate());

            final ByteComparable encodedTerm = asByteComparable(term.duplicate());

            try
            {
                data.putSingleton(encodedTerm, primaryKey, transformer, term.remaining() <= MAX_RECURSIVE_KEY_LENGTH);
            }
            catch (TrieSpaceExhaustedException e)
            {
                Throwables.throwAsUncheckedException(e);
            }
        }

        // memory used by the trie
        onHeapAllocationsTracker.accept((data.usedSizeOnHeap() - initialSizeOnHeap) +
                                        (primaryKeysHeapAllocations.longValue() - initialPrimaryKeysHeapAllocations));
        offHeapAllocationsTracker.accept(data.usedSizeOffHeap() - initialSizeOffHeap);
        return tokenCount;
    }

    // A row with several values (collection elements) has the document length of the last; only BM25 uses it
    private void recordAddedDocLength(PrimaryKey primaryKey, int tokenCount, LongConsumer onHeapAllocationsTracker)
    {
        Integer previous = docLengths.put(primaryKey, tokenCount);
        totalTermCount += tokenCount;
        if (previous == null)
            indexedRows++;
        else
            onHeapAllocationsTracker.accept(docLengthHeapSize(primaryKey));
    }

    // Only the new value of an update sets the document length
    private void recordUpdatedDocLength(PrimaryKey primaryKey, int tokenCount, LongConsumer onHeapAllocationsTracker)
    {
        Integer previous = docLengths.put(primaryKey, tokenCount);
        if (previous == null)
        {
            indexedRows++;
            totalTermCount += tokenCount;
        }
        else
        {
            totalTermCount += tokenCount - previous;
            onHeapAllocationsTracker.accept(docLengthHeapSize(primaryKey));
        }
    }

    private void forgetDocLength(PrimaryKey primaryKey)
    {
        Integer previous = docLengths.remove(primaryKey);
        if (previous != null)
        {
            indexedRows--;
            totalTermCount -= previous;
        }
    }

    private static long docLengthHeapSize(PrimaryKey primaryKey)
    {
        return RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY
               + primaryKey.ramBytesUsed() // TODO do we count these bytes?
               + Integer.BYTES;
    }

    @Override
    public KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        if (logger.isTraceEnabled())
            logger.trace("Searching memtable index on expression '{}'...", expression);

        switch (expression.getOp())
        {
            case MATCH:
            case EQ:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
                return exactMatch(expression, keyRange);
            case RANGE:
            case PREFIX:
                return rangeMatch(expression, keyRange);
            case AUTOMATON:
                // the terms starting with the automaton's common prefix, filtered by the automaton
                return automatonMatch(expression.getAutomaton(), getSubtrie(expression), keyRange);
            default:
                throw new IllegalArgumentException("Unsupported expression: " + expression);
        }
    }

    @Override
    public Iterator<Pair<ByteComparable.Preencoded, List<PkWithFrequency>>> iterator()
    {
        Iterator<Map.Entry<ByteComparable.Preencoded, PrimaryKeys>> iterator = data.entrySet().iterator();
        return new AbstractGuavaIterator<>()
        {
            @Override
            public Pair<ByteComparable.Preencoded, List<PkWithFrequency>> computeNext()
            {
                while (iterator.hasNext())
                {
                    Map.Entry<ByteComparable.Preencoded, PrimaryKeys> entry = iterator.next();
                    PrimaryKeys primaryKeys = entry.getValue();
                    if (primaryKeys.isEmpty())
                        continue;

                    var pairs = new ArrayList<PkWithFrequency>(primaryKeys.size());
                    Iterators.addAll(pairs, primaryKeys.iterator());
                    return Pair.create(entry.getKey(), pairs);
                }
                return endOfData();
            }
        };
    }

    @VisibleForTesting
    long estimatedTrieValuesMemoryUsed()
    {
        return primaryKeysHeapAllocations.longValue();
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderBy(Orderer orderer, @Nullable Expression filter)
    {
        if (data.isEmpty())
            return CloseableIterator.emptyIterator();

        Trie<PrimaryKeys> subtrie = getSubtrie(filter);
        var iter = subtrie.entrySet(orderer.isAscending() ? Direction.FORWARD : Direction.REVERSE).iterator();
        if (filter != null && filter.getOp() == Expression.Op.AUTOMATON)
            iter = acceptedEntries(iter, filter.getAutomaton());
        return new AllTermsIterator(iter);
    }

    /**
     * Filters trie entries by an automaton. Only valid for non-composite literal types, whose trie keys are the
     * raw (already analyzed) term bytes.
     */
    private static Iterator<Map.Entry<ByteComparable.Preencoded, PrimaryKeys>> acceptedEntries(Iterator<Map.Entry<ByteComparable.Preencoded, PrimaryKeys>> entries,
                                                                                               CompiledAutomaton automaton)
    {
        return Iterators.filter(entries, entry -> LikePatterns.accepts(automaton, ByteSourceInverse.readBytes(entry.getKey().getPreencodedBytes())));
    }

    private ByteComparable asByteComparable(ByteBuffer input)
    {
        return version.onDiskFormat().encodeForTrie(input, indexContext.getValidator());
    }

    public KeyRangeIterator exactMatch(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        final ByteComparable prefix = expression.lower == null ? ByteComparable.EMPTY : asByteComparable(expression.lower.value.encoded);
        final PrimaryKeys primaryKeys = data.get(prefix);
        if (primaryKeys == null || primaryKeys.keys().isEmpty())
        {
            return KeyRangeIterator.empty();
        }
        return new FilteringKeyRangeIterator(new SortedSetKeyRangeIterator(primaryKeys.keys(), mayHaveStaticKeys(), indexContext.keyFactory()), keyRange);
    }

    // Only an index on a static or partition key column of a table with static columns holds static row keys; checked
    // when a search starts, as a static column can be added during the lifetime of the memtable
    private boolean mayHaveStaticKeys()
    {
        ColumnMetadata column = indexContext.getDefinition();
        return (column.isStatic() || column.isPartitionKey()) && indexContext.columnFamilyStore().metadata().hasStaticColumns();
    }

    /**
     * Accumulator that adds an occurrence of a primary key to the primary keys set; its frequency counts them.
     */
    static class PrimaryKeysAccumulator implements InMemoryTrie.UpsertTransformer<PrimaryKeys, PrimaryKey>
    {
        private final LongAdder heapAllocations;
        // the terms of the new single value seen so far during an update (under the lock of the TrieMemoryIndex)
        private Set<PrimaryKeys> seenPrimaryKeys;

        PrimaryKeysAccumulator(LongAdder heapAllocations)
        {
            this.heapAllocations = heapAllocations;
        }

        @Override
        public PrimaryKeys apply(PrimaryKeys existing, PrimaryKey neww)
        {
            if (existing == null)
            {
                existing = new PrimaryKeys();
                heapAllocations.add(PrimaryKeys.unsharedHeapSize());
            }

            boolean reset = seenPrimaryKeys != null && seenPrimaryKeys.add(existing);
            heapAllocations.add(reset ? existing.addAndResetFrequency(neww) : existing.addAndIncrementFrequency(neww));
            return existing;
        }
    }

    /**
     * Transformer that removes an occurrence of a primary key, or the key from the terms a new single value lacks.
     */
    static class PrimaryKeysRemover implements InMemoryTrie.UpsertTransformer<PrimaryKeys, PrimaryKey>
    {
        private final LongAdder heapAllocations;
        // the terms of the new single value, which keep the key, during an update (under the TrieMemoryIndex lock)
        private Set<PrimaryKeys> seenPrimaryKeys;

        PrimaryKeysRemover(LongAdder heapAllocations)
        {
            this.heapAllocations = heapAllocations;
        }

        @Override
        public PrimaryKeys apply(PrimaryKeys existing, PrimaryKey neww)
        {
            if (existing == null)
                return null;

            if (seenPrimaryKeys != null)
            {
                if (!seenPrimaryKeys.contains(existing))
                    heapAllocations.add(existing.remove(neww));
                return existing;
            }
            heapAllocations.add(existing.decrementFrequency(neww));
            return existing;
        }
    }

    /**
     * A sorting iterator over items that can either be singleton PrimaryKey or a SortedSetKeyRangeIterator.
     */
    static class SortingSingletonOrSetIterator extends BinaryHeap
    {
        public SortingSingletonOrSetIterator(Collection<Object> data)
        {
            super(data.toArray());
            heapify();
        }

        @Override
        protected boolean greaterThan(Object a, Object b)
        {
            if (a == null || b == null)
                return b != null;

            return peek(a).compareTo(peek(b)) > 0;
        }

        public PrimaryKey nextOrNull()
        {
            Object key = top();
            if (key == null)
                return null;
            PrimaryKey result = peek(key);
            assert result != null;
            replaceTop(advanceItem(key));
            return result;
        }

        public void skipTo(PrimaryKey target)
        {
            advanceTo(target);
        }

        /**
         * Advance the given keys object to the next key.
         * If the keys object contains a single key, null is returned.
         * If the keys object contains more than one key, the first key is dropped and the iterator to the
         * remaining keys is returned.
         */
        @Override
        protected @Nullable Object advanceItem(Object keys)
        {
            if (keys instanceof PrimaryKey)
                return null;

            SortedSetKeyRangeIterator iterator = (SortedSetKeyRangeIterator) keys;
            assert iterator.hasNext();
            iterator.next();
            return iterator.hasNext() ? iterator : null;
        }

        /**
         * Advance the given keys object to the first element that is greater than or equal to the target key.
         * This is only called when the given item is known to be before the target key.
         * If the keys object contains a single key, null is returned.
         * If the keys object contains more than one key, it is skipped to the given target and the iterator to the
         * remaining keys is returned.
         */
        @Override
        protected @Nullable Object advanceItemTo(Object keys, Object target)
        {
            if (keys instanceof PrimaryKey)
                return null;

            SortedSetKeyRangeIterator iterator = (SortedSetKeyRangeIterator) keys;
            iterator.skipTo((PrimaryKey) target);
            return iterator.hasNext() ? iterator : null;
        }

        /**
         * Resolve a keys object to either its singleton value or the current element in the iterator.
         */
        static PrimaryKey peek(Object keys)
        {
            if (keys instanceof PrimaryKey)
                return (PrimaryKey) keys;
            if (keys instanceof SortedSetKeyRangeIterator)
                return ((SortedSetKeyRangeIterator) keys).peek();

            throw new AssertionError("Unreachable");
        }
    }

    static class MergingKeyRangeIterator extends KeyRangeIterator
    {
        // A sorting iterator of items that can be either singletons or SortedSetKeyRangeIterator
        SortingSingletonOrSetIterator keySets;  // class invariant: each object placed in this queue contains at least one key

        MergingKeyRangeIterator(Collection<Object> keySets,
                                PrimaryKey minKey,
                                PrimaryKey maxKey,
                                long count)
        {
            super(minKey, maxKey, count);

            this.keySets = new SortingSingletonOrSetIterator(keySets);
        }

        static Builder builder(AbstractBounds<PartitionPosition> keyRange, PrimaryKey.Factory factory, int capacity, boolean mayHaveStaticKeys)
        {
            return new Builder(keyRange, factory, capacity, mayHaveStaticKeys);
        }

        @Override
        protected void performSkipTo(PrimaryKey nextKey)
        {
            keySets.skipTo(nextKey);
        }

        @Override
        protected PrimaryKey computeNext()
        {
            PrimaryKey result = keySets.nextOrNull();
            if (result == null)
                return endOfData();
            else
                return result;
        }

        @Override
        public void close() throws IOException
        {
        }

        static class Builder
        {
            final List<Object> keySets;

            private final PrimaryKey min;
            private final PrimaryKey max;
            private final PrimaryKey.Factory factory;
            private final boolean mayHaveStaticKeys;
            private long count;


            Builder(AbstractBounds<PartitionPosition> keyRange, PrimaryKey.Factory factory, int capacity, boolean mayHaveStaticKeys)
            {
                this.min = factory.createTokenOnly(keyRange.left.getToken());
                this.max = factory.createTokenOnly(keyRange.right.getToken());
                this.factory = factory;
                this.mayHaveStaticKeys = mayHaveStaticKeys;
                this.keySets = new ArrayList<>(capacity);
            }

            public void add(PrimaryKeys primaryKeys)
            {
                if (primaryKeys.isEmpty())
                    return;

                int size = primaryKeys.size();
                SortedSet<PrimaryKey> keys = primaryKeys.keys();
                if (size == 1)
                    keySets.add(keys.first());
                else
                    keySets.add(new SortedSetKeyRangeIterator(keys, min, max, size, mayHaveStaticKeys, factory));

                count += size;
            }

            public int size()
            {
                return keySets.size();
            }

            public boolean isEmpty()
            {
                return keySets.isEmpty();
            }

            public MergingKeyRangeIterator build()
            {
                return new MergingKeyRangeIterator(keySets, min, max, count);
            }
        }
    }

    static class SortedSetKeyRangeIterator extends KeyRangeIterator
    {
        // whether the set can hold keys of static rows, see mayHaveStaticKeys()
        private final boolean mayHaveStaticKeys;
        private final PrimaryKey.Factory keyFactory;
        private SortedSet<PrimaryKey> primaryKeySet;
        private Iterator<PrimaryKey> iterator;
        private PrimaryKey lastComputedKey;
        // the lower bound of primaryKeySet, set by the last skip
        private PrimaryKey skippedTo;

        SortedSetKeyRangeIterator(SortedSet<PrimaryKey> source, boolean mayHaveStaticKeys, PrimaryKey.Factory keyFactory)
        {
            this(source, source.first(), source.last(), source.size(), mayHaveStaticKeys, keyFactory);
        }

        private SortedSetKeyRangeIterator(SortedSet<PrimaryKey> source,
                                          PrimaryKey min,
                                          PrimaryKey max,
                                          long count,
                                          boolean mayHaveStaticKeys,
                                          PrimaryKey.Factory keyFactory)
        {
            super(min, max, count);
            this.primaryKeySet = source;
            this.mayHaveStaticKeys = mayHaveStaticKeys;
            this.keyFactory = keyFactory;
        }

        @Override
        protected PrimaryKey computeNext()
        {
            // Skip can be called multiple times in a row, so defer iterator creation until needed
            if (iterator == null)
                iterator = primaryKeySet.iterator();
            lastComputedKey = iterator.hasNext() ? iterator.next() : endOfData();
            return lastComputedKey;
        }

        @Override
        protected void performSkipTo(PrimaryKey nextKey)
        {
            // Avoid going backwards. This uses the order of the set, in which the STATIC key of a partition sorts
            // before its WIDE keys, so that after returning the STATIC key we can still skip within its partition.
            if (lastComputedKey != null && comparator().compare(nextKey, lastComputedKey) <= 0)
                return;

            if (mayHaveStaticKeys)
                nextKey = maybeSkipToStaticKey(nextKey);

            // A previous skip that wasn't followed by computeNext() may have gone further already, and tailSet()
            // throws for a key below the lower bound of a previous tailSet(). Equal keys are not necessarily the same
            // position: a token-only key compares equal to every key with the same token.
            if (skippedTo != null && comparator().compare(nextKey, skippedTo) < 0)
                return;

            skippedTo = nextKey;
            primaryKeySet = primaryKeySet.tailSet(nextKey);
            iterator = null;
        }

        /**
         * tailSet(nextKey) would skip past the STATIC key of the target partition, which matches the whole partition:
         * skip to it if the set holds it and we haven't returned it. Not when the set has no STATIC key for that
         * partition: an index on a partition key holds STATIC and WIDE keys.
         */
        private PrimaryKey maybeSkipToStaticKey(PrimaryKey nextKey)
        {
            // Check the token first: reading the clustering of the target can load it from disk. Note that contains()
            // is false for a key outside the bounds of a previous tailSet.
            if (nextKey.isTokenOnly() || !primaryKeySet.contains(keyFactory.createTokenOnly(nextKey.token())))
                return nextKey;

            if (nextKey.isStaticRow())
                return nextKey;

            PrimaryKey staticKey = nextKey.forStaticRow();
            if (!primaryKeySet.contains(staticKey))
                return nextKey;

            // don't go back to a STATIC key we have already returned
            if (lastComputedKey != null && comparator().compare(staticKey, lastComputedKey) <= 0)
                return nextKey;

            return staticKey;
        }

        private Comparator<? super PrimaryKey> comparator()
        {
            Comparator<? super PrimaryKey> comparator = primaryKeySet.comparator();
            return comparator == null ? Comparator.naturalOrder() : comparator;
        }

        @Override
        public void close() throws IOException
        {
        }
    }

    private KeyRangeIterator rangeMatch(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        return iterateSubtrie(getSubtrie(expression), expression, keyRange);
    }

    /**
     * The subtrie of the terms starting with the given raw term bytes, {@code [prefix, nextOf(prefix))}, computed on
     * the unterminated byte-comparable (the whole trie for an empty prefix). This is only valid for non-composite
     * literal types, for which encodeForTrie preserves the raw byte order of the terms.
     */
    private Trie<PrimaryKeys> prefixSubtrie(byte[] prefix)
    {
        if (prefix.length == 0)
            return data;
        byte[] upperBytes = AutomatonQueries.prefixUpperBound(prefix);
        ByteComparable upperBound = upperBytes == null ? null : asByteComparable(ByteBuffer.wrap(upperBytes));
        return data.slice(asByteComparable(ByteBuffer.wrap(prefix)), true, upperBound, false);
    }

    /**
     * Returns the primary keys of all terms accepted by the given byte-level automaton (the non-prefix LIKE matching
     * engine, see {@link AutomatonQueries}). The accepted terms all start with the automaton's common prefix, so only
     * the subtrie of the terms starting with it is scanned.
     */
    @VisibleForTesting
    public KeyRangeIterator automatonMatch(CompiledAutomaton automaton, AbstractBounds<PartitionPosition> keyRange)
    {
        return automatonMatch(automaton, prefixSubtrie(AutomatonQueries.commonPrefixBytes(automaton)), keyRange);
    }

    /**
     * Returns the primary keys of the terms of the given subtrie accepted by the automaton. This is only correct for
     * indexes over non-composite literal types, whose terms are stored in the trie as their raw unterminated bytes
     * (see {@code OnDiskFormat#encodeForTrie}), so a byte-level automaton can be run directly over them.
     */
    private KeyRangeIterator automatonMatch(CompiledAutomaton automaton, Trie<PrimaryKeys> subtrie, AbstractBounds<PartitionPosition> keyRange)
    {
        if (!TypeUtil.isLiteral(indexContext.getValidator()) || TypeUtil.isComposite(indexContext.getValidator()))
            throw new IllegalStateException("Automaton matching is only supported on non-composite literal indexes");

        var capacity = Math.max(MINIMUM_QUEUE_SIZE, lastQueueSize.get());
        var mergingIteratorBuilder = MergingKeyRangeIterator.builder(keyBounds, indexContext.keyFactory(), capacity, mayHaveStaticKeys());
        lastQueueSize.set(mergingIteratorBuilder.size());

        acceptedEntries(subtrie.entrySet().iterator(), automaton).forEachRemaining(entry -> mergingIteratorBuilder.add(entry.getValue()));

        return mergingIteratorBuilder.isEmpty()
               ? KeyRangeIterator.empty()
               : new FilteringKeyRangeIterator(mergingIteratorBuilder.build(), keyRange);
    }

    private KeyRangeIterator iterateSubtrie(Trie<PrimaryKeys> subtrie, Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        var capacity = Math.max(MINIMUM_QUEUE_SIZE, lastQueueSize.get());
        var mergingIteratorBuilder = MergingKeyRangeIterator.builder(keyBounds, indexContext.keyFactory(), capacity, mayHaveStaticKeys());
        lastQueueSize.set(mergingIteratorBuilder.size());

        if (!version.onOrAfter(Version.DB) && TypeUtil.isComposite(expression.validator))
            subtrie.entrySet().forEach(entry -> {
                // Before version DB, we encoded composite types using a non order-preserving function. In order to
                // perform a range query on a map, we use the bounds to get all entries for a given map key and then
                // only keep the map entries that satisfy the expression.
                assert entry.getKey().encodingVersion() == TypeUtil.BYTE_COMPARABLE_VERSION || version == Version.AA;
                byte[] key = ByteSourceInverse.readBytes(entry.getKey().getPreencodedBytes());
                if (expression.isSatisfiedBy(ByteBuffer.wrap(key)))
                    mergingIteratorBuilder.add(entry.getValue());
            });
        else
            subtrie.values().forEach(mergingIteratorBuilder::add);

        return mergingIteratorBuilder.isEmpty()
               ? KeyRangeIterator.empty()
               : new FilteringKeyRangeIterator(mergingIteratorBuilder.build(), keyRange);
    }

    @Override
    public long estimateMatchingRowsCount(Expression expression)
    {
        switch (expression.getOp())
        {
            case MATCH:
            case EQ:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
                return estimateNumRowsMatchingExact(expression);
            case NOT_EQ:
            case NOT_CONTAINS_KEY:
            case NOT_CONTAINS_VALUE:
                // The search returns all the rows when it can't subtract the matches of the negated expression
                if (TypeUtil.supportsRounding(expression.validator) || !IndexContext.canSubtractMatches(expression))
                    return Memtable.estimateRowCount(memtable);
                else
                    // need to clamp at 0, because row count is imprecise
                    return Math.max(0, Memtable.estimateRowCount(memtable) - estimateNumRowsMatchingExact(expression));
            case RANGE:
                return estimateNumRowsMatchingRange(expression);
            case PREFIX:
                return estimateNumRowsMatchingPrefix(expression);
            case AUTOMATON:
                return estimateNumRowsMatchingAutomaton(expression);
            default:
                throw new IllegalArgumentException("Unsupported expression: " + expression);
        }
    }


    private int estimateNumRowsMatchingExact(Expression expression)
    {
        final ByteComparable prefix = expression.lower == null ? ByteComparable.EMPTY : asByteComparable(expression.lower.value.encoded);
        final PrimaryKeys primaryKeys = data.get(prefix);
        return primaryKeys == null ? 0 : primaryKeys.size();
    }

    private long estimateNumRowsMatchingRange(Expression expression)
    {
        ByteComparable queryLower = expression.lower != null ? expression.getEncodedLowerBoundByteComparable(version) : null;
        ByteComparable queryUpper = expression.upper != null ? expression.getEncodedUpperBoundByteComparable(version) : null;
        boolean lowerExclusive = expression.lower != null && !expression.lower.inclusive;
        boolean upperExclusive = expression.upper != null && !expression.upper.inclusive;
        return estimateNumRowsInTermRange(queryLower, lowerExclusive, queryUpper, upperExclusive);
    }

    private long estimateNumRowsMatchingPrefix(Expression expression)
    {
        return estimateNumRowsMatchingPrefix(ByteBufferUtil.getArray(expression.lower.value.encoded));
    }

    /**
     * Estimates the number of rows whose term starts with the given raw term bytes, the rows of the term range
     * {@code [prefix, nextOf(prefix))}.
     */
    private long estimateNumRowsMatchingPrefix(byte[] prefix)
    {
        byte[] upperBytes = AutomatonQueries.prefixUpperBound(prefix);
        ByteComparable lower = prefix.length == 0 ? null : asByteComparable(ByteBuffer.wrap(prefix));
        ByteComparable upper = upperBytes == null ? null : asByteComparable(ByteBuffer.wrap(upperBytes));
        return estimateNumRowsInTermRange(lower, false, upper, true);
    }

    /**
     * Conservatively estimates the number of rows matching an automaton pattern expression: when the automaton
     * has a non-empty common byte prefix, all matching terms live in {@code [prefix, nextOf(prefix))} and the
     * estimate is that of the range, like for a prefix expression; without a common prefix the
     * automaton may match terms anywhere in the dictionary, so the whole index size is returned as a (very)
     * conservative upper bound.
     */
    private long estimateNumRowsMatchingAutomaton(Expression expression)
    {
        byte[] commonPrefix = expression.getAutomatonCommonPrefix();
        if (commonPrefix.length == 0)
            return indexedRows;
        return estimateNumRowsMatchingPrefix(commonPrefix);
    }

    /**
     * Estimates the number of rows of the terms in the given range (a null bound is unbounded).
     * <p>
     * The rows of the first {@link #MAX_COUNTED_TERMS} terms of the range are counted, up to {@link #MAX_COUNTED_ROWS}
     * rows, which gives the exact count when the range doesn't have more terms or rows. Otherwise, the estimate is the
     * greatest of that partial count and of the interpolation over the range
     * (see {@link #interpolateNumRowsInTermRange}): the linear interpolation in the byte space is very coarse for
     * narrow ranges, such as those of long prefixes or of terms that share their first bytes with all the other
     * terms.
     */
    private long estimateNumRowsInTermRange(@Nullable ByteComparable queryLower, boolean lowerExclusive,
                                            @Nullable ByteComparable queryUpper, boolean upperExclusive)
    {
        if (isEmptyRange(queryLower, !lowerExclusive, queryUpper, !upperExclusive))
            return 0;
        // A null lower bound includes the empty term, the root of the trie
        Trie<PrimaryKeys> subtrie = queryLower == null && queryUpper == null
                                    ? data
                                    : data.slice(queryLower,
                                                 !lowerExclusive,
                                                 queryUpper,
                                                 queryUpper != null && !upperExclusive);
        long countedRows = 0;
        int countedTerms = 0;
        boolean complete = true;
        for (PrimaryKeys primaryKeys : subtrie.values())
        {
            if (++countedTerms > MAX_COUNTED_TERMS || countedRows >= MAX_COUNTED_ROWS)
            {
                complete = false;
                break;
            }
            // PrimaryKeys#size() walks the whole skip list: count at most the remaining row budget
            Iterator<PrimaryKey> keys = primaryKeys.keys().iterator();
            while (keys.hasNext() && countedRows < MAX_COUNTED_ROWS)
            {
                keys.next();
                countedRows++;
            }
            if (keys.hasNext())
            {
                complete = false;
                break;
            }
        }
        if (complete)
            return countedRows;

        return Math.max(countedRows, interpolateNumRowsInTermRange(queryLower, lowerExclusive, queryUpper, upperExclusive));
    }

    /**
     * Estimates the number of rows of the terms in the given range by linear interpolation of the range over the
     * span of the indexed terms, {@code [minTerm, maxTerm]}.
     */
    private long interpolateNumRowsInTermRange(@Nullable ByteComparable queryLower, boolean lowerExclusive,
                                               @Nullable ByteComparable queryUpper, boolean upperExclusive)
    {
        if (minTerm == null || maxTerm == null)
            return 0;

        AbstractType<?> termType = indexContext.getValidator();
        ByteComparable minTermComparable = version.onDiskFormat().encodeForTrie(minTerm, termType);
        ByteComparable maxTermComparable = version.onDiskFormat().encodeForTrie(maxTerm, termType);
        // The min/max terms are tracked with the comparator of the column type, which for a reversed (DESC
        // clustering) column is the reverse of the byte order of the trie and of the query bounds: order them.
        BigDecimal minTermValue = toBigDecimal(minTermComparable);
        BigDecimal maxTermValue = toBigDecimal(maxTermComparable);
        BigDecimal indexLowerBound = minTermValue.min(maxTermValue);
        BigDecimal indexUpperBound = minTermValue.max(maxTermValue);

        BigDecimal queryLowerBound = queryLower != null ? toBigDecimal(queryLower) : indexLowerBound;
        // Round the upper bound up, so that a range narrower than the resolution of the conversion, such as that of
        // a long prefix, is not empty.
        BigDecimal queryUpperBound = queryUpper != null ? toBigDecimalRoundingUp(queryUpper) : indexUpperBound;

        if (queryLowerBound.compareTo(indexUpperBound) > 0 || queryUpperBound.compareTo(indexLowerBound) < 0)
            return 0;
        if (queryLowerBound.compareTo(indexUpperBound) == 0 && lowerExclusive)
            return 0;
        if (queryUpperBound.compareTo(indexLowerBound) == 0 && upperExclusive)
            return 0;
        if (queryLowerBound.compareTo(indexLowerBound) <= 0 && queryUpperBound.compareTo(indexUpperBound) >= 0)
            return indexedRows;

        // A contradictory range (lower bound sorting after the upper bound) cannot always be rejected at
        // prepare time because the bounds may be bind markers, and none of the early-outs above fires when both
        // bounds lie inside [minTerm, maxTerm]. Such a range provably matches nothing.
        if (queryLowerBound.compareTo(queryUpperBound) > 0)
            return 0;

        // Clamping into [indexLowerBound, indexUpperBound] is monotonic, so it keeps lower <= upper.
        queryUpperBound = queryUpperBound.min(indexUpperBound).max(indexLowerBound);
        queryLowerBound = queryLowerBound.max(indexLowerBound).min(indexUpperBound);
        assert queryLowerBound.compareTo(queryUpperBound) <= 0
            : "query lower bound (" + queryLowerBound + ") should be less than or equal to query upper bound (" + queryUpperBound + ')';

        BigDecimal indexRangeSize = indexUpperBound.subtract(indexLowerBound);
        BigDecimal queryRangeSize = queryUpperBound.subtract(queryLowerBound);
        double selectivity = indexRangeSize.signum() == 0
                             ? 1.0
                             : TermsDistribution.divide(queryRangeSize, indexRangeSize);
        assert selectivity >= 0.0 && selectivity <= 1.0 : "selectivity (" + selectivity + ") should be between 0.0 and 1.0";
        return Math.round(selectivity * indexedRows);
    }

    /**
     * Converts the term to a BigDecimal in a way that it keeps the sort order
     * (so terms comparing larger yield larger numbers).
     * @see TermsDistribution#toBigDecimal(ByteComparable, AbstractType, Version, ByteComparable.Version)
     */
    private BigDecimal toBigDecimal(ByteComparable term)
    {
        AbstractType<?> type = indexContext.getValidator();
        return TermsDistribution.toBigDecimal(term, type, version, TypeUtil.BYTE_COMPARABLE_VERSION);
    }

    /**
     * Like {@link #toBigDecimal}, but the result is not smaller than the term.
     * @see TermsDistribution#toBigDecimalRoundingUp(ByteComparable, AbstractType, Version, ByteComparable.Version)
     */
    private BigDecimal toBigDecimalRoundingUp(ByteComparable term)
    {
        AbstractType<?> type = indexContext.getValidator();
        return TermsDistribution.toBigDecimalRoundingUp(term, type, version, TypeUtil.BYTE_COMPARABLE_VERSION);
    }

    private Trie<PrimaryKeys> getSubtrie(@Nullable Expression expression)
    {
        if (expression == null)
            return data;

        // The bounds of pattern expressions are the pattern itself, not a term range: slice by the range of terms
        // they can match instead. For automaton patterns this is the range of the terms starting with the
        // automaton's common prefix, a superset of the matching terms (filtered by the callers that need to).
        if (expression.getOp() == Expression.Op.PREFIX)
            return prefixSubtrie(ByteBufferUtil.getArray(expression.lower.value.encoded));
        if (expression.getOp() == Expression.Op.AUTOMATON)
            return prefixSubtrie(expression.getAutomatonCommonPrefix());

        ByteComparable lowerBound, upperBound;
        boolean lowerInclusive, upperInclusive;
        if (expression.lower != null)
        {
            lowerBound = expression.getEncodedLowerBoundByteComparable(version);
            lowerInclusive = expression.lower.inclusive;
        }
        else
        {
            // Unbounded: include the empty term (the root of the trie), which literal indexes index
            lowerBound = null;
            lowerInclusive = true;
        }

        if (expression.upper != null)
        {
            upperBound = expression.getEncodedUpperBoundByteComparable(version);
            upperInclusive = expression.upper.inclusive;
        }
        else
        {
            upperBound = null;
            upperInclusive = false;
        }

        if (isEmptyRange(lowerBound, lowerInclusive, upperBound, upperInclusive))
            return Trie.empty(TypeUtil.byteComparableVersionForTermsData(indexContext.version()));

        return data.slice(lowerBound, lowerInclusive, upperBound, upperInclusive);
    }

    /**
     * Whether the range of terms is empty (lower bound after upper, or equal with one exclusive), as contradictory
     * restrictions such as {@code v >= 5 AND v < 3} give: slicing the trie with it fails. A null lower bound is the
     * empty term.
     */
    private boolean isEmptyRange(@Nullable ByteComparable lowerBound, boolean lowerInclusive,
                                 @Nullable ByteComparable upperBound, boolean upperInclusive)
    {
        if (upperBound == null)
            return false;
        if (lowerBound == null)
        {
            lowerBound = ByteComparable.EMPTY;
            lowerInclusive = true;
        }
        // Compare the bounds as the trie does. The empty value of a non-literal type has no byte-comparable
        // representation (a null source), which sorts before all the others.
        int cmp = ByteComparable.compare(lowerBound, upperBound, TypeUtil.byteComparableVersionForTermsData(indexContext.version()));
        return cmp > 0 || (cmp == 0 && !(lowerInclusive && upperInclusive));
    }

    @Override
    public ByteBuffer getMinTerm()
    {
        return minTerm;
    }

    @Override
    public ByteBuffer getMaxTerm()
    {
        return maxTerm;
    }

    private void setMinMaxTerm(ByteBuffer term)
    {
        assert term != null;

        // Note that an update to a term could make these inaccurate, but they err in the correct direction.
        // An alternative solution could use the trie to find the min/max term, but the trie has ByteComparable
        // objects, not the ByteBuffer, and we would need to implement a custom decoder to undo the encodeForTrie
        // mapping.
        minTerm = TypeUtil.min(term, minTerm, indexContext.getValidator(), version);
        maxTerm = TypeUtil.max(term, maxTerm, indexContext.getValidator(), version);
    }

    /**
     * Iterator that provides ordered access to all indexed terms and their associated primary keys
     * in the TrieMemoryIndex. For each term in the index, yields PrimaryKeyWithSortKey objects that
     * combine a primary key with its associated term.
     * <p>
     * A more verbose name could be KeysMatchingTermsByTermIterator.
     */
    private class AllTermsIterator extends AbstractIterator<PrimaryKeyWithSortKey>
    {
        private final Iterator<Map.Entry<ByteComparable.Preencoded, PrimaryKeys>> iterator;
        private Iterator<PrimaryKey> primaryKeysIterator = CloseableIterator.emptyIterator();
        private ByteComparable.Preencoded byteComparableTerm = null;

        public AllTermsIterator(Iterator<Map.Entry<ByteComparable.Preencoded, PrimaryKeys>> iterator)
        {
            this.iterator = iterator;
        }

        @Override
        protected PrimaryKeyWithSortKey computeNext()
        {
            assert memtable != null;
            if (primaryKeysIterator.hasNext())
                return new PrimaryKeyWithByteComparable(indexContext, memtable, primaryKeysIterator.next(), byteComparableTerm);

            while (iterator.hasNext())
            {
                var entry = iterator.next();
                primaryKeysIterator = entry.getValue().keys().iterator();
                if (!primaryKeysIterator.hasNext())
                    continue;
                byteComparableTerm = entry.getKey();
                return new PrimaryKeyWithByteComparable(indexContext, memtable, primaryKeysIterator.next(), byteComparableTerm);
            }
            return endOfData();
        }
    }
}
