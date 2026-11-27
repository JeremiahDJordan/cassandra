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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ANNOptions;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithScore;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_SHARD_COUNT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VectorMemtableIndexTest extends SAITester
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final Injections.Counter indexSearchCounter = Injections.newCounter("IndexSearchCounter")
                                                                           .add(InvokePointBuilder.newInvokePoint()
                                                                                                  .onClass(TrieMemoryIndex.class)
                                                                                                  .onMethod("search"))
                                                                           .build();

    private ColumnFamilyStore cfs;
    private IndexContext indexContext;
    private VectorMemtableIndex memtableIndex;
    private IPartitioner partitioner;
    private Map<DecoratedKey, Integer> keyMap;
    private Map<Integer, ByteBuffer> rowMap;
    private int dimensionCount;

    @BeforeClass
    public static void setShardCount()
    {
        MEMTABLE_SHARD_COUNT.setInt(8);
    }

    @Before
    public void setup() throws Throwable
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 10), FBUtilities.getBroadcastAddressAndPort());

        TableMetadata tableMetadata = TableMetadata.builder("ks", "tb")
                                                   .addPartitionKeyColumn("pk", Int32Type.instance)
                                                   .addRegularColumn("val", Int32Type.instance)
                                                   .build();
        cfs = MockSchema.newCFS(tableMetadata);
        partitioner = cfs.getPartitioner();
        dimensionCount = getRandom().nextIntBetween(2, 2048);
        indexContext = SAITester.createIndexContext("index", VectorType.getInstance(FloatType.instance, dimensionCount), cfs);
        indexSearchCounter.reset();
        keyMap = new ConcurrentSkipListMap<>();
        rowMap = new ConcurrentHashMap<>();

        Injections.inject(indexSearchCounter);
    }

    @Test
    public void randomQueryTest()
    {
        var memtable = cfs.getCurrentMemtable();
        memtableIndex = new VectorMemtableIndex(indexContext, memtable);

        // insert rows
        int rowCount = ThreadLocalRandom.current().nextInt(1000, 5000);
        IntStream.range(0, rowCount).parallel().forEach(i ->
        {
            var value = randomVectorSerialized();
            while (true)
            {
                var pk = ThreadLocalRandom.current().nextInt(0, 10000);
                if (rowMap.putIfAbsent(pk, value) == null)
                {
                    addRow(pk, value);
                    break;
                }
            }
        });
        memtableIndex.cleanup();
        // master list of (random) keys inserted
        List<DecoratedKey> keys = new ArrayList<>(keyMap.keySet());

        // execute queries both with and without brute force enabled
        validate(keys, 1.0, 1.0);
        // In scenarios where we have 5 or fewer vectors, brute force is always going to be used,
        // so we do that here.
        VectorTester.setMaxBruteForceRows(5);
        // For these tests, we will push some queries that would typically go the brute force path to the graph path.
        // This comes at the cost of some recall, which is why we only assert 50% recall on the individual queries
        // and 90% recall on the overall test.
        validate(keys, 0.5, 0.9);
    }

    private void validate(List<DecoratedKey> keys, double expectedIndividualQueryRecall, double expectedRecall)
    {
        var actualVectorsReturned = new AtomicInteger();
        var expectedVectorsReturned = new AtomicInteger();
        IntStream.range(0, 1_000).parallel().forEach(i ->
        {
            var orderer = randomVectorOrderer();
            AbstractBounds<PartitionPosition> keyRange = generateRandomBounds(keys);
            // compute keys in range of the bounds
            Set<Integer> keysInRange = keys.stream().filter(keyRange::contains)
                                           .map(k -> Int32Type.instance.compose(k.getKey()))
                                           .collect(Collectors.toSet());

            Set<Integer> foundKeys = new HashSet<>();
            int limit = getRandom().nextIntBetween(1, 100);

            long expectedResults = Math.min(limit, keysInRange.size());

            // execute the random ANN expression, and check that we get back as many keys as we asked for
            var iterators = memtableIndex.orderBy(new QueryContext(), orderer, null, keyRange, limit);
            assertEquals(1, iterators.size());
            try (var iterator = iterators.get(0))
            {
                PrimaryKeyWithScore lastKey = null;
                while (iterator.hasNext() && foundKeys.size() < expectedResults)
                {
                    PrimaryKeyWithScore primaryKeyWithScore = (PrimaryKeyWithScore) iterator.next();
                    if (lastKey != null)
                        // This assertion only holds true as long as we query at most the expectedNumResults.
                        // Once we query deeper, we might get a key with a higher score than the last key.
                        // This is a direct consequence of the approximate part of ANN.
                        // Note that PrimaryKeyWithScore is flipped to descending order, so we use >= here.
                        assertTrue("Returned keys are not ordered by score", primaryKeyWithScore.compareTo(lastKey) >= 0);
                    lastKey = primaryKeyWithScore;
                    int key = Int32Type.instance.compose(primaryKeyWithScore.partitionKey().getKey());
                    assertFalse(foundKeys.contains(key));

                    assertTrue(keyRange.contains(primaryKeyWithScore.partitionKey()));
                    assertTrue(rowMap.containsKey(key));
                    foundKeys.add(key);
                }
                // Note that we weight each result evenly instead of each query evenly.
                actualVectorsReturned.addAndGet(foundKeys.size());
                expectedVectorsReturned.addAndGet((int) expectedResults);
                if (foundKeys.size() < expectedResults)
                    assertTrue("Expected at least " + expectedResults + " results but got " + foundKeys.size(),
                               foundKeys.size() >= expectedResults * expectedIndividualQueryRecall);
            }
        });
        assertTrue("Expected at least " + expectedVectorsReturned + " results but got " + actualVectorsReturned,
                   actualVectorsReturned.get() >= expectedVectorsReturned.get() * expectedRecall);
    }

    /**
     * The cells of concurrent writes of a row can reach the index in any order, and more than once. The row must be
     * indexed under the vector of the cell that wins their reconciliation only.
     */
    @Test
    public void testUpdateIndexesVectorOfReconciledCell()
    {
        memtableIndex = new VectorMemtableIndex(indexContext, cfs.getCurrentMemtable());
        ColumnMetadata column = indexContext.getDefinition();
        long nowInSec = FBUtilities.nowInSeconds();
        ByteBuffer older = randomVectorSerialized();
        ByteBuffer newer = randomVectorSerialized();
        ByteBuffer other = randomVectorSerialized();
        update(0, BufferCell.live(column, 1, randomVectorSerialized()));

        // the newest cell first, then older ones, then the newest one again
        update(1, BufferCell.live(column, 20, newer));
        update(1, BufferCell.live(column, 10, older));
        update(1, BufferCell.tombstone(column, 15, nowInSec));
        update(1, BufferCell.live(column, 20, newer));
        assertIndexedUnder(1, newer);

        // a tombstone wins over a live cell of the same timestamp
        update(1, BufferCell.tombstone(column, 20, nowInSec));
        assertIndexedUnder(1, null);
        update(1, BufferCell.live(column, 20, other));
        assertIndexedUnder(1, null);
        update(1, BufferCell.live(column, 21, other));
        assertIndexedUnder(1, other);

        // of two live cells of the same timestamp, the one with the greater value wins, whatever the order
        boolean newerIsGreater = ByteBufferUtil.compareUnsigned(newer, older) > 0;
        ByteBuffer greater = newerIsGreater ? newer : older;
        ByteBuffer lesser = newerIsGreater ? older : newer;
        update(2, BufferCell.live(column, 30, greater));
        update(2, BufferCell.live(column, 30, lesser));
        assertIndexedUnder(2, greater);
        update(3, BufferCell.live(column, 30, lesser));
        update(3, BufferCell.live(column, 30, greater));
        assertIndexedUnder(3, greater);

        // an expired cell removes the vector
        update(3, BufferCell.expiring(column, 40, 1, nowInSec - 10, other));
        assertIndexedUnder(3, null);
    }

    private void update(int pk, Cell<?> cell)
    {
        memtableIndex.update(makeKey(cfs.metadata(), pk), Clustering.EMPTY, cell, cfs.getCurrentMemtable(), new OpOrder().start());
    }

    /**
     * Checks the vector the row is indexed under, with searches of the graph and brute force searches, of the whole
     * ring and of the row's key only.
     */
    private void assertIndexedUnder(int pk, ByteBuffer vector)
    {
        DecoratedKey key = makeKey(cfs.metadata(), pk);
        Orderer orderer = randomVectorOrderer();
        VectorSimilarityFunction similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        List<Float> expected = vector == null ? List.of()
                                              : List.of(similarityFunction.compare(orderer.getVectorTerm(), vts.createFloatVector(((VectorType<?>) indexContext.getValidator()).composeAsFloat(vector))));

        AbstractBounds<PartitionPosition> fullRing = new Range<>(partitioner.getMinimumToken().minKeyBound(), partitioner.getMinimumToken().minKeyBound());
        AbstractBounds<PartitionPosition> keyOnly = new Bounds<>(key.getToken().minKeyBound(), key.getToken().maxKeyBound());
        int maxBruteForceRows = VectorMemtableIndex.GLOBAL_BRUTE_FORCE_ROWS;
        try
        {
            for (int bruteForceRows : new int[]{ 0, Integer.MAX_VALUE })
            {
                VectorMemtableIndex.GLOBAL_BRUTE_FORCE_ROWS = bruteForceRows;
                for (AbstractBounds<PartitionPosition> keyRange : List.of(fullRing, keyOnly))
                {
                    List<Float> scores = new ArrayList<>();
                    var iterators = memtableIndex.orderBy(new QueryContext(), orderer, null, keyRange, 10);
                    assertEquals(1, iterators.size());
                    try (var iterator = iterators.get(0))
                    {
                        while (iterator.hasNext())
                        {
                            PrimaryKeyWithScore result = (PrimaryKeyWithScore) iterator.next();
                            if (result.partitionKey().equals(key))
                                scores.add(result.indexScore);
                        }
                    }
                    assertEquals("Scores of row " + pk + " in " + keyRange + " with brute force rows " + bruteForceRows, expected, scores);
                }
            }
        }
        finally
        {
            VectorMemtableIndex.GLOBAL_BRUTE_FORCE_ROWS = maxBruteForceRows;
        }
    }

    @Test
    public void preFlushWithRowIdClaimedByTwoVectorsTest()
    {
        // A key in the postings of two vectors: both vectors claim the row id, and only the vector of the row keeps it.
        CassandraOnHeapGraph<Integer> graph = new CassandraOnHeapGraph<>(indexContext, false, null);
        ByteBuffer stale = randomVectorSerialized();
        ByteBuffer current = randomVectorSerialized();
        ByteBuffer other = randomVectorSerialized();
        graph.add(stale, 0);
        graph.add(stale, 1);
        graph.add(current, 0);
        graph.add(other, 2);

        assertTrue(graph.preFlush(key -> key, key -> current));
        assertEquals(List.of(1), graph.rowIdsOf(stale));
        assertEquals(List.of(0), graph.rowIdsOf(current));
        assertEquals(List.of(2), graph.rowIdsOf(other));
    }

    /**
     * The flush does a graph insert that its write left undone, and fails if that insert fails, leaving the vector to
     * the searches that score pending vectors.
     */
    @Test
    public void preFlushDoesGraphInsertsLeftToItTest() throws Throwable
    {
        CassandraOnHeapGraph<Integer> graph = new CassandraOnHeapGraph<>(indexContext, false, null);
        graph.add(randomVectorSerialized(), 0, new CassandraOnHeapGraph.PendingInserts());
        assertEquals(0, graph.graphSize());
        assertTrue(graph.preFlush(key -> key));
        assertEquals(1, graph.graphSize());

        Injection failure = Injections.newCustom("graph_insert_fails_at_flush")
                                      .add(InvokePointBuilder.newInvokePoint()
                                                             .onClass(CassandraOnHeapGraph.class)
                                                             .onMethod("insertIntoGraph"))
                                      .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, "\"Injected graph insert failure\""))
                                      .build();
        Injections.inject(failure);
        CassandraOnHeapGraph<Integer> failing = new CassandraOnHeapGraph<>(indexContext, false, null);
        ByteBuffer vector = randomVectorSerialized();
        failing.add(vector, 0, new CassandraOnHeapGraph.PendingInserts());
        assertThatThrownBy(() -> failing.preFlush(key -> key)).hasMessageContaining("Injected graph insert failure");

        VectorFloat<?> query = vts.createFloatVector(((VectorType.VectorSerializer) indexContext.getValidator().getSerializer()).deserializeFloatArray(vector));
        List<Integer> nodes = new ArrayList<>();
        try (CloseableIterator<SearchResult.NodeScore> scores = failing.search(new QueryContext(), query, 1, 0, 0, false, Bits.ALL))
        {
            scores.forEachRemaining(score -> nodes.add(score.node));
        }
        assertEquals(List.of(0), nodes);
    }

    /**
     * Two racing first writes of a key on a skip-list memtable, the callback of the newer one first: the key stays
     * under the newer vector.
     */
    @Test
    public void racingFirstWritesOnSkipListMemtableTest()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val vector<float, 2>) WITH memtable = 'skiplist'");
        VectorType<Float> type = VectorType.getInstance(FloatType.instance, 2);
        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        VectorMemtableIndex index = new VectorMemtableIndex(SAITester.createIndexContext("index", type, store), store.getCurrentMemtable());
        DecoratedKey key = store.getPartitioner().decorateKey(Int32Type.instance.decompose(1));
        ByteBuffer v1 = type.decompose(List.of(1.0f, 1.0f));
        ByteBuffer v2 = type.decompose(List.of(2.0f, 2.0f));

        index.index(key, Clustering.EMPTY, v2.duplicate(), store.getCurrentMemtable(), new OpOrder().start());
        index.index(key, Clustering.EMPTY, v1.duplicate(), store.getCurrentMemtable(), new OpOrder().start());
        index.update(key, Clustering.EMPTY, (ByteBuffer) null, (ByteBuffer) null, store.getCurrentMemtable(), new OpOrder().start());

        // the key is under both vectors; the flush keeps it under the vector of its row
        assertEquals(1, index.getGraph().keysOf(v2).size());
        assertEquals(1, index.getGraph().keysOf(v1).size());
    }

    @Test
    public void indexIteratorTest()
    {
        // VSTODO
    }

    private Orderer randomVectorOrderer()
    {
        return new Orderer(indexContext, Operator.ANN, randomVectorSerialized(), ANNOptions.NONE);
    }

    private ByteBuffer randomVectorSerialized() {
        return randomVectorSerialized(dimensionCount);
    }

    private AbstractBounds<PartitionPosition> generateRandomBounds(List<DecoratedKey> keys)
    {
        PartitionPosition leftBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
                                                                : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().minKeyBound();

        PartitionPosition rightBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
                                                                 : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().maxKeyBound();

        AbstractBounds<PartitionPosition> keyRange;

        if (leftBound.isMinimum() && rightBound.isMinimum())
            keyRange = new Range<>(leftBound, rightBound);
        else
        {
            if (AbstractBounds.strictlyWrapsAround(leftBound, rightBound))
            {
                PartitionPosition temp = leftBound;
                leftBound = rightBound;
                rightBound = temp;
            }
            if (getRandom().nextBoolean())
                keyRange = new Bounds<>(leftBound, rightBound);
            else if (getRandom().nextBoolean())
                keyRange = new ExcludingBounds<>(leftBound, rightBound);
            else
                keyRange = new IncludingExcludingBounds<>(leftBound, rightBound);
        }
        return keyRange;
    }

    private void addRow(int pk, ByteBuffer value)
    {
        DecoratedKey key = makeKey(cfs.metadata(), pk);
        memtableIndex.index(key,
                            Clustering.EMPTY,
                            value,
                            cfs.getCurrentMemtable(),
                            new OpOrder().start());
        keyMap.put(key, pk);
    }

    private DecoratedKey makeKey(TableMetadata table, Integer partitionKey)
    {
        ByteBuffer key = table.partitionKeyType.fromString(partitionKey.toString());
        return table.partitioner.decorateKey(key);
    }
}
