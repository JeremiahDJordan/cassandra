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

package org.apache.cassandra.index.sai.disk.v1.vector;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.concurrent.Semaphore;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OnHeapGraphTest
{
    private static final VectorType<Float> TYPE = VectorType.getInstance(FloatType.instance, 2);

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * A writer interrupted while it waits for a graph insert permit must leave the graph unchanged. A vector that is
     * registered but never linked into the graph is not linked by a retry of the same row, so searches miss it and the
     * flush-time check that every ordinal is in the graph fails.
     */
    @Test
    public void testInterruptWhileWaitingForGraphInsertPermit() throws Exception
    {
        OnHeapGraph<Integer> graph = newGraphBoundingItsOwnInserts();
        Semaphore permits = graphInsertPermits(graph);
        int drained = permits.drain();
        ByteBuffer term = TYPE.decompose(Arrays.asList(1.0f, 0.5f));

        Thread.currentThread().interrupt();
        try
        {
            graph.add(term, 1, OnHeapGraph.InvalidVectorBehavior.FAIL);
            fail("Expected the interrupted wait for a graph insert permit to throw");
        }
        catch (UncheckedInterruptedException expected)
        {
            // the insert is abandoned; the graph must not have registered or cached the vector
        }
        finally
        {
            Thread.interrupted();
        }
        assertEquals(0, graph.size());
        assertTrue(graph.isEmpty());
        assertNull(graph.vectorForKey(1));
        assertEquals("a permit that was never acquired must not be released", 0, permits.permits());

        permits.release(drained);
        graph.add(term, 1, OnHeapGraph.InvalidVectorBehavior.FAIL);
        assertEquals(1, graph.size());
        assertEquals(drained, permits.permits());
        // the retried vector is in the jvector graph under the first ordinal, which maps back to its key
        assertEquals(Collections.singletonList(0), searchNodes(graph, new float[]{ 1.0f, 0.5f }));
        assertEquals(Collections.singletonList(1), graph.keysFromOrdinal(0));
    }

    /**
     * A writer that waits for a graph insert permit and then loses the race to add the same new vector must return its
     * permit; leaked permits would eventually leave every writer of a new vector waiting forever.
     */
    @Test
    public void testWriterLosingRaceToAddVectorReturnsGraphInsertPermit() throws Exception
    {
        OnHeapGraph<Integer> graph = newGraphBoundingItsOwnInserts();
        Semaphore permits = graphInsertPermits(graph);
        int drained = permits.drain();
        ByteBuffer term = TYPE.decompose(Arrays.asList(1.0f, 0.5f));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try
        {
            // with no permits, both writers find the vector absent and then wait; once permits return, one adds the
            // vector and the other loses the race
            Future<?> first = executor.submit(() -> graph.add(term, 1, OnHeapGraph.InvalidVectorBehavior.FAIL));
            Future<?> second = executor.submit(() -> graph.add(term, 2, OnHeapGraph.InvalidVectorBehavior.FAIL));
            Util.spinAssertEquals(2, ((Semaphore.Standard) permits)::waiting, 10);
            permits.release(drained);
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
        }
        finally
        {
            executor.shutdownNow();
        }
        assertEquals(1, graph.size());
        assertEquals("the writer that lost the race must return its permit", drained, permits.permits());
        assertEquals(new HashSet<>(Arrays.asList(1, 2)), new HashSet<>(graph.keysFromOrdinal(0)));
    }

    /**
     * A memtable that does not limit concurrent writes makes the graph bound its own inserts with a semaphore.
     */
    private static OnHeapGraph<Integer> newGraphBoundingItsOwnInserts()
    {
        IndexWriterConfig config = new IndexWriterConfig(IndexWriterConfig.DEFAULT_MAXIMUM_NODE_CONNECTIONS,
                                                         IndexWriterConfig.DEFAULT_CONSTRUCTION_BEAM_WIDTH,
                                                         VectorSimilarityFunction.EUCLIDEAN,
                                                         OptimizeFor.LATENCY);
        Memtable memtable = Mockito.mock(Memtable.class);
        Mockito.when(memtable.limitsConcurrentWritesTo(Mockito.anyInt())).thenReturn(false);
        return new OnHeapGraph<>(TYPE, config, memtable);
    }

    private static Semaphore graphInsertPermits(OnHeapGraph<?> graph) throws ReflectiveOperationException
    {
        Field field = OnHeapGraph.class.getDeclaredField("graphInsertPermits");
        field.setAccessible(true);
        return (Semaphore) field.get(graph);
    }

    private static List<Integer> searchNodes(OnHeapGraph<Integer> graph, float[] query)
    {
        Bits acceptAll = new Bits()
        {
            public boolean get(int index)
            {
                return true;
            }

            public int length()
            {
                return Integer.MAX_VALUE;
            }
        };
        List<Integer> nodes = new ArrayList<>();
        try (CloseableIterator<SearchResult.NodeScore> results = graph.search(query, 10, acceptAll))
        {
            while (results.hasNext())
                nodes.add(results.next().node);
        }
        return nodes;
    }
}
