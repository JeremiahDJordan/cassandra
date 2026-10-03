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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.github.jbellis.jvector.graph.NodeQueue;
import io.github.jbellis.jvector.graph.NodesUnsorted;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.util.BoundedLongHeap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Exercises the reranking behavior that makes a shared approximate floor unsafe for a sorted merge. */
public class RerankFloorTest
{
    private static NodeQueue candidates()
    {
        NodeQueue candidates = queue(2);
        candidates.push(0, 0.93f); // Better exact neighbor, below the earlier segment's approximate floor.
        candidates.push(1, 0.95f); // Worse exact neighbor, above the floor.
        return candidates;
    }

    private static NodeQueue queue(int size)
    {
        return new NodeQueue(new BoundedLongHeap(size), NodeQueue.Order.MIN_HEAP);
    }

    private static final ScoreFunction.ExactScoreFunction EXACT = node -> node == 0 ? 0.96f : 0.85f;

    @Test
    public void sharedApproximateFloorHidesTheBestExactCandidate()
    {
        // An earlier segment's top-one result establishes a floor of 0.94 despite having exact score 0.92.
        // Both later candidates have already been found: this failure does not require a navigation miss.
        NodeQueue floored = queue(1);
        AtomicInteger calls = new AtomicInteger();
        candidates().rerank(1, node -> { calls.incrementAndGet(); return EXACT.similarityTo(node); },
                            0.94f, floored, new NodesUnsorted(2));
        assertEquals(1, calls.get());
        assertEquals(1, floored.topNode());
        assertEquals(0.85f, floored.topScore(), 0.0f);

        NodeQueue unrestricted = queue(1);
        candidates().rerank(1, EXACT, 0.0f, unrestricted, new NodesUnsorted(2));
        assertEquals(0, unrestricted.topNode());
        assertEquals(0.96f, unrestricted.topScore(), 0.0f);
    }

    @Test
    public void resumingWithoutTheFloorCanViolateTheEmittedScoreOrder()
    {
        NodeQueue initial = queue(1);
        NodesUnsorted unused = new NodesUnsorted(2);
        candidates().rerank(1, EXACT, 0.94f, initial, unused);
        assertEquals(1, unused.size());

        // GraphSearcher.resume discards the floor. Its retained candidate is better than the
        // already emitted head; a merge that stops at the global limit may never consume it.
        NodeQueue remaining = queue(2);
        unused.foreach(remaining::push);
        NodeQueue resumed = queue(1);
        remaining.rerank(1, EXACT, 0.0f, resumed, new NodesUnsorted(2));
        assertEquals(0, resumed.topNode());
        assertTrue(resumed.topScore() > initial.topScore());
    }
}
