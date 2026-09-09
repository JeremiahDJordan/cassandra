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
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;

/**
 * Apples-to-apples microbenchmark of BufferPool owner recovery: times {@code Chunk.getParentChunk(buffer)} over a
 * realistic mixed population of pooled buffers. The IDENTICAL source is kept on both the marker-map branch and the
 * address-metadata branch, so the printed ns/op are directly comparable across branches and across JDK 11 / JDK 25.
 * JMH-free; prints one OWNERBENCH line to stdout. Not an assertion test.
 * <p>
 * Run: ant testsome -Dtest.name=org.apache.cassandra.utils.memory.OwnerRecoveryBench
 */
public class OwnerRecoveryBenchTest
{
    @Test
    public void bench() throws Exception
    {
        final long poolBytes = 256L << 20;                         // 256 MiB -> 32 macro chunks (8 MiB each)
        BufferPool pool = new BufferPool("ownerbench", poolBytes, true);
        List<ByteBuffer> buffers = new ArrayList<>();

        final int normalSize = BufferPool.NORMAL_CHUNK_SIZE / 2;   // 64 KiB -> normal chunk
        final int tinySize   = BufferPool.TINY_ALLOCATION_UNIT;    // smallest unit -> tiny chunk

        // ~4000 buffers, alternating normal/tiny, spread across many macro chunks so the lookup is not trivially cached.
        for (int i = 0; i < 4000; i++)
            buffers.add(pool.get((i & 1) == 0 ? normalSize : tinySize, BufferType.OFF_HEAP));

        // Sanity: every pooled buffer must resolve to a non-null owning chunk on this branch/JDK.
        int resolved = 0;
        for (ByteBuffer b : buffers)
            if (BufferPool.Chunk.getParentChunk(b) != null)
                resolved++;
        if (resolved != buffers.size())
            throw new AssertionError("getParentChunk failed to resolve " + (buffers.size() - resolved) + "/" + buffers.size());

        // Deterministic shuffled access order (fixed seed) to defeat trivial branch/stride prediction.
        final int n = buffers.size();
        final int LEN = 1 << 16;
        final int MASK = LEN - 1;
        int[] order = new int[LEN];
        Random rnd = new Random(42);
        for (int i = 0; i < LEN; i++)
            order[i] = rnd.nextInt(n);

        long sink = 0;

        // Warmup: ~25M calls to reach steady-state JIT.
        for (int w = 0; w < 5; w++)
            for (int i = 0; i < 5_000_000; i++)
                sink += BufferPool.Chunk.getParentChunk(buffers.get(order[i & MASK])).baseAddress;

        // Measure: report best and median ns/op over several rounds (cheap baseAddress sink prevents dead-code removal).
        final long iters = 50_000_000L;
        final int rounds = 5;
        double[] all = new double[rounds];
        for (int r = 0; r < rounds; r++)
        {
            long t0 = System.nanoTime();
            for (long i = 0; i < iters; i++)
                sink += BufferPool.Chunk.getParentChunk(buffers.get(order[(int) (i & MASK)])).baseAddress;
            all[r] = (double) (System.nanoTime() - t0) / iters;
        }
        Arrays.sort(all);
        double best = all[0];
        double median = all[rounds / 2];

        System.out.println(String.format("OWNERBENCH java=%s buffers=%d iters=%d rounds=%d best=%.3f_ns/op median=%.3f_ns/op sink=%d",
                                          System.getProperty("java.version"), n, iters, rounds, best, median, sink));

        for (ByteBuffer b : buffers)
            pool.put(b);
    }
}
