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
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;

/**
 * JMH-free micro-measurement of the new address-based {@link BufferPool.Chunk#getParentChunk(ByteBuffer)} versus a
 * stand-in for the old per-chunk identity map lookup (a {@code weakKeys()} identity map keyed on the buffer). Not an
 * assertion test; prints ns/op to stdout. Run with: ant testsome -Dtest.name=...GetParentChunkMicroBench
 */
public class GetParentChunkMicroBenchTest
{
    @Test
    public void bench() throws Exception
    {
        // 64 MiB pool -> 8 macros; allocate a spread of NORMAL and TINY buffers across all of them.
        BufferPool pool = new BufferPool("microbench", 64L << 20, true);
        List<ByteBuffer> buffers = new ArrayList<>();

        int normalSize = BufferPool.NORMAL_CHUNK_SIZE / 2;     // 64 KiB
        int tinySize = BufferPool.TINY_ALLOCATION_UNIT;        // 32 B -> tiny pool

        // Fill several macros: each macro = 64 normal chunks; mix normal + tiny.
        for (int i = 0; i < 2000; i++)
            buffers.add(pool.get((i & 1) == 0 ? normalSize : tinySize, BufferType.OFF_HEAP));

        // Build the comparison identity map (what the old scheme paid: a weakKeys identity map op per lookup).
        ConcurrentMap<ByteBuffer, BufferPool.Chunk> identityMap = new MapMaker().weakKeys().makeMap();
        for (ByteBuffer b : buffers)
        {
            BufferPool.Chunk c = BufferPool.Chunk.getParentChunk(b);
            if (c != null)
                identityMap.put(b, c);
        }

        int n = buffers.size();
        long iters = 20_000_000L;

        // Warmup
        long sink = 0;
        for (int w = 0; w < 5; w++)
        {
            for (long i = 0; i < 2_000_000L; i++)
            {
                ByteBuffer b = buffers.get((int)(i % n));
                sink += System.identityHashCode(BufferPool.Chunk.getParentChunk(b));
                sink += System.identityHashCode(identityMap.get(b));
            }
        }

        // Measure address-based getParentChunk
        long t0 = System.nanoTime();
        for (long i = 0; i < iters; i++)
        {
            ByteBuffer b = buffers.get((int)(i % n));
            sink += System.identityHashCode(BufferPool.Chunk.getParentChunk(b));
        }
        long addrNs = System.nanoTime() - t0;

        // Measure identity-map lookup (old-scheme stand-in)
        long t1 = System.nanoTime();
        for (long i = 0; i < iters; i++)
        {
            ByteBuffer b = buffers.get((int)(i % n));
            sink += System.identityHashCode(identityMap.get(b));
        }
        long mapNs = System.nanoTime() - t1;

        System.out.println(String.format("MICROBENCH sink=%d iters=%d addr=%.2f_ns/op map=%.2f_ns/op speedup(map/addr)=%.2fx",
                                          sink, iters, (double) addrNs / iters, (double) mapNs / iters, (double) mapNs / addrNs));

        for (ByteBuffer b : buffers)
            pool.put(b);
    }
}
