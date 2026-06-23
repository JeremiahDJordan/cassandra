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
package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;

import org.apache.cassandra.net.TransportByteBufAllocator;

import io.netty.buffer.PooledByteBufAllocatorMetric;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * CASSANDRA-21171 (P3): metrics for the transport Netty pool used under "Option B"
 * ({@code cassandra.net.netty_transport_allocator=true}). Under Option B the internode/native TLS wrap/unwrap operands,
 * the socket-read buffers, the copied frame wrap-source ({@code wrapFrame}) and streaming send buffers all live in
 * Netty's {@code PooledByteBufAllocator} arenas instead of the main {@code BufferPool}.
 *
 * <p><b>Operator note:</b> off-heap reported here is <b>not</b> part of {@code BufferPoolMetrics} and is <b>not</b>
 * bounded by {@code networking_cache_size} (a.k.a. {@code file_cache_size}). Under Option B there are therefore two
 * off-heap pools to size and observe: the main {@code BufferPool} (cache + I/O) and these Netty transport arenas. The
 * arena chunk size is fixed at {@link TransportByteBufAllocator#ARENA_CHUNK_SIZE} bytes
 * ({@value org.apache.cassandra.net.TransportByteBufAllocator#ARENA_CHUNK_SIZE}).
 *
 * <p>Gauges read live values from {@link TransportByteBufAllocator#pooledMetric()} on each scrape; they read ~0 when
 * Option B is off (the pool is allocated but never used).
 */
public class TransportBufferPoolMetrics
{
    public static final String TYPE_NAME = "TransportBufferPool";

    /** Direct memory (bytes) reserved across the transport pool's arenas - i.e. arena chunks claimed from the OS. */
    public final Gauge<Long> usedDirect;

    /** Direct memory (bytes) actually pinned by live buffers in the transport pool's arenas. */
    public final Gauge<Long> pinnedDirect;

    /** Number of direct arenas in the transport pool. */
    public final Gauge<Integer> numDirectArenas;

    /** The transport pool's arena {@code PoolChunk} size in bytes (a fixed, explicitly-sized geometry). */
    public final Gauge<Integer> chunkSize;

    private static volatile TransportBufferPoolMetrics instance;

    /**
     * Idempotently register the transport Netty-pool gauges. Safe to call more than once (e.g. across test daemon
     * restarts); only the first call registers.
     */
    public static synchronized TransportBufferPoolMetrics register()
    {
        if (instance == null)
            instance = new TransportBufferPoolMetrics();
        return instance;
    }

    public TransportBufferPoolMetrics()
    {
        MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME, "Netty");

        usedDirect = Metrics.register(factory.createMetricName("UsedDirectMemory"),
                                      TransportByteBufAllocator::usedDirectMemory);
        pinnedDirect = Metrics.register(factory.createMetricName("PinnedDirectMemory"),
                                        TransportByteBufAllocator::pinnedDirectMemory);
        numDirectArenas = Metrics.register(factory.createMetricName("NumDirectArenas"),
                                           () -> pooledMetric().numDirectArenas());
        chunkSize = Metrics.register(factory.createMetricName("ChunkSize"),
                                     () -> pooledMetric().chunkSize());
    }

    private static PooledByteBufAllocatorMetric pooledMetric()
    {
        return TransportByteBufAllocator.pooledMetric();
    }
}
