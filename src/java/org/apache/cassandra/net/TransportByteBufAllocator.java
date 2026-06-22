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
package org.apache.cassandra.net;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_NETTY_TRANSPORT_USES_NETTY_POOL;

/**
 * CASSANDRA-21171 "Netty Option B" selector for the channel-level {@link ByteBufAllocator} used by the internode and
 * native-transport Netty pipelines.
 *
 * <p>When {@code cassandra.net.netty_transport_allocator=true}, the channels use Netty's own
 * {@link PooledByteBufAllocator} so that the TLS {@code SslHandler}'s AES-GCM operands (the unwrap source/destination
 * and the wrap destination) are Netty buffers with a legal {@code att} chain - fixing the JDK 25
 * {@code GaloisCounterMode.overlapDetection} {@code ClassCastException} at the source, with no marker, map or version
 * gate. When false (the default), the channels use {@link GlobalBufferPoolAllocator} (the main {@code BufferPool})
 * exactly as before, relying on the shipped per-chunk marker workaround on JDK 25.
 *
 * <p><b>Known residual (prototype):</b> the encrypt-path wrap <em>source</em> is the {@link FrameEncoder.Payload}
 * buffer, drawn directly from the main {@code BufferPool} independent of this channel allocator. Pointing the channel
 * allocator at Netty does not by itself move that operand; see working/DECISIONS.md.
 */
public final class TransportByteBufAllocator
{
    /** true if transport channels should use Netty's pooled allocator instead of the main BufferPool. */
    public static final boolean USE_NETTY_POOL = CASSANDRA_NETTY_TRANSPORT_USES_NETTY_POOL.getBoolean();

    /**
     * A single shared Netty pooled allocator for all transport channels when {@link #USE_NETTY_POOL} is set. Using one
     * instance keeps the per-thread arena caches shared across the internode and native pipelines (they run on the same
     * Netty event loops), matching how {@code GlobalBufferPoolAllocator} was a single shared instance.
     */
    private static final PooledByteBufAllocator NETTY_POOL = new PooledByteBufAllocator(true);

    private TransportByteBufAllocator() {}

    /**
     * The channel allocator the transport pipelines should install via {@code ChannelOption.ALLOCATOR}: Netty's pooled
     * allocator under Option B, otherwise the {@link GlobalBufferPoolAllocator} (main BufferPool) as before.
     */
    public static ByteBufAllocator instance()
    {
        return USE_NETTY_POOL ? NETTY_POOL : GlobalBufferPoolAllocator.instance;
    }
}
