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

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.memory.BufferPools;

import io.netty.buffer.ByteBuf;
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
 * <p>The channel allocator covers the unwrap source/destination and the wrap destination. The encrypt-path wrap
 * <em>source</em> (the {@link FrameEncoder.Payload} frame buffer, streaming send buffers and the {@code Initiate}
 * handshake message) is drawn directly from the main {@code BufferPool} independent of this channel allocator, so it is
 * moved onto the Netty pool separately by {@link #wrapFrame(java.nio.ByteBuffer)}. With both in place, every AES-GCM
 * operand on the Option B path is a Netty (att-legal) buffer and the marker is not needed for transport; see
 * working/DECISIONS.md (Phase 6).
 */
public final class TransportByteBufAllocator
{
    /** true if transport channels should use Netty's pooled allocator instead of the main BufferPool. */
    public static final boolean USE_NETTY_POOL = CASSANDRA_NETTY_TRANSPORT_USES_NETTY_POOL.getBoolean();

    /**
     * CASSANDRA-21171: when Option B is on, the inbound {@code FrameDecoder} path defaults to <em>zero-copy</em> -
     * wrapping the decrypted Netty {@code ByteBuf}'s {@code nioBuffer()} view directly in a Netty-backed
     * {@link ShareableBytes} - instead of copying the readable bytes into a {@code BufferPool} buffer. This internal
     * toggle ({@code -Dcassandra.net.netty_transport_inbound_zero_copy=false}) forces the original copy strategy so the
     * two inbound strategies can be A/B'd in a benchmark. Defaults to {@code true} (zero-copy) whenever Option B is on;
     * irrelevant when Option B is off (the inbound buffer is then a {@code BufferPoolAllocator.Wrapped}, adopted as
     * before). Only the single-NIO-component case is zero-copied; a multi-component {@code ByteBuf} (e.g. a
     * {@code CompositeByteBuf} from the JDK SSL engine) always falls back to the copy path - see
     * {@code FrameDecoder.channelRead}.
     */
    public static final boolean INBOUND_ZERO_COPY =
        Boolean.parseBoolean(System.getProperty("cassandra.net.netty_transport_inbound_zero_copy", "true"));

    /**
     * Explicit arena page size for the transport Netty pool (bytes). With {@link #ARENA_MAX_ORDER} this fixes the arena
     * {@code PoolChunk} size at {@code pageSize << maxOrder}; see {@link #ARENA_CHUNK_SIZE}.
     */
    private static final int ARENA_PAGE_SIZE = 8192;

    /**
     * Explicit arena {@code maxOrder} for the transport Netty pool. With {@link #ARENA_PAGE_SIZE} = 8192 this gives a
     * {@code 8192 << 9 = 4 MiB} arena chunk - deliberately a quarter of Netty's 16 MiB default, to bound the worst-case
     * arena-pinning amplification on the inbound zero-copy held-slice path (CASSANDRA-21171 P2/P3). A held large-message
     * frame self-contained in one read pins one whole chunk until reassembly/deserialize; 4 MiB caps that exposure, and
     * the P2 copy-out releases such held slices promptly anyway.
     */
    private static final int ARENA_MAX_ORDER = 9;

    /** The transport Netty pool's arena {@code PoolChunk} size in bytes ({@code ARENA_PAGE_SIZE << ARENA_MAX_ORDER}). */
    public static final int ARENA_CHUNK_SIZE = ARENA_PAGE_SIZE << ARENA_MAX_ORDER;

    /**
     * A single shared Netty pooled allocator for all transport channels when {@link #USE_NETTY_POOL} is set. Using one
     * instance keeps the per-thread arena caches shared across the internode and native pipelines (they run on the same
     * Netty event loops), matching how {@code GlobalBufferPoolAllocator} was a single shared instance.
     *
     * <p>CASSANDRA-21171 P3: arenas are sized <em>explicitly</em> (default arena counts, but pageSize/maxOrder fixed for
     * a {@value #ARENA_CHUNK_SIZE}-byte chunk) rather than relying on Netty's defaults, so the arena geometry is a
     * deliberate, documented choice. The off-heap allocated here lives in Netty arenas, <b>not</b> in the main
     * {@code BufferPool}, so it is <b>not</b> bounded by {@code networking_cache_size} and is surfaced via its own
     * metrics (see {@link org.apache.cassandra.metrics.TransportBufferPoolMetrics} / {@link #pooledMetric()}).
     */
    private static final PooledByteBufAllocator NETTY_POOL =
        new PooledByteBufAllocator(true,
                                   PooledByteBufAllocator.defaultNumHeapArena(),
                                   PooledByteBufAllocator.defaultNumDirectArena(),
                                   ARENA_PAGE_SIZE,
                                   ARENA_MAX_ORDER);

    private TransportByteBufAllocator() {}

    /**
     * The channel allocator the transport pipelines should install via {@code ChannelOption.ALLOCATOR}: Netty's pooled
     * allocator under Option B, otherwise the {@link GlobalBufferPoolAllocator} (main BufferPool) as before.
     */
    public static ByteBufAllocator instance()
    {
        return USE_NETTY_POOL ? NETTY_POOL : GlobalBufferPoolAllocator.instance;
    }

    /**
     * The Netty pooled allocator backing Option B, regardless of whether {@link #USE_NETTY_POOL} is set. Used by the
     * frame encoders to obtain the wrap-source buffer; only consulted on the Option B path.
     */
    public static PooledByteBufAllocator nettyPool()
    {
        return NETTY_POOL;
    }

    /**
     * CASSANDRA-21171 (P3): the transport Netty pool's allocator metric ({@code PooledByteBufAllocatorMetric}: arena
     * counts, per-arena active/allocated bytes, {@code usedDirectMemory}, etc.). Off-heap reported here is invisible to
     * {@code BufferPoolMetrics} and unbounded by {@code networking_cache_size}; it is surfaced separately by
     * {@link org.apache.cassandra.metrics.TransportBufferPoolMetrics}.
     */
    public static io.netty.buffer.PooledByteBufAllocatorMetric pooledMetric()
    {
        return NETTY_POOL.metric();
    }

    /** Bytes of direct memory the transport Netty pool has reserved across its arenas (allocated, not just live). */
    public static long usedDirectMemory()
    {
        return NETTY_POOL.metric().usedDirectMemory();
    }

    /** Bytes of direct memory actually pinned by live buffers in the transport Netty pool's arenas. */
    public static long pinnedDirectMemory()
    {
        return NETTY_POOL.pinnedDirectMemory();
    }

    /**
     * Produce the on-wire {@link ByteBuf} that the frame encoders hand to the channel (and thus, under TLS, to the
     * {@code SslHandler} as its <em>wrap source</em>) from a fully-assembled frame sitting in a main-{@code BufferPool}
     * {@code ByteBuffer}.
     *
     * <p><b>Option B off (default):</b> behaves exactly as before - returns a {@link BufferPoolAllocator.Wrapped} over
     * the BufferPool buffer ({@link GlobalBufferPoolAllocator#wrap}); the buffer is returned to the pool when Netty
     * deallocates the {@code Wrapped}.
     *
     * <p><b>Option B on:</b> copies the finished frame into a Netty {@link PooledByteBufAllocator} direct buffer and
     * returns the {@code BufferPool} buffer to the pool immediately. This moves the encrypt-path <em>wrap source</em>
     * off the main {@code BufferPool} so that - together with the channel-allocator swap that covers the unwrap
     * source/destination and wrap destination - <b>every</b> AES-GCM operand is a Netty buffer with a legal {@code att}
     * chain, so JDK 25's {@code GaloisCounterMode.overlapDetection} never casts a {@code Chunk} to {@code java.nio.Buffer}.
     * The frame is already fully built (header + payload + trailer/CRC), so a single copy of the finished bytes
     * suffices; the buffer-building logic upstream is untouched.
     *
     * <p>Refcount/ownership: the returned Netty {@code ByteBuf} starts at refCnt 1, owned by the channel write path,
     * and is released by Netty (or the {@code SslHandler}) once written - standard Netty semantics. The source
     * {@code BufferPool} buffer is returned to the pool <b>only on success</b>. If allocation or the copy throws (e.g.
     * {@code OutOfDirectMemoryError} from {@link PooledByteBufAllocator#directBuffer(int)}, which is allocated outside
     * the {@code try}), the source buffer is <em>not</em> freed here: ownership stays with the caller, whose
     * {@code catch} block frees it exactly once. (CASSANDRA-21171: previously the {@code put} was in a {@code finally},
     * which double-freed {@code frame} on the throw path because the caller's {@code catch} freed it too.)
     *
     * <p><b>P1 (CASSANDRA-21171):</b> the Netty copy only matters when the channel is TLS-encrypted - AES-GCM is
     * TLS-only, so a non-encrypted channel never feeds the frame buffer to {@code GaloisCounterMode} and there is no
     * {@code att}-legality requirement. For non-encrypted channels we therefore skip the copy and return the previous
     * zero-copy {@link GlobalBufferPoolAllocator#wrap} even under Option B, avoiding a per-frame memcpy on the common
     * unencrypted internode/native path.
     *
     * @param encrypted whether the channel this frame is being written to has an {@code SslHandler} installed. Callers
     *                  with a {@link ChannelHandlerContext}/pipeline should pass {@code pipeline.get(SslHandler.class)
     *                  != null}.
     */
    public static ByteBuf wrapFrame(ByteBuffer frame, boolean encrypted)
    {
        // Option B off, or a non-encrypted channel (no GCM operand): keep the zero-copy Wrapped exactly as before.
        if (!USE_NETTY_POOL || !encrypted)
            return GlobalBufferPoolAllocator.wrap(frame);

        return copyToNetty(frame, NETTY_POOL);
    }

    /**
     * CASSANDRA-21171 (C1): copy {@code frame} into a Netty direct buffer from {@code nettyAlloc}, returning the source
     * {@code BufferPool} buffer to the pool <b>only on success</b>. {@code directBuffer()} is deliberately the first
     * thing inside the try so an {@code OutOfDirectMemoryError} there does NOT free {@code frame} (the caller still owns
     * it on every failure path); on a copy failure the partially-allocated {@code out} is released and the throwable
     * rethrown without freeing {@code frame}. Package-visible + allocator-injectable so the double-free regression can
     * be exercised by injecting a failing allocator.
     */
    @com.google.common.annotations.VisibleForTesting
    static ByteBuf copyToNetty(ByteBuffer frame, ByteBufAllocator nettyAlloc)
    {
        ByteBuf out = null;
        try
        {
            out = nettyAlloc.directBuffer(frame.remaining());
            out.writeBytes(frame.duplicate());
        }
        catch (Throwable t)
        {
            if (out != null)
                out.release();
            throw t;
        }
        BufferPools.forNetworking().put(frame);   // only on success; caller's catch owns `frame` on failure
        return out;
    }
}
