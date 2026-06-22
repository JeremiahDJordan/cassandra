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

    /**
     * The Netty pooled allocator backing Option B, regardless of whether {@link #USE_NETTY_POOL} is set. Used by the
     * frame encoders to obtain the wrap-source buffer; only consulted on the Option B path.
     */
    public static PooledByteBufAllocator nettyPool()
    {
        return NETTY_POOL;
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
     * {@code BufferPool} buffer is unconditionally returned to the pool here, so there is no double-free and no leak on
     * either branch.
     */
    public static ByteBuf wrapFrame(ByteBuffer frame)
    {
        if (!USE_NETTY_POOL)
            return GlobalBufferPoolAllocator.wrap(frame);

        ByteBuf out = NETTY_POOL.directBuffer(frame.remaining());
        try
        {
            out.writeBytes(frame.duplicate());
            return out;
        }
        catch (Throwable t)
        {
            out.release();
            throw t;
        }
        finally
        {
            BufferPools.forNetworking().put(frame);
        }
    }
}
