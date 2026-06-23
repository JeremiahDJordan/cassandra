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
import java.util.ArrayList;
import java.util.List;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.BufferPools;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * CASSANDRA-21171 (T3): streaming-output refcount / source-pool balance for {@link AsyncStreamingOutputPlus} under
 * "Option B" ({@code cassandra.net.netty_transport_allocator=true}).
 *
 * <p>On an <b>encrypted</b> streaming channel (P1: the only case where {@code wrapFrame} copies to Netty), each flushed
 * frame is copied into a Netty {@link ByteBuf} and the source {@code BufferPool} buffer returned to the pool exactly
 * once. This asserts:
 * <ul>
 *   <li>the {@code BufferPool} {@code usedSize} returns to its pre-flush baseline (no source-pool leak / double-free);</li>
 *   <li>each outbound {@code ByteBuf} reaches {@code refCnt} 0 after the channel write path releases it.</li>
 * </ul>
 *
 * <p>Skips unless run with {@code -Dcassandra.net.netty_transport_allocator=true} (so {@code wrapFrame} actually copies),
 * since {@link TransportByteBufAllocator#USE_NETTY_POOL} is read once at class load.
 */
public class StreamingOptionBRefcountTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /** Captures and releases every outbound ByteBuf, recording each so we can assert refCnt 0 afterwards. */
    private static class CapturingReleaser extends ChannelOutboundHandlerAdapter
    {
        final List<ByteBuf> written = new ArrayList<>();

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        {
            if (msg instanceof ByteBuf)
            {
                ByteBuf buf = (ByteBuf) msg;
                written.add(buf);
                buf.release();          // simulate the channel/SslHandler consuming and releasing the wrap source
                promise.setSuccess();
            }
            else
            {
                ctx.write(msg, promise);
            }
        }
    }

    @Test
    public void encryptedStreamingFlushBalancesSourcePoolAndRefcounts() throws Throwable
    {
        Assume.assumeTrue("Run with -Dcassandra.net.netty_transport_allocator=true to exercise Option B's wrapFrame copy",
                          TransportByteBufAllocator.USE_NETTY_POOL);

        // An EmbeddedChannel whose pipeline has a real SslHandler so AsyncStreamingOutputPlus.isEncrypted() returns true
        // and wrapFrame copies the frame into a Netty buffer. The capturing handler is added AFTER the SslHandler (i.e.
        // closer to the tail), so outbound writes (tail -> head) hit it first; it releases each write and completes the
        // promise without propagating, so the real SslHandler never has to process (or handshake for) our frames - it is
        // present purely so pipeline.get(SslHandler.class) != null.
        javax.net.ssl.SSLEngine engine = javax.net.ssl.SSLContext.getDefault().createSSLEngine();
        engine.setUseClientMode(true);
        EmbeddedChannel channel = new EmbeddedChannel();
        CapturingReleaser releaser = new CapturingReleaser();
        channel.pipeline().addLast("ssl", new SslHandler(engine));
        channel.pipeline().addLast("capture", releaser);

        long baselineUsed = BufferPools.forNetworking().metrics().usedSize.getValue();

        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
        {
            // doFlush path: write some bytes then flush.
            out.writeLong(0xCAFEBABEDEADBEEFL);
            out.writeInt(42);
            out.doFlush(0);

            // writeToChannel path: fill a buffer via the supplier and flush it.
            out.writeToChannel(alloc -> {
                ByteBuffer b = alloc.get(64);
                for (int i = 0; i < 64; i++)
                    b.put((byte) i);
                b.flip();
            }, StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort()));
        }

        assertTrue("at least two frames were written (doFlush + writeToChannel)", releaser.written.size() >= 2);
        for (int i = 0; i < releaser.written.size(); i++)
            assertEquals("outbound ByteBuf " + i + " released exactly once", 0, releaser.written.get(i).refCnt());

        // The source BufferPool buffers (the wrap sources copied into Netty) must all have been returned to the pool:
        // used size is back to the baseline (modulo the close()/discard() of the final 8 KiB buffer, which is also freed).
        long afterUsed = BufferPools.forNetworking().metrics().usedSize.getValue();
        assertEquals("BufferPool source buffers balanced (no leak / no double-free)", baselineUsed, afterUsed);

        channel.finishAndReleaseAll();
    }
}
