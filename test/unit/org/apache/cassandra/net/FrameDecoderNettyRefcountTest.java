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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * CASSANDRA-21171 "Netty Option B" zero-copy inbound refcount-balance proof.
 *
 * <p>The #1 risk of the zero-copy inbound path is the Netty {@link ByteBuf} being released the wrong number of times.
 * This test drives {@link FrameDecoder#channelRead(ChannelHandlerContext, Object)}'s {@code ByteBuf} branch with REAL
 * {@link FrameDecoderCrc} decoding of REAL {@link FrameEncoderCrc} frames, and asserts the inbound {@code ByteBuf}'s
 * reference count returns to exactly 0 (released exactly once) across every scenario:
 *
 * <ul>
 *   <li>a single self-contained frame in one read;</li>
 *   <li>multiple self-contained frames in one read;</li>
 *   <li>a frame split across two reads (the cross-read {@code stash} copy path);</li>
 *   <li>a read that is only partially consumed (trailing partial frame stashed);</li>
 *   <li>the flow-control paused/{@code reactivate} stash path (processor returns false, then reactivates);</li>
 *   <li>a multi-component {@link CompositeByteBuf} (JDK-SSL-engine shape) - falls back to the copy path;</li>
 *   <li>the zero-copy toggle off - forces the copy path.</li>
 * </ul>
 *
 * <p>Run with {@code -Dio.netty.leakDetection.level=paranoid} (the CI harness for this test does) so Netty samples
 * every buffer and a missed {@code release()} is reported as a leak on GC. The refCnt assertions here catch a missed or
 * double release deterministically regardless of leak detection.
 */
public class FrameDecoderNettyRefcountTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
    }

    /** Build a real CRC self-contained frame whose payload is {@code payload} into a heap ByteBuffer (header+CRC). */
    private static ByteBuffer crcFrame(byte[] payload)
    {
        FrameEncoder.Payload p = FrameEncoderCrc.instance.allocator().allocate(true, payload.length);
        p.buffer.put(payload);
        p.finish();
        ByteBuf framed = FrameEncoderCrc.instance.encode(true, p.buffer);
        try
        {
            ByteBuffer out = ByteBuffer.allocate(framed.readableBytes());
            framed.getBytes(framed.readerIndex(), out);
            out.flip();
            return out;
        }
        finally
        {
            framed.release();
        }
    }

    /** Concatenate several framed byte sequences into one contiguous byte[]. */
    private static byte[] concat(List<ByteBuffer> frames)
    {
        int total = 0;
        for (ByteBuffer b : frames)
            total += b.remaining();
        byte[] all = new byte[total];
        int off = 0;
        for (ByteBuffer b : frames)
        {
            ByteBuffer d = b.duplicate();
            int n = d.remaining();
            d.get(all, off, n);
            off += n;
        }
        return all;
    }

    /** A direct, single-component Netty ByteBuf carrying {@code data[from..to)} - the zero-copy-eligible shape. */
    private static ByteBuf directBuf(ByteBufAllocator alloc, byte[] data, int from, int to)
    {
        ByteBuf b = alloc.directBuffer(to - from);
        b.writeBytes(data, from, to - from);
        return b;
    }

    private static FrameDecoderCrc newDecoder()
    {
        return FrameDecoderCrc.create(GlobalBufferPoolAllocator.instance);
    }

    /** A FrameProcessor that copies out each frame's payload and consumes it; can be told to pause after N frames. */
    private static class Collector implements FrameDecoder.FrameProcessor
    {
        final List<byte[]> payloads = new ArrayList<>();
        int pauseAfter = Integer.MAX_VALUE;

        @Override
        public boolean process(FrameDecoder.Frame frame)
        {
            FrameDecoder.IntactFrame intact = (FrameDecoder.IntactFrame) frame;
            ByteBuffer buf = intact.contents.get();
            byte[] bytes = new byte[buf.remaining()];
            buf.duplicate().get(bytes);
            payloads.add(bytes);
            intact.consume();
            return payloads.size() < pauseAfter;
        }
    }

    // ---- scenarios ---------------------------------------------------------------------------------------------

    @Test
    public void singleSelfContainedFrame() throws Exception
    {
        for (boolean zeroCopy : new boolean[]{ true, false })
            runSingle(zeroCopy);
    }

    private void runSingle(boolean zeroCopy) throws Exception
    {
        ByteBufAllocator alloc = new UnpooledByteBufAllocator(true);
        FrameDecoderCrc decoder = newDecoder();
        Collector collector = new Collector();
        byte[] payload = "hello-21171".getBytes();
        byte[] framed = concat(java.util.Collections.singletonList(crcFrame(payload)));

        ByteBuf in = directBuf(alloc, framed, 0, framed.length);
        decoder.channelReadForTest(in, zeroCopy);
        decoder.processBacklogForTest(collector);

        assertEquals("exactly one frame", 1, collector.payloads.size());
        assertArrayEquals(payload, collector.payloads.get(0));
        assertEquals("ByteBuf released exactly once (zeroCopy=" + zeroCopy + ")", 0, in.refCnt());
        decoder.discardForTest();
    }

    @Test
    public void multipleFramesOneRead() throws Exception
    {
        ByteBufAllocator alloc = new UnpooledByteBufAllocator(true);
        FrameDecoderCrc decoder = newDecoder();
        Collector collector = new Collector();

        List<ByteBuffer> frames = new ArrayList<>();
        byte[][] payloads = { "a".getBytes(), "bb".getBytes(), "ccc-frame-three".getBytes() };
        for (byte[] p : payloads)
            frames.add(crcFrame(p));
        byte[] framed = concat(frames);

        ByteBuf in = directBuf(alloc, framed, 0, framed.length);
        decoder.channelReadForTest(in, true);
        decoder.processBacklogForTest(collector);

        assertEquals(3, collector.payloads.size());
        for (int i = 0; i < payloads.length; i++)
            assertArrayEquals(payloads[i], collector.payloads.get(i));
        assertEquals("ByteBuf released exactly once", 0, in.refCnt());
        decoder.discardForTest();
    }

    @Test
    public void frameSplitAcrossTwoReads() throws Exception
    {
        ByteBufAllocator alloc = new UnpooledByteBufAllocator(true);
        FrameDecoderCrc decoder = newDecoder();
        Collector collector = new Collector();

        byte[] payload = "a-payload-split-across-two-tcp-reads".getBytes();
        byte[] framed = concat(java.util.Collections.singletonList(crcFrame(payload)));
        int split = framed.length / 2;

        ByteBuf in1 = directBuf(alloc, framed, 0, split);
        decoder.channelReadForTest(in1, true);
        decoder.processBacklogForTest(collector);
        assertEquals("no frame yet (stashed)", 0, collector.payloads.size());
        assertEquals("first read's ByteBuf released once", 0, in1.refCnt());

        ByteBuf in2 = directBuf(alloc, framed, split, framed.length);
        decoder.channelReadForTest(in2, true);
        decoder.processBacklogForTest(collector);
        assertEquals("frame assembled from stash + second read", 1, collector.payloads.size());
        assertArrayEquals(payload, collector.payloads.get(0));
        assertEquals("second read's ByteBuf released once", 0, in2.refCnt());
        decoder.discardForTest();
    }

    @Test
    public void partiallyConsumedRead() throws Exception
    {
        // One full frame followed by the first half of a second frame in a single read: the decoder delivers the first
        // and stashes the trailing partial. The single inbound ByteBuf must still be released exactly once.
        ByteBufAllocator alloc = new UnpooledByteBufAllocator(true);
        FrameDecoderCrc decoder = newDecoder();
        Collector collector = new Collector();

        byte[] p1 = "first-complete-frame".getBytes();
        byte[] p2 = "second-frame-only-partially-present".getBytes();
        ByteBuffer f1 = crcFrame(p1);
        ByteBuffer f2 = crcFrame(p2);
        int f1Len = f1.remaining();
        int f2Half = f2.remaining() / 2;

        List<ByteBuffer> both = new ArrayList<>();
        both.add(f1);
        both.add(f2);
        byte[] all = concat(both);

        ByteBuf in = directBuf(alloc, all, 0, f1Len + f2Half);
        decoder.channelReadForTest(in, true);
        decoder.processBacklogForTest(collector);

        assertEquals("only the first, complete frame delivered", 1, collector.payloads.size());
        assertArrayEquals(p1, collector.payloads.get(0));
        assertEquals("ByteBuf released exactly once despite a stashed tail", 0, in.refCnt());
        decoder.discardForTest();
    }

    @Test
    public void flowControlPausedReactivate() throws Exception
    {
        // Two frames in one read; processor pauses after the first (returns false). The decoder retains the second
        // frame in its frames deque (a slice over the inbound ByteBuf). The inbound ByteBuf must NOT be released until
        // that retained slice is later delivered+released on reactivate.
        ByteBufAllocator alloc = new UnpooledByteBufAllocator(true);
        FrameDecoderCrc decoder = newDecoder();
        Collector collector = new Collector();
        collector.pauseAfter = 1;

        List<ByteBuffer> frames = new ArrayList<>();
        byte[][] payloads = { "first".getBytes(), "second-held-under-backpressure".getBytes() };
        for (byte[] p : payloads)
            frames.add(crcFrame(p));
        byte[] framed = concat(frames);

        ByteBuf in = directBuf(alloc, framed, 0, framed.length);
        decoder.channelReadForTest(in, true);
        decoder.processBacklogForTest(collector);

        assertEquals("only first frame delivered before pause", 1, collector.payloads.size());
        assertTrue("inbound ByteBuf still alive: a slice for the held second frame pins it", in.refCnt() >= 1);

        // Reactivate: deliver the held second frame.
        collector.pauseAfter = Integer.MAX_VALUE;
        decoder.processBacklogForTest(collector);
        assertEquals("second frame delivered on reactivate", 2, collector.payloads.size());
        assertArrayEquals(payloads[1], collector.payloads.get(1));
        assertEquals("ByteBuf released exactly once after the held slice is freed", 0, in.refCnt());
        decoder.discardForTest();
    }

    @Test
    public void compositeBufFallsBackToCopy() throws Exception
    {
        // A CompositeByteBuf (the JDK SSL engine's accumulation shape) has nioBufferCount() != 1, so the zero-copy
        // branch must fall back to copying. The composite is still released exactly once.
        ByteBufAllocator alloc = new UnpooledByteBufAllocator(true);
        FrameDecoderCrc decoder = newDecoder();
        Collector collector = new Collector();

        byte[] payload = "composite-bytebuf-from-jdk-ssl-engine".getBytes();
        byte[] framed = concat(java.util.Collections.singletonList(crcFrame(payload)));
        int split = framed.length / 2;

        CompositeByteBuf composite = alloc.compositeBuffer();
        composite.addComponent(true, directBuf(alloc, framed, 0, split));
        composite.addComponent(true, directBuf(alloc, framed, split, framed.length));
        assertTrue("composite has multiple NIO components", composite.nioBufferCount() != 1);

        decoder.channelReadForTest(composite, true);
        decoder.processBacklogForTest(collector);

        assertEquals(1, collector.payloads.size());
        assertArrayEquals(payload, collector.payloads.get(0));
        assertEquals("composite (and its components) released exactly once", 0, composite.refCnt());
        decoder.discardForTest();
    }

    @Test
    public void pooledAllocatorZeroCopyRoundTrip() throws Exception
    {
        // Exercise the real PooledByteBufAllocator (Option B's allocator) so the zero-copy nioBuffer() view is over an
        // actual pooled arena chunk, and confirm the chunk reference is returned (refCnt 0) so the arena slot frees.
        PooledByteBufAllocator alloc = new PooledByteBufAllocator(true);
        FrameDecoderCrc decoder = newDecoder();
        Collector collector = new Collector();

        byte[] payload = "pooled-arena-zero-copy".getBytes();
        byte[] framed = concat(java.util.Collections.singletonList(crcFrame(payload)));

        ByteBuf in = directBuf(alloc, framed, 0, framed.length);
        assertEquals("single nio component for a fresh pooled direct buffer", 1, in.nioBufferCount());
        decoder.channelReadForTest(in, true);
        decoder.processBacklogForTest(collector);

        assertEquals(1, collector.payloads.size());
        assertArrayEquals(payload, collector.payloads.get(0));
        assertEquals("pooled ByteBuf released exactly once", 0, in.refCnt());
        decoder.discardForTest();
    }
}
