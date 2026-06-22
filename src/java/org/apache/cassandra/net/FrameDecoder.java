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
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;

import static org.apache.cassandra.utils.ByteBufferUtil.copyBytes;

/**
 * A Netty inbound handler that decodes incoming frames and passes them forward to
 * {@link InboundMessageHandler} for processing.
 *
 * Handles work stashing, and together with {@link InboundMessageHandler} - flow control.
 *
 * Unlike most Netty inbound handlers, doesn't use the pipeline to talk to its
 * upstream handler. Instead, a {@link FrameProcessor} must be registered with
 * the frame decoder, to be invoked on new frames. See {@link #deliver(FrameProcessor)}.
 *
 * See {@link #activate(FrameProcessor)}, {@link #reactivate()}, and {@link FrameProcessor}
 * for flow control implementation.
 *
 * Five frame decoders currently exist, one used for each connection depending on flags and messaging version:
 * 1. {@link FrameDecoderCrc}:
          no compression; payload is protected by CRC32
 * 2. {@link FrameDecoderLZ4}:
          LZ4 compression with custom frame format; payload is protected by CRC32
 * 3. {@link FrameDecoderUnprotected}:
          no compression; no integrity protection
 */
public abstract class FrameDecoder extends ChannelInboundHandlerAdapter
{
    private static final FrameProcessor NO_PROCESSOR =
        frame -> { throw new IllegalStateException("Frame processor invoked on an unregistered FrameDecoder"); };

    private static final FrameProcessor CLOSED_PROCESSOR =
        frame -> { throw new IllegalStateException("Frame processor invoked on a closed FrameDecoder"); };

    public interface FrameProcessor
    {
        /**
         * Frame processor that the frames should be handed off to.
         *
         * @return true if more frames can be taken by the processor, false if the decoder should pause until
         * it's explicitly resumed.
         */
        boolean process(Frame frame) throws IOException;
    }

    public abstract static class Frame
    {
        public final boolean isSelfContained;
        public final int frameSize;

        Frame(boolean isSelfContained, int frameSize)
        {
            this.isSelfContained = isSelfContained;
            this.frameSize = frameSize;
        }

        abstract void release();
        abstract boolean isConsumed();
    }

    /**
     * The payload bytes of a complete frame, i.e. a frame stripped of its headers and trailers,
     * with any verification supported by the protocol confirmed.
     *
     * If {@code isSelfContained} the payload contains one or more {@link Message}, all of which
     * may be parsed entirely from the bytes provided.  Otherwise, only a part of exactly one
     * {@link Message} is contained in the payload; it can be relied upon that this partial {@link Message}
     * will only be delivered in its own unique {@link Frame}.
     */
    public final static class IntactFrame extends Frame
    {
        public final ShareableBytes contents;

        IntactFrame(boolean isSelfContained, ShareableBytes contents)
        {
            super(isSelfContained, contents.remaining());
            this.contents = contents;
        }

        void release()
        {
            contents.release();
        }

        boolean isConsumed()
        {
            return !contents.hasRemaining();
        }

        public void consume()
        {
            contents.consume();
        }
    }

    /**
     * A corrupted frame was encountered; this represents the knowledge we have about this frame,
     * and whether or not the stream is recoverable.
     *
     * Generally we consider a frame with corrupted header as unrecoverable, and frames with intact header,
     * but corrupted payload - as recoverable, since we know and can skip payload size.
     *
     * {@link InboundMessageHandler} further has its own idea of which frames are and aren't recoverable.
     * A recoverable {@link CorruptFrame} can be considered unrecoverable by {@link InboundMessageHandler}
     * if it's the first frame of a large message (isn't self contained).
     */
    public final static class CorruptFrame extends Frame
    {
        public final int readCRC;
        public final int computedCRC;

        CorruptFrame(boolean isSelfContained, int frameSize, int readCRC, int computedCRC)
        {
            super(isSelfContained, frameSize);
            this.readCRC = readCRC;
            this.computedCRC = computedCRC;
        }

        static CorruptFrame recoverable(boolean isSelfContained, int frameSize, int readCRC, int computedCRC)
        {
            return new CorruptFrame(isSelfContained, frameSize, readCRC, computedCRC);
        }

        static CorruptFrame unrecoverable(int readCRC, int computedCRC)
        {
            return new CorruptFrame(false, Integer.MIN_VALUE, readCRC, computedCRC);
        }

        public boolean isRecoverable()
        {
            return frameSize != Integer.MIN_VALUE;
        }

        void release() { }

        boolean isConsumed()
        {
            return true;
        }
    }

    protected final BufferPoolAllocator allocator;

    @VisibleForTesting
    final Deque<Frame> frames = new ArrayDeque<>(4);
    ByteBuffer stash;

    private boolean isActive;
    private boolean isClosed;
    private ChannelHandlerContext ctx;
    private FrameProcessor processor = NO_PROCESSOR;

    FrameDecoder(BufferPoolAllocator allocator)
    {
        this.allocator = allocator;
    }

    abstract void decode(Collection<Frame> into, ShareableBytes bytes);
    abstract void addLastTo(ChannelPipeline pipeline);

    /**
     * @return true if we are actively decoding and processing frames
     */
    public boolean isActive()
    {
        return isActive;
    }
    
    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to start receiving frames.
     */
    public void activate(FrameProcessor processor)
    {
        if (this.processor != NO_PROCESSOR)
            throw new IllegalStateException("Attempted to activate an already active FrameDecoder");

        this.processor = processor;

        isActive = true;
        ctx.read();
    }

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to resume
     * receiving frames after previously indicating that processing should be paused.
     */
    public void reactivate() throws IOException
    {
        if (isActive)
            throw new IllegalStateException("Tried to reactivate an already active FrameDecoder");

        if (deliver(processor))
        {
            isActive = true;
            onExhausted();
        }
    }

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to resume
     * receiving frames after previously indicating that processing should be paused.
     *
     * Does not reactivate processing or reading from the wire, but permits processing as many frames (or parts thereof)
     * that are already waiting as the processor requires.
     */
    void processBacklog(FrameProcessor processor) throws IOException
    {
        deliver(processor);
    }

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to permanently
     * stop receiving frames, e.g. because of an exception caught.
     */
    public void discard()
    {
        isActive = false;
        processor = CLOSED_PROCESSOR;
        if (stash != null)
        {
            ByteBuffer bytes = stash;
            stash = null;
            allocator.put(bytes);
        }
        while (!frames.isEmpty())
            frames.poll().release();
    }

    /**
     * Called by Netty pipeline when a new message arrives.
     *
     * <p>In the default ({@link GlobalBufferPoolAllocator}) configuration the inbound buffer is a
     * {@link BufferPoolAllocator.Wrapped}: we {@link BufferPoolAllocator.Wrapped#adopt() adopt} its underlying
     * {@code BufferPool} {@link ByteBuffer} (taking ownership) and hand it to {@link #decode(Collection, ShareableBytes)}
     * via a {@link ShareableBytes}, whose release returns it to the pool. This is the zero-copy fast path.
     *
     * <p>Under CASSANDRA-21171 "Netty Option B" ({@code cassandra.net.netty_transport_allocator=true}) the channel
     * allocator is Netty's own {@link io.netty.buffer.PooledByteBufAllocator}, so the {@code SslHandler}'s decrypted
     * output (and the plaintext socket read) arrive here as a plain reference-counted Netty {@link ByteBuf}, NOT a
     * {@code Wrapped}.
     *
     * <p><b>Zero-copy path (default when Option B is on).</b> When the {@code ByteBuf} has a single NIO component
     * ({@code nioBufferCount() == 1}), we wrap its {@code nioBuffer(readerIndex, readableBytes)} - a zero-copy view of
     * the pooled direct buffer - in a Netty-backed {@link ShareableBytes} (see {@link ShareableBytes#wrap(ByteBuffer,
     * ByteBuf)}). We retain the {@code ByteBuf} exactly once so that the {@code ShareableBytes}'s terminal release calls
     * {@code byteBuf.release()} exactly once - balancing the retain - once the whole share/slice tree is released
     * downstream (the {@link IntactFrame} slices outlive this call). The original inbound reference is released in the
     * {@code finally}. The slice/stash/deliver machinery downstream is unchanged: a held slice now pins a Netty arena
     * chunk rather than a BufferPool chunk (see working/DECISIONS.md Phase 7 pinning audit).
     *
     * <p><b>Composite / multi-component fallback.</b> A {@code CompositeByteBuf} (e.g. from the JDK SSL engine
     * accumulating across records) has {@code nioBufferCount() != 1}, so {@code nioBuffer()} would itself copy/
     * consolidate the components. In that case (and when the zero-copy toggle is off) we fall back to copying the
     * readable bytes into a fresh {@code BufferPool} buffer and releasing the Netty buffer here. The copied buffer flows
     * through the same {@code ShareableBytes} machinery, preserving the BufferPool ownership model. The cross-read
     * {@link #stash} copy is always a BufferPool buffer regardless of branch.
     *
     * <p>These buffers are passed to {@link #decode(Collection, ShareableBytes)}, which collects decoded frames into
     * {@link #frames}, which we send upstream in {@link #deliver}.
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException
    {
        if (msg instanceof BufferPoolAllocator.Wrapped)
        {
            ByteBuffer buf = ((BufferPoolAllocator.Wrapped) msg).adopt();
            // netty will probably have mis-predicted the space needed
            allocator.putUnusedPortion(buf);
            channelRead(ShareableBytes.wrap(buf));
        }
        else if (msg instanceof ByteBuf)
        {
            // Option B: a Netty-pooled, reference-counted buffer that we cannot adopt into the BufferPool ownership
            // model.
            channelReadByteBuf((ByteBuf) msg, TransportByteBufAllocator.INBOUND_ZERO_COPY);
        }
        else
        {
            throw new IllegalArgumentException("Unexpected inbound message type: "
                                               + (msg == null ? "null" : msg.getClass().getName()));
        }
    }

    /**
     * Decode a Netty {@link ByteBuf} inbound (Option B). When {@code zeroCopy} and the buffer has a single NIO component
     * ({@code nioBufferCount() == 1}), wrap a zero-copy {@code nioBuffer()} view in a Netty-backed {@link ShareableBytes}
     * (retaining the {@code ByteBuf} once, terminal release drops it once); otherwise (multi-component / toggle off) copy
     * the readable bytes into a {@code BufferPool} buffer and release the Netty buffer. The {@code ByteBuf} is released
     * exactly once in every branch.
     */
    private void channelReadByteBuf(ByteBuf in, boolean zeroCopy) throws IOException
    {
        int length = in.readableBytes();

        if (zeroCopy && in.nioBufferCount() == 1)
        {
            // Zero-copy: wrap a nioBuffer() view of the pooled buffer and hand ByteBuf ownership to ShareableBytes.
            // Retain once for the ShareableBytes; its terminal release (once the whole share/slice tree is freed,
            // which can outlive this call) releases() that one reference. nioBuffer(idx, len) does not retain.
            // decode() always drives newBytes to terminal release in its finally even if it throws, so the retained
            // reference is never leaked. The original inbound reference is dropped in our finally below.
            try
            {
                ByteBuffer view = in.nioBuffer(in.readerIndex(), length);
                in.retain();
                channelRead(ShareableBytes.wrap(view, in));
            }
            finally
            {
                in.release();
            }
        }
        else
        {
            // Composite / multi-component (or toggle off): nioBuffer() would copy anyway. Copy the readable bytes
            // into a BufferPool buffer and release the Netty buffer immediately.
            try
            {
                ByteBuffer copy = allocator.getAtLeast(length);
                copy.limit(length);
                in.getBytes(in.readerIndex(), copy);
                copy.flip();
                channelRead(ShareableBytes.wrap(copy));
            }
            finally
            {
                in.release();
            }
        }
    }

    /**
     * Test-only: drive the Netty-{@code ByteBuf} inbound branch without an active processor or {@code ChannelHandlerContext}.
     * The decoder is left inactive, so frames are decoded into {@link #frames} but not delivered; use
     * {@link #processBacklogForTest} to deliver them. Lets {@code FrameDecoderNettyRefcountTest} assert the inbound
     * {@code ByteBuf}'s refcount is balanced across stash / backpressure paths.
     */
    @VisibleForTesting
    void channelReadForTest(ByteBuf in, boolean zeroCopy) throws IOException
    {
        channelReadByteBuf(in, zeroCopy);
    }

    @VisibleForTesting
    void processBacklogForTest(FrameProcessor processor) throws IOException
    {
        processBacklog(processor);
    }

    @VisibleForTesting
    void discardForTest()
    {
        discard();
    }

    void channelRead(ShareableBytes bytes) throws IOException
    {
        decode(frames, bytes);

        if (isActive)
            isActive = deliver(processor);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx)
    {
        if (isActive)
            onExhausted();
    }

    /**
     * Only to be invoked when frames.isEmpty().
     *
     * If we have been closed, we will now propagate up the channelInactive notification,
     * and otherwise we will ask the channel for more data.
     */
    private void onExhausted()
    {
        if (isClosed)
            close();
        else
            ctx.read();
    }

    /**
     * Deliver any waiting frames, including those that were incompletely read last time, to the provided processor
     * until the processor returns {@code false}, or we finish the backlog.
     *
     * Propagate the final return value of the processor.
     */
    private boolean deliver(FrameProcessor processor) throws IOException
    {
        boolean deliver = true;
        while (deliver && !frames.isEmpty())
        {
            Frame frame = frames.peek();
            deliver = processor.process(frame);

            assert !deliver || frame.isConsumed();
            if (deliver || frame.isConsumed())
            {
                frames.poll();
                frame.release();
            }
        }
        return deliver;
    }

    void stash(ShareableBytes in, int stashLength, int begin, int length)
    {
        ByteBuffer out = allocator.getAtLeast(stashLength);
        copyBytes(in.get(), begin, out, 0, length);
        out.position(length);
        stash = out;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        this.ctx = ctx;
        ctx.channel().config().setAutoRead(false);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        isClosed = true;
        if (frames.isEmpty())
            close();
    }

    private void close()
    {
        discard();
        ctx.fireChannelInactive();
        allocator.release();
    }

    /**
     * Utility: fill {@code out} from {@code in} up to {@code toOutPosition},
     * updating the position of both buffers with the result
     * @return true if there were sufficient bytes to fill to {@code toOutPosition}
     */
    static boolean copyToSize(ByteBuffer in, ByteBuffer out, int toOutPosition)
    {
        int bytesToSize = toOutPosition - out.position();
        if (bytesToSize <= 0)
            return true;

        if (bytesToSize > in.remaining())
        {
            out.put(in);
            return false;
        }

        copyBytes(in, in.position(), out, out.position(), bytesToSize);
        in.position(in.position() + bytesToSize);
        out.position(toOutPosition);
        return true;
    }

    /**
     * @return {@code in} if has sufficient capacity, otherwise
     *         a replacement from {@code BufferPool} that {@code in} is copied into
     */
    ByteBuffer ensureCapacity(ByteBuffer in, int capacity)
    {
        if (in.capacity() >= capacity)
            return in;

        ByteBuffer out = allocator.getAtLeast(capacity);
        in.flip();
        out.put(in);
        allocator.put(in);
        return out;
    }
}
