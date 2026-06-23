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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPools;

/**
 * A wrapper for possibly sharing portions of a single {@link ByteBuffer}, optimised for the case where no sharing is
 * necessary.
 *
 * <p>The underlying buffer's storage is owned by one of two terminal release strategies, selected at {@code wrap} time
 * and held only on the {@code owner}:
 * <ul>
 *   <li>the default {@link #wrap(ByteBuffer)} path, where the buffer comes from {@link BufferPools#forNetworking()} and
 *       the terminal release returns it to the pool ({@code put(bytes)}); and</li>
 *   <li>the CASSANDRA-21171 "Netty Option B" {@link #wrap(ByteBuffer, ByteBuf)} path, where the buffer is a zero-copy
 *       {@code nioBuffer()} view of a reference-counted Netty {@link ByteBuf} and the terminal release calls
 *       {@code byteBuf.release()} exactly once.</li>
 * </ul>
 * The reference-count / slice / retain machinery is identical for both; only the terminal {@link #RELEASED} action
 * differs. Slices share the owner (and hence the owner's releaser), so the terminal release fires exactly once.
 *
 * When sharing is necessary, {@link #share()} method must be invoked by the owning thread
 * before a {@link ShareableBytes} instance can be shared with another thread.
 */
public class ShareableBytes
{
    private final ByteBuffer bytes;
    private final ShareableBytes owner;
    /**
     * The terminal release action, run exactly once when the owner's count reaches {@link #RELEASED}. Held only on the
     * owner (slices delegate every release to the owner), so it fires once for the whole share/slice tree. Null for the
     * default BufferPool path (whose terminal action is the hard-wired {@code BufferPools.forNetworking().put(bytes)}).
     */
    private final Runnable onRelease;
    /**
     * CASSANDRA-21171 (P2): true when the backing storage is a reference-counted Netty {@link ByteBuf} arena buffer (the
     * Option B zero-copy inbound path) rather than a {@code BufferPool} buffer. A held slice over a Netty buffer pins a
     * whole Netty arena {@code PoolChunk} (4 MiB with the configured {@code maxOrder=9,pageSize=8192}) for as long as the
     * slice lives, so the large-message reassembly path copies such slices out into a {@code BufferPool} buffer and
     * releases the Netty slice immediately (see {@link #copyToPoolAndRelease()}). Propagated to slices via the owner.
     */
    private final boolean nettyBacked;
    private volatile int count;

    private static final int UNSHARED = -1;
    private static final int RELEASED = 0;
    private static final AtomicIntegerFieldUpdater<ShareableBytes> countUpdater =
        AtomicIntegerFieldUpdater.newUpdater(ShareableBytes.class, "count");

    private ShareableBytes(ByteBuffer bytes)
    {
        this(bytes, null, false);
    }

    private ShareableBytes(ByteBuffer bytes, Runnable onRelease, boolean nettyBacked)
    {
        this.count = UNSHARED;
        this.owner = this;
        this.bytes = bytes;
        this.onRelease = onRelease;
        this.nettyBacked = nettyBacked;
    }

    private ShareableBytes(ShareableBytes owner, ByteBuffer bytes)
    {
        this.owner = owner;
        this.bytes = bytes;
        // slices delegate release to the owner; the releaser lives on the owner only.
        this.onRelease = null;
        // ...but the backing-storage kind is shared with the owner, so a slice knows it pins a Netty arena chunk.
        this.nettyBacked = owner.nettyBacked;
    }

    public ByteBuffer get()
    {
        assert owner.count != 0;
        return bytes;
    }

    public boolean hasRemaining()
    {
        return bytes.hasRemaining();
    }

    public int remaining()
    {
        return bytes.remaining();
    }

    void skipBytes(int skipBytes)
    {
        bytes.position(bytes.position() + skipBytes);
    }

    void consume()
    {
        bytes.position(bytes.limit());
    }

    /**
     * Ensure this ShareableBytes will use atomic operations for updating its count from now on.
     * The first invocation must occur while the calling thread has exclusive access (though there may be more
     * than one 'owner', these must all either be owned by the calling thread or otherwise not being used)
     */
    public ShareableBytes share()
    {
        int count = owner.count;
        if (count < 0)
            owner.count = -count;
        return this;
    }

    private ShareableBytes retain()
    {
        owner.doRetain();
        return this;
    }

    private void doRetain()
    {
        int count = this.count;
        if (count < 0)
        {
            countUpdater.lazySet(this, count - 1);
            return;
        }

        while (true)
        {
            if (count == RELEASED)
                throw new IllegalStateException("Attempted to reference an already released SharedByteBuffer");

            if (countUpdater.compareAndSet(this, count, count + 1))
                return;

            count = this.count;
        }
    }

    public void release()
    {
        owner.doRelease();
    }

    private void doRelease()
    {
        int count = this.count;

        if (count < 0)
            countUpdater.lazySet(this, count += 1);
        else if (count > 0)
            count = countUpdater.decrementAndGet(this);
        else
            throw new IllegalStateException("Already released");

        if (count == RELEASED)
        {
            if (onRelease != null)
                onRelease.run();
            else
                BufferPools.forNetworking().put(bytes);
        }
    }

    boolean isReleased()
    {
        return owner.count == RELEASED;
    }

    /**
     * Create a slice over the next {@code length} bytes, consuming them from our buffer, and incrementing the owner count
     */
    public ShareableBytes sliceAndConsume(int length)
    {
        int begin = bytes.position();
        int end = begin + length;
        ShareableBytes result = slice(begin, end);
        bytes.position(end);
        return result;
    }

    /**
     * Create a new slice, incrementing the number of owners (making it shared if it was previously unshared)
     */
    ShareableBytes slice(int begin, int end)
    {
        ByteBuffer slice = bytes.duplicate();
        slice.position(begin).limit(end);
        return new ShareableBytes(owner.retain(), slice);
    }

    public static ShareableBytes wrap(ByteBuffer buffer)
    {
        return new ShareableBytes(buffer);
    }

    /**
     * CASSANDRA-21171 "Netty Option B" zero-copy inbound path. Wrap a (zero-copy) {@code nioBuffer()} view of a
     * reference-counted Netty {@link ByteBuf}; the terminal release ({@link #RELEASED}) calls {@code byteBuf.release()}
     * exactly once instead of returning a buffer to the {@code BufferPool}.
     *
     * <p>The caller must have retained {@code byteBuf} exactly once for this {@code ShareableBytes} (the
     * {@code nioBuffer()} view does not retain), so that the single terminal {@code release()} balances that retain. The
     * {@code view} must be a window over {@code byteBuf}'s backing memory (e.g. {@code byteBuf.nioBuffer(idx, len)}); it
     * is read/sliced exactly like a BufferPool buffer and never returned to any pool. Slices share this owner and hence
     * this releaser, so the {@code ByteBuf} is released once for the whole share/slice tree.
     */
    public static ShareableBytes wrap(ByteBuffer view, ByteBuf byteBuf)
    {
        return new ShareableBytes(view, () -> ReferenceCountUtil.release(byteBuf), true);
    }

    /**
     * CASSANDRA-21171 (P2): true if the backing storage is a reference-counted Netty arena {@link ByteBuf} (the Option B
     * zero-copy inbound path), as opposed to a {@code BufferPool} buffer. A held slice over such a buffer pins a whole
     * Netty arena chunk until released, so long-lived holders (large-message reassembly) should
     * {@link #copyToPoolAndRelease() copy it out} rather than retain the Netty slice.
     */
    public boolean isNettyBacked()
    {
        return owner.nettyBacked;
    }

    /**
     * CASSANDRA-21171 (P2): copy this {@code ShareableBytes}'s remaining bytes into a fresh {@code BufferPool} buffer,
     * release {@code this} (dropping our hold on the Netty arena chunk), and return a new BufferPool-backed
     * {@code ShareableBytes} over the copy. Intended for long-lived holders of zero-copy Netty-backed slices (e.g.
     * large-message reassembly), so a few KiB of live frame bytes do not pin a multi-MiB Netty arena chunk until the
     * whole message is reassembled and deserialized.
     *
     * <p>The returned {@code ShareableBytes} is unshared (count {@code UNSHARED}); the caller may {@link #share()} it as
     * it would the original slice. The source {@code this} is released exactly once here.
     */
    public ShareableBytes copyToPoolAndRelease()
    {
        int length = bytes.remaining();
        ByteBuffer copy = BufferPools.forNetworking().get(length, BufferType.OFF_HEAP);
        try
        {
            copy.put(bytes.duplicate());
            copy.flip();
        }
        catch (Throwable t)
        {
            BufferPools.forNetworking().put(copy);
            throw t;
        }
        release();
        return new ShareableBytes(copy);
    }
}

