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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * CASSANDRA-21171 (C1): {@link TransportByteBufAllocator#copyToNetty} must return the source {@code BufferPool} buffer
 * to the pool <b>exactly once</b> on success, and <b>not at all</b> on the throw path (the caller's {@code catch} is the
 * single owner on failure). The earlier {@code finally { put }} double-freed the source buffer when the copy threw,
 * because the caller's {@code catch} freed it too.
 *
 * <p>We inject a {@link ByteBufAllocator} whose {@code directBuffer(int)} throws {@link OutOfDirectMemoryError} (the
 * real-world trigger), and track the {@code BufferPool}'s {@code usedSize} to prove the source buffer was put exactly
 * once across the failure + the caller's single subsequent free.
 */
public class WrapFrameDoubleFreeTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /** Simulates the real-world trigger: directBuffer() throws an OOM-class error from the Netty arena. */
    static final class InjectedAllocationFailure extends Error
    {
        InjectedAllocationFailure() { super("injected directBuffer() failure (stands in for OutOfDirectMemoryError)"); }
    }

    /** An allocator whose directBuffer() always throws, simulating OutOfDirectMemoryError from the Netty arena. */
    private static class FailingAllocator extends AbstractByteBufAllocator
    {
        FailingAllocator() { super(true); }

        @Override
        protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity)
        {
            throw new InjectedAllocationFailure();
        }

        @Override
        protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity)
        {
            throw new InjectedAllocationFailure();
        }

        @Override
        public boolean isDirectBufferPooled()
        {
            return true;
        }
    }

    @Test
    public void sourceBufferNotFreedWhenCopyThrows()
    {
        BufferPool pool = BufferPools.forNetworking();
        long base = pool.metrics().usedSize.getValue();

        ByteBuffer frame = pool.get(1024, BufferType.OFF_HEAP);
        long withFrameHeld = pool.metrics().usedSize.getValue();
        assertTrue("getting a frame raises usedSize above baseline", withFrameHeld > base);

        try
        {
            TransportByteBufAllocator.copyToNetty(frame, new FailingAllocator());
            fail("expected the injected allocation failure to propagate");
        }
        catch (InjectedAllocationFailure expected)
        {
            // good
        }

        // C1: copyToNetty must NOT have freed `frame` on the throw path - the caller still owns it. usedSize unchanged.
        assertEquals("source buffer must NOT be freed on the throw path (caller owns it)",
                     withFrameHeld, (long) pool.metrics().usedSize.getValue());

        // The caller frees it exactly once (as every wrapFrame caller's catch does).
        pool.put(frame);
        assertEquals("after the caller's single put, usedSize is back to baseline (freed exactly once)",
                     base, (long) pool.metrics().usedSize.getValue());
    }

    @Test
    public void sourceBufferFreedExactlyOnceOnSuccess()
    {
        BufferPool pool = BufferPools.forNetworking();
        long base = pool.metrics().usedSize.getValue();

        ByteBuffer frame = pool.get(1024, BufferType.OFF_HEAP);
        frame.position(0).limit(1024);
        assertTrue(pool.metrics().usedSize.getValue() > base);

        ByteBuf out = TransportByteBufAllocator.copyToNetty(frame, new PooledByteBufAllocator(true));
        try
        {
            // On success copyToNetty returns `frame` to the pool exactly once: usedSize back to baseline.
            assertEquals("source buffer freed exactly once on success", base, (long) pool.metrics().usedSize.getValue());
            assertEquals("copied frame length preserved", 1024, out.readableBytes());
        }
        finally
        {
            out.release();
        }
    }
}
