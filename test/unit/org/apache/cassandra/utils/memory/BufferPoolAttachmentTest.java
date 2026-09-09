/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BufferPoolAttachmentTest
{
    @Test
    public void testMarkerHasNoUsableMark()
    {
        Assume.assumeTrue(Runtime.version().feature() >= 25);
        BufferPool pool = new BufferPool("marker_mark", 8 << 20, true);
        ByteBuffer buffer = pool.get(128, BufferType.OFF_HEAP);
        try
        {
            ByteBuffer marker = (ByteBuffer) MemoryUtil.getAttachment(buffer);
            try
            {
                marker.reset();
                fail("Private attachment markers must not expose a reset position");
            }
            catch (InvalidMarkException expected)
            {
            }
            assertEquals(0, marker.position());
            assertEquals(BufferPool.TINY_CHUNK_SIZE, marker.capacity());
            assertNull(MemoryUtil.getAttachment(marker));
        }
        finally
        {
            pool.put(buffer);
            pool.unsafeReset();
        }
    }

    @Test
    public void testParentsSharingAnAddressRemainDistinct()
    {
        BufferPool.Chunk macro = new BufferPool.Chunk(null, ByteBuffer.allocateDirect(8 << 20));
        BufferPool.Chunk normal = new BufferPool.Chunk(null, macro.get(BufferPool.NORMAL_CHUNK_SIZE));
        BufferPool.Chunk tiny = new BufferPool.Chunk(null, normal.get(BufferPool.TINY_CHUNK_SIZE));
        ByteBuffer buffer = tiny.get(128);
        try
        {
            assertEquals(macro.baseAddress, normal.baseAddress);
            assertEquals(normal.baseAddress, tiny.baseAddress);
            assertSame(tiny, BufferPool.Chunk.getParentChunk(buffer));
        }
        finally
        {
            tiny.free(buffer);
            tiny.unsafeFree();
            normal.unsafeFree();
            macro.unsafeFree();
        }
    }

    @Test
    public void testRetiredTinyMarkerDoesNotResolveToReplacement()
    {
        Assume.assumeTrue(Runtime.version().feature() >= 25);
        BufferPool pool = new BufferPool("marker_retirement", 8 << 20, true);
        ByteBuffer first = pool.get(128, BufferType.OFF_HEAP);
        ByteBuffer stale = MemoryUtil.getHollowDirectByteBuffer();
        MemoryUtil.setAttachment(stale, MemoryUtil.getAttachment(first));
        pool.put(first);
        try
        {
            for (int i = 0; i < 1000; ++i)
            {
                ByteBuffer next = pool.get(128, BufferType.OFF_HEAP);
                assertNotNull(BufferPool.Chunk.getParentChunk(next));
                assertNull(BufferPool.Chunk.getParentChunk(stale));
                pool.put(next);
            }
            assertEquals(0, pool.usedSizeInBytes());
        }
        finally
        {
            pool.unsafeReset();
        }
    }

    @Test
    public void testDirectoryGrowthAndReuse()
    {
        Assume.assumeTrue(Runtime.version().feature() >= 25);
        BufferPool pool = new BufferPool("marker_growth", 48 << 20, true);
        List<ByteBuffer> buffers = new ArrayList<>();
        List<ByteBuffer> stale = new ArrayList<>();
        try
        {
            // More normal slabs than the registry's initial directory, with IDs larger than tiny capacity.
            for (int i = 0; i < 300; ++i)
            {
                ByteBuffer buffer = pool.get(BufferPool.NORMAL_CHUNK_SIZE, BufferType.OFF_HEAP);
                buffers.add(buffer);
                ByteBuffer alias = MemoryUtil.getHollowDirectByteBuffer();
                MemoryUtil.setAttachment(alias, MemoryUtil.getAttachment(buffer));
                stale.add(alias);
            }
            for (ByteBuffer buffer : buffers)
                assertNotNull(BufferPool.Chunk.getParentChunk(buffer));
            boolean exceedsCapacity = false;
            // Page IDs may be reused in any order. Seventeen normal slabs ensure at least one ID exceeds 2 KiB.
            for (int i = 0; i < 17 * 128 && !exceedsCapacity; ++i)
            {
                ByteBuffer tiny = pool.get(BufferPool.TINY_ALLOCATION_LIMIT, BufferType.OFF_HEAP);
                buffers.add(tiny);
                ByteBuffer marker = (ByteBuffer) MemoryUtil.getAttachment(tiny);
                exceedsCapacity = -MemoryUtil.getAttachmentMarker(marker) - 2 > marker.capacity();
            }
            assertTrue(exceedsCapacity);
            for (ByteBuffer buffer : buffers)
                pool.put(buffer);
            buffers.clear();
            pool.unsafeReset();

            // Retired normal markers cannot resolve after their directory entries have been reused.
            for (int i = 0; i < 300; ++i)
                buffers.add(pool.get(BufferPool.NORMAL_CHUNK_SIZE, BufferType.OFF_HEAP));
            for (ByteBuffer alias : stale)
                assertNull(BufferPool.Chunk.getParentChunk(alias));
            for (ByteBuffer buffer : buffers)
                assertNotNull(BufferPool.Chunk.getParentChunk(buffer));
        }
        finally
        {
            for (ByteBuffer buffer : buffers)
                pool.put(buffer);
            pool.unsafeReset();
        }
    }

    @Test
    public void testOrdinaryBufferAttachmentIsNotAChunk()
    {
        ByteBuffer root = ByteBuffer.allocateDirect(128);
        try
        {
            assertNull(BufferPool.Chunk.getParentChunk(root.duplicate()));
            root.position(64).mark();
            assertNull(BufferPool.Chunk.getParentChunk(root.slice()));
            root.clear();
            assertNull(BufferPool.Chunk.getParentChunk(root.duplicate()));
        }
        finally
        {
            MemoryUtil.clean(root);
        }
    }

    @Test
    public void testUnrelatedMarkersCannotResolveAnOwner()
    {
        Assume.assumeTrue(Runtime.version().feature() >= 25);
        BufferPool pool = new BufferPool("marker_identity", 8 << 20, true);
        ByteBuffer buffer = pool.get(128, BufferType.OFF_HEAP);
        try
        {
            ByteBuffer actual = (ByteBuffer) MemoryUtil.getAttachment(buffer);
            ByteBuffer marker = MemoryUtil.getHollowDirectByteBuffer();
            ByteBuffer alias = MemoryUtil.getHollowDirectByteBuffer();
            MemoryUtil.setAttachment(alias, marker);
            for (int value : new int[]{ MemoryUtil.getAttachmentMarker(actual), -67, Integer.MIN_VALUE })
            {
                MemoryUtil.setAttachmentMarker(marker, value);
                assertNull(BufferPool.Chunk.getParentChunk(alias));
            }
            assertNotNull(BufferPool.Chunk.getParentChunk(buffer));
        }
        finally
        {
            pool.put(buffer);
            pool.unsafeReset();
        }
    }

    @Test
    public void testGcmWithOverlappingSlices() throws Exception
    {
        // JDK 11's CipherSpi fallback copies direct input in 4 KiB chunks without detecting overlap.
        Assume.assumeTrue(Runtime.version().feature() >= 17);
        Assume.assumeNotNull(Security.getProvider("SunJCE"));
        BufferPool pool = new BufferPool("marker_gcm", 8 << 20, true);
        try
        {
            for (int size : new int[]{ 128, 8192, 65536 })
            {
                for (int offset : new int[]{ -16, 0, 1, 16, 64 })
                {
                    for (boolean readOnly : new boolean[]{ false, true })
                    {
                        byte[] plaintext = new byte[size];
                        new Random(size).nextBytes(plaintext);
                        byte[] encrypted = cipher(Cipher.ENCRYPT_MODE).doFinal(plaintext);
                        for (int mode : new int[]{ Cipher.ENCRYPT_MODE, Cipher.DECRYPT_MODE })
                        {
                            byte[] input = mode == Cipher.ENCRYPT_MODE ? plaintext : encrypted;
                            byte[] expected = mode == Cipher.ENCRYPT_MODE ? encrypted : plaintext;
                            ByteBuffer allocation = pool.get(size + 256, BufferType.OFF_HEAP);
                            try
                            {
                                allocation.position(64).put(input);
                                ByteBuffer source = allocation.duplicate().position(64).limit(64 + input.length).slice();
                                if (readOnly)
                                    source = source.asReadOnlyBuffer();
                                ByteBuffer output = allocation.duplicate().position(64 + offset).limit(64 + offset + expected.length).slice();
                                assertEquals(expected.length, cipher(mode).doFinal(source, output));
                                assertFalse(source.hasRemaining());
                                byte[] actual = new byte[output.flip().remaining()];
                                output.get(actual);
                                assertArrayEquals("size=" + size + ", offset=" + offset + ", mode=" + mode, expected, actual);
                            }
                            finally
                            {
                                pool.put(allocation);
                            }
                        }
                    }
                }
            }
        }
        finally
        {
            pool.unsafeReset();
        }
    }

    private static Cipher cipher(int mode) throws Exception
    {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding", "SunJCE");
        cipher.init(mode, new SecretKeySpec(new byte[16], "AES"), new GCMParameterSpec(128, new byte[12]));
        return cipher;
    }
}
