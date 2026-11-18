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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/**
 * {@link CompositeType#extractFirstComponentAsTrieSearchPrefix} returns the first component of a map entry (its key),
 * with its length and its end-of-component byte, which is 1 for an upper bound. The length is an unsigned short.
 */
public class CompositeTypeTrieSearchPrefixTest
{
    @Test
    public void testComponentSizes()
    {
        for (int size : new int[]{ 0, 1, 100, 32766, 32767, 32768, 40000, 65534 })
        {
            ByteBuffer key = ByteBuffer.wrap(new byte[size]);
            ByteBuffer entry = CompositeType.build(ByteBufferAccessor.instance, key, ByteBufferUtil.bytes("value"));

            assertPrefixes("size " + size, key, entry);

            // the same entry at a non-zero position of a larger buffer
            ByteBuffer larger = ByteBuffer.allocate(entry.remaining() + 10);
            larger.position(7);
            larger.put(entry.duplicate());
            larger.position(7);
            larger.limit(7 + entry.remaining());
            assertPrefixes("size " + size + " at position 7", key, larger);
        }
    }

    private static void assertPrefixes(String description, ByteBuffer key, ByteBuffer entry)
    {
        ByteBuffer lower = CompositeType.extractFirstComponentAsTrieSearchPrefix(entry, true);
        ByteBuffer upper = CompositeType.extractFirstComponentAsTrieSearchPrefix(entry, false);
        assertEquals(description + ", lower bound", prefix(key, (byte) 0), lower);
        assertEquals(description + ", upper bound", prefix(key, (byte) 1), upper);
    }

    private static ByteBuffer prefix(ByteBuffer key, byte endOfComponent)
    {
        ByteBuffer prefix = ByteBuffer.allocate(2 + key.remaining() + 1);
        ByteBufferUtil.writeShortLength(prefix, key.remaining());
        prefix.put(key.duplicate());
        prefix.put(endOfComponent);
        prefix.flip();
        return prefix;
    }
}
