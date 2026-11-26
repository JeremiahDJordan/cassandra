/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ListSerializerTest
{
    private static final ListSerializer<Integer> serializer = ListType.getInstance(Int32Type.instance, false).getSerializer();

    /**
     * A null element of a serialized list matches no value, and the elements after it are still compared.
     */
    @Test
    public void testAnyMatchWithNullElements()
    {
        List<ByteBuffer> values = Arrays.asList(null, Int32Type.instance.decompose(1), null, Int32Type.instance.decompose(2));
        ByteBuffer list = CollectionSerializer.pack(values, values.size(), ProtocolVersion.V3);

        assertTrue(serializer.anyMatch(list, v -> Int32Type.instance.compose(v) == 1));
        assertTrue(serializer.anyMatch(list, v -> Int32Type.instance.compose(v) == 2));
        assertFalse(serializer.anyMatch(list, v -> Int32Type.instance.compose(v) == 3));
        assertFalse(serializer.anyMatch(CollectionSerializer.pack(Arrays.asList(null, null), 2, ProtocolVersion.V3), v -> true));
    }

    @Test(expected = MarshalException.class)
    public void testAnyMatchOfListTruncatedInAnElementLength()
    {
        serializer.anyMatch(truncatedList(6), v -> false);
    }

    @Test(expected = MarshalException.class)
    public void testAnyMatchOfListTruncatedInAnElementValue()
    {
        serializer.anyMatch(truncatedList(2), v -> false);
    }

    /** A list of two ints without its last {@code bytes} bytes. */
    private static ByteBuffer truncatedList(int bytes)
    {
        List<ByteBuffer> values = Arrays.asList(Int32Type.instance.decompose(1), Int32Type.instance.decompose(2));
        ByteBuffer list = CollectionSerializer.pack(values, values.size(), ProtocolVersion.V3);
        list.limit(list.limit() - bytes);
        return list;
    }
}
