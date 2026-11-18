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

package org.apache.cassandra.index.sai.utils;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertSame;

/**
 * Bytes of a frozen collection type that declare more elements than they hold: the byte-comparable encoders of lists,
 * sets and maps reject them with a MarshalException, and {@link TypeUtil#canonicalize} leaves such a value as written.
 */
public class CanonicalizeInvalidCollectionTest
{
    // a count of 0x7a12210c elements, followed by 3 bytes
    private static final ByteBuffer LIST_OR_SET = ByteBufferUtil.hexToBytes("7a12210cf54fb2");
    // 0x3a12210c entries: twice that is still a valid array size
    private static final ByteBuffer MAP = ByteBufferUtil.hexToBytes("3a12210cf54fb2");
    // a negative count
    private static final ByteBuffer NEGATIVE = ByteBufferUtil.hexToBytes("fffffffff54fb2");

    private static final ListType<?> LIST = ListType.getInstance(DecimalType.instance, false);
    private static final SetType<?> SET = SetType.getInstance(IntegerType.instance, false);
    private static final MapType<?, ?> MAP_TYPE = MapType.getInstance(DecimalType.instance, LIST, false);

    @Test
    public void testEncoders()
    {
        assertRejected(LIST, LIST_OR_SET);
        assertRejected(SET, LIST_OR_SET);
        assertRejected(MAP_TYPE, MAP);
        for (AbstractType<?> type : List.of(LIST, SET, MAP_TYPE))
            assertRejected(type, NEGATIVE);
    }

    @Test
    public void testCanonicalize()
    {
        assertSame(LIST_OR_SET, TypeUtil.canonicalize(LIST_OR_SET, LIST));
        assertSame(LIST_OR_SET, TypeUtil.canonicalize(LIST_OR_SET, SET));
        assertSame(MAP, TypeUtil.canonicalize(MAP, MAP_TYPE));
        for (AbstractType<?> type : List.of(LIST, SET, MAP_TYPE))
            assertSame(NEGATIVE, TypeUtil.canonicalize(NEGATIVE, type));

        // in a tuple, which canonicalize parses as a whole
        TupleType tuple = new TupleType(List.of(LIST, Int32Type.instance));
        ByteBuffer tupleValue = TupleType.buildValue(LIST_OR_SET, Int32Type.instance.decompose(1));
        assertSame(tupleValue, TypeUtil.canonicalize(tupleValue, tuple));

        // as the key of a map entry, whose components are canonicalized
        CompositeType entry = CompositeType.getInstance(LIST, Int32Type.instance);
        ByteBuffer entryValue = CompositeType.build(ByteBufferAccessor.instance, LIST_OR_SET,
                                                    Int32Type.instance.decompose(1));
        assertSame(entryValue, TypeUtil.canonicalizeComponents(entryValue, entry));
    }

    private static void assertRejected(AbstractType<?> type, ByteBuffer value)
    {
        assertThatThrownBy(() -> type.asComparableBytes(ByteBufferAccessor.instance, value.duplicate(),
                                                        ByteComparable.Version.OSS41))
        .as(type.asCQL3Type() + " " + ByteBufferUtil.bytesToHex(value))
        .isInstanceOf(MarshalException.class);
    }
}
