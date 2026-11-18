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

package org.apache.cassandra.index.sai.disk.apache;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.StringType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * How Apache Cassandra 5.0 encodes the terms of an index (its {@code IndexTermType}), for the type of the indexed
 * values: the validator of our {@code IndexContext}, which is the same type Apache indexes (the column type, or the
 * type of the collection elements, keys, or entries for a non-frozen collection).
 */
public class ApacheTermType
{
    /**
     * Apache encodes everything in OSS50.
     */
    public static final ByteComparable.Version VERSION = ByteComparable.Version.OSS50;

    private static final byte[] IPV4_PREFIX = new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 };
    private static final int DECIMAL_APPROXIMATION_BYTES = 24;
    private static final int BIG_INTEGER_APPROXIMATION_BYTES = 20;
    private static final int INET_ADDRESS_SIZE = 16;
    private static final int DEFAULT_FIXED_LENGTH = 16;

    private final AbstractType<?> indexType;
    private final boolean literal;
    private final boolean frozen;
    private final boolean composite;
    private final boolean reversed;

    private ApacheTermType(AbstractType<?> indexType)
    {
        this.indexType = indexType;
        AbstractType<?> baseType = indexType.unwrap();

        // Same as Apache's IndexTermType#calculateCapabilities
        this.composite = baseType instanceof CompositeType;
        this.frozen = !composite && !baseType.subTypes().isEmpty() && !baseType.isMultiCell();
        this.literal = baseType instanceof StringType || baseType instanceof BooleanType || frozen || composite;
        // Apache doesn't treat these as reversed (CASSANDRA-20100), and doesn't store them reversed either.
        boolean notReversed = baseType instanceof InetAddressType
                              || baseType instanceof IntegerType
                              || baseType instanceof DecimalType
                              || baseType instanceof LongType;
        this.reversed = indexType.isReversed() && !notReversed;
    }

    public static ApacheTermType of(AbstractType<?> indexType)
    {
        return new ApacheTermType(indexType);
    }

    public AbstractType<?> indexType()
    {
        return indexType;
    }

    public boolean isLiteral()
    {
        return literal;
    }

    public boolean isReversed()
    {
        return reversed;
    }

    /**
     * Whether the terms of a literal index are its raw values, escaped (see {@link ByteSource#of(ByteBuffer, ByteComparable.Version)}),
     * which preserves the order of the bytes: for the {@code text} and {@code ascii} types this is the order of the
     * values. For the other types (or reversed), ranges of terms must be filtered.
     */
    public boolean isOrderedString()
    {
        return indexType instanceof UTF8Type || indexType instanceof AsciiType;
    }

    /**
     * The term of a literal value in Apache's terms dictionary (Apache's {@code IndexTermType#asComparableBytes}).
     */
    public ByteComparable asComparableBytes(ByteBuffer value)
    {
        return v -> asComparableBytes(value, v);
    }

    private ByteSource asComparableBytes(ByteBuffer value, ByteComparable.Version version)
    {
        if (value.remaining() == 0)
            return ByteSource.EMPTY;

        AbstractType<?> baseType = indexType.unwrap();
        if (baseType instanceof InetAddressType || baseType instanceof IntegerType || baseType instanceof DecimalType)
            return ByteSource.optionalFixedLength(ByteBufferAccessor.instance, value);
        else if (baseType instanceof LongType)
            return ByteSource.optionalSignedFixedLengthNumber(ByteBufferAccessor.instance, value);
        else if (frozen)
            return ByteSource.of(value, version);
        return indexType.asComparableBytes(value, version);
    }

    /**
     * Whether the index truncates the values (varint, decimal), so that a range query must include its bounds, and
     * filter later.
     */
    public boolean supportsRounding()
    {
        AbstractType<?> baseType = indexType.unwrap();
        return baseType instanceof IntegerType || baseType instanceof DecimalType;
    }

    /**
     * The size of the terms of a numeric (balanced tree) index (Apache's {@code IndexTermType#fixedSizeOf}).
     */
    public int fixedSizeOf()
    {
        AbstractType<?> baseType = indexType.unwrap();
        if (indexType.isValueLengthFixed())
            return indexType.valueLengthIfFixed();
        else if (baseType instanceof InetAddressType)
            return INET_ADDRESS_SIZE;
        else if (baseType instanceof IntegerType)
            return BIG_INTEGER_APPROXIMATION_BYTES;
        else if (baseType instanceof DecimalType)
            return DECIMAL_APPROXIMATION_BYTES;
        return DEFAULT_FIXED_LENGTH;
    }

    /**
     * The term to look up in Apache's balanced tree for a query bound: the encoded bound for a varint (a query also
     * searches the bound as written, see Expression#withUncanonicalizedBounds, which finds the varints Apache stored
     * with redundant sign bytes), the value for other types, which have a single serialization.
     */
    public byte[] asNumericTerm(Expression.Value value)
    {
        if (indexType.unwrap() instanceof IntegerType)
        {
            byte[] bytes = new byte[fixedSizeOf()];
            ByteBufferUtil.copyBytes(value.encoded, value.encoded.position(), bytes, 0, bytes.length);
            return bytes;
        }
        return asNumericTerm(value.raw);
    }

    /**
     * The term of a numeric value in Apache's balanced tree (Apache's {@code IndexTermType#asIndexBytes} then
     * {@code IndexTermType#toComparableBytes}).
     */
    public byte[] asNumericTerm(ByteBuffer value)
    {
        byte[] bytes = new byte[fixedSizeOf()];
        ByteBuffer indexBytes = asIndexBytes(value);
        AbstractType<?> baseType = indexType.unwrap();
        if (baseType instanceof InetAddressType || baseType instanceof IntegerType || baseType instanceof DecimalType)
            ByteBufferUtil.copyBytes(indexBytes, indexBytes.position(), bytes, 0, bytes.length);
        else
            ByteSourceInverse.copyBytes(asComparableBytes(indexBytes, VERSION), bytes);
        return bytes;
    }

    private ByteBuffer asIndexBytes(ByteBuffer value)
    {
        if (value == null || value.remaining() == 0)
            return value;

        AbstractType<?> baseType = indexType.unwrap();
        if (baseType instanceof InetAddressType)
            return encodeInetAddress(value);
        else if (baseType instanceof IntegerType)
            return TypeUtil.encodeBigIntegerAsWritten(value); // Apache's encoding, which keeps redundant sign bytes
        else if (baseType instanceof DecimalType)
            return encodeDecimal(value);
        return value;
    }

    /**
     * Apache's {@code IndexTermType#encodeInetAddress}: IPv4 addresses as their IPv6 equivalent.
     */
    private static ByteBuffer encodeInetAddress(ByteBuffer value)
    {
        if (value.remaining() == 4)
        {
            ByteBuffer mapped = ByteBuffer.allocate(INET_ADDRESS_SIZE);
            System.arraycopy(IPV4_PREFIX, 0, mapped.array(), 0, IPV4_PREFIX.length);
            ByteBufferUtil.copyBytes(value, value.position(), mapped, IPV4_PREFIX.length, value.remaining());
            return mapped;
        }
        return value;
    }

    /**
     * Apache's {@code IndexTermType#encodeDecimal}: the OSS50 byte-comparable form, cut or padded to 24 bytes.
     */
    private static ByteBuffer encodeDecimal(ByteBuffer value)
    {
        ByteSource bs = DecimalType.instance.asComparableBytes(value, VERSION);
        byte[] bytes = new byte[DECIMAL_APPROXIMATION_BYTES]; // right-padded with 0s
        int b;
        for (int i = 0; i < bytes.length && (b = bs.next()) != ByteSource.END_OF_STREAM; i++)
            bytes[i] = (byte) b;
        return ByteBuffer.wrap(bytes);
    }

    /** The inverse of {@link #asComparableBytes} for the literal terms of a terms dictionary. */
    public ByteBuffer decodeLiteral(byte[] term)
    {
        assert literal : indexType + " is not a literal type";
        if (term.length == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        ByteSource.Peekable source = ByteSource.peekable(ByteSource.preencoded(term));
        if (frozen)
            return ByteBuffer.wrap(ByteSourceInverse.getUnescapedBytes(source));
        return indexType.fromComparableBytes(source, VERSION);
    }
}
