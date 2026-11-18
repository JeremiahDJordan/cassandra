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

package org.apache.cassandra.index.sai.utils;

import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class TypeUtil
{
    private static final byte[] IPV4_PREFIX = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 };

    /**
     * DecimalType / BigDecimal values are indexed by truncating their asComparableBytes representation to this size,
     * padding on the right with zero-value-bytes until this size is reached (if necessary).  This causes
     * false-positives that must be filtered in a separate step after hitting the index and reading the associated
     * (full) values.
     */
    public static final int DECIMAL_APPROXIMATION_BYTES = 24;

    public static final int BIG_INTEGER_APPROXIMATION_BYTES = 20;

    public static final int INET_ADDRESS_SIZE = 16;

    public static final int DEFAULT_FIXED_LENGTH = 16;
    /**
     * Byte comparable version currently used for all SAI files and structures, with the exception of terms data in
     * the early AA on-disk format.
     */
    public static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS41;

    /** The largest map entry value of an entries index: a CompositeType component has an unsigned short length. */
    public static final int MAX_MAP_ENTRY_VALUE_SIZE = FBUtilities.MAX_UNSIGNED_SHORT;

    /** The largest map entry key: 0xFFFF starting a first CompositeType component is read as the static marker. */
    public static final int MAX_MAP_ENTRY_KEY_SIZE = StorageAttachedIndex.MAX_COMPOSITE_FIRST_COMPONENT_SIZE;

    private static final Map<AbstractType<?>, Boolean> NON_UNIQUE_SERIALIZATION = new ConcurrentHashMap<>();

    private TypeUtil() {}

    /**
     * Returns <code>true</code> if given buffer would pass the {@link AbstractType#validate(ByteBuffer)}
     * check. False otherwise.
     */
    public static boolean isValid(ByteBuffer term, AbstractType<?> validator)
    {
        try
        {
            validator.validate(term);
            return true;
        }
        catch (MarshalException e)
        {
            return false;
        }
    }

    /**
     * Indicates if the type encoding supports rounding of the raw value.
     *
     * This is significant in range searches where we have to make all range
     * queries inclusive when searching the indexes in order to avoid excluding
     * rounded values. Excluded values are removed by post-filtering.
     */
    public static boolean supportsRounding(AbstractType<?> type)
    {
        return isBigInteger(type) || isBigDecimal(type);
    }

    /**
     * Returns the smaller of two {@code ByteBuffer} values, based on the result of {@link
     * #compare(ByteBuffer, ByteBuffer, AbstractType, Version)} comparision.
     */
    public static ByteBuffer min(ByteBuffer a, ByteBuffer b, AbstractType<?> type, Version version)
    {
        return a == null ?  b : (b == null || compare(b, a, type, version) > 0) ? a : b;
    }

    /**
     * Returns the greater of two {@code ByteBuffer} values, based on the result of {@link
     * #compare(ByteBuffer, ByteBuffer, AbstractType, Version)} comparision.
     */
    public static ByteBuffer max(ByteBuffer a, ByteBuffer b, AbstractType<?> type, Version version)
    {
        return a == null ?  b : (b == null || compare(b, a, type, version) < 0) ? a : b;
    }

    /**
     * Returns the value length for the given {@link AbstractType}, selecting 16 for types
     * that officially use VARIABLE_LENGTH but are, in fact, of a fixed length.
     */
    public static int fixedSizeOf(AbstractType<?> type)
    {
        if (type.isValueLengthFixed())
            return type.valueLengthIfFixed();
        else if (isInetAddress(type))
            return INET_ADDRESS_SIZE;
        else if (isBigInteger(type))
            return BIG_INTEGER_APPROXIMATION_BYTES;
        else if (isBigDecimal(type))
            return DECIMAL_APPROXIMATION_BYTES;
        return DEFAULT_FIXED_LENGTH;
    }

    public static AbstractType<?> cellValueType(ColumnMetadata columnMetadata, IndexTarget.Type indexType)
    {
        AbstractType<?> type = columnMetadata.type;
        if (isNonFrozenCollection(type))
        {
            CollectionType<?> collection = ((CollectionType<?>) type);
            switch (collection.kind)
            {
                case LIST:
                    return collection.valueComparator();
                case SET:
                    return collection.nameComparator();
                case MAP:
                    switch (indexType)
                    {
                        case KEYS:
                            return collection.nameComparator();
                        case VALUES:
                            return collection.valueComparator();
                        case KEYS_AND_VALUES:
                            return CompositeType.getInstance(collection.nameComparator(), collection.valueComparator());
                    }
            }
        }
        return type;
    }

    /**
     * Allows overriding the default getString method for {@link CompositeType}. It is
     * a requirement of the {@link ConcurrentRadixTree} that the keys are strings but
     * the getString method of {@link CompositeType} does not return a string that compares
     * in the same order as the underlying {@link ByteBuffer}. To get round this we convert
     * the {@link CompositeType} bytes to a hex string.
     */
    public static String getString(ByteBuffer value, AbstractType<?> type)
    {
        if (isComposite(type))
            return ByteBufferUtil.bytesToHex(value);
        return type.getString(value);
    }

    /**
     * The inverse of the above method. Overrides the fromString method on {@link CompositeType}
     * in order to convert the hex string to bytes.
     */
    public static ByteBuffer fromString(String value, AbstractType<?> type)
    {
        if (isComposite(type))
            return ByteBufferUtil.hexToBytes(value);
        return type.fromString(value);
    }

    public static ByteBuffer fromComparableBytes(ByteComparable value, AbstractType<?> type, ByteComparable.Version version)
    {
        if (isInetAddress(type) || isBigInteger(type) || isBigDecimal(type))
            return ByteBuffer.wrap(ByteSourceInverse.readBytes(value.asComparableBytes(version)));

        return type.fromComparableBytes(ByteSource.peekable(value.asComparableBytes(version)), version);
    }

    public static ByteComparable asComparableBytes(ByteBuffer value, AbstractType<?> type)
    {
        return version -> asComparableBytes(value, type, version);
    }

    public static ByteSource asComparableBytes(ByteBuffer value, AbstractType<?> type, ByteComparable.Version version)
    {
        // These types are indexed in their non-reversed form even on DESC clustering columns (see #isIndexedUnreversed),
        // so this must also hold for a ReversedType, as it does in the on-disk balanced tree (see #toComparableBytes).
        if (isInetAddress(type) || isBigInteger(type) || isBigDecimal(type))
            return ByteSource.optionalFixedLength(ByteBufferAccessor.instance, value);
        // The LongType.asComparableBytes uses variableLengthInteger which doesn't play well with
        // the balanced tree because it is expecting fixed length data. So for SAI we use a optionalSignedFixedLengthNumber
        // to keep all comparable values the same length
        else if (type instanceof LongType)
            return ByteSource.optionalSignedFixedLengthNumber(ByteBufferAccessor.instance, value);
        return type.asComparableBytes(value, version);
    }

    /**
     * Convenience method to create a {@link ByteComparable} from a {@link ByteBuffer} value for a given {@link CompositeType}
     * with a terminator. This method is in this class to keep references to the {@link ByteBufferAccessor#instance} here.
     */
    public static ByteComparable asComparableBytes(ByteBuffer value, int terminator, CompositeType type)
    {
        return v -> type.asComparableBytes(ByteBufferAccessor.instance, value, v, terminator);
    }


    /**
     * Fills a byte array with the comparable bytes for a type.
     * <p>
     * This method expects a {@code value} parameter generated by calling {@link #asIndexBytes(ByteBuffer, AbstractType)}.
     * It is not generally safe to pass the output of other serialization methods to this method.  For instance, it is
     * not generally safe to pass the output of {@link AbstractType#decompose(Object)} as the {@code value} parameter
     * (there are certain types for which this is technically OK, but that doesn't hold for all types).
     *
     * @param value a value buffer returned by {@link #asIndexBytes(ByteBuffer, AbstractType)}
     * @param type the type associated with the encoded {@code value} parameter
     * @param bytes this method's output
     */
    public static void toComparableBytes(ByteBuffer value, AbstractType<?> type, byte[] bytes)
    {
        if (isInetAddress(type))
            ByteBufferUtil.copyBytes(value, value.hasArray() ? value.arrayOffset() + value.position() : value.position(), bytes, 0, INET_ADDRESS_SIZE);
        else if (isBigInteger(type))
            ByteBufferUtil.copyBytes(value, value.hasArray() ? value.arrayOffset() + value.position() : value.position(), bytes, 0, BIG_INTEGER_APPROXIMATION_BYTES);
        else if (isBigDecimal(type))
            ByteBufferUtil.copyBytes(value, value.hasArray() ? value.arrayOffset() + value.position() : value.position(), bytes, 0, DECIMAL_APPROXIMATION_BYTES);
        else
            ByteSourceInverse.readBytesMustFit(type.asComparableBytes(value, BYTE_COMPARABLE_VERSION), bytes);
    }

    /**
     * Translates the external value of specific types into a format used by the index.
     */
    public static ByteBuffer asIndexBytes(ByteBuffer value, AbstractType<?> type)
    {
        if (value == null || value.remaining() == 0)
            return value;

        if (isInetAddress(type))
            return encodeInetAddress(value);
        else if (isBigInteger(type))
            return encodeBigInteger(value);
        else if (isBigDecimal(type))
            return encodeDecimal(value);
        else if (type instanceof CompositeType)
            return canonicalizeComponents(value, (CompositeType) type);
        else if (isFrozen(type) && !type.isVector() && hasNonUniqueSerialization(type))
            return canonicalize(value, type);
        return value;
    }

    /**
     * Whether equal values of the given type can have different serializations, so that the index must hold them in
     * a canonical form (see {@link #canonicalize(ByteBuffer, AbstractType)}): decimals (2.5 and 2.50 differ by their
     * scale), varints (0x0001 and 0x01), and the types that contain them (tuples, UDTs, frozen collections, map
     * entries).
     */
    public static boolean hasNonUniqueSerialization(AbstractType<?> type)
    {
        // Every write and every query of a frozen or map entries index asks, so the answer is kept per type (there are
        // few types, and an ALTER TYPE only adds one).
        Boolean cached = NON_UNIQUE_SERIALIZATION.get(type);
        if (cached == null)
        {
            cached = computeHasNonUniqueSerialization(type);
            NON_UNIQUE_SERIALIZATION.put(type, cached);
        }
        return cached;
    }

    private static boolean computeHasNonUniqueSerialization(AbstractType<?> type)
    {
        type = baseType(type);
        if (type instanceof DecimalType || type instanceof IntegerType)
            return true;
        for (AbstractType<?> subType : type.subTypes())
        {
            if (computeHasNonUniqueSerialization(subType))
                return true;
        }
        return false;
    }

    /**
     * Returns the canonical serialization of the given non-empty value of the given type: the one that its
     * byte-comparable form decodes to, which is the same for all the values equal to it (e.g. 2.5, 2.50 and 2.500 give
     * 2.50, and a tuple (2.5, 1) gives (2.50, 1)). It's also the form the storage engine gives map keys and set
     * elements, which the trie memtable stores as byte-comparables.
     */
    public static ByteBuffer canonicalize(ByteBuffer value, AbstractType<?> type)
    {
        if (value == null || !value.hasRemaining())
            return value;
        type = baseType(type);
        try
        {
            ByteSource bytes = type.asComparableBytes(ByteBufferAccessor.instance, value, BYTE_COMPARABLE_VERSION);
            if (bytes == null)
                return value;
            ByteBuffer canonical = type.fromComparableBytes(ByteBufferAccessor.instance,
                                                            ByteSource.peekable(bytes),
                                                            BYTE_COMPARABLE_VERSION);
            if (canonical.equals(value))
                return value;
            // The byte-comparable form of an empty element (e.g. blobAsDecimal(0x)) is that of a null, which a
            // collection can't hold: the decoded collection is then invalid, and comparing it fails later (index view
            // after a flush, compaction). Validation rejects it, and the value is indexed as written (below).
            type.validate(canonical);
            return canonical;
        }
        catch (RuntimeException e)
        {
            // The value doesn't parse with this type, or has no valid canonical form. The first happens to valid
            // values: an index keeps the type its column had when it was created (IndexContext#validator), so after
            // ALTER TYPE ... ADD the values of a UDT have more fields than the type the index knows. Index and search
            // such a value as written, as was done before the canonical form: a write, a flush, a compaction or a read
            // must not fail here.
            return value;
        }
    }

    /**
     * Returns the composite value (a map entry, or a bound of a range over map entries: a key alone, possibly with an
     * end-of-component byte of 1) with its components of a type with several serializations per value (see
     * {@link #hasNonUniqueSerialization(AbstractType)}) replaced by their canonical serialization, the one they decode
     * to from their byte-comparable form (see {@link #canonicalize(ByteBuffer, AbstractType)}), or the value itself if
     * none changes. From {@link Version#DB}, entries are stored as byte-comparables, so their terms don't change.
     */
    public static ByteBuffer canonicalizeComponents(ByteBuffer value, CompositeType type)
    {
        if (value == null || !hasNonUniqueSerialization(type))
            return value;

        List<AbstractType<?>> types = type.subTypes();
        ByteBuffer input = value.duplicate();
        if (CompositeType.isStaticName(input, ByteBufferAccessor.instance))
            return value;

        List<ByteBuffer> components = new ArrayList<>(types.size());
        List<Byte> endOfComponents = new ArrayList<>(types.size());
        boolean changed = false;
        while (input.hasRemaining())
        {
            // A value that doesn't parse as a composite is left alone: its validation will reject it.
            if (input.remaining() < 2)
                return value;
            int length = ByteBufferUtil.readShortLength(input);
            if (length < 0 || input.remaining() < length + 1)
                return value;
            ByteBuffer component = ByteBufferUtil.readBytes(input, length);
            byte endOfComponent = input.get();

            int i = components.size();
            if (i < types.size() && hasNonUniqueSerialization(types.get(i)))
            {
                ByteBuffer canonical = canonicalize(component, types.get(i));
                // The canonical form can be larger than the written one (2.5 is 5 bytes, 2.50 is 6): a component that
                // no longer fits in a composite is left as written, rather than failing in writeShortLength below.
                int maxSize = i == 0 ? MAX_MAP_ENTRY_KEY_SIZE : MAX_MAP_ENTRY_VALUE_SIZE;
                if (canonical != component && canonical.remaining() <= maxSize)
                {
                    component = canonical;
                    changed = true;
                }
            }
            components.add(component);
            endOfComponents.add(endOfComponent);
        }

        if (!changed)
            return value;

        int size = 0;
        for (ByteBuffer component : components)
            size += 2 + component.remaining() + 1;
        ByteBuffer result = ByteBuffer.allocate(size);
        for (int i = 0; i < components.size(); i++)
        {
            ByteBufferUtil.writeShortLength(result, components.get(i).remaining());
            result.put(components.get(i).duplicate());
            result.put(endOfComponents.get(i));
        }
        result.flip();
        return result;
    }

    /**
     * Whether the index term of a value of the type can differ from its term as written (see
     * {@link #asIndexBytesAsWritten}): map entries, frozen values and varints.
     */
    public static boolean hasCanonicalIndexTerms(AbstractType<?> type)
    {
        return isComposite(type) || (isFrozen(type) && !type.isVector()) || isBigInteger(type);
    }

    /**
     * The term of the value as written, as indexes written by Apache Cassandra 5.0 or without canonical terms hold it:
     * the serialized bytes of a map entry or a frozen value as written, and a varint with its redundant sign bytes. For
     * the other types, the same as {@link #asIndexBytes}.
     */
    public static ByteBuffer asIndexBytesAsWritten(ByteBuffer value, AbstractType<?> type)
    {
        if (value == null || value.remaining() == 0)
            return value;
        if (isBigInteger(type))
            return encodeBigIntegerAsWritten(value);
        if (isComposite(type) || (isFrozen(type) && !type.isVector()))
            return value;
        return asIndexBytes(value, type);
    }

    /**
     * Tries its best to return the inverse of {@link #encode}.
     * For most of the types it returns the exact inverse.
     * For big integers and decimals, which could be truncated by encode, some precision loss is possible.
     */
    public static ByteBuffer decode(ByteBuffer value, AbstractType<?> type)
    {
        if (value == null)
            return null;

        if (isInetAddress(type))
            return decodeInetAddress(value);
        else if (isBigInteger(type))
            return decodeBigInteger(value);
        else if (isBigDecimal(type))
            return decodeDecimal(value);
        return value;
    }

    /**
     * Compare two terms based on their type. This is used in place of {@link AbstractType#compare(ByteBuffer, ByteBuffer)}
     * so that the default comparison can be overridden for specific types.
     *
     * Note: This should be used for all term comparison
     */
    public static int compare(ByteBuffer b1, ByteBuffer b2, AbstractType<?> type, Version version)
    {
        if (isInetAddress(type))
            return compareInet(b1, b2);
        else if (useFastByteOperations(type, version))
            return FastByteOperations.compareUnsigned(b1, b2);

        return type.compare(b1, b2);
    }

    /**
     * This is used for value comparison in post-filtering - {@link Expression#isSatisfiedBy(ByteBuffer)}.
     *
     * This allows types to decide whether they should be compared based on their encoded value or their
     * raw value. At present only {@link InetAddressType} values are compared by their encoded values to
     * allow for ipv4 -> ipv6 equivalency in searches.
     */
    public static int comparePostFilter(Expression.Value requestedValue, Expression.Value columnValue, AbstractType<?> type)
    {
        if (isInetAddress(type))
            return compareInet(requestedValue.encoded, columnValue.encoded);
        // varint and decimal values are indexed in their non-reversed form, and Expression doesn't swap their bounds
        // on DESC clustering columns (see #isIndexedUnreversed), so they must be compared with the base type
        else if (isBigInteger(type) || isBigDecimal(type))
            return baseType(type).compare(requestedValue.raw, columnValue.raw);
        // Override comparisons for frozen collections
        else if (isFrozen(type))
            return FastByteOperations.compareUnsigned(requestedValue.raw, columnValue.raw);

        return type.compare(requestedValue.raw, columnValue.raw);
    }

    /**
     * Returns the values of the live cells of the given collection, as indexed by an index of the given type.
     * <p>
     * The map entries that an {@link IndexTarget.Type#KEYS_AND_VALUES} index can't represent (see
     * {@link #fitsMapEntryTerm(Cell)}) are left out, and given to {@code onUnindexableMapEntry} if it isn't null.
     */
    public static Iterator<ByteBuffer> collectionIterator(AbstractType<?> validator,
                                                          ComplexColumnData cellData,
                                                          ColumnMetadata columnMetadata,
                                                          IndexTarget.Type indexType,
                                                          long nowInSecs,
                                                          @Nullable Consumer<Cell<?>> onUnindexableMapEntry)
    {
        if (cellData == null)
            return null;

        Stream<Cell<?>> cells = StreamSupport.stream(cellData.spliterator(), false)
                                             .filter(cell -> cell != null && cell.isLive(nowInSecs));
        if (indexType == IndexTarget.Type.KEYS_AND_VALUES)
        {
            cells = cells.filter(cell -> {
                if (fitsMapEntryTerm(cell))
                    return true;
                if (onUnindexableMapEntry != null)
                    onUnindexableMapEntry.accept(cell);
                return false;
            });
        }
        Stream<ByteBuffer> stream = cells.map(cell -> cellValue(columnMetadata, indexType, cell));

        if (isInetAddress(validator))
            stream = stream.sorted((c1, c2) -> compareInet(encodeInetAddress(c1), encodeInetAddress(c2)));

        return stream.iterator();
    }

    public static Comparator<ByteBuffer> comparator(AbstractType<?> type, Version version)
    {
        // Override the comparator for BigInteger, frozen collections (not including composite types) and
        // composite types before DB version to maintain a consistent order between the in-memory index and the on-disk index.
        if (useFastByteOperations(type, version))
            return FastByteOperations::compareUnsigned;

        return type;
    }

    private static boolean useFastByteOperations(AbstractType<?> type, Version version)
    {
        // BigInteger types, BigDecimal types, frozen types and composite types (map entries) use compareUnsigned to
        // maintain a consistent order between the in-memory index and the on-disk index. Starting with Version.DB,
        // composite types are compared using their AbstractType.
        return isBigInteger(type)
               || isBigDecimal(type)
               || (!isComposite(type) && isFrozen(type))
               || (isComposite(type) && !version.onOrAfter(Version.DB));
    }

    public static float[] decomposeVector(AbstractType<?> type, ByteBuffer byteBuffer)
    {
        return ((VectorType<?>.VectorSerializer)type.getSerializer()).deserializeFloatArray(byteBuffer);
    }

    public static float[] decomposeVector(IndexContext indexContext, ByteBuffer byteBuffer)
    {
        return decomposeVector(indexContext.getValidator(), byteBuffer);
    }

    /**
     * Whether an entry term can be built for the map cell as written: its key at most
     * {@link #MAX_MAP_ENTRY_KEY_SIZE} bytes and its value at most {@link #MAX_MAP_ENTRY_VALUE_SIZE}. Indexing and
     * post-filtering use this: a component whose canonical form doesn't fit is held as written (see
     * {@link #canonicalizeComponents}). Client writes are checked on the canonical sizes (see
     * {@link #isIndexableMapEntry}).
     */
    public static boolean fitsMapEntryTerm(Cell<?> cell)
    {
        return cell.path().get(0).remaining() <= MAX_MAP_ENTRY_KEY_SIZE && cell.valueSize() <= MAX_MAP_ENTRY_VALUE_SIZE;
    }

    /**
     * Whether a client write of the given map cell is accepted by an {@link IndexTarget.Type#KEYS_AND_VALUES} index of
     * the given validator (the {@link CompositeType} of the map's key and value types), i.e. whether its key is at most
     * {@link #MAX_MAP_ENTRY_KEY_SIZE} bytes and its value at most {@link #MAX_MAP_ENTRY_VALUE_SIZE} in canonical form
     * (see {@link #mapEntryKeySize} and {@link #mapEntryValueSize}), so that the index holds them in that form. An
     * entry that only fits as written, which reaches the index without this check, is indexed as written (see
     * {@link #fitsMapEntryTerm}).
     */
    public static boolean isIndexableMapEntry(Cell<?> cell, AbstractType<?> validator)
    {
        return mapEntryKeySize(cell, validator) <= MAX_MAP_ENTRY_KEY_SIZE
               && mapEntryValueSize(cell, validator) <= MAX_MAP_ENTRY_VALUE_SIZE;
    }

    /**
     * The size of the key of the given map cell in the canonical form an entries index of the given validator holds it
     * in when it fits (see {@link #canonicalizeComponents}), which can be larger than the written form (2.5 is 5
     * bytes, 2.50 is 6).
     */
    public static int mapEntryKeySize(Cell<?> cell, AbstractType<?> validator)
    {
        return indexedComponentSize(cell.path().get(0), validator.subTypes().get(0), MAX_MAP_ENTRY_KEY_SIZE);
    }

    /**
     * The size of the value of the given map cell as an entries index of the given validator holds it (see
     * {@link #mapEntryKeySize}).
     */
    public static int mapEntryValueSize(Cell<?> cell, AbstractType<?> validator)
    {
        return indexedComponentSize(cell.buffer(), validator.subTypes().get(1), MAX_MAP_ENTRY_VALUE_SIZE);
    }

    private static int indexedComponentSize(ByteBuffer component, AbstractType<?> type, int maxSize)
    {
        int size = component.remaining();
        // The canonical form can be much larger than the value as written, whatever its size: a tuple or UDT value
        // written with fewer fields than its type is padded to all of them, 4 bytes per missing field.
        if (size > maxSize || !hasNonUniqueSerialization(type))
            return size;
        return canonicalize(component, type).remaining();
    }

    private static ByteBuffer cellValue(ColumnMetadata columnMetadata, IndexTarget.Type indexType, Cell<?> cell)
    {
        if (columnMetadata.type.isCollection() && columnMetadata.type.isMultiCell())
        {
            switch (((CollectionType<?>) columnMetadata.type).kind)
            {
                case LIST:
                    //TODO Is there any optimisation can be done here with cell values?
                    return cell.buffer();
                case SET:
                    return cell.path().get(0);
                case MAP:
                    switch (indexType)
                    {
                        case KEYS:
                            return cell.path().get(0);
                        case VALUES:
                            return cell.buffer();
                        case KEYS_AND_VALUES:
                            return CompositeType.build(ByteBufferAccessor.instance, cell.path().get(0), cell.buffer());
                    }
            }
        }
        return cell.buffer();
    }

    /**
     * Compares 2 InetAddress terms by ensuring that both addresses are represented as
     * ipv6 addresses. An empty value (which {@link InetAddressType} accepts) sorts before every address, as it does
     * for {@link InetAddressType}.
     */
    private static int compareInet(ByteBuffer b1, ByteBuffer b2)
    {
        assert (isIPv6(b1) || !b1.hasRemaining()) && (isIPv6(b2) || !b2.hasRemaining());

        return FastByteOperations.compareUnsigned(b1, b2);
    }

    private static boolean isIPv6(ByteBuffer address)
    {
        return address.remaining() == INET_ADDRESS_SIZE;
    }

    /**
     * Encode a {@link InetAddress} into a fixed width 16 byte encoded value.
     *
     * The encoded value is byte comparable and prefix compressible.
     *
     * The encoding is done by converting ipv4 addresses to their ipv6 equivalent.
     */
    private static ByteBuffer encodeInetAddress(ByteBuffer value)
    {
        if (value.remaining() == 4)
        {
            int position = value.hasArray() ? value.arrayOffset() + value.position() : value.position();
            ByteBuffer mapped = ByteBuffer.allocate(INET_ADDRESS_SIZE);
            System.arraycopy(IPV4_PREFIX, 0, mapped.array(), 0, IPV4_PREFIX.length);
            ByteBufferUtil.copyBytes(value, position, mapped, IPV4_PREFIX.length, value.remaining());
            return mapped;
        }
        return value;
    }

    private static ByteBuffer decodeInetAddress(ByteBuffer value)
    {
        throw new UnsupportedOperationException("Decoding InetAddress not implemented yet");
    }


    /**
     * Encode a {@link BigInteger} into a fixed width 20 byte encoded value.
     *
     * The encoded value is byte comparable and prefix compressible.
     *
     * The format of the encoding is:
     *
     *  The first 4 bytes contain the integer length of the {@link BigInteger} byte array
     *  with the top bit flipped for positive values.
     *
     *  The remaining 16 bytes contain the 16 most significant bytes of the
     *  {@link BigInteger} byte array.
     *
     *  For {@link BigInteger} values whose underlying byte array is less than
     *  16 bytes, the encoded value is sign extended.
     */
    public static ByteBuffer encodeBigInteger(ByteBuffer value)
    {
        return encodeBigIntegerAsWritten(withoutRedundantSignBytes(value));
    }

    /**
     * Returns the given varint without its redundant leading sign bytes (0x0001 is 1, 0xFF80 is -128), which is the
     * same for all the varints equal to it, so that they have the same index term. A varint written minimally, as
     * clients and CQL literals do, is returned as it is.
     */
    private static ByteBuffer withoutRedundantSignBytes(ByteBuffer value)
    {
        int start = value.position();
        int first = start;
        while (value.limit() - first > 1)
        {
            byte b = value.get(first);
            boolean nextIsNegative = (value.get(first + 1) & 0x80) != 0;
            if ((b == 0 && !nextIsNegative) || (b == (byte) 0xFF && nextIsNegative))
                first++;
            else
                break;
        }
        if (first == start)
            return value;
        ByteBuffer minimal = value.duplicate();
        minimal.position(first);
        return minimal;
    }

    /**
     * The index term of the given varint as written, with its redundant sign bytes: the term an index written before
     * {@link #encodeBigInteger} stripped them holds (see {@link #asIndexBytesAsWritten}), which is also the term of
     * an index written by Apache Cassandra 5.0.
     */
    public static ByteBuffer encodeBigIntegerAsWritten(ByteBuffer value)
    {
        int size = value.remaining();
        int position = value.hasArray() ? value.arrayOffset() + value.position() : value.position();
        byte[] bytes = new byte[BIG_INTEGER_APPROXIMATION_BYTES];
        if (size < BIG_INTEGER_APPROXIMATION_BYTES - Integer.BYTES)
        {
            ByteBufferUtil.copyBytes(value, position, bytes, bytes.length - size, size);
            if ((bytes[bytes.length - size] & 0x80) != 0)
                Arrays.fill(bytes, Integer.BYTES, bytes.length - size, (byte)0xff);
            else
                Arrays.fill(bytes, Integer.BYTES, bytes.length - size, (byte)0x00);
        }
        else
        {
            ByteBufferUtil.copyBytes(value, position, bytes, Integer.BYTES, BIG_INTEGER_APPROXIMATION_BYTES - Integer.BYTES);
        }
        if ((bytes[4] & 0x80) != 0)
        {
            size = -size;
        }
        bytes[0] = (byte)(size >> 24 & 0xff);
        bytes[1] = (byte)(size >> 16 & 0xff);
        bytes[2] = (byte)(size >> 8 & 0xff);
        bytes[3] = (byte)(size & 0xff);
        bytes[0] ^= 0x80;
        return ByteBuffer.wrap(bytes);
    }


    public static ByteBuffer decodeBigInteger(ByteBuffer encoded)
    {
        byte[] bytes = new byte[20];
        encoded.get(bytes);
        encoded.rewind();

        // Undo the XOR operation on the first byte
        bytes[0] ^= 0x80;

        // Extract the size (the first 4 bytes)
        int size = ((bytes[0] & 0xff) << 24) | ((bytes[1] & 0xff) << 16) | ((bytes[2] & 0xff) << 8) | (bytes[3] & 0xff);

        boolean isNegative = size < 0;
        if (isNegative)
            size = -size;

        ByteBuffer result;
        if (size < 16)
        {
            int offset = 20 - size;
            result = ByteBuffer.wrap(Arrays.copyOfRange(bytes, offset, 20));
        }
        else
        {
            // Size >= 16 means we extract 16 bytes starting from index 4
            var resultBytes = new byte[size];
            System.arraycopy(bytes, 4, resultBytes, 0, 16);
            result = ByteBuffer.wrap(resultBytes);
        }

        return result;
    }


    /* Type comparison to get rid of ReversedType */

    /**
     * Returns <code>true</code> if values of the given {@link AbstractType} should be indexed as literals. This applies
     * to string types, frozen types, composite types, the bytes (blob) type and the boolean type.
     */
    public static boolean isLiteral(AbstractType<?> type)
    {
        return isUTF8OrAscii(type) || isCompositeOrFrozen(type) || isBytes(type) || baseType(type) instanceof BooleanType;
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is the bytes (blob) type.
     */
    public static boolean isBytes(AbstractType<?> type)
    {
        return baseType(type) instanceof BytesType;
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is UTF8 or Ascii
     */
    public static boolean isUTF8OrAscii(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof UTF8Type || type instanceof AsciiType;
    }

//    /**
//     * Returns <code>true</code> if given {@link AbstractType} is based on a string, e.g. UTF8 or Ascii
//     */
//    public static boolean isString(AbstractType<?> type)
//    {
//        type = baseType(type);
//        return type instanceof StringType;
//    }
//
    /**
     * Returns <code>true</code> if given {@link AbstractType} is a Composite(map entry) or frozen.
     */
    public static boolean isCompositeOrFrozen(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof CompositeType || isFrozen(type);
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is frozen.
     */
    public static boolean isFrozen(AbstractType<?> type)
    {
        type = baseType(type);
        return !type.subTypes().isEmpty() && !type.isMultiCell();
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is a frozen collection.
     */
    public static boolean isFrozenCollection(AbstractType<?> type)
    {
        type = baseType(type);
        return type.isCollection() && !type.isMultiCell();
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is a non-frozen collection.
     */
    public static boolean isNonFrozenCollection(AbstractType<?> type)
    {
        type = baseType(type);
        return type.isCollection() && type.isMultiCell();
    }

    /**
     * Returns {@code true} if the values of the type are indexed unreversed even as a DESC clustering column: inet,
     * varint and decimal, whose index encodings are compared as unsigned bytes and written to the balanced tree without
     * the reversal (see {@link #toComparableBytes}), so queries must treat the type as non-reversed. A DESC
     * {@link LongType} column is indexed reversed.
     */
    public static boolean isIndexedUnreversed(AbstractType<?> type)
    {
        return isInetAddress(type) || isBigInteger(type) || isBigDecimal(type);
    }

    /**
     * Returns {@code true} if the given type is a {@link org.apache.cassandra.db.marshal.ReversedType} of
     * {@link DecimalType}, the type of a DESC decimal clustering column.
     */
    public static boolean isReversedDecimal(AbstractType<?> type)
    {
        return type.isReversed() && isBigDecimal(type);
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is included in the types.
     */
    public static boolean isIn(AbstractType<?> type, Set<AbstractType<?>> types)
    {
        type = baseType(type);
        return types.contains(type);
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is {@link InetAddressType}
     */
    private static boolean isInetAddress(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof InetAddressType;
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is {@link IntegerType}
     */
    private static boolean isBigInteger(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof IntegerType;
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is {@link DecimalType}
     */
    private static boolean isBigDecimal(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof DecimalType;
    }

    /**
     * Returns <code>true</code> if given {@link AbstractType} is {@link CompositeType}
     */
    public static boolean isComposite(AbstractType<?> type)
    {
        type = baseType(type);
        return type instanceof CompositeType;
    }

    /**
     * @return {@code true} if the empty values of the given type should be excluded from indexing, {@code false} otherwise.
     * The answer is the same for a reversed type (a {@code DESC} clustering column) as for its base type.
     */
    public static boolean skipsEmptyValue(AbstractType<?> type)
    {
        return !baseType(type).allowsEmpty() || !isLiteral(type);
    }

    /**
     * @return {@code true} if the given value of the given type belongs in the index: a non-null value, which is not
     * empty unless the type indexes empty values (see {@link #skipsEmptyValue(AbstractType)}). Inserts and updates must
     * agree on this, so that an update adds and removes exactly the values an insert would index.
     */
    public static boolean indexesValue(@Nullable ByteBuffer value, AbstractType<?> type)
    {
        return value != null && (value.hasRemaining() || !skipsEmptyValue(type));
    }

    /**
     * @return base type if given type is reversed, otherwise return itself
     */
    private static AbstractType<?> baseType(AbstractType<?> type)
    {
        return type.unwrap();
    }

    public static ByteBuffer encodeDecimal(ByteBuffer value)
    {
        ByteSource bs = DecimalType.instance.asComparableBytes(value, BYTE_COMPARABLE_VERSION);
        byte[] data = new byte[DECIMAL_APPROXIMATION_BYTES];    // initialized with 0s
        bs.nextBytes(data); // reads up to the number of bytes in the array, leaving 0s in the remaining bytes
        return ByteBuffer.wrap(data);
    }

    public static ByteBuffer decodeDecimal(ByteBuffer value)
    {
        var peekableValue = ByteSource.peekable(ByteSource.preencoded(value));
        return DecimalType.instance.fromComparableBytes(peekableValue, BYTE_COMPARABLE_VERSION);
    }

    public static ByteComparable.Version byteComparableVersionForTermsData(Version version)
    {
        return version.byteComparableVersionFor(IndexComponentType.TERMS_DATA, DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion());
    }
}
