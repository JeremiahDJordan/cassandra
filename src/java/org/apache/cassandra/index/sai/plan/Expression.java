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

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * An {@link Expression} is an internal representation of an index query operation. They are built from
 * CQL {@link Operator} and {@link ByteBuffer} value pairs for a single column.
 * <p>
 * Each {@link Expression} consists of an {@link IndexOperator} and optional lower and upper {@link Bound}s.
 * <p>
 * The {@link IndexedExpression} has a backing {@link StorageAttachedIndex} for the index query but order to support
 * CQL expressions on columns that do not have indexes or use operators that are not supported by the index there is
 * an {@link UnindexedExpression} that does not provide a {@link StorageAttachedIndex} and can only be used for
 * post-filtering
 */
public abstract class Expression
{
    Logger logger = LoggerFactory.getLogger(Expression.class);

    private static final int IPV4_ADDRESS_SIZE = 4;
    private static final int IPV6_ADDRESS_SIZE = 16;
    private static final byte[] IPV4_MIN = new byte[]{ 0, 0, 0, 0 };
    private static final byte[] IPV4_MAX = new byte[]{ -1, -1, -1, -1 };

    private final IndexTermType indexTermType;
    protected IndexOperator operator;

    public Bound lower, upper;
    // The upperInclusive and lowerInclusive flags are maintained separately to the inclusive flags
    // in the upper and lower bounds because the upper and lower bounds have their inclusivity relaxed
    // if the datatype being filtered is rounded in the index. These flags are used in the post-filtering
    // process to remove values equal to the bounds.
    public boolean upperInclusive, lowerInclusive;

    Expression(IndexTermType indexTermType)
    {
        this.indexTermType = indexTermType;
    }

    public static Expression create(StorageAttachedIndex index)
    {
        return new IndexedExpression(index);
    }

    public static Expression create(IndexTermType indexTermType)
    {
        return new UnindexedExpression(indexTermType);
    }

    public static boolean supportsOperator(Operator operator)
    {
        return IndexOperator.valueOf(operator) != null;
    }

    public enum IndexOperator
    {
        EQ, RANGE, CONTAINS_KEY, CONTAINS_VALUE, ANN;

        public static IndexOperator valueOf(Operator operator)
        {
            switch (operator)
            {
                case EQ:
                    return EQ;

                case CONTAINS:
                    return CONTAINS_VALUE; // non-frozen map: value contains term;

                case CONTAINS_KEY:
                    return CONTAINS_KEY; // non-frozen map: value contains key term;

                case LT:
                case GT:
                case LTE:
                case GTE:
                    return RANGE;

                case ANN:
                    return ANN;

                default:
                    return null;
            }
        }

        public boolean isEquality()
        {
            return this == EQ || this == CONTAINS_KEY || this == CONTAINS_VALUE;
        }

        public boolean isEqualityOrRange()
        {
            return isEquality() || this == RANGE;
        }
    }

    public abstract boolean isNotIndexed();

    public abstract StorageAttachedIndex getIndex();

    abstract boolean hasAnalyzer();

    abstract AbstractAnalyzer getAnalyzer();

    public IndexOperator getIndexOperator()
    {
        return operator;
    }

    public IndexTermType getIndexTermType()
    {
        return indexTermType;
    }

    public Bound lower()
    {
        return lower;
    }

    public Bound upper()
    {
        return upper;
    }

    /**
     * This adds an operation to the current {@link Expression} instance and
     * returns the current instance.
     *
     * @param op the CQL3 operation
     * @param value the expression value
     * @return the current expression with the added operation
     */
    public Expression add(Operator op, ByteBuffer value)
    {
        boolean lowerInclusive, upperInclusive;
        // If the type supports rounding then we need to make sure that index
        // range search is always inclusive, otherwise we run the risk of
        // missing values that are within the exclusive range but are rejected
        // because their rounded value is the same as the value being queried.
        lowerInclusive = upperInclusive = indexTermType.supportsRounding();
        switch (op)
        {
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
                lower = new Bound(value, indexTermType, true);
                upper = lower;
                operator = IndexOperator.valueOf(op);
                break;

            case LTE:
                if (indexTermType.isReversed())
                {
                    this.lowerInclusive = true;
                    lowerInclusive = true;
                }
                else
                {
                    this.upperInclusive = true;
                    upperInclusive = true;
                }
            case LT:
                operator = IndexOperator.RANGE;
                if (indexTermType.isReversed())
                    lower = new Bound(value, indexTermType, lowerInclusive);
                else
                    upper = new Bound(value, indexTermType, upperInclusive);
                break;

            case GTE:
                if (indexTermType.isReversed())
                {
                    this.upperInclusive = true;
                    upperInclusive = true;
                }
                else
                {
                    this.lowerInclusive = true;
                    lowerInclusive = true;
                }
            case GT:
                operator = IndexOperator.RANGE;
                if (indexTermType.isReversed())
                    upper = new Bound(value, indexTermType, upperInclusive);
                else
                    lower = new Bound(value, indexTermType, lowerInclusive);
                break;
            case ANN:
                operator = IndexOperator.ANN;
                lower = new Bound(value, indexTermType, true);
                upper = lower;
                break;
            default:
                throw new IllegalArgumentException("Index does not support the " + op + " operator");
        }

        return this;
    }

    /**
     * Used in post-filtering to determine is an indexed value matches the expression
     */
    public boolean isSatisfiedBy(ByteBuffer columnValue)
    {
        // If the expression represents an ANN ordering then we return true because the actual result
        // is approximate and will rarely / never match the expression value
        if (indexTermType.isVector())
            return true;

        if (!indexTermType.isValid(columnValue))
        {
            logger.error("Value is not valid for indexed column {} with {}", indexTermType.columnName(), indexTermType.indexType());
            return false;
        }

        Value value = new Value(columnValue, indexTermType);

        if (lower != null)
        {
            // suffix check
            if (indexTermType.isLiteral())
                return validateStringValue(value.raw, lower.value.raw);
            else
            {
                // range or (not-)equals - (mainly) for numeric values
                int cmp = indexTermType.comparePostFilter(lower.value, value);

                // in case of EQ lower == upper
                if (operator == IndexOperator.EQ || operator == IndexOperator.CONTAINS_KEY || operator == IndexOperator.CONTAINS_VALUE)
                    return cmp == 0;

                if (cmp > 0 || (cmp == 0 && !lowerInclusive))
                    return false;
            }
        }

        if (upper != null && lower != upper)
        {
            // string (prefix or suffix) check
            if (indexTermType.isLiteral())
                return validateStringValue(value.raw, upper.value.raw);
            else
            {
                // range - mainly for numeric values
                int cmp = indexTermType.comparePostFilter(upper.value, value);
                return (cmp > 0 || (cmp == 0 && upperInclusive));
            }
        }

        return true;
    }

    /**
     * Returns the expressions to search the index with for this expression: this expression, except for a range on an
     * inet column. The index orders the addresses in their 16-byte form, in which an IPv4 address is its IPv4-mapped
     * IPv6 address, while the column type orders the 4-byte and the 16-byte values by their bytes. The 4-byte values
     * and the 16-byte values of a range are each a range of index terms, which hold the values of the other form with
     * the same term too, for post-filtering to reject.
     *
     * @return the ranges of index terms that hold the values this expression matches, none if it matches nothing
     */
    public List<Expression> searchedRanges()
    {
        if (operator != IndexOperator.RANGE || !indexTermType.isInetAddress())
            return Collections.singletonList(this);

        Expression ipv4 = inetRange(IPV4_ADDRESS_SIZE);
        Expression ipv6 = inetRange(IPV6_ADDRESS_SIZE);
        if (ipv4 == null)
            return ipv6 == null ? Collections.emptyList() : Collections.singletonList(ipv6);
        if (ipv6 == null)
            return Collections.singletonList(ipv4);

        // the terms of the IPv4 addresses are contiguous, and often in the range of the 16-byte values
        if (isBelow(ipv6.upper, ipv4.lower) || isBelow(ipv4.upper, ipv6.lower))
            return List.of(ipv4, ipv6);
        return Collections.singletonList(inetRange(lowest(ipv4.lower, ipv6.lower), highest(ipv4.upper, ipv6.upper)));
    }

    /**
     * Returns the range of this inet range's values of the given size, or {@code null} if it has none.
     */
    private Expression inetRange(int size)
    {
        // an empty value sorts before every address, and the index doesn't hold it
        if (upper != null && !upper.value.raw.hasRemaining())
            return null;
        Bound lowerBound = inetBound(lower != null && lower.value.raw.hasRemaining() ? lower : null, size, true);
        Bound upperBound = inetBound(upper, size, false);
        if (lowerBound != null && upperBound != null)
        {
            int cmp = ByteBufferUtil.compareUnsigned(lowerBound.value.raw, upperBound.value.raw);
            if (cmp > 0 || (cmp == 0 && !(lowerBound.inclusive && upperBound.inclusive)))
                return null;
        }

        return inetRange(lowerBound, upperBound);
    }

    private Expression inetRange(Bound lowerBound, Bound upperBound)
    {
        Expression result = create(getIndex());
        result.operator = IndexOperator.RANGE;
        result.lower = lowerBound;
        result.upper = upperBound;
        result.lowerInclusive = lowerBound != null && lowerBound.inclusive;
        result.upperInclusive = upperBound != null && upperBound.inclusive;
        return result;
    }

    /**
     * Returns the bound of the values of the given size that is equivalent to the given bound in the column type's
     * order, which sorts a value before the longer values that start with it.
     */
    private Bound inetBound(Bound bound, int size, boolean isLower)
    {
        // the 4-byte values are bounded by the smallest and greatest IPv4 addresses, so that their range of terms
        // holds no other term
        if (bound == null)
            return size == IPV4_ADDRESS_SIZE ? new Bound(ByteBuffer.wrap(isLower ? IPV4_MIN : IPV4_MAX), indexTermType, true) : null;

        ByteBuffer value = bound.value.raw;
        if (value.remaining() == size)
            return bound;

        ByteBuffer converted = ByteBuffer.allocate(size);
        ByteBufferUtil.copyBytes(value, value.position(), converted, 0, IPV4_ADDRESS_SIZE);
        // A 16-byte value sorts after the 4-byte values up to its first 4 bytes and before the greater ones, and a
        // 4-byte value sorts before the 16-byte values that start with it and after the smaller ones.
        return new Bound(converted, indexTermType, size == IPV4_ADDRESS_SIZE ? !isLower : isLower);
    }

    /**
     * Whether the terms up to the upper bound all sort before the terms from the lower bound.
     */
    private boolean isBelow(Bound upperBound, Bound lowerBound)
    {
        if (upperBound == null || lowerBound == null)
            return false;
        int cmp = compareTerms(upperBound, lowerBound);
        return cmp < 0 || (cmp == 0 && !(upperBound.inclusive && lowerBound.inclusive));
    }

    private Bound lowest(Bound a, Bound b)
    {
        if (a == null || b == null)
            return null;
        int cmp = compareTerms(a, b);
        return cmp < 0 || (cmp == 0 && a.inclusive) ? a : b;
    }

    private Bound highest(Bound a, Bound b)
    {
        if (a == null || b == null)
            return null;
        int cmp = compareTerms(a, b);
        return cmp > 0 || (cmp == 0 && a.inclusive) ? a : b;
    }

    private int compareTerms(Bound a, Bound b)
    {
        return indexTermType.compare(a.value.encoded, b.value.encoded);
    }

    private boolean validateStringValue(ByteBuffer columnValue, ByteBuffer requestedValue)
    {
        if (hasAnalyzer())
        {
            AbstractAnalyzer analyzer = getAnalyzer();
            analyzer.reset(columnValue.duplicate());
            try
            {
                while (analyzer.hasNext())
                {
                    if (termMatches(analyzer.next(), requestedValue))
                        return true;
                }
                return false;
            }
            finally
            {
                analyzer.end();
            }
        }
        else
        {
            return termMatches(columnValue, requestedValue);
        }
    }

    private boolean termMatches(ByteBuffer term, ByteBuffer requestedValue)
    {
        boolean isMatch = false;
        switch (operator)
        {
            case EQ:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
                isMatch = indexTermType.compare(term, requestedValue) == 0;
                break;
            case RANGE:
                isMatch = isLowerSatisfiedBy(term) && isUpperSatisfiedBy(term);
                break;
        }
        return isMatch;
    }

    private boolean hasLower()
    {
        return lower != null;
    }

    private boolean hasUpper()
    {
        return upper != null;
    }

    private boolean isLowerSatisfiedBy(ByteBuffer value)
    {
        if (!hasLower())
            return true;

        int cmp = indexTermType.indexType().compare(value, lower.value.raw);
        return cmp > 0 || cmp == 0 && lower.inclusive;
    }

    private boolean isUpperSatisfiedBy(ByteBuffer value)
    {
        if (!hasUpper())
            return true;

        int cmp = indexTermType.indexType().compare(value, upper.value.raw);
        return cmp < 0 || cmp == 0 && upper.inclusive;
    }

    @Override
    public String toString()
    {
        return String.format("Expression{name: %s, op: %s, lower: (%s, %s), upper: (%s, %s)}",
                             indexTermType.columnName(),
                             operator,
                             lower == null ? "null" : indexTermType.asString(lower.value.raw),
                             lower != null && lower.inclusive,
                             upper == null ? "null" : indexTermType.asString(upper.value.raw),
                             upper != null && upper.inclusive);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder().append(indexTermType)
                                    .append(operator)
                                    .append(lower).append(upper).build();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Expression))
            return false;

        if (this == other)
            return true;

        Expression o = (Expression) other;

        return Objects.equals(indexTermType, o.indexTermType)
               && operator == o.operator
               && Objects.equals(lower, o.lower)
               && Objects.equals(upper, o.upper);
    }

    public static class IndexedExpression extends Expression
    {
        private final StorageAttachedIndex index;

        public IndexedExpression(StorageAttachedIndex index)
        {
            super(index.termType());
            this.index = index;
        }

        @Override
        public boolean isNotIndexed()
        {
            return false;
        }

        @Override
        public StorageAttachedIndex getIndex()
        {
            return index;
        }

        @Override
        boolean hasAnalyzer()
        {
            return index.hasAnalyzer();
        }

        @Override
        AbstractAnalyzer getAnalyzer()
        {
            return index.analyzer();
        }
    }

    public static class UnindexedExpression extends Expression
    {
        private UnindexedExpression(IndexTermType indexTermType)
        {
            super(indexTermType);
        }

        @Override
        public boolean isNotIndexed()
        {
            return true;
        }

        @Override
        public StorageAttachedIndex getIndex()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        boolean hasAnalyzer()
        {
            return false;
        }

        @Override
        AbstractAnalyzer getAnalyzer()
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A representation of a column value in its raw and encoded form.
     */
    public static class Value
    {
        public final ByteBuffer raw;
        public final ByteBuffer encoded;

        public Value(ByteBuffer value, IndexTermType indexTermType)
        {
            this.raw = value;
            this.encoded = indexTermType.asIndexBytes(value);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Value))
                return false;

            Value o = (Value) other;
            return raw.equals(o.raw) && encoded.equals(o.encoded);
        }

        @Override
        public int hashCode()
        {
            HashCodeBuilder builder = new HashCodeBuilder();
            builder.append(raw);
            builder.append(encoded);
            return builder.toHashCode();
        }
    }

    public static class Bound
    {
        public final Value value;
        public final boolean inclusive;

        public Bound(ByteBuffer value, IndexTermType indexTermType, boolean inclusive)
        {
            this.value = new Value(value, indexTermType);
            this.inclusive = inclusive;
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Bound))
                return false;

            Bound o = (Bound) other;
            return value.equals(o.value) && inclusive == o.inclusive;
        }

        @Override
        public int hashCode()
        {
            HashCodeBuilder builder = new HashCodeBuilder();
            builder.append(value);
            builder.append(inclusive);
            return builder.toHashCode();
        }
    }
}
