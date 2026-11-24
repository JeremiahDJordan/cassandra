/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Redaction;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.AutomatonQueries;
import org.apache.cassandra.index.sai.utils.GeoUtil;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.automaton.CompiledAutomaton;

public class Expression
{
    private static final Logger logger = LoggerFactory.getLogger(Expression.class);

    private static final int IPV4_ADDRESS_SIZE = 4;
    private static final byte[] IPV4_MIN = new byte[]{ 0, 0, 0, 0 };
    private static final byte[] IPV4_MAX = new byte[]{ -1, -1, -1, -1 };

    public enum Op
    {
        EQ, MATCH, PREFIX, NOT_EQ, RANGE,
        CONTAINS_KEY, CONTAINS_VALUE,
        NOT_CONTAINS_VALUE, NOT_CONTAINS_KEY,
        IN, ORDER_BY, BOUNDED_ANN, AUTOMATON;

        public static Op valueOf(Operator operator)
        {
            switch (operator)
            {
                case EQ:
                    return EQ;

                case NEQ:
                    return NOT_EQ;

                case CONTAINS:
                    return CONTAINS_VALUE; // non-frozen map: value contains term;

                case CONTAINS_KEY:
                    return CONTAINS_KEY; // non-frozen map: value contains key term;

                case NOT_CONTAINS:
                    return NOT_CONTAINS_VALUE;

                case NOT_CONTAINS_KEY:
                    return NOT_CONTAINS_KEY;

                case LT:
                case GT:
                case LTE:
                case GTE:
                    return RANGE;

                case LIKE_PREFIX:
                    return PREFIX;

                // Automaton-served pattern matching: the expression carries the raw pattern (see Expression#add)
                // and lazily compiles it into a Lucene CompiledAutomaton (see Expression#getAutomaton)
                // intersected with the terms dictionaries. LIKE_PREFIX stays on its dedicated bounded-range
                // PREFIX path above.
                case LIKE_SUFFIX:
                case LIKE_CONTAINS:
                case LIKE_MATCHES:
                    return AUTOMATON;

                case ANALYZER_MATCHES:
                    return MATCH;

                case IN:
                    return IN;

                case ANN:
                case BM25:
                case ORDER_BY_ASC:
                case ORDER_BY_DESC:
                    return ORDER_BY;

                case BOUNDED_ANN:
                    return BOUNDED_ANN;

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

        public boolean isNonEquality()
        {
            return this == NOT_EQ || this == NOT_CONTAINS_KEY || this == NOT_CONTAINS_VALUE;
        }

        public boolean isContains()
        {
            return this == CONTAINS_KEY
                   || this == CONTAINS_VALUE
                   || this == NOT_CONTAINS_KEY
                   || this == NOT_CONTAINS_VALUE;
        }
    }

    public final AbstractAnalyzer.AnalyzerFactory analyzerFactory;

    public final IndexContext context;
    public final AbstractType<?> validator;

    // whether the bounds are swapped: a DESC clustering column indexed reversed (see TypeUtil#isIndexedUnreversed)
    private final boolean reversed;

    @VisibleForTesting
    protected Op operation;

    public Bound lower, upper;

    /**
     * For {@link Op#AUTOMATON} expressions, the pattern operator ({@link Operator#LIKE_SUFFIX},
     * {@link Operator#LIKE_CONTAINS} or {@link Operator#LIKE_MATCHES} with wildcards) the pattern in {@link #lower}
     * came from; it defines the pattern syntax. {@code null} for all other operations.
     */
    private Operator patternOperator;

    /**
     * The lazily compiled automaton of an {@link Op#AUTOMATON} expression. Plan expressions are not serialized
     * (only the {@code RowFilter} expression carrying the raw pattern is), so the automaton is compiled here,
     * at most once per query per index, and shared by all the segments/memtable shards the expression is
     * intersected with. The searches of a query run on the thread owning its query context, so no synchronization
     * is needed; the field is volatile only for safe publication, and compilation is idempotent, so a race would
     * only cost a recompile.
     */
    private volatile CompiledAutomaton automaton;

    /**
     * The byte prefix shared by all the terms accepted by {@link #automaton}, computed together with it (see
     * {@link #getAutomatonCommonPrefix()}) so that it isn't recomputed for every segment, memtable shard and estimate.
     */
    private volatile byte[] automatonCommonPrefix;

    private float boundedAnnEuclideanDistanceThreshold = 0;
    private float searchRadiusMeters = 0;
    private float searchRadiusDegreesSquared = 0;
    public int topK;
    // These variables are only meant to be used for final validation of the range search. They are not
    // meant to be used when searching the index. See the 'add' method below for additional explanation.
    private boolean upperInclusive, lowerInclusive;

    final List<ByteBuffer> exclusions = new ArrayList<>();

    // The terms of an analyzed CONTAINS, CONTAINS KEY, NOT CONTAINS or NOT CONTAINS KEY value of several terms, of
    // which the lower bound is one: an element or key holds the value if it holds all of them (see #addAllTerms)
    private List<ByteBuffer> allTerms;

    // The map entry comparison this expression filters with when no index scan serves it, without bounds: its key or
    // value can be too large for a CompositeType component
    RowFilter.MapComparisonExpression unindexedMapEntry;

    public Expression(IndexContext indexContext)
    {
        this.context = indexContext;
        this.analyzerFactory = indexContext.getAnalyzerFactory();
        this.validator = indexContext.getValidator();
        this.reversed = indexContext.getDefinition() != null
                        && indexContext.getDefinition().isReversedType()
                        && !TypeUtil.isIndexedUnreversed(validator);
    }

    /**
     * @return an expression that filters the rows with the given map entry comparison, on a column that no index scan
     * serves
     */
    static Expression unindexedMapEntry(IndexContext indexContext, RowFilter.MapComparisonExpression mapEntry)
    {
        Expression expression = new Expression(indexContext);
        expression.operation = Op.valueOf(mapEntry.operator());
        expression.unindexedMapEntry = mapEntry;
        return expression;
    }

    public boolean isLiteral()
    {
        return context.isLiteral();
    }

    public Expression add(Operator op, ByteBuffer value)
    {
        boolean lowerInclusive, upperInclusive;
        // If the type supports rounding then we need to make sure that index
        // range search is always inclusive, otherwise we run the risk of
        // missing values that are within the exclusive range but are rejected
        // because their rounded value is the same as the value being queried.
        lowerInclusive = upperInclusive = TypeUtil.supportsRounding(validator);
        switch (op)
        {
            case LIKE_PREFIX:
            case LIKE_SUFFIX:
            case LIKE_CONTAINS:
            case LIKE_MATCHES:
            case ANALYZER_MATCHES:
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
            case NOT_CONTAINS:
            case NOT_CONTAINS_KEY:
                // A wildcard-free LIKE '<term>' (LIKE_MATCHES without any wildcard) matches the single term equal
                // to its unescaped literal, so the index serves it exactly like an equality (term lookup, no
                // ordering scan): the automaton path is for the patterns with wildcards. The CQL operator stays
                // LIKE_MATCHES, which is what index selection and post-filtering are about.
                if (op == Operator.LIKE_MATCHES && !LikePatterns.hasWildcard(value))
                {
                    operation = Op.EQ;
                    value = LikePatterns.unescape(value);
                }
                else
                {
                    operation = Op.valueOf(op);
                }
                lower = new Bound(value, validator, true);
                upper = lower;
                // Automaton-served pattern expressions remember which operator the pattern came from, because
                // it defines the pattern syntax (see #getAutomaton()).
                if (operation == Op.AUTOMATON)
                    patternOperator = op;
                break;

            case NEQ:
                // index expressions are priority sorted
                // and NOT_EQ is the lowest priority, which means that operation type
                // is always going to be set before reaching it in case of RANGE or EQ.
                if (operation == null)
                {
                    operation = Op.NOT_EQ;
                    lower = new Bound(value, validator, true);
                    upper = lower;
                }
                else
                    exclusions.add(value);
                break;

            case LTE:
                if (reversed)
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
                operation = Op.RANGE;
                if (reversed)
                    lower = new Bound(value, validator, lowerInclusive);
                else
                    upper = new Bound(value, validator, upperInclusive);
                break;

            case GTE:
                if (reversed)
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
                operation = Op.RANGE;
                if (reversed)
                    upper = new Bound(value, validator,  upperInclusive);
                else
                    lower = new Bound(value, validator, lowerInclusive);
                break;
            case BOUNDED_ANN:
                operation = Op.BOUNDED_ANN;
                lower = new Bound(value, validator, true);
                assert upper != null;
                searchRadiusMeters = FloatType.instance.compose(upper.value.raw);
                boundedAnnEuclideanDistanceThreshold = GeoUtil.amplifiedEuclideanSimilarityThreshold(lower.value.vector, searchRadiusMeters);
                break;
            case ANN:
            case BM25:
            case ORDER_BY_ASC:
            case ORDER_BY_DESC:
                // If we alread have an operation on the column, we don't need to set the ORDER_BY op because
                // it is only used to force validation on a column, and the presence of another operation will do that.
                if (operation == null)
                    operation = Op.ORDER_BY;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + op);
        }

        assert operation != null;

        return this;
    }

    // VSTODO seems like we could optimize for CompositeType here since we know we have a key match
    public boolean isSatisfiedBy(ByteBuffer columnValue)
    {
        if (columnValue == null)
            return false;

        // ORDER_BY is not indepently verifiable, so we always return true
        if (operation == Op.ORDER_BY)
            return true;

        if (!TypeUtil.isValid(columnValue, validator))
        {
            logger.error(context.logMessage("Value is not valid for indexed column {} with {}"), context.getColumnName(), validator);
            return false;
        }

        // The encoded form of a literal value isn't used to check it (validateStringValue and comparePostFilter compare
        // literal values as they are), and can be costly to compute (the canonical form of a frozen value). Vectors,
        // which TypeUtil.isLiteral takes for frozen values, need their decomposed vector.
        Value value = TypeUtil.isLiteral(validator) && !validator.isVector()
                      ? new Value(columnValue, columnValue, null)
                      : new Value(columnValue, validator);

        if (operation == Op.BOUNDED_ANN)
        {
            double haversineDistance = SloppyMath.haversinMeters(lower.value.vector[0], lower.value.vector[1], value.vector[0], value.vector[1]);
            return upperInclusive ? haversineDistance <= searchRadiusMeters : haversineDistance < searchRadiusMeters;
        }

        if (lower != null)
        {
            // suffix check
            if (TypeUtil.isLiteral(validator))
            {
                if (!validateStringValue(value.raw, lower.value.raw))
                    return false;
            }
            else
            {
                // range or (not-)equals - (mainly) for numeric values
                int cmp = TypeUtil.comparePostFilter(lower.value, value, validator);

                // in case of (NOT_)EQ lower == upper
                if (operation == Op.EQ || operation == Op.CONTAINS_KEY || operation == Op.CONTAINS_VALUE)
                    return cmp == 0;

                if (operation == Op.NOT_EQ || operation == Op.NOT_CONTAINS_KEY || operation == Op.NOT_CONTAINS_VALUE)
                    return cmp != 0;

                if (cmp > 0 || (cmp == 0 && !lowerInclusive))
                    return false;
            }
        }

        if (upper != null && lower != upper)
        {
            // string (prefix or suffix) check
            if (TypeUtil.isLiteral(validator))
            {
                if (!validateStringValue(value.raw, upper.value.raw))
                    return false;
            }
            else
            {
                // range - mainly for numeric values
                int cmp = TypeUtil.comparePostFilter(upper.value, value, validator);
                if (cmp < 0 || (cmp == 0 && !upperInclusive))
                    return false;
            }
        }

        // as a last step let's check exclusions for the given field,
        // this covers EQ/RANGE with exclusions.
        for (ByteBuffer term : exclusions)
        {
            if (TypeUtil.isLiteral(validator) ? holdsTerm(value.raw, term)
                                              : TypeUtil.comparePostFilter(new Value(term, validator), value, validator) == 0)
                return false;
        }

        return true;
    }

    /**
     * @return whether the bounds are reversed: for a DESC clustering column indexed reversed (see
     * {@link TypeUtil#isIndexedUnreversed(AbstractType)}), the lower bound holds the greatest value of the range
     */
    public boolean hasReversedBounds()
    {
        return reversed;
    }

    /**
     * Returns the lower bound of the expression as a ByteComparable with an encoding based on the version and the
     * validator.
     * @param version the version of the index
     * @return
     */
    public ByteComparable getEncodedLowerBoundByteComparable(Version version)
    {
        // Note: this value was encoded using the TypeUtil.encode method, but it wasn't
        var bound = getPartiallyEncodedLowerBound(version);
        if (bound == null)
            return null;
        // If the lower bound is inclusive, we use the LT_NEXT_COMPONENT terminator to make sure the bound is not a
        // prefix of some other key. This ensures reverse iteration works correctly too.
        var terminator = lower.inclusive ? ByteSource.LT_NEXT_COMPONENT : ByteSource.GT_NEXT_COMPONENT;
        return getBoundByteComparable(bound, version, terminator);
    }

    /**
     * Returns the upper bound of the expression as a ByteComparable with an encoding based on the version and the
     * validator.
     * @param version the version of the index
     * @return
     */
    public ByteComparable getEncodedUpperBoundByteComparable(Version version)
    {
        var bound = getPartiallyEncodedUpperBound(version);
        if (bound == null)
            return null;
        // If the upper bound is inclusive, we use the LT_NEXT_COMPONENT terminator to make sure the bound is not a
        // prefix of some other key. This ensures reverse iteration works correctly too.
        var terminator = upper.inclusive ? ByteSource.GT_NEXT_COMPONENT : ByteSource.LT_NEXT_COMPONENT;
        return getBoundByteComparable(bound, version, terminator);
    }

    /**
     * For a {@link Op#PREFIX} expression, returns the smallest {@link ByteComparable} that sorts after every term
     * starting with the prefix, encoded for the trie of the given version, or {@code null} if no such bound exists
     * (i.e. the prefix is empty or consists solely of {@code 0xFF} bytes). Together with the prefix itself as an
     * inclusive lower bound, {@code [prefix, nextOf(prefix))} delimits exactly the terms starting with the prefix.
     * <p>
     * This is only valid for non-composite literal types, for which
     * {@link org.apache.cassandra.index.sai.disk.format.OnDiskFormat#encodeForTrie} preserves the raw byte order
     * of the terms.
     *
     * @param version the version of the index
     * @return the exclusive upper bound of the prefix range, or {@code null} if it is unbounded
     */
    public ByteComparable getPrefixUpperBoundByteComparable(Version version)
    {
        assert operation == Op.PREFIX : "Prefix upper bound requested for operation " + operation;
        byte[] upperBytes = AutomatonQueries.prefixUpperBound(ByteBufferUtil.getArray(lower.value.encoded));
        return upperBytes == null ? null : version.onDiskFormat().encodeForTrie(ByteBuffer.wrap(upperBytes), validator);
    }

    /**
     * For {@link Op#AUTOMATON} expressions, returns the pattern operator the expression was built from.
     */
    public Operator getPatternOperator()
    {
        assert operation == Op.AUTOMATON : "Pattern operator requested for operation " + operation;
        return patternOperator;
    }

    /**
     * For {@link Op#AUTOMATON} expressions, returns the compiled byte-level automaton of the pattern carried by
     * this expression, compiling it on first use. The pattern value in {@link #lower} is the (possibly
     * normalized, e.g. lowercased) pattern produced by the analysis in {@code Operation#analyzeGroup}, matching
     * how the stored terms are normalized.
     *
     * @throws org.apache.cassandra.exceptions.InvalidPatternException if the pattern is invalid or too complex
     */
    public CompiledAutomaton getAutomaton()
    {
        assert operation == Op.AUTOMATON : "Automaton requested for operation " + operation;
        CompiledAutomaton compiled = automaton;
        if (compiled == null)
            automaton = compiled = LikePatterns.forPatternOperator(patternOperator, lower.value.raw, context.getColumnName());
        return compiled;
    }

    /**
     * For {@link Op#AUTOMATON} expressions, returns the byte prefix shared by all the terms accepted by
     * {@link #getAutomaton()} (see {@link AutomatonQueries#commonPrefixBytes}), computed on first use. The returned
     * array is shared and must not be modified.
     */
    public byte[] getAutomatonCommonPrefix()
    {
        byte[] prefix = automatonCommonPrefix;
        if (prefix == null)
            automatonCommonPrefix = prefix = AutomatonQueries.commonPrefixBytes(getAutomaton());
        return prefix;
    }

    // This call encodes the byte buffer into a ByteComparable object based on the version of the index, the validator,
    // and whether the expression is in memory or on disk.
    private ByteComparable getBoundByteComparable(ByteBuffer unencodedBound, Version version, int terminator)
    {
        if (TypeUtil.isComposite(validator) && version.onOrAfter(Version.DB))
            // Note that for ranges that have one unrestricted bound, we technically do not need the terminator
            // because we use the 0 or the 1 at the end of the first component as the bound. However, it works
            // with the terminator, so we use it for simplicity.
            return TypeUtil.asComparableBytes(unencodedBound, terminator, (CompositeType) validator);
        else
            return version.onDiskFormat().encodeForTrie(unencodedBound, validator);
    }

    /**
     * This is partially encoded because it uses the {@link TypeUtil#encode(ByteBuffer, AbstractType)} method on the
     * {@link ByteBuffer}, but it does not apply the validator's encoding. We do this because we apply
     * {@link TypeUtil#encode(ByteBuffer, AbstractType)} before we find the min/max on an index and this method is
     * exposed publicly for determining if a bound is within an index's min/max.
     * @param version
     * @return
     */
    public ByteBuffer getPartiallyEncodedLowerBound(Version version)
    {
        return getBound(lower, true, version);
    }

    /**
     * This is partially encoded because it uses the {@link TypeUtil#encode(ByteBuffer, AbstractType)} method on the
     * {@link ByteBuffer}, but it does not apply the validator's encoding. We do this because we apply
     * {@link TypeUtil#encode(ByteBuffer, AbstractType)} before we find the min/max on an index and this method is
     * exposed publicly for determining if a bound is within an index's min/max.
     * @param version
     * @return
     */
    public ByteBuffer getPartiallyEncodedUpperBound(Version version)
    {
        return getBound(upper, false, version);
    }

    private ByteBuffer getBound(Bound bound, boolean isLowerBound, Version version)
    {
        if (bound == null)
            return null;
        // Composite types are currently only used in maps.
        // Before DB, we need to extract the first component of the composite type to use as the trie search prefix.
        // After DB, we can use the encoded value directly because the trie is encoded in order so the range
        // correctly gets all relevant values.
        if (!version.onOrAfter(Version.DB) && validator instanceof CompositeType)
            return CompositeType.extractFirstComponentAsTrieSearchPrefix(bound.value.encoded, isLowerBound);
        return bound.value.encoded;
    }

    public boolean isSatisfiedBy(Iterator<ByteBuffer> values)
    {
        if (values == null)
            values = Collections.emptyIterator();

        boolean success = operation.isNonEquality();
        while (values.hasNext())
        {
            ByteBuffer v = values.next();
            if (isSatisfiedBy(v) ^ success)
                return !success;
        }
        return success;
    }

    /**
     * Whether one of the terms of the given literal value is the given term, whatever the operation of this expression.
     */
    private boolean holdsTerm(ByteBuffer columnValue, ByteBuffer term)
    {
        AbstractAnalyzer analyzer = analyzerFactory.create();
        analyzer.reset(columnValue);
        try
        {
            while (analyzer.hasNext())
            {
                if (validator.compare(analyzer.next(), term) == 0)
                    return true;
            }
            return false;
        }
        finally
        {
            analyzer.end();
        }
    }

    private boolean validateStringValue(ByteBuffer columnValue, ByteBuffer requestedValue)
    {
        if (allTerms != null)
            return holdsAllTerms(columnValue) != operation.isNonEquality();

        AbstractAnalyzer analyzer = analyzerFactory.create();
        analyzer.reset(columnValue);
        try
        {
            while (analyzer.hasNext())
            {
                final ByteBuffer term = analyzer.next();

                boolean isMatch = false;
                switch (operation)
                {
                    case EQ:
                    case MATCH:
                        // Operation.isSatisfiedBy handles conclusion on !=,
                        // here we just need to make sure that term matched it
                    case CONTAINS_KEY:
                    case CONTAINS_VALUE:
                        isMatch = validator.compare(term, requestedValue) == 0;
                        break;
                    case NOT_EQ:
                    case NOT_CONTAINS_KEY:
                    case NOT_CONTAINS_VALUE:
                        // A value satisfies a negated operation if none of its terms is the requested value: a term
                        // that differs from it doesn't make up for another that matches it (an analyzed value has
                        // several terms)
                        if (validator.compare(term, requestedValue) == 0)
                            return false;
                        break;
                    case RANGE:
                        isMatch = isLowerSatisfiedBy(term) && isUpperSatisfiedBy(term);
                        break;

                    case PREFIX:
                        isMatch = ByteBufferUtil.startsWith(term, requestedValue);
                        break;

                    case AUTOMATON:
                        // pattern matching over the (possibly normalized) term: a suffix or contains literal by its
                        // bytes, as its automaton may not compile (see StorageAttachedIndexQueryPlan#validatePatterns),
                        // any other pattern with the automaton of the index searchers
                        isMatch = patternOperator == Operator.LIKE_MATCHES
                                  ? LikePatterns.accepts(getAutomaton(), term)
                                  : patternOperator.isSatisfiedBy(validator, term, requestedValue);
                        break;
                }

                if (isMatch)
                    return true;
            }
            // No term matched: this satisfies a negated operation, and no other
            return operation.isNonEquality();
        }
        finally
        {
            analyzer.end();
        }
    }

    /**
     * Makes this expression of an analyzed value of several terms (CONTAINS, CONTAINS KEY, NOT CONTAINS or
     * NOT CONTAINS KEY) about all the given terms together: an element or key holds the value if it holds all of
     * them, as the coordinator's recheck decides (see {@code RowFilter.SimpleExpression#contains}). CONTAINS keeps one
     * such expression per term, NOT CONTAINS a single one (see Operation).
     */
    public Expression addAllTerms(List<ByteBuffer> terms)
    {
        assert operation == Op.NOT_CONTAINS_VALUE || operation == Op.NOT_CONTAINS_KEY
               || operation == Op.CONTAINS_VALUE || operation == Op.CONTAINS_KEY : operation;
        assert terms.contains(lower.value.raw);
        allTerms = terms;
        return this;
    }

    /**
     * @return whether the terms of the given value include all of {@link #allTerms}
     */
    private boolean holdsAllTerms(ByteBuffer columnValue)
    {
        List<ByteBuffer> terms = new ArrayList<>();
        AbstractAnalyzer analyzer = analyzerFactory.create();
        analyzer.reset(columnValue);
        try
        {
            while (analyzer.hasNext())
                terms.add(analyzer.next());
        }
        finally
        {
            analyzer.end();
        }

        for (ByteBuffer required : allTerms)
        {
            boolean found = false;
            for (ByteBuffer term : terms)
            {
                if (validator.compare(term, required) == 0)
                {
                    found = true;
                    break;
                }
            }
            if (!found)
                return false;
        }
        return true;
    }

    public Op getOp()
    {
        return operation;
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

        int cmp = validator.compare(value, lower.value.raw);
        return cmp > 0 || cmp == 0 && lower.inclusive;
    }

    private boolean isUpperSatisfiedBy(ByteBuffer value)
    {
        if (!hasUpper())
            return true;

        int cmp = validator.compare(value, upper.value.raw);
        return cmp < 0 || cmp == 0 && upper.inclusive;
    }

    public float getEuclideanSearchThreshold()
    {
        return boundedAnnEuclideanDistanceThreshold;
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean redact)
    {
        return String.format("Expression{name: %s, op: %s, lower: (%s, %s), upper: (%s, %s), exclusions: %s%s}",
                             context.getColumnName(),
                             patternOperator == null ? operation : operation + " " + patternOperator,
                             lower == null ? "null" : validator.getString(lower.value.raw, redact ? Redaction.REDACT : Redaction.NONE),
                             lower != null && lower.inclusive,
                             upper == null ? "null" : validator.getString(upper.value.raw, redact ? Redaction.REDACT : Redaction.NONE),
                             upper != null && upper.inclusive,
                             Iterators.toString(Iterators.transform(exclusions.iterator(), x -> validator.getString(x, redact ? Redaction.REDACT : Redaction.NONE))),
                             allTerms == null ? "" : ", all terms: " + Iterators.toString(Iterators.transform(allTerms.iterator(), x -> validator.getString(x, redact ? Redaction.REDACT : Redaction.NONE))));
    }

    public String getIndexName()
    {
        return context.getIndexName();
    }

    public int hashCode()
    {
        return new HashCodeBuilder().append(context.getColumnName())
                                    .append(operation)
                                    .append(patternOperator)
                                    .append(validator)
                                    .append(lower).append(upper)
                                    .append(exclusions)
                                    .append(allTerms)
                                    .append(unindexedMapEntry).build();
    }

    public boolean equals(Object other)
    {
        if (!(other instanceof Expression))
            return false;

        if (this == other)
            return true;

        Expression o = (Expression) other;

        return Objects.equals(context.getColumnName(), o.context.getColumnName())
                && validator.equals(o.validator)
                && operation == o.operation
                && patternOperator == o.patternOperator
                && Objects.equals(lower, o.lower)
                && Objects.equals(upper, o.upper)
                && exclusions.equals(o.exclusions)
                && Objects.equals(allTerms, o.allTerms)
                && Objects.equals(unindexedMapEntry, o.unindexedMapEntry);
    }

    /**
     * Whether a bound of this expression is an empty value that the index doesn't store. Indexes on non-literal types
     * don't index empty values (see {@link TypeUtil#skipsEmptyValue}, CASSANDRA-20313), and their index structures
     * can't encode an empty term, so such an expression must not be searched as it is.
     * See {@link #withoutUnindexedEmptyBounds()}.
     */
    public boolean hasUnindexedEmptyValue()
    {
        return isUnindexedEmpty(lower) || isUnindexedEmpty(upper);
    }

    private boolean isUnindexedEmpty(Bound bound)
    {
        return bound != null && !bound.value.raw.hasRemaining() && TypeUtil.skipsEmptyValue(validator);
    }

    /**
     * Returns the expression to search the index with for an equality or a range with an empty value the index
     * doesn't store (see {@link #hasUnindexedEmptyValue()}). An empty value sorts first, so an equality on it matches
     * nothing, an empty lower bound is dropped and an empty upper bound matches nothing. Rows whose value is empty
     * aren't found.
     *
     * @return {@code null} if nothing can match, the expression without its empty lower bound, or this expression
     */
    public Expression withoutUnindexedEmptyBounds()
    {
        if (!hasUnindexedEmptyValue())
            return this;

        switch (operation)
        {
            case EQ:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
                return null;
            case RANGE:
                // The bounds are in the order of the index, which is the reverse of the order of the column's values
                // for a descending clustering column whose type isn't indexed unreversed (see #reversed and #add).
                if (isUnindexedEmpty(reversed ? lower : upper))
                    return null;

                Expression result = new Expression(context);
                result.operation = operation;
                result.lower = reversed ? lower : null;
                result.upper = reversed ? null : upper;
                result.lowerInclusive = lowerInclusive;
                result.upperInclusive = upperInclusive;
                result.exclusions.addAll(exclusions);
                return result;
            default:
                return this;
        }
    }

    /**
     * For an expression on map entries, a frozen type or a varint (see {@link TypeUtil#hasCanonicalIndexTerms}), the
     * same expression with its bounds as written (see {@link TypeUtil#asIndexBytesAsWritten}), to also find the
     * values an index holds as written. Returns {@code null} if the bounds are already in that form, if they are in
     * the wrong order in that form, or for a non-equality expression.
     */
    public Expression withUncanonicalizedBounds()
    {
        if (!TypeUtil.hasCanonicalIndexTerms(validator) || operation == null || operation.isNonEquality())
            return null;

        Bound uncanonicalizedLower = lower == null ? null : lower.uncanonicalized(validator);
        Bound uncanonicalizedUpper = upper == null ? null
                                                   : upper == lower ? uncanonicalizedLower
                                                                    : upper.uncanonicalized(validator);
        if (uncanonicalizedLower == lower && uncanonicalizedUpper == upper)
            return null;

        // A range whose bounds as written are in the wrong order (vi > 0x000001 AND vi <= 255: as written, the lower
        // bound has 3 bytes and sorts after the upper one) matches nothing in an index holding the values as written,
        // and the memtable index rejects such a range.
        if (uncanonicalizedLower != null && uncanonicalizedUpper != null && uncanonicalizedLower != uncanonicalizedUpper
            && !TypeUtil.isComposite(validator)
            && TypeUtil.compare(uncanonicalizedLower.value.encoded, uncanonicalizedUpper.value.encoded,
                                validator, context.version()) > 0)
            return null;

        Expression result = new Expression(context);
        result.operation = operation;
        result.lower = uncanonicalizedLower;
        result.upper = uncanonicalizedUpper;
        result.lowerInclusive = lowerInclusive;
        result.upperInclusive = upperInclusive;
        result.exclusions.addAll(exclusions);
        return result;
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
        if (operation != Op.RANGE || !TypeUtil.isInetAddress(validator))
            return Collections.singletonList(this);

        assert !reversed : "inet is indexed unreversed";
        Expression ipv4 = inetRange(IPV4_ADDRESS_SIZE);
        Expression ipv6 = inetRange(TypeUtil.INET_ADDRESS_SIZE);
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
        Bound lowerBound = inetBound(lower, size, true);
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
        Expression result = new Expression(context);
        result.operation = Op.RANGE;
        result.lower = lowerBound;
        result.upper = upperBound;
        result.lowerInclusive = lowerBound != null && lowerBound.inclusive;
        result.upperInclusive = upperBound != null && upperBound.inclusive;
        result.exclusions.addAll(exclusions);
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
            return size == IPV4_ADDRESS_SIZE ? new Bound(ByteBuffer.wrap(isLower ? IPV4_MIN : IPV4_MAX), validator, true) : null;

        ByteBuffer value = bound.value.raw;
        if (value.remaining() == size)
            return bound;

        ByteBuffer converted = ByteBuffer.allocate(size);
        ByteBufferUtil.copyBytes(value, value.position(), converted, 0, IPV4_ADDRESS_SIZE);
        // A 16-byte value sorts after the 4-byte values up to its first 4 bytes and before the greater ones, and a
        // 4-byte value sorts before the 16-byte values that start with it and after the smaller ones.
        return new Bound(converted, validator, size == IPV4_ADDRESS_SIZE ? !isLower : isLower);
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
        return TypeUtil.compare(a.value.encoded, b.value.encoded, validator, context.version());
    }

    /**
     * Returns an expression that matches keys not matched by this expression.
     */
    public Expression negated()
    {
        // The negation of an expression of several terms isn't an index search: the rows the index matches for one
        // term are a superset of those with an element holding all the terms, so subtracting them would lose rows
        assert allTerms == null : "Can't negate an expression of several terms: " + this;
        Expression result = new Expression(context);
        result.lower = lower;
        result.upper = upper;

        switch (operation)
        {
            case NOT_EQ:
                result.operation = Op.EQ;
                break;
            case NOT_CONTAINS_KEY:
                result.operation = Op.CONTAINS_KEY;
                break;
            case NOT_CONTAINS_VALUE:
                result.operation = Op.CONTAINS_VALUE;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Negation of operator %s not supported", operation));
        }
        return result;
    }

    /**
     * A representation of a column value in its raw and encoded form.
     */
    public static class Value
    {
        public final ByteBuffer raw;
        public final ByteBuffer encoded;

        /**
         * The native representation of our vector indexes is float[], so we cache that here as well
         * to avoid repeated expensive conversions.  Always null for non-vector types.
         */
        public final float[] vector;

        public Value(ByteBuffer value, AbstractType<?> type)
        {
            this.raw = value;
            this.encoded = TypeUtil.asIndexBytes(value, type);
            this.vector = type.isVector() ? TypeUtil.decomposeVector(type, raw) : null;
        }

        private Value(ByteBuffer raw, ByteBuffer encoded, float[] vector)
        {
            this.raw = raw;
            this.encoded = encoded;
            this.vector = vector;
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

        public Bound(ByteBuffer value, AbstractType<?> type, boolean inclusive)
        {
            this.value = new Value(value, type);
            this.inclusive = inclusive;
        }

        private Bound(Value value, boolean inclusive)
        {
            this.value = value;
            this.inclusive = inclusive;
        }

        /**
         * This bound with its value as written (see {@link TypeUtil#asIndexBytesAsWritten}), or this bound if equal.
         */
        private Bound uncanonicalized(AbstractType<?> type)
        {
            if (value.raw == null)
                return this;
            ByteBuffer asWritten = TypeUtil.asIndexBytesAsWritten(value.raw, type);
            if (asWritten.equals(value.encoded))
                return this;
            return new Bound(new Value(value.raw, asWritten, value.vector), inclusive);
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
