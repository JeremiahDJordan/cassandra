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
package org.apache.cassandra.index.sai.memory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.IntFunction;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TrieMemoryIndexTest
{
    private static final String KEYSPACE = "test_keyspace";
    private static final String TABLE = "test_table";
    private static final String PART_KEY_COL = "key";
    private static final String REG_COL = "col";
    public static final long[] EMTPY_LONG_ARRAY = {};

    private static DecoratedKey key = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes("key"));

    private TableMetadata table;

    @Before
    public void setup()
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void iteratorShouldReturnAllValuesNumeric()
    {
        TrieMemoryIndex index = newTrieMemoryIndex(Int32Type.instance, Int32Type.instance);

        for (int row = 0; row < 100; row++)
        {
            index.add(makeKey(table, Integer.toString(row)), Clustering.EMPTY, Int32Type.instance.decompose(row / 10), allocatedBytes -> {}, allocatesBytes -> {});
        }

        var iterator = index.iterator();
        int valueCount = 0;
        while(iterator.hasNext())
        {
            var pair = iterator.next();
            int value = ByteSourceInverse.getSignedInt(TypeUtil.termBytes(pair.left));
            int idCount = 0;
            for (var pkf : pair.right)
            {
                PrimaryKey primaryKey = pkf.pk;
                int id = Int32Type.instance.compose(primaryKey.partitionKey().getKey());
                assertEquals(id/10, value);
                idCount++;
            }
            assertEquals(10, idCount);
            valueCount++;
        }
        assertEquals(10, valueCount);
    }

    @Test
    public void iteratorShouldReturnAllValuesString()
    {
        TrieMemoryIndex index = newTrieMemoryIndex(UTF8Type.instance, UTF8Type.instance);

        for (int row = 0; row < 100; row++)
        {
            index.add(makeKey(table, Integer.toString(row)), Clustering.EMPTY, UTF8Type.instance.decompose(Integer.toString(row / 10)), allocatedBytes -> {}, allocatesBytes -> {});
        }

        var iterator = index.iterator();
        int valueCount = 0;
        while(iterator.hasNext())
        {
            var pair = iterator.next();
            String value = new String(ByteSourceInverse.readBytes(ByteSource.peekable(TypeUtil.termBytes(pair.left))), StandardCharsets.UTF_8);
            int idCount = 0;
            for (var pkf : pair.right)
            {
                PrimaryKey primaryKey = pkf.pk;
                String id = UTF8Type.instance.compose(primaryKey.partitionKey().getKey());
                assertEquals(Integer.toString(Integer.parseInt(id) / 10), value);
                idCount++;
            }
            assertEquals(10, idCount);
            valueCount++;
        }
        assertEquals(10, valueCount);
    }

    @Test
    public void shouldAcceptPrefixValues()
    {
        shouldAcceptPrefixValuesForType(UTF8Type.instance, i -> UTF8Type.instance.decompose(String.format("%03d", i)));
        shouldAcceptPrefixValuesForType(Int32Type.instance, Int32Type.instance::decompose);
    }

    @Test
    public void testStringRangeEstimates()
    {
        IndexContext context = newIndexContext(UTF8Type.instance, UTF8Type.instance);
        TrieMemoryIndex index = new TrieMemoryIndex(context);
        int numRows = 10000;
        String[] values = new String[numRows];
        Random random = new Random(3);

        for (int i = 0; i < numRows; i++)
        {
            values[i] = randomString(random);
            DecoratedKey pk = makeKey(table, Integer.toString(i));
            ByteBuffer value = UTF8Type.instance.decompose(values[i]);
            index.add(pk, Clustering.EMPTY, value, allocatedBytes -> {}, allocatesBytes -> {});
        }

        for (int i = 0; i < 100; i++)
        {
            String s1 = randomString(random);
            String s2 = randomString(random);
            String lower = s1.compareTo(s2) < 0 ? s1 : s2;
            String upper = s1.compareTo(s2) < 0 ? s2 : s1;

            testStringFilterEstimate(context, index, values, lower, upper);
        }
    }

    // We're not testing full UTF-8 alphabet because the estimation algorithm is not very good
    // when data are non-uniformly distributed. If we use a wider character set, there will be likely
    // huge holes between the code points, and modeling real distribution of characters used by some real
    // text (as in some existing natural language) becomes hairy very quickly.
    // I got already much worse results when using a set of {a-z, 0-9}, because of a hole between
    // encodings of letters and numbers.
    // This test is to make sure the estimation algorithm works correctly under the assumption that
    // data are distributed uniformly, so the following contiguous set of letters is good enough.
    private static final char[] CHARS = "abcdefghijklmnopqrstuvwxyz".toCharArray();

    public static String randomString(Random random)
    {
        char[] chars = new char[4 + random.nextInt(32)];
        for (int i=0; i < chars.length; i++)
            chars[i] = CHARS[random.nextInt(CHARS.length)];
        return new String(chars);
    }

    private void testStringFilterEstimate(IndexContext context, TrieMemoryIndex index, String[] allValues, String lower, String upper)
    {
        Expression expression = new Expression(context);
        expression.add(Operator.GTE, UTF8Type.instance.decompose(lower));
        expression.add(Operator.LTE, UTF8Type.instance.decompose(upper));

        long estimated = index.estimateMatchingRowsCount(expression);
        long actual = Arrays.stream(allValues).filter(t -> t.compareTo(lower) >= 0 && t.compareTo(upper) <= 0).count();
        double ratio = (double) estimated / (actual + 1);

        // the estimation is not very accurate for very narrow text ranges because the distribution of values
        // is not very uniform; we try to somehow account for that by giving more tolerance for narrower ranges
        int rangeSize = Math.abs(lower.charAt(0) - upper.charAt(0));
        double tolerance;
        switch (rangeSize)
        {
            case 0: // same first character, very narrow range
                tolerance = 20.0;
                break;
            case 1: // adjacent first characters, still a narrow range
                tolerance = 4.0;
                break;
            default: // wider range
                tolerance = 1.5;
        }

        assertTrue(String.format("Estimated %d, actual %d  (lower %s, upper %s)", estimated, actual, lower, upper),
                   ratio < tolerance && ratio > 1.0 / tolerance);
    }

    @Test
    public void testLongRangeEstimates()
    {
        // CNDB-17012 regression test
        // Why timestamps? Timestamps are encoded internally as 8 byte longs, but they are not treated
        // as numeric types by the row count estimation code so they go through a generic bytecomparable logic,
        // which had a bug.
        IndexContext context = newIndexContext(UTF8Type.instance, LongType.instance);
        TrieMemoryIndex index = new TrieMemoryIndex(context);

        // Test empty index
        testLongFilterEstimate(context, index, EMTPY_LONG_ARRAY, Long.MIN_VALUE, Long.MIN_VALUE);
        testLongFilterEstimate(context, index, EMTPY_LONG_ARRAY, Long.MIN_VALUE, Long.MAX_VALUE);
        testLongFilterEstimate(context, index, EMTPY_LONG_ARRAY, Long.MAX_VALUE, Long.MAX_VALUE);

        int numRows = 10000;
        long[] values = new long[numRows];
        Random random = new Random(1);
        for (int i = 0; i < numRows; i++)
        {
            values[i] = random.nextLong();  // it's important negative longs are also included
            DecoratedKey pk = makeKey(table, Integer.toString(i));
            ByteBuffer value = LongType.instance.decompose(values[i]);
            index.add(pk, Clustering.EMPTY, value, allocatedBytes -> {}, allocatesBytes -> {});
        }

        for (int i = 0; i < 100; i++)
        {
            long l1 = random.nextLong();
            long l2 = random.nextLong();
            long lower = Math.min(l1, l2);
            long upper = Math.max(l1, l2);
            testLongFilterEstimate(context, index, values, lower, upper);
        }

        testLongFilterEstimate(context, index, values, Long.MIN_VALUE, Long.MIN_VALUE);
        testLongFilterEstimate(context, index, values, Long.MIN_VALUE, Long.MAX_VALUE);
        testLongFilterEstimate(context, index, values, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @Test
    public void testDecimalRangeEstimatesFarApartExponents()
    {
        // The exact difference of the smallest and largest values overflows a BigInteger.
        List<BigDecimal> values = new ArrayList<>();
        values.add(new BigDecimal("1E-300000000"));
        for (int i = 1; i <= 1000; i++)
            values.add(BigDecimal.valueOf(i));
        values.add(new BigDecimal("1E+300000000"));
        TrieMemoryIndex index = newDecimalIndex(values);

        // The range covers half of the span of the values.
        assertEquals(values.size() / 2, estimateDecimalRange(index, BigDecimal.valueOf(100), new BigDecimal("5E+299999999")), 10);
    }

    @Test
    public void testDecimalRangeEstimatesLargestDouble()
    {
        // The differences of these values are finite doubles, but exceed Double.MAX_VALUE when rounded to 16 digits.
        List<BigDecimal> values = new ArrayList<>();
        for (int i = 1; i <= 1000; i++)
            values.add(BigDecimal.valueOf(i));
        values.add(BigDecimal.valueOf(Double.MAX_VALUE));
        TrieMemoryIndex index = newDecimalIndex(values);

        assertEquals(values.size(), estimateDecimalRange(index, BigDecimal.valueOf(100), BigDecimal.valueOf(Double.MAX_VALUE)));
    }

    @Test
    public void testDecimalRangeEstimatesTinyBound()
    {
        // The fractions of the index range in these ranges are smaller than the smallest double.
        List<BigDecimal> values = new ArrayList<>();
        for (int i = 0; i < 5000; i++)
            values.add(BigDecimal.ZERO);
        for (int i = 1; i <= 1000; i++)
            values.add(BigDecimal.valueOf(i));
        values.add(new BigDecimal("1E+10"));
        TrieMemoryIndex index = newDecimalIndex(values);
        // the rows counted before the estimate interpolates
        assertEquals(4096, estimateDecimalRange(index, BigDecimal.ZERO, new BigDecimal("1E-2147483630")));

        values = new ArrayList<>();
        values.add(BigDecimal.ZERO);
        for (int i = 1; i <= 300; i++)
            values.add(BigDecimal.valueOf(i).scaleByPowerOfTen(-2147483600));
        values.add(new BigDecimal("1E+100"));
        index = newDecimalIndex(values);
        assertEquals(256, estimateDecimalRange(index, BigDecimal.ZERO, BigDecimal.valueOf(300).scaleByPowerOfTen(-2147483600)));
    }

    @Test
    public void testDecimalRangeEstimatesSmallestScale()
    {
        // The differences of these values have more than 16 digits at a scale close to Integer.MIN_VALUE.
        List<BigDecimal> values = new ArrayList<>();
        for (int i = 0; i <= 1000; i++)
            values.add(atSmallestScale(i));
        TrieMemoryIndex index = newDecimalIndex(values);
        assertEquals(values.size() * 0.8, estimateDecimalRange(index, atSmallestScale(100), atSmallestScale(900)), 2);

        // and from 0, whose scale is far from theirs
        values.set(0, BigDecimal.ZERO);
        index = newDecimalIndex(values);
        double fraction = atSmallestScale(500).divide(atSmallestScale(1000), MathContext.DECIMAL64).doubleValue();
        assertEquals(values.size() * fraction, estimateDecimalRange(index, BigDecimal.ZERO, atSmallestScale(500)), 2);
    }

    @Test
    public void testDecimalRangeEstimatesZeroNearScaleLimits()
    {
        // The difference of 0 and 18 nines at the scale Integer.MIN_VALUE + 2 rounds up to the scale Integer.MIN_VALUE.
        List<BigDecimal> values = new ArrayList<>();
        values.add(BigDecimal.ZERO);
        for (int i = 1; i <= 999; i++)
            values.add(new BigDecimal(BigInteger.valueOf(i * 1000000000000000L), Integer.MIN_VALUE + 2));
        values.add(new BigDecimal(new BigInteger("999999999999999999"), Integer.MIN_VALUE + 2));
        TrieMemoryIndex index = newDecimalIndex(values);
        assertEquals(values.size() * 0.5, estimateDecimalRange(index, BigDecimal.ZERO, values.get(500)), 2);

        // and 0 with decimals at the scale Integer.MAX_VALUE
        values = new ArrayList<>();
        values.add(BigDecimal.ZERO);
        for (int i = 0; i < 1000; i++)
            values.add(new BigDecimal(BigInteger.valueOf(10 * i + 1), Integer.MAX_VALUE));
        index = newDecimalIndex(values);
        assertEquals(values.size() * 0.5, estimateDecimalRange(index, BigDecimal.ZERO, values.get(500)), 2);
    }

    @Test
    public void testDecimalRangeEstimatesLargestScale()
    {
        // The difference of decimals at the scales Integer.MAX_VALUE - 1 and Integer.MAX_VALUE, in an index range up to 500.
        List<BigDecimal> values = new ArrayList<>();
        for (int i = 0; i < 500; i++)
            values.add(new BigDecimal(BigInteger.valueOf(2 * i + 1), Integer.MAX_VALUE));
        for (int i = 1; i <= 500; i++)
            values.add(BigDecimal.valueOf(i));
        TrieMemoryIndex index = newDecimalIndex(values);
        // the rows counted before the estimate interpolates
        assertEquals(256, estimateDecimalRange(index, values.get(0), new BigDecimal(new BigInteger("10000000000000000001"), Integer.MAX_VALUE - 1)));
    }

    private TrieMemoryIndex newDecimalIndex(List<BigDecimal> values)
    {
        TrieMemoryIndex index = new TrieMemoryIndex(newIndexContext(UTF8Type.instance, DecimalType.instance));
        for (int i = 0; i < values.size(); i++)
            index.add(makeKey(table, Integer.toString(i)), Clustering.EMPTY, DecimalType.instance.decompose(values.get(i)), allocatedBytes -> {}, allocatesBytes -> {});
        return index;
    }

    private long estimateDecimalRange(TrieMemoryIndex index, BigDecimal lower, BigDecimal upper)
    {
        Expression expression = new Expression(index.indexContext);
        expression.add(Operator.GTE, DecimalType.instance.decompose(lower));
        expression.add(Operator.LTE, DecimalType.instance.decompose(upper));
        return index.estimateMatchingRowsCount(expression);
    }

    private void testLongFilterEstimate(IndexContext context, TrieMemoryIndex index, long[] allValues, long lower, long upper)
    {
        Expression expression = new Expression(context);
        expression.add(Operator.GTE, LongType.instance.decompose(lower));
        expression.add(Operator.LTE, LongType.instance.decompose(upper));

        long estimated = index.estimateMatchingRowsCount(expression);
        long actual = Arrays.stream(allValues).filter(t -> t >= lower && t <= upper).count();

        assertTrue(String.format("Estimated %d, actual %d", estimated, actual), (double) Math.abs(estimated - actual) / (actual + 1) < 0.2);
    }

    @Test
    public void testTimestampRangeEstimates()
    {
        // CNDB-17012 regression test
        // Why timestamps? Timestamps are encoded internally as 8 byte longs, but they are not treated
        // as numeric types by the row count estimation code so they go through a generic bytecomparable logic,
        // which had a bug.
        IndexContext context = newIndexContext(UTF8Type.instance, TimestampType.instance);
        TrieMemoryIndex index = new TrieMemoryIndex(context);

        // Test empty index
        testTimestampFilterEstimate(context, index, EMTPY_LONG_ARRAY, Long.MIN_VALUE, Long.MIN_VALUE);
        testTimestampFilterEstimate(context, index, EMTPY_LONG_ARRAY, Long.MIN_VALUE, Long.MAX_VALUE);
        testTimestampFilterEstimate(context, index, EMTPY_LONG_ARRAY, Long.MAX_VALUE, Long.MAX_VALUE);

        int numRows = 10000;
        long[] values = new long[numRows];
        Random random = new Random(1);
        for (int i = 0; i < numRows; i++)
        {
            values[i] = random.nextLong();  // it's important negative longs are also included
            DecoratedKey pk = makeKey(table, Integer.toString(i));
            ByteBuffer value = TimestampType.instance.decompose(new Date(values[i]));
            index.add(pk, Clustering.EMPTY, value, allocatedBytes -> {}, allocatesBytes -> {});
        }

        for (int i = 0; i < 100; i++)
        {
            long l1 = random.nextLong();
            long l2 = random.nextLong();
            long lower = Math.min(l1, l2);
            long upper = Math.max(l1, l2);
            testTimestampFilterEstimate(context, index, values, lower, upper);
        }

        testTimestampFilterEstimate(context, index, values, Long.MIN_VALUE, Long.MIN_VALUE);
        testTimestampFilterEstimate(context, index, values, Long.MIN_VALUE, Long.MAX_VALUE);
        testTimestampFilterEstimate(context, index, values, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    private void testTimestampFilterEstimate(IndexContext context, TrieMemoryIndex index, long[] allValues, long lower, long upper)
    {
        Expression expression = new Expression(context);
        expression.add(Operator.GTE, TimestampType.instance.decompose(new Date(lower)));
        expression.add(Operator.LTE, TimestampType.instance.decompose(new Date(upper)));

        long estimated = index.estimateMatchingRowsCount(expression);
        long actual = Arrays.stream(allValues).filter(t -> t >= lower && t <= upper).count();

        assertTrue(String.format("Estimated %d, actual %d", estimated, actual), (double) Math.abs(estimated - actual) / (actual + 1) < 0.2);
    }

    private void shouldAcceptPrefixValuesForType(AbstractType<?> type, IntFunction<ByteBuffer> decompose)
    {
        final TrieMemoryIndex index = newTrieMemoryIndex(UTF8Type.instance, type);
        for (int i = 0; i < 99; ++i)
        {
            index.add(key, Clustering.EMPTY, decompose.apply(i), allocatedBytes -> {}, allocatesBytes -> {});
        }

        final var iterator = index.iterator();
        int i = 0;
        while (iterator.hasNext())
        {
            var pair = iterator.next();
            assertEquals(1, pair.right.size());

            final int rowId = i;
            final ByteComparable expectedByteComparable = TypeUtil.isLiteral(type)
                                                          ? v -> ByteSource.preencoded(decompose.apply(rowId))
                                                          : version -> type.asComparableBytes(decompose.apply(rowId), version);
            final ByteComparable actualByteComparable = pair.left;
            assertEquals("Mismatch at: " + i, 0, ByteSource.compare(expectedByteComparable.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION), TypeUtil.termBytes(actualByteComparable)));

            i++;
        }
        assertEquals(99, i);
    }


    private IndexContext newIndexContext(AbstractType<?> partitionKeyType, AbstractType<?> columnType)

    {
        table = TableMetadata.builder(KEYSPACE, TABLE)
                             .addPartitionKeyColumn(PART_KEY_COL, partitionKeyType)
                             .addRegularColumn(REG_COL, columnType)
                             .partitioner(Murmur3Partitioner.instance)
                             .caching(CachingParams.CACHE_NOTHING)
                             .build();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        options.put("target", REG_COL);

        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata("col_index", IndexMetadata.Kind.CUSTOM, options);
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(table, indexMetadata);
        return new IndexContext(table.keyspace,
                                table.name,
                                table.id,
                                table.partitionKeyType,
                                table.comparator,
                                target.left,
                                target.right,
                                indexMetadata,
                                MockSchema.newCFS(table));
    }

    private TrieMemoryIndex newTrieMemoryIndex(AbstractType<?> partitionKeyType, AbstractType<?> columnType)
    {
        return new TrieMemoryIndex(newIndexContext(partitionKeyType, columnType));
    }

    DecoratedKey makeKey(TableMetadata table, Object...partitionKeys)
    {
        ByteBuffer key;
        if (TypeUtil.isComposite(table.partitionKeyType))
            key = ((CompositeType)table.partitionKeyType).decompose(partitionKeys);
        else
            key = table.partitionKeyType.fromString((String)partitionKeys[0]);
        return table.partitioner.decorateKey(key);
    }

    /**
     * Returns {@code 100000000000000001 + i * 40000000000000001} at the scale {@code Integer.MIN_VALUE + 1}.
     */
    private static BigDecimal atSmallestScale(int i)
    {
        BigInteger unscaled = BigInteger.valueOf(40000000000000001L).multiply(BigInteger.valueOf(i)).add(BigInteger.valueOf(100000000000000001L));
        return new BigDecimal(unscaled, Integer.MIN_VALUE + 1);
    }
}
