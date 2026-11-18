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
package org.apache.cassandra.index.sai.cql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

/**
 * Range restrictions on varint and decimal indexes with values and bounds far beyond the range of a double return the
 * rows that filtering returns, also where the row estimates interpolate over such terms.
 */
public class LargeNumberBoundsTest extends SAITester.Versioned
{
    private static final BigInteger BYTES_21 = BigInteger.ONE.shiftLeft(20 * 8);
    private static final BigInteger BYTES_201 = BigInteger.ONE.shiftLeft(200 * 8);
    private static final BigInteger BYTES_9000 = BigInteger.ONE.shiftLeft(9000 * 8 - 2);

    @Test
    public void testLargeValuesAndBounds()
    {
        List<BigInteger> values = List.of(BigInteger.valueOf(-2), BigInteger.ZERO, BigInteger.valueOf(5),
                                          BYTES_21, BYTES_201, BYTES_201.negate(), BYTES_9000, BYTES_9000.negate(),
                                          BYTES_9000.add(BigInteger.ONE));
        checkLargeBounds("varint", values);
        checkLargeBounds("decimal", values);
    }

    /**
     * Small values with large bounds; compaction puts the large value of the second sstable in their terms histogram.
     */
    @Test
    public void testSmallValuesLargeBounds()
    {
        List<BigInteger> values = List.of(BigInteger.valueOf(-2), BigInteger.ZERO, BigInteger.valueOf(5), BigInteger.valueOf(7));
        checkLargeBounds("varint", values);
        checkLargeBounds("decimal", values);
    }

    /**
     * Many terms in one partition, hence in one memtable shard, so that the memtable index estimates a range by
     * interpolating over its terms instead of counting them (it counts at most 256 terms), with large values among
     * them.
     */
    @Test
    public void testManyTermsInOnePartition()
    {
        checkManyTermsInOnePartition("varint");
        checkManyTermsInOnePartition("decimal");
    }

    private void checkManyTermsInOnePartition(String type)
    {
        createTable("CREATE TABLE %s (pk int, ck int, v " + type + ", u " + type + ", PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        List<BigInteger> values = new ArrayList<>();
        for (int i = 0; i < 600; i++)
            values.add(BigInteger.valueOf(i - 300));
        values.addAll(List.of(BYTES_201, BYTES_201.negate(), BYTES_9000, BYTES_9000.negate()));
        for (int ck = 0; ck < values.size(); ck++)
        {
            Object value = convert(type, values.get(ck));
            execute("INSERT INTO %s (pk, ck, v, u) VALUES (0, ?, ?, ?)", ck, value, value);
        }

        for (String phase : new String[]{ "memtable", "sstable" })
        {
            if (phase.equals("sstable"))
                flush();
            for (BigInteger bound : List.of(BigInteger.valueOf(-100), BigInteger.valueOf(3), BYTES_201, BYTES_201.negate(), BYTES_9000))
            {
                Object b = convert(type, bound);
                for (String op : new String[]{ "<", "<=", ">", ">=", "!=" })
                    assertRowsMatchFiltering(type, phase, "%c " + op + " ?", b);
            }
            assertRowsMatchFiltering(type, phase, "%c > ? AND %c < ?", convert(type, BigInteger.valueOf(-50)), convert(type, BYTES_201));
            assertRowsMatchFiltering(type, phase, "%c > ? AND %c < ?", convert(type, BYTES_201.negate()), convert(type, BigInteger.TEN));
        }
    }

    private void assertRowsMatchFiltering(String type, String phase, String restriction, Object... values)
    {
        Set<Integer> expected = clusterings("SELECT ck FROM %s WHERE " + restriction.replace("%c", "u") + " ALLOW FILTERING", values);
        Set<Integer> actual = clusterings("SELECT ck FROM %s WHERE " + restriction.replace("%c", "v") + " ALLOW FILTERING", values);
        Assertions.assertThat(actual)
                  .describedAs("%s, %s, %s: %s with bounds of %s bytes", version, type, phase, restriction, sizes(values))
                  .isEqualTo(expected);
    }

    private Set<Integer> clusterings(String query, Object... values)
    {
        Set<Integer> keys = new TreeSet<>();
        for (UntypedResultSet.Row row : execute(query, values))
            keys.add(row.getInt("ck"));
        return keys;
    }

    private void checkLargeBounds(String type, List<BigInteger> values)
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v " + type + ", u " + type + ')');
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        // the unindexed column u holds the same values as the indexed column v
        for (int i = 0; i < values.size(); i++)
        {
            Object value = convert(type, values.get(i));
            execute("INSERT INTO %s (pk, v, u) VALUES (?, ?, ?)", i, value, value);
        }

        List<BigInteger> bounds = List.of(BigInteger.valueOf(3), BYTES_21, BYTES_201, BYTES_201.negate(), BYTES_9000,
                                          BYTES_9000.negate(), BYTES_9000.add(BigInteger.ONE), BYTES_9000.subtract(BigInteger.ONE));
        assertMatchesFiltering(type, "memtable", bounds);
        flush();
        assertMatchesFiltering(type, "sstable", bounds);
        // a second sstable
        Object value = convert(type, BYTES_201.add(BigInteger.ONE));
        execute("INSERT INTO %s (pk, v, u) VALUES (?, ?, ?)", values.size(), value, value);
        flush();
        assertMatchesFiltering(type, "sstables", bounds);
        compact();
        assertMatchesFiltering(type, "compacted", bounds);
    }

    private void assertMatchesFiltering(String type, String phase, List<BigInteger> bounds)
    {
        for (BigInteger bound : bounds)
        {
            Object b = convert(type, bound);
            for (String op : new String[]{ "=", "!=", "<", "<=", ">", ">=" })
                assertMatchesFiltering(type, phase, "%c " + op + " ?", b);

            // a range with the bound as its upper and lower bound (non-empty ranges only)
            Object one = convert(type, BigInteger.ONE);
            if (bound.compareTo(BigInteger.ONE) > 0)
                assertMatchesFiltering(type, phase, "%c > ? AND %c < ?", one, b);
            else
                assertMatchesFiltering(type, phase, "%c > ? AND %c < ?", b, one);
        }
    }

    private void assertMatchesFiltering(String type, String phase, String restriction, Object... values)
    {
        Set<Integer> expected = partitions("SELECT pk FROM %s WHERE " + restriction.replace("%c", "u") + " ALLOW FILTERING", values);
        Set<Integer> actual = partitions("SELECT pk FROM %s WHERE " + restriction.replace("%c", "v") + " ALLOW FILTERING", values);
        Assertions.assertThat(actual)
                  .describedAs("%s, %s, %s: %s with bounds of %s bytes", version, type, phase, restriction, sizes(values))
                  .isEqualTo(expected);
    }

    private Set<Integer> partitions(String query, Object... values)
    {
        Set<Integer> keys = new TreeSet<>();
        for (UntypedResultSet.Row row : execute(query, values))
            keys.add(row.getInt("pk"));
        return keys;
    }

    private static List<String> sizes(Object[] values)
    {
        List<String> sizes = new ArrayList<>();
        for (Object value : values)
        {
            BigInteger i = value instanceof BigDecimal ? ((BigDecimal) value).toBigInteger() : (BigInteger) value;
            sizes.add((i.signum() < 0 ? "-" : "") + i.toByteArray().length);
        }
        return sizes;
    }

    private static Object convert(String type, BigInteger value)
    {
        return type.equals("varint") ? value : new BigDecimal(value);
    }
}
