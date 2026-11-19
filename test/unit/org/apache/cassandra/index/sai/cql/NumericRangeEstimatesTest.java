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
package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.AbstractShardedMemtable;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Row count estimates of range restrictions on numeric SAI indexes: infinite and NaN float and double values don't fail
 * the queries, which return what they return without an index, and the estimates for a DESC clustering column are
 * about as accurate as for the same column ordered ASC.
 */
public class NumericRangeEstimatesTest extends SAITester
{
    // More than the number of terms that the memtable index counts exactly (256), so that it interpolates
    private static final int FILLER_VALUES = 600;

    private static final String[] TABLE_DESCRIPTIONS = { "ASC with index", "ASC without index",
                                                         "DESC with index", "DESC without index" };

    private static String defaultShardCount;

    @BeforeClass
    public static void setShardCount()
    {
        // A fixed, small number of memtable shards, so that each shard of the memtable index holds more terms than it
        // counts exactly, and estimates the rows of a range by interpolation
        defaultShardCount = AbstractShardedMemtable.SHARDED_MEMTABLE_CONFIG.getDefaultShardCount();
        AbstractShardedMemtable.SHARDED_MEMTABLE_CONFIG.setDefaultShardCount("4");
    }

    @AfterClass
    public static void resetShardCount()
    {
        AbstractShardedMemtable.SHARDED_MEMTABLE_CONFIG.setDefaultShardCount(defaultShardCount);
    }

    @Test
    public void testInfiniteAndNaNDoubles()
    {
        checkInfiniteAndNaN("double");
    }

    @Test
    public void testInfiniteAndNaNFloats()
    {
        checkInfiniteAndNaN("float");
    }

    private void checkInfiniteAndNaN(String type)
    {
        String[] specialValues = { "-Infinity", "-1e30", "-1.5", "0", "2.5", "1e30", "Infinity", "NaN" };

        List<String> tables = new ArrayList<>();
        for (String order : new String[]{ "ASC", "DESC" })
        {
            for (boolean indexed : new boolean[]{ true, false })
            {
                String table = createTable(String.format("CREATE TABLE %%s (pk int, c %s, v %s, PRIMARY KEY (pk, c)) " +
                                                         "WITH CLUSTERING ORDER BY (c %s)", type, type, order));
                if (indexed)
                {
                    createIndex(String.format("CREATE INDEX ON %s.%s(c) USING 'sai'", KEYSPACE, table));
                    createIndex(String.format("CREATE INDEX ON %s.%s(v) USING 'sai'", KEYSPACE, table));
                }
                tables.add(table);
            }
        }

        for (String table : tables)
        {
            String insert = String.format("INSERT INTO %s.%s (pk, c, v) VALUES (?, %%s, %%s)", KEYSPACE, table);
            for (String value : specialValues)
                executeFormattedQuery(String.format(insert, value, value), 0);
            for (int i = 0; i < FILLER_VALUES; i++)
                executeFormattedQuery(String.format(insert, i * 3 + 0.25, i * 3 + 0.25), 1);
        }

        List<String> failures = new ArrayList<>();
        checkInfiniteAndNaNQueries("memtable", tables, specialValues, failures);
        flush(KEYSPACE, tables.get(0), tables.subList(1, tables.size()).toArray(new String[0]));
        checkInfiniteAndNaNQueries("sstable", tables, specialValues, failures);

        assertTrue(String.format("%d failures for %s:%n%s", failures.size(), type, String.join("\n", failures)),
                   failures.isEmpty());
        // No estimate failed: the queries above didn't rely on the planner's fallback for them
        for (int indexed : new int[]{ 0, 2 }) // ASC and DESC with indexes
            assertEquals(tables.get(indexed), 0,
                         estimateFailures(Keyspace.open(KEYSPACE).getColumnFamilyStore(tables.get(indexed))));
    }

    private void checkInfiniteAndNaNQueries(String source, List<String> tables, String[] values, List<String> failures)
    {
        for (String column : new String[]{ "c", "v" })
        {
            for (int i = 0; i < values.length; i++)
            {
                // The values are in the order of the index terms, so a value and the next one bound a range
                String value = values[i];
                String next = values[(i + 1) % values.length];
                List<String> restrictions = new ArrayList<>();
                restrictions.add(column + " < " + value);
                restrictions.add(column + " <= " + value);
                restrictions.add(column + " > " + value);
                restrictions.add(column + " >= " + value);
                if (i + 1 < values.length)
                    restrictions.add(column + " >= " + value + " AND " + column + " < " + next);
                restrictions.add(column + " != " + value);
                restrictions.add(column + " NOT IN (" + value + ", " + next + ')');
                for (String where : restrictions)
                {
                    // tables: ASC indexed, ASC unindexed, DESC indexed, DESC unindexed
                    String expected = select(tables.get(1), where);
                    for (int t : new int[]{ 0, 2, 3 })
                    {
                        String actual = select(tables.get(t), where);
                        if (!expected.equals(actual))
                            failures.add(String.format("[%s] WHERE %s on %s: expected %s but got %s",
                                                       source, where, TABLE_DESCRIPTIONS[t], expected, actual));
                    }
                }
            }
        }
    }

    private String select(String table, String where)
    {
        String query = String.format("SELECT pk, c FROM %s.%s WHERE %s ALLOW FILTERING", KEYSPACE, table, where);
        try
        {
            UntypedResultSet rows = executeFormattedQuery(query);
            List<String> keys = new ArrayList<>();
            for (UntypedResultSet.Row row : rows)
                keys.add(row.getInt("pk") + ":" + ByteBufferUtil.bytesToHex(row.getBytes("c")));
            keys.sort(String::compareTo);
            return keys.toString();
        }
        catch (Throwable t)
        {
            return t.getClass().getSimpleName() + ": " + t.getMessage();
        }
    }

    @Test
    public void testReversedEstimatesInt()
    {
        checkReversedEstimates("int", 1);
    }

    @Test
    public void testReversedEstimatesBigint()
    {
        checkReversedEstimates("bigint", 1);
    }

    @Test
    public void testReversedEstimatesSmallint()
    {
        checkReversedEstimates("smallint", 1);
    }

    @Test
    public void testReversedEstimatesTinyint()
    {
        checkReversedEstimates("tinyint", 1);
    }

    @Test
    public void testReversedEstimatesDouble()
    {
        checkReversedEstimates("double", 0.5);
    }

    @Test
    public void testReversedEstimatesFloat()
    {
        checkReversedEstimates("float", 0.5);
    }

    // varint and decimal are indexed unreversed on DESC clustering columns (CASSANDRA-20100)
    @Test
    public void testReversedEstimatesVarint()
    {
        checkReversedEstimates("varint", 1);
    }

    @Test
    public void testReversedEstimatesDecimal()
    {
        checkReversedEstimates("decimal", 0.5);
    }

    /**
     * Indexes the same values in an ASC and a DESC clustering column, and checks that the estimates of range
     * restrictions are close to the actual row counts, and to each other, from the memtable, from an SSTable, and
     * after compaction.
     */
    private void checkReversedEstimates(String type, double step)
    {
        // tinyint holds [-128, 127]
        int count = "tinyint".equals(type) ? 250 : 10000;
        String asc = createTable(String.format("CREATE TABLE %%s (pk int, c %s, PRIMARY KEY (pk, c)) " +
                                               "WITH CLUSTERING ORDER BY (c ASC)", type));
        String ascIndex = createIndex(String.format("CREATE INDEX ON %s.%s(c) USING 'sai'", KEYSPACE, asc));
        String desc = createTable(String.format("CREATE TABLE %%s (pk int, c %s, PRIMARY KEY (pk, c)) " +
                                                "WITH CLUSTERING ORDER BY (c DESC)", type));
        String descIndex = createIndex(String.format("CREATE INDEX ON %s.%s(c) USING 'sai'", KEYSPACE, desc));
        disableCompaction(KEYSPACE, asc);
        disableCompaction(KEYSPACE, desc);

        // Values from -count/2 to count/2, each in its own partition
        for (String table : new String[]{ asc, desc })
            for (int i = 0; i < count; i++)
                executeFormattedQuery(String.format("INSERT INTO %s.%s (pk, c) VALUES (?, %s)", KEYSPACE, table,
                                                    format(type, (i - count / 2) * step)), i);

        List<String> failures = new ArrayList<>();
        StorageAttachedIndex ascSai = getIndex(asc, ascIndex);
        StorageAttachedIndex descSai = getIndex(desc, descIndex);
        checkEstimates("memtable", type, step, count, 1, ascSai, descSai, true, failures);
        flush(KEYSPACE, asc, desc);
        checkEstimates("sstable", type, step, count, 1, ascSai, descSai, false, failures);
        // A second SSTable with the same values, compacted with the first one
        for (String table : new String[]{ asc, desc })
            for (int i = 0; i < count; i++)
                executeFormattedQuery(String.format("INSERT INTO %s.%s (pk, c) VALUES (?, %s)", KEYSPACE, table,
                                                    format(type, (i - count / 2) * step)), count + i);
        flush(KEYSPACE, asc, desc);
        compact(KEYSPACE, asc, desc);
        checkEstimates("compacted sstable", type, step, count, 2, ascSai, descSai, false, failures);

        assertTrue(String.format("%d failures for %s:%n%s", failures.size(), type, String.join("\n", failures)),
                   failures.isEmpty());
    }

    /**
     * @param distinct the number of distinct values, from -distinct/2 to distinct/2 - 1 times the step
     * @param copies the number of rows with each value
     */
    private void checkEstimates(String source, String type, double step, int distinct, int copies,
                                StorageAttachedIndex asc, StorageAttachedIndex desc, boolean memtable,
                                List<String> failures)
    {
        int count = distinct * copies;
        // The estimates of the ASC index are expected within 5% of the rows of the actual count, and those of the
        // DESC index within 2% of the rows of those of the ASC index
        long tolerance = Math.max(2, count / 20);
        long ascDescTolerance = Math.max(2, count / 50);
        int[] thresholds = { -distinct / 2 + 1, -distinct / 3, -7, 0, distinct / 5, distinct / 2 - 2 };
        for (int threshold : thresholds)
        {
            String value = format(type, threshold * step);
            int below = (threshold + distinct / 2) * copies; // rows with a value < threshold
            int equal = copies;
            int above = count - below - equal;
            checkEstimate(source, Operator.LT, value, below, asc, desc, memtable, tolerance, ascDescTolerance, failures);
            checkEstimate(source, Operator.LTE, value, below + equal, asc, desc, memtable, tolerance, ascDescTolerance, failures);
            checkEstimate(source, Operator.GT, value, above, asc, desc, memtable, tolerance, ascDescTolerance, failures);
            checkEstimate(source, Operator.GTE, value, above + equal, asc, desc, memtable, tolerance, ascDescTolerance, failures);
        }
    }

    private void checkEstimate(String source, Operator op, String value, long actual,
                               StorageAttachedIndex asc, StorageAttachedIndex desc, boolean memtable,
                               long tolerance, long ascDescTolerance, List<String> failures)
    {
        long ascEstimate = estimate(asc, op, value, memtable);
        long descEstimate = estimate(desc, op, value, memtable);
        String description = String.format("[%s] c %s %s: %d rows, ASC estimate %d, DESC estimate %d",
                                           source, op, value, actual, ascEstimate, descEstimate);
        if (Math.abs(ascEstimate - actual) > tolerance)
            failures.add(description + ": ASC estimate off by more than " + tolerance);
        if (Math.abs(descEstimate - ascEstimate) > ascDescTolerance)
            failures.add(description + ": DESC estimate off the ASC one by more than " + ascDescTolerance);
    }

    private static long estimate(StorageAttachedIndex index, Operator op, String value, boolean memtable)
    {
        Expression expression = new Expression(index.getIndexContext());
        expression.add(op, index.getIndexContext().getValidator().fromString(value));
        long estimate = 0;
        if (memtable)
        {
            for (MemtableIndex memtableIndex : index.getIndexContext().getLiveMemtables().values())
                estimate += memtableIndex.estimateMatchingRowsCount(expression);
        }
        else
        {
            for (SSTableIndex sstableIndex : index.getIndexContext().getView().getIndexes())
                estimate += sstableIndex.estimateMatchingRowsCount(expression);
        }
        return estimate;
    }

    private static StorageAttachedIndex getIndex(String table, String index)
    {
        return (StorageAttachedIndex) Keyspace.open(KEYSPACE).getColumnFamilyStore(table).getIndexManager().getIndexByName(index);
    }

    private static String format(String type, double value)
    {
        switch (type)
        {
            case "double":
            case "float":
            case "decimal":
                return Double.toString(value);
            default:
                return Long.toString((long) value);
        }
    }
}
