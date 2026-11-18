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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Checks that an SAI index on a clustering column gives the same results whether the column is ordered ASC or DESC
 * (a DESC column has a {@code ReversedType}), and the same results as the same query served by filtering, without an
 * index. SAI doesn't index the empty values of non-literal types (CASSANDRA-20313).
 */
public class ReversedClusteringIndexTest extends SAITester
{
    private static final String[] PARTITION_RESTRICTIONS = { "", "pk = 0 AND " };
    private static final String REJECTED = "rejected";
    private static final String FAILED = "failed";

    @Test
    public void testText()
    {
        test("text", true, EMPTY_BYTE_BUFFER, "a", "ab", "b");
    }

    @Test
    public void testVarchar()
    {
        test("varchar", true, EMPTY_BYTE_BUFFER, "a", "ab", "b");
    }

    @Test
    public void testAscii()
    {
        test("ascii", true, EMPTY_BYTE_BUFFER, "a", "ab", "b");
    }

    @Test
    public void testBoolean()
    {
        test("boolean", true, EMPTY_BYTE_BUFFER, false, true);
    }

    @Test
    public void testFrozenTuple()
    {
        // TODO: the index serves != and NOT IN on frozen tuples, but drops the rows with an empty value, and fails
        //  the read if the excluded value is empty, whatever the clustering order. Only check that ASC and DESC agree.
        test("frozen<tuple<int, text>>", false, EMPTY_BYTE_BUFFER, tuple(1, "a"), tuple(1, "b"), tuple(2, ""));
    }

    @Test
    public void testFrozenUDT()
    {
        String udt = createType("CREATE TYPE %s (i int, t text)");
        // TODO: as for frozen tuples, only check that ASC and DESC agree on != and NOT IN
        test("frozen<" + udt + '>', false,
             EMPTY_BYTE_BUFFER, userType("i", 1, "t", "a"), userType("i", 1, "t", "b"), userType("i", 2, "t", ""));
    }

    @Test
    public void testUUID()
    {
        test("uuid", true,
             UUID.fromString("00000000-0000-1000-8000-000000000001"),
             UUID.fromString("00000000-0000-4000-8000-000000000002"),
             UUID.fromString("ffffffff-ffff-4fff-bfff-ffffffffffff"));
    }

    @Test
    public void testTimeUUID()
    {
        test("timeuuid", true,
             UUID.fromString("00000000-0000-1000-8000-000000000001"),
             UUID.fromString("ffffffff-0000-1000-8000-000000000002"),
             UUID.fromString("00000000-ffff-1fff-8000-000000000003"));
    }

    @Test
    public void testInt()
    {
        test("int", true, Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE);
    }

    @Test
    public void testBigint()
    {
        test("bigint", true, Long.MIN_VALUE, -1L, 0L, 300L, Long.MAX_VALUE);
    }

    @Test
    public void testDouble()
    {
        // Not infinite values: an SSTable index's row count estimate for a range fails the read on them, whatever the
        // clustering order, because TermsDistribution can't convert them to BigDecimal
        test("double", true, -1e300d, -1.5d, 0d, 2.25d, 1e300d);
    }

    // Not tested here: inet, varint and decimal, whose DESC indexes use their base type (CASSANDRA-20100); see
    // DescClusteringRangeQueryTest.

    @Test
    public void testTimestamp()
    {
        test("timestamp", true, new Date(-1000L), new Date(0L), new Date(1700000000000L));
    }

    /**
     * @param notEqualMatchesFiltering whether {@code !=} and {@code NOT IN} must give the same results as filtering,
     * rather than just the same results for ASC and DESC
     */
    private void test(String type, boolean notEqualMatchesFiltering, Object... values)
    {
        String ascIndexed = createIndexedOrPlainTable(type, "ASC", true);
        String descIndexed = createIndexedOrPlainTable(type, "DESC", true);
        String ascFiltered = createIndexedOrPlainTable(type, "ASC", false);
        String descFiltered = createIndexedOrPlainTable(type, "DESC", false);
        Tables tables = new Tables(ascFiltered, descFiltered, ascIndexed, descIndexed);
        for (String table : tables.all)
            disableCompaction(KEYSPACE, table);

        List<String> failures = new ArrayList<>();

        insert(tables, 0, values);
        checkQueries("memtable", tables, notEqualMatchesFiltering, values, failures);
        flushAll(tables);
        checkQueries("sstable", tables, notEqualMatchesFiltering, values, failures);

        insert(tables, 1, values);
        insert(tables, 2, values[0]);
        checkQueries("sstable and memtable", tables, notEqualMatchesFiltering, values, failures);
        flushAll(tables);
        checkQueries("two sstables", tables, notEqualMatchesFiltering, values, failures);

        compact(KEYSPACE, ascFiltered, descFiltered, ascIndexed, descIndexed);
        for (String table : tables.all)
            assertEquals(table, 1, getColumnFamilyStore(KEYSPACE, table).getLiveSSTables().size());
        checkQueries("compacted sstable", tables, notEqualMatchesFiltering, values, failures);

        assertTrue(String.format("%d failures for type %s:%n%s", failures.size(), type, String.join("\n", failures)),
                   failures.isEmpty());
    }

    private String createIndexedOrPlainTable(String type, String order, boolean indexed)
    {
        String table = createTable(String.format("CREATE TABLE %%s (pk int, c %s, v int, PRIMARY KEY (pk, c)) " +
                                                 "WITH CLUSTERING ORDER BY (c %s)", type, order));
        if (indexed)
            createIndex(String.format("CREATE CUSTOM INDEX ON %s.%s(c) USING 'StorageAttachedIndex'", KEYSPACE, table));
        return table;
    }

    private void flushAll(Tables tables)
    {
        flush(KEYSPACE, tables.ascFiltered, tables.descFiltered, tables.ascIndexed, tables.descIndexed);
        for (String table : tables.all)
            assertEquals(table, 0, getColumnFamilyStore(KEYSPACE, table).getTracker().getView().getCurrentMemtable().getOperations());
    }

    private void insert(Tables tables, int pk, Object... values)
    {
        for (String table : tables.all)
            for (Object value : values)
                executeFormattedQuery(String.format("INSERT INTO %s.%s (pk, c, v) VALUES (?, ?, ?)", KEYSPACE, table),
                                      pk, value, pk);
    }

    private void checkQueries(String source, Tables tables, boolean notEqualMatchesFiltering, Object[] values, List<String> failures)
    {
        for (String partition : PARTITION_RESTRICTIONS)
        {
            for (int i = 0; i < values.length; i++)
            {
                Object value = values[i];
                Object next = values[(i + 1) % values.length];
                checkQuery(source, tables, true, failures, partition + "c = ?", value);
                checkQuery(source, tables, true, failures, partition + "c IN (?, ?)", value, next);
                checkQuery(source, tables, notEqualMatchesFiltering, failures, partition + "c != ?", value);
                checkQuery(source, tables, notEqualMatchesFiltering, failures, partition + "c NOT IN (?, ?)", value, next);
                checkQuery(source, tables, true, failures, partition + "c > ?", value);
                checkQuery(source, tables, true, failures, partition + "c >= ?", value);
                checkQuery(source, tables, true, failures, partition + "c < ?", value);
                checkQuery(source, tables, true, failures, partition + "c <= ?", value);
                checkQuery(source, tables, true, failures, partition + "c >= ? AND c < ?", value, next);
            }
        }
    }

    private void checkQuery(String source, Tables tables, boolean matchesFiltering, List<String> failures, String where, Object... values)
    {
        String description = String.format("[%s] WHERE %s with %s", source, where, describe(values));

        // With ALLOW FILTERING, all the tables give the same results as the unindexed ASC table
        String expected = select(tables.ascFiltered, where, true, values);
        check(failures, description + " ALLOW FILTERING on DESC without index",
              expected, select(tables.descFiltered, where, true, values));
        String ascIndexed = select(tables.ascIndexed, where, true, values);
        if (matchesFiltering)
            check(failures, description + " ALLOW FILTERING on ASC with index", expected, ascIndexed);
        else
            expected = ascIndexed;
        // See the TODO of testFrozenTuple: when only checking that ASC and DESC agree, they may fail alike
        check(failures, description + " ALLOW FILTERING on DESC with index",
              expected, select(tables.descIndexed, where, true, values), !matchesFiltering);

        // Without ALLOW FILTERING, the ASC and DESC indexes support the same queries, with the same results
        check(failures, description + " without ALLOW FILTERING on DESC with index",
              select(tables.ascIndexed, where, false, values),
              select(tables.descIndexed, where, false, values),
              !matchesFiltering);
    }

    private static void check(List<String> failures, String description, String expected, String actual)
    {
        check(failures, description, expected, actual, false);
    }

    /**
     * @param sameFailure whether a query that fails other than by being rejected passes if it fails the same way on
     * both tables; otherwise it fails, even if it fails with every table
     */
    private static void check(List<String> failures, String description, String expected, String actual, boolean sameFailure)
    {
        if (!expected.equals(actual) || (expected.startsWith(FAILED) && !sameFailure))
            failures.add(String.format("%s: expected %s but got %s", description, expected, actual));
    }

    /**
     * @return the sorted primary keys of the selected rows, {@link #REJECTED} if the query is invalid, or
     * {@link #FAILED} and the exception if it fails otherwise
     */
    private String select(String table, String where, boolean allowFiltering, Object... values)
    {
        String query = String.format("SELECT pk, c FROM %s.%s WHERE %s%s",
                                     KEYSPACE, table, where, allowFiltering ? " ALLOW FILTERING" : "");
        try
        {
            UntypedResultSet rows = executeFormattedQuery(query, values);
            List<String> keys = new ArrayList<>();
            for (UntypedResultSet.Row row : rows)
                keys.add(row.getInt("pk") + ":" + ByteBufferUtil.bytesToHex(row.getBytes("c")));
            keys.sort(String::compareTo);
            return keys.toString();
        }
        catch (InvalidRequestException e)
        {
            return REJECTED;
        }
        catch (Throwable t)
        {
            return FAILED + ' ' + t;
        }
    }

    private static String describe(Object[] values)
    {
        return Arrays.stream(values)
                     .map(v -> v instanceof ByteBuffer && !((ByteBuffer) v).hasRemaining() ? "<empty>" : String.valueOf(v))
                     .collect(Collectors.joining(", ", "(", ")"));
    }

    private static class Tables
    {
        final String ascFiltered;
        final String descFiltered;
        final String ascIndexed;
        final String descIndexed;
        final List<String> all;

        Tables(String ascFiltered, String descFiltered, String ascIndexed, String descIndexed)
        {
            this.ascFiltered = ascFiltered;
            this.descFiltered = descFiltered;
            this.ascIndexed = ascIndexed;
            this.descIndexed = descIndexed;
            this.all = Arrays.asList(ascFiltered, descFiltered, ascIndexed, descIndexed);
        }
    }
}
