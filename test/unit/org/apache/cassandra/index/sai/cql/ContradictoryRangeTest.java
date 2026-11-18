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

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertTrue;

/**
 * Checks range restrictions that match no value, because their lower bound is above their upper bound, or because
 * their bounds are equal and one of them is exclusive, such as {@code v >= 5 AND v < 3} or {@code v > 5 AND v <= 5}.
 * They must match no row, like the same queries without an index, from the memtable and from SSTables, on regular
 * columns and on ASC and DESC clustering columns, with the values in the query or bound to markers.
 */
public class ContradictoryRangeTest extends SAITester
{
    private static final String[] DESCRIPTIONS = { "ASC with index", "ASC without index",
                                                   "DESC with index", "DESC without index" };

    private static final String[] RANGES = { "%1$s >= %2$s AND %1$s < %3$s",
                                             "%1$s > %2$s AND %1$s <= %3$s",
                                             "%1$s > %2$s AND %1$s < %3$s",
                                             "%1$s >= %2$s AND %1$s <= %3$s" };

    @Test
    public void testInt()
    {
        checkContradictoryRanges("int", "5", "3");
    }

    @Test
    public void testBigint()
    {
        checkContradictoryRanges("bigint", "5", "3");
    }

    @Test
    public void testDouble()
    {
        checkContradictoryRanges("double", "5.5", "3.5");
    }

    private void checkContradictoryRanges(String type, String high, String low)
    {
        List<String> tables = new ArrayList<>();
        for (String order : new String[]{ "ASC", "DESC" })
        {
            for (boolean indexed : new boolean[]{ true, false })
            {
                String table = createTable(String.format("CREATE TABLE %%s (pk int, c %s, v %s, PRIMARY KEY (pk, c)) " +
                                                         "WITH CLUSTERING ORDER BY (c %s)", type, type, order));
                if (indexed)
                {
                    createIndex(String.format("CREATE CUSTOM INDEX ON %s.%s(c) USING 'StorageAttachedIndex'", KEYSPACE, table));
                    createIndex(String.format("CREATE CUSTOM INDEX ON %s.%s(v) USING 'StorageAttachedIndex'", KEYSPACE, table));
                }
                tables.add(table);
            }
        }

        // The values 0 to 9, or 0.5 to 9.5 for doubles
        for (String table : tables)
        {
            for (int i = 0; i < 10; i++)
            {
                String value = "double".equals(type) ? i + ".5" : String.valueOf(i);
                executeFormattedQuery(String.format("INSERT INTO %s.%s (pk, c, v) VALUES (?, %s, %s)",
                                                    KEYSPACE, table, value, value), i);
            }
        }

        List<String> failures = new ArrayList<>();
        checkQueries("memtable", type, tables, high, low, failures);
        for (String table : tables)
            flush(KEYSPACE, table);
        checkQueries("sstable", type, tables, high, low, failures);

        assertTrue(String.format("%d failures for %s:%n%s", failures.size(), type, String.join("\n", failures)),
                   failures.isEmpty());
    }

    private void checkQueries(String source, String type, List<String> tables, String high, String low, List<String> failures)
    {
        for (String column : new String[]{ "c", "v" })
        {
            for (String range : RANGES)
            {
                // A range from the high value down to the low one, a range from the high value to itself, and, for
                // comparison, a range from the low value up to the high one
                for (String[] bounds : new String[][]{ { high, low }, { high, high }, { low, high } })
                {
                    checkQuery(source, type, tables, String.format(range, column, bounds[0], bounds[1]), failures);
                    checkQuery(source, type, tables, String.format(range, column, "?", "?"), failures, bounds[0], bounds[1]);
                }
            }
        }
    }

    private void checkQuery(String source, String type, List<String> tables, String where, List<String> failures, String... values)
    {
        // tables: ASC indexed, ASC unindexed, DESC indexed, DESC unindexed
        String expected = select(type, tables.get(1), where, values);
        for (int t : new int[]{ 0, 2, 3 })
        {
            String actual = select(type, tables.get(t), where, values);
            if (!expected.equals(actual))
                failures.add(String.format("[%s] WHERE %s with %s on %s: expected %s but got %s",
                                           source, where, String.join(", ", values), DESCRIPTIONS[t], expected, actual));
        }
    }

    private static Object bindValue(String type, String value)
    {
        switch (type)
        {
            case "int":
                return Integer.parseInt(value);
            case "bigint":
                return Long.parseLong(value);
            case "double":
                return Double.parseDouble(value);
            default:
                throw new IllegalArgumentException(type);
        }
    }

    private String select(String type, String table, String where, String... values)
    {
        String query = String.format("SELECT pk FROM %s.%s WHERE %s ALLOW FILTERING", KEYSPACE, table, where);
        try
        {
            Object[] bound = new Object[values.length];
            for (int i = 0; i < values.length; i++)
                bound[i] = bindValue(type, values[i]);
            UntypedResultSet rows = executeFormattedQuery(query, bound);
            List<Integer> keys = new ArrayList<>();
            for (UntypedResultSet.Row row : rows)
                keys.add(row.getInt("pk"));
            keys.sort(Integer::compareTo);
            return keys.toString();
        }
        catch (Throwable t)
        {
            return t.getClass().getSimpleName();
        }
    }
}
