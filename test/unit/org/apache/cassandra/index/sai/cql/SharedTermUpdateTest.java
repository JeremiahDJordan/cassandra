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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertTrue;

/**
 * Checks that an update of a row in the memtable doesn't remove from the memtable index a term that the row still
 * holds: in another element of a collection, which the update doesn't give to the index, or in the new value, when
 * the old and new values differ in bytes but map to the same term. Each scenario writes to its own partition, in a
 * table with indexes and in the same table without, and the queries must give the same results on both, with
 * ALLOW FILTERING, from the memtable and after flush.
 */
public class SharedTermUpdateTest extends SAITester
{
    @Test
    public void testDuplicateElements() throws Throwable
    {
        String schema = "CREATE TABLE %s (pk int PRIMARY KEY, l list<text>, mv map<text, text>, st set<text>)";
        String indexed = createTable(schema);
        for (String target : new String[]{ "l", "VALUES(mv)", "st" })
            createIndex(String.format("CREATE CUSTOM INDEX ON %s.%s(%s) USING 'StorageAttachedIndex'", KEYSPACE, indexed, target));
        String filtered = createTable(schema);

        for (String table : new String[]{ indexed, filtered })
        {
            for (String value : new String[]{ "a", "" })
            {
                int base = value.isEmpty() ? 10 : 0;
                // two equal elements, one of them replaced
                writeWithValue(table, value, "INSERT INTO %s (pk, l, mv) VALUES (?, [$, $], {'k1': $, 'k2': $})", base + 1);
                write(table, "UPDATE %s SET l[1] = 'x', mv['k2'] = 'x' WHERE pk = ?", base + 1);
                // two equal elements, one of them removed
                writeWithValue(table, value, "INSERT INTO %s (pk, l, mv) VALUES (?, [$, 'x', $], {'k1': $, 'k2': $})", base + 2);
                write(table, "UPDATE %s SET mv = mv - {'k2'} WHERE pk = ?", base + 2);
                write(table, "DELETE l[2] FROM %s WHERE pk = ?", base + 2);
                // an element added equal to another, then replaced
                writeWithValue(table, value, "INSERT INTO %s (pk, l, mv) VALUES (?, [$], {'k1': $})", base + 3);
                writeWithValue(table, value, "UPDATE %s SET l = l + [$], mv['k2'] = $ WHERE pk = ?", base + 3);
                write(table, "UPDATE %s SET l[1] = 'x', mv['k2'] = 'x' WHERE pk = ?", base + 3);
                // three equal elements, two of them replaced, one at a time
                writeWithValue(table, value, "INSERT INTO %s (pk, l, mv) VALUES (?, [$, $, $], {'k1': $, 'k2': $, 'k3': $})", base + 4);
                write(table, "UPDATE %s SET l[0] = 'x', mv['k1'] = 'x' WHERE pk = ?", base + 4);
                write(table, "UPDATE %s SET l[2] = 'y', mv['k3'] = 'y' WHERE pk = ?", base + 4);
                // two equal elements, both replaced: the value goes
                writeWithValue(table, value, "INSERT INTO %s (pk, l, mv) VALUES (?, [$, $], {'k1': $, 'k2': $})", base + 5);
                write(table, "UPDATE %s SET l[0] = 'x', mv['k1'] = 'x' WHERE pk = ?", base + 5);
                write(table, "UPDATE %s SET l[1] = 'y', mv['k2'] = 'y' WHERE pk = ?", base + 5);
                // an element replaced by an equal one
                writeWithValue(table, value, "INSERT INTO %s (pk, l, mv) VALUES (?, [$], {'k1': $})", base + 6);
                writeWithValue(table, value, "UPDATE %s SET l[0] = $, mv['k1'] = $ WHERE pk = ?", base + 6);
                // a set, whose elements are distinct: the value added and removed
                writeWithValue(table, value, "INSERT INTO %s (pk, st) VALUES (?, {$, 'x'})", base + 7);
                writeWithValue(table, value, "UPDATE %s SET st = st - {$} WHERE pk = ?", base + 7);
                write(table, "INSERT INTO %s (pk, st) VALUES (?, {'x'})", base + 8);
                writeWithValue(table, value, "UPDATE %s SET st = st + {$} WHERE pk = ?", base + 8);
            }
        }

        List<String> failures = new ArrayList<>();
        String[] restrictions = { "l CONTAINS ?", "mv CONTAINS ?", "st CONTAINS ?",
                                  "l NOT CONTAINS ?", "mv NOT CONTAINS ?", "st NOT CONTAINS ?" };
        Object[] values = { "a", "", "x", "y" };
        checkQueries("memtable", indexed, filtered, restrictions, values, failures);
        flush(KEYSPACE, indexed);
        flush(KEYSPACE, filtered);
        checkQueries("sstable", indexed, filtered, restrictions, values, failures);
        assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
    }

    @Test
    public void testSameTermValues() throws Throwable
    {
        String schema = "CREATE TABLE %s (pk int PRIMARY KEY, d decimal, i varint, t text)";
        String indexed = createTable(schema);
        createIndex(String.format("CREATE CUSTOM INDEX ON %s.%s(d) USING 'StorageAttachedIndex'", KEYSPACE, indexed));
        createIndex(String.format("CREATE CUSTOM INDEX ON %s.%s(i) USING 'StorageAttachedIndex'", KEYSPACE, indexed));
        createIndex(String.format("CREATE CUSTOM INDEX ON %s.%s(t) USING 'StorageAttachedIndex' " +
                                  "WITH OPTIONS = {'index_analyzer': 'standard'}", KEYSPACE, indexed));
        String filtered = createTable(schema);

        // Varints that differ beyond the bytes SAI indexes
        String big1 = '1' + "0".repeat(60) + '1';
        String big2 = '1' + "0".repeat(60) + '2';
        for (String table : new String[]{ indexed, filtered })
        {
            // a decimal whose scale changes
            write(table, "INSERT INTO %s (pk, d) VALUES (1, 1.0)");
            write(table, "UPDATE %s SET d = 1.000000000000 WHERE pk = 1");
            // a long varint whose last digit changes
            write(table, "INSERT INTO %s (pk, i) VALUES (2, " + big1 + ')');
            write(table, "UPDATE %s SET i = " + big2 + " WHERE pk = 2");
            // an analyzed text keeping a word
            write(table, "INSERT INTO %s (pk, t) VALUES (3, 'apple pie')");
            write(table, "UPDATE %s SET t = 'apple crust' WHERE pk = 3");
            // an analyzed text with a word twice, keeping it once
            write(table, "INSERT INTO %s (pk, t) VALUES (4, 'apple apple pie')");
            write(table, "UPDATE %s SET t = 'apple crust' WHERE pk = 4");
        }

        List<String> failures = new ArrayList<>();
        String[] restrictions = { "d = 1.0", "d = 1.000000000000", "d >= 1.0 AND d <= 1.0", "d != 1.0",
                                  "i = " + big2, "i >= " + big1, "i = " + big1 };
        for (String source : new String[]{ "memtable", "sstable" })
        {
            if (source.equals("sstable"))
            {
                flush(KEYSPACE, indexed);
                flush(KEYSPACE, filtered);
            }
            checkQueries(source, indexed, filtered, restrictions, new Object[]{ null }, failures);
            // The unindexed table doesn't support the analyzer's ':'
            checkQuery(source, indexed, "t : 'apple'", "[3, 4]", failures);
            checkQuery(source, indexed, "t : 'crust'", "[3, 4]", failures);
            checkQuery(source, indexed, "t : 'pie'", "[]", failures);
        }
        assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
    }

    private void write(String table, String statement, Object... values)
    {
        executeFormattedQuery(String.format(statement, KEYSPACE + '.' + table), values);
    }

    /** Writes the statement with the given text value in place of each {@code $}. */
    private void writeWithValue(String table, String value, String statement, Object... values)
    {
        write(table, statement.replace("$", '\'' + value + '\''), values);
    }

    /** Runs each restriction with each value, or, for a null value, with no bind marker. */
    private void checkQueries(String source, String indexed, String filtered, String[] restrictions, Object[] values,
                              List<String> failures)
    {
        for (String restriction : restrictions)
        {
            for (Object value : values)
            {
                String expected = select(filtered, restriction, value);
                String actual = select(indexed, restriction, value);
                if (!expected.equals(actual) || expected.startsWith("failed"))
                    failures.add(String.format("[%s] WHERE %s with '%s': expected %s but got %s",
                                               source, restriction, value, expected, actual));
            }
        }
    }

    private void checkQuery(String source, String table, String restriction, String expected, List<String> failures)
    {
        String actual = select(table, restriction, null);
        if (!expected.equals(actual))
            failures.add(String.format("[%s] WHERE %s: expected %s but got %s", source, restriction, expected, actual));
    }

    private String select(String table, String where, Object value)
    {
        String query = String.format("SELECT pk FROM %s.%s WHERE %s ALLOW FILTERING", KEYSPACE, table, where);
        try
        {
            UntypedResultSet rows = value == null ? executeFormattedQuery(query) : executeFormattedQuery(query, value);
            List<Integer> keys = new ArrayList<>();
            for (UntypedResultSet.Row row : rows)
                keys.add(row.getInt("pk"));
            keys.sort(Integer::compareTo);
            return keys.toString();
        }
        catch (Throwable t)
        {
            return "failed " + t;
        }
    }
}
