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

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;

import static org.junit.Assert.assertTrue;

/**
 * NOT CONTAINS, NOT CONTAINS KEY and != on indexed columns whose index holds stale entries (values a row no longer
 * holds, after a TTL expired or an update overwrote a collection) return what ALLOW FILTERING returns on the same data
 * without indexes, from the memtable, after flush and after compaction.
 */
public class NotContainsStaleEntriesTest extends SAITester
{
    private static final String SCHEMA = "CREATE TABLE %s (pk int PRIMARY KEY, l list<text>, st set<text>, " +
                                         "mk map<text, text>, mv map<text, text>, n int, x int)";
    // The values whose stale entries the scenarios leave, the empty value included
    private static final String[] VALUES = { "b", "" };
    private static final String[] TARGETS = { "l", "st", "KEYS(mk)", "VALUES(mv)", "n" };

    @Test
    public void testStaleEntries() throws Throwable
    {
        String indexed = createIndexedTable();
        String filtered = createTable(SCHEMA);

        for (String table : new String[]{ indexed, filtered })
        {
            for (String value : VALUES)
            {
                int base = value.isEmpty() ? 10 : 0;
                // a collection with a value added, then overwritten without it
                write(table, "INSERT INTO %s (pk, l, st, mk, mv) VALUES (?, ['a'], {'a'}, {'a': 'x'}, {'k': 'a'})", base + 1);
                writeWithValue(table, value, "UPDATE %s SET l = l + [$], st = st + {$}, mk[$] = 'x', mv['k2'] = $ WHERE pk = ?", base + 1);
                write(table, "UPDATE %s SET l = ['a'], st = {'a'}, mk = {'a': 'x'}, mv = {'k': 'a'} WHERE pk = ?", base + 1);
                // a collection with a value added with a TTL, expiring
                write(table, "INSERT INTO %s (pk, l, st, mk, mv) VALUES (?, ['a'], {'a'}, {'a': 'x'}, {'k': 'a'})", base + 2);
                writeWithValue(table, value, "UPDATE %s USING TTL 1 SET l = l + [$], st = st + {$}, mk[$] = 'x', mv['k2'] = $ WHERE pk = ?", base + 2);
                // a collection with a value removed (the update gives the old element: no stale entry)
                writeWithValue(table, value, "INSERT INTO %s (pk, l, st, mk, mv) VALUES (?, ['a', $], {'a', $}, {'a': 'x', $: 'x'}, {'k': 'a', 'k2': $})", base + 3);
                writeWithValue(table, value, "UPDATE %s SET l = l - [$], st = st - {$}, mk = mk - {$}, mv = mv - {'k2'} WHERE pk = ?", base + 3);
                // a collection written by updates only, with the value added, then overwritten without it
                write(table, "UPDATE %s SET l = ['a'], st = {'a'}, mk = {'a': 'x'}, mv = {'k': 'a'} WHERE pk = ?", base + 4);
                writeWithValue(table, value, "UPDATE %s SET l = l + [$], st = st + {$}, mk[$] = 'x', mv['k2'] = $ WHERE pk = ?", base + 4);
                write(table, "UPDATE %s SET l = ['a'], st = {'a'}, mk = {'a': 'x'}, mv = {'k': 'a'} WHERE pk = ?", base + 4);
                // a whole collection deleted, then written without the value
                writeWithValue(table, value, "INSERT INTO %s (pk, l, st, mk, mv) VALUES (?, [$], {$}, {$: 'x'}, {'k': $})", base + 5);
                write(table, "DELETE l, st, mk, mv FROM %s WHERE pk = ?", base + 5);
                write(table, "UPDATE %s SET l = ['a'], st = {'a'}, mk = {'a': 'x'}, mv = {'k': 'a'} WHERE pk = ?", base + 5);

                if (value.isEmpty())
                    continue;
                // a regular column (int, whose index serves !=): 2 with a TTL, expiring, and the row then written with 1
                write(table, "INSERT INTO %s (pk, n, x) VALUES (?, 2, 0) USING TTL 1", base + 6);
                // a regular column: 2, and the row deleted then inserted with 1
                write(table, "INSERT INTO %s (pk, n) VALUES (?, 2)", base + 7);
                write(table, "DELETE FROM %s WHERE pk = ?", base + 7);
                write(table, "INSERT INTO %s (pk, n) VALUES (?, 1)", base + 7);
                // a regular column: 2 with a TTL, expiring
                write(table, "INSERT INTO %s (pk, n) VALUES (?, 2) USING TTL 1", base + 8);
                // a regular column: 2, and the partition deleted then inserted with 1
                write(table, "INSERT INTO %s (pk, n) VALUES (?, 2)", base + 9);
                write(table, "DELETE FROM %s WHERE pk = ?", base + 9);
                write(table, "INSERT INTO %s (pk, n, x) VALUES (?, 1, 0)", base + 9);
            }
        }
        waitForExpiry();
        for (String table : new String[]{ indexed, filtered })
            write(table, "UPDATE %s SET n = 1 WHERE pk = 6");

        List<String> failures = new ArrayList<>();
        checkQueries("memtable", indexed, filtered, failures);
        flush(KEYSPACE, indexed, filtered);
        checkQueries("sstable", indexed, filtered, failures);
        compact(KEYSPACE, indexed, filtered);
        checkQueries("compacted sstable", indexed, filtered, failures);
        assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
    }

    /**
     * A TTL that expires after the SSTables are compacted leaves stale entries in the compacted SSTable index too.
     */
    @Test
    public void testExpiryAfterCompaction() throws Throwable
    {
        String indexed = createIndexedTable();
        String filtered = createTable(SCHEMA);

        for (String table : new String[]{ indexed, filtered })
        {
            for (String value : VALUES)
            {
                int pk = value.isEmpty() ? 2 : 1;
                write(table, "INSERT INTO %s (pk, l, st, mk, mv, x) VALUES (?, ['a'], {'a'}, {'a': 'x'}, {'k': 'a'}, 0)", pk);
                writeWithValue(table, value, "UPDATE %s USING TTL 3 SET l = l + [$], st = st + {$}, mk[$] = 'x', mv['k2'] = $, n = 2 WHERE pk = ?", pk);
            }
        }
        flush(KEYSPACE, indexed, filtered);
        compact(KEYSPACE, indexed, filtered);
        Thread.sleep(3100);

        List<String> failures = new ArrayList<>();
        checkQueries("compacted sstable, expired", indexed, filtered, failures);
        assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
    }

    /**
     * A list overwritten after an append, in a table with no other collection.
     */
    @Test
    public void testOverwrittenList() throws Throwable
    {
        String schema = "CREATE TABLE %s (pk int PRIMARY KEY, l list<text>, w int)";
        String indexed = createTable(schema);
        createIndex(String.format("CREATE CUSTOM INDEX ON %s.%s(l) USING 'StorageAttachedIndex'", KEYSPACE, indexed));
        String filtered = createTable(schema);
        for (String table : new String[]{ indexed, filtered })
        {
            for (String value : VALUES)
            {
                int pk = value.isEmpty() ? 2 : 1;
                write(table, "UPDATE %s SET l = ['a'] WHERE pk = ?", pk);
                writeWithValue(table, value, "UPDATE %s SET l = l + [$] WHERE pk = ?", pk);
                write(table, "UPDATE %s SET l = ['a'] WHERE pk = ?", pk);
            }
        }

        List<String> failures = new ArrayList<>();
        for (String source : new String[]{ "memtable", "sstable" })
        {
            if (source.equals("sstable"))
                flush(KEYSPACE, indexed, filtered);
            for (String value : new String[]{ "b", "", "a" })
                for (String restriction : new String[]{ "l NOT CONTAINS ?", "l CONTAINS ?" })
                    checkQuery(source, indexed, filtered, restriction, value, failures);
        }
        assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
    }

    /**
     * The search returns all the rows of the index for NOT CONTAINS and NOT CONTAINS KEY, and so must the estimates
     * the query planner uses.
     */
    @Test
    public void testEstimates()
    {
        String table = createIndexedTable();
        int rows = 10;
        for (int pk = 0; pk < rows; pk++)
            write(table, "INSERT INTO %s (pk, l, st, mk, mv) VALUES (?, ['a'], {'a'}, {'a': 'x'}, {'k': 'a'})", pk);
        for (boolean memtable : new boolean[]{ true, false })
        {
            if (!memtable)
                flush(KEYSPACE, table);
            for (String target : new String[]{ "l", "st", "mv" })
                assertTrue(target, estimate(table, target, Operator.NOT_CONTAINS, "a", memtable) >= rows);
            assertTrue("mk", estimate(table, "mk", Operator.NOT_CONTAINS_KEY, "a", memtable) >= rows);
        }
    }

    private static long estimate(String table, String column, Operator operator, String value, boolean memtable)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        long estimate = 0;
        for (Index index : cfs.indexManager.listIndexes())
        {
            IndexContext context = ((StorageAttachedIndex) index).getIndexContext();
            if (!context.getColumnName().equals(column))
                continue;
            Expression expression = new Expression(context).add(operator, UTF8Type.instance.decompose(value));
            if (memtable)
                for (MemtableIndex memtableIndex : context.getLiveMemtables().values())
                    estimate += memtableIndex.estimateMatchingRowsCount(expression);
            else
                for (SSTableIndex sstableIndex : context.getView().getIndexes())
                    estimate += sstableIndex.estimateMatchingRowsCount(expression);
        }
        return estimate;
    }

    private String createIndexedTable()
    {
        String indexed = createTable(SCHEMA);
        for (String target : TARGETS)
            createIndex(String.format("CREATE CUSTOM INDEX ON %s.%s(%s) USING 'StorageAttachedIndex'", KEYSPACE, indexed, target));
        return indexed;
    }

    private static void waitForExpiry() throws InterruptedException
    {
        Thread.sleep(2100);
    }

    private void write(String table, String statement, Object... values)
    {
        executeFormattedQuery(String.format(statement, KEYSPACE + '.' + table), values);
    }

    /**
     * Writes with the given text value in place of each {@code $} in the statement.
     */
    private void writeWithValue(String table, String value, String statement, Object... values)
    {
        write(table, statement.replace("$", '\'' + value + '\''), values);
    }

    private void checkQueries(String source, String indexed, String filtered, List<String> failures)
    {
        for (String value : new String[]{ "b", "", "a" })
            for (String restriction : new String[]{ "l NOT CONTAINS ?", "st NOT CONTAINS ?", "mk NOT CONTAINS KEY ?",
                                                    "mv NOT CONTAINS ?", "l CONTAINS ?", "st CONTAINS ?",
                                                    "mk CONTAINS KEY ?", "mv CONTAINS ?" })
                checkQuery(source, indexed, filtered, restriction, value, failures);
        for (int value : new int[]{ 1, 2 })
            for (String restriction : new String[]{ "n != ?", "n = ?" })
                checkQuery(source, indexed, filtered, restriction, value, failures);
    }

    private void checkQuery(String source, String indexed, String filtered, String restriction, Object value, List<String> failures)
    {
        String expected = select(filtered, restriction, value);
        String actual = select(indexed, restriction, value);
        if (!expected.equals(actual))
            failures.add(String.format("[%s] WHERE %s with '%s': expected %s but got %s",
                                       source, restriction, value, expected, actual));
    }

    private String select(String table, String where, Object value)
    {
        String query = String.format("SELECT pk FROM %s.%s WHERE %s ALLOW FILTERING", KEYSPACE, table, where);
        UntypedResultSet rows = executeFormattedQuery(query, value);
        List<Integer> keys = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            keys.add(row.getInt("pk"));
        keys.sort(Integer::compareTo);
        return keys.toString();
    }
}
