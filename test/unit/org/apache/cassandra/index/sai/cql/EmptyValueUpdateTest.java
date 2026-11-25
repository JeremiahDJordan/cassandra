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
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.junit.Assert.assertTrue;

/**
 * Checks that the memtable index follows the updates of a row from, to, and back to the empty value, which SAI
 * indexes for the non-analyzed literal types. Each scenario writes to its own partition, in the same memtable, to a
 * table with an index on each column and to the same table without indexes. The queries for the empty value and for
 * a non-empty value must give the same results on both, with {@code ALLOW FILTERING}, from the memtable and after
 * flush.
 */
public class EmptyValueUpdateTest extends SAITester
{
    private static final Object EMPTY = EMPTY_BYTE_BUFFER;

    @Test
    public void testText() throws Throwable
    {
        checkScalars("text", "a", "b");
    }

    @Test
    public void testAscii() throws Throwable
    {
        checkScalars("ascii", "a", "b");
    }

    @Test
    public void testBoolean() throws Throwable
    {
        checkScalars("boolean", true, false);
    }

    @Test
    public void testFrozenTuple() throws Throwable
    {
        checkScalars("frozen<tuple<int, text>>", tuple(1, "a"), tuple(2, ""));
    }

    @Test
    public void testFrozenUDT() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (i int, t text)");
        checkScalars("frozen<" + udt + '>', userType("i", 1, "t", "a"), userType("i", 2, "t", ""));
    }

    /**
     * Updates of a regular column v, a static column s, and rows of the clustering column c, between the empty value
     * and the non-empty values a and b.
     */
    private void checkScalars(String type, Object a, Object b) throws Throwable
    {
        String schema = String.format("CREATE TABLE %%s (pk int, c %s, v %s, s %s static, s2 int static, x int, " +
                                      "PRIMARY KEY (pk, c))", type, type, type);
        String indexed = createTable(schema);
        for (String column : new String[]{ "c", "v", "s" })
            createIndex(String.format("CREATE INDEX ON %s.%s(%s) USING 'sai'", KEYSPACE, indexed, column));
        String filtered = createTable(schema);

        for (String table : new String[]{ indexed, filtered })
            writeScalarScenarios(table, a, b);
        waitForExpiry();
        for (String table : new String[]{ indexed, filtered })
            writeScalarScenariosAfterExpiry(table, a);

        List<String> failures = new ArrayList<>();
        String[] restrictions = { "v = ?", "s = ?", "c = ?" };
        checkQueries("memtable", indexed, filtered, restrictions, a, b, failures);
        flush(KEYSPACE, indexed, filtered);
        checkQueries("sstable", indexed, filtered, restrictions, a, b, failures);
        assertTrue(String.format("%d failures for %s:%n%s", failures.size(), type, String.join("\n", failures)),
                   failures.isEmpty());
    }

    private void writeScalarScenarios(String table, Object a, Object b)
    {
        Object c = a; // the clustering value of the rows whose regular column is updated

        // regular column: from a value to the empty value
        write(table, "INSERT INTO %s (pk, c, v) VALUES (1, ?, ?)", c, a);
        write(table, "UPDATE %s SET v = ? WHERE pk = 1 AND c = ?", EMPTY, c);
        // regular column: from no value to the empty value
        write(table, "INSERT INTO %s (pk, c, x) VALUES (2, ?, 0)", c);
        write(table, "UPDATE %s SET v = ? WHERE pk = 2 AND c = ?", EMPTY, c);
        // regular column: the empty value, deleted, then written again
        write(table, "INSERT INTO %s (pk, c, v) VALUES (3, ?, ?)", c, EMPTY);
        write(table, "DELETE v FROM %s WHERE pk = 3 AND c = ?", c);
        write(table, "UPDATE %s SET v = ? WHERE pk = 3 AND c = ?", EMPTY, c);
        // regular column: a row with the empty value, deleted, then inserted again
        write(table, "INSERT INTO %s (pk, c, v) VALUES (4, ?, ?)", c, EMPTY);
        write(table, "DELETE FROM %s WHERE pk = 4 AND c = ?", c);
        write(table, "INSERT INTO %s (pk, c, v) VALUES (4, ?, ?)", c, EMPTY);
        // regular column: from the empty value to a value
        write(table, "INSERT INTO %s (pk, c, v) VALUES (5, ?, ?)", c, EMPTY);
        write(table, "UPDATE %s SET v = ? WHERE pk = 5 AND c = ?", a, c);
        // regular column: the empty value, deleted
        write(table, "INSERT INTO %s (pk, c, v) VALUES (6, ?, ?)", c, EMPTY);
        write(table, "DELETE v FROM %s WHERE pk = 6 AND c = ?", c);
        // regular column: from a value to another one, and to the empty value
        write(table, "INSERT INTO %s (pk, c, v) VALUES (7, ?, ?)", c, a);
        write(table, "UPDATE %s SET v = ? WHERE pk = 7 AND c = ?", b, c);
        write(table, "UPDATE %s SET v = ? WHERE pk = 7 AND c = ?", EMPTY, c);
        // regular column: the empty value with a TTL, expiring (see writeScalarScenariosAfterExpiry)
        write(table, "INSERT INTO %s (pk, c, v) VALUES (8, ?, ?) USING TTL 1", c, EMPTY);
        // regular column: from a value to the empty value with a TTL, expiring
        write(table, "INSERT INTO %s (pk, c, v, x) VALUES (9, ?, ?, 0)", c, a);
        write(table, "UPDATE %s USING TTL 1 SET v = ? WHERE pk = 9 AND c = ?", EMPTY, c);

        // static column: the same scenarios
        write(table, "INSERT INTO %s (pk, s) VALUES (11, ?)", a);
        write(table, "UPDATE %s SET s = ? WHERE pk = 11", EMPTY);
        // (the static row exists, with s2, so this is an update of the static row)
        write(table, "INSERT INTO %s (pk, s2) VALUES (12, 0)");
        write(table, "UPDATE %s SET s = ? WHERE pk = 12", EMPTY);
        write(table, "INSERT INTO %s (pk, s) VALUES (13, ?)", EMPTY);
        write(table, "DELETE s FROM %s WHERE pk = 13");
        write(table, "UPDATE %s SET s = ? WHERE pk = 13", EMPTY);
        write(table, "INSERT INTO %s (pk, s) VALUES (14, ?)", EMPTY);
        write(table, "DELETE FROM %s WHERE pk = 14");
        write(table, "INSERT INTO %s (pk, s) VALUES (14, ?)", EMPTY);
        write(table, "INSERT INTO %s (pk, s) VALUES (15, ?)", EMPTY);
        write(table, "UPDATE %s SET s = ? WHERE pk = 15", a);
        write(table, "INSERT INTO %s (pk, s) VALUES (16, ?)", EMPTY);
        write(table, "DELETE s FROM %s WHERE pk = 16");
        write(table, "INSERT INTO %s (pk, s, s2) VALUES (17, ?, 0)", a);
        write(table, "UPDATE %s USING TTL 1 SET s = ? WHERE pk = 17", EMPTY);

        // clustering column: a row with the empty value, deleted, then inserted again
        write(table, "INSERT INTO %s (pk, c, x) VALUES (21, ?, 0)", EMPTY);
        write(table, "DELETE FROM %s WHERE pk = 21 AND c = ?", EMPTY);
        write(table, "INSERT INTO %s (pk, c, x) VALUES (21, ?, 0)", EMPTY);
        // clustering column: a row with the empty value, updated
        write(table, "INSERT INTO %s (pk, c, x) VALUES (22, ?, 0)", EMPTY);
        write(table, "UPDATE %s SET x = 1 WHERE pk = 22 AND c = ?", EMPTY);
        // clustering column: a row with the empty value, deleted
        write(table, "INSERT INTO %s (pk, c, x) VALUES (23, ?, 0)", EMPTY);
        write(table, "DELETE FROM %s WHERE pk = 23 AND c = ?", EMPTY);
        // clustering column: a row with the empty value with a TTL, expiring
        write(table, "INSERT INTO %s (pk, c, x) VALUES (24, ?, 0) USING TTL 1", EMPTY);
    }

    /**
     * Writes after the TTLs of {@link #writeScalarScenarios} expired: the empty value, expired, written again.
     */
    private void writeScalarScenariosAfterExpiry(String table, Object a)
    {
        write(table, "UPDATE %s SET v = ? WHERE pk = 8 AND c = ?", EMPTY, a);
        write(table, "INSERT INTO %s (pk, c, x) VALUES (24, ?, 0)", EMPTY);
    }

    @Test
    public void testCollections() throws Throwable
    {
        String schema = "CREATE TABLE %s (pk int PRIMARY KEY, l list<text>, st set<text>, mk map<text, text>, " +
                        "mv map<text, text>, x int)";
        String indexed = createTable(schema);
        for (String target : new String[]{ "l", "st", "KEYS(mk)", "VALUES(mv)" })
            createIndex(String.format("CREATE INDEX ON %s.%s(%s) USING 'sai'", KEYSPACE, indexed, target));
        String filtered = createTable(schema);

        for (String table : new String[]{ indexed, filtered })
        {
            // a collection overwritten from a value to the empty value, and back
            write(table, "INSERT INTO %s (pk, l, st, mk, mv) VALUES (1, ['a'], {'a'}, {'a': 'x'}, {'k': 'a'})");
            write(table, "UPDATE %s SET l = [''], st = {''}, mk = {'': 'x'}, mv = {'k': ''} WHERE pk = 1");
            write(table, "INSERT INTO %s (pk, l, st, mk, mv) VALUES (2, [''], {''}, {'': 'x'}, {'k': ''})");
            write(table, "UPDATE %s SET l = ['a'], st = {'a'}, mk = {'a': 'x'}, mv = {'k': 'a'} WHERE pk = 2");
            // the empty value added to a collection that had none, or no collection
            write(table, "INSERT INTO %s (pk, l, st, mk, mv) VALUES (3, ['a'], {'a'}, {'a': 'x'}, {'k': 'a'})");
            write(table, "UPDATE %s SET l = l + [''], st = st + {''}, mk[''] = 'x', mv['k2'] = '' WHERE pk = 3");
            write(table, "INSERT INTO %s (pk, x) VALUES (4, 0)");
            write(table, "UPDATE %s SET l = l + [''], st = st + {''}, mk[''] = 'x', mv['k'] = '' WHERE pk = 4");
            // an element set to the empty value, and from it
            write(table, "INSERT INTO %s (pk, l, mv) VALUES (5, ['a', 'b'], {'k': 'a'})");
            write(table, "UPDATE %s SET l[0] = '', mv['k'] = '' WHERE pk = 5");
            write(table, "INSERT INTO %s (pk, l, mv) VALUES (6, ['', 'b'], {'k': ''})");
            write(table, "UPDATE %s SET l[0] = 'a', mv['k'] = 'a' WHERE pk = 6");
            // the empty value removed
            write(table, "INSERT INTO %s (pk, l, st, mk, mv) VALUES (7, ['', 'a'], {'', 'a'}, {'': 'x', 'a': 'x'}, {'k': '', 'k2': 'a'})");
            write(table, "UPDATE %s SET l = l - [''], st = st - {''}, mk = mk - {''}, mv = mv - {'k'} WHERE pk = 7");
            // the empty value added with a TTL, expiring
            write(table, "INSERT INTO %s (pk, l, st, mk, mv) VALUES (8, ['a'], {'a'}, {'a': 'x'}, {'k': 'a'})");
            write(table, "UPDATE %s USING TTL 1 SET l = l + [''], st = st + {''}, mk[''] = 'x', mv['k2'] = '' WHERE pk = 8");
        }
        waitForExpiry();
        // the empty value, expired, added again
        for (String table : new String[]{ indexed, filtered })
            write(table, "UPDATE %s SET l = l + [''], st = st + {''}, mk[''] = 'x', mv['k3'] = '' WHERE pk = 8");

        List<String> failures = new ArrayList<>();
        String[] restrictions = { "l CONTAINS ?", "st CONTAINS ?", "mk CONTAINS KEY ?", "mv CONTAINS ?" };
        checkQueries("memtable", indexed, filtered, restrictions, "", "a", failures);
        flush(KEYSPACE, indexed, filtered);
        checkQueries("sstable", indexed, filtered, restrictions, "", "a", failures);
        assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
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
     * Runs each restriction with the empty value, and with the values a and b.
     */
    private void checkQueries(String source, String indexed, String filtered, String[] restrictions, Object a, Object b,
                              List<String> failures)
    {
        for (String restriction : restrictions)
        {
            for (Object value : new Object[]{ EMPTY, a, b })
            {
                String expected = select(filtered, restriction, value);
                String actual = select(indexed, restriction, value);
                if (!expected.equals(actual))
                    failures.add(String.format("[%s] WHERE %s with %s: expected %s but got %s",
                                               source, restriction, describe(value), expected, actual));
            }
        }
    }

    private String select(String table, String where, Object value)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE %s ALLOW FILTERING", KEYSPACE, table, where);
        UntypedResultSet rows = executeFormattedQuery(query, value);
        List<String> keys = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            keys.add(row.getInt("pk") + ":" + (row.has("c") ? ByteBufferUtil.bytesToHex(row.getBytes("c")) : "-"));
        keys.sort(String::compareTo);
        return keys.toString();
    }

    private static String describe(Object value)
    {
        return value == EMPTY ? "<empty>" : String.valueOf(value);
    }
}
