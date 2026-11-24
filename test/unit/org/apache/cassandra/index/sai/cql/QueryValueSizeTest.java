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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the coordinator of an SAI range query rejects the values that can't be serialized in the read command sent
 * to the replicas, rather than timing out, that a single-partition query served locally isn't affected, that SAI
 * rejects the map entry keys and values that its composite values and entries index terms can't encode, and that it
 * filters the unindexed map entries that those can't hold.
 */
public class QueryValueSizeTest extends SAITester
{
    private static final int MAX = FBUtilities.MAX_UNSIGNED_SHORT;

    @Test
    public void testText() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, u text, n int, PRIMARY KEY (k, c))");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(n) USING 'sai'");
        String max = "a".repeat(MAX);
        String huge = "a".repeat(MAX + 1);
        execute("INSERT INTO %s (k, c, v, u, n) VALUES (0, 0, 'a', ?, 0)", max);
        execute("INSERT INTO %s (k, c, v, u, n) VALUES (1, 0, 'b', ?, 0)", huge);
        execute("INSERT INTO %s (k, c, v, u, n) VALUES (1, 1, 'c', 'c', 0)");

        beforeAndAfterFlush(() -> {
            // the largest value that can be serialized is accepted, whatever the index holds
            assertRows(execute("SELECT k FROM %s WHERE v = ?", max));
            assertRows(execute("SELECT k FROM %s WHERE n = 0 AND u = ? ALLOW FILTERING", max), row(0));

            // an index query on a single partition served by this node isn't serialized
            assertRows(execute("SELECT c FROM %s WHERE k = 1 AND n = 0 AND u = ? ALLOW FILTERING", huge), row(0));
            assertRows(execute("SELECT c FROM %s WHERE k = 1 AND n = 0 AND u > ? ALLOW FILTERING", huge), row(1));
            assertRows(execute("SELECT c FROM %s WHERE k = 1 AND n = 0 AND u != ? ALLOW FILTERING", huge), row(1));
            assertRows(execute("SELECT c FROM %s WHERE k = 1 AND v IN ('c', ?)", huge), row(1));
            assertRows(execute("SELECT c FROM %s WHERE k = 1 AND c >= 0 AND n = 0 AND u = ? ALLOW FILTERING", huge), row(0));

            // range reads are always sent through messaging
            assertTooLarge("Query value", "v", MAX + 1, "SELECT k FROM %s WHERE v = ?", huge);
            assertTooLarge("Query value", "v", MAX + 1, "SELECT k FROM %s WHERE v LIKE ?", huge + '%'); // the prefix, without the %
            // on any column of the index query, indexed or not
            assertTooLarge("Query value", "u", MAX + 1, "SELECT k FROM %s WHERE n = 0 AND u = ? ALLOW FILTERING", huge);
            assertTooLarge("Query value", "v", MAX + 1, "SELECT k FROM %s WHERE n = 0 AND v != ? ALLOW FILTERING", huge);
            assertTooLarge("Query value", "v", MAX + 1, "SELECT k FROM %s WHERE n = 0 AND (v = ? OR n = 1)", huge);
            assertTooLarge("Query value", "u", MAX + 1, "SELECT k FROM %s WHERE token(k) >= token(0) AND n = 0 AND u = ? ALLOW FILTERING", huge);
        });
    }

    @Test
    public void testInList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, n int)");
        createIndex("CREATE INDEX ON %s(n) USING 'sai'");
        execute("INSERT INTO %s (k, n) VALUES (0, 0)");

        // the packed list holds a 4-byte count, then each value with its 4-byte length
        assertRows(execute("SELECT k FROM %s WHERE n IN ?", ints((MAX - 4) / 8)), row(0));
        List<Integer> tooMany = ints((MAX - 4) / 8 + 1);
        assertTooLarge("IN list", "n", 4 + 8 * tooMany.size(), "SELECT k FROM %s WHERE n IN ?", tooMany);
    }

    @Test
    public void testBlob() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, b blob, n int)");
        createIndex("CREATE INDEX ON %s(b) USING 'sai'");
        createIndex("CREATE INDEX ON %s(n) USING 'sai'");
        execute("INSERT INTO %s (k, b, n) VALUES (0, 0x01, 0)");

        ByteBuffer huge = blob(MAX + 1);
        assertTooLarge("Query value", "b", MAX + 1, "SELECT k FROM %s WHERE b = ?", huge);
        assertTooLarge("Query value", "b", MAX + 1, "SELECT k FROM %s WHERE n = 0 AND b < ? ALLOW FILTERING", huge);
        assertRows(execute("SELECT k FROM %s WHERE n = 0 AND b < ? ALLOW FILTERING", blob(MAX)), row(0));
    }

    @Test
    public void testMapEntry() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, m map<text, text>, e map<text, text>, n int, PRIMARY KEY (k, c))");
        createIndex("CREATE INDEX ON %s(n) USING 'sai'");
        String entriesIndex = createIndex("CREATE INDEX ON %s(ENTRIES(e)) USING 'sai'");
        execute("INSERT INTO %s (k, c, m, e, n) VALUES (0, 0, {'a': 'b'}, {'a': 'b'}, 0)");
        execute("INSERT INTO %s (k, c, m, e, n) VALUES (0, 1, {'a': 'c'}, {'a': 'c'}, 0)");
        execute("INSERT INTO %s (k, c, m, e, n) VALUES (1, 0, {'x': 'b'}, {'x': 'b'}, 1)");

        String max = "a".repeat(MAX);
        String huge = "a".repeat(MAX + 1);
        beforeAndAfterFlush(() -> {
            // a map entry's key and value are serialized separately: on a filtered map, each can be up to the limit
            assertRows(execute("SELECT k FROM %s WHERE n = 0 AND m[?] = ? ALLOW FILTERING", max, max));
            assertTooLarge("Map key", "m", MAX + 1, "SELECT k FROM %s WHERE n = 0 AND m[?] = 'b' ALLOW FILTERING", huge);
            assertTooLarge("Map value", "m", MAX + 1, "SELECT k FROM %s WHERE n = 0 AND m['a'] = ? ALLOW FILTERING", huge);
            assertTooLarge("Map key", "e", MAX + 1, "SELECT k FROM %s WHERE e[?] = 'b'", huge);
            assertTooLarge("Map value", "e", MAX + 1, "SELECT k FROM %s WHERE e['a'] = ?", huge);

            // a single-partition read isn't serialized, but SAI rejects the map entry comparisons that an entry term
            // can't hold, with a key or value of more than 65535 bytes, indexed or not
            assertRows(execute("SELECT c FROM %s WHERE k = 0 AND n = 0 AND m[?] = ? ALLOW FILTERING", max, max));
            assertMapEntryTooLarge("Map key", "m", "SELECT c FROM %s WHERE k = 0 AND n = 0 AND m[?] = 'b' ALLOW FILTERING", huge);
            assertMapEntryTooLarge("Map value", "m", "SELECT c FROM %s WHERE k = 0 AND n = 0 AND m['a'] = ? ALLOW FILTERING", huge);
            String withoutEntriesIndex = " ALLOW FILTERING WITH excluded_indexes = {" + entriesIndex + '}';
            assertMapEntryTooLarge("Map key", "e", "SELECT c FROM %s WHERE k = 0 AND n = 0 AND e[?] = 'b'" + withoutEntriesIndex, huge);
            assertMapEntryTooLarge("Map value", "e", "SELECT c FROM %s WHERE k = 0 AND n = 0 AND e['a'] = ?" + withoutEntriesIndex, huge);
            assertRows(execute("SELECT c FROM %s WHERE k = 0 AND n = 0 AND e[?] = 'b'" + withoutEntriesIndex, max));

            // the entries index's terms can't have a first component (the key) of 65535 bytes, but the value can
            assertSameAsFiltering("e[?] = 'b'", "m[?] = 'b'",
                                  "a".repeat(StorageAttachedIndex.MAX_COMPOSITE_FIRST_COMPONENT_SIZE));
            assertKeyTooLargeForEntriesIndex("SELECT c FROM %s WHERE e[?] = 'b'", max);
            assertKeyTooLargeForEntriesIndex("SELECT c FROM %s WHERE k = 0 AND e[?] = 'b'", max);
            assertKeyTooLargeForEntriesIndex("SELECT c FROM %s WHERE n = 1 OR e[?] = 'b'", max);
            for (String restriction : new String[]{ "%s['a'] = ?", "%s['a'] != ?", "%s['a'] > ?", "n = 0 OR %s['a'] = ?",
                                                    "k = 0 AND %s['a'] != ?" })
                assertSameAsFiltering(String.format(restriction, 'e'), String.format(restriction, 'm'), max);
            assertRows(execute("SELECT c FROM %s WHERE e['a'] = 'b'"), row(0));
        });
    }

    @Test
    public void testFilteredMapEntryLargerThanEntryTerm() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, m map<text, text>, s map<text, text> static, n int, PRIMARY KEY (k, c))");
        createIndex("CREATE INDEX ON %s(n) USING 'sai'");
        // the largest key (its first byte over 0x7F), a key over 32767 bytes, and a value too large for an entry term
        String maxKey = "\u00e9" + "a".repeat(MAX - 2);
        String longKey = "a".repeat(40000);
        String hugeValue = "x".repeat(MAX + 1);
        execute("INSERT INTO %s (k, c, m, s, n) VALUES (0, 0, ?, {'a': 'b'}, 0)", map("a", hugeValue));
        execute("INSERT INTO %s (k, c, m, n) VALUES (0, 1, ?, 0)", map(maxKey, "b"));
        execute("INSERT INTO %s (k, c, m, n) VALUES (0, 2, {'a': 'b'}, 0)");
        execute("INSERT INTO %s (k, c, m, n) VALUES (0, 3, {'a': 'b'}, 0)");
        execute("DELETE m['a'] FROM %s WHERE k = 0 AND c = 3");
        execute("INSERT INTO %s (k, c, n) VALUES (0, 4, 0)");
        execute("UPDATE %s USING TTL 1 SET m['a'] = 'b' WHERE k = 0 AND c = 4");
        Thread.sleep(2000);
        execute("INSERT INTO %s (k, c, m, n) VALUES (0, 5, ?, 0)", map(longKey, "b"));
        execute("INSERT INTO %s (k, c, m, s, n) VALUES (1, 0, ?, ?, 0)", map("a", hugeValue, "b", "b"), map("a", hugeValue));

        beforeAndAfterFlush(() -> {
            assertFilteredMapEntry("m['a'] > 'a'", null, row(0, 0), row(0, 2), row(1, 0));
            assertFilteredMapEntry("m['a'] >= 'x'", null, row(0, 0), row(1, 0));
            assertFilteredMapEntry("m['a'] < 'y'", null, row(0, 0), row(0, 2), row(1, 0));
            assertFilteredMapEntry("m['a'] != 'b'", null, row(0, 0), row(0, 1), row(0, 3), row(0, 4), row(0, 5), row(1, 0));
            assertFilteredMapEntry("m[?] = 'b'", maxKey, row(0, 1));
            assertFilteredMapEntry("m[?] > 'a'", maxKey, row(0, 1));
            assertFilteredMapEntry("m[?] = 'b'", longKey, row(0, 5));
            assertFilteredMapEntry("m[?] > 'a'", longKey, row(0, 5));
            assertFilteredMapEntry("m CONTAINS KEY ?", maxKey, row(0, 1));
            assertFilteredMapEntry("(m['a'] = 'b' OR m[?] = 'b')", maxKey, row(0, 1), row(0, 2));
            assertFilteredMapEntry("m['a'] > 'a' AND m['a'] < 'y'", null, row(0, 0), row(0, 2), row(1, 0));
            assertFilteredMapEntry("m['a'] = 'b' AND m['b'] != 'c'", null, row(0, 2));
            assertFilteredMapEntry("s['a'] > 'a'", null, row(0, 0), row(0, 1), row(0, 2), row(0, 3), row(0, 4), row(0, 5), row(1, 0));
        });
    }

    private void assertFilteredMapEntry(String restriction, Object value, Object[]... expected)
    {
        Object[] values = value == null ? new Object[0] : new Object[]{ value };
        // without a restriction on an indexed column, the query isn't served by SAI
        assertRowsIgnoringOrder(execute("SELECT k, c FROM %s WHERE " + restriction + " ALLOW FILTERING", values), expected);
        assertRowsIgnoringOrder(execute("SELECT k, c FROM %s WHERE n = 0 AND " + restriction + " ALLOW FILTERING", values), expected);
        Object[][] inPartition = Arrays.stream(expected).filter(r -> r[0].equals(0)).toArray(Object[][]::new);
        assertRowsIgnoringOrder(execute("SELECT k, c FROM %s WHERE k = 0 AND n = 0 AND " + restriction + " ALLOW FILTERING", values), inPartition);
    }

    private void assertTooLarge(String what, String column, int size, String query, Object... values)
    {
        assertThatThrownBy(() -> execute(query, values))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(String.format(StorageAttachedIndex.QUERY_VALUE_TOO_LARGE_ERROR_MESSAGE, what, column, size, MAX));
    }

    private void assertMapEntryTooLarge(String what, String column, String query, Object value)
    {
        assertThatThrownBy(() -> execute(query, value))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(String.format(StorageAttachedIndex.MAP_ENTRY_TOO_LARGE_ERROR_MESSAGE, what, column, MAX + 1, MAX));
    }

    private void assertKeyTooLargeForEntriesIndex(String query, Object value)
    {
        assertThatThrownBy(() -> execute(query, value))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(String.format(StorageAttachedIndex.MAP_KEY_TOO_LARGE_FOR_ENTRIES_INDEX_ERROR_MESSAGE,
                                          "Map key", "e", MAX, StorageAttachedIndex.MAX_COMPOSITE_FIRST_COMPONENT_SIZE));
    }

    private void assertSameAsFiltering(String indexed, String filtered, Object value)
    {
        Object[][] expected = getRows(execute("SELECT k, c FROM %s WHERE " + filtered + " ALLOW FILTERING", value));
        assertRowsIgnoringOrder(execute("SELECT k, c FROM %s WHERE " + indexed, value), expected);
    }

    private static List<Integer> ints(int count)
    {
        List<Integer> ints = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            ints.add(i);
        return ints;
    }

    private static ByteBuffer blob(int length)
    {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) 1);
        return ByteBuffer.wrap(bytes);
    }
}
