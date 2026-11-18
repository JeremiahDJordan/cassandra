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
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * NOT CONTAINS and NOT CONTAINS KEY on a collection with an analyzed index return the complement of CONTAINS: the rows
 * none of whose elements holds all the words of the value.
 */
public class AnalyzedNotContainsTest extends SAITester
{
    private static final String[] VALUES = { "Johann Strauss", "Strauss Johann", "Johann", "johann johann", "Bach",
                                              "Johann Sebastian Bach", "Richard Strauss", "Debussy", "Strauss Debussy" };
    // The elements of the collections of the rows 1 to 7 (7 has no collection)
    private static final String[][] ELEMENTS = { { "Johann Strauss" },
                                                 { "Richard Strauss", "Johann Sebastian Bach" },
                                                 { "johann" },
                                                 { "strauss x johann" },
                                                 { "bach" },
                                                 { "" },
                                                 {} };

    @Test
    public void testList() throws Throwable
    {
        checkNotContains("list<text>", "v", "CONTAINS", "NOT CONTAINS", elements -> list((Object[]) elements));
    }

    @Test
    public void testSet() throws Throwable
    {
        checkNotContains("set<text>", "v", "CONTAINS", "NOT CONTAINS", elements -> set((Object[]) elements));
    }

    @Test
    public void testMapValues() throws Throwable
    {
        checkNotContains("map<int, text>", "VALUES(v)", "CONTAINS", "NOT CONTAINS", elements -> {
            Object[] entries = new Object[elements.length * 2];
            for (int i = 0; i < elements.length; i++)
            {
                entries[2 * i] = i;
                entries[2 * i + 1] = elements[i];
            }
            return map(entries);
        });
    }

    @Test
    public void testMapKeys() throws Throwable
    {
        checkNotContains("map<text, int>", "KEYS(v)", "CONTAINS KEY", "NOT CONTAINS KEY", elements -> {
            Object[] entries = new Object[elements.length * 2];
            for (int i = 0; i < elements.length; i++)
            {
                entries[2 * i] = elements[i];
                entries[2 * i + 1] = i;
            }
            return map(entries);
        });
    }

    /**
     * A non-tokenizing analyzer on map keys, and a row without a map.
     */
    @Test
    public void testCaseInsensitiveMapKeys() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<text, text>, x int)");
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(v)) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive': 'false' }");
        execute("INSERT INTO %s (k, v) VALUES (1, {'Johann': 'x'})");
        execute("INSERT INTO %s (k, v) VALUES (2, {'Claude': 'x'})");
        execute("INSERT INTO %s (k, x) VALUES (3, 0)");
        for (String source : new String[]{ "memtable", "sstable" })
        {
            if (source.equals("sstable"))
                flush();
            assertEquals(source, "[1]", select("SELECT k FROM %s WHERE v CONTAINS KEY 'johann'").toString());
            assertEquals(source, "[2, 3]", select("SELECT k FROM %s WHERE v NOT CONTAINS KEY 'johann'").toString());
            assertEquals(source, "[2, 3]", select("SELECT k FROM %s WHERE v NOT CONTAINS KEY 'johann' ALLOW FILTERING").toString());
        }
    }

    private void checkNotContains(String type, String target, String contains, String notContains,
                                  Function<String[], Object> maker) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ", x int)");
        createIndex("CREATE CUSTOM INDEX ON %s(" + target + ") USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        Set<Integer> all = new TreeSet<>();
        for (int i = 0; i < ELEMENTS.length; i++)
        {
            int k = i + 1;
            all.add(k);
            if (ELEMENTS[i].length == 0)
                execute("INSERT INTO %s (k, x) VALUES (?, 0)", k);
            else
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, maker.apply(ELEMENTS[i]));
        }

        List<String> failures = new ArrayList<>();
        for (String source : new String[]{ "memtable", "sstable" })
        {
            if (source.equals("sstable"))
                flush();

            for (String value : VALUES)
            {
                Set<Integer> positive = select("SELECT k FROM %s WHERE v " + contains + " ?", value);
                Set<Integer> negative = select("SELECT k FROM %s WHERE v " + notContains + " ?", value);
                Set<Integer> union = new TreeSet<>(positive);
                union.addAll(negative);
                Set<Integer> intersection = new TreeSet<>(positive);
                intersection.retainAll(negative);
                if (!union.equals(all) || !intersection.isEmpty())
                    failures.add(String.format("[%s] '%s': %s %s but %s %s", source, value, contains, positive, notContains, negative));
            }

            // A row matches if one of its elements holds both words
            check(failures, source, "SELECT k FROM %s WHERE v " + contains + " 'Johann Strauss'", "[1, 4]");
            check(failures, source, "SELECT k FROM %s WHERE v " + notContains + " 'Johann Strauss'", "[2, 3, 5, 6, 7]");
            check(failures, source, "SELECT k FROM %s WHERE v " + notContains + " 'johann johann'", "[5, 6, 7]");
            check(failures, source, "SELECT k FROM %s WHERE v " + notContains + " 'Johann'", "[5, 6, 7]");
            check(failures, source, "SELECT k FROM %s WHERE v " + notContains + " 'Johann Sebastian Bach'", "[1, 3, 4, 5, 6, 7]");
        }
        assertTrue(String.format("%d failures for %s:%n%s", failures.size(), type, String.join("\n", failures)), failures.isEmpty());
    }

    private void check(List<String> failures, String source, String query, String expected)
    {
        String actual = select(query).toString();
        if (!expected.equals(actual))
            failures.add(String.format("[%s] %s: expected %s but got %s", source, query, expected, actual));
    }

    private Set<Integer> select(String query, Object... values)
    {
        Set<Integer> keys = new TreeSet<>();
        for (UntypedResultSet.Row row : execute(query, values))
            keys.add(row.getInt("k"));
        return keys;
    }
}
