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

import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.inject.Injections;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * CONTAINS and CONTAINS KEY of a value of several words, on a collection with an analyzed index, match the rows with an
 * element that holds all the words, on every query path: the replica applies it, not only the coordinator's recheck.
 */
public class AnalyzedContainsTest extends SAITester
{
    // The elements of the collections of the rows 1 to 8 (7 has no collection)
    private static final String[][] ELEMENTS = { { "Johann Strauss" },
                                                 { "Richard Strauss", "Johann Sebastian Bach" },
                                                 { "johann" },
                                                 { "strauss x johann" },
                                                 { "bach" },
                                                 { "" },
                                                 {},
                                                 { "Johann", "Strauss" } };

    @Test
    public void testList() throws Throwable
    {
        checkContains("list<text>", "v", "CONTAINS", "NOT CONTAINS", elements -> list((Object[]) elements));
    }

    @Test
    public void testSet() throws Throwable
    {
        checkContains("set<text>", "v", "CONTAINS", "NOT CONTAINS", elements -> set((Object[]) elements));
    }

    @Test
    public void testMapValues() throws Throwable
    {
        checkContains("map<int, text>", "VALUES(v)", "CONTAINS", "NOT CONTAINS", elements -> {
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
        checkContains("map<text, int>", "KEYS(v)", "CONTAINS KEY", "NOT CONTAINS KEY", elements -> {
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
     * In a disjunction too, the index intersects the rows of the words of an analyzed CONTAINS or CONTAINS KEY: the
     * rows holding only one of the words aren't read.
     */
    @Test
    public void testDisjunctionReadsOnlyRowsWithAllWords() throws Throwable
    {
        disableQueryOptimization();
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<text>, m map<text, int>, x int)");
        createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(m)) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, l, m, x) VALUES (1, ['Johann Strauss'], {'Johann Strauss': 0}, 0)");
        execute("INSERT INTO %s (k, l, m, x) VALUES (2, ['Johann'], {'Johann': 0}, 0)");
        execute("INSERT INTO %s (k, l, m, x) VALUES (3, ['Strauss'], {'Strauss': 0}, 0)");

        Injections.Counter partitionsRead = Injections.newCounter("PartitionsRead")
                                                      .add(newInvokePoint().onClass(QueryController.class)
                                                                           .onMethod("getPartition", List.class, ReadExecutionController.class)
                                                                           .atExit())
                                                      .build();
        Injections.inject(partitionsRead);
        try
        {
            for (String restriction : new String[]{ "l CONTAINS 'Johann Strauss'", "m CONTAINS KEY 'Johann Strauss'" })
            {
                partitionsRead.reset();
                assertRows(execute("SELECT k FROM %s WHERE " + restriction + " OR x = 1"), row(1));
                assertThat(partitionsRead.get()).as(restriction).isEqualTo(1);
            }
        }
        finally
        {
            Injections.deleteAll();
        }
    }

    private void checkContains(String type, String target, String contains, String notContains,
                               Function<String[], Object> maker) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ", x int, t text, vec vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(" + target + ") USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        for (int i = 0; i < ELEMENTS.length; i++)
        {
            int k = i + 1;
            if (ELEMENTS[i].length == 0)
                execute("INSERT INTO %s (k, x, t, vec) VALUES (?, 0, 'word', [1.0, 1.0])", k);
            else
                execute("INSERT INTO %s (k, v, x, t, vec) VALUES (?, ?, 0, 'word', [1.0, 1.0])", k,
                        maker.apply(ELEMENTS[i]));
        }

        List<String> failures = new ArrayList<>();
        for (String source : new String[]{ "memtable", "sstable" })
        {
            if (source.equals("sstable"))
                flush();

            // The rows with an element holding both words, and the others
            String c = "v " + contains + " 'Johann Strauss'";
            String n = "v " + notContains + " 'Johann Strauss'";
            Set<Integer> matching = keys(1, 4);
            Set<Integer> others = keys(2, 3, 5, 6, 7, 8);
            String ann = " ORDER BY vec ANN OF [1.0, 1.0] LIMIT 20";
            String bm25 = " ORDER BY t BM25 OF 'word' LIMIT 20";
            for (boolean internal : new boolean[]{ true, false })
            {
                String label = source + (internal ? ", internal" : ", coordinator");
                check(failures, label, "SELECT k FROM %s WHERE " + c, internal, matching, matching.size());
                check(failures, label, "SELECT k FROM %s WHERE " + n, internal, others, others.size());
                check(failures, label, "SELECT k FROM %s WHERE " + c + " LIMIT 1", internal, matching, 1);
                check(failures, label, "SELECT k FROM %s WHERE " + c + " LIMIT 2", internal, matching, 2);
                check(failures, label, "SELECT k FROM %s WHERE " + c + ann, internal, matching, matching.size());
                check(failures, label, "SELECT k FROM %s WHERE " + c + bm25, internal, matching, matching.size());
                check(failures, label, "SELECT k FROM %s WHERE " + n + ann, internal, others, others.size());
                check(failures, label, "SELECT k FROM %s WHERE " + c + " OR x = 5", internal, matching, matching.size());
                check(failures, label, "SELECT k FROM %s WHERE " + c + " AND x = 0", internal, matching, matching.size());
                check(failures, label, "SELECT k FROM %s WHERE k = 8 AND " + c, internal, keys(), 0);
                check(failures, label, "SELECT k FROM %s WHERE k = 4 AND " + c, internal, keys(4), 1);
            }
        }
        assertTrue(String.format("%d failures for %s:%n%s", failures.size(), type, String.join("\n", failures)), failures.isEmpty());
    }

    /**
     * Paging through the driver: the even rows hold both words in one element, the odd rows in two elements, and every
     * page size must return all the even rows and no odd one.
     */
    @Test
    public void testPaging() throws Throwable
    {
        requireNetwork();
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<text>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        int rows = 40;
        for (int k = 0; k < rows; k++)
        {
            if (k % 2 == 0)
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, list("Johann Strauss"));
            else
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, list("Johann", "Strauss"));
        }

        List<String> failures = new ArrayList<>();
        for (String source : new String[]{ "memtable", "sstable" })
        {
            if (source.equals("sstable"))
                flush();
            for (String where : new String[]{ "v CONTAINS 'Johann Strauss'", "v NOT CONTAINS 'Johann Strauss'" })
            {
                int parity = where.contains("NOT") ? 1 : 0;
                for (int pageSize : new int[]{ 1, 2, 3, 5, 10, 19, 20, 21, 100 })
                {
                    int count = 0, wrong = 0;
                    for (Row row : executeNetWithPaging("SELECT k FROM %s WHERE " + where, pageSize))
                    {
                        count++;
                        if (row.getInt("k") % 2 != parity)
                            wrong++;
                    }
                    if (count != rows / 2 || wrong != 0)
                        failures.add(String.format("[%s] %s, page size %d: %d rows, %d of them wrong (expected %d)",
                                                   source, where, pageSize, count, wrong, rows / 2));
                }
            }
        }
        assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
    }

    /**
     * Checks that the query returns the expected number of rows, all among the allowed ones.
     */
    private void check(List<String> failures, String label, String query, boolean internal, Set<Integer> allowed, int expectedCount)
    {
        Set<Integer> keys = new TreeSet<>();
        String result;
        try
        {
            UntypedResultSet rows = internal ? executeInternal(query) : execute(query);
            for (UntypedResultSet.Row row : rows)
                keys.add(row.getInt("k"));
            result = keys.toString();
        }
        catch (Throwable t)
        {
            result = "failed " + t;
        }
        if (keys.size() != expectedCount || !allowed.containsAll(keys))
            failures.add(String.format("[%s] %s: expected %d of %s but got %s", label, query, expectedCount, allowed, result));
    }

    private static Set<Integer> keys(Integer... keys)
    {
        Set<Integer> set = new TreeSet<>();
        for (Integer key : keys)
            set.add(key);
        return set;
    }
}
