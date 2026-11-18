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
package org.apache.cassandra.db.partitions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A page that ends on the last row a query selects in a partition is resumed in that partition with no clustering left
 * to select; the partition must still return its static row with the columns of the selection only. Checked with a
 * clustering restriction by names ({@code ck = x}) and by slice ({@code ck <= x}), with and without indexes (with an
 * index on a static collection, a negation returns the keys of static rows, which are read with the paging filter), on
 * every memtable kind and after flush.
 */
@RunWith(Parameterized.class)
public class StaticRowSelectionPagingTest extends CQLTester
{
    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameter(1)
    public boolean indexed;

    @Parameterized.Parameters(name = "memtable={0}, indexed={1}")
    public static Collection<Object[]> data()
    {
        List<Object[]> data = new ArrayList<>();
        for (String memtable : new String[]{ "skiplist", "trie", "trie_stage2", "trie_stage3" })
            for (boolean indexed : new boolean[]{ false, true })
                data.add(new Object[]{ memtable, indexed });
        return data;
    }

    @BeforeClass
    public static void setUpNetwork() throws Throwable
    {
        requireNetwork();
    }

    private static final String[] RESTRICTIONS = {
        "ck = 2 AND st = 0",
        "ck <= 2 AND st = 0",
        "ck = 2 AND ss CONTAINS 2",
        "ck = 2 AND ss NOT CONTAINS 1",
        "ck <= 2 AND ss NOT CONTAINS 1",
        "ck IN (1, 2) AND ss NOT CONTAINS 1",
    };

    @Test
    public void testPagingWithStaticRows() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, st int static, ss set<int> static, sx int static, " +
                    "sy int static, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + '\'');
        if (indexed)
        {
            createIndex("CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(ss) USING 'StorageAttachedIndex'");
        }
        // 1: only a static row; 3: rows and a static row; 4: rows only; 6: rows and a static row that doesn't match
        execute("INSERT INTO %s (pk, st, ss, sx, sy) VALUES (1, 0, {2}, 1, 1)");
        for (int pk : new int[]{ 3, 4, 6 })
            for (int ck = 1; ck <= 4; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, ck);
        execute("INSERT INTO %s (pk, st, ss, sx, sy) VALUES (3, 0, {2}, 1, 1)");
        execute("INSERT INTO %s (pk, st, ss, sx, sy) VALUES (6, 1, {1}, 1, 1)");

        List<String> failures = new ArrayList<>();
        check(failures, "memtable");
        flush();
        check(failures, "sstable");
        assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
    }

    private void check(List<String> failures, String source) throws Throwable
    {
        for (String restriction : RESTRICTIONS)
        {
            String query = "SELECT pk, ck FROM %s WHERE " + restriction + " ALLOW FILTERING";
            List<String> expected = new ArrayList<>();
            for (UntypedResultSet.Row row : execute(query))
                expected.add(row.getInt("pk") + "/" + row.getInt("ck"));
            assertFalse(restriction, expected.isEmpty());

            for (int pageSize : new int[]{ 1, 2, 3 })
            {
                List<String> rows = new ArrayList<>();
                try
                {
                    for (com.datastax.driver.core.Row row : executeNetWithPaging(query, pageSize))
                        rows.add(row.getInt("pk") + "/" + row.getInt("ck"));
                }
                catch (Exception e)
                {
                    rows.add("failed: " + e.getClass().getSimpleName());
                }
                if (!expected.equals(rows))
                    failures.add(String.format("[%s] %s, page size %d: expected %s but got %s",
                                               source, restriction, pageSize, expected, rows));
            }
        }
    }
}
