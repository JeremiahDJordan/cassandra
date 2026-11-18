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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertTrue;

/**
 * The negations on an indexed static collection (NOT CONTAINS, NOT CONTAINS KEY and m[k] != v) can't subtract the
 * matches of the index (see IndexContext#canSubtractMatches), so the index returns every key and the filter decides.
 * Every key must include the partitions that only have a static row, which an expression on a static column matches as
 * a whole. Each query is checked against the same query filtered without indexes.
 */
public class StaticCollectionNegationTest extends SAITester.Versioned
{
    private static final String SCHEMA = "CREATE TABLE %s (pk int, ck int, v int, r set<int>, st int static, " +
                                         "ss set<int> static, sl list<int> static, sk map<int, int> static, " +
                                         "sv map<int, int> static, se map<int, int> static, PRIMARY KEY (pk, ck))";

    private static final String[] INDEXES = { "v", "r", "st", "ss", "sl", "KEYS(sk)", "VALUES(sv)", "ENTRIES(se)" };

    private static final String[] RESTRICTIONS = {
        "ss NOT CONTAINS 1",
        "ss NOT CONTAINS 2",
        "sl NOT CONTAINS 1",
        "sk NOT CONTAINS KEY 1",
        "sv NOT CONTAINS 1",
        "se[1] != 1",
        "ss CONTAINS 1",
        "ss NOT CONTAINS 1 AND st = 0",
        "ss NOT CONTAINS 1 AND ss NOT CONTAINS 2",
        "ss NOT CONTAINS 1 AND sk NOT CONTAINS KEY 2",
        "ss NOT CONTAINS 1 AND v = 1",
        "ss NOT CONTAINS 1 AND r NOT CONTAINS 1",
        "r NOT CONTAINS 1",
        "r NOT CONTAINS 1 AND st = 0",
        "pk = 1 AND ss NOT CONTAINS 1",
        "pk = 3 AND ss NOT CONTAINS 1",
        "token(pk) > token(1) AND ss NOT CONTAINS 1",
        "pk = 9 AND ss NOT CONTAINS 1",
        "pk = 9 AND ss NOT CONTAINS 1 AND v = 1",
    };

    private static final String[] LIMITS = { "", " LIMIT 3", " LIMIT 10", " PER PARTITION LIMIT 2",
                                              " PER PARTITION LIMIT 1 LIMIT 4" };

    @BeforeClass
    public static void setUpNetwork() throws Throwable
    {
        requireNetwork();
    }

    @Test
    public void testNegationsOnStaticCollections()
    {
        // the expected rows, filtered without indexes, before and after the second batch of writes
        createTable(SCHEMA);
        writeFirst();
        Map<String, List<String>> first = select(true);
        writeSecond();
        Map<String, List<String>> second = select(true);

        createTable(SCHEMA);
        for (String target : INDEXES)
            createIndex("CREATE CUSTOM INDEX ON %s(" + target + ") USING 'StorageAttachedIndex'");
        disableCompaction();
        List<String> failures = new ArrayList<>();
        writeFirst();
        check(failures, "memtable", first);
        flush();
        check(failures, "sstable", first);
        writeSecond();
        check(failures, "sstable and memtable", second);
        flush();
        check(failures, "two sstables", second);
        compact();
        check(failures, "compacted", second);
        assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
    }

    private void writeFirst()
    {
        // 1, 2, 5, 7: only a static row, with or without the collections; 3, 6: rows and a static row; 4: rows only
        execute("INSERT INTO %s (pk, st, ss, sl, sk, sv, se) VALUES (1, 0, {2}, [2], {2: 2}, {2: 2}, {2: 2})");
        execute("INSERT INTO %s (pk, st) VALUES (2, 0)");
        execute("INSERT INTO %s (pk, ck, v, r) VALUES (3, 1, 1, {1})");
        execute("INSERT INTO %s (pk, ck, v, r) VALUES (3, 2, 2, {2})");
        execute("INSERT INTO %s (pk, st, ss, sl, sk, sv, se) VALUES (3, 0, {2}, [2], {2: 2}, {2: 2}, {2: 2})");
        execute("INSERT INTO %s (pk, ck, v, r) VALUES (4, 1, 1, {1})");
        execute("INSERT INTO %s (pk, st, ss, sl, sk, sv, se) VALUES (5, 0, {1}, [1], {1: 1}, {1: 1}, {1: 1})");
        execute("INSERT INTO %s (pk, ck, v) VALUES (6, 1, 2)");
        execute("INSERT INTO %s (pk, st, ss, sl, sk, sv, se) VALUES (6, 1, {1}, [1], {1: 1}, {1: 1}, {1: 1})");
        execute("INSERT INTO %s (pk, st) VALUES (7, 1)");
        // 9: a wide partition with a static row; 10: a wide partition without one
        for (int ck = 1; ck <= 12; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v, r) VALUES (9, ?, ?, ?)", ck, ck % 3, set(ck % 3));
            execute("INSERT INTO %s (pk, ck, v, r) VALUES (10, ?, ?, ?)", ck, ck % 3, set(ck % 3));
        }
        execute("INSERT INTO %s (pk, st, ss, sl, sk, sv, se) VALUES (9, 0, {2}, [2], {2: 2}, {2: 2}, {2: 2})");
    }

    private void writeSecond()
    {
        // 1 now holds 1, 5's collections are deleted, and 8 is a new partition with only a static row
        execute("UPDATE %s SET ss = ss + {1}, sl = sl + [1], sk[1] = 1, sv[1] = 1, se[1] = 1 WHERE pk = 1");
        execute("DELETE ss, sl, sk, sv, se FROM %s WHERE pk = 5");
        execute("INSERT INTO %s (pk, st, ss) VALUES (8, 0, {3})");
        // rows of the wide partitions deleted and added
        execute("DELETE FROM %s WHERE pk = 9 AND ck = 2");
        execute("DELETE FROM %s WHERE pk = 10 AND ck = 5");
        execute("INSERT INTO %s (pk, ck, v) VALUES (9, 13, 1)");
    }

    private void check(List<String> failures, String source, Map<String, List<String>> expected)
    {
        Map<String, List<String>> actual = select(false);
        for (String key : expected.keySet())
        {
            if (!expected.get(key).equals(actual.get(key)))
                failures.add(String.format("[%s, %s] %s: expected %s but got %s",
                                           version, source, key, expected.get(key), actual.get(key)));
        }
    }

    /**
     * @param expected whether to select the expected rows, filtering without indexes and applying the limits to the
     *                 rows of the query without limits, or to run the queries with the limits in every mode
     * @return the rows of each query, as {@code pk/ck}, in order
     */
    private Map<String, List<String>> select(boolean expected)
    {
        Map<String, List<String>> results = new LinkedHashMap<>();
        for (String restriction : RESTRICTIONS)
        {
            String query = "SELECT pk, ck FROM %s WHERE " + restriction;
            List<String> all = expected ? rows(query + " ALLOW FILTERING", "coordinator") : null;
            for (String limit : LIMITS)
            {
                String limited = query + limit;
                for (String mode : new String[]{ "internal", "coordinator", "page 1", "page 3" })
                    results.put(restriction + limit + ", " + mode, expected ? limit(all, limit) : rows(limited, mode));
            }
        }
        return results;
    }

    private List<String> rows(String query, String mode)
    {
        List<String> rows = new ArrayList<>();
        try
        {
            if (mode.startsWith("page"))
            {
                for (com.datastax.driver.core.Row row : executeNetWithPaging(query, Integer.parseInt(mode.substring(5))))
                    rows.add(row.getInt("pk") + "/" + (row.isNull("ck") ? null : row.getInt("ck")));
            }
            else
            {
                for (UntypedResultSet.Row row : mode.equals("internal") ? executeInternal(query) : execute(query))
                    rows.add(row.getInt("pk") + "/" + (row.has("ck") ? row.getInt("ck") : null));
            }
        }
        catch (Throwable t)
        {
            rows.add("failed: " + t);
        }
        return rows;
    }

    private static List<String> limit(List<String> all, String limits)
    {
        int limit = Integer.MAX_VALUE;
        int perPartitionLimit = Integer.MAX_VALUE;
        String[] words = limits.trim().split(" ");
        for (int i = 0; i < words.length; i++)
        {
            if (words[i].equals("LIMIT"))
            {
                int value = Integer.parseInt(words[i + 1]);
                if (i > 0 && words[i - 1].equals("PARTITION"))
                    perPartitionLimit = value;
                else
                    limit = value;
            }
        }
        List<String> result = new ArrayList<>();
        Map<String, Integer> perPartition = new HashMap<>();
        for (String row : all)
        {
            int count = perPartition.merge(row.substring(0, row.indexOf('/')), 1, Integer::sum);
            if (count <= perPartitionLimit && result.size() < limit)
                result.add(row);
        }
        return result;
    }
}
