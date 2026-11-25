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
package org.apache.cassandra.db.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertTrue;

/**
 * A query with a disjunction of expressions on static columns and on other columns returns the partitions that only
 * have a static row when that row satisfies the disjunction, as a row of their static content; the limits count such a
 * partition as one row. Each query is run without paging and paged by 1, 2 and 3, with LIMIT and PER PARTITION LIMIT,
 * with and without indexes, from the memtable and after flush. Partitions whose static row doesn't satisfy the
 * disjunction and that have only deleted data match nothing. The expected rows are those of the same query on a table
 * that holds only the live data, without limits.
 */
@RunWith(Parameterized.class)
public class StaticDisjunctionLimitTest extends CQLTester
{
    private static final String[] RESTRICTIONS = {
        "st = 0 OR v = 1",
        "v = 1 OR st = 0",
        "ss CONTAINS 2 OR v = 1",
        "st = 0 OR v = 1 OR v = 2",
        "(st = 0 AND v = 1) OR st = 1",
        "(st = 0 AND ss CONTAINS 2) OR v = 2",
        "st = 0 AND (v = 1 OR ss CONTAINS 2)",
        "pk = 3 AND (st = 0 OR v = 1)",
        "pk = 1 AND (st = 0 OR v = 1)",
        "(st = 0 OR v = 1) AND v < 5",
        "st = 0 AND (v = 1 OR v = 2)",
        "v = 1 OR v = 2",
        "st = 0",
        // the deleted data satisfies a clustering restriction or a negation
        "st = 0 OR ck = 1",
        "st = 0 OR v != 9",
        "ss CONTAINS 2 OR ck = 1",
        "(st = 0 OR ck = 1) AND (ss CONTAINS 2 OR v = 1)",
        "pk = 13 AND (st = 0 OR ck = 1)",
        "pk = 14 AND (st = 0 OR v = 1)",
        "pk = 13 AND (v = 1 OR v = 9)",
        "ck = 1 OR ck > 3",
    };

    private static final String[] LIMITS = { "", "LIMIT 1", "LIMIT 2", "LIMIT 3", "LIMIT 4", "LIMIT 6",
                                              "PER PARTITION LIMIT 1", "PER PARTITION LIMIT 1 LIMIT 3",
                                              "PER PARTITION LIMIT 2 LIMIT 4" };

    private static final String SCHEMA = "CREATE TABLE %s (pk int, ck int, v int, st int static, ss set<int> static, " +
                                         "PRIMARY KEY (pk, ck))";

    // the live data, the same in both tables
    private static final String[] LIVE = {
        // 1, 2, 5: only a static row; 3, 6: rows and a static row; 4: rows only
        "INSERT INTO %s (pk, st, ss) VALUES (1, 0, {2})",
        "INSERT INTO %s (pk, st) VALUES (2, 0)",
        "INSERT INTO %s (pk, ck, v) VALUES (3, 1, 1)",
        "INSERT INTO %s (pk, ck, v) VALUES (3, 2, 2)",
        "INSERT INTO %s (pk, ck, v) VALUES (3, 3, 0)",
        "INSERT INTO %s (pk, ck, v) VALUES (3, 4, 1)",
        "INSERT INTO %s (pk, st, ss) VALUES (3, 0, {2})",
        "INSERT INTO %s (pk, ck, v) VALUES (4, 1, 1)",
        "INSERT INTO %s (pk, ck, v) VALUES (4, 2, 2)",
        "INSERT INTO %s (pk, st, ss) VALUES (5, 0, {1})",
        "INSERT INTO %s (pk, ck, v) VALUES (6, 1, 1)",
        "INSERT INTO %s (pk, ck, v) VALUES (6, 2, 2)",
        "INSERT INTO %s (pk, ck, v) VALUES (6, 3, 0)",
        "INSERT INTO %s (pk, st, ss) VALUES (6, 1, {1})",
        // 13 to 16: a static row that doesn't match st = 0 nor ss CONTAINS 2, and only deleted data, but for 16
        "INSERT INTO %s (pk, st, ss) VALUES (13, 1, {1})",
        "INSERT INTO %s (pk, st, ss) VALUES (14, 1, {1})",
        "INSERT INTO %s (pk, st, ss) VALUES (15, 1, {1})",
        "INSERT INTO %s (pk, st, ss) VALUES (16, 1, {1})",
        "INSERT INTO %s (pk, ck) VALUES (16, 1)",
    };

    // the deleted data, only in the table that is tested, written before the live data
    private static final String[] DELETED = {
        // a range tombstone
        "INSERT INTO %s (pk, ck, v) VALUES (13, 1, 1)",
        "DELETE FROM %s WHERE pk = 13 AND ck >= 0",
        // a deleted row
        "INSERT INTO %s (pk, ck, v) VALUES (14, 1, 1)",
        "DELETE FROM %s WHERE pk = 14 AND ck = 1",
        // an expired row
        "INSERT INTO %s (pk, ck, v) VALUES (15, 1, 1) USING TTL 1",
        // a live row whose matching cell is deleted
        "INSERT INTO %s (pk, ck, v) VALUES (16, 1, 1)",
        "DELETE v FROM %s WHERE pk = 16 AND ck = 1",
    };

    @Parameterized.Parameter
    public boolean indexed;

    @Parameterized.Parameters(name = "indexed={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[]{ false }, new Object[]{ true });
    }

    @BeforeClass
    public static void setUpNetwork() throws Throwable
    {
        requireNetwork();
    }

    private String live;

    @Test
    public void testLimits() throws Throwable
    {
        live = createTable(SCHEMA);
        for (String write : LIVE)
            executeFormattedQuery(write.replace("%s", KEYSPACE + '.' + live));

        createTable(SCHEMA);
        if (indexed)
            for (String column : new String[]{ "ck", "v", "st", "ss" })
                createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
        for (String write : DELETED)
            execute(write);
        for (String write : LIVE)
            execute(write);
        // let the TTL expire
        Thread.sleep(2100);

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
            String query = "SELECT pk, ck FROM %s WHERE " + restriction;
            List<String> all = rows(query.replace("%s", KEYSPACE + '.' + live), 0);
            for (String limit : LIMITS)
            {
                List<String> expected = limit(all, limit);
                for (int pageSize : new int[]{ 0, 1, 2, 3 })
                {
                    String limited = query + ' ' + limit;
                    List<String> rows = rows(limited, pageSize);
                    if (!expected.equals(rows))
                        failures.add(String.format("[%s] %s, %s: expected %s but got %s", source, limited,
                                                   pageSize == 0 ? "unpaged" : "page size " + pageSize, expected, rows));
                }
            }
        }
    }

    private List<String> rows(String query, int pageSize)
    {
        query += " ALLOW FILTERING";
        List<String> rows = new ArrayList<>();
        try
        {
            if (pageSize == 0)
            {
                for (UntypedResultSet.Row row : execute(query))
                    rows.add(row.getInt("pk") + "/" + (row.has("ck") ? row.getInt("ck") : null));
            }
            else
            {
                for (com.datastax.driver.core.Row row : executeNetWithPaging(query, pageSize))
                    rows.add(row.getInt("pk") + "/" + (row.isNull("ck") ? null : row.getInt("ck")));
            }
        }
        catch (Throwable t)
        {
            rows.add("failed: " + t);
        }
        return rows;
    }

    /**
     * Applies the given LIMIT and PER PARTITION LIMIT to the rows of the query without limits, in their order.
     */
    private static List<String> limit(List<String> all, String limits)
    {
        int limit = Integer.MAX_VALUE;
        int perPartitionLimit = Integer.MAX_VALUE;
        String[] words = limits.split(" ");
        for (int i = 0; i < words.length; i++)
        {
            if (words[i].equals("LIMIT"))
            {
                int value = Integer.parseInt(words[i + 1]);
                if (i > 1 && words[i - 1].equals("PARTITION"))
                    perPartitionLimit = value;
                else
                    limit = value;
            }
        }
        List<String> result = new ArrayList<>();
        Map<String, Integer> perPartition = new HashMap<>();
        for (String row : all)
        {
            String partition = row.substring(0, row.indexOf('/'));
            int count = perPartition.merge(partition, 1, Integer::sum);
            if (count <= perPartitionLimit && result.size() < limit)
                result.add(row);
        }
        return result;
    }
}
