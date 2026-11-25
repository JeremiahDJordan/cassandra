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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

/**
 * A disjunction of an expression on a static column and an expression on a regular column, like
 * {@code v = 1 OR st = 2}, is satisfied by a row if either expression is: the row's regular column or its partition's
 * static column. With or without indexes, with LIMIT and with paging.
 */
@RunWith(Parameterized.class)
public class StaticColumnDisjunctionTest extends SAITester
{
    private List<String> columns = List.of("pk1", "pk2", "ck", "st", "v");

    @Parameterized.Parameter
    public List<String> indexedColumns;

    @Parameterized.Parameters(name = "indexes={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[]{ List.of() },
                             new Object[]{ List.of("pk1") },
                             new Object[]{ List.of("pk1", "v") },
                             new Object[]{ List.of("v", "st") },
                             new Object[]{ List.of("pk1", "v", "st") });
    }

    @Test
    public void testDisjunctionOfStaticAndRegularColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, ck int, st int static, v int, PRIMARY KEY ((pk1, pk2), ck))");
        for (String column : indexedColumns)
            createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk1, pk2, ck, st, v) VALUES (3, 1, 1, 2, 1)");
        execute("INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 1, 2, 0)");
        execute("INSERT INTO %s (pk1, pk2, ck, st, v) VALUES (3, 2, 1, 0, 1)");
        execute("INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 2, 2, 0)");
        // no static row
        execute("INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 3, 1, 1)");
        execute("INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 3, 2, 0)");
        // static rows only
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 4, 2)");
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 5, 0)");
        // another value of pk1
        execute("INSERT INTO %s (pk1, pk2, ck, st, v) VALUES (4, 1, 1, 2, 0)");

        beforeAndAfterFlush(() -> {
            verify("pk1 = 3 AND (v = 1 OR st = 2)",
                   "3,1,1,2,1", "3,1,2,2,0", "3,2,1,0,1", "3,3,1,null,1", "3,4,null,2,null");
            verify("pk1 = 3 AND (v = 9 OR st = 2)",
                   "3,1,1,2,1", "3,1,2,2,0", "3,4,null,2,null");
            verify("(v = 1 OR st = 2)",
                   "3,1,1,2,1", "3,1,2,2,0", "3,2,1,0,1", "3,3,1,null,1", "3,4,null,2,null", "4,1,1,2,0");
            verify("pk1 = 3 AND v = 1 AND st = 2",
                   "3,1,1,2,1");
            // a disjunction on regular columns only: a partition without matching rows doesn't match
            verify("pk1 = 3 AND st = 2 AND (v = 1 OR v = 0)",
                   "3,1,1,2,1", "3,1,2,2,0");
            // a disjunction on static columns only
            verify("pk1 = 3 AND (st = 2 OR st = 0)",
                   "3,1,1,2,1", "3,1,2,2,0", "3,2,1,0,1", "3,2,2,0,0", "3,4,null,2,null", "3,5,null,0,null");
        });
    }

    /**
     * In a disjunction with a static column, an expression on a clustering column, a map entry or NOT CONTAINS is
     * never satisfied by the static row of a partition without other rows, and doesn't fail the query.
     */
    @Test
    public void testDisjunctionOfStaticAndClusteringOrCollectionColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, ck1 int, ck2 int, st int static, v int, m map<int, int>, s set<int>, " +
                    "PRIMARY KEY ((pk1, pk2), ck1, ck2))");
        for (String column : indexedColumns)
            createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
        columns = List.of("pk1", "pk2", "ck1", "ck2", "st", "v");

        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, st, v, m, s) VALUES (3, 1, 1, 1, 2, 1, {1: 1}, {1})");
        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, v, m, s) VALUES (3, 1, 1, 2, 0, {1: 0}, {2})");
        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, st, v, m, s) VALUES (3, 2, 2, 1, 0, 1, {1: 1}, {1})");
        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, v, m, s) VALUES (3, 2, 2, 2, 0, {1: 0}, {3})");
        // static rows only
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 4, 2)");
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 5, 0)");

        beforeAndAfterFlush(() -> {
            verify("pk1 = 3 AND (ck1 = 1 OR st = 2)",
                   "3,1,1,1,2,1", "3,1,1,2,2,0", "3,4,null,null,2,null");
            verify("pk1 = 3 AND (ck2 = 1 OR st = 2)",
                   "3,1,1,1,2,1", "3,1,1,2,2,0", "3,2,2,1,0,1", "3,4,null,null,2,null");
            verify("pk1 = 3 AND (v = 1 OR st = 2) AND ck2 = 1",
                   "3,1,1,1,2,1", "3,2,2,1,0,1");
            verify("pk1 = 3 AND (m[1] = 1 OR st = 2)",
                   "3,1,1,1,2,1", "3,1,1,2,2,0", "3,2,2,1,0,1", "3,4,null,null,2,null");
            verify("pk1 = 3 AND (m[1] > 0 OR st = 2)",
                   "3,1,1,1,2,1", "3,1,1,2,2,0", "3,2,2,1,0,1", "3,4,null,null,2,null");
            verify("pk1 = 3 AND (s NOT CONTAINS 1 OR st = 2)",
                   "3,1,1,1,2,1", "3,1,1,2,2,0", "3,2,2,2,0,0", "3,4,null,null,2,null");
        });
    }

    /**
     * A partition whose static row doesn't satisfy the filter, and whose rows don't either, doesn't match, even if it
     * has a range deletion. The table without range deletions gives the expected results, which are also given
     * explicitly.
     */
    @Test
    public void testPartitionsWithRangeDeletionsAndNoMatchingRows() throws Throwable
    {
        String schema = "CREATE TABLE %s (pk1 int, pk2 int, ck int, st int static, u int static, v int, PRIMARY KEY ((pk1, pk2), ck))";
        columns = List.of("pk1", "pk2", "ck", "st", "u", "v");
        String control = createTable(schema);
        String table = createTable(schema);
        for (String column : indexedColumns)
        {
            createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
            if (column.equals("st"))
                createIndex("CREATE CUSTOM INDEX ON %s(u) USING 'StorageAttachedIndex'");
        }

        for (String t : new String[]{ control, table })
        {
            String insert = "INSERT INTO " + KEYSPACE + '.' + t + " (pk1, pk2, ck, v) VALUES (3, ?, 0, ?) USING TIMESTAMP 10";
            String update = "UPDATE " + KEYSPACE + '.' + t + " USING TIMESTAMP 10 SET st = ?, u = ? WHERE pk1 = 3 AND pk2 = ?";
            // 1 and 2: u = 0, a row with v = 5
            // 3: u = 0, no rows
            // 4: u = 0, a deleted row
            // 5 and 6: st = 0, u = 9, a row with v = 5
            // 7: st = 0, u = 0, a row with v = 2
            for (int pk2 = 1; pk2 <= 7; pk2++)
            {
                execute(update, pk2 >= 5 ? 0 : null, pk2 == 5 || pk2 == 6 ? 9 : 0, pk2);
                if (pk2 != 3)
                    execute(insert, pk2, pk2 == 7 ? 2 : 5);
            }
            execute("DELETE FROM " + KEYSPACE + '.' + t + " USING TIMESTAMP 11 WHERE pk1 = 3 AND pk2 = 4 AND ck = 0");
        }
        // range deletions that delete no rows, in partitions with and without matching rows
        for (int pk2 : new int[]{ 2, 3, 5, 7 })
            execute("DELETE FROM %s USING TIMESTAMP 11 WHERE pk1 = 3 AND pk2 = ? AND ck >= 5 AND ck <= 6", pk2);

        String[][] restrictions = {
            { "st = 0 AND (u = 0 OR v = 2)", "3,7,0,0,0,2" },
            { "u = 0 AND (st = 0 OR v = 2)", "3,7,0,0,0,2" },
            { "st = 0 AND (v = 9 OR v = 8)" },
            { "st = 0 AND v = 9" },
            { "(st = 0 AND v = 9) OR (u = 5 AND v = 8)" },
            { "u = 5 OR v = 2", "3,7,0,0,0,2" },
            { "u = 9 OR v = 2", "3,5,0,0,9,5", "3,6,0,0,9,5", "3,7,0,0,0,2" },
        };
        beforeAndAfterFlush(() -> {
            for (String[] restriction : restrictions)
            {
                String[] expected = Arrays.copyOfRange(restriction, 1, restriction.length);
                for (String partition : new String[]{ "", "pk1 = 3 AND ", "pk1 = 3 AND pk2 = 2 AND ", "pk1 = 3 AND pk2 = 3 AND ",
                                                      "pk1 = 3 AND pk2 = 5 AND ", "pk1 = 3 AND pk2 = 7 AND " })
                {
                    String where = partition + '(' + restriction[0] + ')';
                    List<String> matching = new ArrayList<>();
                    for (String row : expected)
                        if (partition.isEmpty() || partition.equals("pk1 = 3 AND ") || partition.contains("pk2 = " + row.split(",")[1] + ' '))
                            matching.add(row);
                    verify(where, matching.toArray(new String[0]));

                    String query = "SELECT " + String.join(", ", columns) + " FROM " + KEYSPACE + '.' + control + " WHERE " + where + " ALLOW FILTERING";
                    assertEquals(where + " without range deletions", sorted(matching), sorted(rows(execute(query))));
                }
            }
        });
    }

    private void verify(String restriction, String... expected) throws Throwable
    {
        String query = "SELECT " + String.join(", ", columns) + " FROM %s WHERE " + restriction + " ALLOW FILTERING";
        List<String> all = rows(execute(query));
        assertEquals(restriction, sorted(Arrays.asList(expected)), sorted(all));

        for (int limit = 1; limit <= expected.length; limit++)
        {
            List<String> limited = rows(execute(query.replace(" ALLOW FILTERING", " LIMIT " + limit + " ALLOW FILTERING")));
            assertEquals(restriction + " LIMIT " + limit, all.subList(0, limit), limited);
        }

        for (int pageSize : new int[]{ 1, 2, 3 })
        {
            List<String> paged = new ArrayList<>();
            for (com.datastax.driver.core.Row row : executeNetWithPaging(query, pageSize))
            {
                List<String> values = new ArrayList<>();
                for (String column : columns)
                    values.add(String.valueOf(row.getObject(column)));
                paged.add(String.join(",", values));
            }
            assertEquals(restriction + " page size " + pageSize, all, paged);
        }
    }

    private List<String> rows(UntypedResultSet result)
    {
        List<String> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
        {
            List<String> values = new ArrayList<>();
            for (String column : columns)
                values.add(String.valueOf(value(row, column)));
            rows.add(String.join(",", values));
        }
        return rows;
    }

    private static Integer value(UntypedResultSet.Row row, String column)
    {
        return row.has(column) ? row.getInt(column) : null;
    }

    private static List<String> sorted(List<String> rows)
    {
        List<String> copy = new ArrayList<>(rows);
        copy.sort(String::compareTo);
        return copy;
    }
}
