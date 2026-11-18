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
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertTrue;

/**
 * A partition without matching rows is returned as a row of its static content only if its static row satisfies the
 * restrictions on its own, e.g. not for {@code pk = 13 AND (v = 1 OR v = 9)}. Checked with a range tombstone, a deleted
 * row and an expired row, with and without indexes, internally, through the coordinator and paged, against a table that
 * holds only the live data.
 */
@RunWith(Parameterized.class)
public class StaticContentWithoutMatchingRowsTest extends CQLTester
{
    private static final String SCHEMA = "CREATE TABLE %s (pk int, ck int, v int, st int static, PRIMARY KEY (pk, ck))";

    private static final String[] RESTRICTIONS = {
        "v = 1 OR v = 2",
        "v = 1 OR v = 9",
        "pk = 13 AND (v = 1 OR v = 9)",
        "pk = 14 AND (v = 1 OR v = 9)",
        "ck = 1 OR ck > 3",
        "pk = 13 AND (ck = 1 OR ck = 2)",
        "st = 1 AND (v = 1 OR v = 9)",
        // the partitions with only a static row still match through it
        "st = 1",
        "pk = 3 AND (st = 1 OR v = 1)",
    };

    // the live data, the same in both tables
    private static final String[] LIVE = {
        "INSERT INTO %s (pk, st) VALUES (1, 0)",
        "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)",
        "INSERT INTO %s (pk, ck, v) VALUES (2, 1, 2)",
        "INSERT INTO %s (pk, ck, v) VALUES (2, 4, 0)",
        "INSERT INTO %s (pk, st) VALUES (3, 1)",
        // 13, 14, 15: a static row, and only deleted data in the tested table
        "INSERT INTO %s (pk, st) VALUES (13, 1)",
        "INSERT INTO %s (pk, st) VALUES (14, 1)",
        "INSERT INTO %s (pk, st) VALUES (15, 1)",
    };

    // the deleted data, only in the tested table, written before the live data
    private static final String[] DELETED = {
        "INSERT INTO %s (pk, ck, v) VALUES (13, 1, 1)",
        "INSERT INTO %s (pk, ck, v) VALUES (13, 4, 9)",
        "DELETE FROM %s WHERE pk = 13 AND ck >= 0",
        "INSERT INTO %s (pk, ck, v) VALUES (14, 1, 1)",
        "DELETE FROM %s WHERE pk = 14 AND ck = 1",
        "INSERT INTO %s (pk, ck, v) VALUES (15, 1, 1) USING TTL 1",
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

    @Test
    public void testPartitionsWithoutMatchingRows() throws Throwable
    {
        String live = createTable(SCHEMA);
        for (String write : LIVE)
            executeFormattedQuery(write.replace("%s", KEYSPACE + '.' + live));

        createTable(SCHEMA);
        if (indexed)
            for (String column : new String[]{ "ck", "v", "st" })
                createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
        for (String write : DELETED)
            execute(write);
        for (String write : LIVE)
            execute(write);
        // let the TTL expire
        Thread.sleep(2100);

        List<String> failures = new ArrayList<>();
        check(failures, live, "memtable");
        flush();
        check(failures, live, "sstable");
        assertTrue(String.format("%d failures:%n%s", failures.size(), String.join("\n", failures)), failures.isEmpty());
    }

    private void check(List<String> failures, String live, String source)
    {
        for (String restriction : RESTRICTIONS)
        {
            String query = "SELECT pk, ck FROM %s WHERE " + restriction + " ALLOW FILTERING";
            List<String> expected = rows(query.replace("%s", KEYSPACE + '.' + live), "coordinator");
            for (String mode : new String[]{ "internal", "coordinator", "page 1", "page 3" })
            {
                List<String> rows = rows(query, mode);
                if (!expected.equals(rows))
                    failures.add(String.format("[%s, %s] %s: expected %s but got %s", source, mode, restriction, expected, rows));
            }
        }
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
}
