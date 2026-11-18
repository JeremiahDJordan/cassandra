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

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;

/**
 * ANN ordering by a vector in a partition key column, filter-then-sort: a row whose row marker (and queried columns)
 * were written again with a TTL and have expired is still alive by its older cells, which the query doesn't read. The
 * read of the candidate rows of a partition has to return it, as sort-then-filter and a query without the ordering do.
 */
public class PrimaryKeyVectorRowAliveByOlderCellsTest extends SAITester.Versioned
{
    private static final String[] PLANS = { " WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}",
                                            " WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'}",
                                            "" };

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.stream(new Version[]{ Version.EC, Version.ED, Version.GA }).map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Test
    public void testRowAliveByOlderCells() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, q int, l set<int>, PRIMARY KEY ((k1, k2), c))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        disableCompaction();

        // partition 1: its static row, and a row whose q and l stay alive
        execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
        execute("INSERT INTO %s (k1, k2, c, r, q, l) VALUES (1, [1, 0], 1, 7, 1, {2})");
        // partition 2, without a static row: the same
        execute("INSERT INTO %s (k1, k2, c, r, q, l) VALUES (2, [1, 0.2], 1, 7, 1, {2})");
        // partition 3: an ordinary row
        execute("INSERT INTO %s (k1, k2, c, r, q) VALUES (3, [1, 0.3], 1, 1, 1)");
        flush();
        // the row marker of the rows of partitions 1 and 2 is written again with a TTL (and r, for partition 2), and expires
        execute("INSERT INTO %s (k1, k2, c) VALUES (1, [1, 0], 1) USING TTL 1");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (2, [1, 0.2], 1, 7) USING TTL 1");
        Thread.sleep(2500);

        for (String stage : new String[]{ "sstable and memtable", "two sstables", "compacted" })
        {
            if (stage.equals("two sstables"))
                flush();
            else if (stage.equals("compacted"))
                compact();

            for (String plan : PLANS)
            {
                String message = stage + ", on " + version + plan;
                // the static predicate gives the key of the partition, which is expanded to the keys of its live rows
                check(message, "SELECT k1, c FROM %s WHERE s = 1 ORDER BY k2 ANN OF [1, 0] LIMIT 10" + plan, row(1, 1));
                // the clustering predicate selects the three rows; r, which the query reads, has expired in partition 2
                check(message, "SELECT k1, c FROM %s WHERE c = 1 OR r = 1 ORDER BY k2 ANN OF [1, 0] LIMIT 10" + plan,
                      row(1, 1), row(2, 1), row(3, 1));
                check(message, "SELECT k1, c FROM %s WHERE c >= 0 ORDER BY k2 ANN OF [1, 0] LIMIT 10" + plan,
                      row(1, 1), row(2, 1), row(3, 1));
            }
        }
    }

    private void check(String message, String query, Object[]... rows) throws Throwable
    {
        assertRowsIgnoringOrder(query + ", " + message, execute(query), rows);
    }
}
