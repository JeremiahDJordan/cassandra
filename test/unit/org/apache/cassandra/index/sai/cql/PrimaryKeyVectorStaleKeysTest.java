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
 * ANN ordering by a vector in a primary key column: the index has keys for rows that were deleted or expired since,
 * or that only ever had tombstones. A primary key column has no cell to tell such a key is stale with, so the row has
 * to be live, or the key takes a slot of the limit and of its partition (and from version ED on, where the replica
 * adds a score cell, the dead row is returned to the client).
 */
public class PrimaryKeyVectorStaleKeysTest extends SAITester.Versioned
{
    private static final String[] VARIANTS = { "deleted in the memtable", "deleted after the flush", "deleted in a second sstable",
                                               "deleted, compacted", "expired", "a cell tombstone of a row that doesn't exist" };

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.stream(new Version[]{ Version.EC, Version.ED, Version.GA }).map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    private void killRow(String variant, String insert, String delete, String deleteCell) throws Throwable
    {
        switch (variant)
        {
            case "deleted in the memtable":
                execute(insert);
                execute(delete);
                break;
            case "deleted after the flush":
                execute(insert);
                flush();
                execute(delete);
                break;
            case "deleted in a second sstable":
            case "deleted, compacted":
                execute(insert);
                flush();
                execute(delete);
                flush();
                if (variant.endsWith("compacted"))
                    compact();
                break;
            case "expired":
                execute(insert + " USING TTL 1");
                Thread.sleep(2100);
                break;
            default:
                execute(deleteCell);
        }
    }

    @Test
    public void testClusteringVector() throws Throwable
    {
        for (String variant : VARIANTS)
        {
            createTable("CREATE TABLE %s (k int, c vector<float, 2>, r int, PRIMARY KEY (k, c))");
            createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
            disableCompaction();
            execute("INSERT INTO %s (k, c, r) VALUES (1, [1, 0.1], 11)");
            execute("INSERT INTO %s (k, c, r) VALUES (2, [1, 0.2], 22)");
            execute("INSERT INTO %s (k, c, r) VALUES (2, [1, 0.3], 23)");
            execute("INSERT INTO %s (k, c, r) VALUES (3, [1, 0.4], 34)");
            // the nearest row, [1, 0] of partition 1, is the one that dies
            killRow(variant,
                    "INSERT INTO %s (k, c, r) VALUES (1, [1, 0], 1)",
                    "DELETE FROM %s WHERE k = 1 AND c = [1, 0]",
                    "DELETE r FROM %s WHERE k = 1 AND c = [1, 0]");

            String message = variant + " on " + version;
            assertRows(message, execute("SELECT k, r FROM %s ORDER BY c ANN OF [1, 0] LIMIT 1"), row(1, 11));
            assertRows(message, execute("SELECT k, r FROM %s ORDER BY c ANN OF [1, 0] LIMIT 2"), row(1, 11), row(2, 22));
            assertRows(message, execute("SELECT k, r FROM %s ORDER BY c ANN OF [1, 0] LIMIT 10"), row(1, 11), row(2, 22), row(2, 23), row(3, 34));
            assertRows(message, execute("SELECT k, r FROM %s ORDER BY c ANN OF [1, 0] PER PARTITION LIMIT 1 LIMIT 10"), row(1, 11), row(2, 22), row(3, 34));
            assertRows(message, execute("SELECT k, r FROM %s WHERE k = 1 ORDER BY c ANN OF [1, 0] LIMIT 1"), row(1, 11));
        }
    }

    @Test
    public void testPartitionKeyVector() throws Throwable
    {
        for (boolean withStatic : new boolean[]{ false, true })
        {
            for (String variant : VARIANTS)
            {
                createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, " + (withStatic ? "s int static, " : "") + "r int, PRIMARY KEY ((k1, k2), c))");
                createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
                disableCompaction();
                if (withStatic)
                    execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
                execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 2, 1)");
                execute("INSERT INTO %s (k1, k2, c, r) VALUES (2, [1, 0.2], 1, 1)");
                execute("INSERT INTO %s (k1, k2, c, r) VALUES (3, [1, 0.4], 1, 1)");
                // row c = 1 of partition 1, as near as row c = 2, is the one that dies
                killRow(variant,
                        "INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 1, 1)",
                        "DELETE FROM %s WHERE k1 = 1 AND k2 = [1, 0] AND c = 1",
                        "DELETE r FROM %s WHERE k1 = 1 AND k2 = [1, 0] AND c = 1");

                String message = variant + (withStatic ? " with static rows" : "") + " on " + version;
                String select = "SELECT k1, c FROM %s ";
                assertRows(message, execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 1"), row(1, 2));
                assertRows(message, execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 2"), row(1, 2), row(2, 1));
                assertRows(message, execute(select + "ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, 2), row(2, 1), row(3, 1));
                assertRows(message, execute(select + "ORDER BY k2 ANN OF [1, 0] PER PARTITION LIMIT 1 LIMIT 10"), row(1, 2), row(2, 1), row(3, 1));
                assertRows(message, execute(select + "WHERE k1 = 1 AND k2 = [1, 0] ORDER BY k2 ANN OF [1, 0] LIMIT 1"), row(1, 2));
            }
        }
    }

    /**
     * When every row of a partition has died, its static row is all that is left of it, and the static row answers
     * for the partition; when that is dead too, nothing does.
     */
    @Test
    public void testPartitionKeyVectorWithDeadRowsAndStaticRow() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, PRIMARY KEY ((k1, k2), c))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();
        execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 1, 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 2, 2)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (2, [1, 0.2], 1, 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (3, [1, 0.4], 1, 1)");
        execute("UPDATE %s SET s = 1 WHERE k1 = 3 AND k2 = [1, 0.4]");
        flush();
        execute("DELETE FROM %s WHERE k1 = 1 AND k2 = [1, 0] AND c = 1");
        execute("DELETE FROM %s WHERE k1 = 1 AND k2 = [1, 0] AND c = 2");
        // partition 3 loses its row and its static row
        execute("DELETE FROM %s WHERE k1 = 3 AND k2 = [1, 0.4] AND c = 1");
        execute("DELETE s FROM %s WHERE k1 = 3 AND k2 = [1, 0.4]");

        beforeAndAfterFlush(() -> {
            String message = "on " + version;
            assertRows(message, execute("SELECT k1, c FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, null), row(2, 1));
            assertRows(message, execute("SELECT k1, c FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 1"), row(1, null));
            // filter-then-sort expands the key of the static row, which the predicate gives
            for (String plan : new String[]{ "", " WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}" })
                assertRows(message, execute("SELECT k1, c FROM %s WHERE s = 1 ORDER BY k2 ANN OF [1, 0] LIMIT 10" + plan), row(1, null));
        });
        compact();
        assertRows(execute("SELECT k1, c FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(1, null), row(2, 1));
    }
}
