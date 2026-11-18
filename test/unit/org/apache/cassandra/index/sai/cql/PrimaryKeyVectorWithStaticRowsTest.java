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

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.QueryController;

import static org.junit.Assert.assertEquals;

/**
 * ANN ordering by a vector that is a primary key column, in tables with static rows. The keys of static rows have no
 * clustering, and compare equal to every row key of their partition.
 */
public class PrimaryKeyVectorWithStaticRowsTest extends SAITester
{
    /** Filter-then-sort is the plan without the optimizer. */
    private static final String FILTER_THEN_SORT = "filter_then_sort";

    /** Sets up the given plan and returns the suffix of its query, which these plans don't need. */
    private static String usePlan(String plan)
    {
        QueryController.QUERY_OPT_LEVEL = plan.equals(FILTER_THEN_SORT) ? 0 : 1;
        return "";
    }

    /**
     * Filter-then-sort replaces a partition-level key (the key of a static row, from an index on a partition key
     * component or a static column) by the keys of the partition's rows. A primary key column has a value in every
     * row, but no cell holds it, so the rows must not be looked for by a cell of the ordered column.
     */
    @Test
    public void testFilterThenSortOnPartitionKeyVector() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, PRIMARY KEY ((k1, k2), c))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(k1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 1, 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 2, 2)");
        execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (2, [1, 0.1], 1, 1)");
        execute("INSERT INTO %s (k1, k2, s) VALUES (3, [1, 0.2], 1)");

        beforeAndAfterFlush(() -> {
            for (String hybridPlan : new String[]{ "", FILTER_THEN_SORT })
            {
                String plan = usePlan(hybridPlan);
                assertRows(execute("SELECT k1, c FROM %s WHERE k1 = 1 ORDER BY k2 ANN OF [1, 0] LIMIT 10" + plan), row(1, 1), row(1, 2));
                assertRows(execute("SELECT k1, c FROM %s WHERE k1 = 2 ORDER BY k2 ANN OF [1, 0] LIMIT 10" + plan), row(2, 1));
                // a partition with a static row alone is a match too, as without a predicate
                assertRows(execute("SELECT k1, c FROM %s WHERE k1 = 3 ORDER BY k2 ANN OF [1, 0] LIMIT 10" + plan), row(3, null));
                assertRowsIgnoringOrder(execute("SELECT k1, c FROM %s WHERE s = 1 ORDER BY k2 ANN OF [1, 0] LIMIT 10" + plan),
                                        row(1, 1), row(1, 2), row(3, null));
            }
        });
    }

    /**
     * With a vector in a partition key column, the static row and the rows of a partition all have the partition's
     * vector, so their keys are in the postings of the same vector, where the key of the static row is equal to the
     * keys of the rows. Each must stay there: the rows are returned through their keys, and the static row only for a
     * partition without rows.
     */
    @Test
    public void testOrderByPartitionKeyVectorWithStaticRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, PRIMARY KEY ((k1, k2), c))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        disableCompaction();
        // 1: the static row, then the rows; 2: rows only; 3: the static row only; 4: the rows, then the static row
        execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 1, 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 2, 2)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (2, [1, 0.1], 1, 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (2, [1, 0.1], 2, 2)");
        execute("INSERT INTO %s (k1, k2, s) VALUES (3, [1, 0.2], 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (4, [1, 0.3], 1, 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (4, [1, 0.3], 2, 2)");
        execute("INSERT INTO %s (k1, k2, s) VALUES (4, [1, 0.3], 2)");

        Object[][] all = { row(1, 1), row(1, 2), row(2, 1), row(2, 2), row(3, null), row(4, 1), row(4, 2) };
        CheckedFunction check = () -> {
            assertRowsIgnoringOrder(execute("SELECT k1, c FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 10"), all);
            // the two rows of partition 1, then one of partition 2 (its rows have the same score)
            UntypedResultSet limited = execute("SELECT k1, c FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 3");
            assertEquals(3, limited.size());
            assertEquals(2, countRowsOfPartition(limited, 1));
            assertEquals(1, countRowsOfPartition(limited, 2));
            assertRowsIgnoringOrder(execute("SELECT k1, c FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 2"), row(1, 1), row(1, 2));
            // a partition restriction (brute force over the keys of the partition)
            assertRowsIgnoringOrder(execute("SELECT k1, c FROM %s WHERE k1 = 1 AND k2 = [1, 0] ORDER BY k2 ANN OF [1, 0] LIMIT 10"),
                                    row(1, 1), row(1, 2));
            assertRows(execute("SELECT k1, c FROM %s WHERE k1 = 3 AND k2 = [1, 0.2] ORDER BY k2 ANN OF [1, 0] LIMIT 10"), row(3, null));
        };
        beforeAndAfterFlush(check);

        // deleting a row takes its key out of the postings, not the key of the static row, which is equal to it
        execute("DELETE FROM %s WHERE k1 = 1 AND k2 = [1, 0] AND c = 1");
        execute("DELETE FROM %s WHERE k1 = 4 AND k2 = [1, 0.3] AND c = 1");
        execute("DELETE FROM %s WHERE k1 = 4 AND k2 = [1, 0.3] AND c = 2");
        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT k1, c FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 10"),
                                    row(1, 2), row(2, 1), row(2, 2), row(3, null), row(4, null));
        });
    }

    private static int countRowsOfPartition(UntypedResultSet rows, int k1)
    {
        int count = 0;
        for (UntypedResultSet.Row row : rows)
            if (row.getInt("k1") == k1)
                count++;
        return count;
    }

    @Test
    public void testFilterThenSortOnClusteringVector() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c vector<float, 2>, s int static, r int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();
        execute("INSERT INTO %s (k, c, s, r) VALUES (1, [1, 0], 1, 1)");
        execute("INSERT INTO %s (k, c, r) VALUES (1, [1, 0.5], 2)");
        execute("INSERT INTO %s (k, c, s, r) VALUES (2, [1, 0.1], 2, 1)");
        execute("INSERT INTO %s (k, s) VALUES (3, 1)");

        beforeAndAfterFlush(() -> {
            for (String plan : new String[]{ "", FILTER_THEN_SORT })
                assertRows(execute("SELECT k, r FROM %s WHERE s = 1 ORDER BY c ANN OF [1, 0] LIMIT 10" + usePlan(plan)), row(1, 1), row(1, 2));
        });
    }
}
