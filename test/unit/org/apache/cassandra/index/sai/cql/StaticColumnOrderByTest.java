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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.Ordering;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.restrictions.StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code ORDER BY ... ANN OF} on an indexed static vector column returns every row of the matching partitions, all
 * with the score of the partition's vector (CASSANDRA-20086). The rows of a partition are tied, so they come in
 * clustering order.
 */
public class StaticColumnOrderByTest extends VectorTester.Versioned
{
    private static final String ANN = "ORDER BY v ANN OF [1, 0]";

    private static final Object[][] DEFAULT_ORDER = { row(1, 1), row(1, 2),
                                                      row(0, 1), row(0, 2), row(0, 3),
                                                      row(2, 1),
                                                      row(3, 1), row(3, 2) };

    private static final String[] HYBRID_PLANS = { "",
                                                   "WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}",
                                                   "WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'}" };

    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    private void createTableAndIndexes()
    {
        createTable("CREATE TABLE %s (pk int, ck int, r int, s int static, v vector<float, 2> static, PRIMARY KEY(pk, ck))");
        createVectorIndex("%s", "v");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
    }

    private void insertPartition(int pk, int s, String v, int... cks)
    {
        execute("INSERT INTO %s (pk, s, v) VALUES (?, ?, " + v + ')', pk, s);
        for (int ck : cks)
            execute("INSERT INTO %s (pk, ck, r) VALUES (?, ?, ?)", pk, ck, ck * 10);
    }

    private void insertDefaultData()
    {
        insertPartition(0, 20, "[1, 1]", 1, 2, 3);
        insertPartition(1, 10, "[1, 0]", 1, 2);
        insertPartition(2, 30, "[0, 1]", 1);
        insertPartition(3, 40, "[-1, 0.1]", 1, 2);
    }

    /**
     * Asserts the result of the ANN query with the given WHERE clause for several limits, with and without paging,
     * and with each hybrid plan if there's a WHERE clause.
     */
    private void assertOrdered(String where, Object[]... expected)
    {
        for (String plan : where.isEmpty() ? new String[]{ "" } : HYBRID_PLANS)
        {
            for (int limit : new int[]{ 1, 2, 3, 4, 5, 6, 100 })
            {
                String query = String.format("SELECT pk, ck FROM %%s %s %s LIMIT %d %s", where, ANN, limit, plan);
                Object[][] rows = Arrays.copyOf(expected, Math.min(limit, expected.length));
                assertRows(execute(query), rows);
                for (int pageSize : new int[]{ 1, 2, 3 })
                    assertRowsNet(executeNetWithPaging(query, pageSize), rows);
            }
        }
    }

    @Test
    public void testReturnsEveryRowOfThePartitions() throws Throwable
    {
        createTableAndIndexes();
        insertDefaultData();

        beforeAndAfterFlush(() -> {
            assertOrdered("", DEFAULT_ORDER);
            // Selecting the static columns and the regular column
            assertRows(execute("SELECT pk, ck, r, s FROM %s " + ANN + " LIMIT 4"),
                       row(1, 1, 10, 10), row(1, 2, 20, 10), row(0, 1, 10, 20), row(0, 2, 20, 20));
            assertRows(execute("SELECT * FROM %s " + ANN + " LIMIT 3"),
                       row(1, 1, 10, vector(1f, 0f), 10), row(1, 2, 10, vector(1f, 0f), 20), row(0, 1, 20, vector(1f, 1f), 10));
        });
    }

    @Test
    public void testWithOtherPredicates() throws Throwable
    {
        createTableAndIndexes();
        insertDefaultData();

        beforeAndAfterFlush(() -> {
            // Regular column
            assertOrdered("WHERE r >= 20", row(1, 2), row(0, 2), row(0, 3), row(3, 2));
            assertOrdered("WHERE r = 10", row(1, 1), row(0, 1), row(2, 1), row(3, 1));
            // Clustering column
            assertOrdered("WHERE ck >= 2", row(1, 2), row(0, 2), row(0, 3), row(3, 2));
            assertOrdered("WHERE ck < 3", row(1, 1), row(1, 2), row(0, 1), row(0, 2), row(2, 1), row(3, 1), row(3, 2));
            // Static column
            assertOrdered("WHERE s = 20", row(0, 1), row(0, 2), row(0, 3));
            assertOrdered("WHERE s >= 20", row(0, 1), row(0, 2), row(0, 3), row(2, 1), row(3, 1), row(3, 2));
            // Static and regular columns
            assertOrdered("WHERE s >= 20 AND r >= 20", row(0, 2), row(0, 3), row(3, 2));
            // Partition key
            assertOrdered("WHERE pk = 0", row(0, 1), row(0, 2), row(0, 3));
            assertOrdered("WHERE pk = 0 AND r >= 20", row(0, 2), row(0, 3));
            assertOrdered("WHERE pk = 3 AND r = 100");
        });
    }

    @Test
    public void testAcrossSSTables() throws Throwable
    {
        createTableAndIndexes();
        disableCompaction();

        // The static values in some sstables, the rows in others, and in the memtable
        execute("INSERT INTO %s (pk, s, v) VALUES (0, 20, [1, 1])");
        execute("INSERT INTO %s (pk, s, v) VALUES (1, 10, [1, 0])");
        flush();
        execute("INSERT INTO %s (pk, s, v) VALUES (2, 30, [0, 1])");
        execute("INSERT INTO %s (pk, ck, r) VALUES (0, 1, 10)");
        execute("INSERT INTO %s (pk, ck, r) VALUES (1, 2, 20)");
        flush();
        execute("INSERT INTO %s (pk, ck, r) VALUES (0, 2, 20)");
        execute("INSERT INTO %s (pk, ck, r) VALUES (1, 1, 10)");
        execute("INSERT INTO %s (pk, ck, r) VALUES (2, 1, 10)");
        flush();
        execute("INSERT INTO %s (pk, s, v) VALUES (3, 40, [-1, 0.1])");
        execute("INSERT INTO %s (pk, ck, r) VALUES (0, 3, 30)");
        execute("INSERT INTO %s (pk, ck, r) VALUES (3, 1, 10)");
        execute("INSERT INTO %s (pk, ck, r) VALUES (3, 2, 20)");

        beforeAndAfterFlush(() -> {
            assertOrdered("", DEFAULT_ORDER);
            assertOrdered("WHERE r >= 20", row(1, 2), row(0, 2), row(0, 3), row(3, 2));
            assertOrdered("WHERE ck = 1", row(1, 1), row(0, 1), row(2, 1), row(3, 1));
        });

        // Overwrite static vectors of older sstables
        execute("UPDATE %s SET s = 25, v = [1, 0.5] WHERE pk = 3");
        flush();
        execute("UPDATE %s SET s = 5, v = [1, 0.2] WHERE pk = 2");

        beforeAndAfterFlush(() -> {
            assertOrdered("", row(1, 1), row(1, 2), row(2, 1), row(3, 1), row(3, 2), row(0, 1), row(0, 2), row(0, 3));
            assertOrdered("WHERE s < 20", row(1, 1), row(1, 2), row(2, 1));
        });
    }

    @Test
    public void testUpdateAndDeleteStaticVector() throws Throwable
    {
        createTableAndIndexes();
        disableCompaction();
        insertDefaultData();
        flush();

        // Move partition 3 to the top, through one of its rows
        execute("INSERT INTO %s (pk, ck, s, v) VALUES (3, 1, 5, [1, 0.01])");
        // Move partition 1 to the end
        execute("UPDATE %s SET s = 50, v = [-1, 0] WHERE pk = 1");

        beforeAndAfterFlush(() -> {
            assertOrdered("", row(3, 1), row(3, 2), row(0, 1), row(0, 2), row(0, 3), row(2, 1), row(1, 1), row(1, 2));
            assertOrdered("WHERE s >= 20", row(0, 1), row(0, 2), row(0, 3), row(2, 1), row(1, 1), row(1, 2));
        });

        // Delete the static values of partition 0, a row of partition 3 and the whole partition 2
        execute("DELETE s, v FROM %s WHERE pk = 0");
        execute("DELETE FROM %s WHERE pk = 3 AND ck = 1");
        execute("DELETE FROM %s WHERE pk = 2");

        beforeAndAfterFlush(() -> {
            assertOrdered("", row(3, 2), row(1, 1), row(1, 2));
            assertOrdered("WHERE r = 10", row(1, 1));
            // The deleted row (3, 1) matches the clustering restriction, but partition 3 has no live row that does
            assertOrdered("WHERE ck < 2", row(1, 1));
        });

        // Delete the rows of partition 1, its static row has to be returned alone
        execute("DELETE FROM %s WHERE pk = 1 AND ck >= 0");
        beforeAndAfterFlush(() -> {
            assertOrdered("", row(3, 2), row(1, null));
            assertOrdered("WHERE r >= 0", row(3, 2));
        });
    }

    @Test
    public void testPartitionsWithoutRows() throws Throwable
    {
        createTableAndIndexes();
        insertPartition(0, 20, "[1, 1]", 1, 2);
        insertPartition(1, 10, "[1, 0]");
        insertPartition(2, 30, "[0, 1]");
        insertPartition(3, 40, "[-1, 0.1]", 1);

        beforeAndAfterFlush(() -> {
            assertOrdered("", row(1, null), row(0, 1), row(0, 2), row(2, null), row(3, 1));
            assertOrdered("WHERE s >= 20", row(0, 1), row(0, 2), row(2, null), row(3, 1));
            assertOrdered("WHERE r >= 0", row(0, 1), row(0, 2), row(3, 1));
        });
    }

    @Test
    public void testLargePartitions() throws Throwable
    {
        createTableAndIndexes();
        disableCompaction();

        int rows = 50;
        int[] cks = new int[rows];
        for (int i = 0; i < rows; i++)
            cks[i] = i;

        // Partition p has the vector [1, p], so they are ordered by p
        for (int pk = 0; pk < 4; pk++)
        {
            insertPartition(pk, pk, "[1, " + pk + ']', cks);
            if (pk % 2 == 0)
                flush();
        }

        beforeAndAfterFlush(() -> {
            for (int limit : new int[]{ 1, 10, 49, 50, 51, 120, 1000 })
            {
                Object[][] expected = new Object[Math.min(limit, 4 * rows)][];
                for (int i = 0; i < expected.length; i++)
                    expected[i] = row(i / rows, i % rows);

                String query = "SELECT pk, ck FROM %s " + ANN + " LIMIT " + limit;
                assertRows(execute(query), expected);
                assertRowsNet(executeNetWithPaging(query, 7), expected);
            }
        });
    }

    @Test
    public void testPerPartitionLimitAndDistinct() throws Throwable
    {
        createTableAndIndexes();
        insertDefaultData();

        beforeAndAfterFlush(() -> {
            for (String where : new String[]{ "", "WHERE r > 0" })
            {
                for (String plan : where.isEmpty() ? new String[]{ "" } : HYBRID_PLANS)
                {
                    String query = "SELECT pk, ck FROM %%s " + where + ' ' + ANN + " PER PARTITION LIMIT %d LIMIT %d " + plan;
                    assertRows(execute(String.format(query, 1, 3)), row(1, 1), row(0, 1), row(2, 1));
                    assertRows(execute(String.format(query, 1, 100)), row(1, 1), row(0, 1), row(2, 1), row(3, 1));
                    assertRows(execute(String.format(query, 2, 5)), row(1, 1), row(1, 2), row(0, 1), row(0, 2), row(2, 1));
                    assertRows(execute(String.format(query, 2, 100)),
                               row(1, 1), row(1, 2), row(0, 1), row(0, 2), row(2, 1), row(3, 1), row(3, 2));
                    assertRowsNet(executeNetWithPaging(String.format(query, 2, 5), 2),
                                  row(1, 1), row(1, 2), row(0, 1), row(0, 2), row(2, 1));
                }
            }

            assertRows(execute("SELECT pk, s FROM %s " + ANN + " LIMIT 3"), row(1, 10), row(1, 10), row(0, 20));
        });

        // SELECT DISTINCT is rejected when ANN uses the synthetic score column (the default from version ED)
        if (!Ordering.Ann.useSyntheticScore(version))
        {
            beforeAndAfterFlush(() -> {
                assertRows(execute("SELECT DISTINCT pk, s FROM %s " + ANN + " LIMIT 3"), row(1, 10), row(0, 20), row(2, 30));
                assertRows(execute("SELECT DISTINCT pk, s FROM %s " + ANN + " LIMIT 100"),
                           row(1, 10), row(0, 20), row(2, 30), row(3, 40));
            });
        }
    }

    /**
     * A selective predicate on a larger table, without hints, so the optimizer sorts the rows selected by the other
     * index with the vector index (filter-then-sort).
     */
    @Test
    public void testSelectivePredicate() throws Throwable
    {
        createTableAndIndexes();
        int partitions = 300;
        for (int pk = 0; pk < partitions; pk++)
        {
            // partition p has the vector [1, p / partitions], so the lowest pk is the closest to [1, 0]
            execute("INSERT INTO %s (pk, s, v) VALUES (?, ?, [1, " + ((float) pk / partitions) + "])", pk, pk);
            for (int ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, r) VALUES (?, ?, ?)", pk, ck, pk * 10 + ck);
        }

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk, ck FROM %s WHERE r = 1502 " + ANN + " LIMIT 3"), row(150, 2));
            assertRows(execute("SELECT pk, ck FROM %s WHERE r >= 1502 AND r <= 1511 " + ANN + " LIMIT 3"),
                       row(150, 2), row(150, 3), row(151, 0));
            assertRows(execute("SELECT pk, ck FROM %s WHERE r >= 1502 " + ANN + " LIMIT 3"), row(150, 2), row(150, 3), row(151, 0));
            assertRows(execute("SELECT pk, ck FROM %s WHERE ck = 3 " + ANN + " LIMIT 3"), row(0, 3), row(1, 3), row(2, 3));
            assertRows(execute("SELECT pk, ck FROM %s WHERE pk = 150 AND r = 1502 " + ANN + " LIMIT 3"), row(150, 2));
            assertRows(execute("SELECT pk, ck FROM %s WHERE s = 150 AND r = 1502 " + ANN + " LIMIT 3"), row(150, 2));
            assertRows(execute("SELECT pk, ck FROM %s WHERE s = 150 " + ANN + " LIMIT 3"), row(150, 0), row(150, 1), row(150, 2));
        });
        compact();
        assertRows(execute("SELECT pk, ck FROM %s WHERE r = 1502 " + ANN + " LIMIT 3"), row(150, 2));
        assertRows(execute("SELECT pk, ck FROM %s WHERE r >= 1502 " + ANN + " LIMIT 3"), row(150, 2), row(150, 3), row(151, 0));
    }

    @Test
    public void testReversedClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, r int, s int static, v vector<float, 2> static, PRIMARY KEY(pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC)");
        createVectorIndex("%s", "v");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");
        insertDefaultData();

        beforeAndAfterFlush(() -> {
            assertOrdered("", row(1, 2), row(1, 1), row(0, 3), row(0, 2), row(0, 1), row(2, 1), row(3, 2), row(3, 1));
            assertOrdered("WHERE r >= 20", row(1, 2), row(0, 3), row(0, 2), row(3, 2));
        });
    }

    /**
     * Rows that still exist without the regular column, and rows whose only cell was removed.
     */
    @Test
    public void testRowsWithoutLiveCells() throws Throwable
    {
        createTableAndIndexes();
        disableCompaction();
        insertDefaultData();
        flush();
        execute("UPDATE %s SET r = 5 WHERE pk = 1 AND ck = 0");
        execute("UPDATE %s SET r = null WHERE pk = 1 AND ck = 0");
        execute("DELETE r FROM %s WHERE pk = 1 AND ck = 1");
        execute("UPDATE %s SET r = null WHERE pk = 0 AND ck = 1");

        beforeAndAfterFlush(() -> {
            // (1, 1) and (0, 1) were inserted with INSERT, so they still exist without r, but (1, 0) doesn't exist
            assertOrdered("", DEFAULT_ORDER);
            assertOrdered("WHERE r > 0", row(1, 2), row(0, 2), row(0, 3), row(2, 1), row(3, 1), row(3, 2));
        });
    }

    /**
     * A regular vector column in a table with static columns: once the rows of a partition are deleted, only its
     * static row is left, and the partition isn't a match.
     */
    @Test
    public void testRegularVectorWithStaticRowLeftBehind() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, r int, s int static, v vector<float, 2>, PRIMARY KEY(pk, ck))");
        createVectorIndex("%s", "v");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, r, s, v) VALUES (0, 1, 1, 20, [1, 1])");
        execute("INSERT INTO %s (pk, ck, r, s, v) VALUES (1, 1, 1, 10, [1, 0])");
        execute("INSERT INTO %s (pk, ck, r, s, v) VALUES (1, 2, 1, 10, [1, 0.1])");
        execute("INSERT INTO %s (pk, ck, r, s, v) VALUES (2, 1, 1, 30, [0, 1])");
        flush();
        assertRows(execute("SELECT pk, ck FROM %s " + ANN + " LIMIT 10"), row(1, 1), row(1, 2), row(0, 1), row(2, 1));
        assertRows(execute("SELECT pk, ck FROM %s " + ANN + " PER PARTITION LIMIT 1 LIMIT 2"), row(1, 1), row(0, 1));

        execute("DELETE FROM %s WHERE pk = 1 AND ck >= 0");
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk, ck FROM %s " + ANN + " LIMIT 10"), row(0, 1), row(2, 1));
            assertRows(execute("SELECT pk, ck FROM %s " + ANN + " LIMIT 1"), row(0, 1));
            assertRows(execute("SELECT pk, s FROM %s " + ANN + " LIMIT 10"), row(0, 20), row(2, 30));
            assertRows(execute("SELECT pk, ck FROM %s WHERE s >= 10 " + ANN + " LIMIT 1"), row(0, 1));
            assertRows(execute("SELECT pk, ck FROM %s WHERE s = 10 " + ANN + " LIMIT 10"));
        });
    }

    @Test
    public void testGenericOrderByRejectsStaticColumn()
    {
        createTableAndIndexes();

        for (String direction : new String[]{ "ASC", "DESC" })
            assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s ORDER BY s " + direction + " LIMIT 10"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessage(String.format(NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, "s"));
    }
}
