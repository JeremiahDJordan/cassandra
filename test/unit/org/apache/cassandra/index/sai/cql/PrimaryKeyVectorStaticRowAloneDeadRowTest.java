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

import org.apache.cassandra.index.sai.SAITester;

/**
 * ANN ordering by a vector in a partition key column: a partition whose only row is dead (deleted, expired, or with
 * all its cells deleted) and that has a live static row isn't a match for a filter that the static row doesn't satisfy
 * on its own, even if the dead row satisfies it (a negation on a regular column, which a missing value satisfies).
 * <p>
 * The static-row-alone rule of StorageAttachedIndexSearcher's readPartitionUnit must be
 * FilterTree#isSatisfiedByStaticRow.
 */
public class PrimaryKeyVectorStaticRowAloneDeadRowTest extends SAITester
{
    @Test
    public void testDeadRowSatisfyingNegationWithLiveStaticRow() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, l set<int>, PRIMARY KEY ((k1, k2), c))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex'");
        disableCompaction();

        // every partition has a live static row with s = 1
        for (int k = 1; k <= 5; k++)
            execute("INSERT INTO %s (k1, k2, s) VALUES (?, ?, 1)", k, vector(1f, k / 10f));
        // 1: a deleted row
        execute("INSERT INTO %s (k1, k2, c, r, l) VALUES (1, [1, 0.1], 1, 0, {2})");
        execute("DELETE FROM %s WHERE k1 = 1 AND k2 = [1, 0.1] AND c = 1");
        // 2: an expired row
        execute("INSERT INTO %s (k1, k2, c, r, l) VALUES (2, [1, 0.2], 1, 0, {2}) USING TTL 1");
        // 3: a row without a marker, whose cells are deleted
        execute("UPDATE %s SET r = 0 WHERE k1 = 3 AND k2 = [1, 0.3] AND c = 1");
        execute("DELETE r FROM %s WHERE k1 = 3 AND k2 = [1, 0.3] AND c = 1");
        // 4: a live row that matches; 5: a live row that doesn't
        execute("INSERT INTO %s (k1, k2, c, r, l) VALUES (4, [1, 0.4], 1, 0, {2})");
        execute("INSERT INTO %s (k1, k2, c, r, l) VALUES (5, [1, 0.5], 1, 0, {1})");
        Thread.sleep(2500);

        for (String stage : new String[]{ "memtable", "sstable" })
        {
            if (stage.equals("sstable"))
                flush();
            for (String plan : new String[]{ "",
                                             " WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}",
                                             " WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'}" })
            {
                // the same statement without the ordering, on a table without indexes, returns (4, 1) alone
                assertRowsIgnoringOrder(execute("SELECT k1, c FROM %s WHERE s = 2 OR l NOT CONTAINS 1 ORDER BY k2 ANN OF [1, 0] LIMIT 10" + plan),
                                        row(4, 1));
                assertRowsIgnoringOrder(executeInternal(formatQuery("SELECT k1, c FROM %s WHERE s = 2 OR l NOT CONTAINS 1 ORDER BY k2 ANN OF [1, 0] LIMIT 10" + plan)),
                                        row(4, 1));
                // the partitions with a dead row are nearer: their static rows must not take the slot of the limit
                assertRows(execute("SELECT k1, c FROM %s WHERE s = 2 OR l NOT CONTAINS 1 ORDER BY k2 ANN OF [1, 0] LIMIT 1" + plan),
                           row(4, 1));
            }
        }
    }
}
