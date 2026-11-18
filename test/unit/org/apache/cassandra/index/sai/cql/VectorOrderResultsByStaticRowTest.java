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
 * Filter-then-sort ANN maps the keys of the predicate to the row ids of each sstable. A key that isn't in the sstable
 * leads to the ceiling row of the sstable, which can be the static row of a partition, whose key compares equal to
 * the row keys of that partition.
 */
public class VectorOrderResultsByStaticRowTest extends SAITester
{
    private static final String QUERY = "SELECT pk1, pk2, ck FROM %s WHERE ck >= 0 ORDER BY vec ANN OF [1, 0] LIMIT 1000 " +
                                        "WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}";

    @Test
    public void testDeletedStaticCell() throws Throwable
    {
        test("DELETE st FROM %s WHERE pk1 = 3 AND pk2 = 3");
    }

    @Test
    public void testLiveStaticCell() throws Throwable
    {
        test("INSERT INTO %s (pk1, pk2, st) VALUES (3, 3, 5)");
    }

    private void test(String staticRowWrite) throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, ck int, st int static, v int, vec vector<float, 2>, PRIMARY KEY ((pk1, pk2), ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        disableCompaction();

        // partition (3, 3) has a static row in the sstable; (4, 1) sorts before it and is in the memtable only
        execute(staticRowWrite);
        execute("INSERT INTO %s (pk1, pk2, ck, v, vec) VALUES (4, 0, 5, 124, [7.746041, 8.3552])");
        execute("INSERT INTO %s (pk1, pk2, ck, v, vec) VALUES (3, 3, 2, 125, [5.55044, 9.288937])");
        flush();
        execute("INSERT INTO %s (pk1, pk2, ck) VALUES (4, 1, 2)");

        assertRowsIgnoringOrder(execute(QUERY), row(3, 3, 2), row(4, 0, 5));
        flush();
        assertRowsIgnoringOrder(execute(QUERY), row(3, 3, 2), row(4, 0, 5));
    }
}
