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
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

/**
 * Queries on an index that yields the key of a static row (here, an index on a partition key component) first for a
 * wide partition, read that partition in full: check that LIMIT and paging return the right rows of it, in the
 * memtable and on disk.
 */
public class WidePartitionWithStaticRowTest extends SAITester
{
    private static final int ROWS = 500;

    @Test
    public void testLimitAndPaging()
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, ck int, st int static, v int, PRIMARY KEY ((pk1, pk2), ck))");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(pk1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // a wide partition with a static row, a wide partition without one, and a static row only
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 1, 5)");
        for (int ck = 0; ck < ROWS; ck++)
        {
            execute("INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 1, ?, ?)", ck, ck % 10);
            execute("INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 2, ?, ?)", ck, ck % 10);
        }
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 3, 6)");

        verify("memtable");
        flush();
        verify("flushed");
        // the static row in one sstable, the regular rows in another
        execute("INSERT INTO %s (pk1, pk2, st) VALUES (3, 1, 7)");
        flush();
        verify("2 sstables");
        compact();
        verify("compacted");
    }

    private void verify(String stage)
    {
        for (String restriction : new String[]{ "pk1 = 3", "pk1 = 3 AND v = 1" })
        {
            String query = "SELECT pk1, pk2, ck, v FROM %s WHERE " + restriction;
            List<String> all = rows(query);
            assertEquals(stage + ' ' + restriction, restriction.contains("v") ? 2 * ROWS / 10 : 2 * ROWS + 1, all.size());

            for (int limit : new int[]{ 1, 2, 10, 100, ROWS + 1 })
                assertEquals(stage + ' ' + restriction + " LIMIT " + limit, all.subList(0, Math.min(limit, all.size())), rows(query + " LIMIT " + limit));

            for (int pageSize : new int[]{ 1, 7, 100 })
            {
                List<String> paged = new ArrayList<>();
                for (com.datastax.driver.core.Row row : executeNetWithPaging(query, pageSize))
                    paged.add(row.getObject("pk1") + "," + row.getObject("pk2") + ',' + row.getObject("ck") + ',' + row.getObject("v"));
                assertEquals(stage + ' ' + restriction + " page size " + pageSize, all, paged);
            }
        }
    }

    private List<String> rows(String query)
    {
        List<String> rows = new ArrayList<>();
        for (var row : execute(query))
            rows.add((row.has("pk1") ? row.getInt("pk1") : null) + "," + (row.has("pk2") ? row.getInt("pk2") : null) + ','
                     + (row.has("ck") ? row.getInt("ck") : null) + ',' + (row.has("v") ? row.getInt("v") : null));
        return rows;
    }
}
