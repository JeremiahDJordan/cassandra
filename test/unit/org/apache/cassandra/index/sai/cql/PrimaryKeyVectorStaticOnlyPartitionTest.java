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
 * ANN ordering by a vector in a partition key column: a partition with a static row alone is a match, through the key
 * of its static row. That key can come from several sources (a memtable and an sstable, two sstables), and its copies
 * must give the partition once, taking one slot of the limit.
 */
public class PrimaryKeyVectorStaticOnlyPartitionTest extends SAITester.Versioned
{
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.stream(new Version[]{ Version.EC, Version.ED, Version.GA }).map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Test
    public void testStaticOnlyPartitionInSeveralSources() throws Throwable
    {
        String select = "SELECT k1, c FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT ";
        // the result depends on the token order of the two partitions, so try both orders of a few pairs
        for (int[] ids : new int[][]{ { 1, 2 }, { 2, 1 }, { 3, 4 }, { 4, 3 }, { 5, 6 }, { 6, 5 } })
        {
            for (String variant : new String[]{ "memtable and sstable", "two sstables" })
            {
                createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, PRIMARY KEY ((k1, k2), c))");
                createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
                disableCompaction();
                int a = ids[0], p = ids[1];
                // a, the nearest, has a static row and a row; p has a static row alone, written twice
                execute("INSERT INTO %s (k1, k2, s) VALUES (?, [1, 0], 1)", a);
                execute("INSERT INTO %s (k1, k2, c, r) VALUES (?, [1, 0], 1, 1)", a);
                execute("INSERT INTO %s (k1, k2, s) VALUES (?, [1, 0.1], 1)", p);
                execute("INSERT INTO %s (k1, k2, c, r) VALUES (7, [1, 0.2], 1, 1)");
                execute("INSERT INTO %s (k1, k2, c, r) VALUES (8, [1, 0.3], 1, 1)");
                execute("INSERT INTO %s (k1, k2, c, r) VALUES (9, [1, 0.4], 1, 1)");
                flush();
                execute("INSERT INTO %s (k1, k2, s) VALUES (?, [1, 0.1], 2)", p);
                if (!variant.equals("memtable and sstable"))
                    flush();

                String message = String.format("static-only partition %d in %s, nearer partition %d, on %s", p, variant, a, version);
                assertRows(message, execute(select + 2), row(a, 1), row(p, null));
                assertRows(message, execute(select + 3), row(a, 1), row(p, null), row(7, 1));
                assertRows(message, execute(select + 4), row(a, 1), row(p, null), row(7, 1), row(8, 1));
                assertRows(message, execute(select + 10), row(a, 1), row(p, null), row(7, 1), row(8, 1), row(9, 1));
            }
        }
    }
}
