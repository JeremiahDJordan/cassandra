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
 * A key restricted ANN search of an sstable, where the first row id of the range has no vector: the static row of the
 * partition, or a row without a value for a regular vector column, at the versions whose vector postings map row ids
 * to ordinals with a sorted table (before DC).
 */
public class VectorKeyRestrictedRowsWithoutVectorTest extends SAITester.Versioned
{
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.stream(new Version[]{ Version.CA, Version.DB, Version.DC, Version.EC, Version.GA })
                     .map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Test
    public void testStaticRow() throws Throwable
    {
        // partition 3 is the last one in token order, so its rows are at the end of the sstable
        for (int withStatic : new int[]{ 1, 2, 3, 4 })
        {
            createTable("CREATE TABLE %s (k int, c vector<float, 2>, s int static, r int, PRIMARY KEY (k, c))");
            createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
            disableCompaction();
            for (int k = 1; k <= 4; k++)
            {
                if (k == withStatic)
                    execute("INSERT INTO %s (k, s) VALUES (?, 1)", k);
                execute("INSERT INTO %s (k, c, r) VALUES (?, [1, 0], 1)", k);
                execute("INSERT INTO %s (k, c, r) VALUES (?, ?, 1)", k, vector(0.5f, k / 10f));
            }
            beforeAndAfterFlush(() -> {
                for (int k = 1; k <= 4; k++)
                {
                    String message = "partition " + k + ", static row in " + withStatic + ", on " + version;
                    assertRowsIgnoringOrder(message, execute("SELECT k, r FROM %s WHERE k = ? ORDER BY c ANN OF [1, 0] LIMIT 10", k), row(k, 1), row(k, 1));
                    assertRowsIgnoringOrder(message, execute("SELECT k, r FROM %s WHERE token(k) >= token(?) AND token(k) <= token(?) ORDER BY c ANN OF [1, 0] LIMIT 10", k, k),
                                            row(k, 1), row(k, 1));
                }
            });
        }
    }

    @Test
    public void testRowsWithoutVector() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v vector<float, 2>, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        disableCompaction();
        for (int k = 1; k <= 4; k++)
        {
            // the first row of every partition has no vector
            execute("INSERT INTO %s (k, c) VALUES (?, 0)", k);
            execute("INSERT INTO %s (k, c, v) VALUES (?, 1, [1, 0])", k);
            execute("INSERT INTO %s (k, c, v) VALUES (?, 2, ?)", k, vector(0.5f, k / 10f));
        }
        beforeAndAfterFlush(() -> {
            for (int k = 1; k <= 4; k++)
            {
                String message = "partition " + k + " on " + version;
                assertRowsIgnoringOrder(message, execute("SELECT k, c FROM %s WHERE k = ? ORDER BY v ANN OF [1, 0] LIMIT 10", k), row(k, 1), row(k, 2));
            }
        });
    }
}
