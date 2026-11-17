/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.cql;

import java.util.Arrays;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

/**
 * Index-ordered queries (ANN, generic ORDER BY, BM25) on a fully restricted partition with clustering column
 * restrictions that don't need filtering, on clustering columns that aren't indexed (CASSANDRA-19544). Those
 * restrictions are applied by the clustering index filter of the query, so the ordered index search must skip the rows
 * it doesn't select, which here are the best ranked ones, and still return LIMIT rows.
 */
public class OrderByWithClusteringRestrictionsTest extends SAITester
{
    private static final int ROWS = 1000;

    @Test
    public void testAnn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, v vector<float, 2>, PRIMARY KEY(pk, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        disableCompaction();

        // the closer a row is to [0, 0], the lower its clustering, in both partitions
        for (int pk = 0; pk < 2; pk++)
        {
            for (int c = 0; c < ROWS; c++)
            {
                execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", pk, c, vector(new float[]{ c + 1, 1 }));
                if (c % 300 == 299)
                    flush();
            }
        }

        assertOrderedRows("SELECT c FROM %s WHERE pk = 1 AND c >= 900 ORDER BY v ANN OF [0, 0] LIMIT 5", 900, 901, 902, 903, 904);
        assertOrderedRows("SELECT c FROM %s WHERE pk = 1 AND c > 995 ORDER BY v ANN OF [0, 0] LIMIT 10", 996, 997, 998, 999);
        assertOrderedRows("SELECT c FROM %s WHERE pk = 1 AND c > 500 AND c <= 502 ORDER BY v ANN OF [0, 0] LIMIT 10", 501, 502);
        assertOrderedRows("SELECT c FROM %s WHERE pk = 1 AND c = 777 ORDER BY v ANN OF [0, 0] LIMIT 10", 777);
        assertOrderedRows("SELECT c FROM %s WHERE pk = 1 AND c IN (999, 3, 500) ORDER BY v ANN OF [0, 0] LIMIT 2", 3, 500);
        assertOrderedRows("SELECT c FROM %s WHERE pk = 1 AND c < 0 ORDER BY v ANN OF [0, 0] LIMIT 10");

        flush();
        compact();
        assertOrderedRows("SELECT c FROM %s WHERE pk = 0 AND c >= 900 ORDER BY v ANN OF [0, 0] LIMIT 5", 900, 901, 902, 903, 904);
        assertOrderedRows("SELECT c FROM %s WHERE pk = 0 AND c IN (999, 3, 500) ORDER BY v ANN OF [0, 0] LIMIT 2", 3, 500);
    }

    @Test
    public void testGenericOrderBy() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c int, val int, PRIMARY KEY(pk, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction();

        for (int c = 0; c < ROWS; c++)
        {
            execute("INSERT INTO %s (pk, c, val) VALUES (0, ?, ?)", c, c);
            execute("INSERT INTO %s (pk, c, val) VALUES (1, ?, ?)", c, c);
            if (c % 300 == 299)
                flush();
        }

        runThenFlushThenCompact(() -> {
            assertOrderedRows("SELECT c FROM %s WHERE pk = 0 AND c >= 900 ORDER BY val ASC LIMIT 3", 900, 901, 902);
            assertOrderedRows("SELECT c FROM %s WHERE pk = 0 AND c < 100 ORDER BY val DESC LIMIT 3", 99, 98, 97);
            assertOrderedRows("SELECT c FROM %s WHERE pk = 0 AND c IN (5, 700, 4) ORDER BY val DESC LIMIT 2", 700, 5);
        });
    }

    @Test
    public void testBM25() throws Throwable
    {
        SAIUtil.setCurrentVersion(Version.LATEST);
        createTable("CREATE TABLE %s (pk int, c int, v text, PRIMARY KEY(pk, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        // the lower the clustering, the more often "apple" appears in the row, and the better its score
        for (int c = 0; c < 100; c++)
        {
            StringBuilder text = new StringBuilder();
            for (int i = 0; i < 100 - c; i++)
                text.append("apple ");
            for (int i = 0; i < c; i++)
                text.append("pear ");
            execute("INSERT INTO %s (pk, c, v) VALUES (0, ?, ?)", c, text.toString());
        }

        runThenFlushThenCompact(() -> {
            assertOrderedRows("SELECT c FROM %s WHERE pk = 0 AND c >= 90 ORDER BY v BM25 OF 'apple' LIMIT 3", 90, 91, 92);
        });
    }

    @After
    public void resetVersion()
    {
        SAIUtil.resetCurrentVersion();
    }

    private void assertOrderedRows(String query, int... expected)
    {
        assertRows(execute(query), Arrays.stream(expected).mapToObj(c -> row(c)).toArray(Object[][]::new));
    }
}
