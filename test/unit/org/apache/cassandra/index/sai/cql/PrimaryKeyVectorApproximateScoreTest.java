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

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

/**
 * ANN ordering by a vector that is a primary key column, with the approximate scores of a search without reranking.
 * A primary key column has no cell to compute the exact score from.
 */
public class PrimaryKeyVectorApproximateScoreTest extends SAITester
{
    private static final String RERANKLESS = " WITH ann_options = {'rerank_k': 0}";

    @After
    public void resetVersion()
    {
        SAIUtil.resetCurrentVersion();
    }

    @Test
    public void testClusteringVector() throws Throwable
    {
        SAIUtil.setCurrentVersion(Version.LATEST);
        createTable("CREATE TABLE %s (k int, v vector<float, 2>, r int, PRIMARY KEY (k, v))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        execute("INSERT INTO %s (k, v, r) VALUES (1, [1, 0], 1)");
        execute("INSERT INTO %s (k, v, r) VALUES (1, [1, 0.1], 2)");
        execute("INSERT INTO %s (k, v, r) VALUES (2, [1, 0.2], 3)");
        flush();
        assertRows(execute("SELECT k, r FROM %s ORDER BY v ANN OF [1, 0] LIMIT 2" + RERANKLESS), row(1, 1), row(1, 2));
        assertRows(execute("SELECT k, r FROM %s ORDER BY v ANN OF [1, 0] LIMIT 3" + RERANKLESS), row(1, 1), row(1, 2), row(2, 3));
    }

    @Test
    public void testPartitionKeyVector() throws Throwable
    {
        SAIUtil.setCurrentVersion(Version.LATEST);
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, r int, PRIMARY KEY ((k1, k2), c))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 1, 1)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 2, 2)");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (2, [1, 0.2], 1, 3)");
        flush();
        assertRows(execute("SELECT c, r FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 2" + RERANKLESS), row(1, 1), row(2, 2));
        assertRows(execute("SELECT c, r FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 3" + RERANKLESS), row(1, 1), row(2, 2), row(1, 3));
    }
}
