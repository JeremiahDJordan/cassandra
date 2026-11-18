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

package org.apache.cassandra.index.sai.disk.apache;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.IntPredicate;
import java.util.function.ToDoubleBiFunction;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads the vector indexes Apache Cassandra 5.0 wrote, with jvector 1.0.2 (test/data/legacy-sai/apache-aa, written by
 * Apache cassandra-5.0 at fb7f1b7aee): jvector 4 reads their graphs and product quantization.
 */
public class ApacheVectorIndexTest extends ApacheIndexTester
{
    @Test
    public void testSkinny() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text, n int, v vector<float, 3>)");
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX skinny_n ON %s(n) USING 'sai'");
        createIndex("CREATE INDEX skinny_v ON %s(v) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("skinny");
        assertReadFromApacheComponents("skinny_s", "skinny_n", "skinny_v");

        // The rows are (pk, s = "s" + pk % 10, n = pk, v = [pk, pk % 7, 1]).
        assertRows(execute("SELECT pk FROM %s ORDER BY v ANN OF [42.0, 0.0, 1.0] LIMIT 1"), row(42));
        for (float[] query : new float[][]{ { 42, 0, 1 }, { 1, 1, 1 }, { 99, 6, 1 }, { -3, 2, 0.5f } })
        {
            assertThat(annPks("SELECT pk FROM %s ORDER BY v ANN OF " + vectorLiteral(query) + " LIMIT 5"))
                    .isEqualTo(expectedAnn(query, 5, pk -> true));
            assertThat(annPks("SELECT pk FROM %s WHERE n > 50 ORDER BY v ANN OF " + vectorLiteral(query) + " LIMIT 5"))
                    .isEqualTo(expectedAnn(query, 5, pk -> pk > 50));
            assertThat(annPks("SELECT pk FROM %s WHERE s = 's3' ORDER BY v ANN OF " + vectorLiteral(query) + " LIMIT 3"))
                    .isEqualTo(expectedAnn(query, 3, pk -> pk % 10 == 3));
        }
        assertThat(annPks("SELECT pk FROM %s WHERE pk = 17 ORDER BY v ANN OF [1.0, 1.0, 1.0] LIMIT 5")).containsExactly(17);
    }

    /**
     * A DOT_PRODUCT index with product quantization on vectors not of unit length ("dot": 4000 vectors of 8 gaussian
     * components, the first 200 scaled by 10) returns the same neighbours as brute force.
     */
    @Test
    public void testDotProductWithProductQuantization() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v vector<float, 8>)");
        disableCompaction();
        createIndex("CREATE INDEX dot_v ON %s(v) USING 'sai' WITH OPTIONS = {'similarity_function': 'DOT_PRODUCT'}");
        waitForTableIndexesQueryable();
        loadApacheSSTables("dot");
        assertReadFromApacheComponents("dot_v");
        Map<Integer, float[]> vectors = vectors("v");
        assertThat(vectors).hasSize(4000);

        float[] axis = { 1, 0, 0, 0, 0, 0, 0, 0 };
        assertThat(annPks("SELECT pk FROM %s ORDER BY v ANN OF " + vectorLiteral(axis) + " LIMIT 10"))
        .containsExactlyInAnyOrderElementsOf(bruteForce(vectors, axis, 10, ApacheVectorIndexTest::dotProduct));
        assertThat(recall(vectors, "v", new Random(1), ApacheVectorIndexTest::dotProduct)).isGreaterThanOrEqualTo(0.95);

        rebuildIndexes("dot_v");
        waitForTableIndexesQueryable();
        assertNoApacheComponents("dot_v");
        assertThat(recall(vectors, "v", new Random(1), ApacheVectorIndexTest::dotProduct)).isGreaterThanOrEqualTo(0.95);
    }

    /**
     * The average recall of 20 random ANN queries (LIMIT 10) on the provided column, against a brute force search of
     * the provided vectors.
     */
    private double recall(Map<Integer, float[]> vectors, String column, Random random,
                          ToDoubleBiFunction<float[], float[]> similarity)
    {
        int found = 0, expected = 0;
        for (int q = 0; q < 20; q++)
        {
            float[] query = new float[8];
            for (int i = 0; i < query.length; i++)
                query[i] = (float) random.nextGaussian();
            List<Integer> exact = bruteForce(vectors, query, 10, similarity);
            List<Integer> actual = annPks("SELECT pk FROM %s ORDER BY " + column + " ANN OF " + vectorLiteral(query) + " LIMIT 10");
            expected += exact.size();
            found += (int) actual.stream().filter(exact::contains).count();
        }
        return (double) found / expected;
    }

    private List<Integer> annPks(String query)
    {
        List<Integer> pks = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(query))
            pks.add(row.getInt("pk"));
        return pks;
    }

    /**
     * The pks of the rows of the skinny test data accepted by the filter, by decreasing cosine similarity of their
     * vector with the query.
     */
    private static List<Integer> expectedAnn(float[] query, int limit, IntPredicate filter)
    {
        List<Integer> pks = new ArrayList<>();
        for (int pk = 0; pk < 100; pk++)
            if (filter.test(pk))
                pks.add(pk);
        return pks.stream()
                  .sorted(Comparator.comparingDouble((Integer pk) -> -cosine(query, new float[]{ pk, pk % 7, 1 })).thenComparing(pk -> pk))
                  .limit(limit)
                  .collect(Collectors.toList());
    }
}
