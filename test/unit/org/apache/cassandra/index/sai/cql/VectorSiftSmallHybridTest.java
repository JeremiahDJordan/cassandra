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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;

import static org.apache.cassandra.index.sai.cql.VectorSiftSmallTest.readFvecs;
import static org.junit.Assert.assertTrue;

/**
 * Hybrid (ANN plus predicate) queries on the siftsmall dataset (CASSANDRA-20086). It only runs with the current
 * on-disk version: VectorSiftSmallTest and its shards cover the versions.
 */
public class VectorSiftSmallHybridTest extends VectorTester
{
    private static final String DATASET = "siftsmall";

    @Test
    public void testSiftSmallWithBooleanPredicatesOfVaryingSelectivity() throws Throwable
    {
        List<float[]> baseVectors = readFvecs(String.format("test/data/%s/%s_base.fvecs", DATASET, DATASET));
        List<float[]> queryVectors = readFvecs(String.format("test/data/%s/%s_query.fvecs", DATASET, DATASET));

        // Create table with regular id column and add SAI index on it
        createTable("CREATE TABLE %s (pk int, id int, val vector<float, 128>, PRIMARY KEY(pk))");
        // the ground truth below is computed with the euclidean distance
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(id) USING 'StorageAttachedIndex'");

        // Insert all vectors into unique partitions with id column
        insertVectorsWithId(baseVectors);

        int totalVectors = baseVectors.size();
        int topK = 100;

        // Test with tiny range restriction (1% of data)
        int tinyRangeEnd = totalVectors / 100;
        // Test with small range restriction (10% of data)
        int smallRangeEnd = totalVectors / 10;
        // Test with medium range restriction (50% of data)
        int mediumRangeEnd = totalVectors / 2;

        // Execute queries of different selectivity. We compute the ground truth for each query vector by brute
        // force because the dataset doesn't have it for filtered queries.
        beforeAndAfterFlush(() -> {
            double tinyRangeRecall = testRecallWithIdRange(queryVectors, baseVectors, 0, tinyRangeEnd, topK);
            assertTrue("Tiny range recall is " + tinyRangeRecall, tinyRangeRecall >= 0.99);

            double smallRangeRecall = testRecallWithIdRange(queryVectors, baseVectors, 0, smallRangeEnd, topK);
            assertTrue("Small range recall is " + smallRangeRecall, smallRangeRecall >= 0.975);

            double mediumRangeRecall = testRecallWithIdRange(queryVectors, baseVectors, 0, mediumRangeEnd, topK);
            assertTrue("Medium range recall is " + mediumRangeRecall, mediumRangeRecall >= 0.975);
        });

        // Finish by deleting all rows and verifying queries return correctly.
        // Using this test because it has many vectors already
        disableCompaction();
        for (int i = 0; i < totalVectors; i++)
            execute("DELETE FROM %s WHERE pk = ?", i);

        float[] vec = baseVectors.get(0);
        beforeAndAfterFlush(() -> {
            // Confirm all queries produce 0 rows. These queries hit several edge cases that are otherwise hard to
            // test, so we add them at the end of this hybrid sift test.
            assertRows(execute("SELECT pk FROM %s WHERE id >= ? AND id <= ? ORDER BY val ANN OF ? LIMIT ?", 0, tinyRangeEnd, vector(vec), 10));
            assertRows(execute("SELECT pk FROM %s WHERE id >= ? AND id <= ? ORDER BY val ANN OF ? LIMIT ?", 0, smallRangeEnd, vector(vec), 10));
            assertRows(execute("SELECT pk FROM %s WHERE id >= ? AND id <= ? ORDER BY val ANN OF ? LIMIT ?", 0, mediumRangeEnd, vector(vec), 10));
        });
    }

    private double testRecallWithIdRange(List<float[]> queryVectors, List<float[]> baseVectors,
                                         int idStart, int idEnd, int topK)
    {
        AtomicInteger topKfound = new AtomicInteger(0);

        // Perform query with id range restriction and compute recall
        IntStream.range(0, queryVectors.size()).parallel().forEach(i -> {
            float[] queryVector = queryVectors.get(i);
            String queryVectorAsString = Arrays.toString(queryVector);

            try
            {
                // Compute ground truth for this filtered range by brute force
                Set<Integer> filteredGroundTruth = computeGroundTruthForRange(queryVector, baseVectors, idStart, idEnd, topK);

                String query = String.format("SELECT pk FROM %%s WHERE id >= %d AND id <= %d ORDER BY val ANN OF %s LIMIT %d",
                                             idStart, idEnd, queryVectorAsString, topK);
                UntypedResultSet result = execute(query);

                // Count how many results are in the ground truth
                int n = (int) result.stream().filter(row -> filteredGroundTruth.contains(row.getInt("pk"))).count();
                topKfound.addAndGet(n);
            }
            catch (Throwable throwable)
            {
                throw new RuntimeException(throwable);
            }
        });

        return (double) topKfound.get() / (queryVectors.size() * topK);
    }

    private static Set<Integer> computeGroundTruthForRange(float[] queryVector, List<float[]> baseVectors,
                                                           int idStart, int idEnd, int topK)
    {
        // Create a list of (id, distance) pairs for vectors in the range
        List<Map.Entry<Integer, Float>> candidates = new ArrayList<>();

        for (int id = idStart; id <= idEnd && id < baseVectors.size(); id++)
        {
            float distance = euclideanDistance(queryVector, baseVectors.get(id));
            candidates.add(new AbstractMap.SimpleEntry<>(id, distance));
        }

        // Sort by distance and take top K
        candidates.sort(Map.Entry.comparingByValue());

        Set<Integer> groundTruth = new HashSet<>();
        int limit = Math.min(topK, candidates.size());
        for (int i = 0; i < limit; i++)
            groundTruth.add(candidates.get(i).getKey());

        return groundTruth;
    }

    private static float euclideanDistance(float[] a, float[] b)
    {
        float sum = 0;
        for (int i = 0; i < a.length; i++)
        {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    private void insertVectorsWithId(List<float[]> baseVectors)
    {
        IntStream.range(0, baseVectors.size()).parallel().forEach(i -> {
            try
            {
                execute("INSERT INTO %s (pk, id, val) VALUES (?, ?, ?)", i, i, vector(baseVectors.get(i)));
            }
            catch (Throwable throwable)
            {
                throw new RuntimeException(throwable);
            }
        });
    }
}
