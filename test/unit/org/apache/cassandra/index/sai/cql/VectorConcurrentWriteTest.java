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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.junit.Assert.assertEquals;

/**
 * Concurrent writes of the same row into a {@code SkipListMemtable} must leave the memtable vector index with the
 * vector the row ends up with, and only that one.
 */
public class VectorConcurrentWriteTest extends SAITester
{
    private static final String CREATE_TABLE = "CREATE TABLE %s (pk int PRIMARY KEY, v vector<float, 2>) WITH memtable = 'skiplist'";
    private static final String CREATE_INDEX = "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}";

    @Test
    public void testConcurrentUpdatesWithClientTimestamps() throws Throwable
    {
        runConcurrentUpdates(true);
    }

    @Test
    public void testConcurrentUpdatesWithServerTimestamps() throws Throwable
    {
        runConcurrentUpdates(false);
    }

    /**
     * Many threads update the vectors of a few rows, each update with a vector of its own, then every row must be
     * indexed under the vector it has, and only under that one.
     */
    private void runConcurrentUpdates(boolean clientTimestamps) throws Throwable
    {
        createTable(CREATE_TABLE);
        createIndex(CREATE_INDEX);
        disableCompaction();

        int rows = 4;
        int threads = 16;
        int updatesPerThread = 2000;
        AtomicInteger updates = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try
        {
            List<Future<?>> futures = new ArrayList<>();
            long seed = 0;
            for (int t = 0; t < threads; t++)
            {
                Random random = new Random(seed++);
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < updatesPerThread; i++)
                    {
                        int pk = random.nextInt(rows);
                        int update = updates.incrementAndGet();
                        String vector = "[" + pk + ".0, " + update + ".0]";
                        if (clientTimestamps)
                            execute("UPDATE %s USING TIMESTAMP " + (1 + random.nextInt(1_000_000)) + " SET v = " + vector + " WHERE pk = " + pk);
                        else
                            execute("UPDATE %s SET v = " + vector + " WHERE pk = " + pk);
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures)
                future.get(5, TimeUnit.MINUTES);
        }
        finally
        {
            executor.shutdownNow();
        }

        Map<Integer, List<String>> expected = new HashMap<>();
        for (UntypedResultSet.Row row : execute("SELECT pk, v FROM %s"))
            expected.put(row.getInt("pk"), List.of(vectorString(row)));
        assertEquals(rows, expected.size());
        assertEquals(expected, memtableIndexedVectors());
    }

    private static String vectorString(UntypedResultSet.Row row)
    {
        return vectorString(row, "v");
    }

    private static String vectorString(UntypedResultSet.Row row, String column)
    {
        return row.getVector(column, FloatType.instance, 2).toString();
    }

    private static String vectorString(VectorFloat<?> vector)
    {
        List<Float> values = new ArrayList<>(vector.length());
        for (int i = 0; i < vector.length(); i++)
            values.add(vector.get(i));
        return values.toString();
    }

    /**
     * @return the vectors each row is indexed under in the graphs of the live memtable indexes, by partition key
     */
    @SuppressWarnings("unchecked")
    private Map<Integer, List<String>> memtableIndexedVectors() throws Exception
    {
        Field graphField = VectorMemtableIndex.class.getDeclaredField("graph");
        graphField.setAccessible(true);
        Field postingsMapField = CassandraOnHeapGraph.class.getDeclaredField("postingsMap");
        postingsMapField.setAccessible(true);

        Map<Integer, List<String>> vectors = new HashMap<>();
        StorageAttachedIndex index = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        for (MemtableIndex memtableIndex : index.getIndexContext().getLiveMemtables().values())
        {
            CassandraOnHeapGraph<PrimaryKey> graph = (CassandraOnHeapGraph<PrimaryKey>) graphField.get(memtableIndex);
            Map<VectorFloat<?>, VectorPostings<PrimaryKey>> postingsMap = (Map<VectorFloat<?>, VectorPostings<PrimaryKey>>) postingsMapField.get(graph);
            for (Map.Entry<VectorFloat<?>, VectorPostings<PrimaryKey>> entry : postingsMap.entrySet())
            {
                for (PrimaryKey primaryKey : entry.getValue().getPostings())
                {
                    int pk = Int32Type.instance.compose(primaryKey.partitionKey().getKey());
                    vectors.computeIfAbsent(pk, k -> new ArrayList<>()).add(vectorString(entry.getKey()));
                }
            }
        }

        return vectors;
    }
}
