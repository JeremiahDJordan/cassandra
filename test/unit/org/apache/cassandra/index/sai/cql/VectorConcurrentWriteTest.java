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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v1.vector.OnHeapGraph;
import org.apache.cassandra.index.sai.disk.v1.vector.VectorPostings;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.VectorMemoryIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Concurrent writes of the same row into a {@code SkipListMemtable}, whose partitions retry the merge of a write until
 * it is the one that publishes the partition, must leave the memtable vector index with the vector the row ends up
 * with, and only that one.
 */
public class VectorConcurrentWriteTest extends SAITester
{
    private static final String CREATE_TABLE = "CREATE TABLE %s (pk int PRIMARY KEY, v vector<float, 2>) WITH memtable = 'skiplist'";
    private static final String CREATE_INDEX = "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}";

    // the write held before its first attempt to publish its merge, and how many attempts it made
    private static volatile Thread heldWriter;
    private static volatile boolean held;
    private static volatile CountDownLatch holdReached;
    private static volatile CountDownLatch release;
    private static final AtomicInteger heldWriterAttempts = new AtomicInteger();

    /**
     * Called by the injected rule before each attempt of a write to publish its merge of a partition.
     */
    public static void beforePublish()
    {
        if (Thread.currentThread() != heldWriter)
            return;

        heldWriterAttempts.incrementAndGet();
        if (!held)
        {
            held = true;
            holdReached.countDown();
            try
            {
                release.await(1, TimeUnit.MINUTES);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    @After
    public void removeInjections()
    {
        Injections.deleteAll();
    }

    @Test
    public void testUpdateMergedBeforeNewerUpdate() throws Throwable
    {
        createTableWithOtherRows();
        execute("INSERT INTO %s (pk, v) VALUES (1, [1.0, 0.0]) USING TIMESTAMP 10");

        race("UPDATE %s USING TIMESTAMP 20 SET v = [0.0, 1.0] WHERE pk = 1",
             "UPDATE %s USING TIMESTAMP 30 SET v = [1.0, 0.0] WHERE pk = 1");

        assertRowOneIndexedUnderItsVectorOnly();

        flush();
        assertAnnResults();
    }

    @Test
    public void testDeleteMergedBeforeNewerUpdate() throws Throwable
    {
        createTableWithOtherRows();
        execute("INSERT INTO %s (pk, v) VALUES (1, [1.0, 0.0]) USING TIMESTAMP 10");

        race("DELETE v FROM %s USING TIMESTAMP 20 WHERE pk = 1",
             "UPDATE %s USING TIMESTAMP 30 SET v = [1.0, 0.0] WHERE pk = 1");

        assertRowOneIndexedUnderItsVectorOnly();
    }

    @Test
    public void testInsertMergedBeforeNewerInsert() throws Throwable
    {
        createTableWithOtherRows();

        race("INSERT INTO %s (pk, v) VALUES (1, [0.0, 1.0]) USING TIMESTAMP 20",
             "INSERT INTO %s (pk, v) VALUES (1, [1.0, 0.0]) USING TIMESTAMP 30");

        assertRowOneIndexedUnderItsVectorOnly();
    }

    @Test
    public void testUpdateMergedBeforeOlderUpdate() throws Throwable
    {
        createTableWithOtherRows();
        execute("INSERT INTO %s (pk, v) VALUES (1, [0.0, 1.0]) USING TIMESTAMP 10");

        race("UPDATE %s USING TIMESTAMP 30 SET v = [1.0, 0.0] WHERE pk = 1",
             "UPDATE %s USING TIMESTAMP 20 SET v = [0.0, 1.0] WHERE pk = 1");

        assertRowOneIndexedUnderItsVectorOnly();
    }

    /**
     * The newer write deletes the vector of a row that isn't in the memtable yet, and has no live data then, so the
     * index must see that row. The older write merges again against it, which changes nothing, so the index gets no
     * cell for that attempt.
     */
    @Test
    public void testUpdateMergedBeforeNewerVectorDelete() throws Throwable
    {
        createTableWithOtherRows();

        race("UPDATE %s USING TIMESTAMP 10 SET v = [0.0, 1.0] WHERE pk = 1",
             "DELETE v FROM %s USING TIMESTAMP 20 WHERE pk = 1");

        assertEmpty(execute("SELECT v FROM %s WHERE pk = 1"));
        assertEquals(Map.of(2, List.of("[0.9, 0.1]"), 3, List.of("[0.1, 0.9]")), memtableIndexedVectors());
    }

    @Test
    public void testInsertMergedBeforeNewerVectorDelete() throws Throwable
    {
        createTableWithOtherRows();

        race("INSERT INTO %s (pk, v) VALUES (1, [0.0, 1.0]) USING TIMESTAMP 10",
             "DELETE v FROM %s USING TIMESTAMP 20 WHERE pk = 1");

        assertRows(execute("SELECT pk, v FROM %s WHERE pk = 1"), row(1, null));
        assertEquals(Map.of(2, List.of("[0.9, 0.1]"), 3, List.of("[0.1, 0.9]")), memtableIndexedVectors());
    }

    /**
     * The newer write deletes a row that has no vector in the memtable: as a new row with no live data, and as an
     * update of a row with another column.
     */
    @Test
    public void testUpdateMergedBeforeNewerRowDelete() throws Throwable
    {
        for (boolean rowInMemtable : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int, c int, w int, v vector<float, 2>, PRIMARY KEY (pk, c)) WITH memtable = 'skiplist'");
            createIndex(CREATE_INDEX);
            execute("INSERT INTO %s (pk, c, v) VALUES (2, 0, [0.9, 0.1]) USING TIMESTAMP 1");
            if (rowInMemtable)
                execute("INSERT INTO %s (pk, c, w) VALUES (1, 0, 1) USING TIMESTAMP 1");

            race("UPDATE %s USING TIMESTAMP 10 SET v = [0.0, 1.0] WHERE pk = 1 AND c = 0",
                 "DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 1 AND c = 0");

            assertEmpty(execute("SELECT * FROM %s WHERE pk = 1"));
            assertEquals(Map.of(2, List.of("[0.9, 0.1]")), memtableIndexedVectors());
            Injections.deleteAll();
        }
    }

    /**
     * The writes with no live data that the vector index sees include a row deletion of a row that isn't in the
     * memtable. If the vector is part of the primary key, the deleted row must not be indexed under it.
     */
    @Test
    public void testRowDeleteWithVectorInPrimaryKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, w int, PRIMARY KEY (pk, v)) WITH memtable = 'skiplist'");
        createIndex(CREATE_INDEX);
        execute("INSERT INTO %s (pk, v, w) VALUES (2, [0.9, 0.1], 1)");
        execute("DELETE FROM %s WHERE pk = 1 AND v = [0.0, 1.0]");

        assertEquals(Map.of(2, List.of("[0.9, 0.1]")), memtableIndexedVectors());
    }

    @Test
    public void testConcurrentUpdatesWithClientTimestamps() throws Throwable
    {
        testConcurrentUpdates(true);
    }

    @Test
    public void testConcurrentUpdatesWithServerTimestamps() throws Throwable
    {
        testConcurrentUpdates(false);
    }

    /**
     * Many threads update the vectors of a few rows, each update with a vector of its own, then every row must be
     * indexed under the vector it has, and only under that one.
     */
    private void testConcurrentUpdates(boolean clientTimestamps) throws Throwable
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

    private void createTableWithOtherRows()
    {
        createTable(CREATE_TABLE);
        createIndex(CREATE_INDEX);
        execute("INSERT INTO %s (pk, v) VALUES (2, [0.9, 0.1]) USING TIMESTAMP 1");
        execute("INSERT INTO %s (pk, v) VALUES (3, [0.1, 0.9]) USING TIMESTAMP 1");
    }

    /**
     * Runs {@code firstMerged} on another thread and holds it once it has merged its write into the partition (and
     * passed the merged row to the index) but before it publishes the merge, then runs {@code firstPublished}, then
     * releases the first write, which has to merge again.
     */
    private void race(String firstMerged, String firstPublished) throws Throwable
    {
        holdReached = new CountDownLatch(1);
        release = new CountDownLatch(1);
        held = false;
        heldWriterAttempts.set(0);
        Injections.inject(holdBeforePublish());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> heldWrite = executor.submit(() -> {
                heldWriter = Thread.currentThread();
                execute(firstMerged);
                return null;
            });
            assertTrue("The first write did not reach the hold", holdReached.await(1, TimeUnit.MINUTES));
            execute(firstPublished);
            release.countDown();
            heldWrite.get(1, TimeUnit.MINUTES);
        }
        finally
        {
            heldWriter = null;
            release.countDown();
            executor.shutdownNow();
        }

        // the held write lost the race to publish its first merge and merged again
        assertEquals(2, heldWriterAttempts.get());
    }

    private static Injection holdBeforePublish()
    {
        return Injections.newCustom("hold_before_publish")
                         .add(InvokePointBuilder.newInvokePoint()
                                                .onClass("org.apache.cassandra.db.partitions.AtomicBTreePartition$Updater")
                                                .onMethod("tryUpdateData")
                                                .atInvoke("compareAndSet"))
                         .add(ActionBuilder.newActionBuilder()
                                           .actions()
                                           .doAction(Expression.expr(VectorConcurrentWriteTest.class.getName() + ".beforePublish").args()))
                         .build();
    }

    private void assertRowOneIndexedUnderItsVectorOnly() throws Throwable
    {
        assertEquals("[1.0, 0.0]", vectorString(execute("SELECT v FROM %s WHERE pk = 1").one()));
        assertEquals(Map.of(1, List.of("[1.0, 0.0]"),
                            2, List.of("[0.9, 0.1]"),
                            3, List.of("[0.1, 0.9]")),
                     memtableIndexedVectors());
        assertAnnResults();
    }

    private void assertAnnResults()
    {
        assertThat(annResults("[1.0, 0.0]")).containsExactly(1, 2, 3);
        assertThat(annResults("[0.0, 1.0]")).containsExactly(3, 2, 1);
    }

    private List<Integer> annResults(String vector)
    {
        List<Integer> keys = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT pk FROM %s ORDER BY v ANN OF " + vector + " LIMIT 3"))
            keys.add(row.getInt("pk"));
        return keys;
    }

    private static String vectorString(UntypedResultSet.Row row)
    {
        return row.getVector("v", FloatType.instance, 2).toString();
    }

    /**
     * @return the vectors each row is indexed under in the live memtable indexes, by partition key
     */
    @SuppressWarnings("unchecked")
    private Map<Integer, List<String>> memtableIndexedVectors() throws Exception
    {
        Field memoryIndexField = MemtableIndex.class.getDeclaredField("memoryIndex");
        memoryIndexField.setAccessible(true);
        Field graphField = VectorMemoryIndex.class.getDeclaredField("graph");
        graphField.setAccessible(true);
        Field postingsMapField = OnHeapGraph.class.getDeclaredField("postingsMap");
        postingsMapField.setAccessible(true);

        Map<Integer, List<String>> vectors = new HashMap<>();
        StorageAttachedIndex index = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        for (MemtableIndex memtableIndex : index.memtableIndexManager().getLiveMemtableIndexesSnapshot())
        {
            OnHeapGraph<PrimaryKey> graph = (OnHeapGraph<PrimaryKey>) graphField.get(memoryIndexField.get(memtableIndex));
            Map<float[], VectorPostings<PrimaryKey>> postingsMap = (Map<float[], VectorPostings<PrimaryKey>>) postingsMapField.get(graph);
            for (Map.Entry<float[], VectorPostings<PrimaryKey>> entry : postingsMap.entrySet())
            {
                for (PrimaryKey primaryKey : entry.getValue().getPostings())
                {
                    int pk = Int32Type.instance.compose(primaryKey.partitionKey().getKey());
                    vectors.computeIfAbsent(pk, k -> new ArrayList<>()).add(Arrays.toString(entry.getKey()));
                }
            }
        }
        return vectors;
    }
}
