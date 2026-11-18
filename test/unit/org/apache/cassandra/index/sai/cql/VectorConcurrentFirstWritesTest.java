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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Several writers write the same new keys at the same time, each with its own vector. On a skip-list memtable the
 * index callbacks run inside the CAS loop of AtomicBTreePartition, so the vector memtable index can end up with a key
 * under the vectors of several writers; the flush must keep each row under the vector the row has.
 */
public class VectorConcurrentFirstWritesTest extends SAITester
{
    private static final int KEYS = 500;
    private static final int WRITERS = 3;

    @Test
    public void testSkipListMemtable() throws Throwable
    {
        test("skiplist");
    }

    @Test
    public void testTrieMemtable() throws Throwable
    {
        test("trie");
    }

    private void test(String memtable) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, n int, val vector<float, 2>) WITH memtable = '" + memtable + "'");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        disableCompaction();

        CyclicBarrier barrier = new CyclicBarrier(WRITERS);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < WRITERS; w++)
        {
            int writer = w;
            Thread thread = new Thread(() -> {
                try
                {
                    for (int k = 0; k < KEYS; k++)
                    {
                        barrier.await();
                        execute("INSERT INTO %s (pk, n, val) VALUES (?, ?, ?)", k, writer, vector(10f * k, 1000f * writer));
                    }
                }
                catch (Throwable t)
                {
                    failure.compareAndSet(null, t);
                    barrier.reset();
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads)
            thread.join();
        assertNull(failure.get());

        // In the memtable index the key of each row must be under the vector of the row (it can also be under the
        // vectors of the other writers, which the flush resolves).
        IndexContext context = getIndexContext(index);
        Memtable current = getCurrentColumnFamilyStore().getCurrentMemtable();
        CassandraOnHeapGraph<PrimaryKey> graph = ((VectorMemtableIndex) context.getLiveMemtables().get(current)).getGraph();
        int notUnderItsVector = 0;
        int underSeveralVectors = 0;
        for (int k = 0; k < KEYS; k++)
        {
            PrimaryKey key = context.keyFactory().create(Util.dk(Int32Type.instance.decompose(k)), Clustering.EMPTY);
            if (!graph.keysOf(vectorOf(k, writerOf(k))).contains(key))
                notUnderItsVector++;
            int vectors = 0;
            for (int w = 0; w < WRITERS; w++)
                if (graph.keysOf(vectorOf(k, w)).contains(key))
                    vectors++;
            if (vectors > 1)
                underSeveralVectors++;
        }
        assertEquals("rows whose key is not under their vector in the memtable index", 0, notUnderItsVector);
        long conflictsBefore = context.getIndexMetrics().get().memtableIndexFlushVectorRowIdConflicts.getCount();

        // After the flush, each row must be among the nearest rows of its own vector. A row indexed under the vector
        // of another writer is at least 1000 away from it, behind the 100 nearest rows. The flush counts the row id
        // of each key that was under several vectors.
        flush();
        assertEquals(underSeveralVectors, context.getIndexMetrics().get().memtableIndexFlushVectorRowIdConflicts.getCount() - conflictsBefore);
        int wrong = 0;
        String example = null;
        for (int k = 0; k < KEYS; k++)
        {
            int writer = writerOf(k);
            List<Integer> nearest = new ArrayList<>();
            for (UntypedResultSet.Row row : execute("SELECT pk FROM %s ORDER BY val ANN OF " + vectorLiteral(k, writer) + " LIMIT 100"))
                nearest.add(row.getInt("pk"));
            if (!nearest.contains(k))
            {
                wrong++;
                if (example == null)
                    example = "row " + k + " with vector " + vectorLiteral(k, writer);
            }
        }
        assertEquals("rows not among the 100 nearest rows of their own vector after the flush, e.g. " + example, 0, wrong);
    }

    private int writerOf(int k)
    {
        return execute("SELECT n FROM %s WHERE pk = ?", k).one().getInt("n");
    }

    private static ByteBuffer vectorOf(int k, int writer)
    {
        return VectorType.getInstance(FloatType.instance, 2).decompose(List.of(10f * k, 1000f * writer));
    }

    private static String vectorLiteral(int k, int writer)
    {
        return "[" + (10f * k) + ", " + (1000f * writer) + ']';
    }
}
