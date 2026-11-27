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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.ConcurrentVectorValues;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.utils.CloseableIterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * A write inserts the new vectors it adds into the memtable graph once the memtable no longer holds the partition's
 * locks, before it returns.
 */
public class VectorGraphInsertTest extends SAITester
{
    private static final int WRITERS = 8;
    private static final int ROWS_PER_WRITER = 100;
    private static final int SHARED_VECTORS = 20;

    @Test
    public void testConcurrentWritersToOnePartitionSkipList() throws Throwable
    {
        testConcurrentWritersToOnePartition("skiplist");
    }

    @Test
    public void testConcurrentWritersToOnePartitionTrie() throws Throwable
    {
        testConcurrentWritersToOnePartition("trie");
    }

    /**
     * Writers add rows to one partition, half with a vector of their own and half with one of a few vectors they all
     * write: every vector is in the graph once they returned, and every row is found.
     */
    private void testConcurrentWritersToOnePartition(String memtable) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v vector<float, 2>, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
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
                    for (int i = 0; i < ROWS_PER_WRITER; i++)
                    {
                        barrier.await();
                        int ck = writer * ROWS_PER_WRITER + i;
                        execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", ck, vectorOf(ck));
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

        CassandraOnHeapGraph<PrimaryKey> graph = memtableGraph(index);
        assertEquals(SHARED_VECTORS + WRITERS * ROWS_PER_WRITER / 2, graph.size());
        assertEquals("vectors not in the graph", graph.size(), graph.graphSize());

        assertEveryRowFound(memtable);
        flush();
        assertEveryRowFound(memtable + ", after the flush");
    }

    @Test
    public void testSearchesDuringHeldInsertSkipList() throws Throwable
    {
        testSearchesDuringHeldInsert("skiplist");
    }

    @Test
    public void testSearchesDuringHeldInsertTrie() throws Throwable
    {
        testSearchesDuringHeldInsert("trie");
    }

    /**
     * The graph insert of a new vector [1, 1] is held, then another write of the vector returns, and graph searches
     * find every row with the vector: the row of the held write, moved from [-5, -5] or new, and that of the other write.
     */
    private void testSearchesDuringHeldInsert(String memtable) throws Throwable
    {
        // the held write, the write done during the hold, and the rows with [1, 1] then
        String[][] cases = {
            { "UPDATE %s SET v = [1, 1] WHERE pk = 0 AND ck = 1", null, "0:1" },
            { "INSERT INTO %s (pk, ck, v) VALUES (0, 2, [1, 1]) USING TIMESTAMP 10",
              "INSERT INTO %s (pk, ck, v) VALUES (0, 2, [1, 1]) USING TIMESTAMP 20", "0:2" },
            { "INSERT INTO %s (pk, ck, v) VALUES (0, 2, [1, 1]) USING TIMESTAMP 10",
              "INSERT INTO %s (pk, ck, v) VALUES (0, 2, [1, 1]) USING TIMESTAMP 10", "0:2" },
            { "INSERT INTO %s (pk, ck, v) VALUES (0, 2, [1, 1])", "INSERT INTO %s (pk, ck, v) VALUES (0, 3, [1, 1])", "0:2,0:3" },
            { "INSERT INTO %s (pk, ck, v) VALUES (0, 2, [1, 1])", "INSERT INTO %s (pk, ck, v) VALUES (1, 3, [1, 1])", "0:2,1:3" },
        };
        for (int c = 0; c < cases.length; c++)
        {
            String heldWrite = cases[c][0];
            String context = memtable + ", " + heldWrite + ", then " + cases[c][1];
            createTable("CREATE TABLE %s (pk int, ck int, v vector<float, 2>, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
            disableCompaction();
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, 0, [5, 5])");
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, 1, [-5, -5])");

            Injections.Barrier holdInsert = holdInsert("hold_graph_insert_" + memtable + c, false);
            CompletableFuture<Void> held = CompletableFuture.runAsync(() -> execute(heldWrite));
            try
            {
                waitForAssert(() -> assertThat(holdInsert.getCount()).isEqualTo(1), 60, TimeUnit.SECONDS);
                holdInsert.disable();
                if (cases[c][1] != null)
                    execute(cases[c][1]);

                Set<String> expected = Set.of(cases[c][2].split(","));
                assertEquals(context, expected, rowsNear("[1, 1]", expected.size()));
                assertEquals(context, expected, rowsOf(execute("SELECT pk, ck FROM %s WHERE GEO_DISTANCE(v, [1, 1]) < 1000")));
                assertFalse(context + ": the held write returned", held.isDone());
            }
            finally
            {
                holdInsert.countDown();
            }
            held.get(60, TimeUnit.SECONDS);

            CassandraOnHeapGraph<PrimaryKey> graph = memtableGraph(index);
            assertEquals(context, graph.size(), graph.graphSize());
            assertEquals(context, Set.of(cases[c][2].split(",")), rowsNear("[1, 1]", cases[c][2].split(",").length));
        }
    }

    /**
     * A vector whose node is in the graph, but whose insert hasn't ended: a search returns it once.
     */
    @Test
    public void testVectorInGraphBeforeItsInsertEnds() throws Throwable
    {
        for (String memtable : new String[]{ "skiplist", "trie" })
        {
            createTable("CREATE TABLE %s (pk int, ck int, v vector<float, 2>, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
            disableCompaction();
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, 0, [5, 5])");

            Injections.Barrier holdInsert = holdInsert("hold_graph_insert_exit_" + memtable, true);
            CompletableFuture<Void> held = CompletableFuture.runAsync(() -> execute("INSERT INTO %s (pk, ck, v) VALUES (0, 1, [1, 1])"));
            try
            {
                waitForAssert(() -> assertThat(holdInsert.getCount()).isEqualTo(1), 60, TimeUnit.SECONDS);
                CassandraOnHeapGraph<PrimaryKey> graph = memtableGraph(index);
                assertEquals(memtable, 2, graph.graphSize());
                List<Integer> nodes = new ArrayList<>();
                VectorFloat<?> query = VectorizationProvider.getInstance().getVectorTypeSupport().createFloatVector(new float[]{ 1, 1 });
                try (CloseableIterator<SearchResult.NodeScore> scores = graph.search(new QueryContext(), query, 2, 0, 0, false, Bits.ALL))
                {
                    scores.forEachRemaining(score -> nodes.add(score.node));
                }
                assertEquals(memtable + ": nodes found " + nodes, 2, new HashSet<>(nodes).size());
                assertEquals(memtable + ": nodes found " + nodes, 2, nodes.size());
                assertFalse(memtable + ": the held write returned", held.isDone());
            }
            finally
            {
                holdInsert.countDown();
            }
            held.get(60, TimeUnit.SECONDS);
        }
    }

    /**
     * A write of another partition adds a row to a new vector while the write adding the vector is held before it
     * gives it its ordinal: the second write returns once the vector has its ordinal, and searches then find both rows.
     * On a skip-list memtable, as the partitions of a trie memtable can share a shard lock.
     */
    @Test
    public void testWriteJoiningVectorWaitsForItsOrdinal() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v vector<float, 2>, PRIMARY KEY (pk, ck)) WITH memtable = 'skiplist'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 0, [5, 5])");

        Injections.Barrier holdOrdinal = Injections.newBarrier("hold_vector_values_add", 2, false)
                                                   .add(InvokePointBuilder.newInvokePoint()
                                                                          .onClass(ConcurrentVectorValues.class)
                                                                          .onMethod("add"))
                                                   .build();
        Injections.Counter waits = Injections.newCounter("await_ordinal")
                                             .add(InvokePointBuilder.newInvokePoint()
                                                                    .onClass("org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph$GraphInsert")
                                                                    .onMethod("awaitReady"))
                                             .build();
        Injections.inject(holdOrdinal, waits);
        CompletableFuture<Void> adding = CompletableFuture.runAsync(() -> execute("INSERT INTO %s (pk, ck, v) VALUES (0, 1, [1, 1])"));
        CompletableFuture<Void> joining;
        try
        {
            waitForAssert(() -> assertThat(holdOrdinal.getCount()).isEqualTo(1), 60, TimeUnit.SECONDS);
            holdOrdinal.disable();
            joining = CompletableFuture.runAsync(() -> execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, [1, 1])"));
            waitForAssert(() -> assertThat(waits.get()).isEqualTo(1), 60, TimeUnit.SECONDS);
            assertFalse("the write adding a row to the vector returned", joining.isDone());
            assertFalse("the write adding the vector returned", adding.isDone());
        }
        finally
        {
            holdOrdinal.countDown();
        }
        adding.get(60, TimeUnit.SECONDS);
        joining.get(60, TimeUnit.SECONDS);
        assertEquals(Set.of("0:1", "1:2"), rowsNear("[1, 1]", 2));
    }

    // holds the first graph insert at its entry, or at its exit once the node is in the graph
    private static Injections.Barrier holdInsert(String name, boolean atExit) throws Throwable
    {
        InvokePointBuilder point = InvokePointBuilder.newInvokePoint().onClass(CassandraOnHeapGraph.class).onMethod("insertIntoGraph");
        Injections.Barrier holdInsert = Injections.newBarrier(name, 2, false).add(atExit ? point.atExit() : point).build();
        Injections.inject(holdInsert);
        return holdInsert;
    }

    // the rows an unrestricted search of the memtable index returns, which searches its graph
    private Set<String> rowsNear(String vector, int limit)
    {
        return rowsOf(execute("SELECT pk, ck FROM %s ORDER BY v ANN OF " + vector + " LIMIT " + limit));
    }

    private static Set<String> rowsOf(UntypedResultSet result)
    {
        Set<String> rows = new HashSet<>();
        for (UntypedResultSet.Row row : result)
            rows.add(row.getInt("pk") + ":" + row.getInt("ck"));
        return rows;
    }

    private CassandraOnHeapGraph<PrimaryKey> memtableGraph(String index)
    {
        VectorMemtableIndex memtableIndex = (VectorMemtableIndex) getIndexContext(index).getLiveMemtables()
                                                                                        .get(getCurrentColumnFamilyStore().getCurrentMemtable());
        return memtableIndex.getGraph();
    }

    // a LIMIT of every row of the table, so that the graph search returns every vector
    private void assertEveryRowFound(String context)
    {
        int rows = WRITERS * ROWS_PER_WRITER;
        Set<Integer> found = new HashSet<>();
        for (UntypedResultSet.Row row : execute("SELECT ck FROM %s ORDER BY v ANN OF [0, 0] LIMIT " + rows))
            found.add(row.getInt("ck"));
        List<Integer> missing = new ArrayList<>();
        for (int ck = 0; ck < rows; ck++)
            if (!found.contains(ck))
                missing.add(ck);
        assertEquals(context + ": rows not found", List.of(), missing);
    }

    // even rows share one of SHARED_VECTORS vectors, odd rows have one of their own
    private static Object vectorOf(int ck)
    {
        return ck % 2 == 0 ? vector((float) ((ck / 2) % SHARED_VECTORS), 0f) : vector(1000f + ck, 1f);
    }
}
