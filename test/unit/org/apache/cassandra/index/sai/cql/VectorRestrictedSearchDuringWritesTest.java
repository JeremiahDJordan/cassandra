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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A key restricted ANN search of a memtable brute-forces the keys of the restricted range, which writes to that range
 * change at the same time.
 */
public class VectorRestrictedSearchDuringWritesTest extends SAITester
{
    private static final int WRITERS = 2;
    private static final int READERS = 4;
    private static final long DURATION_MILLIS = 8000;

    @Test
    public void testPartitionRestrictedSearchDuringWrites() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ck int, v vector<float, 2>, PRIMARY KEY (k, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        disableCompaction();
        for (int ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (k, ck, v) VALUES (1, ?, ?)", ck, vector(1f, ck / 10f));

        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger queries = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < WRITERS; w++)
        {
            threads.add(new Thread(() -> {
                try
                {
                    // rows come and go in the partition, so its keys in the memtable index grow and shrink
                    while (!stop.get())
                    {
                        int ck = 20 + ThreadLocalRandom.current().nextInt(30);
                        if (ThreadLocalRandom.current().nextBoolean())
                            execute("INSERT INTO %s (k, ck, v) VALUES (1, ?, ?)", ck, vector(1f, ck / 10f));
                        else
                            execute("DELETE FROM %s WHERE k = 1 AND ck = ?", ck);
                    }
                }
                catch (Throwable t)
                {
                    failure.compareAndSet(null, t);
                }
            }));
        }
        for (int r = 0; r < READERS; r++)
        {
            threads.add(new Thread(() -> {
                try
                {
                    while (!stop.get())
                    {
                        execute("SELECT ck FROM %s WHERE k = 1 ORDER BY v ANN OF [1, 0] LIMIT 5");
                        queries.incrementAndGet();
                    }
                }
                catch (Throwable t)
                {
                    failure.compareAndSet(null, t);
                }
            }));
        }
        threads.forEach(Thread::start);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DURATION_MILLIS);
        while (System.nanoTime() < deadline && failure.get() == null)
            Thread.sleep(100);
        stop.set(true);
        for (Thread thread : threads)
            thread.join();

        assertNull("a query or a write failed after " + queries.get() + " queries", failure.get());
        assertTrue(queries.get() > 0);
    }
}
