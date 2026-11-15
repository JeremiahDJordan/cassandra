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

package org.apache.cassandra.metrics;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import org.apache.cassandra.metrics.PairedSlidingWindowReservoir.IntIntPair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PairedSlidingWindowReservoirTest
{
    @Test
    public void testSnapshotKeepsLastValues()
    {
        PairedSlidingWindowReservoir reservoir = new PairedSlidingWindowReservoir(3);
        assertEquals(0, reservoir.getSnapshot().values.length);

        reservoir.update(1, 10);
        reservoir.update(2, 20);
        IntIntPair[] values = reservoir.getSnapshot().values;
        assertEquals(2, values.length);
        assertEquals(1, values[0].x);
        assertEquals(20, values[1].y);

        for (int i = 3; i <= 5; i++)
            reservoir.update(i, i * 10);
        values = reservoir.getSnapshot().values;
        assertEquals(3, values.length);
        // the window keeps the last 3 updates (3, 4 and 5), in slots 2, 0 and 1
        assertEquals(4, values[0].x);
        assertEquals(5, values[1].x);
        assertEquals(3, values[2].x);
        for (IntIntPair pair : values)
            assertEquals(pair.x * 10, pair.y);
    }

    /**
     * V2VectorIndexSearcher updates its reservoir after each search and snapshots it for each query plan, from
     * concurrent read threads. A snapshot taken while the window becomes full must not contain null elements, which
     * made LinearFit.interceptSlopeFor throw an NPE (VectorSiftSmallTest.testCompaction).
     */
    @Test
    public void testSnapshotHasNoNullsWhileFilling() throws Exception
    {
        int size = 20;
        int readers = 3;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        AtomicLong nulls = new AtomicLong();
        ExecutorService pool = Executors.newFixedThreadPool(readers + 1);
        try
        {
            for (int iteration = 0; iteration < 20_000 && System.nanoTime() < deadline; iteration++)
            {
                PairedSlidingWindowReservoir reservoir = new PairedSlidingWindowReservoir(size);
                for (int i = 0; i < size - 1; i++)
                    reservoir.update(i, i);

                CountDownLatch start = new CountDownLatch(1);
                AtomicBoolean written = new AtomicBoolean();
                Future<?>[] readerFutures = new Future<?>[readers];
                for (int r = 0; r < readers; r++)
                {
                    readerFutures[r] = pool.submit(() -> {
                        start.await();
                        while (!written.get())
                        {
                            for (IntIntPair pair : reservoir.getSnapshot().values)
                            {
                                if (pair == null)
                                {
                                    nulls.incrementAndGet();
                                    break;
                                }
                            }
                        }
                        return null;
                    });
                }
                Future<?> writer = pool.submit(() -> {
                    start.await();
                    try
                    {
                        reservoir.update(size - 1, size - 1);
                    }
                    finally
                    {
                        // the readers spin until this is set
                        written.set(true);
                    }
                    return null;
                });
                start.countDown();
                writer.get();
                for (Future<?> f : readerFutures)
                    f.get();

                for (IntIntPair pair : reservoir.getSnapshot().values)
                    assertNotNull(pair);
            }
        }
        finally
        {
            pool.shutdownNow();
        }
        assertEquals("snapshots with a null element", 0, nulls.get());
    }
}
