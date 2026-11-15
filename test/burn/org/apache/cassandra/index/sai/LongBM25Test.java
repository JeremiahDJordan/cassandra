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

package org.apache.cassandra.index.sai;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import org.apache.cassandra.db.memtable.TrieMemtable;

public class LongBM25Test extends SAITester
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(LongBM25Test.class);

    private static final List<String> documentLines = new ArrayList<>();

    // Any multi-line English text works as a corpus: documents and queries are built from random lines of it
    private static final String CORPUS = "tokenization/adventures_of_huckleberry_finn_mark_twain.txt";

    static
    {
        try (InputStream in = LongBM25Test.class.getClassLoader().getResourceAsStream(CORPUS))
        {
            if (in == null)
                throw new RuntimeException("Could not find test resource " + CORPUS);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)))
            {
                reader.lines()
                      .map(String::trim)
                      .filter(line -> !line.isEmpty())
                      .forEach(documentLines::add);
            }
            if (documentLines.isEmpty())
                throw new RuntimeException("No document lines loaded from " + CORPUS);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to load test documents", e);
        }
    }

    KeySet keysInserted = new KeySet();
    private final int threadCount = 12;

    // Sized for the 1 GB heap of the test-burn JVM: with 4 memtable shards per writer thread and 10M keys, the 12
    // writers, which flush at random, keep enough memtables and index data in flight that the JVM spends its time in
    // back-to-back full GCs. One shard per writer thread and 100k keys run in a few minutes.
    private static final int numKeys = 100_000;

    @Before
    public void setup() throws Throwable
    {
        // we don't get loaded until after TM, so we can't affect the very first memtable,
        // but this will affect all subsequent ones
        TrieMemtable.SHARD_COUNT = threadCount;
    }

    @FunctionalInterface
    private interface Op
    {
        void run(int i) throws Throwable;
    }

    public void testConcurrentOps(Op op) throws ExecutionException, InterruptedException
    {
        createTable("CREATE TABLE %s (key int primary key, value text)");
        // Create analyzed index following BM25Test pattern
        createIndex("CREATE CUSTOM INDEX ON %s(value) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = {" +
                    "'index_analyzer': '{" +
                    "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                    "\"filters\" : [{\"name\" : \"porterstem\"}]" +
                    "}'}"
        );

        AtomicInteger counter = new AtomicInteger();
        long start = System.currentTimeMillis();
        var fjp = new ForkJoinPool(threadCount);
        var keys = IntStream.range(0, numKeys).boxed().collect(Collectors.toList());
        Collections.shuffle(keys);
        var task = fjp.submit(() -> keys.stream().parallel().forEach(i ->
                                                                     {
                                                                         wrappedOp(op, i);
                                                                         if (counter.incrementAndGet() % 10_000 == 0)
                                                                         {
                                                                             var elapsed = System.currentTimeMillis() - start;
                                                                             logger.info("{} ops in {}ms = {} ops/s", counter.get(), elapsed, counter.get() * 1000.0 / elapsed);
                                                                         }
                                                                         if (ThreadLocalRandom.current().nextDouble() < 0.001)
                                                                             flush();
                                                                     }));
        fjp.shutdown();
        task.get(); // re-throw
    }

    private static void wrappedOp(Op op, Integer i)
    {
        try
        {
            op.run(i);
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    private static String randomDocument()
    {
        var R = ThreadLocalRandom.current();
        int numLines = R.nextInt(5, 51); // 5 to 50 lines inclusive
        var selectedLines = new ArrayList<String>();

        for (int i = 0; i < numLines; i++)
        {
            selectedLines.add(randomQuery(R));
        }

        return String.join("\n", selectedLines);
    }

    private static String randomLine(ThreadLocalRandom R)
    {
        return documentLines.get(R.nextInt(documentLines.size()));
    }

    @Test
    public void testConcurrentReadsWritesDeletes() throws ExecutionException, InterruptedException
    {
        testConcurrentOps(i -> {
            var R = ThreadLocalRandom.current();
            if (R.nextDouble() < 0.2 || keysInserted.isEmpty())
            {
                var doc = randomDocument();
                execute("INSERT INTO %s (key, value) VALUES (?, ?)", i, doc);
                keysInserted.add(i);
            }
            else if (R.nextDouble() < 0.1)
            {
                var key = keysInserted.getRandom();
                execute("DELETE FROM %s WHERE key = ?", key);
            }
            else
            {
                var line = randomQuery(R);
                execute("SELECT * FROM %s ORDER BY value BM25 OF ? LIMIT ?", line, R.nextInt(1, 100));
            }
        });
    }

    private static String randomQuery(ThreadLocalRandom R)
    {
        while (true)
        {
            var line = randomLine(R);
            if (line.chars().anyMatch(Character::isAlphabetic))
                return line;
        }
    }

    @Test
    public void testConcurrentReadsWrites() throws ExecutionException, InterruptedException
    {
        testConcurrentOps(i -> {
            var R = ThreadLocalRandom.current();
            if (R.nextDouble() < 0.1 || keysInserted.isEmpty())
            {
                var doc = randomDocument();
                execute("INSERT INTO %s (key, value) VALUES (?, ?)", i, doc);
                keysInserted.add(i);
            }
            else
            {
                var line = randomQuery(R);
                execute("SELECT * FROM %s ORDER BY value BM25 OF ? LIMIT ?", line, R.nextInt(1, 100));
            }
        });
    }

    @Test
    public void testConcurrentWrites() throws ExecutionException, InterruptedException
    {
        testConcurrentOps(i -> {
            var doc = randomDocument();
            execute("INSERT INTO %s (key, value) VALUES (?, ?)", i, doc);
        });
    }

    private static class KeySet
    {
        private final Map<Integer, Integer> keys = new ConcurrentHashMap<>();
        private final AtomicInteger ordinal = new AtomicInteger();

        public void add(int key)
        {
            var i = ordinal.getAndIncrement();
            keys.put(i, key);
        }

        public int getRandom()
        {
            if (isEmpty())
                throw new IllegalStateException();
            var i = ThreadLocalRandom.current().nextInt(ordinal.get());
            // in case there is race with add(key), retry another random
            return keys.containsKey(i) ? keys.get(i) : getRandom();
        }

        public boolean isEmpty()
        {
            return keys.isEmpty();
        }
    }
}
