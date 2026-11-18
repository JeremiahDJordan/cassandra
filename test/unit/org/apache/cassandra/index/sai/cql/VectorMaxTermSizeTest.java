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

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.IndexContext;

/**
 * Vectors over the maximum vector term size are never indexed, whether they are in a memtable, a flushed SSTable or a
 * compacted SSTable, so ANN queries never find them (CASSANDRA-18926). The rows themselves are kept.
 */
public class VectorMaxTermSizeTest extends VectorTester
{
    private static final String ANN_QUERY = "SELECT k FROM %s ORDER BY v ANN OF ? LIMIT 10";

    @Test
    public void testMaxTermSize() throws Throwable
    {
        // The vector must be over the maximum vector term size whichever way it is set: here, by the
        // cassandra.sai.max_vector_term_size_kb property (16KiB by default); with the SAI term size guardrails
        // (CASSANDRA-19493), by the fail threshold of the sai_vector_term_size guardrail (32KiB by default).
        long guardrailFailThreshold = DatabaseDescriptor.getGuardrailsConfig().getSaiVectorTermSizeFailThreshold().toBytes();
        long maxVectorTermSize = Math.max(IndexContext.MAX_VECTOR_TERM_SIZE, guardrailFailThreshold);
        int maxFloatVectorDimensions = (int) (maxVectorTermSize / Float.BYTES);
        int dimensions = maxFloatVectorDimensions + 1;
        Vector<Float> largeVector = vector(randomVector(dimensions));
        Vector<Float> otherLargeVector = vector(randomVector(dimensions));

        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, v vector<float, " + dimensions + ">)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        disableCompaction(KEYSPACE);

        // verify that the large vectors aren't written into the memtable index
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", largeVector);
        assertRows(execute("SELECT k, v FROM %s"), row(0, largeVector));
        assertEmpty(execute(ANN_QUERY, largeVector));

        // ...including when they replace another large vector in the same memtable
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", otherLargeVector);
        assertRows(execute("SELECT k, v FROM %s"), row(0, otherLargeVector));
        assertEmpty(execute(ANN_QUERY, otherLargeVector));

        // verify that the large vectors aren't written into the sstable indexes after flush
        flush();
        assertRows(execute("SELECT k, v FROM %s"), row(0, otherLargeVector));
        assertEmpty(execute(ANN_QUERY, otherLargeVector));

        // verify that the large vectors aren't written into the sstable indexes after compaction
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", largeVector);
        flush();
        compact();
        assertRowsIgnoringOrder(execute("SELECT k, v FROM %s"), row(0, otherLargeVector), row(1, largeVector));
        assertEmpty(execute(ANN_QUERY, largeVector));
    }
}
