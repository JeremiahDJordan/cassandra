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

package org.apache.cassandra.index.sai.disk.vector;

import java.util.ArrayList;
import java.util.Random;
import org.junit.Test;

import io.github.jbellis.jvector.disk.ByteBufferIndexWriter;
import io.github.jbellis.jvector.disk.ByteBufferReader;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import static org.junit.Assert.*;

public class PqEncodingReuseTest
{
    @Test
    public void writeReuseDetachesSearchLayoutAndPreservesMetadata() throws Exception
    {
        var types = VectorizationProvider.getInstance().getVectorTypeSupport();
        var vectors = new ArrayList<VectorFloat<?>>();
        var random = new Random(941);
        for (int i = 0; i < 256; i++)
        {
            var vector = types.createFloatVector(32);
            for (int d = 0; d < 32; d++) vector.set(d, random.nextFloat());
            vectors.add(vector);
        }
        var values = new ListRandomAccessVectorValues(vectors, 32);
        var original = ProductQuantization.compute(values, 8, 256, false);
        var writer = ByteBufferIndexWriter.create(original.compressorSize() + 32, false);
        original.write(writer, OnDiskGraphIndex.CURRENT_VERSION);
        var search = ProductQuantization.loadForSearch(new ByteBufferReader(writer.getWrittenData()), VectorSimilarityFunction.COSINE);
        boolean transposed = search.isTransposed();
        var source = new CassandraOnHeapGraph.PqInfo(search, true, ProductQuantization.MAX_PQ_TRAINING_SET_SIZE);
        var reused = source.forEncoding();
        assertFalse(reused.pq.isTransposed());
        assertEquals(transposed, source.pq.isTransposed());
        assertTrue(reused.unitVectors);
        assertEquals(source.rowCount, reused.rowCount);
        assertEquals(original, reused.pq);
        assertEquals(original.encodeAll(values), reused.pq.encodeAll(values));
        assertSame(reused, reused.forEncoding());
        if (transposed) assertNotSame(source, reused);
        else assertSame(source, reused);
    }
}
