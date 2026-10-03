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

import java.nio.ByteBuffer;
import java.nio.file.Files;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.github.jbellis.jvector.vector.types.FloatArray;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;


public class ConcurrentVectorValuesTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    @Test(expected = IllegalStateException.class)
    public void testConcurrentVectorValuesFailsForAlreadySetOrdinal()
    {
        var vectorValues = new ConcurrentVectorValues(1);
        vectorValues.add(0, vts.createFloatVector(new float[]{ 1.0f }));
        vectorValues.add(0, vts.createFloatVector(new float[]{ 2.0f }));
    }

    @Test()
    public void testGet()
    {
        var vectorValues = new ConcurrentVectorValues(1);
        vectorValues.add(1, vts.createFloatVector(new float[]{ 1.0f }));
        vectorValues.add(2, vts.createFloatVector(new float[]{ 2.0f }));

        assertNull(vectorValues.getVector(0));
        assertArrayEquals(new float[]{ 1.0f }, ((FloatArray) vectorValues.getVector(1)).array(), 0f);
    }

    @Test
    public void testWriteProviderVectorsInMappedOrder() throws Exception
    {
        var vectors = new ConcurrentVectorValues(2);
        vectors.add(2, vts.createFloatVector(new float[]{ -0.0f, 3.5f }));
        vectors.add(7, vts.createFloatVector(new float[]{ 1.25f, -2.5f }));
        File file = new File(temporaryFolder.newFile());
        try (SequentialWriter writer = new SequentialWriter(file, SequentialWriterOption.newBuilder().finishOnClose(true).build()))
        {
            vectors.write(writer, ordinal -> ordinal == 0 ? 7 : 2);
        }
        ByteBuffer bytes = ByteBuffer.wrap(Files.readAllBytes(file.toPath()));
        assertEquals(2, bytes.getInt());
        assertEquals(2, bytes.getInt());
        for (float value : new float[]{ 1.25f, -2.5f, -0.0f, 3.5f })
            assertEquals(Float.floatToRawIntBits(value), Float.floatToRawIntBits(bytes.getFloat()));
        assertEquals(0, bytes.remaining());
    }
}
