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

package org.apache.cassandra.distributed.upgrade;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.github.jbellis.jvector.disk.BufferedRandomAccessWriter;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.ByteSequence;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableTriFunction;
import org.apache.cassandra.distributed.shared.Versions.Version;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Graph and product quantization files written by this build's jvector in the jvector file format version 2, as an
 * {@code aa} vector index writes them, read by the jvector of an Apache node the way its SAI reads them.
 */
public class ApacheVectorFormatTest extends ApacheMixedClusterTestBase
{
    private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final int JVECTOR_FORMAT_VERSION = 2;
    private static final int VECTORS = 1500;
    private static final int DIMENSION = 16;
    private static final int SUBSPACES = 8;
    private static final int MAX_DEGREE = 16;
    private static final long GRAPH_OFFSET = 64;
    private static final int PQ_OFFSET = 48;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testApacheReadsGraphAndCompressedVectors() throws Throwable
    {
        // the Apache node loads ApacheReader from this build's test classes, after its own classes
        Version apache = apacheVersion();
        URL testClasses = ApacheReader.class.getProtectionDomain().getCodeSource().getLocation();
        Version apacheWithReader = new Version(apache.version, ArrayUtils.add(apache.classpath, testClasses));

        Random random = new Random(42);
        List<VectorFloat<?>> vectors = new ArrayList<>();
        for (int i = 0; i < VECTORS; i++)
        {
            float[] vector = new float[DIMENSION];
            for (int d = 0; d < DIMENSION; d++)
                vector[d] = random.nextFloat();
            vectors.add(VTS.createFloatVector(vector));
        }
        RandomAccessVectorValues values = new ListRandomAccessVectorValues(vectors, DIMENSION);

        GraphIndexBuilder builder = new GraphIndexBuilder(values, VectorSimilarityFunction.COSINE, MAX_DEGREE, 100, 1.0f, 1.2f, false);
        ImmutableGraphIndex graph = builder.build(values);

        Path dir = temporaryFolder.newFolder().toPath();
        Path graphFile = dir.resolve("graph");
        try (OnDiskGraphIndexWriter writer = new OnDiskGraphIndexWriter.Builder(graph, graphFile)
                                             .withStartOffset(GRAPH_OFFSET)
                                             .withVersion(JVECTOR_FORMAT_VERSION)
                                             .with(new InlineVectors(DIMENSION))
                                             .build())
        {
            writer.write(Map.of(FeatureId.INLINE_VECTORS, node -> new InlineVectors.State(values.getVector(node))));
        }

        ProductQuantization pq = ProductQuantization.compute(values, SUBSPACES, 256, false);
        PQVectors compressed = (PQVectors) pq.encodeAll(values);
        Path pqFile = dir.resolve("pq");
        try (BufferedRandomAccessWriter out = new BufferedRandomAccessWriter(pqFile))
        {
            out.write(new byte[PQ_OFFSET]);
            out.writeBoolean(true);
            compressed.write(out, JVECTOR_FORMAT_VERSION);
        }

        try (UpgradeableCluster cluster = startCluster(1, config -> {}, apacheWithReader))
        {
            Object[] read = ((IInvokableInstance) cluster.get(1)).appliesOnInstance(ApacheReader.read())
                                                                 .apply(graphFile.toString(), pqFile.toString(), new long[]{ GRAPH_OFFSET, PQ_OFFSET });

            int[] header = (int[]) read[0];
            float[][] readVectors = (float[][]) read[1];
            int[][] readNeighbors = (int[][]) read[2];
            byte[][] readCodes = (byte[][]) read[3];
            float[][] readDecoded = (float[][]) read[4];

            ImmutableGraphIndex.View view = graph.getView();
            assertEquals(VECTORS, header[0]);
            assertEquals(view.entryNode().node, header[1]);
            assertEquals(graph.maxDegree(), header[2]);

            VectorFloat<?> decoded = VTS.createFloatVector(DIMENSION);
            for (int node = 0; node < VECTORS; node++)
            {
                assertArrayEquals((float[]) values.getVector(node).get(), readVectors[node], 0f);
                assertArrayEquals(sorted(view.getNeighborsIterator(0, node)), sorted(readNeighbors[node]));

                ByteSequence<?> code = compressed.get(node);
                byte[] codeBytes = new byte[code.length()];
                for (int i = 0; i < codeBytes.length; i++)
                    codeBytes[i] = code.get(i);
                assertArrayEquals(codeBytes, readCodes[node]);

                pq.decode(code, decoded);
                assertArrayEquals((float[]) decoded.get(), readDecoded[node], 0f);
            }
        }
    }

    private static int[] sorted(NodesIterator neighbors)
    {
        int[] nodes = new int[neighbors.size()];
        for (int i = 0; i < nodes.length; i++)
            nodes[i] = neighbors.nextInt();
        return sorted(nodes);
    }

    private static int[] sorted(int[] nodes)
    {
        int[] copy = nodes.clone();
        Arrays.sort(copy);
        return copy;
    }

    /**
     * Runs on the Apache node, where jvector is the version that node ships, so it calls jvector and the node's
     * SAI reader through reflection. Opens the files as {@code DiskAnn} does.
     */
    public static class ApacheReader
    {
        static SerializableTriFunction<String, String, long[], Object[]> read()
        {
            return (graphPath, pqPath, offsets) -> {
                try
                {
                    Object graphHandle = openFileHandle(graphPath);
                    try
                    {
                        Object pqHandle = openFileHandle(pqPath);
                        try
                        {
                            Object[] graph = readGraph(graphHandle, offsets[0]);
                            int size = ((int[]) graph[0])[0];
                            Object[] compressed = readCompressedVectors(pqHandle, offsets[1], size);
                            return new Object[]{ graph[0], graph[1], graph[2], compressed[0], compressed[1] };
                        }
                        finally
                        {
                            invoke(pqHandle, "close");
                        }
                    }
                    finally
                    {
                        invoke(graphHandle, "close");
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            };
        }

        /**
         * The header (size, entry node, maximum degree), vectors and neighbours of the graph at {@code offset}.
         */
        private static Object[] readGraph(Object handle, long offset) throws Exception
        {
            Class<?> fileHandleClass = load("org.apache.cassandra.io.util.FileHandle");
            Method createSupplier = load("org.apache.cassandra.index.sai.disk.v1.vector.RandomAccessReaderAdapter").getDeclaredMethod("createSupplier", fileHandleClass);
            createSupplier.setAccessible(true);
            Object supplier = createSupplier.invoke(null, handle);
            Object graph = load("io.github.jbellis.jvector.disk.OnDiskGraphIndex").getConstructor(load("io.github.jbellis.jvector.disk.ReaderSupplier"), long.class)
                                                                                     .newInstance(supplier, offset);
            try
            {
                int size = (int) invoke(graph, "size");
                int maxDegree = (int) invoke(graph, "maxDegree");
                Object view = invoke(graph, "getView");
                try
                {
                    int entryNode = (int) invoke(view, "entryNode");
                    float[][] vectors = new float[size][];
                    int[][] neighbors = new int[size][];
                    for (int node = 0; node < size; node++)
                    {
                        vectors[node] = (float[]) invoke(view, "getVector", node);
                        Object iterator = invoke(view, "getNeighborsIterator", node);
                        neighbors[node] = new int[(int) invoke(iterator, "size")];
                        for (int i = 0; i < neighbors[node].length; i++)
                            neighbors[node][i] = (int) invoke(iterator, "nextInt");
                    }
                    return new Object[]{ new int[]{ size, entryNode, maxDegree }, vectors, neighbors };
                }
                finally
                {
                    invoke(view, "close");
                }
            }
            finally
            {
                invoke(graph, "close");
            }
        }

        /**
         * The codes and decoded vectors of the compressed vectors whose segment starts at {@code offset}.
         */
        private static Object[] readCompressedVectors(Object handle, long offset, int size) throws Exception
        {
            Class<?> fileHandleClass = load("org.apache.cassandra.io.util.FileHandle");
            Constructor<?> adapterConstructor = load("org.apache.cassandra.index.sai.disk.v1.vector.RandomAccessReaderAdapter").getDeclaredConstructor(fileHandleClass);
            adapterConstructor.setAccessible(true);
            Object reader = adapterConstructor.newInstance(handle);
            Object compressed;
            try
            {
                invoke(reader, "seek", offset);
                if (!(boolean) invoke(reader, "readBoolean"))
                    throw new AssertionError("No compressed vectors");
                long position = (long) invoke(reader, "getFilePointer");
                compressed = load("io.github.jbellis.jvector.pq.CompressedVectors").getMethod("load", load("io.github.jbellis.jvector.disk.RandomAccessReader"), long.class)
                                                                                   .invoke(null, reader, position);
            }
            finally
            {
                invoke(reader, "close");
            }

            Field pqField = compressed.getClass().getDeclaredField("pq");
            pqField.setAccessible(true);
            Object pq = pqField.get(compressed);
            int dimension = (int) invoke(pq, "getOriginalDimension");
            byte[][] codes = new byte[size][];
            float[][] decoded = new float[size][];
            for (int node = 0; node < size; node++)
            {
                codes[node] = (byte[]) invoke(compressed, "get", node);
                decoded[node] = new float[dimension];
                invoke(pq, "decode", codes[node], decoded[node]);
            }
            return new Object[]{ codes, decoded };
        }

        private static Object openFileHandle(String path) throws Exception
        {
            Class<?> fileClass = load("org.apache.cassandra.io.util.File");
            Object builder = load("org.apache.cassandra.io.util.FileHandle$Builder").getConstructor(fileClass)
                                                                                    .newInstance(fileClass.getConstructor(String.class).newInstance(path));
            builder = builder.getClass().getMethod("mmapped", boolean.class).invoke(builder, true);
            return invoke(builder, "complete");
        }

        private static Class<?> load(String name) throws ClassNotFoundException
        {
            return Class.forName(name, true, ApacheReader.class.getClassLoader());
        }

        private static Object invoke(Object target, String name, Object... args) throws Exception
        {
            for (Class<?> c = target.getClass(); c != null; c = c.getSuperclass())
            {
                for (Method method : c.getMethods())
                {
                    if (method.getName().equals(name) && method.getParameterCount() == args.length && !method.isBridge())
                    {
                        method.setAccessible(true);
                        return method.invoke(target, args);
                    }
                }
            }
            throw new NoSuchMethodException(target.getClass().getName() + '.' + name);
        }
    }
}
