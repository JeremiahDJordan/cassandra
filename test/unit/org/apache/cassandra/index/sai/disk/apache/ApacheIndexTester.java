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

package org.apache.cassandra.index.sai.disk.apache;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToDoubleBiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Helpers for the tests that read SAI indexes written by Apache Cassandra 5.0 (test/data/legacy-sai/apache-aa).
 * <p>
 * The main check is {@link #assertSameResultsAfterRebuild}: run queries while the indexes are read from Apache's
 * components, rebuild the indexes (in our format, which also replaces Apache's components), and check the same queries
 * return the same rows.
 */
public abstract class ApacheIndexTester extends SAITester
{
    protected static final String APACHE_PREFIX = "SAI+aa+";
    protected static final String SKINNY = "CREATE TABLE %s (pk int PRIMARY KEY, s text, n int, " +
                                           "v vector<float, 3>, x int)";
    protected static final String WIDE = "CREATE TABLE %s (pk int, ck int, st int static, s text, n bigint, " +
                                         "PRIMARY KEY (pk, ck))";
    protected static final String STATICS = "CREATE TABLE %s (pk1 int, pk2 text, ck int, st text static, " +
                                            "sti int static, v int, PRIMARY KEY ((pk1, pk2), ck))";
    protected static final String TYPES = "CREATE TABLE %s (pk int PRIMARY KEY, i int, bi bigint, si smallint, " +
                                          "ti tinyint, f float, d double, dec decimal, vi varint, dt date, tm time, " +
                                          "ts timestamp, u uuid, tu timeuuid, ip inet, b boolean, t text, a ascii)";

    protected static volatile CountDownLatch ownerPaused;
    protected static volatile CountDownLatch releaseOwner;
    protected static final AtomicInteger pauses = new AtomicInteger();

    // called by byteman
    @SuppressWarnings("unused")
    public static void pauseOwner() throws InterruptedException
    {
        if (pauses.incrementAndGet() == 1)
        {
            ownerPaused.countDown();
            releaseOwner.await(60, TimeUnit.SECONDS);
        }
    }

    /** An injection pausing the first index writer that has completed our per-SSTable components. */
    protected static Injection pauseAfterPerSSTableComplete()
    {
        ownerPaused = new CountDownLatch(1);
        releaseOwner = new CountDownLatch(1);
        pauses.set(0);
        return Injections.newCustom("pause_after_per_sstable_complete")
                         .add(afterPerSSTableWriterComplete())
                         .add(ActionBuilder.newActionBuilder()
                                           .actions()
                                           .doAction(ApacheIndexTester.class.getName() + ".pauseOwner()"))
                         .build();
    }

    protected static InvokePointBuilder afterPerSSTableWriterComplete()
    {
        return InvokePointBuilder.newInvokePoint()
                                 .onClass(StorageAttachedIndexWriter.class)
                                 .onMethod("complete")
                                 .after("INVOKE org.apache.cassandra.index.sai.disk.PerSSTableWriter.complete");
    }

    /** Loads the sstables of the provided Apache test data set (a directory of test/data/legacy-sai/apache-aa). */
    protected void loadApacheSSTables(String data) throws Exception
    {
        loadApacheSSTables(currentTable(), data);
    }

    protected void loadApacheSSTables(String table, String data) throws Exception
    {
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        FileUtils.copySSTablesAndIndexes(dir.toJavaIOFile(), "apache-aa/" + data);
        cfs.loadNewSSTables();
        assertThat(cfs.getLiveSSTables()).isNotEmpty();
    }

    /** Loads the first sstable of the provided Apache test data set only. */
    protected void loadSingleApacheSSTable(String data) throws Exception
    {
        ColumnFamilyStore table = getCurrentColumnFamilyStore();
        File dir = table.getDirectories().getDirectoryForNewSSTables();
        FileUtils.copySSTablesAndIndexes(dir.toJavaIOFile(), "apache-aa/" + data);
        for (File file : dir.tryList())
            if (file.name().startsWith("nb-2-"))
                file.delete();
        table.loadNewSSTables();
        assertThat(table.getLiveSSTables()).hasSize(1);
    }

    /** Checks the provided indexes are read from Apache's components on every sstable. */
    protected void assertReadFromApacheComponents(String... indexes)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = group.descriptorFor(sstable);
            assertThat(descriptor.perSSTableComponents().version())
                      .as("per-SSTable components of %s", sstable).isEqualTo(Version.APACHE_AA);
            for (String index : indexes)
                assertThat(descriptor.perIndexComponents(getIndexContext(index)).version())
                          .as("index %s on %s", index, sstable).isEqualTo(Version.APACHE_AA);
        }

        for (String index : indexes)
        {
            IndexContext context = getIndexContext(index);
            assertThat(context.getView().getIndexes()).hasSize(cfs.getLiveSSTables().size());
            for (SSTableIndex sstableIndex : context.getView().getIndexes())
                assertThat(sstableIndex.getVersion()).as("index %s on %s", index, sstableIndex.getSSTable()).isEqualTo(Version.APACHE_AA);
        }
    }

    /** Checks no sstable has Apache components, in its TOC or on disk, and the indexes use our current version. */
    protected void assertNoApacheComponents(String... indexes) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            File toc = sstable.descriptor.fileFor(SSTableFormat.Components.TOC);
            assertThat(Files.readAllLines(toc.toPath())).noneMatch(c -> c.startsWith(APACHE_PREFIX));
            String prefix = sstable.descriptor.baseFile().name() + '-' + APACHE_PREFIX;
            assertThat(Arrays.stream(sstable.descriptor.directory.tryList()).map(File::name)).noneMatch(n -> n.startsWith(prefix));

            IndexDescriptor descriptor = group.descriptorFor(sstable);
            assertThat(descriptor.perSSTableComponents().version()).isEqualTo(Version.current(KEYSPACE));
            for (String index : indexes)
                assertThat(descriptor.perIndexComponents(getIndexContext(index)).version()).isEqualTo(Version.current(KEYSPACE));
        }
    }

    /**
     * Runs the queries on the Apache components of the indexes, rebuilds the indexes, checks the queries return the
     * same rows from our components, and returns the results.
     */
    protected Map<String, Object> assertSameResultsAfterRebuild(List<String> queries, String... indexes) throws Throwable
    {
        assertReadFromApacheComponents(indexes);
        Map<String, Object> fromApache = runQueries(queries);
        // all the queries must have run on the indexes
        fromApache.forEach((query, result) -> assertThat(result).as(query).isNotInstanceOf(Throwable.class));

        rebuildIndexes(indexes);
        waitForTableIndexesQueryable();
        assertNoApacheComponents(indexes);

        Map<String, Object> fromOurs = runQueries(queries);
        for (String query : queries)
            assertThat(fromApache.get(query)).as(query).isEqualTo(fromOurs.get(query));
        return fromApache;
    }

    /**
     * Checks the queries return the same rows from the indexes of the current table as with ALLOW FILTERING from a
     * table without indexes loaded with the same data set, which is the current table afterwards.
     */
    protected void assertSameResultsAsUnindexed(String data, String createTableWithoutIndexes, List<String> queries) throws Throwable
    {
        assertSameResultsAsUnindexed(data, createTableWithoutIndexes, runQueries(queries));
    }

    /** The same, for the results of queries already run (see {@link #runQueries}). */
    protected void assertSameResultsAsUnindexed(String data, String createTableWithoutIndexes, Map<String, Object> fromApache) throws Throwable
    {
        List<String> queries = new ArrayList<>(fromApache.keySet());
        fromApache.forEach((query, result) -> assertThat(result).as(query).isNotInstanceOf(Throwable.class));

        createTable(createTableWithoutIndexes);
        disableCompaction();
        loadApacheSSTables(data);
        UnaryOperator<String> filtering = q -> q.contains("ALLOW FILTERING") ? q : q + " ALLOW FILTERING";
        Map<String, Object> filtered = runQueries(queries.stream().map(filtering).collect(Collectors.toList()));
        for (String query : queries)
            assertThat(fromApache.get(query)).as(query).isEqualTo(filtered.get(filtering.apply(query)));
    }

    /** Runs the queries; for each, its rows (as lists of values, in the order returned) or the exception it threw. */
    protected Map<String, Object> runQueries(List<String> queries)
    {
        Map<String, Object> results = new LinkedHashMap<>();
        for (String query : queries)
        {
            try
            {
                results.put(query, rows(execute(query)));
            }
            catch (Throwable t)
            {
                results.put(query, t);
            }
        }
        return results;
    }

    private static List<List<Object>> rows(UntypedResultSet resultSet)
    {
        List<List<Object>> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : resultSet)
        {
            List<Object> values = new ArrayList<>();
            for (var column : row.getColumns())
            {
                ByteBuffer bytes = row.getBytes(column.name.toString());
                values.add(bytes == null ? null : column.type.getString(bytes));
            }
            rows.add(values);
        }
        return rows;
    }

    /** The names of the files of the current table's sstables that are Apache components. */
    protected Set<String> apacheFiles()
    {
        return sstableFiles('-' + APACHE_PREFIX);
    }

    protected Set<String> sstableFiles(String marker)
    {
        return getCurrentColumnFamilyStore().getLiveSSTables()
                                            .stream()
                                            .flatMap(s -> Arrays.stream(s.descriptor.directory.tryList()))
                                            .map(File::name)
                                            .filter(n -> n.contains(marker))
                                            .collect(Collectors.toSet());
    }

    /** The vectors of the provided column, by pk, of the rows of the current table that have one. */
    protected Map<Integer, float[]> vectors(String column)
    {
        Map<Integer, float[]> vectors = new HashMap<>();
        for (UntypedResultSet.Row row : execute("SELECT pk, " + column + " FROM %s"))
        {
            if (!row.has(column))
                continue;
            List<Float> v = row.getVector(column, FloatType.instance, 8);
            float[] vector = new float[v.size()];
            for (int i = 0; i < vector.length; i++)
                vector[i] = v.get(i);
            vectors.put(row.getInt("pk"), vector);
        }
        return vectors;
    }

    /**
     * @return the pks of the provided vectors most similar to the query, by decreasing similarity
     */
    protected static List<Integer> bruteForce(Map<Integer, float[]> vectors, float[] query, int limit,
                                              ToDoubleBiFunction<float[], float[]> similarity)
    {
        return vectors.entrySet()
                      .stream()
                      .sorted(Comparator.comparingDouble((Map.Entry<Integer, float[]> e) -> -similarity.applyAsDouble(query, e.getValue()))
                      .thenComparing(Map.Entry::getKey))
                      .limit(limit)
                      .map(Map.Entry::getKey)
                      .collect(Collectors.toList());
    }

    protected static double dotProduct(float[] a, float[] b)
    {
        double dot = 0;
        for (int i = 0; i < a.length; i++)
            dot += a[i] * b[i];
        return dot;
    }

    protected static double euclidean(float[] a, float[] b)
    {
        double sum = 0;
        for (int i = 0; i < a.length; i++)
            sum += (a[i] - b[i]) * (a[i] - b[i]);
        return 1 / (1 + sum);
    }

    protected static double cosine(float[] a, float[] b)
    {
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.length; i++)
        {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        return dot / Math.sqrt(na * nb);
    }

    protected static String vectorLiteral(float[] v)
    {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < v.length; i++)
            builder.append(i == 0 ? "" : ", ").append(v[i]);
        return builder.append(']').toString();
    }
}
