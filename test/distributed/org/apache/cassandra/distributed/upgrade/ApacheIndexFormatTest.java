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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MINIMUM_POSTINGS_LEAVES;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_TEST_LAST_VALID_SEGMENTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VECTOR_ENABLE_NVQ;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_5_MODE_ENABLED;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * SSTables whose SAI indexes this build wrote in Apache Cassandra 5.0's format, loaded by an Apache node without
 * building their indexes, and queried there.
 */
public class ApacheIndexFormatTest extends ApacheMixedClusterTestBase
{
    private static final String WIDE = "wide";
    private static final String SKINNY = "skinny";
    // the numeric types Apache indexes with a balanced tree, in two tables of fewer than 10 indexes each
    private static final String NUMBERS = "numbers";
    private static final String OTHERS = "others";
    // a bigint clustering column in descending order, which Apache indexes unreversed
    private static final String DESCENDING = "descending";
    private static final String[] NUMERIC_TABLES = { NUMBERS, OTHERS, DESCENDING };
    private static final Map<String, List<String>> NUMERIC_COLUMNS = Map.of(NUMBERS, List.of("i", "bi", "si", "ti", "vi", "d", "f"),
                                                                            OTHERS, List.of("db", "dt", "tm", "ts", "ip", "u"),
                                                                            DESCENDING, List.of("ck"));
    // enough for posting lists of several blocks, and several segments, in each sstable
    private static final int PARTITIONS = 500;
    private static final int SKINNY_PARTITIONS = 1500;
    // rows per segment, so that every index has several segments in each sstable
    private static final int SEGMENT_ROWS = 300;
    // enough for numeric segments of several leaf blocks of 1024 values, and posting lists of internal nodes
    private static final int NUMERIC_ROWS = 36000;
    private static final int NUMERIC_SEGMENT_ROWS = 10000;
    private static final int MIDDLE = NUMERIC_ROWS / 2;
    private static final int VARINT_AS_WRITTEN = 7;
    private static final String VECTORS = "vectors";
    private static final int VECTOR_ROWS = 6000;
    private static final int VECTOR_DIMENSION = 16;
    private static final int VECTOR_SEGMENT_ROWS = 1500;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testLiteralIndexes() throws Throwable
    {
        Path files;
        try (WithProperties properties = new WithProperties().set(SAI_TEST_LAST_VALID_SEGMENTS, SEGMENT_ROWS))
        {
            files = writeInApacheFormat(ApacheIndexFormatTest::createLiteralSchema, node -> {
                for (int pk = 0; pk < PARTITIONS; pk++)
                {
                    node.executeInternal(withKeyspace("INSERT INTO %s.wide (pk, st) VALUES (?, ?)"), pk, "st" + (pk % 3));
                    for (long ck = 0; ck < 5; ck++)
                        node.executeInternal(withKeyspace("INSERT INTO %s.wide (pk, ck, s, t) VALUES (?, ?, ?, ?)"), pk, ck, "s" + ck, "t" + ck);
                    // a row without live data, which Apache's row ids leave out
                    if (pk % 2 == 0)
                        node.executeInternal(withKeyspace("DELETE FROM %s.wide WHERE pk = ? AND ck = 1"), pk);
                }
                for (int pk = 0; pk < SKINNY_PARTITIONS; pk++)
                {
                    // decimals of a scale other than that of their canonical form (2.5 against 2.50), which Apache
                    // indexes as written
                    String decimal = pk % 3 + ".5";
                    node.executeInternal(withKeyspace("INSERT INTO %s.skinny (pk, s, fl, m, a, se) VALUES (?, ?, [" + decimal + "], {'k': " + decimal + "}, ?, {'x', 'y'})"),
                                         pk, "s" + (pk % 2), "Abc" + (pk % 2));
                }
            }, WIDE, SKINNY);
        }
        // the column completion markers say whether the index is empty, after the header ("aa")
        try (Stream<Path> markers = Files.list(files.resolve(WIDE)).filter(path -> path.toString().endsWith("+ColumnComplete.db")))
        {
            for (Path marker : markers.collect(Collectors.toList()))
                assertThat(Files.readAllBytes(marker)[7]).as(marker.toString()).isEqualTo((byte) (marker.toString().contains("+e_idx+") ? 1 : 0));
        }

        loadOnApache(files, ApacheIndexFormatTest::createLiteralSchema, (cluster, node, tableDirs) -> {
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE s = 's1'")))).containsExactlyInAnyOrderElementsOf(pks(PARTITIONS, pk -> pk % 2 == 1));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE s = 's3'")))).containsExactlyInAnyOrderElementsOf(pks(PARTITIONS, pk -> true));
            assertRows(node.executeInternal(withKeyspace("SELECT pk, ck FROM %s.wide WHERE s = 's3' AND pk = 7")), row(7, 3L));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE st = 'st1' AND ck = 2 ALLOW FILTERING")))).containsExactlyInAnyOrderElementsOf(pks(PARTITIONS, pk -> pk % 3 == 1));
            assertThat(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE e = 'e'"))).isEmpty();
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.skinny WHERE s = 's1'")))).containsExactlyInAnyOrderElementsOf(pks(SKINNY_PARTITIONS, pk -> pk % 2 == 1));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.skinny WHERE fl = [2.5]")))).containsExactlyInAnyOrderElementsOf(pks(SKINNY_PARTITIONS, pk -> pk % 3 == 2));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.skinny WHERE m['k'] = 2.5")))).containsExactlyInAnyOrderElementsOf(pks(SKINNY_PARTITIONS, pk -> pk % 3 == 2));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.skinny WHERE a = 'abc1'")))).containsExactlyInAnyOrderElementsOf(pks(SKINNY_PARTITIONS, pk -> pk % 2 == 1));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.skinny WHERE se CONTAINS 'y'")))).containsExactlyInAnyOrderElementsOf(pks(SKINNY_PARTITIONS, pk -> true));
            // the segments count their terms, two per row for the set
            Object[][] cells = node.executeInternal("SELECT cell_count FROM system_views.sai_sstable_indexes WHERE keyspace_name = ? AND index_name = 'skinny_se'", KEYSPACE);
            assertThat(Arrays.stream(cells).mapToLong(row -> (long) row[0]).sum()).isEqualTo(2L * SKINNY_PARTITIONS);


            // Apache builds a new index on the per-sstable components as written, numbering the rows as it does
            Map<String, Object> perSSTableFiles = fileKeys(tableDirs.get(WIDE), ApacheIndexFormatTest::isPerSSTableFile);
            cluster.schemaChange(withKeyspace("CREATE INDEX t_idx ON %s.wide (t) USING 'sai'"));
            await().atMost(1, TimeUnit.MINUTES)
                   .until(() -> Arrays.stream(node.executeInternal("SELECT is_queryable FROM system_views.sai_column_indexes WHERE keyspace_name = ? AND index_name = 't_idx'", KEYSPACE))
                                      .anyMatch(row -> Boolean.TRUE.equals(row[0])));
            assertThat(fileKeys(tableDirs.get(WIDE), ApacheIndexFormatTest::isPerSSTableFile)).isEqualTo(perSSTableFiles);
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE t = 't1'")))).containsExactlyInAnyOrderElementsOf(pks(PARTITIONS, pk -> pk % 2 == 1));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE t = 't4'")))).containsExactlyInAnyOrderElementsOf(pks(PARTITIONS, pk -> true));

            assertRebuildWritesSameBytes(node, WIDE, files.resolve(WIDE), tableDirs.get(WIDE), SEGMENT_ROWS, List.of("s_idx", "st_idx", "e_idx", "t_idx"));
            assertRebuildWritesSameBytes(node, SKINNY, files.resolve(SKINNY), tableDirs.get(SKINNY), SEGMENT_ROWS, List.of("skinny_s", "skinny_fl", "skinny_m", "skinny_a", "skinny_se"));
        }, WIDE, SKINNY);
    }

    @Test
    public void testClusteringWithNullFields() throws Throwable
    {
        Consumer<Consumer<String>> schema = schemaChange -> {
            schemaChange.accept(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"));
            schemaChange.accept(withKeyspace("CREATE TABLE %s.tuples (pk int, ck tuple<int, int>, ck2 frozen<list<tuple<int, int>>>, a text, b text, " +
                                             "m map<int, frozen<tuple<int, int>>>, PRIMARY KEY (pk, ck, ck2)) WITH CLUSTERING ORDER BY (ck DESC, ck2 ASC)"));
            schemaChange.accept(withKeyspace("CREATE INDEX tuples_a ON %s.tuples (a) USING 'sai'"));
            schemaChange.accept(withKeyspace("CREATE INDEX tuples_b ON %s.tuples (b) USING 'sai'"));
            schemaChange.accept(withKeyspace("CREATE INDEX tuples_m ON %s.tuples (ENTRIES(m)) USING 'sai'"));
        };
        Path files = writeInApacheFormat(schema, node -> {
            node.executeInternal(withKeyspace("INSERT INTO %s.tuples (pk, ck, ck2, a, b, m) VALUES (0, (null, 3), [(null, 1)], 'x', 'y', {1: (null, 2)})"));
            node.executeInternal(withKeyspace("INSERT INTO %s.tuples (pk, ck, ck2, a, b, m) VALUES (0, (null, 2), [(null, 1)], 'y', 'x', {1: (null, 3)})"));
            node.executeInternal(withKeyspace("INSERT INTO %s.tuples (pk, ck, ck2, a, b, m) VALUES (0, (null, 1), [(null, 1)], 'x', 'x', {1: (null, 2)})"));
        }, "tuples");
        loadOnApache(files, schema, (cluster, node, tableDirs) -> {
            assertRows(node.executeInternal(withKeyspace("SELECT pk, a, b FROM %s.tuples WHERE a = 'x' AND b = 'x'")), row(0, "x", "x"));
            // map entries whose value is a tuple with a null field
            assertRows(node.executeInternal(withKeyspace("SELECT pk, a, b FROM %s.tuples WHERE m[1] = (null, 3)")), row(0, "y", "x"));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.tuples WHERE m[1] = (null, 2)")))).hasSize(2);
            assertRebuildWritesSameBytes(node, "tuples", files.resolve("tuples"), tableDirs.get("tuples"), SEGMENT_ROWS, List.of("tuples_a", "tuples_b", "tuples_m"));
        }, "tuples");
    }

    @Test
    public void testNumericIndexes() throws Throwable
    {
        // posting lists for the internal nodes with two leaves or more below them, rather than 64, here and on Apache
        try (WithProperties properties = new WithProperties().set(SAI_TEST_LAST_VALID_SEGMENTS, NUMERIC_SEGMENT_ROWS)
                                                             .set(SAI_MINIMUM_POSTINGS_LEAVES, 2))
        {
            Path files = writeInApacheFormat(ApacheIndexFormatTest::createNumericSchema, node -> {
                for (int pk = 0; pk < NUMERIC_ROWS; pk++)
                {
                    for (String table : NUMERIC_TABLES)
                        node.executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk, " + String.join(", ", NUMERIC_COLUMNS.get(table)) + ") " +
                                                          "VALUES (" + pk + ", " + String.join(", ", numericValues(table, pk)) + ')'));
                }
            }, NUMERIC_TABLES);

            loadOnApache(files, ApacheIndexFormatTest::createNumericSchema, (cluster, node, tableDirs) -> checkNumericIndexes(node, files, tableDirs), NUMERIC_TABLES);
        }
    }

    private static void checkNumericIndexes(IInvokableInstance node, Path files, Map<String, Path> tableDirs) throws Throwable
    {
        // a value of each column, and ranges across several leaf blocks and segments
        int value = 21000;
        for (String table : NUMERIC_TABLES)
        {
            List<String> columns = NUMERIC_COLUMNS.get(table);
            List<String> values = numericValues(table, value);
            for (int c = 0; c < columns.size(); c++)
            {
                IntPredicate expected = columns.get(c).equals("ti") ? pk -> pk % 256 == value % 256 : pk -> pk == value;
                assertNumericRows(node, table, columns.get(c) + " = " + values.get(c), expected);
            }
        }
        assertNumericRows(node, NUMBERS, "i >= -10000 AND i < 10000", pk -> pk >= MIDDLE - 10000 && pk < MIDDLE + 10000);
        assertNumericRows(node, NUMBERS, "bi >= -5000000000000 AND bi < 7000000000000", pk -> pk >= MIDDLE - 5000 && pk < MIDDLE + 7000);
        assertNumericRows(node, NUMBERS, "si >= -17000 AND si <= 0", pk -> pk >= MIDDLE - 17000 && pk <= MIDDLE);
        assertNumericRows(node, NUMBERS, "ti >= 0 AND ti < 10", pk -> pk % 256 >= 128 && pk % 256 < 138);
        assertNumericRows(node, NUMBERS, "vi >= -50000000000000000000000000000 AND vi < 120000000000000000000000000000", pk -> pk >= MIDDLE - 5000 && pk < MIDDLE + 12000 || pk == VARINT_AS_WRITTEN);
        assertNumericRows(node, NUMBERS, "d >= -125.0 AND d < 32.5", pk -> pk >= MIDDLE - 12500 && pk < MIDDLE + 3250);
        assertNumericRows(node, NUMBERS, "f > -100.0 AND f < 100.0", pk -> pk >= MIDDLE - 100 && pk < MIDDLE + 100);
        assertNumericRows(node, OTHERS, "db >= -10000.0 AND db <= 2500.0", pk -> pk >= MIDDLE - 8000 && pk <= MIDDLE + 2000);
        assertNumericRows(node, OTHERS, "dt >= '2020-02-01' AND dt < '2021-01-01'", pk -> pk >= 31 && pk < 366);
        assertNumericRows(node, OTHERS, "tm >= '01:00:00' AND tm < '05:00:00'", pk -> pk >= 3600 && pk < 18000);
        assertNumericRows(node, OTHERS, "ts >= 5000000 AND ts < 29000000", pk -> pk >= 5000 && pk < 29000);
        assertNumericRows(node, OTHERS, "ip >= '10.0.10.0' AND ip < '10.0.100.0'", pk -> pk >= 10 * 256 && pk < 100 * 256);
        assertNumericRows(node, DESCENDING, "ck >= -100 AND ck < 50", pk -> pk >= MIDDLE - 100 && pk < MIDDLE + 50);

        for (String table : NUMERIC_TABLES)
        {
            List<String> indexes = NUMERIC_COLUMNS.get(table).stream().map(column -> table + '_' + column).collect(Collectors.toList());
            assertRebuildWritesSameBytes(node, table, files.resolve(table), tableDirs.get(table), NUMERIC_SEGMENT_ROWS, indexes);
        }
    }

    /**
     * Checks that Apache rebuilds the indexes, with segments of the same rows, into the bytes this build wrote: the
     * files staged in {@code stagedDir}, which the rebuild replaces in {@code tableDir}.
     */
    private static void assertRebuildWritesSameBytes(IInvokableInstance node, String table, Path stagedDir, Path tableDir, long segmentRows, List<String> indexes) throws Throwable
    {
        node.runOnInstance(() -> {
            try
            {
                Class.forName("org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder")
                     .getMethod("updateLastValidSegmentRowId", long.class)
                     .invoke(null, segmentRows);
            }
            catch (ReflectiveOperationException e)
            {
                throw new RuntimeException(e);
            }
        });
        Map<String, String> written = saiFiles(stagedDir);
        Map<String, Object> keys = fileKeys(tableDir, written.keySet()::contains);
        for (String index : indexes)
            node.nodetoolResult("rebuild_index", KEYSPACE, table, index).asserts().success();

        Map<String, String> rebuilt = saiFiles(tableDir);
        rebuilt.keySet().retainAll(written.keySet());
        assertThat(Maps.difference(written, rebuilt).entriesDiffering().keySet()).as("files rewritten differently").isEmpty();
        assertThat(rebuilt.keySet()).as("files rewritten").isEqualTo(written.keySet());
        Map<String, Object> rebuiltKeys = fileKeys(tableDir, written.keySet()::contains);
        assertThat(keys.keySet()).as("files replaced").allMatch(name -> !keys.get(name).equals(rebuiltKeys.get(name)));
    }

    /**
     * The checksums of the SAI files in {@code directory}, by name.
     */
    private static Map<String, String> saiFiles(Path directory) throws IOException, NoSuchAlgorithmException
    {
        Map<String, String> files = new TreeMap<>();
        try (Stream<Path> paths = Files.list(directory))
        {
            for (Path path : paths.filter(path -> path.getFileName().toString().contains("-SAI+")).collect(Collectors.toList()))
                files.put(path.getFileName().toString(), Hex.bytesToHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path))));
        }
        assertThat(files).isNotEmpty();
        return files;
    }

    @Test
    public void testVectorIndexes() throws Throwable
    {
        Random random = new Random(42);
        float[][] vectors = new float[VECTOR_ROWS][VECTOR_DIMENSION];
        // vectors of other lengths than 1, for the dot product
        float[][] products = new float[VECTOR_ROWS][VECTOR_DIMENSION];
        for (int pk = 0; pk < VECTOR_ROWS; pk++)
        {
            for (int d = 0; d < VECTOR_DIMENSION; d++)
            {
                vectors[pk][d] = random.nextFloat();
                products[pk][d] = 1 + 4 * random.nextFloat();
            }
            // rows sharing a vector
            if (pk % 10 == 5)
                vectors[pk] = vectors[pk - 1];
        }

        // segments of more and fewer vectors than Apache compresses (1024), and NVQ enabled for our formats
        try (WithProperties properties = new WithProperties().set(SAI_TEST_LAST_VALID_SEGMENTS, VECTOR_SEGMENT_ROWS)
                                                             .set(SAI_VECTOR_ENABLE_NVQ, true))
        {
            Path files = writeInApacheFormat(ApacheIndexFormatTest::createVectorSchema, node -> {
                for (int pk = 0; pk < VECTOR_ROWS; pk++)
                {
                    String s = pk % 2 == 0 ? "'a'" : "'b'";
                    // a live row without vectors
                    if (pk % 10 == 7)
                        node.executeInternal(withKeyspace("INSERT INTO %s.vectors (pk, s) VALUES (" + pk + ", " + s + ')'));
                    else
                        node.executeInternal(withKeyspace("INSERT INTO %s.vectors (pk, s, v, p) VALUES (" + pk + ", " + s + ", " +
                                                          vectorLiteral(vectors[pk]) + ", " + vectorLiteral(products[pk]) + ')'));
                    // a row without live data, which Apache's row ids leave out
                    if (pk % 10 == 3)
                        node.executeInternal(withKeyspace("DELETE FROM %s.vectors WHERE pk = " + pk));
                    // the files Apache loads are those of a compaction of two sstables
                    if (pk == VECTOR_ROWS / 2)
                        node.flush(KEYSPACE);
                }
                node.flush(KEYSPACE);
                node.forceCompact(KEYSPACE, VECTORS);
                // the files of the compacted sstables are gone before the table's files are copied
                node.runOnInstance(() -> LifecycleTransaction.waitForDeletions());
            }, VECTORS);

            loadOnApache(files, ApacheIndexFormatTest::createVectorSchema, (cluster, node, tableDirs) -> {
                for (int pk = 0; pk < VECTOR_ROWS; pk += 97)
                {
                    String query = vectorLiteral(vectors[pk]);
                    // each live row with a vector is its own nearest neighbour, or that of the row sharing its vector
                    Object[][] rows = node.executeInternal(withKeyspace("SELECT pk FROM %s.vectors ORDER BY v ANN OF " + query + " LIMIT 10"));
                    assertThat(pks(rows)).hasSize(10).doesNotHaveDuplicates().noneMatch(key -> (int) key % 10 == 3 || (int) key % 10 == 7);
                    if (pk % 10 != 3 && pk % 10 != 7)
                        assertThat(rows[0][0]).as("nearest to the vector of %d", pk).isIn(pk, pk % 10 == 4 ? pk + 1 : pk % 10 == 5 ? pk - 1 : pk);
                    // both rows sharing a vector
                    if (pk % 10 == 4 || pk % 10 == 5)
                        assertThat(pks(rows)).contains(pk, pk % 10 == 4 ? pk + 1 : pk - 1);

                    rows = node.executeInternal(withKeyspace("SELECT pk FROM %s.vectors WHERE s = 'a' ORDER BY v ANN OF " + query + " LIMIT 10"));
                    assertThat(pks(rows)).hasSize(10).doesNotHaveDuplicates().allMatch(key -> (int) key % 2 == 0);
                    if (pk % 2 == 0)
                        assertThat(rows[0][0]).as("nearest even row to the vector of %d", pk).isEqualTo(pk);

                    // the first row by dot product is among the ten that are
                    float[] product = products[pk];
                    rows = node.executeInternal(withKeyspace("SELECT pk FROM %s.vectors ORDER BY p ANN OF " + vectorLiteral(product) + " LIMIT 10"));
                    List<Object> best = IntStream.range(0, VECTOR_ROWS)
                                                 .filter(key -> key % 10 != 3 && key % 10 != 7)
                                                 .boxed()
                                                 .sorted((x, y) -> Float.compare(dot(products[y], product), dot(products[x], product)))
                                                 .limit(10)
                                                 .collect(Collectors.toList());
                    assertThat(pks(rows)).hasSize(10).doesNotHaveDuplicates().noneMatch(key -> (int) key % 10 == 3 || (int) key % 10 == 7);
                    assertThat(rows[0][0]).as("first by dot product with the vector of %d", pk).isIn(best);
                }
            }, VECTORS);
        }
    }

    private static void assertNumericRows(IInvokableInstance node, String table, String restriction, IntPredicate expected)
    {
        Object[][] rows = node.executeInternal(withKeyspace("SELECT pk FROM %s." + table + " WHERE " + restriction));
        assertThat(Arrays.stream(rows).mapToInt(row -> (int) row[0]).sorted().toArray()).as(restriction)
                                                                                         .isEqualTo(IntStream.range(0, NUMERIC_ROWS).filter(expected).toArray());
    }

    /**
     * The values of the columns of {@code table} of the row {@code pk}, as CQL literals in the order of
     * {@link #NUMERIC_COLUMNS}.
     */
    private static List<String> numericValues(String table, int pk)
    {
        int n = pk - MIDDLE;
        if (table.equals(DESCENDING))
            return List.of(Integer.toString(n));
        if (table.equals(NUMBERS))
            return List.of(Integer.toString(n),
                           Long.toString(n * 1_000_000_000L),
                           Integer.toString(n),
                           Integer.toString(pk % 256 - 128),
                           // a varint with a redundant sign byte, which Apache indexes as written
                           pk == VARINT_AS_WRITTEN ? "blobAsVarint(0xff80)" : BigInteger.valueOf(n).multiply(BigInteger.TEN.pow(25)).toString(),
                           BigDecimal.valueOf(n, 2).toPlainString(),
                           Double.toString(n + 0.5));
        return List.of(Double.toString(n * 1.25),
                       "'" + LocalDate.of(2020, 1, 1).plusDays(pk) + "'",
                       "'" + LocalTime.ofSecondOfDay(pk).format(DateTimeFormatter.ISO_LOCAL_TIME) + "'",
                       Long.toString(pk * 1000L),
                       "'10.0." + pk / 256 + '.' + pk % 256 + "'",
                       new UUID(pk, pk).toString());
    }

    /**
     * Writes sstables of the tables in Apache Cassandra's SAI format on a node of this build in
     * storage_compatibility_mode CASSANDRA_5, and copies the files of each table to its own directory, whose TOCs
     * list exactly their files.
     */
    private Path writeInApacheFormat(Consumer<Consumer<String>> schema, Consumer<IInvokableInstance> write, String... tables) throws Throwable
    {
        Path files = temporaryFolder.newFolder().toPath();
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_5_MODE_ENABLED, true);
             Cluster cluster = Cluster.build(1)
                                      .withConfig(config -> {
                                          config.set("storage_compatibility_mode", "CASSANDRA_5");
                                          oneDataDirectory(config);
                                      })
                                      .start())
        {
            schema.accept(cluster::schemaChange);
            // the copied directories hold only the files of live sstables
            cluster.get(1).nodetoolResult("disableautocompaction", KEYSPACE).asserts().success();
            write.accept(cluster.get(1));
            cluster.get(1).flush(KEYSPACE);

            for (String table : tables)
            {
                Path tableFiles = Files.createDirectory(files.resolve(table));
                List<String> dirs = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).getDirectories().getCFDirectories()
                                                                                .stream().map(dir -> dir.absolutePath()).collect(Collectors.toList()));
                for (String dir : dirs)
                {
                    try (Stream<Path> paths = Files.list(Paths.get(dir)))
                    {
                        for (Path path : paths.filter(Files::isRegularFile).collect(Collectors.toList()))
                            Files.copy(path, tableFiles.resolve(path.getFileName()));
                    }
                }
                assertTableOfContentsListsFiles(tableFiles);
            }
        }
        return files;
    }

    /**
     * Starts an Apache Cassandra node with the schema, stops it, copies the files of each table to its directory,
     * restarts it, checks that it built no index and that the checksums of the index files are valid, then runs the
     * checks.
     */
    private static void loadOnApache(Path files, Consumer<Consumer<String>> schema, ApacheChecks checks, String... tables) throws Throwable
    {
        // the Apache node loads the validation below from this build's test classes, after its own classes
        Versions.Version apache = apacheVersion();
        URL testClasses = ApacheIndexFormatTest.class.getProtectionDomain().getCodeSource().getLocation();
        Versions.Version apacheWithTests = new Versions.Version(apache.version, ArrayUtils.add(apache.classpath, testClasses));
        try (UpgradeableCluster cluster = startCluster(1, config -> oneDataDirectory(config.set("uuid_sstable_identifiers_enabled", true)), apacheWithTests))
        {
            schema.accept(cluster::schemaChange);
            IInvokableInstance node = (IInvokableInstance) cluster.get(1);
            Map<String, Path> tableDirs = new HashMap<>();
            for (String table : tables)
            {
                try (Stream<Path> dirs = Files.list(Paths.get(((String[]) node.config().get("data_file_directories"))[0], KEYSPACE)))
                {
                    tableDirs.put(table, dirs.filter(dir -> dir.getFileName().toString().startsWith(table + '-')).findFirst().orElseThrow());
                }
            }
            long mark = node.logs().mark();
            node.shutdown().get();
            // the sstables, with their index files, as a node of this build writes them
            for (String table : tables)
            {
                try (Stream<Path> paths = Files.list(files.resolve(table)))
                {
                    for (Path path : paths.collect(Collectors.toList()))
                        Files.copy(path, tableDirs.get(table).resolve(path.getFileName()));
                }
            }
            Map<String, Map<String, Object>> keys = new HashMap<>();
            for (String table : tables)
                keys.put(table, fileKeys(tableDirs.get(table), name -> name.contains("-SAI+")));
            node.startup();

            assertThat(node.logs().grep(mark, "initial index builds").getResult()).isEmpty();
            for (String table : tables)
                assertThat(fileKeys(tableDirs.get(table), name -> name.contains("-SAI+"))).as("index files of %s kept", table).isEqualTo(keys.get(table));
            for (String table : tables)
            {
                assertThat(node.callOnInstance(() -> {
                    ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                    return cfs.indexManager.validateSSTableAttachedIndexes(cfs.getLiveSSTables(), true, true);
                })).isTrue();
            }
            checks.check(cluster, node, tableDirs);
        }
    }

    // which vector indexes require, on Apache and in the mode
    private static void oneDataDirectory(IInstanceConfig config)
    {
        config.set("data_file_directories", new String[]{ ((String[]) config.get("data_file_directories"))[0] });
    }

    private interface ApacheChecks
    {
        void check(UpgradeableCluster cluster, IInvokableInstance node, Map<String, Path> tableDirs) throws Throwable;
    }

    private static void createLiteralSchema(Consumer<String> schemaChange)
    {
        schemaChange.accept(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"));
        schemaChange.accept(withKeyspace("CREATE TABLE %s.wide (pk int, ck bigint, st text static, s text, t text, e text, PRIMARY KEY (pk, ck))"));
        schemaChange.accept(withKeyspace("CREATE INDEX s_idx ON %s.wide (s) USING 'sai'"));
        schemaChange.accept(withKeyspace("CREATE INDEX st_idx ON %s.wide (st) USING 'sai'"));
        // an index without values
        schemaChange.accept(withKeyspace("CREATE INDEX e_idx ON %s.wide (e) USING 'sai'"));
        schemaChange.accept(withKeyspace("CREATE TABLE %s.skinny (pk int PRIMARY KEY, s text, fl frozen<list<decimal>>, m map<text, decimal>, a text, se set<text>)"));
        schemaChange.accept(withKeyspace("CREATE INDEX skinny_s ON %s.skinny (s) USING 'sai'"));
        schemaChange.accept(withKeyspace("CREATE INDEX skinny_fl ON %s.skinny (FULL(fl)) USING 'sai'"));
        schemaChange.accept(withKeyspace("CREATE INDEX skinny_m ON %s.skinny (ENTRIES(m)) USING 'sai'"));
        schemaChange.accept(withKeyspace("CREATE INDEX skinny_a ON %s.skinny (a) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}"));
        schemaChange.accept(withKeyspace("CREATE INDEX skinny_se ON %s.skinny (se) USING 'sai'"));
    }

    private static void createVectorSchema(Consumer<String> schemaChange)
    {
        schemaChange.accept(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"));
        schemaChange.accept(withKeyspace("CREATE TABLE %s.vectors (pk int PRIMARY KEY, s text, v vector<float, " + VECTOR_DIMENSION + ">, " +
                                         "p vector<float, " + VECTOR_DIMENSION + ">)"));
        schemaChange.accept(withKeyspace("CREATE INDEX vectors_s ON %s.vectors (s) USING 'sai'"));
        schemaChange.accept(withKeyspace("CREATE INDEX vectors_v ON %s.vectors (v) USING 'sai'"));
        schemaChange.accept(withKeyspace("CREATE INDEX vectors_p ON %s.vectors (p) USING 'sai' WITH OPTIONS = {'similarity_function': 'dot_product'}"));
    }

    private static float dot(float[] x, float[] y)
    {
        float product = 0;
        for (int d = 0; d < x.length; d++)
            product += x[d] * y[d];
        return product;
    }

    private static String vectorLiteral(float[] vector)
    {
        StringBuilder literal = new StringBuilder("[");
        for (int d = 0; d < vector.length; d++)
            literal.append(d == 0 ? "" : ", ").append(vector[d]);
        return literal.append(']').toString();
    }

    private static void createNumericSchema(Consumer<String> schemaChange)
    {
        schemaChange.accept(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"));
        schemaChange.accept(withKeyspace("CREATE TABLE %s.numbers (pk int PRIMARY KEY, i int, bi bigint, si smallint, ti tinyint, vi varint, d decimal, f float)"));
        schemaChange.accept(withKeyspace("CREATE TABLE %s.others (pk int PRIMARY KEY, db double, dt date, tm time, ts timestamp, ip inet, u uuid)"));
        schemaChange.accept(withKeyspace("CREATE TABLE %s.descending (pk int, ck bigint, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)"));
        for (String table : NUMERIC_TABLES)
        {
            for (String column : NUMERIC_COLUMNS.get(table))
                schemaChange.accept(withKeyspace("CREATE INDEX " + table + '_' + column + " ON %s." + table + " (" + column + ") USING 'sai'"));
        }
    }

    /**
     * Checks that the TOC of each sstable in {@code directory} lists exactly its files there, but for the PartitionToSize
     * file of a skinny table, which Apache Cassandra writes and doesn't list either.
     */
    private static void assertTableOfContentsListsFiles(Path directory) throws IOException
    {
        try (Stream<Path> tocs = Files.list(directory).filter(path -> path.toString().endsWith("-TOC.txt")))
        {
            for (Path toc : tocs.collect(Collectors.toList()))
            {
                String prefix = toc.getFileName().toString().replace("TOC.txt", "");
                List<String> listed = Files.readAllLines(toc);
                boolean skinny = !listed.contains("SAI+aa+ClusteringKeyBlocks.db");
                try (Stream<Path> paths = Files.list(directory))
                {
                    List<String> components = paths.map(path -> path.getFileName().toString())
                                                   .filter(name -> name.startsWith(prefix) && !(skinny && name.endsWith("+PartitionToSize.db")))
                                                   .map(name -> name.substring(prefix.length()))
                                                   .collect(Collectors.toList());
                    assertThat(listed).as(toc.toString()).containsExactlyInAnyOrderElementsOf(components);
                }
            }
        }
    }

    /**
     * The file keys (inodes) and modification times of the files in {@code directory} whose names {@code names}
     * accepts, which a rewrite changes.
     */
    private static Map<String, Object> fileKeys(Path directory, Predicate<String> names) throws IOException
    {
        Map<String, Object> keys = new HashMap<>();
        try (Stream<Path> paths = Files.list(directory))
        {
            for (Path path : paths.filter(path -> names.test(path.getFileName().toString())).collect(Collectors.toList()))
            {
                BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
                keys.put(path.getFileName().toString(), List.of(attributes.fileKey(), attributes.lastModifiedTime()));
            }
        }
        assertThat(keys).isNotEmpty();
        return keys;
    }

    // the per-sstable SAI files are named ...-SAI+aa+<component>, the per-index ones have the index name too
    private static boolean isPerSSTableFile(String name)
    {
        return name.contains("-SAI+aa+") && name.split("\\+").length == 3;
    }

    private static List<Object> pks(Object[][] rows)
    {
        return Arrays.stream(rows).map(row -> row[0]).collect(Collectors.toList());
    }

    private static List<Object> pks(int rows, IntPredicate filter)
    {
        return IntStream.range(0, rows).filter(filter).boxed().collect(Collectors.toList());
    }
}
