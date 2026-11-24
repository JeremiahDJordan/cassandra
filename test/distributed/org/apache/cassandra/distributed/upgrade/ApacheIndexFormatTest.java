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
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.FieldValue;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.Component;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_TEST_LAST_VALID_SEGMENTS;
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
    private static final String[] TABLES = { WIDE, SKINNY };
    // enough for posting lists of several blocks, and several segments, in each sstable
    private static final int PARTITIONS = 500;
    private static final int SKINNY_PARTITIONS = 1500;
    // rows per segment, so that every index has several segments in each sstable
    private static final int SEGMENT_ROWS = 300;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testLiteralIndexes() throws Throwable
    {
        Path files = temporaryFolder.newFolder().toPath();
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_5_MODE_ENABLED, true)
                                                             .set(SAI_TEST_LAST_VALID_SEGMENTS, SEGMENT_ROWS);
             Cluster cluster = Cluster.build(1)
                                      .withInstanceInitializer(WriteApacheFormat::install)
                                      .withConfig(config -> config.set("storage_compatibility_mode", "CASSANDRA_5"))
                                      .start())
        {
            createSchema(cluster::schemaChange);
            for (int pk = 0; pk < PARTITIONS; pk++)
            {
                cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.wide (pk, st) VALUES (?, ?)"), pk, "st" + (pk % 3));
                for (long ck = 0; ck < 5; ck++)
                    cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.wide (pk, ck, s, t) VALUES (?, ?, ?, ?)"), pk, ck, "s" + ck, "t" + ck);
                // a row without live data, which Apache's row ids leave out
                if (pk % 2 == 0)
                    cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.wide WHERE pk = ? AND ck = 1"), pk);
            }
            for (int pk = 0; pk < SKINNY_PARTITIONS; pk++)
            {
                // decimals of a scale other than that of their canonical form (2.5 against 2.50), which Apache indexes
                // as written
                String decimal = pk % 3 + ".5";
                cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.skinny (pk, s, fl, m, a, se) VALUES (?, ?, [" + decimal + "], {'k': " + decimal + "}, ?, {'x', 'y'})"),
                                               pk, "s" + (pk % 2), "Abc" + (pk % 2));
            }
            cluster.get(1).flush(KEYSPACE);

            for (String table : TABLES)
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
                assertTableOfContentsListsFiles(tableFiles, table.equals(SKINNY));
            }
        }
        // the column completion markers say whether the index is empty, after the header ("aa")
        try (Stream<Path> markers = Files.list(files.resolve(WIDE)).filter(path -> path.toString().endsWith("+ColumnComplete.db")))
        {
            for (Path marker : markers.collect(Collectors.toList()))
                assertThat(Files.readAllBytes(marker)[7]).as(marker.toString()).isEqualTo((byte) (marker.toString().contains("+e_idx+") ? 1 : 0));
        }

        // the Apache node loads the validation below from this build's test classes, after its own classes
        Versions.Version apache = apacheVersion();
        URL testClasses = ApacheIndexFormatTest.class.getProtectionDomain().getCodeSource().getLocation();
        Versions.Version apacheWithTests = new Versions.Version(apache.version, ArrayUtils.add(apache.classpath, testClasses));
        try (UpgradeableCluster cluster = startCluster(1, config -> config.set("uuid_sstable_identifiers_enabled", true), apacheWithTests))
        {
            createSchema(cluster::schemaChange);
            IInvokableInstance node = (IInvokableInstance) cluster.get(1);
            Map<String, Path> tableDirs = new HashMap<>();
            for (String table : TABLES)
            {
                try (Stream<Path> dirs = Files.list(Paths.get(((String[]) node.config().get("data_file_directories"))[0], KEYSPACE)))
                {
                    tableDirs.put(table, dirs.filter(dir -> dir.getFileName().toString().startsWith(table + '-')).findFirst().orElseThrow());
                }
            }
            long mark = node.logs().mark();
            node.shutdown().get();
            // the sstables, with their index files, as a node of this build writes them
            for (String table : TABLES)
            {
                try (Stream<Path> paths = Files.list(files.resolve(table)))
                {
                    for (Path path : paths.collect(Collectors.toList()))
                        Files.copy(path, tableDirs.get(table).resolve(path.getFileName()));
                }
            }
            node.startup();

            assertThat(node.logs().grep(mark, "initial index builds").getResult()).isEmpty();
            for (String table : TABLES)
            {
                assertThat(node.callOnInstance(() -> {
                    ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                    return cfs.indexManager.validateSSTableAttachedIndexes(cfs.getLiveSSTables(), true, true);
                })).isTrue();
            }

            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE s = 's1'")))).containsExactlyInAnyOrderElementsOf(pks(pk -> pk % 2 == 1));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE s = 's3'")))).containsExactlyInAnyOrderElementsOf(pks(pk -> true));
            assertRows(node.executeInternal(withKeyspace("SELECT pk, ck FROM %s.wide WHERE s = 's3' AND pk = 7")), row(7, 3L));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE st = 'st1' AND ck = 2 ALLOW FILTERING")))).containsExactlyInAnyOrderElementsOf(pks(pk -> pk % 3 == 1));
            assertThat(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE e = 'e'"))).isEmpty();
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.skinny WHERE s = 's1'")))).containsExactlyInAnyOrderElementsOf(skinnyPks(pk -> pk % 2 == 1));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.skinny WHERE fl = [2.5]")))).containsExactlyInAnyOrderElementsOf(skinnyPks(pk -> pk % 3 == 2));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.skinny WHERE m['k'] = 2.5")))).containsExactlyInAnyOrderElementsOf(skinnyPks(pk -> pk % 3 == 2));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.skinny WHERE a = 'abc1'")))).containsExactlyInAnyOrderElementsOf(skinnyPks(pk -> pk % 2 == 1));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.skinny WHERE se CONTAINS 'y'")))).containsExactlyInAnyOrderElementsOf(skinnyPks(pk -> true));
            // the segments count their terms, two per row for the set
            Object[][] cells = node.executeInternal("SELECT cell_count FROM system_views.sai_sstable_indexes WHERE keyspace_name = ? AND index_name = 'skinny_se'", KEYSPACE);
            assertThat(Arrays.stream(cells).mapToLong(row -> (long) row[0]).sum()).isEqualTo(2L * SKINNY_PARTITIONS);

            // Apache builds a new index on the per-sstable components as written, numbering the rows as it does
            Map<Path, Object> perSSTableFiles = fileKeys(tableDirs.get(WIDE));
            cluster.schemaChange(withKeyspace("CREATE INDEX t_idx ON %s.wide (t) USING 'sai'"));
            await().atMost(1, TimeUnit.MINUTES)
                   .until(() -> Arrays.stream(node.executeInternal("SELECT is_queryable FROM system_views.sai_column_indexes WHERE keyspace_name = ? AND index_name = 't_idx'", KEYSPACE))
                                      .anyMatch(row -> Boolean.TRUE.equals(row[0])));
            assertThat(fileKeys(tableDirs.get(WIDE))).isEqualTo(perSSTableFiles);
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE t = 't1'")))).containsExactlyInAnyOrderElementsOf(pks(pk -> pk % 2 == 1));
            assertThat(pks(node.executeInternal(withKeyspace("SELECT pk FROM %s.wide WHERE t = 't4'")))).containsExactlyInAnyOrderElementsOf(pks(pk -> true));
        }
    }

    private static void createSchema(Consumer<String> schemaChange)
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

    /**
     * Checks that the TOC of each sstable in {@code directory} lists exactly its files there, but for the PartitionToSize
     * file of a skinny table, which Apache Cassandra writes and doesn't list either.
     */
    private static void assertTableOfContentsListsFiles(Path directory, boolean skinny) throws IOException
    {
        try (Stream<Path> tocs = Files.list(directory).filter(path -> path.toString().endsWith("-TOC.txt")))
        {
            for (Path toc : tocs.collect(Collectors.toList()))
            {
                String prefix = toc.getFileName().toString().replace("TOC.txt", "");
                try (Stream<Path> paths = Files.list(directory))
                {
                    List<String> components = paths.map(path -> path.getFileName().toString())
                                                   .filter(name -> name.startsWith(prefix) && !(skinny && name.endsWith("+PartitionToSize.db")))
                                                   .map(name -> name.substring(prefix.length()))
                                                   .collect(Collectors.toList());
                    assertThat(Files.readAllLines(toc)).as(toc.toString()).containsExactlyInAnyOrderElementsOf(components);
                }
            }
        }
    }

    /**
     * The file keys (inodes) of the per-sstable SAI files in {@code directory}, which a rewrite changes.
     */
    private static Map<Path, Object> fileKeys(Path directory) throws IOException
    {
        Map<Path, Object> keys = new HashMap<>();
        try (Stream<Path> paths = Files.list(directory))
        {
            for (Path path : paths.collect(Collectors.toList()))
            {
                String name = path.getFileName().toString();
                // the per-sstable files are named ...-SAI+aa+<component>, the per-index ones have the index name too
                if (name.contains("-SAI+aa+") && name.split("\\+").length == 3)
                    keys.put(path, Files.readAttributes(path, BasicFileAttributes.class).fileKey());
            }
        }
        assertThat(keys).isNotEmpty();
        return keys;
    }

    private static List<Object> pks(Object[][] rows)
    {
        return Arrays.stream(rows).map(row -> row[0]).collect(Collectors.toList());
    }

    private static List<Object> pks(IntPredicate filter)
    {
        return IntStream.range(0, PARTITIONS).filter(filter).boxed().collect(Collectors.toList());
    }

    private static List<Object> skinnyPks(IntPredicate filter)
    {
        return IntStream.range(0, SKINNY_PARTITIONS).filter(filter).boxed().collect(Collectors.toList());
    }

    /**
     * Writes and lists the SAI components of new sstables in Apache's format.
     */
    public static class WriteApacheFormat
    {
        static void install(ClassLoader classLoader, Integer node)
        {
            new ByteBuddy().rebase(IndexDescriptor.class)
                           .method(named("versionForNewComponents"))
                           .intercept(MethodDelegation.to(WriteApacheFormat.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            new ByteBuddy().rebase(StorageAttachedIndexGroup.class)
                           .method(named("componentsForNewSSTable"))
                           .intercept(MethodDelegation.to(ListApacheFormat.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static Version versionForNewComponents()
        {
            return Version.APACHE_AA;
        }
    }

    public static class ListApacheFormat
    {
        @SuppressWarnings("unused")
        public static Set<Component> componentsForNewSSTable(@FieldValue("indices") Set<StorageAttachedIndex> indices,
                                                             @FieldValue("baseCfs") ColumnFamilyStore baseCfs)
        {
            return IndexDescriptor.componentsForNewlyFlushedSSTable(indices, Version.APACHE_AA, baseCfs.metadata().hasClustering());
        }
    }
}
