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
package org.apache.cassandra.index.sai.disk.format;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SSTables written by Apache Cassandra 5.0 carry SAI files ("SAI+aa+...") of the format {@link Version#APACHE_AA}, not
 * {@link Version#AA}. When they aren't read ({@link CassandraRelevantProperties#SAI_READ_APACHE_FORMAT} off), upgrades
 * and imports rebuild the indexes in our format and remove the Apache files.
 * <p>
 * Test data in test/data/legacy-sai/apache-aa was written by Apache cassandra-5.0 at fb7f1b7aee.
 */
public class ApacheSAIFilesTest extends SAITester
{
    private WithProperties properties;

    @Before
    public void dontReadApacheFormat()
    {
        properties = new WithProperties().set(CassandraRelevantProperties.SAI_READ_APACHE_FORMAT, false);
    }

    @After
    public void resetReadApacheFormat()
    {
        properties.close();
    }

    @Test
    public void testNotDiscovered() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, s text, n bigint, PRIMARY KEY (pk, ck))");
        disableCompaction();
        loadApacheSSTables("wide");
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            assertThat(IndexComponentDiscovery.instance().discoverComponents(sstable).isEmpty()).isTrue();
    }

    @Test
    public void testSkinnyIndexedAfterLoad() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text, n int, v vector<float, 3>)");
        disableCompaction();
        loadApacheSSTables("skinny");

        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX skinny_n ON %s(n) USING 'sai'");
        createIndex("CREATE INDEX skinny_v ON %s(v) USING 'sai'");
        waitForTableIndexesQueryable();

        assertSkinnyQueries();
        assertRestartKeepsIndexes(List.of("skinny_s", "skinny_n", "skinny_v"));
        assertSkinnyQueries();
    }

    @Test
    public void testWideIndexedAfterLoad() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, s text, n bigint, PRIMARY KEY (pk, ck))");
        disableCompaction();
        loadApacheSSTables("wide");

        createIndex("CREATE INDEX wide_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX wide_n ON %s(n) USING 'sai'");
        createIndex("CREATE INDEX wide_st ON %s(st) USING 'sai'");
        waitForTableIndexesQueryable();

        assertWideQueries();
        assertRestartKeepsIndexes(List.of("wide_s", "wide_n", "wide_st"));
        assertWideQueries();
    }

    @Test
    public void testSkinnyImportedWithExistingIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text, n int, v vector<float, 3>)");
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX skinny_n ON %s(n) USING 'sai'");
        createIndex("CREATE INDEX skinny_v ON %s(v) USING 'sai'");
        waitForTableIndexesQueryable();

        loadApacheSSTables("skinny");
        waitForTableIndexesQueryable();

        assertSkinnyQueries();
        assertRestartKeepsIndexes(List.of("skinny_s", "skinny_n", "skinny_v"));
        assertSkinnyQueries();
    }

    /**
     * The Apache files are streamable components: an index build that isn't holding the SSTable's stream/rebuild
     * reservation (here, the initial build of CREATE INDEX) must not delete them from under an entire-SSTable stream.
     */
    @Test
    public void testApacheFilesKeptWhileStreaming() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text, n int, v vector<float, 3>)");
        disableCompaction();
        loadApacheSSTables("skinny");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        for (SSTableReader sstable : sstables)
            assertThat(sstable.streamRebuildState().tryBeginStreaming()).isTrue();

        try
        {
            createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
            waitForTableIndexesQueryable();
            assertThat(execute("SELECT pk FROM %s WHERE s = 's3'")).hasSize(10);

            for (SSTableReader sstable : sstables)
            {
                assertThat(tocEntries(sstable)).anyMatch(c -> c.startsWith(Version.APACHE_COMPONENT_PREFIX));
                for (Component component : sstable.components())
                    assertThat(sstable.descriptor.fileFor(component).exists()).as("%s", component).isTrue();
            }
        }
        finally
        {
            for (SSTableReader sstable : sstables)
                sstable.streamRebuildState().endStreaming();
        }

        // The next build of the SSTables removes them.
        createIndex("CREATE INDEX skinny_n ON %s(n) USING 'sai'");
        waitForTableIndexesQueryable();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            assertThat(tocEntries(sstable)).noneMatch(c -> c.startsWith(Version.APACHE_COMPONENT_PREFIX));
            assertThat(onDiskFiles(sstable)).noneMatch(Version::isApacheComponentFileName);
        }
    }

    private void loadApacheSSTables(String data) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        FileUtils.copySSTablesAndIndexes(dir.toJavaIOFile(), "apache-aa/" + data);
        cfs.loadNewSSTables();
        assertThat(cfs.getLiveSSTables()).hasSize(2);
    }

    private void assertSkinnyQueries()
    {
        assertThat(execute("SELECT pk FROM %s WHERE s = 's3'")).hasSize(10);
        assertThat(execute("SELECT pk FROM %s WHERE n > 89")).hasSize(10);
        assertRows(execute("SELECT pk FROM %s ORDER BY v ANN OF [42.0, 0.0, 1.0] LIMIT 1"), row(42));
    }

    private void assertWideQueries()
    {
        assertThat(execute("SELECT pk, ck FROM %s WHERE s = 'w1'")).hasSize(100);
        assertRows(execute("SELECT pk, ck FROM %s WHERE n = 423"), row(42, 3));
        assertThat(execute("SELECT pk FROM %s WHERE st = 2 PER PARTITION LIMIT 1")).hasSize(20);
    }

    private void assertRestartKeepsIndexes(List<String> indexes) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            assertThat(tocEntries(sstable)).noneMatch(c -> c.startsWith(Version.APACHE_COMPONENT_PREFIX));
            assertThat(onDiskFiles(sstable)).noneMatch(Version::isApacheComponentFileName);
        }

        // A rebuild rewrites the component files, possibly under the same names (without immutable components, a
        // rebuild produces the same build id), so we detect it through modification times: backdate every SAI file,
        // and check none was touched by the restart.
        Map<Path, FileTime> backdated = backdateSAIFiles(cfs);
        assertThat(backdated).isNotEmpty();

        simulateNodeRestart();

        // With immutable components, a rebuild would write new files instead: the set of files must not change either.
        assertThat(saiFiles(cfs)).isEqualTo(backdated.keySet());
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            SSTableIndexComponentsState state = IndexComponentDiscovery.instance().discoverComponents(sstable);
            assertThat(state.perSSTableBuild()).isNotNull();
            for (String index : indexes)
                assertThat(state.perIndexBuild(index)).as("index %s on %s", index, sstable).isNotNull();
        }
        for (Map.Entry<Path, FileTime> e : backdated.entrySet())
            assertThat(Files.getLastModifiedTime(e.getKey())).as("rewritten by the restart: %s", e.getKey()).isEqualTo(e.getValue());
    }

    private static Map<Path, FileTime> backdateSAIFiles(ColumnFamilyStore cfs) throws Exception
    {
        FileTime old = FileTime.fromMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
        Map<Path, FileTime> files = new HashMap<>();
        for (Path path : saiFiles(cfs))
        {
            Files.setLastModifiedTime(path, old);
            files.put(path, Files.getLastModifiedTime(path));
        }
        return files;
    }

    private static Set<Path> saiFiles(ColumnFamilyStore cfs)
    {
        Set<Path> files = new HashSet<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            for (String name : onDiskFiles(sstable))
                if (Version.tryParseFileName(name).isPresent())
                    files.add(sstable.descriptor.directory.toPath().resolve(name));
        return files;
    }

    private static Set<String> tocEntries(SSTableReader sstable) throws Exception
    {
        File toc = sstable.descriptor.fileFor(SSTableFormat.Components.TOC);
        return Set.copyOf(Files.readAllLines(toc.toPath()));
    }

    private static Set<String> onDiskFiles(SSTableReader sstable)
    {
        String prefix = sstable.descriptor.baseFile().name();
        return Arrays.stream(sstable.descriptor.directory.tryList())
                     .map(File::name)
                     .filter(n -> n.startsWith(prefix))
                     .collect(Collectors.toSet());
    }
}
