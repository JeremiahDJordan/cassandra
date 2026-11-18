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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.disk.format.IndexDefinitionDigest;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.util.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The completion marker of the per-index components written by Apache Cassandra doesn't hold the definition of their
 * index: one of the two markers Apache Cassandra writes makes them complete, anything else doesn't.
 */
public class ApacheCompletionMarkerTest extends ApacheIndexTester
{
    private static final String MARKER_SUFFIX = "+ColumnComplete.db";

    @Test
    public void testMarkersOfTestData() throws Exception
    {
        List<Path> markers;
        try (Stream<Path> files = Files.walk(Paths.get("test/data/legacy-sai/apache-aa")))
        {
            markers = files.filter(p -> p.getFileName().toString().contains(APACHE_PREFIX))
                           .filter(p -> p.getFileName().toString().endsWith(MARKER_SUFFIX))
                           .collect(Collectors.toList());
        }
        assertThat(markers).isNotEmpty();
        for (Path marker : markers)
            assertThat(IndexDefinitionDigest.isApacheMarker(Files.readAllBytes(marker))).as(marker.toString()).isTrue();
    }

    @Test
    public void testMarkerUsed() throws Throwable
    {
        createSkinnyTableWithIndexes();
        loadApacheSSTables("skinny");
        assertReadFromApacheComponents("skinny_s", "skinny_n");
        assertThat(execute("SELECT pk FROM %s WHERE s = 's3'")).hasSize(10);
    }

    @Test
    public void testEmptyMarkerNotUsed() throws Throwable
    {
        testDamagedMarkerNotUsed(content -> new byte[0]);
    }

    @Test
    public void testTruncatedMarkerNotUsed() throws Throwable
    {
        testDamagedMarkerNotUsed(content -> Arrays.copyOf(content, content.length - 1));
    }

    @Test
    public void testMarkerWithTrailingBytesNotUsed() throws Throwable
    {
        testDamagedMarkerNotUsed(content -> Arrays.copyOf(content, content.length + 1));
    }

    @Test
    public void testMarkerWithWrongChecksumNotUsed() throws Throwable
    {
        testDamagedMarkerNotUsed(content -> {
            byte[] damaged = content.clone();
            damaged[damaged.length - 1] ^= 1;
            return damaged;
        });
    }

    /**
     * The marker of skinny_s on the first of the two SSTables is damaged before they are loaded: the index is built
     * on that SSTable, and the Apache components of the other one are used.
     */
    private void testDamagedMarkerNotUsed(UnaryOperator<byte[]> damage) throws Throwable
    {
        createSkinnyTableWithIndexes();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        FileUtils.copySSTablesAndIndexes(dir.toJavaIOFile(), "apache-aa/skinny");
        Path marker = dir.toPath().resolve("nb-1-big-" + APACHE_PREFIX + "skinny_s" + MARKER_SUFFIX);
        Files.write(marker, damage.apply(Files.readAllBytes(marker)));
        cfs.loadNewSSTables();
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        waitForTableIndexesQueryable();

        // the import gives the SSTables new generations: one of them keeps the Apache components of skinny_s
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        List<Version> versions = cfs.getLiveSSTables()
                                    .stream()
                                    .map(sstable -> group.descriptorFor(sstable).perIndexComponents(getIndexContext("skinny_s")).version())
                                    .collect(Collectors.toList());
        assertThat(versions).containsExactlyInAnyOrder(Version.APACHE_AA, Version.current(KEYSPACE));
        assertThat(execute("SELECT pk FROM %s WHERE s = 's3'")).hasSize(10);
    }

    private void createSkinnyTableWithIndexes()
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX skinny_n ON %s(n) USING 'sai'");
        waitForTableIndexesQueryable();
    }
}
