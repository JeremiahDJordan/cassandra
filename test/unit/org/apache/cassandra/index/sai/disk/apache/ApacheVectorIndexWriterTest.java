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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Vector indexes written in storage_compatibility_mode CASSANDRA_5, in Apache Cassandra 5.0's format, read back.
 */
public class ApacheVectorIndexWriterTest extends SAITester
{
    private StorageCompatibilityMode mode;

    @Before
    public void enterCassandra5Mode()
    {
        mode = DatabaseDescriptor.getStorageCompatibilityMode();
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_5);
    }

    @After
    public void leaveCassandra5Mode()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(mode);
    }

    @Test
    public void testSegments() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v vector<float, 16>)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        Random random = new Random(42);
        float[][] vectors = new float[2500][16];
        for (int pk = 0; pk < vectors.length; pk++)
        {
            for (int d = 0; d < 16; d++)
                vectors[pk][d] = random.nextFloat();
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, vector(vectors[pk]));
            if (pk % 10 == 3)
                execute("DELETE FROM %s WHERE pk = ?", pk);
        }
        // segments of more and fewer vectors than Apache compresses (1024)
        long lastValidSegmentRowId = SegmentBuilder.updateLastValidSegmentRowId(1500);
        try
        {
            flush();
        }
        finally
        {
            SegmentBuilder.updateLastValidSegmentRowId(lastValidSegmentRowId);
        }

        assertWrittenInApacheFormat(index);
        // Apache's product quantization: half as many subspaces as dimensions, of 256 clusters
        StorageAttachedIndex sai = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(index);
        CassandraOnHeapGraph.PqInfo pq = CassandraOnHeapGraph.getPqIfPresent(sai.getIndexContext(), compression -> true);
        assertThat(pq).isNotNull();
        assertThat(pq.pq.getSubspaceCount()).isEqualTo(8);
        assertThat(pq.pq.getClusterCount()).isEqualTo(256);
        // a vector per row, in Apache's layout of the postings, which starts with the count of deleted ordinals
        SSTableReader sstable = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        File postings = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore()).descriptorFor(sstable)
                                                 .perIndexComponents(sai.getIndexContext()).get(IndexComponentType.POSTING_LISTS).file();
        try (FileInputStreamPlus input = postings.newInputStream())
        {
            input.skipBytes(SAICodecUtils.headerSize());
            assertThat(input.readInt()).isZero();
        }
        for (int pk = 0; pk < vectors.length; pk += 97)
        {
            if (pk % 10 == 3)
                continue;
            UntypedResultSet rows = execute("SELECT pk FROM %s ORDER BY v ANN OF ? LIMIT 1", vector(vectors[pk]));
            assertRows(rows, row(pk));
        }
    }

    @Test
    public void testIndexOnSSTablesWithOtherIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text, v vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, s, v) VALUES (0, 'a', [1.0, 0.0])");
        execute("INSERT INTO %s (pk, s, v) VALUES (1, 'b', [0.0, 1.0])");
        flush();
        Map<String, List<Object>> files = fileKeys();

        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        assertWrittenInApacheFormat(index);
        // the other files of the sstable, its per-sstable components and the other index's included, are kept
        assertThat(fileKeys()).containsAllEntriesOf(files);
        assertRows(execute("SELECT pk FROM %s ORDER BY v ANN OF [1.0, 0.1] LIMIT 1"), row(0));
    }

    @Test
    public void testIndexRefusedOnSSTablesWithoutJVector() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text, v vector<float, 2>)");
        disableCompaction();
        // an sstable whose indexes are in a version without jvector, written outside the mode, then one in Apache's
        // format, which sorts before it
        Version current = Version.current(KEYSPACE);
        DatabaseDescriptor.setStorageCompatibilityMode(mode);
        SAIUtil.setCurrentVersion(Version.BA);
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, s, v) VALUES (0, 'a', [1.0, 0.0])");
        flush();
        SAIUtil.setCurrentVersion(current);
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_5);
        execute("INSERT INTO %s (pk, s, v) VALUES (1, 'b', [0.0, 1.0])");
        flush();

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'"))
        .hasMessageContaining(StorageAttachedIndex.vectorUnsupportedByExistingVersionError(Version.BA));
    }

    private Map<String, List<Object>> fileKeys() throws IOException
    {
        Map<String, List<Object>> keys = new HashMap<>();
        SSTableReader sstable = Iterables.getOnlyElement(getCurrentColumnFamilyStore().getLiveSSTables());
        for (Component component : sstable.components())
        {
            // the TOC lists the new index's components
            if (component.equals(SSTableFormat.Components.TOC))
                continue;
            BasicFileAttributes attributes = Files.readAttributes(sstable.descriptor.fileFor(component).toPath(), BasicFileAttributes.class);
            keys.put(component.name, List.of(attributes.fileKey(), attributes.lastModifiedTime()));
        }
        return keys;
    }

    private void assertWrittenInApacheFormat(String index)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertThat(cfs.getLiveSSTables()).isNotEmpty();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = group.descriptorFor(sstable);
            IndexComponents.ForRead components = descriptor.perIndexComponents(((StorageAttachedIndex) cfs.indexManager.getIndexByName(index)).getIndexContext());
            assertThat(components.version()).isEqualTo(Version.APACHE_AA);
            assertThat(components.isComplete()).isTrue();
            assertThat(components.isValid(true)).isTrue();
        }
    }

    @Test
    public void testOneDimension() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v vector<float, 1>)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        // more vectors than Apache compresses, which it can't for one dimension
        for (int pk = 0; pk < 1100; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, vector(new float[]{ pk }));
        flush();

        assertWrittenInApacheFormat(index);
        assertRows(execute("SELECT pk FROM %s ORDER BY v ANN OF [500.2] LIMIT 1"), row(500));
    }
}
