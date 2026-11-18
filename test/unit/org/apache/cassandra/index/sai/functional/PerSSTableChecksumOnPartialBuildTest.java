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

package org.apache.cassandra.index.sai.functional;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A partial index build (here, the initial build of an index created on an existing table) rewrites per-SSTable
 * components that fail checksum validation, and rebuilds every index of the SSTable on them (CASSANDRA-18490). With
 * {@code cassandra.sai.validate_per_sstable_checksum_on_partial_build} set to false, it reuses them as they are.
 */
public class PerSSTableChecksumOnPartialBuildTest extends SAITester
{
    private static final int ROWS = 1000;

    @After
    public void resetProperty()
    {
        SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD.reset();
    }

    @Test
    public void testCorruptPerSSTableComponentsRewrittenByDefault() throws Throwable
    {
        String v = setUpCorruptPerSSTableComponents();
        Map<String, Object> vFiles = perIndexFiles(v);

        String w = createIndex("CREATE INDEX ON %s(w) USING 'sai'");
        waitForIndexQueryable(w);

        // the per-SSTable components were rewritten, and the other index was rebuilt on them
        assertThat(perSSTableComponentsValid()).isTrue();
        assertThat(perIndexFiles(v)).isNotEqualTo(vFiles);
        assertQueries();
    }

    @Test
    public void testCorruptPerSSTableComponentsReusedWithoutValidation() throws Throwable
    {
        SAI_VALIDATE_PER_SSTABLE_CHECKSUM_ON_PARTIAL_BUILD.setBoolean(false);
        String v = setUpCorruptPerSSTableComponents();
        Map<String, Object> vFiles = perIndexFiles(v);

        String w = createIndex("CREATE INDEX ON %s(w) USING 'sai'");
        waitForIndexQueryable(w);

        // nothing validated the per-SSTable components: they are still corrupt, and the other index wasn't rebuilt
        assertThat(perSSTableComponentsValid()).isFalse();
        assertThat(perIndexFiles(v)).isEqualTo(vFiles);
    }

    @Test
    public void testValidPerSSTableComponentsReused() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        String v = createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        writeRows();
        Map<String, Object> vFiles = perIndexFiles(v);

        String w = createIndex("CREATE INDEX ON %s(w) USING 'sai'");
        waitForIndexQueryable(w);

        // valid components are reused, as without the validation
        assertThat(perSSTableComponentsValid()).isTrue();
        assertThat(perIndexFiles(v)).isEqualTo(vFiles);
        assertQueries();
    }

    /**
     * Creates a table with an index on v and one SSTable whose largest per-SSTable component has a flipped byte, which
     * only checksum validation detects; returns the name of the index.
     */
    private String setUpCorruptPerSSTableComponents() throws IOException
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, w int)");
        disableCompaction();
        String v = createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        writeRows();

        IndexComponent.ForRead component = descriptor().perSSTableComponents()
                                                       .all()
                                                       .stream()
                                                       .filter(c -> c.componentType() != IndexComponentType.GROUP_COMPLETION_MARKER)
                                                       .max(Comparator.comparingLong(c -> c.file().length()))
                                                       .orElseThrow();
        File file = component.file();
        assertThat(file.length()).isGreaterThan(100);
        try (RandomAccessFile raf = new RandomAccessFile(file.toJavaIOFile(), "rw"))
        {
            long position = file.length() / 2;
            raf.seek(position);
            int b = raf.read();
            raf.seek(position);
            raf.write(b ^ 0xFF);
        }
        assertThat(perSSTableComponentsValid()).isFalse();
        return v;
    }

    private void writeRows()
    {
        for (int k = 0; k < ROWS; k++)
            execute("INSERT INTO %s (k, v, w) VALUES (?, ?, ?)", k, k % 10, k % 7);
        flush();
        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1);
    }

    private void assertQueries()
    {
        assertThat(execute("SELECT k FROM %s WHERE v = 3")).hasSize(ROWS / 10);
        assertThat(execute("SELECT k FROM %s WHERE w = 3")).hasSize(143);
        assertThat(execute("SELECT k FROM %s WHERE v = 3 AND w = 3")).hasSize(15);
    }

    private IndexDescriptor descriptor()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        return loadDescriptor(sstable, cfs);
    }

    private boolean perSSTableComponentsValid()
    {
        return descriptor().perSSTableComponents().isValid(true);
    }

    /**
     * @return the files of the per-index components of the provided index, with their file keys (inodes), which change
     * when they are written again
     */
    private Map<String, Object> perIndexFiles(String index) throws IOException
    {
        Map<String, Object> files = new HashMap<>();
        for (IndexComponent.ForRead component : descriptor().perIndexComponents(getIndexContext(index)).all())
            files.put(component.file().name(), Files.readAttributes(component.file().toPath(), BasicFileAttributes.class).fileKey());
        return files;
    }
}
