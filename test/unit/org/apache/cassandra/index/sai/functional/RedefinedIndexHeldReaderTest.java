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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A query holds the reader of an index (next to another index, which keeps the per-SSTable components) while the node
 * gets the index dropped and created again with other options in one schema change: the new index is built from the
 * data, not from the files of the old one.
 */
public class RedefinedIndexHeldReaderTest extends SAITester
{
    @Test
    public void testReaderHeldAcrossRedefinition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, a int, t text)");
        disableCompaction();
        execute("INSERT INTO %s (pk, a, t) VALUES (0, 1, 'Quick')");
        flush();
        createIndex("CREATE CUSTOM INDEX a_idx ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX t_idx ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': 'false'}");
        waitForTableIndexesQueryable();

        SSTableIndex held = getIndexContext("t_idx").getView().getIndexes().iterator().next();
        assertThat(held.reference()).isTrue();
        try
        {
            redefine("t_idx", "case_sensitive", "true");
            waitForTableIndexesQueryable();
            assertThat(execute("SELECT pk FROM %s WHERE t = 'Quick'")).hasSize(1);
            assertThat(execute("SELECT pk FROM %s WHERE t = 'quick'")).isEmpty();
        }
        finally
        {
            held.release();
        }
        assertThat(execute("SELECT pk FROM %s WHERE t = 'Quick'")).hasSize(1);
        assertThat(execute("SELECT pk FROM %s WHERE t = 'quick'")).isEmpty();
        assertIndexFilesOnDisk("t_idx");
    }

    /**
     * An SSTable written by DSE 6.8 (format bb), whose SAI files are found by listing the directory.
     */
    @Test
    public void testDSE68ReaderHeldAcrossRedefinition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, int_value int, text_value text)");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        FileUtils.copySSTablesAndIndexes(cfs.getDirectories().getDirectoryForNewSSTables().toJavaIOFile(), "aa");
        cfs.loadNewSSTables();
        assertThat(cfs.getLiveSSTables().iterator().next().descriptor.version.version).isEqualTo("bb");
        createIndex("CREATE CUSTOM INDEX int_index ON %s(int_value) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        SSTableIndex held = getIndexContext("text_index").getView().getIndexes().iterator().next();
        assertThat(held.reference()).isTrue();
        try
        {
            redefine("text_index", "index_analyzer", "{\"tokenizer\":{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"1\", \"maxGramSize\":\"1\"}}}");
            waitForTableIndexesQueryable();
            assertThat(execute("SELECT pk FROM %s WHERE text_value : '1'")).hasSize(19);
        }
        finally
        {
            held.release();
        }
        assertThat(execute("SELECT pk FROM %s WHERE text_value : '1'")).hasSize(19);
        assertIndexFilesOnDisk("text_index");
    }

    /**
     * The release of the old reader mustn't delete the files of the new index, which have the same names.
     */
    private void assertIndexFilesOnDisk(String index)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            Collection<IndexComponent.ForRead> components = loadDescriptor(sstable, cfs).perIndexComponents(getIndexContext(index)).all();
            assertThat(components).anyMatch(IndexComponent::isCompletionMarker);
            assertThat(components).allMatch(c -> c.file().exists());
        }
    }

    private void redefine(String name, String option, String value)
    {
        TableMetadata table = currentTableMetadata();
        IndexMetadata old = table.indexes.get(name).get();
        Map<String, String> options = new HashMap<>(old.options);
        options.put(option, value);
        IndexMetadata redefined = IndexMetadata.fromSchemaMetadata(name, old.kind, options);
        SchemaTestUtil.announceTableUpdate(table.unbuild().indexes(table.indexes.replace(redefined)).build());
    }
}
