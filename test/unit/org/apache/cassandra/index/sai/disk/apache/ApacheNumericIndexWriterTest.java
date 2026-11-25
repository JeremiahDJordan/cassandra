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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MINIMUM_POSTINGS_LEAVES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Numeric indexes written in storage_compatibility_mode CASSANDRA_5, in Apache Cassandra 5.0's format, read back.
 */
public class ApacheNumericIndexWriterTest extends SAITester
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
    public void testSegmentsOfSeveralLeaves() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        for (int pk = 0; pk < 12000; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, pk - 6000);
        // segments of 10 and 2 leaf blocks, with posting lists of internal nodes
        long lastValidSegmentRowId = SegmentBuilder.updateLastValidSegmentRowId(10000);
        try (WithProperties properties = new WithProperties().set(SAI_MINIMUM_POSTINGS_LEAVES, 2))
        {
            flush();
        }
        finally
        {
            SegmentBuilder.updateLastValidSegmentRowId(lastValidSegmentRowId);
        }

        assertWrittenInApacheFormat(index);
        assertRows(execute("SELECT pk FROM %s WHERE v = 1234"), row(7234));
        assertRowCount(execute("SELECT pk FROM %s WHERE v >= -5000 AND v < 4000"), 9000);
    }

    @Test
    public void testDescendingClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck bigint, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        for (int pk = 0; pk < 4; pk++)
            for (long ck = -750; ck < 750; ck++)
                execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", pk, ck);
        // segments whose bounds are the least and greatest of their values, not of the descending order
        long lastValidSegmentRowId = SegmentBuilder.updateLastValidSegmentRowId(1000);
        try
        {
            flush();
        }
        finally
        {
            SegmentBuilder.updateLastValidSegmentRowId(lastValidSegmentRowId);
        }

        assertWrittenInApacheFormat(index);
        IndexContext context = ((StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(index)).getIndexContext();
        for (SSTableIndex sstableIndex : context.getView().getIndexes())
        {
            assertThat(sstableIndex.getSegments()).hasSize(6);
            for (Segment segment : sstableIndex.getSegments())
                assertThat(LongType.instance.compose(segment.metadata.minTerm)).isLessThan(LongType.instance.compose(segment.metadata.maxTerm));
        }
        assertRowCount(execute("SELECT pk FROM %s WHERE ck = 123"), 4);
        assertRowCount(execute("SELECT pk FROM %s WHERE ck >= -100 AND ck < 50"), 600);
    }

    @Test
    public void testCollectionWithEmptyLastElement() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, l list<int>)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, l) VALUES (0, [7, blobAsInt(0x)])");
        flush();

        assertWrittenInApacheFormat(index);
        assertRows(execute("SELECT pk FROM %s WHERE l CONTAINS 7"), row(0));
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
            assertThat(components.isEmpty()).isFalse();
            assertThat(components.isValid(true)).isTrue();
        }
    }
}
