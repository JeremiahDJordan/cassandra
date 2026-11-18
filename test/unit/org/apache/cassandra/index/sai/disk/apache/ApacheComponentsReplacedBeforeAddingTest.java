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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The build of the indexes missing on SSTables that aren't live yet replaces their Apache components; that doesn't
 * change the disk space the table accounts for, which the SSTables aren't part of yet.
 */
public class ApacheComponentsReplacedBeforeAddingTest extends ApacheIndexTester
{
    private boolean incrementalBackups;

    @Before
    public void disableIncrementalBackups()
    {
        // The SSTables are added to the table twice, which incremental backups don't support.
        incrementalBackups = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
    }

    @After
    public void restoreIncrementalBackups()
    {
        DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackups);
    }

    @Test
    public void testBuildBeforeAddingKeepsDiskSpaceMetrics() throws Throwable
    {
        createTable(SKINNY);
        disableCompaction();
        createIndex("CREATE INDEX skinny_s ON %s(s) USING 'sai'");
        loadApacheSSTables("skinny");
        assertReadFromApacheComponents("skinny_s");
        Object[][] expected = getRows(execute("SELECT pk FROM %s WHERE s = 's3'"));
        assertThat(expected).isNotEmpty();

        // Take the SSTables out of the table and of its metrics, as if a stream had just received them
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        long bytes = sstables.stream().mapToLong(SSTableReader::bytesOnDisk).sum();
        cfs.getTracker().removeUnsafe(sstables);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        group.onSSTableChanged(sstables, Collections.emptySet(), group.getIndexes(), false);
        cfs.metric.liveDiskSpaceUsed.dec(bytes);
        cfs.metric.totalDiskSpaceUsed.dec(bytes);

        // A new index is missing on them: building it replaces all their Apache components
        createIndex("CREATE INDEX skinny_x ON %s(x) USING 'sai'");
        waitForTableIndexesQueryable();
        long liveBefore = cfs.metric.liveDiskSpaceUsed.getCount();
        long totalBefore = cfs.metric.totalDiskSpaceUsed.getCount();
        cfs.indexManager.buildMissingSSTableAttachedIndexes(sstables);
        assertThat(cfs.metric.liveDiskSpaceUsed.getCount()).isEqualTo(liveBefore);
        assertThat(cfs.metric.totalDiskSpaceUsed.getCount()).isEqualTo(totalBefore);

        cfs.addSSTables(sstables, OperationType.STREAM);
        cfs.indexManager.checkSSTableAttachedIndexesAfterAdding(sstables);
        long actual = sstables.stream().mapToLong(SSTableReader::bytesOnDisk).sum();
        assertThat(cfs.metric.liveDiskSpaceUsed.getCount() - liveBefore).isEqualTo(actual);
        assertThat(cfs.metric.totalDiskSpaceUsed.getCount() - totalBefore).isEqualTo(actual);

        assertNoApacheComponents("skinny_s", "skinny_x");
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE s = 's3'"), expected);
    }
}
