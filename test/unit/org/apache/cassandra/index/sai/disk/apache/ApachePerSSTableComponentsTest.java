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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads the per-SSTable components that Apache Cassandra 5.0 wrote (test/data/legacy-sai/apache-aa, written by
 * Apache cassandra-5.0 at fb7f1b7aee), and checks they map every indexed row to its primary key and back.
 */
public class ApachePerSSTableComponentsTest extends ApacheIndexTester
{
    @Test
    public void testSkinny() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s text, n int, v vector<float, 3>)");
        assertPrimaryKeyMaps("skinny");
    }

    @Test
    public void testWide() throws Throwable
    {
        createTable(WIDE);
        assertPrimaryKeyMaps("wide");
    }

    @Test
    public void testChecksumValidation() throws Throwable
    {
        createTable(WIDE);
        disableCompaction();
        loadApacheSSTables("wide");

        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            IndexComponents.ForRead perSSTable = IndexDescriptor.load(sstable, Set.of()).perSSTableComponents();
            assertThat(perSSTable.isValid(false)).isTrue();
            // Apache uses CRC32C (CASSANDRA-18836)
            assertThat(perSSTable.isValid(true)).isTrue();

            // flip a byte in the middle of a component: the header and footer still validate, the checksum doesn't
            flipMiddleByte(perSSTable.get(IndexComponentType.ROW_TO_TOKEN).file());
            assertThat(perSSTable.isValid(false)).isTrue();
            assertThat(perSSTable.isValid(true)).isFalse();
        }
    }

    private void assertPrimaryKeyMaps(String data) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        disableCompaction();
        loadApacheSSTables(data);

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexComponents.ForRead perSSTable = IndexDescriptor.load(sstable, Set.of()).perSSTableComponents();
            assertThat(perSSTable.isComplete()).isTrue();
            assertThat(perSSTable.version()).isEqualTo(Version.APACHE_AA);
            assertThat(perSSTable.onDiskFormat().indexFeatureSet().isRowAware()).isTrue();

            SSTableContext context = SSTableContext.create(sstable, perSSTable);
            try
            {
                List<PrimaryKey> expected = indexedKeys(context, sstable);
                try (PrimaryKeyMap map = context.primaryKeyMapFactory().newPerSSTablePrimaryKeyMap())
                {
                    assertThat(map.count()).isEqualTo(expected.size());
                    for (int rowId = 0; rowId < expected.size(); rowId++)
                    {
                        PrimaryKey key = expected.get(rowId);
                        PrimaryKey actual = map.primaryKeyFromRowId(rowId).loadDeferred();
                        assertThat(actual.partitionKey()).as("row %d", rowId).isEqualTo(key.partitionKey());
                        assertThat(actual.clustering()).as("row %d", rowId).isEqualTo(key.clustering());
                        assertThat(actual.compareTo(key)).as("row %d", rowId).isZero();
                    }
                }

                // The lookups by key only move forward (like queries do), so we check each kind in its own pass.
                try (PrimaryKeyMap map = context.primaryKeyMapFactory().newPerSSTablePrimaryKeyMap())
                {
                    for (int rowId = 0; rowId < expected.size(); rowId++)
                        assertThat(map.exactRowIdOrInvertedCeiling(expected.get(rowId))).as("row %d", rowId).isEqualTo(rowId);
                }
                try (PrimaryKeyMap map = context.primaryKeyMapFactory().newPerSSTablePrimaryKeyMap())
                {
                    for (int rowId = 0; rowId < expected.size(); rowId++)
                        assertThat(map.ceiling(expected.get(rowId))).as("row %d", rowId).isEqualTo(rowId);
                }
                try (PrimaryKeyMap map = context.primaryKeyMapFactory().newPerSSTablePrimaryKeyMap())
                {
                    // (the floor of a static row's key is the last row of the partition, like for our components)
                    for (int rowId = 0; rowId < expected.size(); rowId++)
                        if (expected.get(rowId).clustering() != Clustering.STATIC_CLUSTERING)
                            assertThat(map.floor(expected.get(rowId))).as("row %d", rowId).isEqualTo(rowId);
                }
            }
            finally
            {
                context.close();
            }
        }
    }

    /**
     * The primary keys of the rows Apache indexes, in order: the static row of each partition, if any, then the rows
     * with live data.
     */
    private static List<PrimaryKey> indexedKeys(SSTableContext context, SSTableReader sstable)
    {
        long nowInSec = FBUtilities.nowInSeconds();
        List<PrimaryKey> keys = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    if (!partition.staticRow().isEmpty() && partition.staticRow().hasLiveData(nowInSec, false))
                        keys.add(context.primaryKeyFactory().create(partition.partitionKey(), partition.staticRow().clustering()));
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (unfiltered.isRow() && ((Row) unfiltered).hasLiveData(nowInSec, false))
                            keys.add(context.primaryKeyFactory().create(partition.partitionKey(), ((Row) unfiltered).clustering()));
                    }
                }
            }
        }
        return keys;
    }
}
