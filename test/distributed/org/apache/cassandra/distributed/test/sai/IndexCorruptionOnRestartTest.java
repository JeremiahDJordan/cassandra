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

package org.apache.cassandra.distributed.test.sai;

import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * An SAI index with a corrupt component must not be used for the SSTable it belongs to after a restart: the index
 * must be rebuilt for that SSTable, and only for that SSTable, the other SSTables must stay indexed, and queries must
 * return every row.
 */
public class IndexCorruptionOnRestartTest extends TestBaseImpl
{
    private static final int ROWS = 100;

    private enum Corruption
    {
        /** the header of a per-index component, which header/footer validation detects */
        PER_INDEX_HEADER,
        /** the body of the per-index metadata, keeping its header and footer, so only reading it detects it */
        PER_INDEX_META_BODY,
        /** the header of a per-SSTable component, which validation detects */
        PER_SSTABLE_HEADER
    }

    @Test
    public void testRestartWithCorruptComponents() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(c -> c.with(GOSSIP, NETWORK)).start()))
        {
            for (Corruption corruption : Corruption.values())
            {
                String table = table(corruption);
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + table + " (k int PRIMARY KEY, v int) " +
                                     "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false}");
                cluster.schemaChange("CREATE CUSTOM INDEX " + table + "_idx ON " + KEYSPACE + '.' + table + "(v) USING 'StorageAttachedIndex'");
            }
            SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE);

            // Write each table in two flushes, so it has several SSTables, and only one of them gets corrupted
            for (int i = 0; i < ROWS; i++)
            {
                for (Corruption corruption : Corruption.values())
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + table(corruption) + " (k, v) VALUES (?, ?)", ConsistencyLevel.ONE, i, i);
                if (i == ROWS / 2 - 1 || i == ROWS - 1)
                    cluster.get(1).flush(KEYSPACE);
            }
            for (Corruption corruption : Corruption.values())
                assertThat(countRows(cluster, corruption)).isEqualTo(ROWS);

            String[] paths = new String[Corruption.values().length];
            for (Corruption corruption : Corruption.values())
                paths[corruption.ordinal()] = componentToCorrupt(cluster, corruption);

            long mark = cluster.get(1).logs().mark();
            cluster.get(1).shutdown().get();
            for (Corruption corruption : Corruption.values())
                corrupt(paths[corruption.ordinal()], corruption == Corruption.PER_INDEX_META_BODY);
            cluster.get(1).startup();

            SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE);
            Map<Corruption, Integer> rowsAfterRestart = new EnumMap<>(Corruption.class);
            for (Corruption corruption : Corruption.values())
                rowsAfterRestart.put(corruption, countRows(cluster, corruption));
            for (Corruption corruption : Corruption.values())
                assertThat(rowsAfterRestart).as("Rows found after the restart").containsEntry(corruption, ROWS);

            // Only the corrupted SSTable of each table must be rebuilt, not the valid one
            Map<Corruption, Integer> rebuiltSSTables = new EnumMap<>(Corruption.class);
            for (Corruption corruption : Corruption.values())
                rebuiltSSTables.put(corruption, cluster.get(1).logs().grep(mark, "\\." + table(corruption) + "\\.\\*\\] Starting index build on").getResult().size());
            for (Corruption corruption : Corruption.values())
                assertThat(rebuiltSSTables).as("SSTables rebuilt after the restart").containsEntry(corruption, 1);

            assertThat(cluster.get(1).logs().grep(mark, "NullPointerException").getResult()).isEmpty();
        }
    }

    private static String table(Corruption corruption)
    {
        return corruption.name().toLowerCase();
    }

    private static int countRows(Cluster cluster, Corruption corruption)
    {
        return cluster.coordinator(1).execute("SELECT k FROM " + KEYSPACE + '.' + table(corruption) + " WHERE v >= 0", ConsistencyLevel.ONE).length;
    }

    private static String componentToCorrupt(Cluster cluster, Corruption corruption)
    {
        String table = table(corruption);
        int ordinal = corruption.ordinal();
        return cluster.get(1).callOnInstance(() -> {
            Corruption c = Corruption.values()[ordinal];
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
            SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
            StorageAttachedIndex index = StorageAttachedIndexGroup.getIndexGroup(cfs).getIndexes().iterator().next();
            IndexContext context = index.getIndexContext();
            IndexDescriptor descriptor = IndexDescriptor.load(sstable, Set.of(context));
            switch (c)
            {
                case PER_INDEX_HEADER:
                    return descriptor.perIndexComponents(context).get(IndexComponentType.KD_TREE).file().absolutePath();
                case PER_INDEX_META_BODY:
                    return descriptor.perIndexComponents(context).get(IndexComponentType.META).file().absolutePath();
                default:
                    IndexComponents.ForRead perSSTable = descriptor.perSSTableComponents();
                    IndexComponentType type = perSSTable.has(IndexComponentType.TOKEN_VALUES)
                                              ? IndexComponentType.TOKEN_VALUES
                                              : perSSTable.expectedComponentsForVersion().stream()
                                                          .filter(t -> t != IndexComponentType.GROUP_COMPLETION_MARKER && perSSTable.has(t))
                                                          .findFirst()
                                                          .orElseThrow();
                    return perSSTable.get(type).file().absolutePath();
            }
        });
    }

    private static void corrupt(String path, boolean keepHeaderAndFooter) throws Exception
    {
        try (RandomAccessFile file = new RandomAccessFile(path, "rw"))
        {
            // the junk goes between the first 40 bytes (the header) and the 16-byte footer
            int offset = keepHeaderAndFooter ? 40 : 0;
            int length = keepHeaderAndFooter ? (int) (file.length() - offset - 16) : (int) Math.min(64, file.length());
            assertThat(length).isPositive();
            byte[] junk = new byte[length];
            Arrays.fill(junk, (byte) 0x5A);
            file.seek(offset);
            file.write(junk);
        }
    }
}
