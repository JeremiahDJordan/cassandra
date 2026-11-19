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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Restarts a node with tables that have several SAI indexes and many SSTables, where the index on {@code u} of each
 * table isn't built for one SSTable (its completion marker is missing, as after a build interrupted by a restart).
 * That index is the first one the table opens on restart (indexes are added in name order: u, v, w), so it needs an
 * initial build, which starts asynchronously while the table is still being opened, as the other two indexes are
 * loaded synchronously by the startup fast path. The build must succeed, and every index must find every row.
 */
public class IndexRestartWithManyIndexesTest extends TestBaseImpl
{
    private static final int TABLES = 6;
    private static final int SSTABLES = 40;
    private static final int ROWS_PER_SSTABLE = 3;

    @Test
    public void testRestartWithAnIndexToBuild() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withDataDirCount(1).withConfig(c -> c.with(GOSSIP, NETWORK)).start()))
        {
            for (int t = 0; t < TABLES; t++)
            {
                String table = "t" + t;
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + table + " (k int PRIMARY KEY, v int, w int, u int) " +
                                     "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false}");
                for (String column : new String[]{ "v", "w", "u" })
                    cluster.schemaChange("CREATE CUSTOM INDEX " + table + '_' + column + " ON " + KEYSPACE + '.' + table + '(' + column + ") USING 'StorageAttachedIndex'");
            }
            SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE);

            for (int s = 0; s < SSTABLES; s++)
            {
                for (int t = 0; t < TABLES; t++)
                    for (int j = 0; j < ROWS_PER_SSTABLE; j++)
                        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t" + t + " (k, v, w, u) VALUES (?, ?, ?, ?)",
                                                       ConsistencyLevel.ONE, s * ROWS_PER_SSTABLE + j, s, s, s);
                cluster.get(1).flush(KEYSPACE);
            }

            List<String> markers = new ArrayList<>();
            for (int t = 0; t < TABLES; t++)
                markers.add(firstIndexMarkerOfFirstSSTable(cluster, "t" + t));

            long mark = cluster.get(1).logs().mark();
            cluster.get(1).shutdown().get();
            for (String marker : markers)
                Files.delete(Paths.get(marker));
            cluster.get(1).startup();

            SAIUtil.waitForIndexQueryableOnFirstNode(cluster, KEYSPACE);
            for (int t = 0; t < TABLES; t++)
                for (String column : new String[]{ "v", "w", "u" })
                    assertThat(cluster.coordinator(1).execute("SELECT k FROM " + KEYSPACE + ".t" + t + " WHERE " + column + " >= 0", ConsistencyLevel.ONE))
                    .as("t" + t + '.' + column)
                    .hasNumberOfRows(SSTABLES * ROWS_PER_SSTABLE);

            assertThat(cluster.get(1).logs().grep(mark, "NullPointerException").getResult()).isEmpty();
        }
    }

    private static String firstIndexMarkerOfFirstSSTable(Cluster cluster, String table)
    {
        return cluster.get(1).callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
            List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
            sstables.sort(SSTableReader.idComparator);
            StorageAttachedIndex index = StorageAttachedIndexGroup.getIndexGroup(cfs).getIndexes().stream()
                                                                  .filter(i -> i.getIndexContext().getIndexName().endsWith("_u"))
                                                                  .findFirst()
                                                                  .orElseThrow();
            IndexContext context = index.getIndexContext();
            IndexDescriptor descriptor = IndexDescriptor.load(sstables.get(0), Set.of(context));
            for (IndexComponent.ForRead component : descriptor.perIndexComponents(context).all())
                if (component.isCompletionMarker())
                    return component.file().absolutePath();
            throw new AssertionError("No completion marker for " + context.getIndexName() + " on " + sstables.get(0));
        });
    }
}
