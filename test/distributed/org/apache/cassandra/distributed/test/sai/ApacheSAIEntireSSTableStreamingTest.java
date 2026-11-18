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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Entire-SSTable streaming between our nodes of sstables that have SAI components written by Apache Cassandra 5.0
 * (test/data/legacy-sai/apache-aa): the receiver reads the streamed Apache components, without rebuilding them.
 */
public class ApacheSAIEntireSSTableStreamingTest extends TestBaseImpl
{
    private static final String TABLE = "wide";

    @Test
    public void testStreamApacheComponents() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("stream_entire_sstables", true)
                                                             .set("autocompaction_on_startup_enabled", false))
                                           .start()))
        {
            cluster.disableAutoCompaction(KEYSPACE);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int, ck int, st int static, s text, n bigint, PRIMARY KEY (pk, ck))"));
            for (String column : new String[]{ "s", "n", "st" })
                cluster.schemaChange(withKeyspace("CREATE INDEX wide_" + column + " ON %s." + TABLE + '(' + column + ") USING 'sai'"));
            SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);

            IInvokableInstance sender = cluster.get(1);
            IInvokableInstance receiver = cluster.get(2);

            // The sender imports the Apache sstables, and reads their components.
            sender.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
                Path target = cfs.getDirectories().getDirectoryForNewSSTables().toPath();
                try (Stream<Path> files = Files.list(Paths.get("test/data/legacy-sai/apache-aa/wide")))
                {
                    for (Path file : (Iterable<Path>) files::iterator)
                        Files.copy(file, target.resolve(file.getFileName()));
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
                cfs.loadNewSSTables();
                assertApacheComponents();
            });
            assertQueries(sender);

            // The receiver gets them by entire-SSTable streaming (it replicates everything, and has nothing yet).
            receiver.nodetoolResult("rebuild", "--keyspace", KEYSPACE).asserts().success();
            receiver.runOnInstance(ApacheSAIEntireSSTableStreamingTest::assertApacheComponents);
            assertQueries(receiver);
        }
    }

    /**
     * Every sstable of the table has its Apache components listed in its TOC, and all the indexes read them.
     */
    private static void assertApacheComponents()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            try
            {
                assertThat(Files.readAllLines(sstable.descriptor.fileFor(SSTableFormat.Components.TOC).toPath()))
                    .anyMatch(component -> component.startsWith(Version.APACHE_COMPONENT_PREFIX));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            IndexDescriptor descriptor = group.descriptorFor(sstable);
            assertThat(descriptor.perSSTableComponents().version()).isEqualTo(Version.APACHE_AA);
            for (StorageAttachedIndex index : group.getIndexes())
                assertThat(descriptor.perIndexComponents(index.getIndexContext()).version()).isEqualTo(Version.APACHE_AA);
        }
        for (StorageAttachedIndex index : group.getIndexes())
            assertThat(index.getIndexContext().getView().getIndexes()).hasSize(2);
    }

    private static void assertQueries(IInvokableInstance node)
    {
        assertThat(node.executeInternal(withKeyspace("SELECT pk, ck FROM %s." + TABLE + " WHERE s = 'w1'")).length).isEqualTo(100);
        Object[][] rows = node.executeInternal(withKeyspace("SELECT pk, ck FROM %s." + TABLE + " WHERE n = 423"));
        assertThat(rows.length).isEqualTo(1);
        assertThat(rows[0]).containsExactly(42, 3);
        assertThat(node.executeInternal(withKeyspace("SELECT pk FROM %s." + TABLE + " WHERE st = 2 PER PARTITION LIMIT 1")).length).isEqualTo(20);
    }
}
