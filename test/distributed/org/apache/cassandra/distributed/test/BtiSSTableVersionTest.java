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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

public class BtiSSTableVersionTest extends TestBaseImpl
{
    // the startup warning, which names the tables
    private static final String NEWER_VERSIONS_WARNING = "sstables of versions newer than a release of storage_compatibility_mode HCD_1 reads .*, in .*" + KEYSPACE + ".tbl";

    @Test
    public void testVersionPerStorageCompatibilityMode() throws IOException
    {
        Assume.assumeFalse(CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.isPresent());
        assertFlushedVersion(StorageCompatibilityMode.NONE, "da");
        assertFlushedVersion(StorageCompatibilityMode.UPGRADING, "da");
        assertFlushedVersion(StorageCompatibilityMode.HCD_1, "cc");
    }

    /**
     * CASSANDRA_4, where bti can't be the selected format, writes bti "da", as Apache Cassandra does.
     */
    @Test
    public void testVersionInCassandra4() throws IOException
    {
        Assume.assumeFalse(CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.isPresent());
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.set("storage_compatibility_mode", StorageCompatibilityMode.CASSANDRA_4.toString())
                                                                       .set("sstable", Map.of("selected_format", "big")))
                                           .start()))
        {
            assertThat(cluster.get(1).callOnInstance(() -> BtiFormat.getInstance().getLatestVersion().version)).isEqualTo("da");
        }
    }

    @Test
    public void testExplicitVersion() throws IOException
    {
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION, "cc"))
        {
            assertFlushedVersion(StorageCompatibilityMode.NONE, "cc");
        }
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION, "da"))
        {
            assertFlushedVersion(StorageCompatibilityMode.HCD_1, "da");
        }
    }

    /**
     * A node restarted in HCD_1 reads the "da" sstables written in NONE and warns that a downgrade isn't possible while
     * they exist, until nodetool upgradesstables -a rewrites them as "cc".
     */
    @Test
    public void testRewriteForDowngrade() throws Exception
    {
        Assume.assumeFalse(CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.isPresent());
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.set("storage_compatibility_mode", StorageCompatibilityMode.NONE.toString())
                                                                       .set("sstable", Map.of("selected_format", "bti")))
                                           .start()))
        {
            IInvokableInstance node = cluster.get(1);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            node.executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (1, 1)"));
            node.flush(KEYSPACE);
            assertThat(liveVersions(node)).containsExactly("bti-da");

            restart(node, StorageCompatibilityMode.HCD_1);
            assertThat(liveVersions(node)).containsExactly("bti-da");
            assertThat(node.logs().grep(NEWER_VERSIONS_WARNING).getResult()).hasSize(1);
            assertRows(node.executeInternal(withKeyspace("SELECT pk, v FROM %s.tbl")), row(1, 1));

            assertThat(node.nodetool("upgradesstables", "-a", KEYSPACE, "tbl")).isZero();
            assertThat(liveVersions(node)).containsExactly("bti-cc");
            long mark = node.logs().mark();
            restart(node, StorageCompatibilityMode.HCD_1);
            assertThat(node.logs().grep(mark, NEWER_VERSIONS_WARNING).getResult()).isEmpty();
            assertRows(node.executeInternal(withKeyspace("SELECT pk, v FROM %s.tbl")), row(1, 1));
        }
    }

    private static void restart(IInvokableInstance node, StorageCompatibilityMode mode) throws Exception
    {
        node.shutdown().get();
        node.config().set("storage_compatibility_mode", mode.toString());
        node.startup();
    }

    private static List<String> liveVersions(IInvokableInstance node)
    {
        return node.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                 .getColumnFamilyStore("tbl")
                                                 .getLiveSSTables()
                                                 .stream()
                                                 .map(sstable -> sstable.descriptor.version.toFormatAndVersionString())
                                                 .collect(Collectors.toList()));
    }

    private static void assertFlushedVersion(StorageCompatibilityMode mode, String version) throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.set("storage_compatibility_mode", mode.toString())
                                                                       .set("sstable", Map.of("selected_format", "bti")))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (1, 1)"));
            cluster.get(1).flush(KEYSPACE);
            assertThat(liveVersions(cluster.get(1))).describedAs(mode.toString()).containsExactly("bti-" + version);
            // HCD_1 warns at startup when it writes a version its releases don't read
            assertThat(cluster.get(1).logs().grep("makes this node write bti sstables of a version newer").getResult())
            .hasSize(mode == StorageCompatibilityMode.HCD_1 && !version.equals("cc") ? 1 : 0);
        }
    }
}
