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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

public class MemtableSchemaCompatibilityModeTest extends TestBaseImpl
{
    @Test
    public void testMemtableSchemaInCassandra4Mode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(1, config -> config.set("enable_materialized_views", true)
                                                                      .set("memtable", memtableConfigurations())
                                                                      .set("sstable", Map.of("selected_format", "big"))
                                                                      .set("storage_compatibility_mode", StorageCompatibilityMode.CASSANDRA_4.toString()))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v int) WITH memtable = 'skiplist'");
            cluster.schemaChange("CREATE MATERIALIZED VIEW " + KEYSPACE + ".mv AS SELECT * FROM " + KEYSPACE + ".t " +
                                 "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k) WITH memtable = 'skiplist'");

            // the map layout has no form for sharded memtables
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".sharded (k int PRIMARY KEY, v int) WITH memtable = 'skiplist_sharded'");

            assertMemtableCells(cluster, "skiplist");
            assertRows(cluster.coordinator(1).execute("SELECT memtable FROM system_schema.tables WHERE keyspace_name = ? AND table_name = 'sharded'", ALL, KEYSPACE),
                       row("skiplist_sharded"));
            assertThat(describe(cluster, "TABLE " + KEYSPACE + ".t")).contains("memtable = 'skiplist'");
            assertThat(describe(cluster, "MATERIALIZED VIEW " + KEYSPACE + ".mv")).contains("memtable = 'skiplist'");

            cluster.get(1).flush(SchemaConstants.SCHEMA_KEYSPACE_NAME);
            assertThat(memtableTypesInSSTables(cluster.get(1), SchemaKeyspaceTables.TABLES)).containsOnly("text");
            assertThat(memtableTypesInSSTables(cluster.get(1), SchemaKeyspaceTables.VIEWS)).containsOnly("text");

            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            assertThat(memtableConfiguration(cluster.get(1), "t")).isEqualTo("skiplist");
            assertThat(memtableConfiguration(cluster.get(1), "mv")).isEqualTo("skiplist");
            assertThat(memtableConfiguration(cluster.get(1), "sharded")).isEqualTo("skiplist_sharded");
        }
    }

    @Test
    public void testMemtableSchemaAfterSwitchToCassandra4Mode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(1, config -> config.set("memtable", memtableConfigurations())
                                                                      .set("sstable", Map.of("selected_format", "big"))
                                                                      .set("storage_compatibility_mode", StorageCompatibilityMode.HCD_1.toString()))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v int) WITH memtable = 'trie'");
            cluster.get(1).flush(SchemaConstants.SCHEMA_KEYSPACE_NAME);
            assertThat(memtableTypesInSSTables(cluster.get(1), SchemaKeyspaceTables.TABLES)).contains("frozen<map<text, text>>");

            cluster.get(1).shutdown().get();
            cluster.get(1).config().set("storage_compatibility_mode", StorageCompatibilityMode.CASSANDRA_4.toString());
            cluster.get(1).startup();

            // the map written in HCD_1 is still read as the table's configuration
            assertThat(memtableConfiguration(cluster.get(1), "t")).isEqualTo("trie");
            assertThat(describe(cluster, "TABLE " + KEYSPACE + ".t")).contains("memtable = 'trie'");

            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".t WITH memtable = 'skiplist'");
            assertRows(cluster.coordinator(1).execute("SELECT memtable FROM system_schema.tables WHERE keyspace_name = ? AND table_name = 't'", ALL, KEYSPACE),
                       row("skiplist"));

            cluster.get(1).forceCompact(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES);
            assertThat(memtableTypesInSSTables(cluster.get(1), SchemaKeyspaceTables.TABLES)).containsOnly("text");

            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            assertThat(memtableConfiguration(cluster.get(1), "t")).isEqualTo("skiplist");
        }
    }

    @Test
    public void testMemtableSchemaBetweenModes() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(Feature.GOSSIP, Feature.NETWORK)
                                                                       .set("memtable", memtableConfigurations())
                                                                       .set("sstable", Map.of("selected_format", "big"))
                                                                       .set("storage_compatibility_mode", config.num() == 1
                                                                                                          ? StorageCompatibilityMode.HCD_1.toString()
                                                                                                          : StorageCompatibilityMode.CASSANDRA_4.toString()))
                                           .start()))
        {
            // node 1 writes the map, as earlier releases do in CASSANDRA_4
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v int) WITH memtable = 'trie'", false, cluster.get(1));
            assertMemtableConfiguration(cluster, "trie");

            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".t WITH memtable = 'skiplist'", false, cluster.get(2));
            assertMemtableConfiguration(cluster, "skiplist");
        }
    }

    private static void assertMemtableConfiguration(Cluster cluster, String configuration)
    {
        for (IInvokableInstance instance : cluster)
            assertThat(memtableConfiguration(instance, "t")).isEqualTo(configuration);
        assertThat(schemaVersion(cluster.get(1))).isEqualTo(schemaVersion(cluster.get(2)));
    }

    private static String schemaVersion(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> Schema.instance.getVersion().toString());
    }

    private static Map<String, Object> memtableConfigurations()
    {
        return Map.of("configurations", Map.of("skiplist", Map.of("class_name", "SkipListMemtable"),
                                               "skiplist_sharded", Map.of("class_name", "ShardedSkipListMemtable"),
                                               "trie", Map.of("class_name", "TrieMemtable")));
    }

    private static void assertMemtableCells(Cluster cluster, String configuration)
    {
        assertRows(cluster.coordinator(1).execute("SELECT memtable FROM system_schema.tables WHERE keyspace_name = ? AND table_name = 't'", ALL, KEYSPACE),
                   row(configuration));
        assertRows(cluster.coordinator(1).execute("SELECT memtable FROM system_schema.views WHERE keyspace_name = ? AND view_name = 'mv'", ALL, KEYSPACE),
                   row(configuration));
    }

    private static String describe(Cluster cluster, String element)
    {
        return (String) cluster.coordinator(1).execute("DESCRIBE " + element, ALL)[0][3];
    }

    private static String memtableConfiguration(IInvokableInstance instance, String table)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metadata().params.memtable.configurationKey());
    }

    /**
     * The types the sstables of the schema table store the memtable column with.
     */
    private static Set<String> memtableTypesInSSTables(IInvokableInstance instance, String schemaTable)
    {
        return instance.callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME).getColumnFamilyStore(schemaTable);
            ColumnMetadata memtable = cfs.metadata().getColumn(ByteBufferUtil.bytes("memtable"));
            Set<String> types = new HashSet<>();
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                if (sstable.header.columns().contains(memtable))
                    types.add(sstable.header.getType(memtable).asCQL3Type().toString());
            }
            return types;
        });
    }
}
