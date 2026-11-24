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

package org.apache.cassandra.schema;

import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ApacheUpgradePath;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.schema.SchemaCompatibility.Target.APACHE_5_0;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema changes refused while the Apache upgrade path is active, because Apache Cassandra 5.0 doesn't read or
 * accept the result, and the ones it accepts.
 */
public class SchemaCompatibilityTest extends CQLTester
{
    @BeforeClass
    public static void setUpApacheUpgradePath() throws Exception
    {
        // in UPGRADING after CASSANDRA_5, beside a ring member in CASSANDRA_5
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.UPGRADING);
        FileUtils.write(new File(DatabaseDescriptor.getMetadataDirectory(), ApacheUpgradePath.FILE),
                        Collections.singletonList(StorageCompatibilityMode.CASSANDRA_5.name()),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        InetAddressAndPort member = InetAddressAndPort.getByName("127.0.0.2");
        UUID hostId = UUID.randomUUID();
        Gossiper.instance.initializeNodeUnsafe(member, hostId, MessagingService.VERSION_50, 1);
        Gossiper.instance.injectApplicationState(member, ApacheUpgradePath.GOSSIP_STATE,
                                                 StorageService.instance.valueFactory.compatibilityModes("mode=CASSANDRA_5;compat=CASSANDRA_5"));
        StorageService.instance.getTokenMetadata().updateHostId(hostId, member);
        ApacheUpgradePath.instance.initialize();
        ApacheUpgradePath.instance.beforeGossipStart(new HashMap<>());
        assertThat(ApacheUpgradePath.isActive()).isTrue();
    }

    @Test
    public void testCompaction()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': '6', 'bucket_high': '2'}");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '80'}");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '2', 'bucket_high': '2'}");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T4, L10, N, -2'}");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'min_sstable_size': '700MiB', 'flush_size_override': '1MiB'}");

        assertRefused("compaction option 'log' of SizeTieredCompactionStrategy",
                      "CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'log': 'all'}");
        assertRefused("compaction option 'adaptive' of UnifiedCompactionStrategy",
                      "CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'adaptive': 'false'}");
        assertRefused("compaction option 'max_sstables_per_shard_factor' of UnifiedCompactionStrategy",
                      "CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'max_sstables_per_shard_factor': '2'}");
        assertRefused("compaction options of UnifiedCompactionStrategy: ",
                      "CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'min_sstable_size': 'auto'}");
        assertRefused("compaction options of UnifiedCompactionStrategy: min_sstable_size 800MiB",
                      "CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'min_sstable_size': '800MiB'}");
        assertRefused("compaction options of UnifiedCompactionStrategy: flush_size_override 512KiB",
                      "CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'flush_size_override': '512KiB'}");

        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertRefused("compaction option 'num_shards' of UnifiedCompactionStrategy",
                      "ALTER TABLE " + KEYSPACE + '.' + table + " WITH compaction = {'class': 'UnifiedCompactionStrategy', 'num_shards': '4'}");
    }

    @Test
    public void testCompression()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '32', 'lz4_compressor_type': 'high'}");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compression = {'class': 'ZstdCompressor', 'compression_level': '5'}");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compression = {'enabled': 'false'}");

        assertRefused("compressor org.apache.cassandra.io.compress.AdaptiveCompressor",
                      "CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compression = {'class': 'AdaptiveCompressor'}");
    }

    @Test
    public void testStorageAttachedIndex()
    {
        createTable("CREATE TABLE %s (k int, ck text, t text, t2 text, i int, b blob, l list<blob>, fl frozen<list<blob>>, mb map<int, blob>, v vector<float, 2>, PRIMARY KEY (k, ck))");
        createIndex("CREATE INDEX ON %s (t) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true'}");
        createIndex("CREATE INDEX ON %s (i) USING 'sai'");
        createIndex("CREATE INDEX ON %s (FULL(fl)) USING 'sai'");
        createIndex("CREATE INDEX ON %s (v) USING 'sai' WITH OPTIONS = {'similarity_function': 'dot_product', 'optimize_for': 'recall'}");

        String table = KEYSPACE + '.' + currentTable();
        assertRefused("option 'index_analyzer'",
                      "CREATE INDEX t2_analyzed ON " + table + " (t2) USING 'sai' WITH OPTIONS = {'index_analyzer': 'standard'}");
        assertRefused("analysis options on column ck",
                      "CREATE INDEX ck_analyzed ON " + table + " (ck) USING 'sai' WITH OPTIONS = {'ascii': 'true'}");
        assertRefused("more than one storage-attached index on column t", "CREATE INDEX t_unanalyzed ON " + table + " (t) USING 'sai'");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 2>)");
        table = KEYSPACE + '.' + currentTable();
        assertRefused("option 'maximum_node_connections' needs cassandra.sai.vector.allow_custom_parameters",
                      "CREATE INDEX v_index ON " + table + " (v) USING 'sai' WITH OPTIONS = {'maximum_node_connections': '32'}");

        String longColumn = "c".repeat(49);
        createTable("CREATE TABLE %s (k int PRIMARY KEY, " + longColumn + " int)");
        assertRefused("target column name '" + longColumn + "' isn't a word of at most 48 characters",
                      "CREATE INDEX long_target ON " + KEYSPACE + '.' + currentTable() + " (" + longColumn + ") USING 'sai'");
    }

    @Test
    public void testKeyspacesAndTypes()
    {
        createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}");

        assertRefused("replication strategy org.apache.cassandra.locator.EverywhereStrategy",
                      "CREATE KEYSPACE everywhere WITH replication = {'class': 'EverywhereStrategy'}");
        assertRefused("name longer than 48 characters",
                      "CREATE KEYSPACE " + "k".repeat(49) + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        assertRefused("type 'org.apache.cassandra.db.marshal.DateRangeType'",
                      "CREATE TABLE %s (k int PRIMARY KEY, m map<int, frozen<list<'DateRangeType'>>>)");

        assertRefused("type 'org.apache.cassandra.db.marshal.DateRangeType'",
                      "CREATE TABLE %s (k int PRIMARY KEY) WITH DROPPED COLUMN RECORD old 'DateRangeType' USING TIMESTAMP 1680702275400000");

        String type = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, u " + type + ", fu frozen<" + type + ">)");
        String table = KEYSPACE + '.' + currentTable();
        execute("ALTER TABLE " + table + " DROP fu");
        assertRefused("dropped non-frozen column u of a user type", "ALTER TABLE " + table + " DROP u");
    }

    @Test
    public void testMemtable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH memtable = 'skiplist_remapped'");

        assertRefused("memtable configuration 'trie_stage1' of org.apache.cassandra.db.memtable.TrieMemtableStage1",
                      "CREATE TABLE %s (k int PRIMARY KEY, v int) WITH memtable = 'trie_stage1'");
    }

    @Test
    public void testFunctionsAndAggregates() throws Throwable
    {
        String function = createFunction(KEYSPACE, "int, int", "CREATE FUNCTION %s (a int, b int) CALLED ON NULL INPUT RETURNS int " +
                                                               "LANGUAGE java AS 'return a == null || b == null ? null : a + b;'");
        createAggregate(KEYSPACE, "int", "CREATE AGGREGATE %s (int) SFUNC " + shortFunctionName(function) + " STYPE int INITCOND 0");

        assertRefused("DETERMINISTIC or MONOTONIC",
                      "CREATE FUNCTION " + KEYSPACE + ".deterministic_plus (a int, b int) CALLED ON NULL INPUT RETURNS int " +
                      "DETERMINISTIC LANGUAGE java AS 'return a == null || b == null ? null : a + b;'");
        assertRefused("DETERMINISTIC",
                      "CREATE AGGREGATE " + KEYSPACE + ".deterministic_total (int) SFUNC " + shortFunctionName(function) +
                      " STYPE int DETERMINISTIC");
    }

    @Test
    public void testChangedTableCheckedWhole()
    {
        TableMetadata table = CreateTableStatement.parse("CREATE TABLE t (k int PRIMARY KEY, v int) " +
                                                         "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'log': 'all'}", KEYSPACE)
                                                  .build();
        KeyspaceMetadata keyspace = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(table));
        KeyspaceMetadata commented = keyspace.withSwapped(keyspace.tables.withSwapped(table.withSwapped(table.params.unbuild().comment("c").build())));

        assertThat(SchemaCompatibility.incompatibilities(APACHE_5_0, Keyspaces.of(keyspace), Keyspaces.of(keyspace))).isEmpty();
        assertThat(SchemaCompatibility.incompatibilities(APACHE_5_0, Keyspaces.of(keyspace), Keyspaces.of(commented)))
        .containsExactly("table " + KEYSPACE + ".t: compaction option 'log' of SizeTieredCompactionStrategy");
    }

    @Test
    public void testClassesOutsideApachePackage()
    {
        TableMetadata table = CreateTableStatement.parse("CREATE TABLE t (k int PRIMARY KEY, v int)", KEYSPACE).build();
        KeyspaceMetadata keyspace = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(table));

        assertThat(SchemaCompatibility.incompatibilities(APACHE_5_0, Keyspaces.none(), Keyspaces.of(withTrigger(keyspace, table, "com.example.Trigger")))).isEmpty();
        assertThat(SchemaCompatibility.incompatibilities(APACHE_5_0, Keyspaces.none(), Keyspaces.of(withTrigger(keyspace, table, "org.apache.cassandra.triggers.Trigger"))))
        .containsExactly("table " + KEYSPACE + ".t: trigger org.apache.cassandra.triggers.Trigger");
    }

    @Test
    public void testIndexesOnBlobValues()
    {
        TableMetadata table = CreateTableStatement.parse("CREATE TABLE t (k int PRIMARY KEY, b blob, l list<blob>, fl frozen<list<blob>>, mb map<int, blob>)", KEYSPACE).build();

        assertThat(withStorageAttachedIndex(table, "full(fl)")).isEmpty();
        assertThat(withStorageAttachedIndex(table, "b")).containsExactly("table " + KEYSPACE + ".t index i: index on blob values of column b");
        assertThat(withStorageAttachedIndex(table, "values(l)")).containsExactly("table " + KEYSPACE + ".t index i: index on blob values of column l");
        assertThat(withStorageAttachedIndex(table, "entries(mb)")).containsExactly("table " + KEYSPACE + ".t index i: index on blob values of column mb");
    }

    private static List<String> withStorageAttachedIndex(TableMetadata table, String target)
    {
        IndexMetadata index = IndexMetadata.fromSchemaMetadata("i", IndexMetadata.Kind.CUSTOM, Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName(),
                                                                                                       IndexTarget.TARGET_OPTION_NAME, target));
        KeyspaceMetadata keyspace = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(table.withSwapped(Indexes.of(index))));
        return SchemaCompatibility.incompatibilities(APACHE_5_0, Keyspaces.none(), Keyspaces.of(keyspace));
    }

    @Test
    public void testCustomIndexWithoutTarget()
    {
        TableMetadata table = CreateTableStatement.parse("CREATE TABLE t (k int PRIMARY KEY, v int)", KEYSPACE).build();
        IndexMetadata index = IndexMetadata.fromSchemaMetadata("i", IndexMetadata.Kind.CUSTOM, Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, "com.example.Index"));
        KeyspaceMetadata keyspace = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(table.withSwapped(Indexes.of(index))));

        assertThat(SchemaCompatibility.incompatibilities(APACHE_5_0, Keyspaces.none(), Keyspaces.of(keyspace))).isEmpty();
    }

    @Test
    public void testReportInCassandra5Mode()
    {
        TableMetadata table = CreateTableStatement.parse("CREATE TABLE t (k int PRIMARY KEY, v int) " +
                                                         "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'log': 'all'}", KEYSPACE)
                                                  .build();
        Keyspaces received = Keyspaces.of(KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(table)));
        assertThat(SchemaCompatibility.reportInCassandra5Mode(Keyspaces.none(), received)).isEmpty();

        StorageCompatibilityMode mode = DatabaseDescriptor.getStorageCompatibilityMode();
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_5);
        try
        {
            assertThat(SchemaCompatibility.reportInCassandra5Mode(Keyspaces.none(), received))
            .containsExactly("table " + KEYSPACE + ".t: compaction option 'log' of SizeTieredCompactionStrategy");
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(mode);
        }
    }

    @Test
    public void testStoredSchemaReported()
    {
        TableMetadata table = CreateTableStatement.parse("CREATE TABLE t (k int PRIMARY KEY, v int) " +
                                                         "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'log': 'all'}", "stored_only")
                                                  .build();
        KeyspaceMetadata stored = KeyspaceMetadata.create("stored_only", KeyspaceParams.simple(1), Tables.of(table));
        // in the schema tables only, as a replayed schema mutation is before the schema is reloaded
        SchemaKeyspace.applyChanges(SchemaKeyspace.convertSchemaDiffToMutations(Keyspaces.diff(Keyspaces.none(), Keyspaces.of(stored)), FBUtilities.timestampMicros()));

        inCassandra5Mode(() -> assertThat(SchemaCompatibility.reportStoredSchemaInCassandra5Mode())
                               .contains("table stored_only.t: compaction option 'log' of SizeTieredCompactionStrategy"));
    }

    @Test
    public void testReceivedSchemaReported()
    {
        TableMetadata table = CreateTableStatement.parse("CREATE TABLE received (k int PRIMARY KEY, v int) " +
                                                         "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'log': 'all'}", KEYSPACE)
                                                  .build();
        KeyspaceMetadata before = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        KeyspaceMetadata after = before.withSwapped(before.tables.with(table));
        Collection<Mutation> mutations = SchemaKeyspace.convertSchemaDiffToMutations(Keyspaces.diff(Keyspaces.of(before), Keyspaces.of(after)), FBUtilities.timestampMicros());

        Logger logger = (Logger) LoggerFactory.getLogger(SchemaCompatibility.class);
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
        try
        {
            inCassandra5Mode(() -> ((DefaultSchemaUpdateHandler) Schema.instance.updateHandler).applyMutations(mutations));
        }
        finally
        {
            logger.detachAppender(logs);
        }
        assertThat(logs.list).extracting(ILoggingEvent::getFormattedMessage)
                             .anyMatch(message -> message.contains("table " + KEYSPACE + ".received: compaction option 'log' of SizeTieredCompactionStrategy"));
    }

    @Test
    public void testCompressionOverrideInCassandra5Mode()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        inCassandra5Mode(() -> assertThatThrownBy(() -> cfs.setCompressionParameters(Map.of("class", "AdaptiveCompressor")))
                               .hasMessageContaining("storage_compatibility_mode CASSANDRA_5 doesn't allow compression Apache Cassandra 5.0 can't read")
                               .hasMessageContaining("compressor org.apache.cassandra.io.compress.AdaptiveCompressor"));
        inCassandra5Mode(() -> cfs.setCompressionParameters(Map.of("class", "ZstdCompressor")));
    }

    private static void inCassandra5Mode(Runnable runnable)
    {
        StorageCompatibilityMode mode = DatabaseDescriptor.getStorageCompatibilityMode();
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.CASSANDRA_5);
        try
        {
            runnable.run();
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(mode);
        }
    }

    private static KeyspaceMetadata withTrigger(KeyspaceMetadata keyspace, TableMetadata table, String triggerClass)
    {
        return keyspace.withSwapped(keyspace.tables.withSwapped(table.withSwapped(Triggers.of(TriggerMetadata.create("tr", triggerClass)))));
    }

    /**
     * Checks that {@code query}, in which a table to create is {@code %s}, is refused for {@code incompatibility}.
     */
    private void assertRefused(String incompatibility, String query)
    {
        String formatted = query.replace("%s", KEYSPACE + ".refused");
        assertInvalidMessage("as Apache Cassandra 5.0 doesn't support: ", formatted);
        assertInvalidMessage(incompatibility, formatted);
    }
}
