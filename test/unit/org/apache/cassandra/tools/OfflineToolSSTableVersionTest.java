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

package org.apache.cassandra.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.Upgrader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_LEGACY_SSTABLE_ROOT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * An offline tool whose configuration sets a storage compatibility mode, or none. It runs in that mode, as in Apache
 * Cassandra.
 */
@Ignore
public abstract class OfflineToolSSTableVersionTest
{
    private static final String KEYSPACE = "legacy_tables";

    private static WithProperties properties;
    private static StorageCompatibilityMode configured;

    /**
     * @param mode the mode of the configuration, or null for a configuration that doesn't set one
     */
    protected static void initialize(@Nullable StorageCompatibilityMode mode) throws IOException
    {
        String configUrl = CassandraRelevantProperties.CASSANDRA_CONFIG.getString();
        URL url;
        try
        {
            url = new URL(configUrl);
        }
        catch (MalformedURLException e)
        {
            url = OfflineToolSSTableVersionTest.class.getClassLoader().getResource(configUrl);
        }
        List<String> config;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream())))
        {
            config = reader.lines().filter(line -> !line.startsWith("storage_compatibility_mode:")).collect(Collectors.toList());
        }
        if (mode != null)
            config.add("storage_compatibility_mode: " + mode);
        // CASSANDRA_4 doesn't allow bti as the selected format
        if (mode == StorageCompatibilityMode.CASSANDRA_4)
        {
            int sstable = config.indexOf("sstable:");
            if (sstable >= 0)
                config.subList(sstable, sstable + 2).clear();
            config.add("sstable:");
            config.add("  selected_format: big");
        }
        Path file = Files.createTempFile("cassandra", ".yaml");
        file.toFile().deleteOnExit();
        Files.write(file, config);

        // without a mode, a tool runs in the mode of a node without one: HCD_1 outside of tests
        configured = mode == null ? CassandraRelevantProperties.TEST_STORAGE_COMPATIBILITY_MODE.getEnum(true, StorageCompatibilityMode.class) : mode;
        properties = new WithProperties().set(CassandraRelevantProperties.CASSANDRA_CONFIG, file.toUri().toString());
        DatabaseDescriptor.toolInitialization();
    }

    @AfterClass
    public static void tearDown()
    {
        properties.close();
    }

    /**
     * A tool reads and writes the versions of the mode of its configuration.
     */
    @Test
    public void testVersionsOfConfiguredMode()
    {
        assertThat(DatabaseDescriptor.getStorageCompatibilityMode()).isEqualTo(configured);
        assertThat(DatabaseDescriptor.getStorageCompatibilityModeForWrites()).isEqualTo(configured);
        assertThat(BigFormat.getInstance().getLatestVersion().version).isEqualTo(configured.isBefore(5) ? BigFormat.STORAGE_COMPATIBILITY_4_VERSION : "oa");
        assertThat(BigFormat.getInstance().getVersion("oa").isCompatible()).isEqualTo(configured != StorageCompatibilityMode.CASSANDRA_4);
        assertThat(BtiFormat.getInstance().getVersion("da").isCompatible()).isTrue();
        if (configured != StorageCompatibilityMode.UPGRADING)
            assertThat(CellData.getVersionedMaxDeletiontionTime()).isEqualTo(configured.isBefore(5) ? CellData.MAX_DELETION_TIME_2038_LEGACY_CAP : CellData.MAX_DELETION_TIME);

        Assume.assumeFalse(CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.isPresent());
        assertThat(BtiFormat.getInstance().getLatestVersion().version).isEqualTo(configured == StorageCompatibilityMode.HCD_1 ? BtiFormat.STORAGE_COMPATIBILITY_HCD_1_VERSION : "da");
    }

    /**
     * sstableupgrade rewrites a legacy sstable in the version of the mode of its configuration.
     */
    @Test
    public void testUpgradeToVersionOfConfiguredMode() throws IOException
    {
        Assume.assumeFalse(CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.isPresent());
        String legacyVersion = configured == StorageCompatibilityMode.HCD_1 ? "da" : "cc";
        String expected = BtiFormat.isSelected()
                          ? configured == StorageCompatibilityMode.HCD_1 ? BtiFormat.STORAGE_COMPATIBILITY_HCD_1_VERSION : "da"
                          : configured.isBefore(5) ? BigFormat.STORAGE_COMPATIBILITY_4_VERSION : "oa";
        String table = "legacy_" + legacyVersion + "_simple";

        Schema.instance.transform(SchemaTransformations.addKeyspace(KeyspaceMetadata.create(KEYSPACE,
                                                                                            KeyspaceParams.simple(1),
                                                                                            Tables.of(CreateTableStatement.parse("CREATE TABLE " + table + " (pk text PRIMARY KEY, val text)", KEYSPACE).build())),
                                                                    true));
        Keyspace.setInitialized();
        ColumnFamilyStore cfs = Keyspace.openWithoutSSTables(KEYSPACE).getColumnFamilyStore(table);
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        try (Stream<Path> legacyFiles = Files.list(new File(TEST_LEGACY_SSTABLE_ROOT.getString(), legacyVersion + '/' + KEYSPACE + '/' + table).toPath()))
        {
            for (Path legacyFile : legacyFiles.collect(Collectors.toList()))
                Files.copy(legacyFile, dir.toPath().resolve(legacyFile.getFileName()));
        }

        for (Map.Entry<Descriptor, Set<Component>> entry : cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).list().entrySet())
        {
            SSTableReader sstable = SSTableReader.openNoValidation(entry.getKey(), entry.getValue(), cfs);
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.UPGRADE_SSTABLES, sstable))
            {
                new Upgrader(cfs, txn, new OutputHandler.LogOutput()).upgrade(false);
            }
        }

        LifecycleTransaction.waitForDeletions();
        Set<Descriptor> upgraded = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).list().keySet();
        assertThat(upgraded).isNotEmpty();
        assertThat(upgraded).allMatch(descriptor -> descriptor.version.version.equals(expected));
    }
}
