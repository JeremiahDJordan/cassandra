/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable;

import java.util.List;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CQLSSTableWriterClientTest extends CQLSSTableWriterTest
{
    private IPartitioner oldPartitioner;

    @Before
    public void setUp()
    {
        DatabaseDescriptor.clientInitialization(true,
                                                () -> {
                                                    Config config = new Config();
                                                    config.data_file_directories = new String[]{ dataDir.absolutePath() };
                                                    config.default_compaction = new ParameterizedClass("LeveledCompactionStrategy");
                                                    config.num_tokens = 1;
                                                    return config;
                                                });
        DatabaseDescriptor.setDataDirectories(new File[] { dataDir});
        CassandraRelevantProperties.FORCE_LOAD_LOCAL_KEYSPACES.setBoolean(true);
        oldPartitioner = DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        Keyspace.setInitialized();
    }

    /**
     * A client whose configuration doesn't set storage_compatibility_mode writes the versions of HCD_1.
     */
    @Test
    public void testVersionsWithoutConfiguredMode() throws Exception
    {
        Assume.assumeFalse(CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.isPresent());
        assertEquals(StorageCompatibilityMode.NONE, DatabaseDescriptor.getStorageCompatibilityMode());

        for (SSTableFormat<?, ?> format : List.of(BtiFormat.getInstance(), BigFormat.getInstance()))
        {
            File dir = new File(dataDir, format.name());
            assertTrue(dir.tryCreateDirectories());
            try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                           .inDirectory(dir)
                                                           .forTable("CREATE TABLE client_versions." + table + " (k int PRIMARY KEY, v int)")
                                                           .withFormat(format)
                                                           .using("INSERT INTO client_versions." + table + " (k, v) VALUES (?, ?)")
                                                           .build())
            {
                writer.addRow(1, 1);
            }
            String expected = BtiFormat.is(format) ? BtiFormat.STORAGE_COMPATIBILITY_HCD_1_VERSION : BigFormat.STORAGE_COMPATIBILITY_4_VERSION;
            File[] dataFiles = dir.tryList(file -> file.name().endsWith("Data.db"));
            assertTrue(dataFiles.length > 0);
            for (File dataFile : dataFiles)
                assertEquals(dataFile.toString(), expected, Descriptor.fromFile(dataFile).version.version);
        }
    }

    /**
     * A client whose configuration doesn't set storage_compatibility_mode limits expirations to 2038, as HCD_1 does.
     */
    @Test
    public void testMaxExpirationWithoutConfiguredMode()
    {
        assertEquals(CellData.MAX_DELETION_TIME_2038_LEGACY_CAP, CellData.getVersionedMaxDeletiontionTime());
    }

    @After
    public void tearDown()
    {
        if (oldPartitioner != null)
            DatabaseDescriptor.setPartitionerUnsafe(oldPartitioner);
    }
}
