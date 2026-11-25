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

package org.apache.cassandra.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.SchemaCompatibility;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.NODES_PERSISTENCE_CLASS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_READ_APACHE_FORMAT;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VERSION_SELECTOR_CLASS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_CHECKSUM_TYPE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_COMPRESSION_SELECTOR_CLASS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_NTS_DC_OVERRIDE_PROPERTY;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_5_MODE_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.schema.SchemaCompatibility.Target.APACHE_5_0;

/**
 * Checks of a node in {@link StorageCompatibilityMode#CASSANDRA_5}, made before it writes anything: settings with
 * which it would write what Apache Cassandra 5.0 doesn't read.
 */
public final class Cassandra5ModeChecks
{
    private static final String NODES_PERSISTENCE = "org.apache.cassandra.nodes.NodesPersistence";

    private Cassandra5ModeChecks()
    {
    }

    /**
     * Throws if {@link StorageCompatibilityMode#CASSANDRA_5} isn't available, or if a node, {@code daemon}, sets
     * another messaging version than the one of the mode.
     */
    static void validateMode(boolean daemon)
    {
        if (!TEST_CASSANDRA_5_MODE_ENABLED.getBoolean())
            throw new ConfigurationException("storage_compatibility_mode " + StorageCompatibilityMode.CASSANDRA_5 + " isn't supported", false);
        if (daemon && DS_CURRENT_MESSAGING_VERSION.isPresent() && DS_CURRENT_MESSAGING_VERSION.getInt() != MessagingService.VERSION_50)
            throw new ConfigurationException(String.format("-D%s=%s isn't allowed in storage_compatibility_mode %s, which uses messaging version %d",
                                                           DS_CURRENT_MESSAGING_VERSION.getKey(), DS_CURRENT_MESSAGING_VERSION.getString(),
                                                           StorageCompatibilityMode.CASSANDRA_5, MessagingService.VERSION_50),
                                             false);
    }

    /**
     * Throws, naming each setting, if the configuration makes the node write what Apache Cassandra 5.0 doesn't read.
     */
    static void validateConfiguration(Config conf)
    {
        List<String> refused = new ArrayList<>();

        if (TRIE_INDEX_FORMAT_VERSION.isPresent() && !"da".equals(TRIE_INDEX_FORMAT_VERSION.getString()))
            refused.add(String.format("-D%s=%s", TRIE_INDEX_FORMAT_VERSION.getKey(), TRIE_INDEX_FORMAT_VERSION.getString()));
        if (!"CRC32".equalsIgnoreCase(SSTABLE_CHECKSUM_TYPE.getString()))
            refused.add(String.format("-D%s=%s", SSTABLE_CHECKSUM_TYPE.getKey(), SSTABLE_CHECKSUM_TYPE.getString()));
        if (conf.flush_compression == Config.FlushCompression.adaptive)
            refused.add("flush_compression: adaptive");
        if (!SSTABLE_COMPRESSION_SELECTOR_CLASS.getDefaultValue().equals(SSTABLE_COMPRESSION_SELECTOR_CLASS.getString()))
            refused.add(String.format("-D%s=%s", SSTABLE_COMPRESSION_SELECTOR_CLASS.getKey(), SSTABLE_COMPRESSION_SELECTOR_CLASS.getString()));
        if (conf.commitlog_compression != null)
            refused.addAll(SchemaCompatibility.compressorIncompatibilities(APACHE_5_0, "commitlog_compression", conf.commitlog_compression));
        // unlike the commit log, hints don't treat a compressor without a class as no compression
        if (conf.hints_compression != null && (conf.hints_compression.class_name == null || conf.hints_compression.class_name.isEmpty()))
            refused.add("hints_compression: no compressor class");
        else if (conf.hints_compression != null)
            refused.addAll(SchemaCompatibility.compressorIncompatibilities(APACHE_5_0, "hints_compression", conf.hints_compression));
        if (conf.default_compaction != null)
        {
            CompactionParams defaultCompaction = CompactionParams.create(CompactionParams.classFromName(conf.default_compaction.class_name),
                                                                         conf.default_compaction.parameters == null ? Collections.emptyMap() : conf.default_compaction.parameters);
            refused.addAll(SchemaCompatibility.compactionIncompatibilities(APACHE_5_0, "default_compaction", defaultCompaction));
        }
        if (SYSTEM_DISTRIBUTED_NTS_DC_OVERRIDE_PROPERTY.isPresent() || SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY.isPresent())
            refused.add(String.format("-D%s and -D%s", SYSTEM_DISTRIBUTED_NTS_DC_OVERRIDE_PROPERTY.getKey(), SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY.getKey()));
        if (IMMUTABLE_SAI_COMPONENTS.getBoolean())
            refused.add(String.format("-D%s=true", IMMUTABLE_SAI_COMPONENTS.getKey()));
        // the SAI indexes the node writes are in Apache Cassandra's format
        if (!SAI_READ_APACHE_FORMAT.getBoolean())
            refused.add(String.format("-D%s=false", SAI_READ_APACHE_FORMAT.getKey()));
        if (!SAI_VERSION_SELECTOR_CLASS.getString().isEmpty())
            refused.add(String.format("-D%s=%s", SAI_VERSION_SELECTOR_CLASS.getKey(), SAI_VERSION_SELECTOR_CLASS.getString()));
        for (SSTableFormat<?, ?> format : DatabaseDescriptor.getSSTableFormats().values())
        {
            if (!(format instanceof BigFormat || format instanceof BtiFormat))
                refused.add("sstable format " + format.name() + " of " + format.getClass().getName());
        }
        if (NODES_PERSISTENCE_CLASS.isPresent() && !NODES_PERSISTENCE.equals(NODES_PERSISTENCE_CLASS.getString()))
            refused.add(String.format("-D%s=%s", NODES_PERSISTENCE_CLASS.getKey(), NODES_PERSISTENCE_CLASS.getString()));

        if (!refused.isEmpty())
            throw new ConfigurationException(String.format("storage_compatibility_mode %s doesn't allow settings with which this node would " +
                                                           "write what Apache Cassandra 5.0 can't read: %s",
                                                           StorageCompatibilityMode.CASSANDRA_5, String.join("; ", refused)),
                                             false);
    }
}
