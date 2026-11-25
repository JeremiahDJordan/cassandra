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

import java.util.Collections;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.config.CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.NODES_PERSISTENCE_CLASS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_READ_APACHE_FORMAT;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VERSION_SELECTOR_CLASS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_CHECKSUM_TYPE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_COMPRESSION_SELECTOR_CLASS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class Cassandra5ModeChecksTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testMessagingVersionProperty()
    {
        try (WithProperties properties = new WithProperties().set(DS_CURRENT_MESSAGING_VERSION, 111))
        {
            assertThatThrownBy(() -> Cassandra5ModeChecks.validateMode(true)).hasMessageContaining("-Dds.current_messaging_version=111 isn't allowed");
            Cassandra5ModeChecks.validateMode(false);
        }

        try (WithProperties properties = new WithProperties().set(DS_CURRENT_MESSAGING_VERSION, 13))
        {
            Cassandra5ModeChecks.validateMode(true);
        }
    }

    @Test
    public void testConfiguration()
    {
        Config config = new Config();
        config.commitlog_compression = new ParameterizedClass("LZ4Compressor", Map.of("lz4_compressor_type", "fast"));
        config.hints_compression = new ParameterizedClass("org.apache.cassandra.io.compress.ZstdCompressor", Map.of("compression_level", "3"));
        config.default_compaction = new ParameterizedClass("SizeTieredCompactionStrategy", Map.of("min_threshold", "4"));
        assertThatCode(() -> Cassandra5ModeChecks.validateConfiguration(config)).doesNotThrowAnyException();
        config.commitlog_compression = new ParameterizedClass("", Collections.emptyMap());
        assertThatCode(() -> Cassandra5ModeChecks.validateConfiguration(config)).doesNotThrowAnyException();

        assertRefused("flush_compression: adaptive", c -> c.flush_compression = Config.FlushCompression.adaptive);
        assertRefused("commitlog_compression: compressor org.apache.cassandra.io.compress.AdaptiveCompressor",
                      c -> c.commitlog_compression = new ParameterizedClass("AdaptiveCompressor", Collections.emptyMap()));
        assertRefused("commitlog_compression: compressor com.example.Compressor",
                      c -> c.commitlog_compression = new ParameterizedClass("com.example.Compressor", Collections.emptyMap()));
        assertRefused("hints_compression: no compressor class",
                      c -> c.hints_compression = new ParameterizedClass("", Collections.emptyMap()));
        assertRefused("hints_compression: compressor option 'level'",
                      c -> c.hints_compression = new ParameterizedClass("ZstdCompressor", Map.of("level", "3")));
        assertRefused("default_compaction: compaction option 'adaptive' of UnifiedCompactionStrategy",
                      c -> c.default_compaction = new ParameterizedClass("UnifiedCompactionStrategy", Map.of("adaptive", "false")));

        assertRefused("-Dcassandra.trie_index_format_version=cc", new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, "cc"));
        assertRefused("-Dcassandra.sstable.checksums.type=XXHASH64", new WithProperties().set(SSTABLE_CHECKSUM_TYPE, "XXHASH64"));
        assertRefused("-Dcassandra.sstable.compression.selector.class=com.example.Selector", new WithProperties().set(SSTABLE_COMPRESSION_SELECTOR_CLASS, "com.example.Selector"));
        assertRefused("-Dcassandra.system_distributed_replication_dc_names", new WithProperties().set(SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY, "3"));
        assertRefused("-Dcassandra.sai.immutable_components=true", new WithProperties().set(IMMUTABLE_SAI_COMPONENTS, true));
        assertRefused("-Dcassandra.sai.read_apache_format=false", new WithProperties().set(SAI_READ_APACHE_FORMAT, false));
        assertRefused("-Dcassandra.sai.version.selector.class=com.example.Selector", new WithProperties().set(SAI_VERSION_SELECTOR_CLASS, "com.example.Selector"));
        assertRefused("-Dcassandra.nodes.persistence_class=com.example.Persistence", new WithProperties().set(NODES_PERSISTENCE_CLASS, "com.example.Persistence"));
    }

    private static void assertRefused(String refusal, java.util.function.Consumer<Config> change)
    {
        Config config = new Config();
        change.accept(config);
        assertThatThrownBy(() -> Cassandra5ModeChecks.validateConfiguration(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(refusal);
    }

    private static void assertRefused(String refusal, WithProperties properties)
    {
        try (WithProperties ignored = properties)
        {
            assertThatThrownBy(() -> Cassandra5ModeChecks.validateConfiguration(new Config()))
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining(refusal);
        }
    }
}
