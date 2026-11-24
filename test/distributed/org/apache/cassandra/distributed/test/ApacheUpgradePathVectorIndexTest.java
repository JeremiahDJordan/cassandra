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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_5_MODE_ENABLED;

/**
 * While the Apache upgrade path is active, a vector index is accepted on a node with one data directory and a
 * separate local system data directory, as Apache Cassandra 5.0 counts only the data directories.
 */
public class ApacheUpgradePathVectorIndexTest extends TestBaseImpl
{
    @Test
    public void testVectorIndexWithSeparateLocalSystemDataDirectory() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_5_MODE_ENABLED, true);
             Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.set("storage_compatibility_mode", "CASSANDRA_5")
                                                                       .set("local_system_data_file_directory",
                                                                            ((String[]) config.get("data_file_directories"))[0] + "_local_system"))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v vector<float, 2>)"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t (v) USING 'sai'"));
        }
    }
}
