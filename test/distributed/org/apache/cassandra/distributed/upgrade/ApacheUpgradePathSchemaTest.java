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

package org.apache.cassandra.distributed.upgrade;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_5_MODE_ENABLED;
import static org.apache.cassandra.distributed.impl.AbstractCluster.CURRENT_VERSION;

/**
 * A node of this build moved from storage_compatibility_mode CASSANDRA_5 to UPGRADING has the same schema version as
 * an Apache node, for the internal keyspaces each node defines itself and for schema changes made through either
 * node.
 */
public class ApacheUpgradePathSchemaTest extends ApacheMixedClusterTestBase
{
    @Test
    public void testSchemaVersionsAgree() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(TEST_CASSANDRA_5_MODE_ENABLED, true);
             UpgradeableCluster cluster = startCluster(2,
                                                       config -> {
                                                           config.set("user_defined_functions_enabled", true);
                                                           if (config.num() == 2)
                                                               config.set("storage_compatibility_mode", "CASSANDRA_5");
                                                       },
                                                       apacheVersion(), CURRENT_VERSION))
        {
            IUpgradeableInstance apache = cluster.get(1);
            IUpgradeableInstance current = cluster.get(2);
            current.shutdown().get();
            current.config().set("storage_compatibility_mode", "UPGRADING");
            current.startup(cluster);
            assertSchemaAgreement(cluster);

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"), false, apache);
            cluster.schemaChange(withKeyspace("CREATE TYPE %s.address (street text, zip int)"), false, current);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v text, a frozen<address>, m map<int, text>, PRIMARY KEY (pk, ck)) " +
                                              "WITH CLUSTERING ORDER BY (ck DESC) AND comment = 'c' AND gc_grace_seconds = 3600 " +
                                              "AND compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '16'}"), false, apache);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.defaults (pk int PRIMARY KEY, v int)"), false, current);
            cluster.schemaChange(withKeyspace("CREATE INDEX tbl_v ON %s.tbl (v) USING 'sai'"), false, current);
            cluster.schemaChange(withKeyspace("CREATE FUNCTION %s.plus (a int, b int) CALLED ON NULL INPUT RETURNS int " +
                                              "LANGUAGE java AS 'return a == null || b == null ? null : a + b;'"), false, current);
            cluster.schemaChange(withKeyspace("CREATE AGGREGATE %s.total (int) SFUNC plus STYPE int INITCOND 0"), false, current);
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.defaults WITH comment = 'altered'"), false, apache);
            assertSchemaAgreement(cluster);
        }
    }
}
