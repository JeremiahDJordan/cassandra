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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.APACHE_UPGRADE_PATH;
import static org.apache.cassandra.distributed.impl.AbstractCluster.CURRENT_VERSION;
import static org.awaitility.Awaitility.await;

/**
 * A node of this build outside the Apache-compatible storage mode, with the Apache upgrade path active, has the same
 * schema version as an Apache node, for the internal keyspaces each node defines itself and for schema changes made
 * through either node.
 */
public class ApacheUpgradePathSchemaTest extends ApacheMixedClusterTestBase
{
    @Test
    public void testSchemaVersionsAgree() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(APACHE_UPGRADE_PATH, "active");
             UpgradeableCluster cluster = startCluster(2,
                                                       config -> {
                                                           config.set("user_defined_functions_enabled", true);
                                                           if (config.num() == 2)
                                                               config.set("storage_compatibility_mode", "UPGRADING");
                                                       },
                                                       apacheVersion(), CURRENT_VERSION))
        {
            IUpgradeableInstance apache = cluster.get(1);
            IUpgradeableInstance current = cluster.get(2);
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

    /**
     * Waits until every node reports, over the native protocol, its own schema version and every peer's in
     * system.local and system.peers, all of them equal.
     */
    private static void assertSchemaAgreement(UpgradeableCluster cluster)
    {
        List<com.datastax.driver.core.Cluster> drivers = new ArrayList<>();
        try
        {
            List<Session> sessions = new ArrayList<>();
            cluster.stream().forEach(instance -> {
                InetSocketAddress address = new InetSocketAddress(instance.broadcastAddress().getAddress(), 9042);
                com.datastax.driver.core.Cluster driver = com.datastax.driver.core.Cluster.builder()
                                                                                          .addContactPointsWithPorts(address)
                                                                                          .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), Collections.singletonList(address)))
                                                                                          .build();
                drivers.add(driver);
                sessions.add(driver.connect());
            });

            await().atMost(1, TimeUnit.MINUTES).until(() -> {
                List<UUID> versions = new ArrayList<>();
                for (Session session : sessions)
                {
                    versions.add(session.execute("SELECT schema_version FROM system.local").one().getUUID("schema_version"));
                    List<Row> peers = session.execute("SELECT schema_version FROM system.peers").all();
                    if (peers.size() != cluster.size() - 1)
                        return false;
                    peers.forEach(peer -> versions.add(peer.getUUID("schema_version")));
                }
                return !versions.contains(null) && new HashSet<>(versions).size() == 1;
            });
        }
        finally
        {
            drivers.forEach(com.datastax.driver.core.Cluster::close);
        }
    }
}
