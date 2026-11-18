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

import java.net.InetSocketAddress;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_20;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_21;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A rolling upgrade to 5.0.10: nodes on {@link MessagingService#VERSION_DS_21} and a node on
 * {@link MessagingService#VERSION_DS_20} work together.
 */
public class DataStax21MessagingVersionTest extends TestBaseImpl
{
    @Test
    public void testMixedVersions() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withInstanceInitializer(SAIUtil.pinMessagingVersion(VERSION_DS_20, 3))
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start()))
        {
            assertThat(currentVersion(cluster, 1)).isEqualTo(VERSION_DS_21);
            assertThat(currentVersion(cluster, 3)).isEqualTo(VERSION_DS_20);

            // schema changes from either version
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, PRIMARY KEY (k, c))"),
                                 false, cluster.get(1));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.counters (k int PRIMARY KEY, n counter)"),
                                 false, cluster.get(3));
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.t ADD w int"), false, cluster.get(3));

            // a node on DS 21 knows which of the others are on DS 21
            assertThat(peerVersion(cluster, 1, 2)).isEqualTo(VERSION_DS_21);
            assertThat(peerVersion(cluster, 1, 3)).isEqualTo(VERSION_DS_20);

            for (int coordinator = 1; coordinator <= 3; coordinator++)
            {
                cluster.coordinator(coordinator)
                       .execute(withKeyspace("INSERT INTO %s.t (k, c, v, w) VALUES (?, 0, 1, 1)"),
                                ConsistencyLevel.ALL, coordinator);
                cluster.coordinator(coordinator).execute("BEGIN BATCH " +
                                                         "INSERT INTO " + KEYSPACE + ".t (k, c, v) VALUES (?, 1, 2); " +
                                                         "INSERT INTO " + KEYSPACE + ".t (k, c, v) VALUES (?, 2, 3); " +
                                                         "APPLY BATCH",
                                                         ConsistencyLevel.ALL, coordinator, coordinator + 10);
                cluster.coordinator(coordinator)
                       .execute(withKeyspace("UPDATE %s.t SET v = 5 WHERE k = ? AND c = 0 IF v = 1"),
                                ConsistencyLevel.QUORUM, coordinator);
                cluster.coordinator(coordinator)
                       .execute(withKeyspace("UPDATE %s.counters SET n = n + 1 WHERE k = 0"), ConsistencyLevel.ALL);
            }

            for (int coordinator = 1; coordinator <= 3; coordinator++)
            {
                for (int k = 1; k <= 3; k++)
                {
                    for (ConsistencyLevel consistency : new ConsistencyLevel[]{ ConsistencyLevel.QUORUM,
                                                                                ConsistencyLevel.ALL,
                                                                                ConsistencyLevel.SERIAL })
                    {
                        assertRows(cluster.coordinator(coordinator)
                                          .execute(withKeyspace("SELECT c, v FROM %s.t WHERE k = ?"), consistency, k),
                                   row(0, 5), row(1, 2));
                    }
                    assertRows(cluster.coordinator(coordinator)
                                      .execute(withKeyspace("SELECT c, v FROM %s.t WHERE k = ?"),
                                               ConsistencyLevel.ALL, k + 10),
                               row(2, 3));
                }
                assertThat(cluster.coordinator(coordinator).execute(withKeyspace("SELECT * FROM %s.t"),
                                                                    ConsistencyLevel.ALL).length).isEqualTo(9);
                assertThat(cluster.coordinator(coordinator).executeWithPaging(withKeyspace("SELECT * FROM %s.t"),
                                                                              ConsistencyLevel.ALL, 2))
                .toIterable()
                .hasSize(9);
                assertRows(cluster.coordinator(coordinator)
                                  .execute(withKeyspace("SELECT n FROM %s.counters WHERE k = 0"), ConsistencyLevel.ALL),
                           row(3L));
            }

            // incremental and full repair, coordinated by either version, streaming rows that only one node has
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (100, 0, 1)"));
            cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (101, 0, 1)"));
            cluster.forEach(node -> node.flush(KEYSPACE));
            cluster.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
            cluster.get(3).nodetoolResult("repair", "--full", KEYSPACE).asserts().success();
            for (int node = 1; node <= 3; node++)
            {
                for (int k : new int[]{ 100, 101 })
                    assertRows(cluster.get(node).executeInternal(withKeyspace("SELECT c, v FROM %s.t WHERE k = ?"), k),
                               row(0, 1));
            }
        }
    }

    /**
     * A node on DS 21 forwards a write to the other node of its datacenter as it received it, cells of a column it has
     * dropped included.
     */
    @Test
    public void testForwardedWriteWithDroppedColumn() throws Throwable
    {
        try (Cluster cluster = Cluster.build(4)
                                      .withNodeIdTopology(networkTopology(4, node -> node <= 2 ? dcAndRack("dc1", "rack1")
                                                                                               : dcAndRack("dc2", "rack1")))
                                      .withConfig(config -> config.with(NETWORK))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = " +
                                 "{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}");
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, w int, PRIMARY KEY (k, c))"));
            for (int node = 3; node <= 4; node++)
                cluster.get(node).schemaChangeInternal(withKeyspace("ALTER TABLE %s.t DROP w"));

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, c, v, w) VALUES (0, 0, 1, 1) USING TIMESTAMP 1"),
                                           ConsistencyLevel.ALL);
            for (int node = 1; node <= 4; node++)
                assertRows(cluster.get(node).executeInternal(withKeyspace("SELECT k, c, v FROM %s.t")), row(0, 0, 1));
        }
    }

    private static int currentVersion(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> MessagingService.current_version);
    }

    private static int peerVersion(Cluster cluster, int node, int peer)
    {
        InetSocketAddress address = cluster.get(peer).broadcastAddress();
        return cluster.get(node).callOnInstance(() -> MessagingService.instance().versions.getRaw(InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort())));
    }
}
