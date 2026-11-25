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
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.ApacheUpgradePath;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Schema exchange and repair between nodes in storage_compatibility_mode CASSANDRA_5, at messaging version 13, and
 * nodes in UPGRADING on the Apache upgrade path.
 */
public class ApacheUpgradePathBridgeTest extends TestBaseImpl
{
    @Test
    public void testSchemaExchange() throws Throwable
    {
        try (Cluster cluster = start("CASSANDRA_5", "UPGRADING", "UPGRADING"))
        {
            // pushed: from node 2 to the mode node, and from the mode node to the nodes in UPGRADING
            IMessageFilters.Filter noPulls = cluster.filters().verbs(Verb.SCHEMA_PULL_REQ.id).drop();
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"), false, cluster.get(2));
            cluster.schemaChange(withKeyspace("CREATE FUNCTION %s.plus (a int, b int) CALLED ON NULL INPUT RETURNS int " +
                                              "LANGUAGE java AS 'return a == null || b == null ? null : a + b;'"), false, cluster.get(2));
            cluster.schemaChange(withKeyspace("CREATE AGGREGATE %s.total (int) SFUNC plus STYPE int INITCOND 0"), false, cluster.get(2));
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl WITH comment = 'pushed'"), false, cluster.get(1));
            noPulls.off();

            // pulled by the mode node from a node in UPGRADING
            cluster.get(1).shutdown().get();
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl WITH comment = 'pulled from UPGRADING'"), true, cluster.get(2));
            cluster.get(1).startup();
            awaitSchemaAgreement(cluster);
            assertRows(cluster.get(1).executeInternal(withKeyspace("SELECT aggregate_name FROM system_schema.aggregates WHERE keyspace_name = '%s'")), row("total"));
            assertRows(cluster.get(1).executeInternal(withKeyspace("SELECT comment FROM system_schema.tables WHERE keyspace_name = '%s'")), row("pulled from UPGRADING"));

            // pulled by the nodes in UPGRADING from the mode node
            cluster.get(2).shutdown().get();
            cluster.get(3).shutdown().get();
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl WITH comment = 'pulled from CASSANDRA_5'"), true, cluster.get(1));
            cluster.get(2).startup();
            cluster.get(3).startup();
            awaitSchemaAgreement(cluster);
            for (IInvokableInstance instance : cluster)
                assertRows(instance.executeInternal(withKeyspace("SELECT comment FROM system_schema.tables WHERE keyspace_name = '%s'")), row("pulled from CASSANDRA_5"));
        }
    }

    @Test
    public void testRepair() throws Throwable
    {
        try (Cluster cluster = start("CASSANDRA_5", "UPGRADING", "UPGRADING"))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            String insert = withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)");
            String select = withKeyspace("SELECT pk, v FROM %s.tbl WHERE pk = ?");

            cluster.get(1).executeInternal(insert, 1, 1);
            cluster.get(1).nodetoolResult("repair", "-full", KEYSPACE).asserts().success();
            cluster.get(2).executeInternal(insert, 2, 2);
            cluster.get(2).nodetoolResult("repair", KEYSPACE).asserts().success();
            cluster.get(3).executeInternal(insert, 3, 3);
            cluster.get(3).nodetoolResult("repair", "-full", "-hosts", "127.0.0.2", "-hosts", "127.0.0.3", KEYSPACE).asserts().success();

            for (IInvokableInstance instance : cluster)
            {
                assertRows(instance.executeInternal(select, 1), row(1, 1));
                assertRows(instance.executeInternal(select, 2), row(2, 2));
            }
            assertRows(cluster.get(2).executeInternal(select, 3), row(3, 3));
        }
    }

    @Test
    public void testRepairRefusedFromUnidentifiedPeer() throws Throwable
    {
        try (Cluster cluster = start("CASSANDRA_5", "UPGRADING"))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));

            // the mode node takes repair preparation from a node above 13 only if it gossips UPGRADING after CASSANDRA_5
            gossipModes(cluster.get(1), cluster.get(2), "mode=NONE;compat=none", 1_000_000);
            long mark = cluster.get(1).logs().mark();
            cluster.get(2).nodetoolResult("repair", "-full", KEYSPACE).asserts().failure();
            assertThat(cluster.get(1).logs().grep(mark, "Rejecting repair .* runs messaging version \\d+ and isn't identified as a node on the Apache upgrade path").getResult()).isNotEmpty();

            gossipModes(cluster.get(1), cluster.get(2), "mode=UPGRADING;compat=CASSANDRA_5", 1_000_001);
            cluster.get(2).nodetoolResult("repair", "-full", KEYSPACE).asserts().success();
        }
    }

    /**
     * Makes {@code observer} see {@code member} gossip {@code modes}, at a version above the member's own.
     */
    private static void gossipModes(IInvokableInstance observer, IInvokableInstance member, String modes, int version)
    {
        InetSocketAddress address = member.broadcastAddress();
        observer.runOnInstance(() -> Gossiper.instance.injectApplicationState(InetAddressAndPort.getByAddress(address),
                                                                              ApacheUpgradePath.GOSSIP_STATE,
                                                                              VersionedValue.unsafeMakeVersionedValue(modes, version)));
    }

    /**
     * Starts a cluster in CASSANDRA_5 and moves node {@code n} to {@code modes[n - 1]} by a restart.
     */
    private static Cluster start(String... modes) throws Exception
    {
        Cluster cluster = Cluster.build(modes.length)
                                 .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                             .set("user_defined_functions_enabled", true)
                                                             .set("storage_compatibility_mode", "CASSANDRA_5"))
                                 .start();
        for (IInvokableInstance instance : cluster)
        {
            String mode = modes[instance.config().num() - 1];
            if (!mode.equals("CASSANDRA_5"))
            {
                instance.shutdown().get();
                instance.config().set("storage_compatibility_mode", mode);
                instance.startup();
            }
        }
        return init(cluster);
    }

    private static void awaitSchemaAgreement(Cluster cluster)
    {
        await().atMost(1, TimeUnit.MINUTES)
               .until(() -> cluster.stream().map(IInvokableInstance::schemaVersion).distinct().count() == 1);
    }
}
