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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ApacheUpgradePath;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Nodes moved from storage_compatibility_mode CASSANDRA_5 to UPGRADING, then NONE: the record of the last
 * compatibility mode, the schema version over Apache's schema columns while a node runs at messaging version 13, and
 * the schema changes and features refused on the Apache upgrade path; and a replacement started in CASSANDRA_5.
 */
public class ApacheUpgradePathTest extends TestBaseImpl
{
    @Test
    public void testRoll() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                       .set("user_defined_functions_enabled", true)
                                                                       .set("storage_compatibility_mode", "CASSANDRA_5"))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, a int, b int)"));
            cluster.schemaChange(withKeyspace("CREATE FUNCTION %s.f (a int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return a;'"));
            String or = withKeyspace("SELECT * FROM %s.tbl WHERE a = 1 OR b = 1 ALLOW FILTERING");
            String deterministic = withKeyspace("CREATE FUNCTION %s.g (a int) CALLED ON NULL INPUT RETURNS int DETERMINISTIC LANGUAGE java AS 'return a;'");

            // from its first schema load, the node in UPGRADING has the version over Apache's schema columns
            UUID modeVersion = cluster.get(1).schemaVersion();
            long mark = cluster.get(1).logs().mark();
            restart(cluster.get(1), "UPGRADING");
            assertThat(cluster.get(1).logs().grep(mark, "Schema updated: ").getResult().get(0)).contains("--> " + modeVersion);
            assertThat(record(cluster.get(1))).isEqualTo("CASSANDRA_5");
            awaitSchemaAgreement(cluster);
            assertThatThrownBy(() -> cluster.coordinator(1).execute(or, ALL)).hasMessageContaining("OR operator is not supported");
            assertThatThrownBy(() -> cluster.coordinator(1).execute(deterministic, ALL)).hasMessageContaining("DETERMINISTIC");

            // a node in CASSANDRA_5 restarts beside a node in UPGRADING after CASSANDRA_5
            restart(cluster.get(2), "CASSANDRA_5");
            awaitSchemaAgreement(cluster);
            UUID apacheColumnsVersion = cluster.get(1).schemaVersion();

            restart(cluster.get(2), "UPGRADING");
            awaitSchemaAgreement(cluster);
            assertThat(cluster.get(1).schemaVersion()).isNotEqualTo(apacheColumnsVersion);
            cluster.coordinator(1).execute(or, ALL);
            cluster.schemaChange(deterministic, false, cluster.get(1));
            for (IInvokableInstance instance : cluster)
                assertRows(instance.executeInternal(withKeyspace("SELECT deterministic FROM system_schema.functions WHERE keyspace_name = '%s' AND function_name = 'g'")), row(true));

            // an internal table keeps the compaction it was created with: Apache's default, or default_compaction if set
            String rolesCompaction = "SELECT compaction['class'] FROM system_schema.tables WHERE keyspace_name = 'system_auth' AND table_name = 'roles'";
            String created = cluster.get(1).callOnInstance(() -> DatabaseDescriptor.getDefaultCompaction() == null
                                                                 ? SizeTieredCompactionStrategy.class.getName()
                                                                 : CompactionParams.DEFAULT.klass().getName());
            assertRows(cluster.get(1).executeInternal(rolesCompaction), row(created));
            restart(cluster.get(1), "NONE");
            assertThat(record(cluster.get(1))).isNull();
            assertThat(record(cluster.get(2))).isEqualTo("CASSANDRA_5");
            awaitSchemaAgreement(cluster);
            assertRows(cluster.get(1).executeInternal(rolesCompaction), row(created));
            assertThat(cluster.get(1).callOnInstance(() -> Schema.instance.getTableMetadata("system_auth", "roles").params.compaction.klass().getName()))
            .isEqualTo(created);
        }
    }

    @Test
    public void testReplacementStartsInCassandra5Mode() throws Throwable
    {
        // the replacement, node 4, takes node 3's token
        TokenSupplier tokens = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = init(Cluster.build(3)
                                           .withTokenSupplier(node -> tokens.token(Math.min(node, 3)))
                                           .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                       .set("user_defined_functions_enabled", true)
                                                                       .set("storage_compatibility_mode", "CASSANDRA_5"))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            restart(cluster.get(3), "UPGRADING");

            // the dead node in UPGRADING is replaced by a node started in CASSANDRA_5
            stopUnchecked(cluster.get(3));
            IInvokableInstance replacement = replaceHostAndStart(cluster, cluster.get(3));
            await().atMost(1, TimeUnit.MINUTES).until(() -> replacement.schemaVersion().equals(cluster.get(1).schemaVersion()));
            assertThatThrownBy(() -> replacement.coordinator().execute(withKeyspace("CREATE FUNCTION %s.f (a int) CALLED ON NULL INPUT RETURNS int " +
                                                                                     "DETERMINISTIC LANGUAGE java AS 'return a;'"), ALL))
            .hasMessageContaining("DETERMINISTIC");
        }
    }

    @Test
    public void testUndrainedRestartInUpgrading() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                       .set("hints_flush_period", "100ms")
                                                                       .set("storage_compatibility_mode", "CASSANDRA_5"))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            restart(cluster.get(1), "UPGRADING");
            restart(cluster.get(2), "UPGRADING");

            // a hint for node 2, which is down, and a write node 1 hasn't flushed
            cluster.get(2).shutdown().get();
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (1, 1)"), ONE);
            Path hints = Paths.get(cluster.get(1).config().getString("hints_directory"));
            await().atMost(1, TimeUnit.MINUTES).until(() -> {
                try (Stream<Path> files = Files.list(hints))
                {
                    return files.anyMatch(file -> file.toString().endsWith(".hints"));
                }
            });
            assertThat(cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().isEmpty())).isTrue();

            cluster.get(1).shutdown(false).get();
            cluster.get(1).startup();
            assertRows(cluster.get(1).executeInternal(withKeyspace("SELECT v FROM %s.tbl WHERE pk = 1")), row(1));
            cluster.get(2).startup();
            await().atMost(1, TimeUnit.MINUTES).until(() -> cluster.get(2).executeInternal(withKeyspace("SELECT v FROM %s.tbl WHERE pk = 1")).length == 1);
            assertRows(cluster.get(2).executeInternal(withKeyspace("SELECT v FROM %s.tbl WHERE pk = 1")), row(1));
        }
    }

    private static void restart(IInvokableInstance instance, String mode) throws Exception
    {
        instance.shutdown().get();
        instance.config().set("storage_compatibility_mode", mode);
        instance.startup();
    }

    private static String record(IInvokableInstance instance) throws Exception
    {
        Path file = Paths.get(instance.config().getString("metadata_directory")).resolve(ApacheUpgradePath.FILE);
        return Files.exists(file) ? String.join("", Files.readAllLines(file)).trim() : null;
    }

    private static void awaitSchemaAgreement(Cluster cluster)
    {
        await().atMost(1, TimeUnit.MINUTES).until(() -> cluster.get(1).schemaVersion().equals(cluster.get(2).schemaVersion()));
    }
}
