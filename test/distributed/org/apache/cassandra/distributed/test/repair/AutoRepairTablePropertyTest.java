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

package org.apache.cassandra.distributed.test.repair;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Distributed tests that verify "auto_repair" column behavior in system_schema.tables.
 * <p>
 * This test verifies that:
 * 1. When auto-repair scheduling is enabled, the auto_repair column contains data
 * 2. When auto-repair scheduling is disabled, the auto_repair column does not contain data
 * <p>
 * This test uses a distributed cluster to ensure JVM properties are properly initialized
 * before the schema is loaded.
 */
public class AutoRepairTablePropertyTest extends TestBaseImpl
{
    private static Cluster cluster;

    @BeforeClass
    public static void init() throws IOException
    {
        // Ensure AUTOREPAIR_ENABLE is true so the auto_repair column exists in the schema
        CassandraRelevantProperties.AUTOREPAIR_ENABLE.setBoolean(true);

        // Configure a single-node cluster with auto_repair enabled
        cluster = Cluster.build(1)
                         .withConfig(config -> config
                                 .set("auto_repair",
                                      ImmutableMap.of(
                                              "repair_type_overrides",
                                              ImmutableMap.of(AutoRepairConfig.RepairType.FULL.getConfigName(),
                                                              ImmutableMap.of(
                                                                      "enabled", "true",
                                                                      "initial_scheduler_delay", "1h")))))
                         .start();
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testAutoRepairColumnExistsWhenEnabled()
    {
        // Verify the auto_repair column exists in system_schema.tables
        cluster.get(1).runOnInstance(() -> {
            ColumnFamilyStore tables =  Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME)
                                                .getColumnFamilyStore(SchemaKeyspaceTables.TABLES);

            ColumnMetadata autoRepairColumn = tables.metadata().getColumn(ByteBufferUtil.bytes("auto_repair"));

            // When AUTOREPAIR_ENABLE is true, the column should exist
            assertTrue("auto_repair column should exist when AUTOREPAIR_ENABLE is true",
                       autoRepairColumn != null);
        });
    }

    @Test
    public void testAutoRepairColumnDataWrittenWhenSchedulerEnabled()
    {
        // When auto-repair scheduling is enabled, verify data is written to the auto_repair column
        cluster.get(1).runOnInstance(() -> {
            // Enable scheduling
            DatabaseDescriptor.getAutoRepairConfig().setAutoRepairSchedulingEnabled(true);

            ColumnFamilyStore tables = Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME)
                                               .getColumnFamilyStore(SchemaKeyspaceTables.TABLES);

            ColumnMetadata autoRepairColumn = tables.metadata().getColumn(ByteBufferUtil.bytes("auto_repair"));

            SimpleBuilders.RowBuilder builder = new SimpleBuilders.RowBuilder(tables.metadata(), "table_name");
            SchemaKeyspace.addTableParamsToRowBuilder(tables.metadata().params, builder);
            Row row = builder.build();

            ColumnData data = row.getCell(autoRepairColumn);

            // When scheduling is enabled, data should be written
            assertTrue("auto_repair data should be written when scheduling is enabled", data != null);
        });
    }

    @Test
    public void testAutoRepairColumnDataNotWrittenWhenSchedulerDisabled()
    {
        // When auto-repair scheduling is disabled, verify no data is written to the auto_repair column
        cluster.get(1).runOnInstance(() -> {
            // Disable scheduling
            DatabaseDescriptor.getAutoRepairConfig().setAutoRepairSchedulingEnabled(false);

            ColumnFamilyStore tables = Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME)
                                               .getColumnFamilyStore(SchemaKeyspaceTables.TABLES);

            ColumnMetadata autoRepairColumn =
                    tables.metadata().getColumn(ByteBufferUtil.bytes("auto_repair"));

            SimpleBuilders.RowBuilder builder =
                    new SimpleBuilders.RowBuilder(tables.metadata(), "table_name");
            SchemaKeyspace.addTableParamsToRowBuilder(tables.metadata().params, builder);
            Row row = builder.build();

            ColumnData data = row.getCell(autoRepairColumn);

            // When scheduling is disabled, data should NOT be written
            assertFalse("auto_repair data should NOT be written when scheduling is disabled", data != null);
        });
    }

    @Test
    public void testAutoRepairColumnInViewsSchema()
    {
        // Verify the auto_repair column behavior in system_schema.views as well
        cluster.get(1).runOnInstance(() -> {
            ColumnFamilyStore views = Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME)
                                              .getColumnFamilyStore(SchemaKeyspaceTables.VIEWS);

            ColumnMetadata autoRepairColumn =  views.metadata().getColumn(ByteBufferUtil.bytes("auto_repair"));

            // When AUTOREPAIR_ENABLE is true, the column should exist in views table too
            assertTrue("auto_repair column should exist in views schema when AUTOREPAIR_ENABLE is true",
                       autoRepairColumn != null);
        });
    }

    @Test
    public void testAutoRepairKeysStoredInLowerCase()
    {
        cluster.get(1).runOnInstance(() -> DatabaseDescriptor.getAutoRepairConfig().setAutoRepairSchedulingEnabled(true));
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.upper_case_keys (k int PRIMARY KEY) WITH auto_repair = {'FULL_ENABLED': 'false', 'Priority': '7'}"));

        assertRows(cluster.coordinator(1).execute("SELECT auto_repair FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
                                                  ConsistencyLevel.ONE, KEYSPACE, "upper_case_keys"),
                   row(ImmutableMap.of("full_enabled", "false",
                                       "incremental_enabled", "true",
                                       "preview_repaired_enabled", "true",
                                       "priority", "7")));

        Object[][] describe = cluster.coordinator(1).execute(withKeyspace("DESCRIBE TABLE %s.upper_case_keys"), ConsistencyLevel.ONE);
        assertTrue((String) describe[0][3], ((String) describe[0][3]).contains("auto_repair = {'full_enabled': 'false', 'incremental_enabled': 'true', 'preview_repaired_enabled': 'true', 'priority': '7'}"));
    }

    @Test
    public void testStoredAutoRepairKeysInSeveralCases()
    {
        cluster.get(1).runOnInstance(() -> DatabaseDescriptor.getAutoRepairConfig().setAutoRepairSchedulingEnabled(true));
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.stored_keys (k int PRIMARY KEY)"));
        cluster.get(1).runOnInstance(() -> {
            QueryProcessor.executeInternal("UPDATE system_schema.tables SET auto_repair = {'FULL_ENABLED': 'false', 'Incremental_Enabled': 'maybe', 'Priority': '7', " +
                                           "'full_enabled': 'true', 'incremental_enabled': 'true', 'preview_repaired_enabled': 'true', 'priority': '2'} " +
                                           "WHERE keyspace_name = ? AND table_name = ?", KEYSPACE, "stored_keys");
            Schema.instance.reloadSchemaAndAnnounceVersion();
            assertEquals(ImmutableMap.of("full_enabled", "true",
                                         "incremental_enabled", "true",
                                         "preview_repaired_enabled", "true",
                                         "priority", "2"),
                         Keyspace.open(KEYSPACE).getColumnFamilyStore("stored_keys").metadata().params.autoRepair.options());
        });

        cluster.schemaChange(withKeyspace("ALTER TABLE %s.stored_keys WITH comment = 'rewritten'"));
        assertRows(cluster.coordinator(1).execute("SELECT auto_repair FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
                                                  ConsistencyLevel.ONE, KEYSPACE, "stored_keys"),
                   row(ImmutableMap.of("full_enabled", "true",
                                       "incremental_enabled", "true",
                                       "preview_repaired_enabled", "true",
                                       "priority", "2")));
    }
}
