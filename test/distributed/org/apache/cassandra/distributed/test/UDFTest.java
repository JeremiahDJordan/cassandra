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

package org.apache.cassandra.distributed.test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.awaitility.Awaitility;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static java.lang.Boolean.TRUE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class UDFTest extends TestBaseImpl
{
    // the regular columns of Cassandra 4.1's and 5.0's system_schema.functions and aggregates
    private static final Set<String> CASSANDRA_FUNCTIONS_COLUMNS = ImmutableSet.of("argument_names", "body", "language", "return_type", "called_on_null_input");
    private static final Set<String> CASSANDRA_AGGREGATES_COLUMNS = ImmutableSet.of("final_func", "initcond", "return_type", "state_func", "state_type");

    @Test
    public void testUDFContextKeyspaceNotNull() throws Throwable
    {
        /*
        To test the keyspace is not null in the context we call UDFContext.newUDTValue() because it
        relies on the keyspace not being null.
         */
        String[] createStmts = {
        "CREATE TABLE " + KEYSPACE + ".current (city text, PRIMARY KEY (city))",
        "CREATE TYPE IF NOT EXISTS " + KEYSPACE + ".aggst (lt int, ge int)",
        "CREATE FUNCTION " + KEYSPACE + ".udf_not_null ()\n" +
        "CALLED ON NULL INPUT\n" +
        "RETURNS boolean LANGUAGE java AS $$\n" +
        "udfContext.newUDTValue(\"aggst\");\n" +
        "return Boolean.TRUE;\n" +
        "$$;",
        };

        try (Cluster cluster = init(Cluster.create(1, config -> config.set("enable_user_defined_functions", "true"))))
        {
            for (String stmt : createStmts)
                cluster.schemaChange(stmt);

            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".current (city) VALUES ('Helsinki')", ALL);

            assertRows(cluster.coordinator(1).execute("SELECT " + KEYSPACE + ".udf_not_null() AS m FROM " + KEYSPACE + ".current", ALL), row(TRUE));

            cluster.get(1).shutdown().get();
            cluster.get(1).startup();

            assertRows(cluster.coordinator(1).execute("SELECT " + KEYSPACE + ".udf_not_null() AS m FROM " + KEYSPACE + ".current", ALL), row(TRUE));
        }
    }

    @Test
    public void testFunctionsSchemaInCassandra4Mode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(1, config -> config.set("enable_user_defined_functions", "true")
                                                                      .set("sstable", Map.of("selected_format", "big"))
                                                                      .set("storage_compatibility_mode", StorageCompatibilityMode.CASSANDRA_4.toString()))))
        {
            createFunctionAndAggregate(cluster, "");
            assertThatThrownBy(() -> cluster.schemaChange("CREATE FUNCTION " + KEYSPACE + ".f_deterministic (input int) " +
                                                          "CALLED ON NULL INPUT RETURNS int DETERMINISTIC LANGUAGE java AS 'return input;'"))
            .hasMessageContaining("DETERMINISTIC and MONOTONIC functions are not supported while storage_compatibility_mode is CASSANDRA_4");
            assertThatThrownBy(() -> cluster.schemaChange("CREATE AGGREGATE " + KEYSPACE + ".a_deterministic (int) " +
                                                          "SFUNC f_sum STYPE int INITCOND 0 DETERMINISTIC"))
            .hasMessageContaining("DETERMINISTIC aggregates are not supported while storage_compatibility_mode is CASSANDRA_4");

            assertSchemaTableColumns(cluster);
            cluster.get(1).flush(SchemaConstants.SCHEMA_KEYSPACE_NAME);
            assertThat(sstableColumns(cluster.get(1), SchemaKeyspaceTables.FUNCTIONS)).isEqualTo(CASSANDRA_FUNCTIONS_COLUMNS);
            assertThat(sstableColumns(cluster.get(1), SchemaKeyspaceTables.AGGREGATES)).isEqualTo(CASSANDRA_AGGREGATES_COLUMNS);

            cluster.get(1).shutdown().get();
            cluster.get(1).startup();

            assertFunctionAndAggregate(cluster);
        }
    }

    @Test
    public void testDeterministicFunctionsAfterSwitchToCassandra4Mode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(1, config -> config.set("enable_user_defined_functions", "true")
                                                                      .set("sstable", Map.of("selected_format", "big"))
                                                                      .set("storage_compatibility_mode", StorageCompatibilityMode.HCD_1.toString()))))
        {
            createFunctionAndAggregate(cluster, "DETERMINISTIC");
            cluster.get(1).flush(SchemaConstants.SCHEMA_KEYSPACE_NAME);
            assertThat(sstableColumns(cluster.get(1), SchemaKeyspaceTables.FUNCTIONS)).contains("deterministic", "monotonic", "monotonic_on");
            assertThat(sstableColumns(cluster.get(1), SchemaKeyspaceTables.AGGREGATES)).contains("deterministic");

            // f_monotonic is only in the commit log
            int sstables = sstableCount(cluster.get(1), SchemaKeyspaceTables.FUNCTIONS);
            CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);
            try
            {
                cluster.schemaChange("CREATE FUNCTION " + KEYSPACE + ".f_monotonic (input int) " +
                                     "CALLED ON NULL INPUT RETURNS int DETERMINISTIC MONOTONIC LANGUAGE java AS 'return input;'");
                assertThat(sstableCount(cluster.get(1), SchemaKeyspaceTables.FUNCTIONS)).isEqualTo(sstables);
                cluster.get(1).shutdown().get();
            }
            finally
            {
                CassandraRelevantProperties.UNSAFE_SYSTEM.reset();
            }
            cluster.get(1).config().set("storage_compatibility_mode", StorageCompatibilityMode.CASSANDRA_4.toString());
            cluster.get(1).startup();

            // the schema is loaded before the commit log is replayed
            assertRows(cluster.coordinator(1).execute("SELECT function_name FROM system_schema.functions WHERE keyspace_name = ? AND function_name = 'f_monotonic'", ALL, KEYSPACE),
                       row("f_monotonic"));
            restartInMode(cluster, StorageCompatibilityMode.CASSANDRA_4);

            assertFunctionAndAggregate(cluster);
            assertRows(cluster.coordinator(1).execute("SELECT " + KEYSPACE + ".f_monotonic(v) FROM " + KEYSPACE + ".t WHERE k = 1", ALL), row(2));
            assertSchemaTableColumns(cluster);
            assertThat(describe(cluster, "FUNCTION " + KEYSPACE + ".f_monotonic")).doesNotContain("DETERMINISTIC", "MONOTONIC");
            assertThat(describe(cluster, "AGGREGATE " + KEYSPACE + ".a_sum")).doesNotContain("DETERMINISTIC");

            cluster.get(1).flush(SchemaConstants.SCHEMA_KEYSPACE_NAME);
            cluster.get(1).forceCompact(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.FUNCTIONS);
            cluster.get(1).forceCompact(SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.AGGREGATES);
            assertThat(sstableColumns(cluster.get(1), SchemaKeyspaceTables.FUNCTIONS)).isEqualTo(CASSANDRA_FUNCTIONS_COLUMNS);
            assertThat(sstableColumns(cluster.get(1), SchemaKeyspaceTables.AGGREGATES)).isEqualTo(CASSANDRA_AGGREGATES_COLUMNS);
        }
    }

    @Test
    public void testReplacedFunctionsAfterSwitchFromCassandra4Mode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(1, config -> config.set("enable_user_defined_functions", "true")
                                                                      .set("sstable", Map.of("selected_format", "big"))
                                                                      .set("storage_compatibility_mode", StorageCompatibilityMode.HCD_1.toString()))))
        {
            createFunctionAndAggregate(cluster, "DETERMINISTIC");
            cluster.get(1).flush(SchemaConstants.SCHEMA_KEYSPACE_NAME);

            restartInMode(cluster, StorageCompatibilityMode.CASSANDRA_4);
            cluster.schemaChange("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".f_sum (state int, input int) " +
                                 "CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return input + state;'");
            cluster.schemaChange("CREATE OR REPLACE AGGREGATE " + KEYSPACE + ".a_sum (int) SFUNC f_sum STYPE int INITCOND 1");

            // the sstables with the earlier definitions are not compacted
            restartInMode(cluster, StorageCompatibilityMode.UPGRADING);
            assertRows(cluster.coordinator(1).execute("SELECT " + KEYSPACE + ".a_sum(v) FROM " + KEYSPACE + ".t", ALL), row(6));
            assertRows(cluster.coordinator(1).execute("SELECT deterministic, monotonic, monotonic_on FROM system_schema.functions WHERE keyspace_name = ? AND function_name = 'f_sum'", ALL, KEYSPACE),
                       row(null, null, null));
            assertRows(cluster.coordinator(1).execute("SELECT deterministic FROM system_schema.aggregates WHERE keyspace_name = ? AND aggregate_name = 'a_sum'", ALL, KEYSPACE),
                       row((Object) null));
        }
    }

    @Test
    public void testFunctionsFromNodeInOtherMode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(Feature.GOSSIP, Feature.NETWORK)
                                                                       .set("enable_user_defined_functions", "true")
                                                                       .set("sstable", Map.of("selected_format", "big"))
                                                                       .set("storage_compatibility_mode", config.num() == 1
                                                                                                          ? StorageCompatibilityMode.CASSANDRA_4.toString()
                                                                                                          : StorageCompatibilityMode.UPGRADING.toString()))
                                           .start()))
        {
            // the nodes don't agree on the schema version, so the change isn't made with schemaChange
            cluster.coordinator(2).execute("CREATE FUNCTION " + KEYSPACE + ".f_upgrading (input int) " +
                                           "CALLED ON NULL INPUT RETURNS int DETERMINISTIC LANGUAGE java AS 'return input;'", ALL);
            Awaitility.await().atMost(Duration.ofMinutes(1))
                      .until(() -> cluster.get(1).callOnInstance(() -> Schema.instance.getKeyspaceMetadata(KEYSPACE).userFunctions.size() == 1));

            cluster.get(1).flush(SchemaConstants.SCHEMA_KEYSPACE_NAME);
            assertThat(sstableColumns(cluster.get(1), SchemaKeyspaceTables.FUNCTIONS)).isEqualTo(CASSANDRA_FUNCTIONS_COLUMNS);
            assertThat(describe(cluster, "FUNCTION " + KEYSPACE + ".f_upgrading")).doesNotContain("DETERMINISTIC");
        }
    }

    private static void restartInMode(Cluster cluster, StorageCompatibilityMode mode) throws Exception
    {
        cluster.get(1).shutdown().get();
        cluster.get(1).config().set("storage_compatibility_mode", mode.toString());
        cluster.get(1).startup();
    }

    private static String describe(Cluster cluster, String element)
    {
        return (String) cluster.coordinator(1).execute("DESCRIBE " + element, ALL)[0][3];
    }

    private static void createFunctionAndAggregate(Cluster cluster, String options)
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v int)");
        cluster.schemaChange("CREATE FUNCTION " + KEYSPACE + ".f_sum (state int, input int) " +
                             "CALLED ON NULL INPUT RETURNS int " + options + " LANGUAGE java AS 'return state + input;'");
        cluster.schemaChange("CREATE AGGREGATE " + KEYSPACE + ".a_sum (int) SFUNC f_sum STYPE int INITCOND 0 " + options);
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t (k, v) VALUES (1, 2)", ALL);
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t (k, v) VALUES (2, 3)", ALL);
    }

    private static void assertFunctionAndAggregate(Cluster cluster)
    {
        assertRows(cluster.coordinator(1).execute("SELECT " + KEYSPACE + ".f_sum(v, v) FROM " + KEYSPACE + ".t WHERE k = 1", ALL), row(4));
        assertRows(cluster.coordinator(1).execute("SELECT " + KEYSPACE + ".a_sum(v) FROM " + KEYSPACE + ".t", ALL), row(5));
    }

    private static void assertSchemaTableColumns(Cluster cluster)
    {
        assertThat(cluster.coordinator(1).executeWithResult("SELECT * FROM system_schema.functions", ALL).names())
        .containsExactlyInAnyOrder("keyspace_name", "function_name", "argument_types", "argument_names", "body", "language", "return_type", "called_on_null_input");
        assertThat(cluster.coordinator(1).executeWithResult("SELECT * FROM system_schema.aggregates", ALL).names())
        .containsExactlyInAnyOrder("keyspace_name", "aggregate_name", "argument_types", "final_func", "initcond", "return_type", "state_func", "state_type");
    }

    private static int sstableCount(IInvokableInstance instance, String schemaTable)
    {
        return instance.callOnInstance(() -> Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME).getColumnFamilyStore(schemaTable).getLiveSSTables().size());
    }

    private static Set<String> sstableColumns(IInvokableInstance instance, String schemaTable)
    {
        return instance.callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME).getColumnFamilyStore(schemaTable);
            Set<String> columns = new HashSet<>();
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                for (ColumnMetadata column : sstable.header.columns())
                    columns.add(column.name.toString());
            }
            return columns;
        });
    }
}
