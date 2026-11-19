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

import java.io.IOException;
import java.util.UUID;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.functions.UserFunction;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.security.UDFSecurityMechanism;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.config.CassandraRelevantProperties.UDF_SECURITY_MECHANISM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * UDF validation on the coordinator, and schema loading on nodes that receive the schema (pushed or pulled) and after
 * a restart.
 */
public class UDFSchemaLoadTest extends TestBaseImpl
{
    // Accepted by the Security Manager sandbox, rejected by the bytecode sandbox
    private static final String BYTECODE_REJECTED_BODY = "return (double) Object.class.getModule().getDescriptor().packages().size();";
    private static final String NOT_LOADED = "hasn't been loaded successfully";

    @Test
    public void testNodeWithUdfsDisabledAppliesPushedFunction() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.set("user_defined_functions_enabled", c.num() == 1))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v double)"));
            cluster.schemaChange(withKeyspace("CREATE FUNCTION %s.f(v double) CALLED ON NULL INPUT RETURNS double " +
                                              "LANGUAGE java AS 'return v;'"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t2 (k int PRIMARY KEY)"));

            assertRows(cluster.get(2).executeInternal("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?", KEYSPACE, "t2"), row("t2"));
            assertTrue(isBroken(cluster.get(2), "f"));
            assertFalse(isBroken(cluster.get(1), "f"));
            assertEquals(schemaVersion(cluster.get(1)), schemaVersion(cluster.get(2)));
        }
    }

    @Test
    public void testCoordinatorRejectsFunctionItCannotCompile() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.set("user_defined_functions_enabled", c.num() == 1))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v double)"));
            assertThatThrownBy(() -> cluster.coordinator(2).execute(withKeyspace("CREATE FUNCTION %s.f(v double) CALLED ON NULL INPUT " +
                                                                                 "RETURNS double LANGUAGE java AS 'return v;'"), ALL))
            .hasMessageContaining("User-defined functions are disabled");
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t2 (k int PRIMARY KEY)"), false, cluster.get(2));

            for (IInvokableInstance node : new IInvokableInstance[]{ cluster.get(1), cluster.get(2) })
            {
                assertRows(node.executeInternal("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?", KEYSPACE, "t2"), row("t2"));
                assertRows(node.executeInternal("SELECT function_name FROM system_schema.functions WHERE keyspace_name = ? AND function_name = ?", KEYSPACE, "f"));
            }
        }
    }

    @Test
    public void testNodeWithBytecodeSandboxAppliesPushedFunction() throws Exception
    {
        assumeSecurityManagerSandbox();
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.set("user_defined_functions_enabled", true))
                                           .start()))
        {
            restartWithBytecodeSandbox(cluster.get(2));
            assertEquals(UDFSecurityMechanism.SECURITY_MANAGER.name(), mechanism(cluster.get(1)));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v double)"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (1, 1)"), ALL);
            cluster.schemaChange(withKeyspace("CREATE FUNCTION %s.f(v double) CALLED ON NULL INPUT RETURNS double " +
                                              "LANGUAGE java AS '" + BYTECODE_REJECTED_BODY + "'"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t2 (k int PRIMARY KEY)"));

            assertRows(cluster.get(2).executeInternal("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?", KEYSPACE, "t2"), row("t2"));
            assertFalse(isBroken(cluster.get(1), "f"));
            assertTrue(isBroken(cluster.get(2), "f"));
            cluster.coordinator(1).execute(withKeyspace("SELECT %1$s.f(v) FROM %1$s.t WHERE k = 1"), ALL);
            assertThatThrownBy(() -> cluster.coordinator(2).execute(withKeyspace("SELECT %1$s.f(v) FROM %1$s.t WHERE k = 1"), ALL))
            .hasMessageContaining(NOT_LOADED)
            .hasMessageContaining("Java UDF validation failed");

            // The coordinator's own sandbox validates CREATE FUNCTION
            assertThatThrownBy(() -> cluster.coordinator(2).execute(withKeyspace("CREATE FUNCTION %s.g(v double) CALLED ON NULL INPUT " +
                                                                                 "RETURNS double LANGUAGE java AS '" + BYTECODE_REJECTED_BODY + "'"), ALL))
            .hasMessageContaining("Java UDF validation failed");

            // An aggregate over a function that is broken on node 2 can be created
            cluster.schemaChange(withKeyspace("CREATE FUNCTION %s.state(s double, v double) CALLED ON NULL INPUT RETURNS double " +
                                              "LANGUAGE java AS 'return s + v + Object.class.getModule().getDescriptor().packages().size();'"));
            cluster.schemaChange(withKeyspace("CREATE AGGREGATE %s.agg(double) SFUNC state STYPE double INITCOND 0"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t3 (k int PRIMARY KEY)"));
            assertRows(cluster.get(2).executeInternal("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?", KEYSPACE, "t3"), row("t3"));
            assertTrue(isBroken(cluster.get(2), "state"));

            // A broken function can be dropped from the node where it is broken
            cluster.schemaChange(withKeyspace("DROP AGGREGATE %s.agg"), false, cluster.get(2));
            cluster.schemaChange(withKeyspace("DROP FUNCTION %s.f"), false, cluster.get(2));
            assertRows(cluster.get(1).executeInternal("SELECT function_name FROM system_schema.functions WHERE keyspace_name = ? AND function_name = ?", KEYSPACE, "f"));
            assertRows(cluster.get(2).executeInternal("SELECT function_name FROM system_schema.functions WHERE keyspace_name = ? AND function_name = ?", KEYSPACE, "f"));
        }
    }

    @Test
    public void testNodeWithBytecodeSandboxPullsFunction() throws Exception
    {
        assumeSecurityManagerSandbox();
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.set("user_defined_functions_enabled", true).with(GOSSIP))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v double)"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (1, 2)"), ALL);
            cluster.get(2).shutdown().get();
            createFunctions(cluster);

            // node 2 was down for the schema changes, so it pulls them when it starts
            startWithBytecodeSandbox(cluster.get(2));
            await().atMost(1, MINUTES).until(() -> schemaVersion(cluster.get(1)).equals(schemaVersion(cluster.get(2))));
            assertRejectedFunctionBroken(cluster, 2);
            assertFalse(isBroken(cluster.get(1), "rejected"));
        }
    }

    @Test
    public void testRestartWithBytecodeSandbox() throws Exception
    {
        assumeSecurityManagerSandbox();
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.set("user_defined_functions_enabled", true))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v double)"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.t (k, v) VALUES (1, 2)"));
            createFunctions(cluster);

            restartWithBytecodeSandbox(cluster.get(1));
            assertRejectedFunctionBroken(cluster, 1);
        }
    }

    // BYTECODE_REJECTED_BODY can only be created on a node with the Security Manager sandbox
    private static void assumeSecurityManagerSandbox()
    {
        Assume.assumeTrue(UDFSecurityMechanism.current() == UDFSecurityMechanism.SECURITY_MANAGER);
    }

    private static void createFunctions(Cluster cluster)
    {
        cluster.schemaChangeIgnoringStoppedInstances(withKeyspace("CREATE FUNCTION %s.rejected(v double) CALLED ON NULL INPUT RETURNS double " +
                                                                  "LANGUAGE java AS '" + BYTECODE_REJECTED_BODY + "'"));
        cluster.schemaChangeIgnoringStoppedInstances(withKeyspace("CREATE FUNCTION %s.accepted(v double) CALLED ON NULL INPUT RETURNS double " +
                                                                  "LANGUAGE java AS 'return v * 2;'"));
    }

    private static void restartWithBytecodeSandbox(IInvokableInstance node) throws Exception
    {
        node.shutdown().get();
        startWithBytecodeSandbox(node);
    }

    private static void startWithBytecodeSandbox(IInvokableInstance node)
    {
        try (WithProperties properties = new WithProperties().set(UDF_SECURITY_MECHANISM, UDFSecurityMechanism.BYTECODE.value))
        {
            node.startup();
        }
        assertEquals(UDFSecurityMechanism.BYTECODE.name(), mechanism(node));
    }

    private static void assertRejectedFunctionBroken(Cluster cluster, int node)
    {
        assertTrue(isBroken(cluster.get(node), "rejected"));
        assertFalse(isBroken(cluster.get(node), "accepted"));
        assertRows(cluster.coordinator(node).execute(withKeyspace("SELECT %1$s.accepted(v) FROM %1$s.t WHERE k = 1"), ALL), row(4d));
        assertThatThrownBy(() -> cluster.coordinator(node).execute(withKeyspace("SELECT %1$s.rejected(v) FROM %1$s.t WHERE k = 1"), ALL))
        .hasMessageContaining(NOT_LOADED)
        .hasMessageContaining("Java UDF validation failed");
    }

    private static String mechanism(IInvokableInstance node)
    {
        return node.callOnInstance(() -> UDFSecurityMechanism.current().name());
    }

    private static UUID schemaVersion(IInvokableInstance node)
    {
        return node.callOnInstance(() -> Schema.instance.getVersion());
    }

    private static boolean isBroken(IInvokableInstance node, String function)
    {
        return node.callOnInstance(() -> {
            for (UserFunction f : Schema.instance.getKeyspaceMetadata(KEYSPACE).userFunctions.get(new FunctionName(KEYSPACE, function)))
                if (f instanceof UDFunction && !f.isAggregate())
                    return ((UDFunction) f).isBroken();
            throw new AssertionError("no function " + function);
        });
    }
}
