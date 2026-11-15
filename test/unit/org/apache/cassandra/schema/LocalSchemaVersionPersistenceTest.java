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
package org.apache.cassandra.schema;

import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;

/**
 * Drivers decide that a schema change has reached every node by comparing the {@code schema_version} column of
 * {@code system.local} and {@code system.peers(_v2)} on the coordinator of the change. The new version must therefore
 * be visible in {@code system.local} by the time the schema change is acknowledged. Otherwise the coordinator reports
 * its previous version, which matches the previous version of the other nodes, and the driver considers the schema
 * agreed before the other nodes have applied the change.
 */
@RunWith(BMUnitRunner.class)
public class LocalSchemaVersionPersistenceTest extends CQLTester
{
    @Test
    @BMRule(name = "Delay persisting the local node information",
            targetClass = "org.apache.cassandra.nodes.NodesPersistence",
            targetMethod = "saveLocal",
            targetLocation = "AT ENTRY",
            action = "Thread.sleep(500)")
    public void testSchemaVersionIsInSystemLocalWhenSchemaChangeReturns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        assertSystemLocalHasCurrentSchemaVersion();

        alterTable("ALTER TABLE %s ADD v2 int");
        assertSystemLocalHasCurrentSchemaVersion();
    }

    private void assertSystemLocalHasCurrentSchemaVersion() throws Throwable
    {
        UUID expected = Schema.instance.getVersion();
        UntypedResultSet rows = execute("SELECT schema_version FROM system.local WHERE key = 'local'");
        assertEquals(expected, rows.one().getUUID("schema_version"));
    }
}
