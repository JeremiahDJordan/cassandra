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

package org.apache.cassandra.cql3.validation.entities;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.security.BytecodeSandboxRule;
import org.apache.cassandra.security.UDFSecurityMechanism;

import static org.junit.Assert.assertEquals;

/**
 * The bytecode UDF sandbox rejects {@code invokedynamic}, so string concatenation must compile to
 * {@code StringBuilder} calls, which depends on the UDF compiler's ECJ options such as the compliance level.
 */
public class UFConcatBytecodeTest extends CQLTester
{
    @ClassRule
    public static final BytecodeSandboxRule bytecodeSandbox = new BytecodeSandboxRule();

    @Test
    public void testConcatenationCompilesWithoutInvokedynamic() throws Throwable
    {
        assertEquals(UDFSecurityMechanism.BYTECODE, UDFSecurityMechanism.current());
        createTable("CREATE TABLE %s (key int primary key, i int, d double, s text)");
        execute("INSERT INTO %s (key, i, d, s) VALUES (1, 2, 1.5, 'x')");
        String f = createFunction(KEYSPACE, "int, double, text",
                                  "CREATE FUNCTION %s(i int, d double, s text) CALLED ON NULL INPUT RETURNS text " +
                                  "LANGUAGE java AS $$ Object o = Boolean.TRUE; return s + i + d + o + 'c'; $$");
        assertRows(execute("SELECT " + f + "(i, d, s) FROM %s WHERE key = 1"), row("x21.5truec"));
    }
}
