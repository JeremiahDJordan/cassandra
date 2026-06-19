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

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.security.ThreadAwareSecurityManager;

import static org.apache.cassandra.config.CassandraRelevantProperties.UDF_SECURITY_MECHANISM;

/**
 * Verifies that the SecurityManager-free sandbox preserves the legacy {@code allow_extra_insecure_udfs}
 * escape hatch rather than silently tightening it.
 * <p>
 * Under the old SecurityManager, the UDF class loader only blocked {@code java.lang.System} (and the
 * property-read aliases) when {@code (!user_defined_functions_threads_enabled && !allow_extra_insecure_udfs)}.
 * The explicit insecure opt-in — threads disabled plus {@code allow_extra_insecure_udfs=true} — ran UDFs on a
 * non-secured thread where {@code System.*} was deliberately permitted. The sandbox enforces the same model
 * statically at CREATE FUNCTION time, gating the byte-code block on
 * {@code (enableUserDefinedFunctionsThreads() || !allowExtraInsecureUDFs())}. This test exercises the
 * skip-the-block branch and asserts the dangerous {@code System.*} functions are created successfully.
 * <p>
 * {@code JavaBasedUDFunction} selects its byte-code verifier dynamically per CREATE FUNCTION (via
 * {@code verifierFor()}), so it is enough for these flags to be in effect before the first function is
 * created below. This is a dedicated class so the insecure combo cannot leak into other UDF tests sharing a
 * forked JVM. The complementary default-config behaviour — the same functions being rejected — is covered by
 * {@link UFSecurityTest#testSandboxBlocksDangerousSystemMethods}.
 */
public class UFInsecureSystemAccessTest extends CQLTester
{
    @BeforeClass
    public static void setUpInsecureUDFs()
    {
        // Force the SM-free sandbox even on JDKs where the SecurityManager is still supported, so the
        // byte-code verifier path (not the SM) is what would enforce the block. Read dynamically by
        // ThreadAwareSecurityManager.useSecurityManager(); applied before the first CREATE FUNCTION below
        // loads JavaBasedUDFunction and configures its verifier.
        UDF_SECURITY_MECHANISM.setString("sandbox");

        // The legacy "allowed" insecure combination: threads disabled + extra-insecure opt-in.
        Config conf = DatabaseDescriptor.getRawConfig();
        conf.user_defined_functions_enabled = true;
        conf.user_defined_functions_threads_enabled = false;
        conf.allow_extra_insecure_udfs = true;
    }

    /**
     * With the insecure opt-in active, the byte-code verifier must NOT reject {@code System.*} (or the
     * property-read aliases): the functions are created successfully, matching the permissive behaviour the
     * SecurityManager era allowed for this exact configuration.
     */
    @Test
    public void testExtraInsecureUDFsAllowSystemAccess() throws Throwable
    {
        Assume.assumeFalse("legacy SecurityManager mechanism in use; the insecure combo is enforced by the SM, not the verifier",
                           ThreadAwareSecurityManager.useSecurityManager());

        // Sanity-check the flags really put us on the skip-the-block branch.
        Assert.assertFalse(DatabaseDescriptor.enableUserDefinedFunctionsThreads());
        Assert.assertTrue(DatabaseDescriptor.allowExtraInsecureUDFs());

        String[][] sources =
        {
        {"getProperty",     "System.getProperty(\"foo\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"getenv",          "System.getenv(\"PATH\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"getlogger",       "System.getLogger(\"x\"); return 0d;"},
        {"getinteger",      "Integer.getInteger(\"x\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"getlong",         "Long.getLong(\"x\"); return 0d;"}, // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        {"getboolean",      "Boolean.getBoolean(\"x\"); return 0d;"} // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        };

        for (String[] s : sources)
        {
            String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                          "CREATE OR REPLACE FUNCTION %s(val double) " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS double " +
                                          "LANGUAGE JAVA\n" +
                                          "AS '" + s[1] + "';");
            Assert.assertNotNull("expected insecure UDF using System." + s[0] + " to be created", fName);
        }
    }
}
