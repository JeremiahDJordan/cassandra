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

package org.apache.cassandra.db.guardrails;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the guardrail on index queries that don't restrict the partition key,
 * {@link Guardrails#nonPartitionRestrictedIndexQueryEnabled}.
 */
public class GuardrailNonPartitionRestrictedIndexQueryTest extends GuardrailTester
{
    private static final String MESSAGE = "Non-partition key restricted query is not allowed";

    private boolean enabled;

    @Before
    public void setupTest()
    {
        createTable("CREATE TABLE %s (k int, c int, a int, b int, x int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        enabled = guardrails().getNonPartitionRestrictedQueryEnabled();
    }

    @After
    public void teardownTest()
    {
        guardrails().setNonPartitionRestrictedQueryEnabled(enabled);
    }

    /**
     * The guardrail applies to the reads that use an index, at any depth of their index restrictions. The reads whose
     * index restrictions are in a disjunction with a branch that the index doesn't support use no index, so it doesn't
     * apply to them.
     */
    @Test
    public void testIndexRestrictionsAtAnyDepth() throws Throwable
    {
        String[] unrestricted = { "a = 1",
                                  "a = 1 AND b = 2",
                                  "a = 1 OR b = 2",
                                  "(a = 1 AND b = 2) OR (a = 3 AND b = 4)",
                                  "((a = 1 OR a = 3) AND b = 2) OR a = 9" };
        // an index can't serve a disjunction with a branch that it doesn't support, at any depth: no index is used
        String[] unindexedBranch = { "x = 1 OR a = 1",
                                     "x = 1 OR (a = 1 AND b = 2)",
                                     "x = 1 OR (a = 1 AND (b = 2 OR b = 3))",
                                     "c = 0 AND (x = 1 OR (a = 1 AND b = 2))" };
        String[] restricted = { "k = 0 AND a = 1",
                                "k = 0 AND ((a = 1 AND b = 2) OR (a = 3 AND b = 4))",
                                "k IN (0, 1) AND ((a = 1 AND b = 2) OR a = 3)" };

        guardrails().setNonPartitionRestrictedQueryEnabled(false);
        for (String where : unrestricted)
            assertFails("SELECT * FROM %s WHERE " + where + " ALLOW FILTERING", MESSAGE);
        for (String where : restricted)
            assertValid("SELECT * FROM %s WHERE " + where + " ALLOW FILTERING");
        // no index is used
        assertValid("SELECT * FROM %s WHERE c = 0 ALLOW FILTERING");
        for (String where : unindexedBranch)
            assertValid("SELECT * FROM %s WHERE " + where + " ALLOW FILTERING");

        guardrails().setNonPartitionRestrictedQueryEnabled(true);
        for (String where : unrestricted)
            assertValid("SELECT * FROM %s WHERE " + where + " ALLOW FILTERING");
    }
}
