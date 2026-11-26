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

package org.apache.cassandra.index.sai.cql;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Without authentication, the SAI term size guardrails don't apply to the anonymous user, although other guardrails
 * do (see {@link CassandraRelevantProperties#ENABLE_GUARDRAILS_FOR_ANONYMOUS_USER}). As for superusers, its oversized
 * terms are written but not indexed.
 */
public class TermSizeGuardrailWithoutAuthTest extends SAITester
{
    @BeforeClass
    public static void setupClass()
    {
        requireNetwork();
        assertTrue(CassandraRelevantProperties.ENABLE_GUARDRAILS_FOR_ANONYMOUS_USER.getBoolean());
    }

    @Test
    public void testAnonymousUserIsNotGuarded()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        String large = "a".repeat(2048); // over the 1KiB warning threshold
        String oversized = "b".repeat(9000); // over the 8KiB failure threshold

        ResultSet result = executeNet("INSERT INTO %s (k, v) VALUES (0, ?)", large);
        assertTrue(result.getExecutionInfo().getWarnings().isEmpty());
        result = executeNet("INSERT INTO %s (k, v) VALUES (1, ?)", oversized);
        assertTrue(result.getExecutionInfo().getWarnings().isEmpty());

        assertEquals(1, execute("SELECT k FROM %s WHERE v = ?", large).size());
        assertEquals(0, execute("SELECT k FROM %s WHERE v = ?", oversized).size());
        assertEquals(2, execute("SELECT k FROM %s").size());
    }
}
