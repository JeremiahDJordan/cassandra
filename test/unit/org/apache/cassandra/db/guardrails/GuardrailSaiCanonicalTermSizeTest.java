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

package org.apache.cassandra.db.guardrails;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertEquals;

/**
 * The SAI term size guardrails must check a term as the index holds it. A frozen value or a map entry holding decimals
 * is indexed in canonical form, which can be larger than the value as written (2.5 is 5 bytes, 2.50 is 6; a tuple
 * written with fewer fields than its type is padded to all of them). A client write of a value that fits the failure
 * threshold only as written must be rejected, not accepted and then left out of the index as too large.
 */
public class GuardrailSaiCanonicalTermSizeTest extends GuardrailTester
{
    // 850 times 2.5: 7654 bytes as written, 8504 bytes in canonical form; 700 times: 6304 and 7004 bytes
    private static final String OVER = list(850, "2.5");
    private static final String UNDER = list(700, "2.5");
    // 300 times (2.5) for a tuple of a decimal and 9 ints: 3904 bytes as written, 15004 bytes in canonical form
    private static final String PADDED = list(300, "(2.5)");

    private static final String NOTE = "The size is that of the value in canonical form, which is what the index holds";

    @Before
    public void setupTest()
    {
        Guardrails.instance.setSaiFrozenTermSizeThreshold("1KiB", "8KiB");
        Guardrails.instance.setSaiStringTermSizeThreshold("1KiB", "8KiB");
    }

    @Test
    public void testFrozenListColumn() throws Throwable
    {
        checkCanonicalSize("frozen<list<decimal>>", "FULL(%s)", "%s", "%s = %s");
    }

    @Test
    public void testSetElement() throws Throwable
    {
        checkCanonicalSize("set<frozen<list<decimal>>>", "%s", "{%s}", "%s CONTAINS %s");
    }

    @Test
    public void testMapKey() throws Throwable
    {
        checkCanonicalSize("map<frozen<list<decimal>>, int>", "KEYS(%s)", "{%s: 1}", "%s CONTAINS KEY %s");
    }

    @Test
    public void testMapEntry() throws Throwable
    {
        checkCanonicalSize("map<frozen<list<decimal>>, int>", "ENTRIES(%s)", "{%s: 1}", "%s[%s] = 1");
    }

    @Test
    public void testTupleHoldingList() throws Throwable
    {
        checkCanonicalSize("frozen<tuple<frozen<list<decimal>>, int>>", "%s", "(%s, 1)", "%s = (%s, 1)");
    }

    @Test
    public void testPaddedTuples() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, " +
                    "c frozen<list<frozen<tuple<decimal, int, int, int, int, int, int, int, int, int>>>>)");
        createIndex("CREATE CUSTOM INDEX ON %s(FULL(c)) USING 'StorageAttachedIndex'");
        assertRejected("INSERT INTO %s (k, c) VALUES (0, " + PADDED + ')', true);
        assertEquals(0, execute("SELECT k FROM %s").size());
    }

    /**
     * @param type the type of the indexed column
     * @param target the index target, with %s for the column
     * @param value a value of the column holding the list %s
     * @param restriction a restriction finding the value, with %s for the column and the list
     */
    private void checkCanonicalSize(String type, String target, String value, String restriction) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c " + type + ')');
        createIndex("CREATE CUSTOM INDEX ON %s(" + String.format(target, "c") + ") USING 'StorageAttachedIndex'");

        // over the limit in canonical form only, and in both forms
        assertRejected("INSERT INTO %s (k, c) VALUES (0, " + String.format(value, OVER) + ')', true);
        assertRejected("INSERT INTO %s (k, c) VALUES (0, " + String.format(value, OVER.replace("2.5", "2.50")) + ')',
                       false);
        assertEquals(0, execute("SELECT k FROM %s").size());

        // under the limit in both forms: accepted, indexed, with two warnings (the guardrail's, and the note on the
        // canonical size)
        String insert = "INSERT INTO %s (k, c) VALUES (1, " + String.format(value, UNDER) + ')';
        ClientWarn.instance.captureWarnings();
        try
        {
            execute(userClientState, insert);
            List<String> warnings = ClientWarn.instance.getWarnings();
            assertThat(warnings).as(insert).hasSize(2);
            assertThat(warnings.get(0)).contains("term_size violated: Value of column 'c' has size");
            assertThat(warnings.get(1)).containsPattern("^The size given for the value of column 'c' by the " +
                                                        "sai_(frozen|string)_term_size guardrail, [0-9]+ bytes, is " +
                                                        "that of its canonical form, which is what the index holds; " +
                                                        "the value is [0-9]+ bytes as written\\.$");
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
            listener.clear();
        }
        String query = "SELECT k FROM %s WHERE " + String.format(restriction, "c", UNDER);
        beforeAndAfterFlush(() -> assertEquals(1, execute(query).size()));
    }

    /**
     * With {@code -Dcassandra.sai.validate_max_term_size_at_coordinator=true}, the maximum term size is checked
     * instead of the guardrail, for every write: its rejection says the size is the canonical one too, and is not a
     * guardrail's.
     */
    @Test
    public void testLegacyCoordinatorValidation() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR, true))
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, c frozen<list<decimal>>)");
            createIndex("CREATE CUSTOM INDEX ON %s(FULL(c)) USING 'StorageAttachedIndex'");
            String canonicalOnly = "INSERT INTO %s (k, c) VALUES (0, " + OVER + ')';
            String both = "INSERT INTO %s (k, c) VALUES (0, " + OVER.replace("2.5", "2.50") + ')';
            for (ClientState state : new ClientState[]{ userClientState, superClientState, systemClientState })
            {
                assertThat(catchThrowable(() -> execute(state, canonicalOnly))).as(canonicalOnly)
                    .isInstanceOf(InvalidRequestException.class)
                    .isNotInstanceOf(GuardrailViolatedException.class)
                    .hasMessageContaining("Term of column c exceeds")
                    .hasMessageContaining(NOTE)
                    .hasMessageContaining("7654 bytes as written")
                    .hasCauseInstanceOf(InvalidRequestException.class);
                assertThat(catchThrowable(() -> execute(state, both))).as(both)
                    .isInstanceOf(InvalidRequestException.class)
                    .isNotInstanceOf(GuardrailViolatedException.class)
                    .hasMessageContaining("Term of column c exceeds")
                    .hasMessageNotContaining(NOTE);
            }
            assertEquals(0, execute("SELECT k FROM %s").size());
        }
    }

    /**
     * Asserts that the given write is rejected by the guardrail, as a {@link GuardrailViolatedException}.
     *
     * @param canonical whether only the value's canonical form is over the threshold, which the error must say
     */
    private void assertRejected(String query, boolean canonical)
    {
        if (canonical)
            assertThat(catchThrowable(() -> execute(userClientState, query))).as(query)
                .isInstanceOf(GuardrailViolatedException.class)
                .hasMessageContaining("has size")
                .hasMessageContaining(NOTE)
                .hasMessageContaining("bytes as written")
                .hasCauseInstanceOf(GuardrailViolatedException.class);
        else
            assertThat(catchThrowable(() -> execute(userClientState, query))).as(query)
                .isInstanceOf(GuardrailViolatedException.class)
                .hasMessageContaining("has size")
                .hasMessageNotContaining(NOTE);
        listener.clear();
    }

    private static String list(int size, String element)
    {
        return '[' + String.join(", ", Collections.nCopies(size, element)) + ']';
    }
}
