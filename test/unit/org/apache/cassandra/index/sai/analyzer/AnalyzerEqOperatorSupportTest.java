/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.index.sai.analyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientWarn;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;

import static java.lang.String.format;
import static org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport.EQ_RESTRICTION_ON_ANALYZED_WARNING;
import static org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport.LWT_CONDITION_ON_ANALYZED_WARNING;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link AnalyzerEqOperatorSupport}.
 */
public class AnalyzerEqOperatorSupportTest extends SAITester
{
    @Before
    public void createTable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, f text)");
    }

    private void populateTable()
    {
        execute("INSERT INTO %s (k, v) VALUES (1, 'Quick fox')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'Lazy fox')");
    }

    @Test
    public void testWithoutAnyIndex()
    {
        populateTable();

        // equals (=)
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick fox' ALLOW FILTERING", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox' ALLOW FILTERING");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'fox' ALLOW FILTERING");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v = 'Lazy fox' ALLOW FILTERING", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v = 'lazy fox' ALLOW FILTERING");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick' OR v = 'Lazy' ALLOW FILTERING");
        assertInvalidMessage("v cannot be restricted by more than one relation if it includes an Equal",
                             "SELECT k FROM %s WHERE v = 'Quick' AND v = 'fox' ALLOW FILTERING");

        // matches (:)
        assertInvalidMessage(": restriction is only supported on properly indexed columns. v : 'Quick fox' is not valid.",
                             "SELECT k FROM %s WHERE v : 'Quick fox' ALLOW FILTERING");

        // LWT
        assertRowsWithoutWarning("UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v = 'Quick fox'", row(true));
        assertRowsWithoutWarning("UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v = 'fox'", row(false, "Quick fox"));
        assertInvalidMessage(ColumnCondition.ANALYZER_MATCHES_ERROR, "UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v : 'Quick fox'");
    }

    @Test
    public void testWithLegacyIndex()
    {
        populateTable();

        createIndex("CREATE INDEX ON %s(v) USING 'legacy_local_table'");

        // equals (=)
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'fox'");
        assertInvalidMessage("v cannot be restricted by more than one relation if it includes an Equal",
                             "SELECT k FROM %s WHERE v = 'Quick' AND v = 'fox'");
        assertInvalidMessage(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_DISJUNCTION,
                             "SELECT k FROM %s WHERE v = 'Quick fox' OR v = 'Lazy fox'");

        // matches (:)
        assertInvalidMessage(format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_ANALYZER_MATCHES_MESSAGE, 'v'),
                             "SELECT k FROM %s WHERE v : 'Quick fox'");

        // LWT
        assertRowsWithoutWarning("UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v = 'Quick fox'", row(true));
        assertRowsWithoutWarning("UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v = 'fox'", row(false, "Quick fox"));
        assertInvalidMessage(ColumnCondition.ANALYZER_MATCHES_ERROR, "UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v : 'Quick fox'");
    }

    @Test
    public void testNonAnalyzedIndexWithDefaults()
    {
        assertIndexQueries("{}", () -> {

            // equals (=)
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick fox'", row(1));
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox'");
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'fox'");
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v = 'Lazy fox'", row(1), row(2));
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v = 'lazy fox'");
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick' OR v = 'Lazy'");
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick' OR v = 'lazy'");
            assertInvalidMessage("v cannot be restricted by more than one relation if it includes an Equal",
                                 "SELECT k FROM %s WHERE v = 'Quick' AND v = 'fox'");

            // matches (:)
            assertIndexDoesNotSupportMatches();

            // LWT
            assertRowsWithoutWarning("UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v = 'Quick fox'", row(true));
            assertRowsWithoutWarning("UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v = 'fox'", row(false, "Quick fox"));
            assertInvalidMessage(ColumnCondition.ANALYZER_MATCHES_ERROR, "UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v : 'Quick fox'");
        });
    }

    @Test
    public void testNonAnalyzedIndexWithMatch()
    {
        assertIndexThrowsNotAnalyzedError("{'equals_behaviour_when_analyzed': 'MATCH'}");
    }

    @Test
    public void testNonAnalyzedIndexWithUnsupported()
    {
        assertIndexThrowsNotAnalyzedError("{'equals_behaviour_when_analyzed': 'UNSUPPORTED'}");
    }

    @Test
    public void testNonAnalyzedIndexWithWrongValue()
    {
        assertIndexThrowsNotAnalyzedError("{'equals_behaviour_when_analyzed': 'WRONG'}");
    }

    @Test
    public void testNonTokenizedIndexWithDefaults()
    {
        assertIndexQueries("{'case_sensitive': 'false'}", () -> {
            assertNonTokenizedIndexSupportsEquality();
            assertNonTokenizedIndexSupportsMatches();
            assertNonTokenizedIndexSupportsMixedEqualityAndMatches();
            assertNonTokenizedIndexSupportsIn();
            assertNonTokenizedIndexSupportsNotEquals();
        });
    }

    @Test
    public void testNonTokenizedIndexWithMatch()
    {
        assertIndexQueries("{'case_sensitive': 'false', 'equals_behaviour_when_analyzed': 'MATCH'}", () -> {
            assertNonTokenizedIndexSupportsEquality();
            assertNonTokenizedIndexSupportsMatches();
            assertNonTokenizedIndexSupportsMixedEqualityAndMatches();
            assertNonTokenizedIndexSupportsIn();
            assertNonTokenizedIndexSupportsNotEquals();
        });
    }

    @Test
    public void testNonTokenizedIndexWithUnsupported()
    {
        assertIndexQueries("{'case_sensitive': 'false', 'equals_behaviour_when_analyzed': 'UNSUPPORTED'}", () -> {
            assertIndexDoesNotSupportEquals();
            assertNonTokenizedIndexSupportsMatches();
        });
    }

    @Test
    public void testNonTokenizedIndexWithWrongValue()
    {
        assertIndexThrowsUnrecognizedOptionError("{'case_sensitive': 'false', 'equals_behaviour_when_analyzed': 'WRONG'}");
    }

    private void assertNonTokenizedIndexSupportsEquality()
    {
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick fox'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick fox'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'fox'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v = 'Lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v = 'lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' OR v = 'Lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' OR v = 'lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' AND v = 'fox'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' AND v = 'fox'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' AND v = 'Lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' AND v = 'lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v = 'quick' AND v = 'fox') OR v = 'dog'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v = 'quick' AND v = 'fox') OR v = 'Lazy'");

        // LWT
        assertRowsWithLWTWarning("UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v = 'Quick fox'", row(true));
        assertRowsWithLWTWarning("UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v = 'fox'", row(false, "Quick fox"));
    }

    private void assertNonTokenizedIndexSupportsIn()
    {
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('Quick fox', 'Lazy fox')", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('quick fox', 'LAZY FOX')", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('quick', 'lazy fox')", row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('quick', 'lazy')");

        // bound values
        assertRows(execute("SELECT k FROM %s WHERE v IN ?", list("QUICK FOX", "lazy")), row(1));
        assertRows(execute("SELECT k FROM %s WHERE v IN ?", list("LAZY FOX")), row(2));
        assertEmpty(execute("SELECT k FROM %s WHERE v IN ?", list()));

        // filtered without the index
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('quick', 'lazy fox') OR f = 'Lazy' ALLOW FILTERING", row(2));
    }

    private void assertNonTokenizedIndexSupportsNotEquals()
    {
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v != 'quick fox' ALLOW FILTERING", row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v != 'fox' ALLOW FILTERING", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v NOT IN ('quick fox', 'lazy') ALLOW FILTERING", row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v NOT IN ('quick fox', 'LAZY FOX') ALLOW FILTERING");
        assertRows(execute("SELECT k FROM %s WHERE v NOT IN ? ALLOW FILTERING", list("QUICK FOX")), row(2));
        assertRows(executeInternal("SELECT k FROM %s WHERE v != 'quick fox' ALLOW FILTERING"), row(2));

        // LWT
        assertRowsWithLWTWarning("UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v != 'quick fox'", row(true));
    }

    private void assertNonTokenizedIndexSupportsMatches()
    {
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'fox'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick fox' OR v : 'Lazy fox'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick fox' OR v : 'lazy fox'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' OR v : 'Lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' OR v : 'lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' AND v : 'fox'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' AND v : 'fox'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' AND v : 'Lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' AND v : 'lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE (v : 'quick' AND v : 'fox') OR v : 'dog'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE (v : 'quick' AND v : 'fox') OR v : 'Lazy'");

        // LWT
        assertInvalidMessage(ColumnCondition.ANALYZER_MATCHES_ERROR, "UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v : 'Quick fox'");
    }

    private void assertNonTokenizedIndexSupportsMixedEqualityAndMatches()
    {
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v : 'Lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v : 'lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' OR v : 'Lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' OR v : 'lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' AND v : 'fox'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' AND v : 'fox'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' AND v : 'Lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' AND v : 'lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v = 'quick' AND v : 'fox') OR v = 'dog'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v = 'quick' AND v : 'fox') OR v = 'Lazy'");

        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'Quick fox' OR v = 'Lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'quick fox' OR v = 'lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'Quick' OR v = 'Lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'quick' OR v = 'lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'Quick' AND v = 'fox'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'quick' AND v = 'fox'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'Quick' AND v = 'Lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'quick' AND v = 'lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v : 'quick' AND v = 'fox') OR v : 'dog'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v : 'quick' AND v = 'fox') OR v : 'Lazy'");

        // LWT
        assertInvalidMessage(ColumnCondition.ANALYZER_MATCHES_ERROR,
                             "UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v = 'Quick fox' AND v : 'Quick fox'");
    }

    @Test
    public void testTokenizedIndexWithDefaults()
    {
        assertIndexQueries("{'index_analyzer': 'standard'}", () -> {
            assertTokenizedIndexSupportsEquality();
            assertTokenizedIndexSupportsMatches();
            assertTokenizedIndexSupportsMixedEqualityAndMatches();
            assertTokenizedIndexSupportsIn();
            assertTokenizedIndexSupportsNotEquals();
        });
    }

    @Test
    public void testTokenizedIndexWithMatch()
    {
        assertIndexQueries("{'index_analyzer': 'standard', 'equals_behaviour_when_analyzed': 'MATCH'}", () -> {
            assertTokenizedIndexSupportsEquality();
            assertTokenizedIndexSupportsMatches();
            assertTokenizedIndexSupportsMixedEqualityAndMatches();
            assertTokenizedIndexSupportsIn();
            assertTokenizedIndexSupportsNotEquals();
        });
    }

    @Test
    public void testTokenizedIndexWithUnsupported()
    {
        assertIndexQueries("{'index_analyzer': 'standard', 'equals_behaviour_when_analyzed': 'UNSUPPORTED'}", () -> {
            assertIndexDoesNotSupportEquals();
            assertTokenizedIndexSupportsMatches();

            // test mixed with another non-indexed column
            assertInvalidMessage(": restriction is only supported on properly indexed columns",
                                 "SELECT k FROM %s WHERE v : 'Quick' OR f : 'Lazy' ALLOW FILTERING");
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' OR f = 'Lazy' ALLOW FILTERING", row(1));
            assertInvalidMessage(": restriction is only supported on properly indexed columns",
                                 "SELECT k FROM %s WHERE v = 'Quick' OR f : 'Lazy' ALLOW FILTERING");
            assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'Quick' OR f = 'Lazy' ALLOW FILTERING");
        });
    }

    @Test
    public void testTokenizedIndexWithWrongValue()
    {
        assertIndexThrowsUnrecognizedOptionError("{'index_analyzer': 'standard', 'equals_behaviour_when_analyzed': 'WRONG'}");
    }

    private void assertTokenizedIndexSupportsEquality()
    {
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick fox'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick fox'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v = 'Lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v = 'lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' OR v = 'Lazy'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' OR v = 'lazy'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' AND v = 'fox'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' AND v = 'fox'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' AND v = 'Lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' AND v = 'lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v = 'quick' AND v = 'fox') OR v = 'dog'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v = 'quick' AND v = 'fox') OR v = 'Lazy'", row(1), row(2));
    }

    private void assertTokenizedIndexSupportsIn()
    {
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('Quick fox', 'Lazy fox')", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('quick', 'lazy')", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('quick dog', 'lazy')", row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('quick dog', 'lazy cat')");

        // an element whose serialized length isn't valid UTF-8
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('lazy', '" + "a".repeat(128) + "')", row(2));

        // bound values
        assertRows(execute("SELECT k FROM %s WHERE v IN ?", list("quick dog", "lazy")), row(2));
        assertRows(execute("SELECT k FROM %s WHERE v IN ?", list("quick")), row(1));
        assertEmpty(execute("SELECT k FROM %s WHERE v IN ?", list()));

        // the rows returned by the index, without the coordinator filtering them
        assertRows(executeInternal("SELECT k FROM %s WHERE v IN ('quick dog', 'lazy')"), row(2));

        // filtered without the index
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('quick dog', 'lazy') OR f = 'Lazy' ALLOW FILTERING", row(2));
    }

    private void assertTokenizedIndexSupportsNotEquals()
    {
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v != 'quick' ALLOW FILTERING", row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v != 'quick fox' ALLOW FILTERING", row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v != 'fox' ALLOW FILTERING");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v != 'quick dog' ALLOW FILTERING", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v NOT IN ('quick', 'lazy') ALLOW FILTERING");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v NOT IN ('quick dog', 'lazy') ALLOW FILTERING", row(1));
        assertRows(executeInternal("SELECT k FROM %s WHERE v != 'quick' ALLOW FILTERING"), row(2));

        // LWT
        assertRowsWithLWTWarning("UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v != 'quick'", row(true));
    }

    private void assertTokenizedIndexSupportsMatches()
    {
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'fox'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick fox' OR v : 'Lazy fox'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick fox' OR v : 'lazy fox'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' OR v : 'Lazy'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' OR v : 'lazy'", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' AND v : 'fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' AND v : 'fox'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' AND v : 'Lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'quick' AND v : 'lazy'");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE (v : 'quick' AND v : 'fox') OR v : 'dog'", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE (v : 'quick' AND v : 'fox') OR v : 'Lazy'", row(1), row(2));
    }

    private void assertTokenizedIndexSupportsMixedEqualityAndMatches()
    {
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick fox' OR v : 'Lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick fox' OR v : 'lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' OR v : 'Lazy'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' OR v : 'lazy'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' AND v : 'fox'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' AND v : 'fox'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' AND v : 'Lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'quick' AND v : 'lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v = 'quick' AND v : 'fox') OR v = 'dog'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v = 'quick' AND v : 'fox') OR v = 'Lazy'", row(1), row(2));

        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'Quick fox' OR v = 'Lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'quick fox' OR v = 'lazy fox'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'Quick' OR v = 'Lazy'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'quick' OR v = 'lazy'", row(1), row(2));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'Quick' AND v = 'fox'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'quick' AND v = 'fox'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'Quick' AND v = 'Lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v : 'quick' AND v = 'lazy'");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v : 'quick' AND v = 'fox') OR v : 'dog'", row(1));
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE (v : 'quick' AND v = 'fox') OR v : 'Lazy'", row(1), row(2));

        // test mixed with another non-indexed column
        String errorMsg = ": restriction is only supported on properly indexed columns";
        assertInvalidMessage(errorMsg, "SELECT k FROM %s WHERE v : 'Quick' OR f : 'Lazy' ALLOW FILTERING");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v : 'Quick' OR f = 'Lazy' ALLOW FILTERING", row(1));
        assertInvalidMessage(errorMsg, "SELECT k FROM %s WHERE v = 'Quick' OR f : 'Lazy' ALLOW FILTERING");
        assertRowsWithSelectWarning("SELECT k FROM %s WHERE v = 'Quick' OR f = 'Lazy' ALLOW FILTERING", row(1));
    }

    @Test
    public void testInWithStopWords()
    {
        String options = "{'index_analyzer': '{\"tokenizer\":{\"name\":\"standard\"}, " +
                         "\"filters\":[{\"name\":\"lowercase\"}, {\"name\":\"stop\"}]}'}";
        assertIndexQueries(options, () -> {
            // a value of stop words only has no terms, which no row holds
            assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('the', 'lazy')", row(2));
            assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('the', 'lazy') OR f = 'Lazy' ALLOW FILTERING", row(2));
        });
    }

    @Test
    public void testInWithQueryAnalyzer()
    {
        assertIndexQueries("{'index_analyzer': 'standard', 'query_analyzer': 'whitespace'}", () -> {
            // the values are analyzed with the query analyzer, which doesn't lowercase them
            assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('Quick', 'lazy')", row(2));
            assertRowsWithSelectWarning("SELECT k FROM %s WHERE v IN ('Quick', 'lazy') OR f = 'Lazy' ALLOW FILTERING", row(2));
        });
    }

    @Test
    public void testInWithUnsupportedAndAnotherIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, i int, x int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                    "{'index_analyzer': 'standard', 'equals_behaviour_when_analyzed': 'UNSUPPORTED'}");
        createIndex("CREATE CUSTOM INDEX ON %s(i) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v, i, x) VALUES (1, 'Quick fox', 1, 0)");
        execute("INSERT INTO %s (k, v, i, x) VALUES (2, 'Lazy fox', 1, 1)");
        execute("INSERT INTO %s (k, v, i, x) VALUES (3, 'Quick fox', 2, 0)");

        // the IN is filtered on the raw values of the rows the index of i selects
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT k FROM %s WHERE v IN ('Quick fox', 'lazy fox') AND i = 1 ALLOW FILTERING"), row(1));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE (v IN ('Quick fox', 'lazy fox') OR x = 1) AND i = 1 ALLOW FILTERING"),
                                    row(1), row(2));
        });
    }

    @Test
    public void testInInDisjunction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s text STATIC, v text, f int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        execute("INSERT INTO %s (k, c, s, v, f) VALUES (1, 1, 'Quick fox', 'Quick fox', 0)");
        execute("INSERT INTO %s (k, c, v, f) VALUES (1, 2, 'Lazy fox', 1)");
        execute("INSERT INTO %s (k, c, s, v, f) VALUES (2, 1, 'Lazy fox', 'Quick dog', 0)");
        execute("INSERT INTO %s (k, s) VALUES (3, 'Quick fox')");
        execute("INSERT INTO %s (k, c, s, v, f) VALUES (4, 1, 'Quick dog', 'Lazy dog', 1)");

        // an IN matches the rows of the disjunction of its equalities, also next to another column and on a static column
        beforeAndAfterFlush(() -> {
            assertSameRows("SELECT k, c FROM %s WHERE v IN ('quick fox', 'lazy fox')",
                           "SELECT k, c FROM %s WHERE v = 'quick fox' OR v = 'lazy fox'");
            assertSameRows("SELECT k, c FROM %s WHERE v IN ('quick fox', 'lazy fox') OR f = 1 ALLOW FILTERING",
                           "SELECT k, c FROM %s WHERE v = 'quick fox' OR v = 'lazy fox' OR f = 1 ALLOW FILTERING");
            assertSameRows("SELECT k, c FROM %s WHERE s IN ('quick fox', 'lazy fox')",
                           "SELECT k, c FROM %s WHERE s = 'quick fox' OR s = 'lazy fox'");
            assertSameRows("SELECT k, c FROM %s WHERE s IN ('quick fox', 'lazy fox') OR f = 1 ALLOW FILTERING",
                           "SELECT k, c FROM %s WHERE s = 'quick fox' OR s = 'lazy fox' OR f = 1 ALLOW FILTERING");
        });
    }

    private void assertSameRows(String query, String equivalentQuery)
    {
        assertEquals(parseReadCommand(equivalentQuery).rowFilter().toString(), parseReadCommand(query).rowFilter().toString());
        Object[][] expected = getRows(execute(equivalentQuery));
        Assertions.assertThat(expected).isNotEmpty();
        assertRowsIgnoringOrder(execute(query), expected);
    }

    @Test
    public void testNotEqualsAndNotIn() throws Throwable
    {
        String[] values = { "Quick fox", "quick fox", "fox quick", "Lazy dog", "", null };
        Set<Integer> withValue = new TreeSet<>();
        for (int k = 0; k < values.length; k++)
        {
            if (values[k] != null)
                withValue.add(k);
        }

        for (String analyzer : List.of("'case_sensitive': 'false'", "'index_analyzer': 'standard'"))
        {
            for (AnalyzerEqOperatorSupport.Value eqBehaviour : AnalyzerEqOperatorSupport.Value.values())
            {
                createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, raw text, i int)");
                createIndex(format("CREATE CUSTOM INDEX ON %%s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {%s, '%s': '%s'}",
                                   analyzer, AnalyzerEqOperatorSupport.OPTION, eqBehaviour));
                createIndex("CREATE CUSTOM INDEX ON %s(i) USING 'StorageAttachedIndex'");
                for (int k = 0; k < values.length; k++)
                    execute("INSERT INTO %s (k, v, raw, i) VALUES (?, ?, ?, 1)", k, values[k], values[k]);

                // != and NOT IN match the rows with a value that = and IN don't match, which compare raw values as
                // the unindexed column does with equals_behaviour_when_analyzed: unsupported, with or without the
                // index of i, next to a range, and on the replica
                beforeAndAfterFlush(() -> {
                    List<Integer> inRange = keys(false, "SELECT k FROM %s WHERE v < 'q' ALLOW FILTERING");
                    for (String restriction : List.of("%s 'quick fox'", "%s 'Quick fox'", "%s 'QUICK'", "%s 'dog'", "%s ''",
                                                      "%s ('quick fox', 'dog')", "%s ('QUICK', '')"))
                    {
                        boolean in = restriction.contains("(");
                        String positive = format("SELECT k FROM %%s WHERE v " + restriction + " ALLOW FILTERING", in ? "IN" : "=");
                        for (String other : List.of("", " AND i = 1", " AND v < 'q' AND i = 1"))
                        {
                            String select = "SELECT k FROM %s WHERE %s " + restriction + other + " ALLOW FILTERING";
                            String negative = format(select, "%s", "v", in ? "NOT IN" : "!=");
                            String rawNegative = format(select, "%s", "raw", in ? "NOT IN" : "!=").replace("v <", "raw <");
                            for (boolean internal : new boolean[]{ false, true })
                            {
                                Set<Integer> expected = new TreeSet<>(withValue);
                                expected.removeAll(keys(internal, positive));
                                if (other.contains("v <"))
                                    expected.retainAll(inRange);
                                Assertions.assertThat(keys(internal, negative))
                                          .as("%s %s %s", analyzer, eqBehaviour, negative)
                                          .containsExactlyElementsOf(expected);
                                if (eqBehaviour == AnalyzerEqOperatorSupport.Value.UNSUPPORTED)
                                    Assertions.assertThat(keys(internal, negative))
                                              .as("%s %s %s", analyzer, eqBehaviour, negative)
                                              .isEqualTo(keys(internal, rawNegative));
                            }
                        }
                    }
                });
            }
        }
    }

    @Test
    public void testNotEqualsOnStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s text STATIC, i int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': 'false'}");
        createIndex("CREATE CUSTOM INDEX ON %s(i) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, c, s, i) VALUES (1, 1, 'Quick', 1)");
        execute("INSERT INTO %s (k, c, i) VALUES (1, 2, 1)");
        execute("INSERT INTO %s (k, c, s, i) VALUES (2, 1, 'quick', 1)");
        execute("INSERT INTO %s (k, c, s, i) VALUES (3, 1, 'dog', 1)");
        execute("INSERT INTO %s (k, s) VALUES (4, 'cat')");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT k, c FROM %s WHERE s != 'QUICK' ALLOW FILTERING"), row(3, 1), row(4, null));
            assertRowsIgnoringOrder(executeInternal("SELECT k, c FROM %s WHERE s != 'QUICK' ALLOW FILTERING"), row(3, 1), row(4, null));
            assertRows(execute("SELECT k, c FROM %s WHERE s NOT IN ('QUICK', 'cat') AND i = 1 ALLOW FILTERING"), row(3, 1));
            assertRows(executeInternal("SELECT k, c FROM %s WHERE s NOT IN ('QUICK', 'cat') AND i = 1 ALLOW FILTERING"), row(3, 1));
        });
    }

    private List<Integer> keys(boolean internal, String query)
    {
        List<Integer> keys = new ArrayList<>();
        for (UntypedResultSet.Row row : internal ? executeInternal(query) : execute(query))
            keys.add(row.getInt("k"));
        Collections.sort(keys);
        return keys;
    }

    @Test
    public void testEmptyIn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, f text, i int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        createIndex("CREATE CUSTOM INDEX ON %s(i) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v, f, i) VALUES (1, 'Quick fox', 'a', 1)");
        execute("INSERT INTO %s (k, v, f, i) VALUES (2, 'Lazy fox', 'b', 2)");

        // an IN without values matches no row, with and without the index
        beforeAndAfterFlush(() -> {
            assertEmpty(execute("SELECT k FROM %s WHERE v IN ?", list()));
            assertEmpty(execute("SELECT k FROM %s WHERE k = 1 AND v IN ?", list()));
            Assertions.assertThat(readAll(serialize(parseReadCommand("SELECT k FROM %s WHERE k = 1 AND v IN ()")))).isEmpty();
            assertRows(execute("SELECT k FROM %s WHERE v IN ? OR i = 2", list()), row(2));
            assertRows(execute("SELECT k FROM %s WHERE v IN () OR f = 'b' ALLOW FILTERING"), row(2));
            assertRows(execute("SELECT k FROM %s WHERE v IN (null, null) OR f = 'b' ALLOW FILTERING"), row(2));
        });
    }

    @Test
    public void testInOnReplicaWithoutIndex() throws Throwable
    {
        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        populateTable();

        // a read of a full primary key filters without the index, which a replica without it can't do, for IN as for =
        ReadCommand eq = parseReadCommand("SELECT k FROM %s WHERE k = 1 AND v = 'quick'");
        ReadCommand in = parseReadCommand("SELECT k FROM %s WHERE k = 1 AND v IN ('quick', 'dog')");
        assertNull(eq.indexQueryPlan());
        assertNull(in.indexQueryPlan());
        byte[] serializedEq = serialize(eq);
        byte[] serializedIn = serialize(in);
        assertEquals(1, readAll(serializedEq).size());
        assertEquals(1, readAll(serializedIn).size());

        dropIndex("DROP INDEX %s." + index);
        Throwable eqError = catchThrowable(() -> readAll(serializedEq));
        assertNotNull(eqError);
        assertThatThrownBy(() -> readAll(serializedIn)).isInstanceOf(eqError.getClass()).hasMessage(eqError.getMessage());
    }

    private static byte[] serialize(ReadCommand command) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadCommand.serializer.serialize(command, out, MessagingService.current_version);
            return out.toByteArray();
        }
    }

    private static List<FilteredPartition> readAll(byte[] command) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(command))
        {
            return Util.getAll(ReadCommand.serializer.deserialize(in, MessagingService.current_version));
        }
    }

    @Test
    public void testCompoundPrimaryKeyRestrictionsWithAnalyzerEqOperatorSupportUnsupported() throws Throwable
    {
        // Note that we test clustering columns, but they don't technically work quite right. I include them
        // in this test to cover their behavior and prevent unintentional changes.
        createTable("CREATE TABLE %s (x text, y text, z text, a set<text>, PRIMARY KEY ((x, y), z))");

        // Create case-insensitive index on all columns except y since it is essentially the same as x here
        createIndex(createCaseInsensitiveIndexString("x", AnalyzerEqOperatorSupport.Value.UNSUPPORTED));
        createIndex(createCaseInsensitiveIndexString("z", AnalyzerEqOperatorSupport.Value.UNSUPPORTED));
        createIndex(createCaseInsensitiveIndexString("values(a)", AnalyzerEqOperatorSupport.Value.UNSUPPORTED));

        // Set up two unique rows that will map to the same index terms
        execute("INSERT INTO %s (x, y, z, a) VALUES (?, ?, ?, ?)", "a", "b", "c", set("d", "e", "f"));
        execute("INSERT INTO %s (x, y, z, a) VALUES (?, ?, ?, ?)", "A", "B", "C", set("D", "E", "F"));
        execute("INSERT INTO %s (x, y, z, a) VALUES (?, ?, ?, ?)", "z", "z", "z", set("z", "z", "z"));

        beforeAndAfterFlush(
        () -> {
            // Fully qualified partition key and primary key
            assertRows(execute("SELECT x FROM %s WHERE x = 'a' AND y = 'b'"), row("a"));
            assertRows(execute("SELECT x FROM %s WHERE x = 'a' AND y = 'b' AND z = 'c'"), row("a"));
            assertRows(execute("SELECT x FROM %s WHERE x = 'A' AND y = 'B'"), row("A"));
            assertRows(execute("SELECT x FROM %s WHERE x = 'A' AND y = 'B' AND z = 'C'"), row("A"));
            assertRows(execute("SELECT * FROM %s WHERE x = 'A' AND y = 'b'"));

            // Index based query for x and a
            assertEquals(2, execute("SELECT * FROM %s WHERE x : 'a'").size());
            assertEquals(2, execute("SELECT * FROM %s WHERE a CONTAINS 'd'").size());

            // Setting this as a proper exception so that we get a good error back to the client. It has
            // been failing for a while, so this at least produces a nice error message.
            assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE z : 'c'"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining(String.format(SingleColumnRestriction.AnalyzerMatchesRestriction.CANNOT_BE_RESTRICTED_BY_CLUSTERING_ERROR, "z"));

            // Expect failure for eq on partition key column since it is interpreted as eq and forms an incomplete
            // partition key restriction.
            assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE x = 'a'"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance.");

            assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE z = 'c'"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("Column 'z' has an index but does not support the operators specified in the query. " +
                                  "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING");
        });
    }

    @Test
    public void testCompoundPrimaryKeyRestrictionsWithAnalyzerEqOperatorSupportMatch() throws Throwable
    {
        createTable("CREATE TABLE %s (x text, y text, z text, a set<text>, PRIMARY KEY ((x, y), z))");

        // Create case-insensitive index on all columns except y since it is essentially the same as x here
        createIndex(createCaseInsensitiveIndexString("x", AnalyzerEqOperatorSupport.Value.MATCH));
        createIndex(createCaseInsensitiveIndexString("z", AnalyzerEqOperatorSupport.Value.MATCH));
        createIndex(createCaseInsensitiveIndexString("values(a)", AnalyzerEqOperatorSupport.Value.MATCH));

        // Set up two unique rows that will map to the same index terms
        execute("INSERT INTO %s (x, y, z, a) VALUES (?, ?, ?, ?)", "a", "b", "c", set("d", "e", "f"));
        execute("INSERT INTO %s (x, y, z, a) VALUES (?, ?, ?, ?)", "A", "B", "C", set("D", "E", "F"));

        beforeAndAfterFlush(
        () -> {
            // Fully qualified partition key
            assertRows(execute("SELECT x FROM %s WHERE x = 'a' AND y = 'b'"), row("a"));
            assertRows(execute("SELECT x FROM %s WHERE x = 'a' AND y = 'b' AND z = 'c'"), row("a"));
            assertRows(execute("SELECT x FROM %s WHERE x = 'A' AND y = 'B'"), row("A"));
            assertRows(execute("SELECT x FROM %s WHERE x = 'A' AND y = 'B' AND z = 'C'"), row("A"));
            assertRows(execute("SELECT * FROM %s WHERE x = 'A' AND y = 'b'"));

            // EQ gets interpreted as a match query here
            assertRows(execute("SELECT x FROM %s WHERE x = 'a'"), row("A"), row("a"));

            // != compares raw values on primary key columns
            assertRows(execute("SELECT x FROM %s WHERE x != 'a' ALLOW FILTERING"), row("A"));
            assertRows(execute("SELECT x FROM %s WHERE z != 'c' ALLOW FILTERING"), row("A"));
            // This only gets 1 row instead of 2 because the index is on a clustering column and that applies post
            // filtering in surprising and problematic way.
            assertRows(execute("SELECT x FROM %s WHERE z = 'c'"), row("a"));

            // Index based query for x, z, and a
            assertRows(execute("SELECT x FROM %s WHERE x : 'a'"), row("A"), row("a"));
            assertRows(execute("SELECT x FROM %s WHERE a CONTAINS 'd'"), row("A"), row("a"));

            // Setting this as a proper exception so that we get a good error back to the client. It has
            // been failing for a while, so this at least produces a nice error message.
            assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE z : 'c'"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining(String.format(SingleColumnRestriction.AnalyzerMatchesRestriction.CANNOT_BE_RESTRICTED_BY_CLUSTERING_ERROR, "z"));
        });
    }

    private String createCaseInsensitiveIndexString(String column, AnalyzerEqOperatorSupport.Value eqBehaviour)
    {
        return "CREATE CUSTOM INDEX ON %s(" + column + ") " +
               "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
               "WITH OPTIONS = { 'case_sensitive': false, 'equals_behaviour_when_analyzed': '" + eqBehaviour + "'}";
    }

    private void assertIndexDoesNotSupportEquals()
    {
        // the EQ query should not be supported by the index
        String query = "SELECT k FROM %s WHERE v = 'Quick fox'";
        assertInvalidMessage("Column 'v' has an index but does not support the operators specified in the query", query);

        // the EQ query should stil be supported with filtering without index intervention
        assertRowsWithoutWarning(query + " ALLOW FILTERING", row(1));

        // the EQ query should not use any kind of transformation when filtered without index intervention
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'quick fox' ALLOW FILTERING");
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v = 'fox' ALLOW FILTERING");

        // nor the IN query, a disjunction of EQs
        query = "SELECT k FROM %s WHERE v IN ('Quick fox', 'lazy fox')";
        assertInvalidMessage("Column 'v' has an index but does not support the operators specified in the query", query);
        assertRowsWithoutWarning(query + " ALLOW FILTERING", row(1));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v IN ('quick', 'fox') ALLOW FILTERING");
        assertRows(execute("SELECT k FROM %s WHERE v IN ? ALLOW FILTERING", list("Quick fox", "lazy fox")), row(1));

        // nor != and NOT IN, their negations
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v != 'quick fox' ALLOW FILTERING", row(1), row(2));
        assertRowsWithoutWarning("SELECT k FROM %s WHERE v NOT IN ('Quick fox', 'lazy fox') ALLOW FILTERING", row(2));
        assertRowsWithoutWarning("UPDATE %s SET v = 'Quick fox' WHERE k = 1 IF v != 'quick fox'", row(true));
    }

    private void assertIndexDoesNotSupportMatches()
    {
        String query = "SELECT k FROM %s WHERE v : 'Quick fox'";
        String errorMessage = "Index on column v does not support ':' restrictions.";
        assertInvalidMessage(errorMessage, query);
        assertInvalidMessage(errorMessage, query + " ALLOW FILTERING");
    }

    private void assertIndexThrowsNotAnalyzedError(String indexOptions)
    {
        assertInvalidMessage(AnalyzerEqOperatorSupport.NOT_ANALYZED_ERROR,
                             "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS =" + indexOptions);
    }

    private void assertIndexThrowsUnrecognizedOptionError(String indexOptions)
    {
        assertInvalidMessage(AnalyzerEqOperatorSupport.WRONG_OPTION_ERROR,
                             "CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS =" + indexOptions);
    }

    private void assertIndexQueries(String indexOptions, Runnable queries)
    {
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = " + indexOptions);
        populateTable();

        queries.run();
        flush();
        queries.run();
    }

    private void assertRowsWithoutWarning(String query, Object[]... rows)
    {
        assertRows(query, rows).isNullOrEmpty();
    }

    private void assertRowsWithSelectWarning(String query, Object[]... rows)
    {
        assertRows(query, rows).hasSize(1).contains(format(EQ_RESTRICTION_ON_ANALYZED_WARNING, 'v', currentIndex()));
    }

    private void assertRowsWithLWTWarning(String query, Object[]... rows)
    {
        assertRows(query, rows).hasSize(1).contains(format(LWT_CONDITION_ON_ANALYZED_WARNING, 'v'));
    }

    private ListAssert<String> assertRows(String query, Object[]... rows)
    {
        ClientWarn.instance.captureWarnings();
        CQLTester.disablePreparedReuseForTest();
        assertRows(execute(query), rows);
        ListAssert<String> assertion = Assertions.assertThat(ClientWarn.instance.getWarnings());
        ClientWarn.instance.resetWarnings();
        return assertion;
    }
}
