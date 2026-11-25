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

package org.apache.cassandra.index.sai.cql;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.analyzer.NonTokenizingOptions;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;

public class AnalyzerTest extends SAITester
{
    @Test
    public void createAnalyzerWrongTypeTest()
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 text, val int, val2 int, PRIMARY KEY((pk1, pk2)))");
        createIndex("CREATE CUSTOM INDEX ON %s(pk1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(pk2) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk1, pk2, val) VALUES (-1, 'b', 1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (0, 'b', 2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, 'b', 3)");

        execute("INSERT INTO %s (pk1, pk2, val) VALUES (-1, 'a', -1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (0, 'a', -2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, 'a', -3)");

        flush();

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'};");

        execute("INSERT INTO %s (pk1, pk2, val) VALUES (-1, 'd', 1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (0, 'd', 2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, 'd', 3)");

        execute("INSERT INTO %s (pk1, pk2, val) VALUES (-1, 'c', -1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (0, 'c', -2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, 'c', -3)");
    }

    /**
     * Test that we cannot use an analyzer, tokenizing or not, on a frozen collection.
     */
    @Test
    public void analyzerOnFrozenCollectionTest()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l frozen<list<text>>, s frozen<set<text>>, m frozen<map<text, text>>)");

        for (String column : Arrays.asList("l", "s", "m"))
        {
            assertRejectsNonFullIndexCreationOnFrozenCollection(column);
            column = String.format("full(%s)", column);

            // non-tokenizing options that produce an analyzer should be rejected
            assertRejectsAnalyzerOnFrozenCollection(column, String.format("{'%s': %s}", NonTokenizingOptions.CASE_SENSITIVE, false));
            assertRejectsAnalyzerOnFrozenCollection(column, String.format("{'%s': %s}", NonTokenizingOptions.NORMALIZE, true));
            assertRejectsAnalyzerOnFrozenCollection(column, String.format("{'%s': %s}", NonTokenizingOptions.ASCII, true));

            // non-tokenizing options that do not produce an analyzer should be accepted
            assertAcceptsIndexOptions(column, String.format("{'%s': %s}", NonTokenizingOptions.CASE_SENSITIVE, true));
            assertAcceptsIndexOptions(column, String.format("{'%s': %s}", NonTokenizingOptions.NORMALIZE, false));
            assertAcceptsIndexOptions(column, String.format("{'%s': %s}", NonTokenizingOptions.ASCII, false));

            // Lucene analyzer should always be rejected
            assertRejectsAnalyzerOnFrozenCollection(column, "{'index_analyzer': 'standard'}");
            assertRejectsAnalyzerOnFrozenCollection(column,
                    "{'index_analyzer': " +
                    "  '{\"tokenizer\":{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"3\"}}," +
                    "    \"filters\":[{\"name\":\"lowercase\"}]}'}");
            assertRejectsAnalyzerOnFrozenCollection(column,
                    "{'index_analyzer':'\n" +
                            "  {\"tokenizer\":{\"name\" : \"whitespace\"},\n" +
                            "   \"filters\":[{\"name\":\"stop\", \"args\": {\"words\": \"the, test\", \"format\": \"wordset\"}}]}'}");

            // no options should be accepted
            assertAcceptsIndexOptions(column, null);
            assertAcceptsIndexOptions(column, "{}");
        }
    }

    private void assertRejectsNonFullIndexCreationOnFrozenCollection(String column)
    {
        Assertions.assertThatThrownBy(() -> createSAIIndex(column, null))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining("Cannot create values() index on frozen column " + column);

        Assertions.assertThatThrownBy(() -> createSAIIndex("KEYS(" + column + ')', null))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining("Cannot create keys() index on frozen column " + column);

        Assertions.assertThatThrownBy(() -> createSAIIndex("VALUES(" + column + ')', null))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining("Cannot create values() index on frozen column " + column);
    }

    private void assertAcceptsIndexOptions(String column, @Nullable String options)
    {
        String index = createSAIIndex(column, options);
        dropIndex("DROP INDEX %s." + index); // clear for further tests
    }

    private void assertRejectsAnalyzerOnFrozenCollection(String column, String options)
    {
        Assertions.assertThatThrownBy(() -> createSAIIndex(column, options))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining("Cannot use an analyzer on " + column + " because it's a frozen collection.");
    }

    @Test
    public void testEmptyOnInitialBuild()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (k, v) VALUES (1, '')");
        flush();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                    "'index_analyzer': 'standard'};");
    }

    @Test
    public void testEmptyOnCompaction()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                    "'index_analyzer': 'standard'};");
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple orange')");
        flush();
        execute("INSERT INTO %s (k, v) VALUES (1, '')");
        flush();
        compact();
    }

    @Test
    public void testEmptyWithStopwords()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (k, v) VALUES (1, 'and then')");  // will yield no indexed terms
        flush();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                    "'index_analyzer': 'english'};");
        assertEmpty(execute("SELECT * FROM %s WHERE v = 'and'"));
    }

    /**
     * A match of a value that the query analyzer makes no terms of matches no row, and its negation every row, also
     * with an index analyzer that makes the empty term of an empty value.
     */
    @Test
    public void testMatchOfValueWithoutTerms() throws Throwable
    {
        for (String analyzer : new String[]{ "'case_sensitive': 'false'", "'index_analyzer': 'keyword'" })
        {
            String options = "{" + analyzer + ", 'query_analyzer': 'standard'}";
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, w text, i int, u int, l list<text>, m map<text, int>)");
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = " + options);
            createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex' WITH OPTIONS = " + options);
            createIndex("CREATE CUSTOM INDEX ON %s(KEYS(m)) USING 'StorageAttachedIndex' WITH OPTIONS = " + options);
            createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex' WITH OPTIONS = {" + analyzer + '}');
            createIndex("CREATE CUSTOM INDEX ON %s(i) USING 'StorageAttachedIndex'");
            execute("INSERT INTO %s (k, v, w, i, u, l, m) VALUES (1, '', '', 1, 1, [''], {'': 1})");
            execute("INSERT INTO %s (k, v, i, u, l, m) VALUES (2, 'x', 2, 2, ['x'], {'x': 2})");
            execute("INSERT INTO %s (k, i, m) VALUES (3, 1, {'y': 3})");

            beforeAndAfterFlush(() -> {
                assertMatches("v = '!!!'");
                assertMatches("v : '!!!'");
                assertMatches("v = ''");
                assertMatches("v = '!!!' AND i = 1");
                assertMatches("v = '!!!' OR i = 2", row(2));
                assertMatches("v : '!!!' OR v = 'x'", row(2));
                assertMatches("l CONTAINS '!!!'");
                assertMatches("l CONTAINS '!!!' OR i = 2", row(2));
                assertMatches("i = 1 AND (v = '!!!' OR v : '!!!' OR l CONTAINS '!!!' OR m CONTAINS KEY '!!!' OR u = 2) ALLOW FILTERING");
                assertMatches("m CONTAINS KEY '!!!'");
                assertMatches("l NOT CONTAINS '!!!'", row(1), row(2), row(3));
                assertMatches("l NOT CONTAINS '!!!' AND i = 1", row(1), row(3));
                assertMatches("m NOT CONTAINS KEY '!!!'", row(1), row(2), row(3));
                assertMatches("w = ''", row(1));
            });
        }
    }

    private void assertMatches(String where, Object[]... rows)
    {
        String query = "SELECT k FROM %s WHERE " + where;
        assertRowsIgnoringOrder(executeInternal(query), rows);
        assertRowsIgnoringOrder(execute(query), rows);
    }
}
