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
package org.apache.cassandra.db.filter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests which row filters can be sent to nodes on a messaging version below DS 10, such as Apache Cassandra 5.0 nodes
 * (see {@link RowFilter#dataStaxOnlyFeature()} and {@link RowFilter#validateForPeers}): those that only use
 * expression kinds and operators that Apache Cassandra 5.0 reads and evaluates the same way are serialized with only
 * those, and the others are refused.
 */
public class RowFilterPeerCompatibilityTest extends SAITester
{
    private static final int APACHE_50 = MessagingService.VERSION_50;

    @Test
    public void testFilters() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, n int, t text, a text, b text, u text, " +
                    "m map<text, int>, s set<int>, vec vector<float, 2>, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': 'false'}");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        createIndex("CREATE CUSTOM INDEX ON %s(u) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'case_sensitive': 'false', 'equals_behaviour_when_analyzed': 'unsupported'}");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(m)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");

        // query -> the DataStax-only feature it uses, or null if Apache Cassandra 5.0 nodes can use it
        Map<String, String> queries = new LinkedHashMap<>();
        queries.put("SELECT * FROM %s WHERE n = 1", null);
        queries.put("SELECT * FROM %s WHERE n > 1 AND n <= 5", null);
        queries.put("SELECT * FROM %s WHERE n IN (1, 2)", null);
        queries.put("SELECT * FROM %s WHERE n NOT IN (1, 2)", null); // sent as != expressions
        queries.put("SELECT * FROM %s WHERE n != 1", null);
        queries.put("SELECT * FROM %s WHERE t = 'x'", null);
        queries.put("SELECT * FROM %s WHERE a = 'X'", null); // sent as an analyzed equality
        queries.put("SELECT * FROM %s WHERE a : 'X'", null); // same
        queries.put("SELECT * FROM %s WHERE m['a'] = 1", null);
        queries.put("SELECT * FROM %s WHERE s CONTAINS 1", null);
        queries.put("SELECT * FROM %s WHERE m CONTAINS KEY 'a' ALLOW FILTERING", null);
        queries.put("SELECT * FROM %s WHERE k = 1 AND n = 1", null);
        queries.put("SELECT * FROM %s WHERE token(k) > 0 AND n = 1", null);
        queries.put("SELECT * FROM %s WHERE n = 1 AND (t = 'x' AND c = 1)", null);
        queries.put("SELECT * FROM %s ORDER BY vec ANN OF [1, 1] LIMIT 1", null);
        queries.put("SELECT * FROM %s WHERE n = 1 ORDER BY vec ANN OF [1, 1] LIMIT 1", null);
        queries.put("SELECT * FROM %s WHERE k = 1 AND c > 1 ORDER BY vec ANN OF [1, 1] LIMIT 1", null);
        queries.put("SELECT * FROM %s WHERE m['a'] > 1", "The > operator on a map entry");
        queries.put("SELECT * FROM %s WHERE m['a'] >= 1 AND m['a'] < 5", "operator on a map entry");
        queries.put("SELECT * FROM %s WHERE m['a'] != 1", "The != operator on a map entry");
        queries.put("SELECT * FROM %s WHERE t LIKE 'abc'", "The LIKE operator");
        queries.put("SELECT * FROM %s WHERE t LIKE 'abc%%'", "The LIKE operator");
        queries.put("SELECT * FROM %s WHERE t LIKE '%%abc%%'", "The LIKE operator");
        queries.put("SELECT * FROM %s WHERE t LIKE 'a_c'", "The LIKE operator");
        queries.put("SELECT * FROM %s WHERE b = 'x'", "The : operator (and = on an analyzed index)");
        queries.put("SELECT * FROM %s WHERE b : 'x'", "The : operator (and = on an analyzed index)");
        // an index with equals_behaviour_when_analyzed: unsupported can't serve = in place of :
        queries.put("SELECT * FROM %s WHERE u : 'x'", "The : operator (and = on an analyzed index)");
        queries.put("SELECT * FROM %s WHERE s NOT CONTAINS 1", "The NOT CONTAINS operator");
        queries.put("SELECT * FROM %s WHERE n = 1 OR n = 2", "The OR operator");
        queries.put("SELECT * FROM %s ORDER BY n LIMIT 1", "ORDER BY on a non-clustering column");
        queries.put("SELECT * FROM %s WHERE GEO_DISTANCE(vec, [1, 1]) < 1000", "GEO_DISTANCE");

        // check every query before failing, to report all of them
        List<String> failures = new ArrayList<>();
        for (Map.Entry<String, String> entry : queries.entrySet())
        {
            String query = entry.getKey();
            try
            {
                RowFilter filter = rowFilter(query);
                assertThat(filter.isEmpty()).as(query).isFalse();

                String feature = entry.getValue();
                if (feature == null)
                {
                    assertThat(filter.dataStaxOnlyFeature()).as(query).isNull();
                    assertSerializedForApache(query, filter);
                }
                else
                {
                    assertThat(filter.dataStaxOnlyFeature()).as(query).contains(feature);
                    // even if the coordinator doesn't know that a node is below DS 10, the filter is not sent to it;
                    // computing its size doesn't fail though, as messaging does that when a serialization fails
                    RowFilter.serializer.serializedSize(filter, APACHE_50);
                    assertThatThrownBy(() -> serialize(filter, APACHE_50)).as(query)
                                                                          .isInstanceOf(InvalidRequestException.class)
                                                                          .hasMessageContaining(feature);
                }
            }
            catch (AssertionError e)
            {
                failures.add(e.getMessage());
            }
        }
        assertThat(failures).isEmpty();
    }

    /**
     * Checks that the given filter is serialized for Apache Cassandra 5.0 nodes with only the expression kinds that it
     * reads as we mean them (simple, map equality, custom and user expressions) and the operators it knows.
     */
    private void assertSerializedForApache(String query, RowFilter filter) throws Throwable
    {
        byte[] bytes = serialize(filter, APACHE_50);
        RowFilter deserialized;
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            deserialized = RowFilter.serializer.deserialize(in, APACHE_50, currentTableMetadata(), filter.needsReconciliation());
        }

        assertThat(deserialized.root().children()).as(query).isEmpty();
        assertThat(deserialized.expressions()).as(query).hasSameSizeAs(filter.expressions());
        for (RowFilter.Expression expression : deserialized.expressions())
        {
            assertThat(expression.kind()).as(query).isIn(RowFilter.Expression.Kind.SIMPLE,
                                                         RowFilter.Expression.Kind.MAP_COMPARISON,
                                                         RowFilter.Expression.Kind.CUSTOM,
                                                         RowFilter.Expression.Kind.USER);
            Operator operator = expression.operator();
            assertThat(operator.getValue()).as(query).isBetween(0, Operator.ANN.getValue());
            assertThat(operator.isLike()).as(query).isFalse();
            if (expression.kind() == RowFilter.Expression.Kind.MAP_COMPARISON)
                assertThat(operator).as(query).isEqualTo(Operator.EQ);
        }

        // the analyzer matches of a non-tokenizing index are sent as the analyzed equalities they are
        if (filter.expressions().stream().anyMatch(e -> e.operator() == Operator.ANALYZER_MATCHES))
            assertThat(deserialized.expressions().stream().map(RowFilter.Expression::operator)).as(query).contains(Operator.EQ);
    }

    private RowFilter rowFilter(String query)
    {
        SelectStatement statement = (SelectStatement) QueryProcessor.prepareInternal(formatQuery(query)).statement;
        return statement.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds()).rowFilter();
    }

    private static byte[] serialize(RowFilter filter, int version) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RowFilter.serializer.serializedSize(filter, version);
            RowFilter.serializer.serialize(filter, out, version);
            return out.toByteArray();
        }
    }
}
