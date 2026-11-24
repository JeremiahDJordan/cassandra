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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.exceptions.IncompatibleMessagingVersionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
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
        queries.put("SELECT * FROM %s WHERE a != 'X' ALLOW FILTERING", "A != or NOT IN on an analyzed index");
        queries.put("SELECT * FROM %s WHERE a NOT IN ('X', 'Y') ALLOW FILTERING", "A != or NOT IN on an analyzed index");
        queries.put("SELECT * FROM %s WHERE b != 'x' ALLOW FILTERING", "A != or NOT IN on an analyzed index");
        queries.put("SELECT * FROM %s WHERE u != 'X' ALLOW FILTERING", null); // compares raw values
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
     * When SAI filters the read, a != or a NOT IN with a restriction that selects an index on nodes below DS 10 isn't
     * sent to them, as their SAI ignores it, nor a != in a filter that needs reconciliation and intersects mutable
     * columns, which they apply strictly without SAI, nor an IN in a filter that they relax, which their SAI refuses,
     * nor a CONTAINS or CONTAINS KEY on a frozen collection, which their SAI compares with the whole collection or
     * refuses. Other filters with a !=, and filters that SAI doesn't serve, are sent.
     */
    @Test
    public void testSAIFiltering() throws Throwable
    {
        // reads need reconciliation only with several replicas
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
        createTable(keyspace, "CREATE TABLE %s (k int, c int, d int, x int, y int, z int, w int, s set<int>, st int static, " +
                                      "fs frozen<set<int>>, fm frozen<map<text, int>>, fl frozen<list<int>>, PRIMARY KEY (k, c, d))");
        createIndex(keyspace, "CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        createIndex(keyspace, "CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex(keyspace, "CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex'");
        createIndex(keyspace, "CREATE INDEX ON %s(z) USING 'legacy_local_table'");

        String nonStrictIn = String.format(RowFilter.SAI_NON_STRICT_OPERATOR, "IN");
        String frozenElement = RowFilter.FROZEN_COLLECTION_ELEMENT_RESTRICTION;
        Object[][] queries = {
            { "SELECT * FROM %s WHERE x = 1 AND y != 0 ALLOW FILTERING", ConsistencyLevel.ONE, RowFilter.SAI_EXCLUSION },
            { "SELECT * FROM %s WHERE x > 0 AND x != 2", ConsistencyLevel.ONE, RowFilter.SAI_EXCLUSION },
            { "SELECT * FROM %s WHERE x = 1 AND y NOT IN (0, 2) ALLOW FILTERING", ConsistencyLevel.ONE, RowFilter.SAI_EXCLUSION },
            { "SELECT * FROM %s WHERE k = 0 AND x = 1 AND y != 0 ALLOW FILTERING", ConsistencyLevel.ONE, RowFilter.SAI_EXCLUSION },
            { "SELECT * FROM %s WHERE s CONTAINS 1 AND y != 0 ALLOW FILTERING", ConsistencyLevel.QUORUM, RowFilter.SAI_EXCLUSION },
            { "SELECT * FROM %s WHERE x != 1 AND s CONTAINS 1 ALLOW FILTERING", ConsistencyLevel.ONE, RowFilter.SAI_EXCLUSION },
            { "SELECT * FROM %s WHERE x != 1", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE x NOT IN (1, 2)", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE x != 1 AND y != 0 ALLOW FILTERING", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE x != 1 AND y != 0 ALLOW FILTERING", ConsistencyLevel.QUORUM, RowFilter.SAI_EXCLUSION },
            { "SELECT * FROM %s WHERE x NOT IN (1, 2)", ConsistencyLevel.ALL, null },
            // z has a legacy index, w none
            { "SELECT * FROM %s WHERE z = 1 AND y != 0 ALLOW FILTERING", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE w = 1 AND y != 0 ALLOW FILTERING", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE w != 1 AND y != 0 ALLOW FILTERING", ConsistencyLevel.ALL, null },
            // their SAI serves no restriction of these
            { "SELECT * FROM %s WHERE x != 1 AND w = 0 ALLOW FILTERING", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE x != 1 AND z = 0 ALLOW FILTERING", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE x != 1 AND y IN (0, 2) ALLOW FILTERING", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE x != 1 AND st IN (1, 2) ALLOW FILTERING", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE x != 1 AND w = 0 ALLOW FILTERING", ConsistencyLevel.QUORUM, RowFilter.SAI_EXCLUSION },
            { "SELECT * FROM %s WHERE w = 1 AND y IN (0, 2) ALLOW FILTERING", ConsistencyLevel.ALL, null },
            { "SELECT * FROM %s WHERE x = 1 AND y IN (0, 2) ALLOW FILTERING", ConsistencyLevel.QUORUM, nonStrictIn },
            { "SELECT * FROM %s WHERE k = 0 AND x = 1 AND y IN (0, 2) ALLOW FILTERING", ConsistencyLevel.ALL, nonStrictIn },
            { "SELECT * FROM %s WHERE x = 1 AND y IN (0, 2) ALLOW FILTERING", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE x IN (1, 2)", ConsistencyLevel.QUORUM, null },
            { "SELECT * FROM %s WHERE x IN (1, 2) AND y = 0 ALLOW FILTERING", ConsistencyLevel.ALL, nonStrictIn },
            { "SELECT * FROM %s WHERE x > 0 AND x < 5 AND d IN (1, 2) ALLOW FILTERING", ConsistencyLevel.QUORUM, nonStrictIn },
            { "SELECT * FROM %s WHERE st = 1 AND d IN (1, 2) ALLOW FILTERING", ConsistencyLevel.ALL, nonStrictIn },
            { "SELECT * FROM %s WHERE st = 1 AND d IN (1, 2) ALLOW FILTERING", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE d IN (1, 2) AND s CONTAINS 1 AND s CONTAINS 2 ALLOW FILTERING", ConsistencyLevel.QUORUM, nonStrictIn },
            { "SELECT * FROM %s WHERE d IN (1, 2) AND s CONTAINS 1 ALLOW FILTERING", ConsistencyLevel.QUORUM, null },
            { "SELECT * FROM %s WHERE z = 1 AND y IN (0, 2) ALLOW FILTERING", ConsistencyLevel.QUORUM, null },
            { "SELECT * FROM %s WHERE x = 1 AND fs CONTAINS 1 ALLOW FILTERING", ConsistencyLevel.ONE, frozenElement },
            { "SELECT * FROM %s WHERE x = 1 AND fm CONTAINS KEY 'a' ALLOW FILTERING", ConsistencyLevel.ONE, frozenElement },
            { "SELECT * FROM %s WHERE x = 1 AND fm CONTAINS 1 ALLOW FILTERING", ConsistencyLevel.ONE, frozenElement },
            { "SELECT * FROM %s WHERE s CONTAINS 1 AND fl CONTAINS 1 ALLOW FILTERING", ConsistencyLevel.ONE, frozenElement },
            { "SELECT * FROM %s WHERE x = 1 AND fs = {1, 2} ALLOW FILTERING", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE fs CONTAINS 1 ALLOW FILTERING", ConsistencyLevel.ONE, null },
            { "SELECT * FROM %s WHERE z = 1 AND fs CONTAINS 1 ALLOW FILTERING", ConsistencyLevel.ONE, null },
        };

        // as while nodes below DS 10 can be contacted, the coordinator lets replicas relax filters
        boolean nonStrict = DatabaseDescriptor.getSAINonStrictReplicaFilteringEnabled();
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(true);
        List<String> failures = new ArrayList<>();
        try
        {
            for (Object[] query : queries)
            {
                String what = query[0] + " at " + query[1];
                String feature = (String) query[2];
                try
                {
                    ReadCommand command = readCommand(formatQuery(keyspace, (String) query[0]), (ConsistencyLevel) query[1]);
                    assertThat(command.rowFilter().dataStaxOnlyFeature()).as(what).isNull();
                    assertThat(command.rowFilter().dataStaxOnlyFeature(command.isFilteredBySAI())).as(what).isEqualTo(feature);
                    serialize(command, MessagingService.VERSION_DS_20);
                    if (feature == null)
                    {
                        serialize(command, APACHE_50);
                    }
                    else
                    {
                        assertThatThrownBy(() -> serialize(command, APACHE_50)).as(what)
                                                                               .isInstanceOf(IncompatibleMessagingVersionException.class)
                                                                               .hasMessageContaining(feature);
                    }
                }
                catch (AssertionError e)
                {
                    failures.add(e.getMessage());
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(nonStrict);
        }
        assertThat(failures).isEmpty();
    }

    /**
     * LIKE isn't sent to nodes below DS 21, which may evaluate it with other semantics, unless
     * new_feature_peer_version_check is none. Computing the size of the filter doesn't fail.
     */
    @Test
    public void testLikeForNodesBelowDS21() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t text)");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex'");

        Config.NewFeaturePeerVersionCheck check = DatabaseDescriptor.getNewFeaturePeerVersionCheck();
        try
        {
            for (String query : new String[]{ "SELECT * FROM %s WHERE t LIKE 'abc'",
                                              "SELECT * FROM %s WHERE t LIKE 'a%%c'",
                                              "SELECT * FROM %s WHERE t LIKE 'abc' AND k = 1" })
            {
                RowFilter filter = rowFilter(query);
                serialize(filter, MessagingService.VERSION_DS_21);
                assertReadRequestSize(query, MessagingService.VERSION_DS_21);
                for (int version : new int[]{ MessagingService.VERSION_DS_10, MessagingService.VERSION_DS_12,
                                              MessagingService.VERSION_DS_20, MessagingService.VERSION_DSE_68 })
                {
                    DatabaseDescriptor.setNewFeaturePeerVersionCheck(Config.NewFeaturePeerVersionCheck.messaging);
                    RowFilter.serializer.serializedSize(filter, version);
                    assertThatThrownBy(() -> serialize(filter, version))
                        .as(query + " for " + version)
                        .isInstanceOf(IncompatibleMessagingVersionException.class)
                        .hasMessage("Cannot serialize a row filter for messaging version " + version +
                                    ": the LIKE operator needs DS 21");

                    DatabaseDescriptor.setNewFeaturePeerVersionCheck(Config.NewFeaturePeerVersionCheck.none);
                    serialize(filter, version);
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setNewFeaturePeerVersionCheck(check);
        }
    }

    /**
     * A != from a node below DS 10 compares raw values, as on that node, also where an analyzed index serves = as a match.
     */
    @Test
    public void testNotEqualsFromNodesBelowDS10() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String query = "SELECT * FROM %s WHERE v != 'X' ALLOW FILTERING";
        byte[] bytes = serialize(rowFilter(query), APACHE_50);
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': 'false'}");

        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            RowFilter filter = RowFilter.serializer.deserialize(in, APACHE_50, currentTableMetadata(), false);
            assertThat(filter.expressions().get(0).analyzer()).isNull();
        }

        bytes = serialize(rowFilter(query), MessagingService.current_version);
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            RowFilter filter = RowFilter.serializer.deserialize(in, MessagingService.current_version, currentTableMetadata(), false);
            assertThat(filter.expressions().get(0).analyzer()).isNotNull();
        }
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

    /**
     * Checks that a read request with the given query is serialized to the size computed for it, which messaging
     * computes for DS 21 as for DS 20.
     */
    private void assertReadRequestSize(String query, int version) throws Exception
    {
        ReadCommand command = readCommand(formatQuery(query), ConsistencyLevel.ONE);
        Message<ReadCommand> message = Message.out(Verb.READ_REQ, command);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            int size = message.serializedSize(version);
            Message.serializer.serialize(message, out, version);
            assertThat(out.getLength()).as(query).isEqualTo(size);
        }
    }

    private static ReadCommand readCommand(String query, ConsistencyLevel consistency)
    {
        SelectStatement statement = (SelectStatement) QueryProcessor.prepareInternal(query).statement;
        QueryOptions options = QueryOptions.forInternalCalls(consistency, Collections.emptyList());
        ReadQuery readQuery = statement.getQuery(options, FBUtilities.nowInSeconds());
        return readQuery instanceof SinglePartitionReadQuery.Group
               ? (ReadCommand) ((SinglePartitionReadQuery.Group<?>) readQuery).queries.get(0)
               : (ReadCommand) readQuery;
    }

    private static void serialize(ReadCommand command, int version) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadCommand.serializer.serializedSize(command, version);
            ReadCommand.serializer.serialize(command, out, version);
        }
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
