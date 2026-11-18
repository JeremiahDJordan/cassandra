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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that a replica which does not know the index a read command was built with answers the read without the
 * index, rather than failing to deserialize the command, or fails it as an index not available if only an index can
 * evaluate it.
 */
public class ReadCommandUnknownIndexTest extends CQLTester
{
    @Test
    public void testDeserializeWithUnknownIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String index = createIndex("CREATE INDEX ON %s (v)");

        ReadCommand command = parseReadCommand("SELECT * FROM %s WHERE v = 1");
        assertNotNull(command.indexQueryPlan());

        DataOutputBuffer out = new DataOutputBuffer();
        ReadCommand.serializer.serialize(command, out, MessagingService.current_version);

        // the replica the command reaches has not seen the index yet, which the drop stands in for
        dropIndex("DROP INDEX %s." + index);

        try (DataInputBuffer in = new DataInputBuffer(out.getData()))
        {
            ReadCommand deserialized = ReadCommand.serializer.deserialize(in, MessagingService.current_version);
            assertNull(deserialized.indexQueryPlan());
        }
    }

    @Test
    public void testExecuteWithUnknownSAIIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (0, 1)");
        execute("INSERT INTO %s (k, v) VALUES (1, 2)");
        String index = createIndex("CREATE INDEX ON %s (v) USING 'sai'");

        ReadCommand command = deserializeWithoutIndex("SELECT * FROM %s WHERE v = 1", index);
        assertEquals(1, Util.getAll(command).size());
    }

    @Test
    public void testExecuteWithUnknownSAIIndexOnlyOperators() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t text, vec vector<float, 2>)");
        execute("INSERT INTO %s (k, t, vec) VALUES (0, 'a b', [1.0, 2.0])");
        String textIndex = createIndex("CREATE CUSTOM INDEX ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");

        assertIndexNotAvailable(deserializeWithoutIndex("SELECT * FROM %s WHERE t : 'a'", textIndex));
        textIndex = createIndex("CREATE CUSTOM INDEX ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        assertIndexNotAvailable(deserializeWithoutIndex("SELECT * FROM %s ORDER BY t BM25 OF 'a' LIMIT 1", textIndex));

        String vectorIndex = createIndex("CREATE INDEX ON %s (vec) USING 'sai'");
        assertIndexNotAvailable(deserializeWithoutIndex("SELECT * FROM %s ORDER BY vec ANN OF [1.0, 2.0] LIMIT 1", vectorIndex));
    }

    @Test
    public void testExecuteWithUnknownSAIIndexAndOrderBy() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (0, 1)");
        assertOrderByIndexNotAvailable();
    }

    @Test
    public void testExecuteWithUnknownSAIIndexAndOrderByOnEmptyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertOrderByIndexNotAvailable();
    }

    @Test
    public void testExecuteWithFullPrimaryKeyAndAnalyzerMatch() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t text)");
        execute("INSERT INTO %s (k, t) VALUES (0, 'Quick brown fox')");
        String tIndex = createIndex("CREATE CUSTOM INDEX ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        waitForTableIndexesQueryable();
        ReadCommand command = parseReadCommand("SELECT * FROM %s WHERE k = 0 AND t : 'quick'");
        assertNull(command.indexQueryPlan());
        byte[] serialized = serialize(command);
        assertEquals(1, Util.getAll(deserialize(serialized)).size());

        // the replica has no index on t, so no analyzer for the analyzer match
        dropIndex("DROP INDEX %s." + tIndex);
        assertIndexNotAvailable(deserialize(serialized));
    }

    @Test
    public void testExecuteWithAnalyzerMatchUserExpression() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t text)");
        execute("INSERT INTO %s (k, t) VALUES (0, 'Quick')");
        execute("INSERT INTO %s (k, t) VALUES (1, 'Slow')");
        assertEquals(1, Util.getAll(withCaseInsensitiveMatch(parseReadCommand("SELECT * FROM %s WHERE k = 0"), "quick")).size());
        assertEquals(0, Util.getAll(withCaseInsensitiveMatch(parseReadCommand("SELECT * FROM %s WHERE k = 1"), "quick")).size());
    }

    @Test
    public void testExecuteWithUnknownIndexAndOtherAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t text)");
        execute("INSERT INTO %s (k, t) VALUES (0, 'Quick brown fox')");
        String index = createIndex("CREATE CUSTOM INDEX new_idx ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        waitForTableIndexesQueryable();
        ReadCommand command = parseReadCommand("SELECT * FROM %s WHERE t : 'quick'");
        byte[] serialized = serialize(command);

        // this node has another index on the column, with another analyzer, but not the one the command was built with
        dropIndex("DROP INDEX %s." + index);
        createIndex("CREATE CUSTOM INDEX old_idx ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': 'false'}");
        waitForTableIndexesQueryable();
        assertIndexNotAvailable(deserialize(serialized));
    }

    @Test
    public void testExecuteWithKnownIndexAndUnknownIndexOnlyOperatorIndex() throws Throwable
    {
        assertIndexNotAvailableWithKnownIndex("vec vector<float, 2>", "[1.0, 2.0]",
                                              "CREATE INDEX ON %s (vec) USING 'sai'",
                                              "SELECT * FROM %s WHERE a = 1 ORDER BY vec ANN OF [1.0, 2.0] LIMIT 1");
        assertIndexNotAvailableWithKnownIndex("t text", "'quick brown fox'",
                                              "CREATE CUSTOM INDEX ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}",
                                              "SELECT * FROM %s WHERE a = 1 AND t : 'quick'");
    }

    @Test
    public void testExecuteWithNamedIndexOfOtherGroupAndOrderingIndex() throws Throwable
    {
        // the replica has a legacy index of the serialized index's name, which can't order by the vector index
        assertIndexNotAvailableWithKnownIndex("vec vector<float, 2>", "[1.0, 2.0]",
                                              "CREATE INDEX ON %s (vec) USING 'sai'",
                                              "SELECT * FROM %s WHERE a = 1 ORDER BY vec ANN OF [1.0, 2.0] LIMIT 1",
                                              (aIndex, vecIndex) -> {
                                                  dropIndex("DROP INDEX %s." + aIndex);
                                                  createIndex("CREATE INDEX " + aIndex + " ON %s (a) USING 'legacy_local_table'");
                                              });
    }

    @Test
    public void testExecuteWithExcludedOrderingIndex() throws Throwable
    {
        // the replica's only index on vec has the name of an index the command excludes
        assertIndexNotAvailableWithKnownIndex("vec vector<float, 2>, b int", "[1.0, 2.0]",
                                              "CREATE INDEX ON %s (vec) USING 'sai'",
                                              "SELECT * FROM %s WHERE a = 1 ORDER BY vec ANN OF [1.0, 2.0] LIMIT 1 WITH excluded_indexes = {old_idx}",
                                              (aIndex, vecIndex) -> {
                                                  dropIndex("DROP INDEX %s." + vecIndex);
                                                  dropIndex("DROP INDEX %s.old_idx");
                                                  createIndex("CREATE INDEX old_idx ON %s (vec) USING 'sai'");
                                              },
                                              "CREATE INDEX old_idx ON %s (b) USING 'sai'");
    }

    @Test
    public void testExecuteWithExcludedAnalyzerMatchIndex() throws Throwable
    {
        // the replica's only index on t has the name of an index the command excludes
        assertIndexNotAvailableWithKnownIndex("t text, b int", "'quick brown fox'",
                                              "CREATE CUSTOM INDEX ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}",
                                              "SELECT * FROM %s WHERE a = 1 AND t : 'quick' WITH excluded_indexes = {old_idx}",
                                              (aIndex, tIndex) -> {
                                                  dropIndex("DROP INDEX %s." + tIndex);
                                                  dropIndex("DROP INDEX %s.old_idx");
                                                  createIndex("CREATE CUSTOM INDEX old_idx ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
                                              },
                                              "CREATE INDEX old_idx ON %s (b) USING 'sai'");
    }

    @Test
    public void testExecuteWithAnalyzerMatchIndexOfOtherGroupAndNamedSAIIndex() throws Throwable
    {
        // the replica's only index on t is a custom index of another group, so the SAI plan of the index on a can't
        // evaluate the analyzer match
        assertIndexNotAvailableWithKnownIndex("t text", "'quick brown fox'",
                                              "CREATE CUSTOM INDEX ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}",
                                              "SELECT * FROM %s WHERE a = 1 AND t : 'quick'",
                                              (aIndex, tIndex) -> {
                                                  dropIndex("DROP INDEX %s." + tIndex);
                                                  createIndex("CREATE CUSTOM INDEX ON %s (t) USING '" + AnalyzerMatchStubIndex.class.getName() + '\'');
                                              });
    }

    @Test
    public void testExecuteWithNamedAnalyzerMatchIndexOfOtherGroupWithoutAnalyzer() throws Throwable
    {
        // the replica's index of the serialized index's name is a custom index of another group without an analyzer,
        // whose plan leaves the analyzer match to the row filter
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t text)");
        execute("INSERT INTO %s (k, t) VALUES (0, 'Quick brown fox')");
        String tIndex = createIndex("CREATE CUSTOM INDEX ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        waitForTableIndexesQueryable();
        byte[] serialized = serialize(parseReadCommand("SELECT * FROM %s WHERE t : 'quick'"));

        dropIndex("DROP INDEX %s." + tIndex);
        createIndex("CREATE CUSTOM INDEX " + tIndex + " ON %s (t) USING '" + AnalyzerMatchStubIndex.class.getName() + '\'');
        assertIndexNotAvailable(deserialize(serialized));
    }

    @Test
    public void testExecuteWithNamedIndexLeavingAnalyzedAnalyzerMatch() throws Throwable
    {
        // the named custom index leaves the analyzer match to the row filter with an analyzer of its own
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t text)");
        execute("INSERT INTO %s (k, t) VALUES (0, 'quick')");
        execute("INSERT INTO %s (k, t) VALUES (1, 'slow')");
        createIndex("CREATE CUSTOM INDEX ON %s (t) USING '" + ResidualAnalyzerStubIndex.class.getName() + '\'');
        waitForTableIndexesQueryable();
        ReadCommand command = parseReadCommand("SELECT * FROM %s WHERE t : 'quick'");
        assertEquals(1, Util.getAll(command).size());

        List<FilteredPartition> partitions = Util.getAll(deserializeWithIndex(serialize(command)));
        assertEquals(1, partitions.size());
        assertEquals(0, (int) Int32Type.instance.compose(partitions.get(0).partitionKey().getKey()));
    }

    @Test
    public void testExecuteWithAnalyzerMatchAndNamedIndexOfOtherGroup() throws Throwable
    {
        // the legacy index on a serves the read, and the row filter evaluates the analyzer match with the SAI index on t
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, t text)");
        execute("INSERT INTO %s (k, a, t) VALUES (0, 1, 'Quick brown fox')");
        execute("INSERT INTO %s (k, a, t) VALUES (1, 1, 'slow dog')");
        String aIndex = createIndex("CREATE INDEX a_idx ON %s (a) USING 'legacy_local_table'");
        createIndex("CREATE CUSTOM INDEX ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        waitForTableIndexesQueryable();
        ReadCommand command = parseReadCommand("SELECT * FROM %s WHERE a = 1 AND t : 'quick' ALLOW FILTERING WITH included_indexes = {" + aIndex + '}');
        assertEquals(aIndex, command.indexQueryPlan().getFirst().getIndexMetadata().name);

        List<FilteredPartition> partitions = Util.getAll(deserializeWithIndex(serialize(command)));
        assertEquals(1, partitions.size());
        assertEquals(0, (int) Int32Type.instance.compose(partitions.get(0).partitionKey().getKey()));
    }

    @Test
    public void testExecuteWithAnalyzerMatchIndexInNamedGroup() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, t text)");
        execute("INSERT INTO %s (k, a, t) VALUES (0, 1, 'Quick brown fox')");
        execute("INSERT INTO %s (k, a, t) VALUES (1, 1, 'slow dog')");
        String aIndex = createIndex("CREATE INDEX ON %s (a) USING 'sai'");
        String tIndex = createIndex("CREATE CUSTOM INDEX ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        waitForTableIndexesQueryable();
        for (String index : new String[]{ aIndex, tIndex })
        {
            ReadCommand deserialized = deserializeWithIndex(serialize(readCommandNaming("SELECT * FROM %s WHERE a = 1 AND t : 'quick'", index)));
            List<FilteredPartition> partitions = Util.getAll(deserialized);
            assertEquals(1, partitions.size());
            assertEquals(0, (int) Int32Type.instance.compose(partitions.get(0).partitionKey().getKey()));
        }
    }

    @Test
    public void testExecuteWithExcludedAnalyzerMatchIndexInNamedGroupAndIndexOfOtherGroup() throws Throwable
    {
        // the replica's index on t in the named index's group has the name of an index the command excludes, and its
        // other index on t is a custom index of another group
        assertIndexNotAvailableWithKnownIndex("t text, b int", "'quick brown fox'",
                                              "CREATE CUSTOM INDEX ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}",
                                              "SELECT * FROM %s WHERE a = 1 AND t : 'quick' WITH excluded_indexes = {old_idx}",
                                              (aIndex, tIndex) -> {
                                                  dropIndex("DROP INDEX %s." + tIndex);
                                                  dropIndex("DROP INDEX %s.old_idx");
                                                  createIndex("CREATE CUSTOM INDEX old_idx ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
                                                  createIndex("CREATE CUSTOM INDEX ON %s (t) USING '" + AnalyzerMatchStubIndex.class.getName() + '\'');
                                              },
                                              "CREATE INDEX old_idx ON %s (b) USING 'sai'");
    }

    @Test
    public void testExecuteWithUnknownIncludedIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, t text)");
        execute("INSERT INTO %s (k, a, t) VALUES (0, 1, 'Quick brown fox')");
        String aIndex = createIndex("CREATE INDEX ON %s (a) USING 'sai'");
        createIndex("CREATE CUSTOM INDEX new_idx ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        waitForTableIndexesQueryable();
        String query = "SELECT * FROM %s WHERE a = 1 AND t : 'quick' WITH included_indexes = {" + aIndex + ", new_idx}";
        byte[] serialized = serialize(readCommandNaming(query, aIndex));

        // the replica doesn't know the included index on t, and has another one with another analyzer
        dropIndex("DROP INDEX %s.new_idx");
        createIndex("CREATE CUSTOM INDEX old_idx ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': 'false'}");
        waitForTableIndexesQueryable();
        assertIndexNotAvailable(deserialize(serialized));
    }

    @Test
    public void testExecuteWithUnknownIncludedIndexAndEquality() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");
        execute("INSERT INTO %s (k, a, b) VALUES (0, 1, 2)");
        execute("INSERT INTO %s (k, a, b) VALUES (1, 1, 3)");
        String aIndex = createIndex("CREATE INDEX ON %s (a) USING 'sai'");
        createIndex("CREATE INDEX b_new ON %s (b) USING 'sai'");
        waitForTableIndexesQueryable();
        byte[] serialized = serialize(readCommandNaming("SELECT * FROM %s WHERE a = 1 AND b = 2 WITH included_indexes = {b_new}", aIndex));

        // the replica doesn't know the included index, although the named one could serve the equality read
        dropIndex("DROP INDEX %s.b_new");
        assertIndexNotAvailable(deserialize(serialized));
    }

    @Test
    public void testExecuteWithUnknownIncludedIndexAndOtherIndexOfColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, t text)");
        execute("INSERT INTO %s (k, a, t) VALUES (0, 1, 'quick')");
        execute("INSERT INTO %s (k, a, t) VALUES (1, 1, 'Quick')");
        String aIndex = createIndex("CREATE INDEX ON %s (a) USING 'sai'");
        createIndex("CREATE CUSTOM INDEX t_exact ON %s (t) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX t_ci ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': 'false'}");
        waitForTableIndexesQueryable();
        byte[] serialized = serialize(readCommandNaming("SELECT * FROM %s WHERE a = 1 AND t = 'quick' WITH included_indexes = {t_exact}", aIndex));

        // the replica doesn't know the included exact index, and its other index on t is case-insensitive
        dropIndex("DROP INDEX %s.t_exact");
        assertIndexNotAvailable(deserialize(serialized));
    }

    @Test
    public void testExecuteWithUnknownIncludedAnalyzedCollectionIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, tags set<text>)");
        execute("INSERT INTO %s (k, a, tags) VALUES (0, 1, {'apple'})");
        execute("INSERT INTO %s (k, a, tags) VALUES (1, 1, {'apple banana'})");
        String aIndex = createIndex("CREATE INDEX ON %s (a) USING 'sai'");
        createIndex("CREATE CUSTOM INDEX tags_analyzed ON %s (tags) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        waitForTableIndexesQueryable();
        byte[] serialized = serialize(readCommandNaming("SELECT * FROM %s WHERE a = 1 AND tags CONTAINS 'apple' WITH included_indexes = {tags_analyzed}", aIndex));

        // the replica doesn't know the included analyzed index
        dropIndex("DROP INDEX %s.tags_analyzed");
        assertIndexNotAvailable(deserialize(serialized));
    }

    @Test
    public void testExecuteWithUnregisteredIncludedIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, tags set<text>)");
        execute("INSERT INTO %s (k, a, tags) VALUES (0, 1, {'apple'})");
        execute("INSERT INTO %s (k, a, tags) VALUES (1, 1, {'apple banana'})");
        String aIndex = createIndex("CREATE INDEX ON %s (a) USING 'sai'");
        createIndex("CREATE CUSTOM INDEX tags_analyzed ON %s (tags) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        waitForTableIndexesQueryable();
        byte[] serialized = serialize(readCommandNaming("SELECT * FROM %s WHERE a = 1 AND tags CONTAINS 'apple' WITH included_indexes = {tags_analyzed}", aIndex));

        // the replica's schema has the included analyzed index, but its index manager hasn't registered it
        getCurrentColumnFamilyStore().indexManager.removeIndex("tags_analyzed");
        assertIndexNotAvailable(deserialize(serialized));
    }

    @Test
    public void testExecuteWithUnknownExcludedIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int, t text)");
        execute("INSERT INTO %s (k, a, t) VALUES (0, 1, 'Quick brown fox')");
        execute("INSERT INTO %s (k, a, t) VALUES (1, 1, 'slow dog')");
        String aIndex = createIndex("CREATE INDEX ON %s (a) USING 'sai'");
        createIndex("CREATE CUSTOM INDEX ON %s (t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        createIndex("CREATE INDEX gone_idx ON %s (b) USING 'sai'");
        waitForTableIndexesQueryable();
        String query = "SELECT * FROM %s WHERE a = 1 AND t : 'quick' WITH excluded_indexes = {gone_idx}";
        byte[] serialized = serialize(readCommandNaming(query, aIndex));

        // the replica doesn't know the excluded index
        dropIndex("DROP INDEX %s.gone_idx");
        List<FilteredPartition> partitions = Util.getAll(deserializeWithIndex(serialized));
        assertEquals(1, partitions.size());
        assertEquals(0, (int) Int32Type.instance.compose(partitions.get(0).partitionKey().getKey()));
    }

    @Test
    public void testExecuteWithOrderingIndexInNamedGroupAndIncludedIndexOfOtherGroup() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, vec vector<float, 2>)");
        execute("INSERT INTO %s (k, a, vec) VALUES (0, 1, [1.0, 2.0])");
        execute("INSERT INTO %s (k, a, vec) VALUES (1, 1, [5.0, 5.0])");
        String aIndex = createIndex("CREATE INDEX ON %s (a) USING 'sai'");
        String vecIndex = createIndex("CREATE INDEX ON %s (vec) USING 'sai'");
        waitForTableIndexesQueryable();
        String query = "SELECT * FROM %s WHERE a = 1 ORDER BY vec ANN OF [1.0, 2.0] LIMIT 1 WITH included_indexes = {" + aIndex + '}';
        byte[] serialized = serialize(readCommandNaming(query, vecIndex));

        // the replica's index of the included name is a custom index of another group that also supports ANN
        dropIndex("DROP INDEX %s." + aIndex);
        createIndex("CREATE INDEX a_new_idx ON %s (a) USING 'sai'");
        createIndex("CREATE CUSTOM INDEX " + aIndex + " ON %s (vec) USING '" + AnnStubIndex.class.getName() + '\'');
        waitForTableIndexesQueryable();
        ReadCommand deserialized = deserializeWithIndex(serialized);
        assertTrue(deserialized.indexQueryPlan().isTopK());
        List<FilteredPartition> partitions = Util.getAll(deserialized);
        assertEquals(1, partitions.size());
        assertEquals(0, (int) Int32Type.instance.compose(partitions.get(0).partitionKey().getKey()));
    }

    /**
     * Checks a query that the indexes on a and on the given column serve, deserialized where only the index on a
     * exists, or with the given replica schema. The serialized command names the index on a.
     */
    private void assertIndexNotAvailableWithKnownIndex(String column, String value, String createIndex, String query) throws Throwable
    {
        assertIndexNotAvailableWithKnownIndex(column, value, createIndex, query,
                                              (aIndex, otherIndex) -> dropIndex("DROP INDEX %s." + otherIndex));
    }

    private void assertIndexNotAvailableWithKnownIndex(String column, String value, String createIndex, String query,
                                                       ReplicaSchema replicaSchema, String... moreIndexes) throws Throwable
    {
        String name = column.substring(0, column.indexOf(' '));
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, " + column + ')');
        execute("INSERT INTO %s (k, a, " + name + ") VALUES (0, 1, " + value + ')');
        String aIndex = createIndex("CREATE INDEX ON %s (a) USING 'sai'");
        for (String moreIndex : moreIndexes)
            createIndex(moreIndex);
        String otherIndex = createIndex(createIndex);
        waitForTableIndexesQueryable();
        byte[] serialized = serialize(readCommandNaming(query, aIndex));
        replicaSchema.apply(aIndex, otherIndex);
        assertIndexNotAvailable(deserialize(serialized));
    }

    /**
     * The schema of the replica that deserializes the command, made from the schema the command was built with.
     */
    private interface ReplicaSchema
    {
        void apply(String aIndex, String otherIndex) throws Throwable;
    }

    /**
     * Returns the command built for the query, with the given index of its plan as the plan's first index, which is the
     * index a serialized command names.
     */
    private ReadCommand readCommandNaming(String query, String indexName)
    {
        PartitionRangeReadCommand command = (PartitionRangeReadCommand) parseReadCommand(query);
        Index.QueryPlan plan = command.indexQueryPlan();
        Index first = plan.getIndexes().stream()
                          .filter(i -> i.getIndexMetadata().name.equals(indexName))
                          .findFirst()
                          .orElseThrow(AssertionError::new);
        return PartitionRangeReadCommand.create(command.metadata(), command.nowInSec(), command.columnFilter(), command.rowFilter(),
                                                command.limits(), command.dataRange(), new FirstIndexQueryPlan(plan, first), false);
    }

    private static byte[] serialize(ReadCommand command) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadCommand.serializer.serialize(command, out, MessagingService.current_version);
            return out.toByteArray();
        }
    }

    private static ReadCommand deserialize(byte[] serialized) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(serialized))
        {
            ReadCommand deserialized = ReadCommand.serializer.deserialize(in, MessagingService.current_version);
            assertNull(deserialized.indexQueryPlan());
            return deserialized;
        }
    }

    private static ReadCommand deserializeWithIndex(byte[] serialized) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(serialized))
        {
            ReadCommand deserialized = ReadCommand.serializer.deserialize(in, MessagingService.current_version);
            assertNotNull(deserialized.indexQueryPlan());
            return deserialized;
        }
    }

    private ReadCommand deserializeWithoutIndex(String query, String index) throws Throwable
    {
        waitForTableIndexesQueryable();
        ReadCommand command = parseReadCommand(query);
        assertNotNull(command.indexQueryPlan());
        byte[] serialized = serialize(command);
        dropIndex("DROP INDEX %s." + index);
        return deserialize(serialized);
    }

    private static ReadCommand withCaseInsensitiveMatch(ReadCommand read, String value)
    {
        SinglePartitionReadCommand command = (SinglePartitionReadCommand) read;
        RowFilter.Builder builder = RowFilter.builder(false);
        builder.addUserExpression(new CaseInsensitiveMatch(command.metadata().getColumn(ColumnIdentifier.getInterned("t", false)), value));
        return SinglePartitionReadCommand.create(command.metadata(), command.nowInSec(), command.columnFilter(), builder.build(),
                                                 command.limits(), command.partitionKey(), command.clusteringIndexFilter());
    }

    private void assertOrderByIndexNotAvailable() throws Throwable
    {
        for (String order : new String[]{ "ASC", "DESC" })
        {
            String index = createIndex("CREATE INDEX ON %s (v) USING 'sai'");
            waitForTableIndexesQueryable();
            byte[] serialized = serialize(readCommandNaming("SELECT * FROM %s ORDER BY v " + order + " LIMIT 1", index));
            dropIndex("DROP INDEX %s." + index);
            assertIndexNotAvailable(deserialize(serialized));
        }
    }

    private static void assertIndexNotAvailable(ReadCommand command)
    {
        assertThatThrownBy(() -> Util.getAll(command)).isInstanceOf(IndexNotAvailableException.class);
    }

    /**
     * A query plan with the given index as its first index, and otherwise the given plan.
     */
    private static class FirstIndexQueryPlan implements Index.QueryPlan
    {
        private final Index.QueryPlan plan;
        private final Index first;

        FirstIndexQueryPlan(Index.QueryPlan plan, Index first)
        {
            this.plan = plan;
            this.first = first;
        }

        @Override
        public Set<Index> getIndexes()
        {
            return plan.getIndexes();
        }

        @Override
        public Index getFirst()
        {
            return first;
        }

        @Override
        public Index.Searcher searcherFor(ReadCommand command)
        {
            return plan.searcherFor(command);
        }

        @Override
        public RowFilter postIndexQueryFilter()
        {
            return plan.postIndexQueryFilter();
        }
    }

    /**
     * A user expression with the analyzer match operator that evaluates itself, matching its value case-insensitively.
     */
    private static class CaseInsensitiveMatch extends RowFilter.UserExpression
    {
        CaseInsensitiveMatch(ColumnMetadata column, String value)
        {
            super(column, Operator.ANALYZER_MATCHES, UTF8Type.instance.decompose(value));
        }

        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            Cell<?> cell = row.getCell(column);
            return cell != null && UTF8Type.instance.compose(cell.buffer()).equalsIgnoreCase(UTF8Type.instance.compose(value));
        }

        @Override
        public String toCQLString(boolean redact)
        {
            return column.name.toCQLString() + " : " + UTF8Type.instance.compose(value);
        }

        @Override
        protected void serialize(DataOutputPlus out, int version)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected long serializedSize(int version)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A custom index that supports ANN expressions, in its own index group.
     */
    public static class AnnStubIndex extends StubIndex
    {
        public AnnStubIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public boolean supportsExpression(ColumnMetadata column, Operator operator)
        {
            return operator == Operator.ANN;
        }
    }

    /**
     * A custom index that supports analyzer matches, in its own index group.
     */
    public static class AnalyzerMatchStubIndex extends StubIndex
    {
        public AnalyzerMatchStubIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public boolean supportsExpression(ColumnMetadata column, Operator operator)
        {
            return operator == Operator.ANALYZER_MATCHES;
        }
    }

    /**
     * A custom index that supports analyzer matches, and leaves them to the row filter with an analyzer of its own.
     */
    public static class ResidualAnalyzerStubIndex extends AnalyzerMatchStubIndex
    {
        public ResidualAnalyzerStubIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public RowFilter getPostIndexQueryFilter(RowFilter filter)
        {
            RowFilter.Builder builder = RowFilter.builder(false);
            for (RowFilter.Expression expression : filter.expressions())
            {
                if (expression.operator() == Operator.ANALYZER_MATCHES)
                    builder.add(new RowFilter.SimpleExpression(expression.column(), expression.operator(), expression.getIndexValue(),
                                                               analyzer(expression.getIndexValue()), null));
                else
                    builder.add(expression);
            }
            return builder.build();
        }

        private static Index.Analyzer analyzer(ByteBuffer queriedValue)
        {
            return new Index.Analyzer()
            {
                @Override
                public List<ByteBuffer> indexedTokens(ByteBuffer indexedValue)
                {
                    return List.of(indexedValue);
                }

                @Override
                public List<ByteBuffer> queriedTokens()
                {
                    return List.of(queriedValue);
                }
            };
        }
    }
}
