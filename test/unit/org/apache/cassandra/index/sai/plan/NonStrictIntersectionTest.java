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

package org.apache.cassandra.index.sai.plan;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the replica-side evaluation of the SAI intersections of reads that need reconciliation (CASSANDRA-19018): a
 * union of the index matches on unrepaired data, and their intersection on repaired data.
 */
public class NonStrictIntersectionTest extends SAITester
{
    private String keyspace;
    private boolean previousNonStrict;
    private String bIndex;

    @Before
    public void setup()
    {
        // With more than one replica, reads at QUORUM need reconciliation at the coordinator
        keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}");
        createTable(keyspace, "CREATE TABLE %s (k int PRIMARY KEY, a int, b int, c int, d int)");
        createIndex(keyspace, "CREATE INDEX ON %s(a) USING 'sai'");
        bIndex = createIndex(keyspace, "CREATE INDEX ON %s(b) USING 'sai'");
        previousNonStrict = DatabaseDescriptor.getSAINonStrictReplicaFilteringEnabled();
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(true);
    }

    @After
    public void restoreNonStrictFiltering()
    {
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(previousNonStrict);
    }

    /**
     * With non-strict replica filtering disabled, the coordinator makes the row filter strict, so the local query
     * intersects the index matches, and doesn't look for unrepaired matches.
     */
    @Test
    public void testNonStrictFilteringDisabled()
    {
        insert(1, 1, 1, 0);
        insert(2, 1, 2, 0);
        insert(3, 2, 2, 0);

        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(false);
        for (boolean flushed : new boolean[]{ false, true })
        {
            if (flushed)
                flush(keyspace);

            ReadCommand command = command("a = 1 AND b = 2", ConsistencyLevel.QUORUM);
            assertThat(command.rowFilter().needsReconciliation()).isTrue();
            assertThat(command.rowFilter().isMutableIntersection()).isTrue();
            assertThat(command.rowFilter().isStrict()).isTrue();
            assertThat(localMatches(command)).containsExactly(2);
            assertUnrepairedMatches(command, false);
        }

        // a command built while it was enabled stays non-strict
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(true);
        ReadCommand command = command("a = 1 AND b = 2", ConsistencyLevel.QUORUM);
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(false);
        assertThat(command.rowFilter().isStrict()).isFalse();
        assertThat(localMatches(command)).containsExactly(1, 2, 3);
    }

    /**
     * Replicas apply the non-strict filters of the commands they receive strictly until the read verb handler finds
     * that their coordinator asks for non-strict filtering.
     */
    @Test
    public void testDeserializedCommands() throws IOException
    {
        insert(1, 1, 1, 0);
        insert(2, 1, 2, 0);
        insert(3, 2, 2, 0);

        ReadCommand command = command("a = 1 AND b = 2", ConsistencyLevel.QUORUM);
        assertThat(command.rowFilter().isStrict()).isFalse();
        ReadCommand received = serializationRoundTrip(command);
        assertThat(received.rowFilter().isStrict()).isFalse();
        assertThat(received.isStrictFilteringRequired()).isTrue();
        assertThat(localMatches(received)).containsExactly(2);

        received.allowNonStrictFiltering();
        assertThat(localMatches(received)).containsExactly(1, 2, 3);

        // a strict filter is received as non-strict, since it isn't serialized, but its command filters strictly
        DatabaseDescriptor.setSAINonStrictReplicaFilteringEnabled(false);
        command = command("a = 1 AND b = 2", ConsistencyLevel.QUORUM);
        assertThat(command.rowFilter().isStrict()).isTrue();
        received = serializationRoundTrip(command);
        assertThat(received.isStrictFilteringRequired()).isTrue();
        assertThat(localMatches(received)).containsExactly(2);
    }

    private static ReadCommand serializationRoundTrip(ReadCommand command) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadCommand.serializer.serialize(command, out, MessagingService.current_version);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), true))
            {
                return ReadCommand.serializer.deserialize(in, MessagingService.current_version);
            }
        }
    }

    @Test
    public void testUnrepairedData()
    {
        insert(1, 1, 1, 0);
        insert(2, 1, 2, 0);
        insert(3, 2, 2, 0);
        insert(4, 3, 3, 0);

        // in Memtable indexes
        assertUnrepairedMatches("a = 1 AND b = 2", true);
        assertNonStrictMatches("a = 1 AND b = 2", 1, 2, 3);
        assertStrictMatches("a = 1 AND b = 2", 2);

        // in indexes on unrepaired SSTables
        flush(keyspace);
        assertNonStrictMatches("a = 1 AND b = 2", 1, 2, 3);
        assertStrictMatches("a = 1 AND b = 2", 2);

        // a single mutable column can be strictly filtered
        assertNonStrictMatches("a = 1", 1, 2);

        // the relaxed intersections include unindexed columns and nested disjunctions
        assertNonStrictMatches("a = 1 AND c = 5 ALLOW FILTERING", 1, 2);
        assertNonStrictMatches("a = 3 AND (b = 1 OR b = 2)", 1, 2, 3, 4);
        assertNonStrictMatches("a = 3 AND b IN (1, 2)", 1, 2, 3, 4);
        assertNonStrictMatches("(a = 2 AND b = 1) OR b = 3", 1, 3, 4);

        // nested operations that can't use indexes don't narrow down the candidates, but don't select everything either
        assertNonStrictMatches("a = 1 AND c IN (7, 8) ALLOW FILTERING", 1, 2);
        assertNonStrictMatches("a = 1 AND (c = 7 OR c = 8) ALLOW FILTERING", 1, 2);
        assertNonStrictMatches("a = 1 AND (b = 2 OR c = 7) ALLOW FILTERING", 1, 2);
        assertNonStrictMatches("(a = 1 OR a = 2) AND (c = 7 OR c = 8) ALLOW FILTERING", 1, 2, 3);
    }

    @Test
    public void testRepairedData() throws IOException
    {
        insert(1, 1, 1, 0);
        insert(2, 1, 2, 0);
        insert(3, 2, 2, 0);
        insert(4, 3, 3, 0);
        flush(keyspace);
        markRepaired();

        // repaired data is consistent across replicas, so its matches are strictly intersected
        assertUnrepairedMatches("a = 1 AND b = 2", false);
        assertNonStrictMatches("a = 1 AND b = 2", 2);
        assertNonStrictMatches("a = 2 AND b = 1");
        assertNonStrictMatches("a = 3 AND (b = 1 OR b = 2)");
        assertNonStrictMatches("a = 1 AND b IN (2, 3)", 2);
    }

    @Test
    public void testRepairedAndUnrepairedData() throws IOException
    {
        insert(1, 1, 1, 0);
        insert(2, 1, 2, 0);
        insert(3, 2, 2, 0);
        flush(keyspace);
        markRepaired();

        // a partial update of an unindexed column doesn't produce unrepaired index matches
        execute("UPDATE " + table() + " SET c = 7 WHERE k = 3");
        assertUnrepairedMatches("a = 2 AND b = 1", false);
        assertNonStrictMatches("a = 2 AND b = 1");

        // unrepaired matches of any column are returned even if they don't match the other columns...
        insert(4, 4, 1, 0);
        assertUnrepairedMatches("a = 2 AND b = 1", true);
        assertNonStrictMatches("a = 2 AND b = 1", 4);
        assertNonStrictMatches("a = 4 AND b = 2", 4);

        // ...but rows matching on repaired data still need to match all the columns there
        assertNonStrictMatches("a = 1 AND b = 1", 1, 4);
        assertStrictMatches("a = 1 AND b = 1", 1);

        // without unrepaired matches, the intersection isn't relaxed
        assertUnrepairedMatches("a = 1 AND b = 3", false);
        assertNonStrictMatches("a = 1 AND b = 3");

        // the same applies to unrepaired SSTables
        flush(keyspace);
        assertNonStrictMatches("a = 2 AND b = 1", 4);
        assertNonStrictMatches("a = 4 AND b = 2", 4);
        assertNonStrictMatches("a = 1 AND b = 1", 1, 4);
        assertStrictMatches("a = 1 AND b = 1", 1);
        assertNonStrictMatches("a = 1 AND b = 3");
    }

    /**
     * If all the index matches are on repaired data, the indexed columns are intersected, but the restrictions on
     * several unindexed columns are unioned, as those might have been updated on different replicas (CASSANDRA-20189).
     */
    @Test
    public void testRepairedDataWithUnindexedColumns() throws IOException
    {
        insert(1, 1, 1, 7, 0);
        insert(2, 1, 1, 0, 8);
        insert(3, 1, 1, 0, 0);
        insert(4, 2, 1, 7, 8);
        flush(keyspace);
        markRepaired();

        // a single unindexed column is intersected with the indexed ones
        assertNonStrictMatches("a = 1 AND c = 7 ALLOW FILTERING", 1);
        assertNonStrictMatches("a = 1 AND b = 1 AND c = 7 ALLOW FILTERING", 1);

        // several unindexed columns are unioned, and intersected with the indexed ones
        assertUnrepairedMatches("a = 1 AND c = 7 AND d = 8 ALLOW FILTERING", false);
        assertNonStrictMatches("a = 1 AND c = 7 AND d = 8 ALLOW FILTERING", 1, 2);
        assertStrictMatches("a = 1 AND c = 7 AND d = 8 ALLOW FILTERING");
        assertNonStrictMatches("a = 1 AND b = 1 AND c = 7 AND d = 8 ALLOW FILTERING", 1, 2);

        // nested operations on unindexed columns are unioned with the unindexed columns
        assertNonStrictMatches("a = 1 AND c = 7 AND (d = 8 OR d = 9) ALLOW FILTERING", 1, 2);
        assertNonStrictMatches("a = 1 AND (c = 7 OR c = 9) AND (d = 8 OR d = 9) ALLOW FILTERING", 1, 2);

        // nested operations only on indexed columns are intersected
        assertNonStrictMatches("a = 1 AND c = 7 AND (b = 2 OR b = 3) ALLOW FILTERING");

        // indexed columns whose index is excluded by the query hints count as unindexed
        execute("UPDATE " + table() + " SET b = 2 WHERE k = 3");
        flush(keyspace);
        markRepaired();
        assertNonStrictMatches("a = 1 AND b = 2 AND c = 7 ALLOW FILTERING");
        assertNonStrictMatches("a = 1 AND b = 2 AND c = 7 ALLOW FILTERING WITH excluded_indexes = {" + bIndex + '}', 1, 3);
    }

    @Test
    public void testRepairedDataWithUnindexedCollection() throws IOException
    {
        createTable(keyspace, "CREATE TABLE %s (k int PRIMARY KEY, a int, s set<int>, m map<int, int>)");
        createIndex(keyspace, "CREATE INDEX ON %s(a) USING 'sai'");
        execute("INSERT INTO " + table() + " (k, a, s, m) VALUES (1, 1, {7}, {1: 7})");
        execute("INSERT INTO " + table() + " (k, a, s, m) VALUES (2, 1, {8}, {2: 8})");
        execute("INSERT INTO " + table() + " (k, a, s, m) VALUES (3, 1, {0}, {0: 0})");
        flush(keyspace);
        markRepaired();

        // the elements of an unindexed collection might have been updated on different replicas, so several
        // expressions on one such column are unioned
        assertNonStrictMatches("a = 1 AND s CONTAINS 7 ALLOW FILTERING", 1);
        assertNonStrictMatches("a = 1 AND s CONTAINS 7 AND s CONTAINS 8 ALLOW FILTERING", 1, 2);
        assertNonStrictMatches("a = 1 AND m CONTAINS KEY 1 AND m CONTAINS KEY 2 ALLOW FILTERING", 1, 2);
        assertNonStrictMatches("a = 1 AND m[1] = 7 AND m[2] = 8 ALLOW FILTERING", 1, 2);
        assertStrictMatches("a = 1 AND s CONTAINS 7 AND s CONTAINS 8 ALLOW FILTERING");
    }

    private void insert(int k, int a, int b, int c)
    {
        execute("INSERT INTO " + table() + " (k, a, b, c) VALUES (?, ?, ?, ?)", k, a, b, c);
    }

    private void insert(int k, int a, int b, int c, int d)
    {
        execute("INSERT INTO " + table() + " (k, a, b, c, d) VALUES (?, ?, ?, ?, ?)", k, a, b, c, d);
    }

    private String table()
    {
        return keyspace + '.' + currentTable();
    }

    private void markRepaired() throws IOException
    {
        ColumnFamilyStore cfs = getColumnFamilyStore(keyspace, currentTable());
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, 1, null, false);
            sstable.reloadSSTableMetadata();
        }
        assertThat(cfs.getLiveSSTables()).allMatch(SSTableReader::isRepaired);
    }

    /**
     * Asserts the keys of the rows that the local query returns at QUORUM, which needs reconciliation.
     * <p>
     * Without plan optimization, those are exactly the rows matching any of the restrictions on unrepaired data, plus
     * those matching all the restrictions on repaired data. The plan optimizer can widen the intersections on repaired
     * data, and so return more rows, which the coordinator filters out.
     */
    private void assertNonStrictMatches(String where, Integer... expectedKeys)
    {
        String withoutOptimization = (where.contains(" WITH ") ? " AND" : " WITH") + " optimizer_options = {'query_optimization_level': '0'}";
        ReadCommand command = command(where + withoutOptimization, ConsistencyLevel.QUORUM);
        assertThat(command.rowFilter().isStrict()).isEqualTo(!command.rowFilter().isMutableIntersection());
        assertThat(localMatches(command)).containsExactly(expectedKeys);

        command = command(where, ConsistencyLevel.QUORUM);
        assertThat(localMatches(command)).contains(expectedKeys);
    }

    /**
     * Asserts the keys of the rows that the local query returns at ONE, which doesn't need reconciliation.
     */
    private void assertStrictMatches(String where, Integer... expectedKeys)
    {
        ReadCommand command = command(where, ConsistencyLevel.ONE);
        assertThat(command.rowFilter().isStrict()).isTrue();
        assertThat(localMatches(command)).containsExactly(expectedKeys);
    }

    /**
     * Asserts whether the local query at QUORUM finds index matches on unrepaired data.
     */
    private void assertUnrepairedMatches(String where, boolean expected)
    {
        ReadCommand command = command(where, ConsistencyLevel.QUORUM);
        assertThat(command.rowFilter().isStrict()).isFalse();
        assertUnrepairedMatches(command, expected);
    }

    private static void assertUnrepairedMatches(ReadCommand command, boolean expected)
    {
        StorageAttachedIndexSearcher searcher = (StorageAttachedIndexSearcher) command.indexSearcher();
        try
        {
            searcher.buildPlan();
            assertThat(searcher.queryContext().hasUnrepairedMatches()).isEqualTo(expected);
        }
        finally
        {
            searcher.abort();
        }
    }

    private ReadCommand command(String where, ConsistencyLevel consistency)
    {
        SelectStatement select = (SelectStatement) parseStatement("SELECT * FROM " + table() + " WHERE " + where);
        QueryOptions options = QueryOptions.forInternalCalls(consistency, Collections.emptyList());
        ReadQuery query = select.getQuery(options, FBUtilities.nowInSeconds());
        assertThat(query).isInstanceOf(ReadCommand.class);
        return (ReadCommand) query;
    }

    private Set<Integer> localMatches(ReadCommand command)
    {
        Set<Integer> keys = new TreeSet<>();
        for (Partition partition : Util.getAllUnfiltered(command))
        {
            try (UnfilteredRowIterator rows = partition.unfilteredIterator())
            {
                while (rows.hasNext())
                {
                    Unfiltered unfiltered = rows.next();
                    if (unfiltered.isRow() && ((Row) unfiltered).hasLiveData(command.nowInSec(), false))
                        keys.add(Int32Type.instance.compose(partition.partitionKey().getKey()));
                }
            }
        }
        return keys;
    }
}
