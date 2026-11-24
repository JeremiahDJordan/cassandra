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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.RepairedDataInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.TopKProcessor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.serializers.SerializationUtils.cycleSerialization;

/**
 * Tests that ANN read commands with a synthetic score column are sent without it to nodes on a messaging version below
 * DS 10, such as Apache Cassandra 5.0 nodes, which can't deserialize it, and with the vector column instead, and that
 * the responses of those nodes are read with the column filter they were sent.
 */
@RunWith(Parameterized.class)
public class VectorSyntheticScoreMessagingVersionTest extends SAITester
{
    private static final int[] VERSIONS_BELOW_DS_10 = { MessagingService.VERSION_40, MessagingService.VERSION_50 };
    private static final int[] DS_VERSIONS = { MessagingService.VERSION_DS_10, MessagingService.VERSION_DS_20, MessagingService.current_version };

    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        // the versions that use synthetic scores
        return Version.ALL.stream()
                          .filter(v -> v.onOrAfter(Version.ED))
                          .map(v -> new Object[]{ v })
                          .collect(Collectors.toList());
    }

    @Before
    public void setCurrentVersion()
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Test
    public void testRegularVector() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, n int, v vector<float, 2>, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (k, c, n, v) VALUES (0, ?, ?, ?)", i, i, vector(1f, (float) i));

        for (String query : new String[]{ "SELECT c FROM %s ORDER BY v ANN OF [1, 0] LIMIT 3",
                                          "SELECT * FROM %s ORDER BY v ANN OF [1, 0] LIMIT 3",
                                          "SELECT n, v FROM %s ORDER BY v ANN OF [1, 0] LIMIT 3",
                                          "SELECT c FROM %s WHERE k = 0 ORDER BY v ANN OF [1, 0] LIMIT 3",
                                          "SELECT n FROM %s WHERE n > 0 ORDER BY v ANN OF [1, 0] LIMIT 3" })
        {
            ReadCommand command = parseReadCommand(query);
            ColumnMetadata vector = currentTableMetadata().getColumn(ColumnIdentifier.getInterned("v", false));
            assertSentForVersions(query, command, vector);
            assertResponseOfNodeBelowDS10(query, command, vector, false);
        }
    }

    @Test
    public void testStaticVector() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, n int, v vector<float, 2> static, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (k, c, n, v) VALUES (?, 0, ?, ?)", i, i, vector(1f, (float) i));

        String query = "SELECT k FROM %s ORDER BY v ANN OF [1, 0] LIMIT 3";
        ReadCommand command = parseReadCommand(query);
        ColumnMetadata vector = currentTableMetadata().getColumn(ColumnIdentifier.getInterned("v", false));
        assertSentForVersions(query, command, vector);
        assertResponseOfNodeBelowDS10(query, command, vector, true);
    }

    @Test
    public void testClusteringVector() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v vector<float, 2>, n int, PRIMARY KEY (k, v))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (k, v, n) VALUES (0, ?, ?)", vector(1f, (float) i), i);

        String query = "SELECT k FROM %s ORDER BY v ANN OF [1, 0] LIMIT 3";
        ReadCommand command = parseReadCommand(query);
        ColumnMetadata vector = currentTableMetadata().getColumn(ColumnIdentifier.getInterned("v", false));
        assertSentForVersions(query, command, vector);
        assertResponseOfNodeBelowDS10(query, command, vector, false);
    }

    @Test
    public void testBM25()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t text)");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        execute("INSERT INTO %s (k, t) VALUES (0, 'apple pie')");

        // the coordinator can't compute BM25 scores, so the command keeps its score column
        String query = "SELECT k FROM %s ORDER BY t BM25 OF 'apple' LIMIT 3";
        ReadCommand command = parseReadCommand(query);
        Assertions.assertThat(hasSyntheticColumns(command.columnFilter())).as(query).isTrue();
        for (int messagingVersion : VERSIONS_BELOW_DS_10)
            Assertions.assertThat(command.columnFilter(messagingVersion)).as(query + " at " + messagingVersion).isEqualTo(command.columnFilter());
    }

    /**
     * A BM25 score that is older than the text, as from a replica that time-stamps it in seconds, is still used: the
     * coordinator can't compute it.
     */
    @Test
    public void testBM25ScoreOlderThanText()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t text)");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        ReadCommand command = parseReadCommand("SELECT k FROM %s ORDER BY t BM25 OF 'apple' LIMIT 3");
        TableMetadata metadata = command.metadata();
        ColumnMetadata text = metadata.getColumn(ColumnIdentifier.getInterned("t", false));
        ColumnMetadata score = ColumnMetadata.syntheticScoreColumn(text, FloatType.instance);

        List<UnfilteredPartitionIterator> partitions = new ArrayList<>();
        for (int k = 0; k < 2; k++)
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.EMPTY);
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(2_000_000L, command.nowInSec()));
            builder.addCell(BufferCell.live(score, 2L, FloatType.instance.decompose((float) k)));
            builder.addCell(BufferCell.live(text, 2_000_000L, UTF8Type.instance.decompose("apple")));
            Row row = builder.build();
            DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(k));
            RegularAndStaticColumns columns = RegularAndStaticColumns.builder().add(text).add(score).build();
            partitions.add(new SingletonUnfilteredPartitionIterator(UnfilteredRowIterators.singleton(row, metadata, key, DeletionTime.LIVE, columns,
                                                                                                     Rows.EMPTY_STATIC_ROW, false, EncodingStats.NO_STATS)));
        }

        List<Integer> keys = new ArrayList<>();
        try (PartitionIterator reordered = new TopKProcessor(command).reorder(UnfilteredPartitionIterators.filter(UnfilteredPartitionIterators.concat(partitions), command.nowInSec())))
        {
            while (reordered.hasNext())
            {
                try (RowIterator partition = reordered.next())
                {
                    while (partition.hasNext())
                    {
                        partition.next();
                        keys.add(Int32Type.instance.compose(partition.partitionKey().getKey()));
                    }
                }
            }
        }
        Assertions.assertThat(keys).containsExactly(1, 0);
    }

    private static void assertSentForVersions(String query, ReadCommand command, ColumnMetadata vector)
    {
        Assertions.assertThat(hasSyntheticColumns(command.columnFilter())).as(query).isTrue();

        for (int messagingVersion : DS_VERSIONS)
        {
            ColumnFilter sent = cycleSerialization(command, ReadCommand.serializer, messagingVersion).columnFilter();
            Assertions.assertThat(sent).as(query + " at " + messagingVersion).isEqualTo(command.columnFilter());
        }

        for (int messagingVersion : VERSIONS_BELOW_DS_10)
        {
            String what = query + " at " + messagingVersion;
            ColumnFilter sent = cycleSerialization(command, ReadCommand.serializer, messagingVersion).columnFilter();
            Assertions.assertThat(hasSyntheticColumns(sent)).as(what).isFalse();
            if (!vector.isPrimaryKeyColumn())
                Assertions.assertThat(sent.queriedColumns().contains(vector)).as(what).isTrue();
        }
    }

    /**
     * Executes the command as a replica below DS 10 receives it, and reads its response on the coordinator: the rows
     * have their vector and no score.
     */
    private void assertResponseOfNodeBelowDS10(String query, ReadCommand command, ColumnMetadata vector, boolean isStatic)
    {
        int messagingVersion = MessagingService.VERSION_50;
        ReadCommand received = cycleSerialization(command, ReadCommand.serializer, messagingVersion);
        ReadResponse response;
        try (ReadExecutionController controller = received.executionController();
             UnfilteredPartitionIterator partitions = received.executeLocally(controller))
        {
            response = received.createResponse(partitions, RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);
        }
        response = cycleSerialization(response, ReadResponse.serializer, messagingVersion);

        ColumnMetadata score = ColumnMetadata.syntheticScoreColumn(vector, FloatType.instance);
        List<Row> rows = new ArrayList<>();
        try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    if (isStatic)
                        rows.add(partition.staticRow());
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (unfiltered.isRow() && !isStatic)
                            rows.add((Row) unfiltered);
                    }
                }
            }
        }

        Assertions.assertThat(rows).as(query).hasSize(3);
        for (Row row : rows)
        {
            Assertions.assertThat(row.getColumnData(score)).as(query).isNull();
            if (vector.isPrimaryKeyColumn())
                continue;
            Cell<?> cell = row.getCell(vector);
            Assertions.assertThat(cell).as(query).isNotNull();
            Assertions.assertThat(vector.type.compose(cell.buffer())).as(query).isNotNull();
        }
    }

    private static boolean hasSyntheticColumns(ColumnFilter filter)
    {
        for (RegularAndStaticColumns columns : new RegularAndStaticColumns[]{ filter.fetchedColumns(), filter.queriedColumns() })
        {
            for (ColumnMetadata column : columns)
            {
                if (column.isSynthetic())
                    return true;
            }
        }
        return false;
    }
}
