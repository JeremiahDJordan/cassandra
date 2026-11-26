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

package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.filter.DataLimits.NO_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class DataLimitsTest
{
    private final static Logger logger = LoggerFactory.getLogger(DataLimitsTest.class);

    private static final TableMetadata ROWS = TableMetadata.builder("ks", "t")
                                                           .partitioner(Murmur3Partitioner.instance)
                                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                                           .addClusteringColumn("ck", Int32Type.instance)
                                                           .addRegularColumn("v", Int32Type.instance)
                                                           .addRegularColumn("w", Int32Type.instance)
                                                           .addRegularColumn("m", MapType.getInstance(Int32Type.instance, Int32Type.instance, true))
                                                           .build();

    ByteBuffer lastReturnedKey = ByteBufferUtil.bytes("lastReturnedKey");

    DataLimits cqlLimits = DataLimits.cqlLimits(19, 17);
    DataLimits cqlLimitsForPagingInRows = cqlLimits.forPaging(PageSize.inRows(13));
    DataLimits cqlLimitsForPagingInBytes = cqlLimits.forPaging(PageSize.inBytes(13));
    DataLimits cqlLimitsForPagingInRowsWithLastRow = cqlLimits.forPaging(PageSize.inRows(13), lastReturnedKey, 5);
    DataLimits cqlLimitsForPagingInBytesWithLastRow = cqlLimits.forPaging(PageSize.inBytes(13), lastReturnedKey, 5);
    DataLimits groupByLimits = DataLimits.groupByLimits(19, 17, NO_LIMIT, NO_LIMIT, AggregationSpecification.AGGREGATE_EVERYTHING);
    DataLimits groupByLimitsForPagingInRows = groupByLimits.forPaging(PageSize.inRows(13));
    DataLimits groupByLimitsForPagingInBytes = groupByLimits.forPaging(PageSize.inBytes(13));
    DataLimits groupByLimitsForPagingInRowsWithLastRow = groupByLimits.forPaging(PageSize.inRows(13), lastReturnedKey, 5);
    DataLimits groupByLimitsForPagingInBytesWithLastRow = groupByLimits.forPaging(PageSize.inBytes(13), lastReturnedKey, 5);

    @BeforeClass
    public static void initClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void serializationTest()
    {
        TableMetadata metadata = SchemaLoader.standardCFMD("ks", "cf", 1, IntegerType.instance, IntegerType.instance, IntegerType.instance).build();
        for (MessagingService.Version version : MessagingService.Version.values())
        {
            checkSerialization(version, cqlLimits, "cql limits", metadata);
            checkSerialization(version, cqlLimitsForPagingInRows, "cql limits for paging in rows", metadata);
            checkSerialization(version, cqlLimitsForPagingInBytes, "cql limits for paging in bytes", metadata);
            checkSerialization(version, cqlLimitsForPagingInRowsWithLastRow, "cql limits for paging in rows with last row", metadata);
            checkSerialization(version, cqlLimitsForPagingInBytesWithLastRow, "cql limits for paging in bytes with last row", metadata);
            checkSerialization(version, groupByLimits, "group by limits", metadata);
            checkSerialization(version, groupByLimitsForPagingInRows, "group by limits for paging in rows", metadata);
            checkSerialization(version, groupByLimitsForPagingInBytes, "group by limits for paging in bytes", metadata);
            checkSerialization(version, groupByLimitsForPagingInRowsWithLastRow, "group by limits for paging in rows with last row", metadata);
            checkSerialization(version, groupByLimitsForPagingInBytesWithLastRow, "group by limits for paging in bytes with last row", metadata);
        }
    }

    @Test
    public void toStringTest()
    {
        String lastRetKeyStr = String.format("lastReturnedKey=%s", ByteBufferUtil.bytesToHex(lastReturnedKey));
        String lastRetKeyRemainingStr = "lastReturnedKeyRemaining=5";

        assertThat(cqlLimits.toString()).contains("LIMIT 19").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT");
        assertThat(cqlLimitsForPagingInRows.toString()).contains("LIMIT 13").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT");
        assertThat(cqlLimitsForPagingInBytes.toString()).contains("BYTES LIMIT 13").contains("LIMIT 19").contains("PER PARTITION LIMIT 17");
        assertThat(cqlLimitsForPagingInRowsWithLastRow.toString()).contains("LIMIT 13").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
        assertThat(cqlLimitsForPagingInBytesWithLastRow.toString()).contains("BYTES LIMIT 13").contains("LIMIT 19").contains("PER PARTITION LIMIT 17").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);

        assertThat(groupByLimits.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT");
        assertThat(groupByLimitsForPagingInRows.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("LIMIT 13").doesNotContain("BYTES LIMIT");
        assertThat(groupByLimitsForPagingInBytes.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("BYTES LIMIT 13");
        assertThat(groupByLimitsForPagingInRowsWithLastRow.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("LIMIT 13").doesNotContain("BYTES LIMIT").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
        assertThat(groupByLimitsForPagingInBytesWithLastRow.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("BYTES LIMIT 13").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
    }

    @Test
    public void testDataStax40LiveDataSize()
    {
        long now = 1000;
        Row row = row(1, now, DeletionTime.LIVE);

        // DataStax 4.0 counts the clustering, the expiring liveness, the row deletion, the live cell of v, and the
        // complex deletion and the two live cells of m, with 4-byte deletion and expiration times
        int dataStax40Size = 4 + (8 + 4 + 4) + 13 + (8 + 4 + 4 + 4) + 12 + 2 * (8 + 4 + 4 + 4 + 4);
        assertThat(DataLimits.dataStax40LiveDataSize(row, now)).isEqualTo(dataStax40Size);
        assertThat(row.liveDataSize(now)).isEqualTo(dataStax40Size + 4 * 4);
        // once the liveness and the expiring cell have expired
        assertThat(DataLimits.dataStax40LiveDataSize(row, now + 10)).isEqualTo(4 + 8 + 13 + (8 + 4 + 4 + 4) + 12 + (8 + 4 + 4 + 4 + 4));
        // a map whose deletion isn't live counts nothing
        Row overwritten = row(1, now, DeletionTime.build(now * 1000000 - 1, now));
        assertThat(DataLimits.dataStax40LiveDataSize(overwritten, now)).isEqualTo(4 + (8 + 4 + 4) + 13 + (8 + 4 + 4 + 4));
    }

    @Test
    public void testBytesCountOfReadsFromDataStax40() throws IOException
    {
        long now = 1000;
        DataLimits page = DataLimits.cqlLimits(NO_LIMIT).forPaging(PageSize.inBytes(120));
        assertThat(countedRows(roundTrip(page, MessagingService.VERSION_DS_12, ROWS), now)).isEqualTo(2);
        assertThat(countedRows(roundTrip(page, MessagingService.VERSION_DS_20, ROWS), now)).isEqualTo(1);
        assertThat(countedRows(page, now)).isEqualTo(1);
    }

    /** The rows that the limits' counter takes from a partition of three rows like those of testDataStax40LiveDataSize. */
    private static int countedRows(DataLimits limits, long now)
    {
        PartitionUpdate.Builder update = PartitionUpdate.builder(ROWS, ByteBufferUtil.bytes(0), ROWS.regularAndStaticColumns(), 3);
        for (int ck = 0; ck < 3; ck++)
            update.add(row(ck, now, DeletionTime.LIVE));
        DataLimits.Counter counter = limits.newCounter(now, false, true, false);
        try (UnfilteredRowIterator rows = counter.applyTo(update.build().unfilteredIterator()))
        {
            while (rows.hasNext())
                rows.next();
        }
        return counter.counted();
    }

    /**
     * A row of the given clustering with an expiring liveness, a live cell of v, a cell tombstone of w, and a live, an
     * expiring and a deleted cell of m with the given complex deletion.
     */
    private static Row row(int ck, long now, DeletionTime complexDeletion)
    {
        ColumnMetadata v = ROWS.getColumn(ByteBufferUtil.bytes("v"));
        ColumnMetadata w = ROWS.getColumn(ByteBufferUtil.bytes("w"));
        ColumnMetadata m = ROWS.getColumn(ByteBufferUtil.bytes("m"));
        long timestamp = now * 1000000;
        ByteBuffer one = ByteBufferUtil.bytes(1);
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.make(ByteBufferUtil.bytes(ck)));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.expiring(timestamp, 10, now));
        builder.addCell(BufferCell.live(v, timestamp, one));
        builder.addCell(BufferCell.tombstone(w, timestamp, now));
        builder.addComplexDeletion(m, complexDeletion);
        builder.addCell(BufferCell.live(m, timestamp, one, CellPath.create(ByteBufferUtil.bytes(1))));
        builder.addCell(BufferCell.expiring(m, timestamp, 10, now, one, CellPath.create(ByteBufferUtil.bytes(2))));
        builder.addCell(BufferCell.tombstone(m, timestamp, now, CellPath.create(ByteBufferUtil.bytes(3))));
        return builder.build();
    }

    private static DataLimits roundTrip(DataLimits limits, int version, TableMetadata metadata) throws IOException
    {
        int size = (int) DataLimits.serializer.serializedSize(limits, version, metadata.comparator);
        try (DataOutputBuffer out = new DataOutputBuffer(2 * size))
        {
            DataLimits.serializer.serialize(limits, out, version, metadata.comparator);
            out.flush();
            assertThat(out.getLength()).describedAs("serialized size of %s for version %d", limits, version).isEqualTo(size);
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                return DataLimits.serializer.deserialize(in, version, metadata);
            }
        }
    }

    private void checkSerialization(MessagingService.Version version, DataLimits limits, String name, TableMetadata metadata)
    {
        String msg = String.format("serialization of %s for version %s", name, version);
        try
        {
            DataLimits deserializedLimits = roundTrip(limits, version.value, metadata);
            assertThat(deserializedLimits.count()).describedAs(msg).isEqualTo(limits.count());

            if (version.value >= MessagingService.VERSION_DS_10)
                assertThat(deserializedLimits.bytes()).describedAs(msg).isEqualTo(limits.bytes());
            else
                assertThat(deserializedLimits.bytes()).describedAs(msg).isEqualTo(NO_LIMIT);

            // a replica counts the bytes of a read from a DataStax 4.0 coordinator as DataStax 4.0 does
            boolean dataStax40 = limits.bytes() != NO_LIMIT && version.value >= MessagingService.VERSION_DS_10 && version.value < MessagingService.VERSION_DS_20;
            assertThat(deserializedLimits.newCounter(0, false, true, false).countsBytesAsDataStax40()).describedAs(msg).isEqualTo(dataStax40);

            assertThat(deserializedLimits.rows()).describedAs(msg).isEqualTo(limits.rows());
            assertThat(deserializedLimits.perPartitionCount()).describedAs(msg).isEqualTo(limits.perPartitionCount());
            assertThat(deserializedLimits.isDistinct()).describedAs(msg).isEqualTo(limits.isDistinct());
            assertThat(deserializedLimits.isUnlimited()).describedAs(msg).isEqualTo(limits.isUnlimited());
            assertThat(deserializedLimits.kind()).describedAs(msg).isEqualTo(limits.kind());
            assertThat(deserializedLimits.isGroupByLimit()).describedAs(msg).isEqualTo(limits.isGroupByLimit());
        }
        catch (IOException | RuntimeException e)
        {
            logger.error("Failed to serialize or deserialize: " + msg, e);
            fail(msg);
        }
    }
}