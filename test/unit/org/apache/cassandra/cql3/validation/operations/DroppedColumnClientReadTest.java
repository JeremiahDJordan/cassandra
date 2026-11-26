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

package org.apache.cassandra.cql3.validation.operations;

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RepairedDataInfo;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Client reads of partitions that hold cells of a dropped column that are newer than the drop.
 */
public class DroppedColumnClientReadTest extends CQLTester
{
    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    @Test
    public void testDroppedSimpleColumn() throws Throwable
    {
        checkDroppedColumn("int", "5", 5, false);
        checkDroppedColumn("int", "5", 5, true);
    }

    @Test
    public void testDroppedComplexColumn() throws Throwable
    {
        checkDroppedColumn("set<int>", "{5, 6}", set(5, 6), false);
        checkDroppedColumn("set<int>", "{5, 6}", set(5, 6), true);
    }

    private void checkDroppedColumn(String type, String literal, Object value, boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, l list<int>, c " + type + ", PRIMARY KEY (pk, ck))");
        disableCompaction();
        long timestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, " + literal + ") USING TIMESTAMP " + timestamp);
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 3)");
        if (flush)
            flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (timestamp - 1));

        assertRowsNet(executeNet("SELECT * FROM %s WHERE pk = 1"), row(1, 1, null, 1), row(1, 2, null, 2));
        assertRowsNet(executeNet("SELECT * FROM %s WHERE pk = 1 AND ck = 1"), row(1, 1, null, 1));
        assertRowsNet(executeNet("SELECT pk, ck, v FROM %s WHERE pk = 1"), row(1, 1, 1), row(1, 2, 2));
        assertRowsNet(executeNet("SELECT * FROM %s"), row(1, 1, null, 1), row(1, 2, null, 2), row(2, 1, null, 3));
        assertRowsNet(executeNet("SELECT * FROM %s WHERE pk = 1 AND ck >= 1"), row(1, 1, null, 1), row(1, 2, null, 2));

        execute("ALTER TABLE %s ADD c " + type);
        assertRowsNet(executeNet("SELECT pk, ck, v, c FROM %s WHERE pk = 1"), row(1, 1, 1, value), row(1, 2, 2, null));
    }

    /**
     * An sstable that has no cell of a dropped column newer than the drop is read with the drop records alone, without
     * looking up its columns in the fetched ones. The cells newer than the drop in another sstable are still left out,
     * until the column is re-added, when no sstable needs the lookup any more.
     */
    @Test
    public void testDroppedColumnOlderThanDrop() throws Throwable
    {
        testDroppedColumnOlderThanDrop("int", "5", 5);
        testDroppedColumnOlderThanDrop("set<int>", "{5, 6}", set(5, 6));
    }

    private void testDroppedColumnOlderThanDrop(String type, String literal, Object value) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c " + type + ", PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, " + literal + ") USING TIMESTAMP 10");
        execute("UPDATE %s USING TIMESTAMP 10 SET c = " + literal + " WHERE pk = 1 AND ck = 2");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 2) USING TIMESTAMP 10");
        flush();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (3, 1, 3, " + literal + ") USING TIMESTAMP 30");
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP 20");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            boolean newerThanDrop = sstable.getMaxTimestamp() == 30;
            assertEquals(newerThanDrop, sstable.header.hasDroppedColumnNotFetched(cfs.metadata(), sstable.getMaxTimestamp(), ColumnFilter.all(cfs.metadata())));
        }

        assertRowsNet(executeNet("SELECT * FROM %s WHERE pk = 1"), row(1, 1, 1));
        assertRowsNet(executeNet("SELECT * FROM %s WHERE pk = 1 AND ck = 1"), row(1, 1, 1));
        assertRowsNet(executeNet("SELECT * FROM %s"), row(1, 1, 1), row(2, 1, 2), row(3, 1, 3));
        assertRowsNet(executeNet("SELECT * FROM %s WHERE pk = 3"), row(3, 1, 3));

        execute("ALTER TABLE %s ADD c " + type);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertFalse(sstable.header.hasDroppedColumnNotFetched(cfs.metadata(), sstable.getMaxTimestamp(), ColumnFilter.all(cfs.metadata())));
        assertRowsNet(executeNet("SELECT pk, ck, v, c FROM %s"), row(1, 1, 1, null), row(2, 1, 2, null), row(3, 1, 3, value));
    }

    /**
     * A read whose column filter was built before a dropped column was re-added, as for a read in flight during the
     * ALTER, or one from a coordinator that has not applied it yet.
     */
    @Test
    public void testReaddedColumnWithOlderColumnFilter() throws Throwable
    {
        testReaddedColumnWithOlderColumnFilter("int", "2");
        testReaddedColumnWithOlderColumnFilter("set<int>", "{2}");
    }

    private void testReaddedColumnWithOlderColumnFilter(String type, String literal) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c " + type + ", PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, " + literal + ") USING TIMESTAMP 10");
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP 20");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        int nowInSec = FBUtilities.nowInSeconds();
        List<ReadCommand> commands = Arrays.asList(SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), nowInSec, cfs.decorateKey(Int32Type.instance.decompose(1))),
                                                   PartitionRangeReadCommand.allDataRead(cfs.metadata(), nowInSec));

        execute("ALTER TABLE %s ADD c " + type);
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 2, 2, " + literal + ") USING TIMESTAMP 30");
        flush();

        for (ReadCommand command : commands)
        {
            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator partitions = command.executeLocally(controller))
            {
                ReadResponse.createDataResponse(partitions, command, RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);
            }
        }
    }

    @Test
    public void testDroppedStaticColumn() throws Throwable
    {
        for (boolean flush : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, s int static, sc int static, PRIMARY KEY (pk, ck))");
            disableCompaction();
            long timestamp = FBUtilities.timestampMicros();
            execute("INSERT INTO %s (pk, ck, v, s, sc) VALUES (1, 1, 1, 2, 3) USING TIMESTAMP " + timestamp);
            if (flush)
                flush();
            execute("ALTER TABLE %s DROP sc USING TIMESTAMP " + (timestamp - 1));

            assertRowsNet(executeNet("SELECT * FROM %s WHERE pk = 1"), row(1, 1, 2, 1));
            assertRowsNet(executeNet("SELECT * FROM %s"), row(1, 1, 2, 1));
        }
    }
}
