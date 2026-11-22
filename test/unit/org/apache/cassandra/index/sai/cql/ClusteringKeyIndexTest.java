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

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.utils.TypeUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class ClusteringKeyIndexTest extends SAITester
{
    @Before
    public void createTableAndIndex()
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 text, val int, val2 int, PRIMARY KEY((pk1), pk2)) WITH CLUSTERING ORDER BY (pk2 DESC)");
        createIndex("CREATE CUSTOM INDEX pk2_idx ON %s(pk2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX val2_idx ON %s(val2) USING 'StorageAttachedIndex'");

        disableCompaction();
    }

    @After
    public void resetSegmentSize()
    {
        SegmentBuilder.updateLastValidSegmentRowId(-1);
    }

    private void insertData1() throws Throwable
    {
        execute("INSERT INTO %s (pk1, pk2, val, val2) VALUES (1, '1', 1, 1)");
        execute("INSERT INTO %s (pk1, pk2, val, val2) VALUES (2, '2', 2, 2)");
        execute("INSERT INTO %s (pk1, pk2, val, val2) VALUES (3, '3', 3, 3)");
    }

    private void insertData2() throws Throwable
    {
        execute("INSERT INTO %s (pk1, pk2, val, val2) VALUES (4, '4', 4, 4)");
        execute("INSERT INTO %s (pk1, pk2, val, val2) VALUES (5, '5', 5, 5)");
        execute("INSERT INTO %s (pk1, pk2, val, val2) VALUES (6, '6', 6, 6)");
    }

    @Test
    public void queryFromMemtable() throws Throwable
    {
        insertData1();
        insertData2();
        runQueries();
    }

    @Test
    public void queryFromSingleSSTable() throws Throwable
    {
        insertData1();
        insertData2();
        flush();
        runQueries();
    }

    @Test
    public void queryFromMultipleSSTables() throws Throwable
    {
        insertData1();
        flush();
        insertData2();
        flush();
        runQueries();
    }

    @Test
    public void queryFromMemtableAndSSTables() throws Throwable
    {
        insertData1();
        flush();
        insertData2();
        runQueries();
    }

    @Test
    public void queryFromCompactedSSTable() throws Throwable
    {
        insertData1();
        flush();
        insertData2();
        flush();
        compact();
        runQueries();
    }

    @Test
    public void reversedBigintClusteringMultiSegment()
    {
        createTable("CREATE TABLE %s (pk int, ck bigint, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
        createIndex("CREATE INDEX ON %s(ck) USING 'sai'");
        disableCompaction(keyspace());

        for (long ck = 10; ck < 16; ck++)
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 1, ck);
        flush();

        for (long ck = 16; ck < 22; ck++)
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 1, ck);
        flush();

        // Force compaction with a small segment size cap, producing multiple segments.
        SegmentBuilder.updateLastValidSegmentRowId(3);
        compact();

        for (long ck = 10; ck < 22; ck++)
        {
            int cnt = getRows(execute("SELECT ck FROM %s WHERE ck = ?", ck)).length;
            assertEquals("Missing row for ck=" + ck, 1, cnt);
        }
    }

    @Test
    public void reversedInetClusteringMultiSegment() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck inet, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
        disableCompaction();
        // two SSTables with the same terms
        for (int pk = 1; pk <= 2; pk++)
        {
            for (int ck = 10; ck < 22; ck++)
                execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", pk, inet(ck));
            flush();
        }

        // Build the index with a small segment size cap, producing multiple segments.
        SegmentBuilder.updateLastValidSegmentRowId(3);
        String index = createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");

        Collection<SSTableIndex> sstableIndexes = getIndexContext(index).getView().getIndexes();
        assertEquals(2, sstableIndexes.size());
        for (SSTableIndex sstableIndex : sstableIndexes)
        {
            assertEquals(TypeUtil.asIndexBytes(InetAddressType.instance.decompose(inet(10)), InetAddressType.instance), sstableIndex.minTerm());
            assertEquals(TypeUtil.asIndexBytes(InetAddressType.instance.decompose(inet(21)), InetAddressType.instance), sstableIndex.maxTerm());
        }

        for (int ck = 10; ck < 22; ck++)
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE ck = ?", inet(ck)), row(1), row(2));
    }

    private static InetAddress inet(int lastByte) throws UnknownHostException
    {
        return InetAddress.getByAddress(new byte[]{ 127, 0, 0, (byte) lastByte });
    }

    @Test
    public void decimalClusteringAtScaleLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck decimal, val int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        BigDecimal[] cks = { new BigDecimal("-5E-2147483647"), new BigDecimal("1E-2147483647"), new BigDecimal("1.25") };
        for (int i = 0; i < cks.length; i++)
            execute("INSERT INTO %s (pk, ck, val) VALUES (0, ?, ?)", cks[i], i);
        runDecimalClusteringQueries(cks);
        flush();
        runDecimalClusteringQueries(cks);
    }

    private void runDecimalClusteringQueries(BigDecimal[] cks) throws Throwable
    {
        for (int i = 0; i < cks.length; i++)
            assertRows(execute("SELECT * FROM %s WHERE val = ?", i), row(0, cks[i], i));
        assertRows(execute("SELECT * FROM %s WHERE val >= 0"), row(0, cks[0], 0), row(0, cks[1], 1), row(0, cks[2], 2));
    }

    private Object[] expectedRow(int index)
    {
        return row(index, Integer.toString(index), index, index);
    }

    private void runQueries() throws Throwable
    {
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 = 2"), expectedRow(2));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk2 = '2'"), expectedRow(2));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 = -1 AND pk2 = '2'"));

        assertThatThrownBy(()->execute("SELECT * FROM %s WHERE pk1 = -1 AND val = 2")).hasMessageContaining("use ALLOW FILTERING");

        // Add an assertion that covers searching a non-primary key column
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE val2 = 1"), expectedRow(1));
    }
}
