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

package org.apache.cassandra.db.memtable;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/**
 * Clustering keys, set elements and map keys whose byte-comparable form is shared with other forms of the value
 * (decimals with another scale, varints with redundant leading bytes, tuples with fewer or empty components) keep the
 * bytes written, read from the memtable, after a flush and in partition updates.
 */
@RunWith(Parameterized.class)
public class KeyValuesMemtableTest extends CQLTester
{
    // in clustering order
    private static final BigDecimal[] VALUES = { new BigDecimal("-12.5"), new BigDecimal("0.00"), new BigDecimal("1.1"),
                                                 new BigDecimal("1.200"), new BigDecimal("100"), new BigDecimal("1E+3") };

    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object> parameters()
    {
        // "default" means no memtable option on the table, i.e. TrieMemtable
        return Arrays.asList("default", "TrieMemtable", "TrieMemtableStage1", "SkipListMemtable");
    }

    private String withMemtable(String createTable)
    {
        if ("default".equals(memtable))
            return createTable;
        return createTable + (createTable.contains(" WITH ") ? " AND" : " WITH") + " memtable = { 'class': '" + memtable + "' }";
    }

    @Test
    public void testDecimals() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck decimal, t frozen<tuple<int, decimal>>, v decimal, s set<decimal>, m map<decimal, decimal>, PRIMARY KEY (pk, ck, t))"));
        disableCompaction();
        for (BigDecimal value : VALUES)
            execute("INSERT INTO %s (pk, ck, t, v, s, m) VALUES (0, ?, ?, ?, ?, ?)", value, tuple(1, value), value, set(value), map(value, value));
        for (BigDecimal value : VALUES)
            execute("UPDATE %s SET v = ?, s = s + ?, m = m + ? WHERE pk = 1 AND ck = ? AND t = ?", value, set(value), map(value, value), value, tuple(1, value));

        assertDecimals();
        flush();
        assertDecimals();
    }

    private void assertDecimals() throws Throwable
    {
        for (int pk = 0; pk < 2; pk++)
        {
            List<Object[]> rows = new ArrayList<>();
            for (BigDecimal value : VALUES)
                rows.add(row(value, tuple(1, value), value, set(value), map(value, value)));
            assertRows(execute("SELECT ck, t, v, s, m FROM %s WHERE pk = ?", pk), rows.toArray(new Object[0][]));
            for (BigDecimal value : VALUES)
            {
                assertRows(execute("SELECT ck, s FROM %s WHERE pk = ? AND ck = ?", pk, value.setScale(value.scale() + 2)),
                           row(value, set(value)));
                assertRows(execute("SELECT m[?] FROM %s WHERE pk = ? AND ck = ?", value, pk, value), row(value));
            }
        }
    }

    @Test
    public void testOverwriteWithOtherScale() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck decimal, v int, s set<decimal>, PRIMARY KEY (pk, ck))"));
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v, s) VALUES (0, ?, 1, ?)", new BigDecimal("1.1"), set(new BigDecimal("2.5")));
        execute("UPDATE %s SET v = 2, s = s + ? WHERE pk = 0 AND ck = ?", set(new BigDecimal("2.50")), new BigDecimal("1.100"));

        assertRows(execute("SELECT ck, v, s FROM %s WHERE pk = 0"), row(new BigDecimal("1.1"), 2, set(new BigDecimal("2.50"))));
        flush();
        assertRows(execute("SELECT ck, v, s FROM %s WHERE pk = 0"), row(new BigDecimal("1.1"), 2, set(new BigDecimal("2.50"))));
    }

    @Test
    public void testOverwriteWithOtherVarintForm() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck varint, v int, PRIMARY KEY (pk, ck))"));
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, blobAsVarint(0x01), 1)");
        execute("UPDATE %s SET v = 2 WHERE pk = 0 AND ck = blobAsVarint(0x0001)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, blobAsVarint(0x0001), 1)");
        execute("UPDATE %s SET v = 2 WHERE pk = 1 AND ck = blobAsVarint(0x01)");

        assertOverwriteWithOtherVarintForm();
        flush();
        assertOverwriteWithOtherVarintForm();
    }

    private void assertOverwriteWithOtherVarintForm() throws Throwable
    {
        assertRows(execute("SELECT varintAsBlob(ck), v FROM %s WHERE pk = 0"), row(ByteBufferUtil.hexToBytes("01"), 2));
        assertRows(execute("SELECT varintAsBlob(ck), v FROM %s WHERE pk = 1"), row(ByteBufferUtil.hexToBytes("0001"), 2));
        assertRows(execute("SELECT varintAsBlob(ck) FROM %s WHERE pk = 0 AND ck = blobAsVarint(0x0001)"), row(ByteBufferUtil.hexToBytes("01")));
        assertRows(execute("SELECT varintAsBlob(ck) FROM %s WHERE pk = 1 AND ck = blobAsVarint(0x01)"), row(ByteBufferUtil.hexToBytes("0001")));
    }

    @Test
    public void testLongClusterings() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck1 decimal, ck2 text, v int, PRIMARY KEY (pk, ck1, ck2))"));
        disableCompaction();
        String text = Strings.repeat("x", 40000);
        execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (0, ?, ?, 0)", new BigDecimal("1.1"), text);
        execute("BEGIN UNLOGGED BATCH " +
                "INSERT INTO %1$s (pk, ck1, ck2, v) VALUES (1, 2.5, '" + text + "', 1); " +
                "INSERT INTO %1$s (pk, ck1, ck2, v) VALUES (1, 3.5, '" + text + "', 2); " +
                "APPLY BATCH");

        assertLongClusterings(text);
        flush();
        assertLongClusterings(text);
    }

    private void assertLongClusterings(String text) throws Throwable
    {
        assertRows(execute("SELECT ck1, ck2, v FROM %s WHERE pk = 0"), row(new BigDecimal("1.1"), text, 0));
        assertRows(execute("SELECT ck1, ck2, v FROM %s WHERE pk = 1"), row(new BigDecimal("2.5"), text, 1), row(new BigDecimal("3.5"), text, 2));
    }

    @Test
    public void testVarints() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck varint, s set<varint>, m map<varint, int>, PRIMARY KEY (pk, ck))"));
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, s, m) VALUES (0, blobAsVarint(0x0001), {blobAsVarint(0x0001)}, {blobAsVarint(0xff80): 1})");
        execute("INSERT INTO %s (pk, ck, s, m) VALUES (0, blobAsVarint(0xff80), {blobAsVarint(0xff80)}, {blobAsVarint(0x0001): 2})");

        assertVarints();
        flush();
        assertVarints();
    }

    private void assertVarints() throws Throwable
    {
        List<String> found = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT ck, s, m FROM %s WHERE pk = 0"))
        {
            found.add(ByteBufferUtil.bytesToHex(row.getBytes("ck")));
            for (ByteBuffer element : row.getSet("s", BytesType.instance))
                found.add(ByteBufferUtil.bytesToHex(element));
            for (ByteBuffer key : row.getMap("m", BytesType.instance, Int32Type.instance).keySet())
                found.add(ByteBufferUtil.bytesToHex(key));
        }
        assertEquals(Arrays.asList("ff80", "ff80", "0001", "0001", "0001", "ff80"), found);
    }

    @Test
    public void testTupleForms() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck frozen<tuple<int, int>>, s set<frozen<tuple<int, int>>>, PRIMARY KEY (pk, ck))"));
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, s) VALUES (0, (1), {(2)})");
        // a null of length -2 and an empty int
        execute("INSERT INTO %s (pk, ck, s) VALUES (1, ?, ?)", ByteBufferUtil.hexToBytes("00000004000000ff" + "fffffffe"),
                set(ByteBufferUtil.hexToBytes("00000000" + "0000000400000003")));

        assertTupleForms();
        flush();
        assertTupleForms();
    }

    private void assertTupleForms() throws Throwable
    {
        List<String> found = new ArrayList<>();
        for (int pk = 0; pk < 2; pk++)
        {
            for (UntypedResultSet.Row row : execute("SELECT ck, s FROM %s WHERE pk = ?", pk))
            {
                found.add(ByteBufferUtil.bytesToHex(row.getBytes("ck")));
                for (ByteBuffer element : row.getSet("s", BytesType.instance))
                    found.add(ByteBufferUtil.bytesToHex(element));
            }
        }
        assertEquals(Arrays.asList("0000000400000001", "0000000400000002", "00000004000000fffffffffe", "000000000000000400000003"), found);
    }

    @Test
    public void testEmptyValuesInFrozenCollections() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck frozen<list<int>>, s set<frozen<tuple<int, int>>>, PRIMARY KEY (pk, ck))"));
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, s) VALUES (0, [blobAsInt(0x), 2], {(blobAsInt(0x), 1)})");

        assertEmptyValuesInFrozenCollections();
        flush();
        assertEmptyValuesInFrozenCollections();
    }

    private void assertEmptyValuesInFrozenCollections() throws Throwable
    {
        List<String> found = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT ck, s FROM %s WHERE pk = 0"))
        {
            found.add(ByteBufferUtil.bytesToHex(row.getBytes("ck")));
            for (ByteBuffer element : row.getSet("s", BytesType.instance))
                found.add(ByteBufferUtil.bytesToHex(element));
        }
        assertEquals(Arrays.asList("00000002" + "00000000" + "0000000400000002", "00000000" + "0000000400000001"), found);
    }

    @Test
    public void testBooleans() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck boolean, s set<boolean>, PRIMARY KEY (pk, ck))"));
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, s) VALUES (0, blobAsBoolean(0x02), {blobAsBoolean(0x03)})");

        assertBooleans();
        flush();
        assertBooleans();
    }

    private void assertBooleans() throws Throwable
    {
        UntypedResultSet.Row row = execute("SELECT ck, s FROM %s WHERE pk = 0").one();
        assertEquals("02", ByteBufferUtil.bytesToHex(row.getBytes("ck")));
        assertEquals(ImmutableSet.of(ByteBufferUtil.hexToBytes("03")), row.getSet("s", BytesType.instance));
    }

    @Test
    public void testUserTypeWithAddedField() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int)");
        createTable(withMemtable("CREATE TABLE %s (pk int, ck frozen<" + type + ">, v int, PRIMARY KEY (pk, ck))"));
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, {a: 1}, 1)");
        execute("ALTER TYPE " + KEYSPACE + '.' + type + " ADD b int");

        assertEquals("0000000400000001", ByteBufferUtil.bytesToHex(execute("SELECT ck FROM %s WHERE pk = 0").one().getBytes("ck")));
        flush();
        assertEquals("0000000400000001", ByteBufferUtil.bytesToHex(execute("SELECT ck FROM %s WHERE pk = 0").one().getBytes("ck")));
    }

    @Test
    public void testUserTypeInRangeDeletionWithAddedField() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int)");
        createTable(withMemtable("CREATE TABLE %s (pk int, ck frozen<list<frozen<" + type + ">>>, v int, PRIMARY KEY (pk, ck))"));
        disableCompaction();
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= [{a: 1}] AND ck <= [{a: 2}]");
        execute("ALTER TYPE " + KEYSPACE + '.' + type + " ADD b int");

        List<String> expected = Arrays.asList("00000001" + "00000008" + "0000000400000001", "00000001" + "00000008" + "0000000400000002");
        assertEquals(expected, rangeTombstoneBounds());
        flush();
        assertEquals(expected, rangeTombstoneBounds());
    }

    private List<String> rangeTombstoneBounds()
    {
        List<String> found = new ArrayList<>();
        try (UnfilteredRowIterator iterator = Util.getOnlyPartitionUnfiltered(Util.cmd(getCurrentColumnFamilyStore(), 0).build()).unfilteredIterator())
        {
            while (iterator.hasNext())
                found.add(ByteBufferUtil.bytesToHex(iterator.next().clustering().bufferAt(0)));
        }
        return found;
    }

    @Test
    public void testDescendingOrderAndStaticRow() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck decimal, st decimal static, v int, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)"));
        disableCompaction();
        execute("INSERT INTO %s (pk, st) VALUES (0, ?)", new BigDecimal("1.1"));
        for (int i = 0; i < VALUES.length; i++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", VALUES[i], i);

        assertDescendingOrderAndStaticRow();
        flush();
        assertDescendingOrderAndStaticRow();
    }

    private void assertDescendingOrderAndStaticRow() throws Throwable
    {
        List<Object[]> rows = new ArrayList<>();
        for (int i = VALUES.length - 1; i >= 0; i--)
            rows.add(row(VALUES[i], new BigDecimal("1.1"), i));
        assertRows(execute("SELECT ck, st, v FROM %s WHERE pk = 0"), rows.toArray(new Object[0][]));
        assertRows(execute("SELECT ck FROM %s WHERE pk = 0 AND ck < ? AND ck > ?", new BigDecimal("1.20"), new BigDecimal("0")),
                   row(new BigDecimal("1.1")));
    }

    @Test
    public void testIndexes() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck decimal, v int, w int, s set<decimal>, PRIMARY KEY (pk, ck))"));
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE INDEX ON %s(w)");
        disableCompaction();
        for (int i = 0; i < VALUES.length; i++)
            execute("INSERT INTO %s (pk, ck, v, w, s) VALUES (0, ?, ?, ?, ?)", VALUES[i], i, i, set(VALUES[i]));

        assertIndexQueries();
        flush();
        assertIndexQueries();
    }

    private void assertIndexQueries() throws Throwable
    {
        for (int i = 0; i < VALUES.length; i++)
        {
            assertRows(execute("SELECT ck, s FROM %s WHERE v = ?", i), row(VALUES[i], set(VALUES[i])));
            assertRows(execute("SELECT ck, s FROM %s WHERE w = ?", i), row(VALUES[i], set(VALUES[i])));
        }
    }

    @Test
    public void testPartitionUpdate() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck decimal, v int, s set<decimal>, PRIMARY KEY (pk, ck))"));
        BigDecimal value = new BigDecimal("1.1");
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(currentTableMetadata(), 0);
        builder.row(value).add("v", 1).add("s", ImmutableSet.of(value));
        builder.row(new BigDecimal("100")).delete();
        builder.addRangeTombstone().start(new BigDecimal("2.5")).end(new BigDecimal("3.0")).inclStart().exclEnd();
        PartitionUpdate update = builder.build();

        assertPartitionUpdate(update);
        try (UnfilteredRowIterator iterator = update.unfilteredIterator())
        {
            assertPartitionUpdate(TriePartitionUpdate.fromIterator(iterator));
        }
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(update, out, MessagingService.current_version);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                assertPartitionUpdate(PartitionUpdate.serializer.deserialize(in, MessagingService.current_version, DeserializationHelper.Flag.LOCAL));
            }
        }
    }

    private void assertPartitionUpdate(PartitionUpdate update)
    {
        List<String> found = new ArrayList<>();
        try (UnfilteredRowIterator iterator = update.unfilteredIterator())
        {
            while (iterator.hasNext())
            {
                Unfiltered unfiltered = iterator.next();
                found.add(clusteringString(unfiltered.clustering()));
                if (unfiltered.isRow())
                    for (Cell<?> cell : ((Row) unfiltered).cells())
                        if (cell.path() != null)
                            found.add(DecimalType.instance.compose(cell.path().get(0)).toString());
            }
        }
        assertEquals(Arrays.asList("1.1", "1.1", "2.5", "3.0", "100"), found);
    }

    private static String clusteringString(ClusteringPrefix<?> clustering)
    {
        return DecimalType.instance.compose(clustering.bufferAt(0)).toString();
    }

    @Test
    public void testTombstones() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck decimal, v int, PRIMARY KEY (pk, ck))"));
        disableCompaction();
        execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", new BigDecimal("100"));
        execute("DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 0 AND ck >= ? AND ck < ?", new BigDecimal("2.5"), new BigDecimal("3.0"));
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 0 AND ck >= ? AND ck <= ?", new BigDecimal("3.0"), new BigDecimal("3.5"));

        assertTombstones();
        flush();
        assertTombstones();
    }

    private void assertTombstones()
    {
        List<String> found = new ArrayList<>();
        try (UnfilteredRowIterator iterator = Util.getOnlyPartitionUnfiltered(Util.cmd(getCurrentColumnFamilyStore(), 0).build()).unfilteredIterator())
        {
            while (iterator.hasNext())
            {
                Unfiltered unfiltered = iterator.next();
                found.add(unfiltered.clustering().kind() + " " + clusteringString(unfiltered.clustering()));
            }
        }
        assertEquals(Arrays.asList("INCL_START_BOUND 2.5", "EXCL_END_INCL_START_BOUNDARY 3.0", "INCL_END_BOUND 3.5", "CLUSTERING 100"), found);
    }
}
