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

package org.apache.cassandra.tools;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataSerializer;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link Util#metadataFromSSTable} on sstables of the 3.x format "me" with user type columns. The sstables are written
 * as "nb", which has the same data format, and their statistics are rewritten for "me". For the frozen columns, the
 * header type is made non-frozen, as Cassandra 3.0 writes it (see CASSANDRA-15035).
 */
public class MetadataFromLegacySSTableUserTypeTest extends CQLTester
{
    private static final AtomicInteger generation = new AtomicInteger(1);

    @BeforeClass
    public static void setUpFormat()
    {
        System.setProperty(SSTableFormat.FORMAT_DEFAULT_PROP, SSTableFormat.Type.BIG.name);
    }

    @Test
    public void testFrozenUserTypesNotMarkedFrozenInHeader() throws Throwable
    {
        assertFrozen("INSERT INTO %s (pk, ck, v) VALUES (0, 0, {a: 1, b: 'abc'})");
        assertFrozen("INSERT INTO %s (pk, ck, v) VALUES (0, 0, null)");
        assertFrozen("INSERT INTO %s (pk, ck, v) VALUES (0, 0, {a: 1, b: 'abc'}) USING TTL 1000");
        assertFrozen("INSERT INTO %s (pk, ck, x) VALUES (0, 0, 1)");
        assertFrozen("INSERT INTO %s (pk, s) VALUES (0, {a: 7, b: 's'})");
    }

    @Test
    public void testNonFrozenUserTypes() throws Throwable
    {
        assertNonFrozen("INSERT INTO %s (pk, ck, v) VALUES (0, 0, {a: 1, b: 'abc'})");
        assertNonFrozen("UPDATE %s SET v.a = 1 WHERE pk = 0 AND ck = 0");
        assertNonFrozen("UPDATE %s SET v = null WHERE pk = 0 AND ck = 0");
        assertNonFrozen("DELETE v.b FROM %s WHERE pk = 0 AND ck = 0");
        assertNonFrozen("UPDATE %s SET s.b = 'q' WHERE pk = 0");
    }

    /**
     * With a non-frozen user type column next to the frozen one, the sstable does not read with all of them frozen,
     * so the header's types are kept.
     */
    @Test
    public void testFrozenAndNonFrozenUserTypes() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk int, ck int, s frozen<" + type + "> static, v frozen<" + type + ">, n " + type + ", x int, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': false}");
        Descriptor desc = writeMe("INSERT INTO %s (pk, ck, v, n) VALUES (0, ?, {a: ?, b: 'abc'}, {a: ?, b: 'def'})", true);
        TableMetadata metadata = Util.metadataFromSSTable(desc);
        assertTrue(metadata.getColumn(ByteBuffer.wrap("v".getBytes())).type.isMultiCell());
        assertTrue(metadata.getColumn(ByteBuffer.wrap("n".getBytes())).type.isMultiCell());
    }

    private void assertFrozen(String firstWrite) throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk int, ck int, s frozen<" + type + "> static, v frozen<" + type + ">, x int, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': false}");
        assertUserTypeColumns(writeMe(firstWrite, true), false);
    }

    private void assertNonFrozen(String firstWrite) throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk int, ck int, s " + type + " static, v " + type + ", x int, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': false}");
        assertUserTypeColumns(writeMe(firstWrite, false), true);
    }

    private static void assertUserTypeColumns(Descriptor desc, boolean multiCell) throws Throwable
    {
        TableMetadata metadata = Util.metadataFromSSTable(desc);
        // the header has the columns that have data
        ColumnMetadata s = metadata.getColumn(ByteBuffer.wrap("s".getBytes()));
        if (s != null)
            assertEquals(multiCell, s.type.isMultiCell());
        assertEquals(multiCell, metadata.getColumn(ByteBuffer.wrap("v".getBytes())).type.isMultiCell());

        int rows = 0;
        SSTableReader sstable = SSTableReader.openNoValidation(desc, TableMetadataRef.forOfflineTools(metadata));
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    if (!partition.staticRow().isEmpty())
                        rows++;
                    while (partition.hasNext())
                    {
                        partition.next();
                        rows++;
                    }
                }
            }
        }
        finally
        {
            sstable.selfRef().release();
        }
        assertEquals(5, rows);
    }

    /**
     * Writes the specified first row and 4 more, and returns the sstable as an "me" one, with the header types of the
     * user type columns made non-frozen if {@code unfreezeUserTypes}.
     */
    private Descriptor writeMe(String firstWrite, boolean unfreezeUserTypes) throws Throwable
    {
        disableCompaction();
        if (firstWrite.contains("?"))
        {
            // the same write for every row
            for (int ck = 0; ck < 5; ck++)
                execute(firstWrite, ck, ck, ck);
        }
        else
        {
            execute(firstWrite);
            for (int ck = 1; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v, x) VALUES (0, ?, {a: ?, b: 'row'}, ?)", ck, ck, ck);
        }
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        if (cfs.getLiveSSTables().size() > 1)
            cfs.forceMajorCompaction();
        assertEquals(1, cfs.getLiveSSTables().size());
        Descriptor nb = cfs.getLiveSSTables().iterator().next().descriptor;
        assertEquals("nb", nb.version.getVersion());

        File dir = new File(DatabaseDescriptor.getAllDataFileLocations()[0], "me_" + nb.ksname + '/' + nb.cfname);
        dir.tryCreateDirectories();
        Descriptor me = new Descriptor(BigFormat.instance.getVersion("me"), dir, nb.ksname, nb.cfname,
                                       new SequenceBasedSSTableId(generation.getAndIncrement()), SSTableFormat.Type.BIG);
        for (Component component : SSTable.discoverComponentsFor(nb))
        {
            if (!component.equals(Component.STATS))
                Files.copy(nb.fileFor(component).toPath(), me.fileFor(component).toPath());
        }

        Map<MetadataType, MetadataComponent> metadata = nb.getMetadataSerializer().deserialize(nb, EnumSet.allOf(MetadataType.class));
        if (unfreezeUserTypes)
            metadata.put(MetadataType.HEADER, unfreezeUserTypes((SerializationHeader.Component) metadata.get(MetadataType.HEADER)));
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(me.fileFor(Component.STATS)))
        {
            new MetadataSerializer().serialize(metadata, out, me);
        }
        return me;
    }

    private static SerializationHeader.Component unfreezeUserTypes(SerializationHeader.Component header) throws Exception
    {
        Constructor<SerializationHeader.Component> constructor =
            SerializationHeader.Component.class.getDeclaredConstructor(AbstractType.class, List.class, LinkedHashMap.class, LinkedHashMap.class, EncodingStats.class);
        constructor.setAccessible(true);
        return constructor.newInstance(header.getKeyType(), header.getClusteringTypes(), unfreezeUserTypes(header.getStaticColumns()),
                                       unfreezeUserTypes(header.getRegularColumns()), header.getEncodingStats());
    }

    private static LinkedHashMap<ByteBuffer, AbstractType<?>> unfreezeUserTypes(Map<ByteBuffer, AbstractType<?>> columns)
    {
        LinkedHashMap<ByteBuffer, AbstractType<?>> unfrozen = new LinkedHashMap<>();
        for (Map.Entry<ByteBuffer, AbstractType<?>> entry : columns.entrySet())
        {
            AbstractType<?> type = entry.getValue();
            if (type.isUDT() && !type.isMultiCell())
            {
                UserType userType = (UserType) type;
                type = new UserType(userType.keyspace, userType.name, userType.fieldNames(), userType.fieldTypes(), true);
            }
            unfrozen.put(entry.getKey(), type);
        }
        return unfrozen;
    }
}
