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

package org.apache.cassandra.io.sstable;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tools.JsonTransformer;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@code scrub}, extended {@code verify} and {@code sstabledump} of an sstable with cells of a dropped user type column
 * that are newer than the drop. Once the sstable is reopened after the drop, the column is described from the drop
 * record, which has no field names: a non-frozen UDT becomes a multi-cell tuple, whose cells are still read field by
 * field. The frozen column and the sstable kept open are controls.
 */
@RunWith(Parameterized.class)
public class DroppedUserTypeColumnSSTableTest extends CQLTester
{
    private static final Object[][] ROWS = { row(1, 1, 1), row(2, 1, 2), row(3, 1, 3), row(4, 1, 4) };

    @Parameterized.Parameter(0)
    public boolean frozen;

    @Parameterized.Parameter(1)
    public boolean reopenSSTables;

    @Parameterized.Parameters(name = "frozen={0}, reopen={1}")
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(new Object[][]{ { false, true }, { false, false }, { true, true }, { true, false } });
    }

    @Test
    public void testScrub() throws Throwable
    {
        ColumnFamilyStore cfs = createSSTableWithCellsNewerThanDrop();
        CompactionManager.AllSSTableOpStatus status = cfs.scrub(true, IScrubber.options().checkData().build(), 1);
        assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL, status);
        assertRowsIgnoringOrder(execute("SELECT pk, ck, v FROM %s"), ROWS);
    }

    @Test
    public void testExtendedVerify() throws Throwable
    {
        ColumnFamilyStore cfs = createSSTableWithCellsNewerThanDrop();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        IVerifier.Options options = IVerifier.options()
                                              .invokeDiskFailurePolicy(false)
                                              .extendedVerification(true)
                                              .validateAllRows(true)
                                              .build();
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, options))
        {
            verifier.verify();
        }
        assertRowsIgnoringOrder(execute("SELECT pk, ck, v FROM %s"), ROWS);
    }

    /**
     * The JSON of {@code sstabledump} for the output of a compaction. For the non-frozen column, the cell paths are the
     * field names, or the field positions once the column is described from the drop record.
     */
    @Test
    public void testSSTableDumpAfterCompaction() throws Throwable
    {
        ColumnFamilyStore cfs = createSSTableWithCellsNewerThanDrop();
        Descriptor input = cfs.getLiveSSTables().iterator().next().descriptor;
        cfs.forceMajorCompaction();
        assertEquals(1, cfs.getLiveSSTables().size());
        Descriptor descriptor = cfs.getLiveSSTables().iterator().next().descriptor;
        assertNotEquals(input, descriptor);

        TableMetadata metadata = Util.metadataFromSSTable(descriptor);
        AbstractType<?> typeOfC = metadata.getColumn(ColumnIdentifier.getInterned("c", false)).type;
        assertEquals(!reopenSSTables, typeOfC.isUDT());
        assertEquals(!frozen, typeOfC.isMultiCell());

        TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(metadata);
        SSTableReader sstable = SSTableReader.openNoValidation(null, descriptor, metadataRef);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            JsonTransformer.toJson(scanner, Util.iterToStream(scanner), false, metadata, out);
        }
        finally
        {
            sstable.selfRef().release();
        }
        String json = out.toString(StandardCharsets.UTF_8.name()).replaceAll("\\s+", " ");
        for (int pk = 1; pk <= 4; pk++)
            assertTrue(json, json.contains("\"key\" : [ \"" + pk + "\" ]"));
        if (!frozen)
        {
            String a = reopenSSTables ? "0" : "a";
            String b = reopenSSTables ? "1" : "b";
            assertTrue(json, json.contains("{ \"name\" : \"c\", \"path\" : [ \"" + a + "\" ], \"value\" : 2"));
            assertTrue(json, json.contains("{ \"name\" : \"c\", \"path\" : [ \"" + b + "\" ], \"deletion_info\""));
        }
    }

    /**
     * Creates the table with the user type column {@code c} and one sstable with four rows, three of them with cells of
     * {@code c}, then drops {@code c} with a timestamp older than those cells, and maybe reopens the sstable.
     */
    private ColumnFamilyStore createSSTableWithCellsNewerThanDrop() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk int, ck int, v int, c " + (frozen ? "frozen<" + type + '>' : type) +
                    ", PRIMARY KEY (pk, ck))");
        disableCompaction();
        long timestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, {a: 1, b: 'x'}) USING TIMESTAMP " + timestamp);
        // For the non-frozen column, a field cell, and in another row a field tombstone, both with a cell path.
        String set = "UPDATE %s USING TIMESTAMP " + timestamp + " SET ";
        execute(set + (frozen ? "v = 2, c = {a: 2}" : "v = 2, c.a = 2") + " WHERE pk = 2 AND ck = 1");
        execute("INSERT INTO %s (pk, ck, v) VALUES (3, 1, 3) USING TIMESTAMP " + timestamp);
        execute(set + (frozen ? "v = 4, c = null" : "v = 4, c.b = null") + " WHERE pk = 4 AND ck = 1");
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (timestamp - 1));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        if (reopenSSTables)
        {
            cfs.clearUnsafe();
            cfs.loadNewSSTables();
        }
        assertEquals(1, cfs.getLiveSSTables().size());
        AbstractType<?> typeOfC = typeOfCInHeaderColumns(cfs.getLiveSSTables().iterator().next());
        assertEquals(!reopenSSTables, typeOfC.isUDT());
        assertTrue(typeOfC.isUDT() || typeOfC.isTuple());
        assertEquals(!frozen, typeOfC.isMultiCell());
        assertRowsIgnoringOrder(execute("SELECT pk, ck, v FROM %s"), ROWS);
        return cfs;
    }

    /**
     * The type of the column object that the sstable's header has for {@code c}, which is what validation uses.
     */
    private static AbstractType<?> typeOfCInHeaderColumns(SSTableReader sstable)
    {
        for (ColumnMetadata column : sstable.header.columns())
        {
            if (column.name.toString().equals("c"))
                return column.type;
        }
        throw new AssertionError("No column c in " + sstable);
    }
}
