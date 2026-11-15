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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.compaction.CompactionDroppedColumnVersionsTest.typeOfC;
import static org.junit.Assert.assertEquals;

/**
 * The type that the header of a compaction output gives a column whose type changed while the input sstables were
 * open, when the sstables written before the change have the highest id.
 */
public class CompactionOutputHeaderTypeTest extends CQLTester
{
    @Test
    public void testTextReAddedAsBlob() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c text, PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, 'a')");
        flush();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 2, 1, 'b')");
        flush();
        execute("ALTER TABLE %s DROP c");
        execute("ALTER TABLE %s ADD c blob");
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 2, 0xff00fe)");
        flush();

        compactAfterEarlierTypeSSTables(sstable -> typeOfC(sstable) instanceof UTF8Type);

        Object[][] rows = { row(1, 1, 2, ByteBufferUtil.hexToBytes("ff00fe")), row(1, 2, 1, null) };
        assertRows(execute("SELECT pk, ck, v, c FROM %s"), rows);
        assertEquals(schemaTypeOfC(), typeOfC(onlySSTable()));
        scrub();
        assertRows(execute("SELECT pk, ck, v, c FROM %s"), rows);
    }

    @Test
    public void testTextReAddedAsBlobAndDropped() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c text, PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, 'a')");
        flush();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 2, 1, 'b')");
        flush();
        execute("ALTER TABLE %s DROP c");
        execute("ALTER TABLE %s ADD c blob");
        long timestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 2, 0xff00fe) USING TIMESTAMP " + timestamp);
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (timestamp - 1));

        compactAfterEarlierTypeSSTables(sstable -> typeOfC(sstable) instanceof UTF8Type);

        Object[][] rows = { row(1, 1, 2), row(1, 2, 1) };
        assertRows(execute("SELECT pk, ck, v FROM %s"), rows);
        assertEquals(droppedTypeOfC(), typeOfC(onlySSTable()));
        scrub();
        assertRows(execute("SELECT pk, ck, v FROM %s"), rows);
        execute("ALTER TABLE %s ADD c blob");
        assertRows(execute("SELECT pk, ck, v, c FROM %s"), row(1, 1, 2, ByteBufferUtil.hexToBytes("ff00fe")), row(1, 2, 1, null));
    }

    /**
     * Two type changes before the column is dropped, with a cell of the middle type that is newer than the drop. No
     * input sstable has the type the column was dropped with.
     */
    @Test
    public void testTwoTypeChangesBeforeDrop() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c ascii, PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, 'a')");
        flush();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 2, 1, 'b')");
        flush();
        execute("ALTER TABLE %s DROP c");
        execute("ALTER TABLE %s ADD c text");
        long timestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 3, 2, 'é') USING TIMESTAMP " + timestamp);
        flush();
        // a drop can't be older than an earlier drop of the column
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (timestamp - 2));
        execute("ALTER TABLE %s ADD c blob");
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (timestamp - 1));

        compactAfterEarlierTypeSSTables(sstable -> typeOfC(sstable) instanceof AsciiType);

        Object[][] rows = { row(1, 1, 1), row(1, 2, 1), row(1, 3, 2) };
        assertRows(execute("SELECT pk, ck, v FROM %s"), rows);
        assertEquals(droppedTypeOfC(), typeOfC(onlySSTable()));
        scrub();
        assertRows(execute("SELECT pk, ck, v FROM %s"), rows);
    }

    /**
     * Compacts the sstables matching {@code earlierType} together, so that the result has the highest id, then
     * compacts everything. The tests write a single partition, so that each compaction has a single output.
     */
    private void compactAfterEarlierTypeSSTables(Predicate<SSTableReader> earlierType) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<Descriptor> earlier = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (earlierType.test(sstable))
                earlier.add(sstable.descriptor);
        }
        assertEquals(2, earlier.size());
        FBUtilities.waitOnFuture(CompactionManager.instance.submitUserDefined(cfs, earlier, CompactionManager.NO_GC));
        assertEquals(2, cfs.getLiveSSTables().size());
        compact();
        onlySSTable();
    }

    private void scrub() throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL,
                     cfs.scrub(true, IScrubber.options().checkData().build(), 1));
    }

    private SSTableReader onlySSTable()
    {
        assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
        return getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
    }

    private AbstractType<?> schemaTypeOfC()
    {
        return currentTableMetadata().getColumn(ColumnIdentifier.getInterned("c", false)).type;
    }

    private AbstractType<?> droppedTypeOfC()
    {
        return currentTableMetadata().getDroppedColumn(UTF8Type.instance.decompose("c")).type;
    }
}
