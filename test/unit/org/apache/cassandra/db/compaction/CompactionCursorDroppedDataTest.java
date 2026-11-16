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

import java.util.Objects;

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.compaction.SortedStringTableCursor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_CURSOR_COMPACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Cursor compaction of sstables without tombstones, where some rows and partitions hold nothing but data of a dropped
 * column that is older than the drop. The compaction leaves that data out, and must leave out the rows and partitions
 * it empties too, as iterator compaction does: the two paths must write the same sstable.
 */
public class CompactionCursorDroppedDataTest extends CQLTester
{
    /**
     * What a compaction wrote: the number of partitions and of rows, and the size of the data.
     */
    private static class Output
    {
        final int partitions;
        final int rows;
        final long dataLength;

        Output(SSTableReader sstable)
        {
            int partitions = 0;
            int rows = 0;
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        ++partitions;
                        if (!partition.staticRow().isEmpty())
                            ++rows;
                        while (partition.hasNext())
                        {
                            partition.next();
                            ++rows;
                        }
                    }
                }
            }
            this.partitions = partitions;
            this.rows = rows;
            this.dataLength = sstable.uncompressedLength();
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Output))
                return false;
            Output that = (Output) other;
            return partitions == that.partitions && rows == that.rows && dataLength == that.dataLength;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitions, rows, dataLength);
        }

        @Override
        public String toString()
        {
            return partitions + " partition(s), " + rows + " row(s), " + dataLength + " bytes of data";
        }
    }

    @Test
    public void testSimpleColumn() throws Throwable
    {
        checkCursorsMatchIterators("int", "c = 1");
    }

    @Test
    public void testComplexColumn() throws Throwable
    {
        checkCursorsMatchIterators("set<int>", "c = c + {1}");
    }

    @Test
    public void testStaticComplexColumn() throws Throwable
    {
        checkCursorsMatchIterators("set<int> static", "c = c + {1}");
    }

    /**
     * The only static column is dropped, and some of its cells are newer than the drop. Both paths must leave out the
     * static rows, as the iterators' merge returns no static row for a table with no static column, and the partition
     * left empty: the cells newer than the drop are not kept, and do not come back when the column is re-added.
     */
    @Test
    public void testLastStaticColumnWithCellNewerThanDrop() throws Throwable
    {
        Output byIterators = compactWithLastStaticColumnDropped(false);
        assertEquals(2, byIterators.partitions);
        assertEquals(2, byIterators.rows);
        assertEquals(byIterators, compactWithLastStaticColumnDropped(true));
    }

    /**
     * As {@link #testLastStaticColumnWithCellNewerThanDrop}, with all the data newer than the drop: the cursor skips
     * the static rows, which leaves partition 3 empty, although it leaves out no cell of a dropped column.
     */
    @Test
    public void testLastStaticColumnWithAllDataNewerThanDrop() throws Throwable
    {
        Output byIterators = compactWithLastStaticColumnDroppedBeforeData(false);
        assertEquals(2, byIterators.partitions);
        assertEquals(2, byIterators.rows);
        assertEquals(byIterators, compactWithLastStaticColumnDroppedBeforeData(true));
    }

    private Output compactWithLastStaticColumnDroppedBeforeData(boolean useCursors) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, s int static, PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("UPDATE %s USING TIMESTAMP 200 SET s = 1 WHERE pk = 1");
        execute("UPDATE %s USING TIMESTAMP 200 SET v = 1 WHERE pk = 1 AND ck = 1");
        flush();
        execute("UPDATE %s USING TIMESTAMP 200 SET s = 2 WHERE pk = 2");
        execute("UPDATE %s USING TIMESTAMP 200 SET v = 2 WHERE pk = 2 AND ck = 1");
        execute("UPDATE %s USING TIMESTAMP 200 SET s = 3 WHERE pk = 3");
        flush();
        execute("ALTER TABLE %s DROP s USING TIMESTAMP 100");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            assertFalse(sstable.header.hasColumnDroppedAtOrAfter(cfs.metadata(), sstable.getMinTimestamp()));
            assertTrue(mayLeaveEmptyData(sstable));
        }
        Output output = compactWithoutTombstones(useCursors);
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 1, 1), row(2, 1, 2));
        return output;
    }

    /**
     * Only an sstable that has data of a dropped column at or before the drop, or static rows of a table that has no
     * static column any more, is read through a cursor that skips the empty data that leaving them out can leave.
     * A column dropped before the data was written, or dropped and re-added before the data was written, does not
     * make any difference to the others.
     */
    @Test
    public void testSkippingEmptyDataOnlyForSSTablesWithDataOfDroppedColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c set<int>, d int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        execute("ALTER TABLE %s DROP d USING TIMESTAMP 100");
        execute("UPDATE %s USING TIMESTAMP 200 SET v = 1, c = c + {1} WHERE pk = 1 AND ck = 1");
        flush();
        SSTableReader afterDrop = Iterables.getOnlyElement(cfs.getLiveSSTables());
        assertFalse(mayLeaveEmptyData(afterDrop));

        execute("UPDATE %s USING TIMESTAMP 300 SET c = c + {2} WHERE pk = 2 AND ck = 1");
        execute("UPDATE %s USING TIMESTAMP 300 SET c = c + {3} WHERE pk = 3 AND ck = 1");
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP 400");
        execute("ALTER TABLE %s ADD c set<int>");
        execute("UPDATE %s USING TIMESTAMP 600 SET c = c + {4} WHERE pk = 3 AND ck = 1");
        execute("UPDATE %s USING TIMESTAMP 600 SET c = c + {5} WHERE pk = 4 AND ck = 1");
        flush();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            // only the sstable written after c was re-added has no data at or before a drop
            boolean afterReAdd = sstable.getMinTimestamp() == 600;
            assertEquals(sstable.toString(), !afterReAdd, mayLeaveEmptyData(sstable));
        }

        Output byCursors = compactWithoutTombstones(true);
        assertEquals(3, byCursors.partitions);
        assertRowsIgnoringOrder(execute("SELECT pk, ck, v, c FROM %s"), row(1, 1, 1, null), row(3, 1, null, set(4)), row(4, 1, null, set(5)));
    }

    private static boolean mayLeaveEmptyData(SSTableReader sstable)
    {
        try (SortedStringTableCursor cursor = new SortedStringTableCursor(sstable))
        {
            return CompactionCursor.mayLeaveEmptyData(sstable, cursor, sstable.metadata());
        }
    }

    /**
     * Compacts the same data with cursors and with iterators, and checks that both write one partition, with the same
     * content and size.
     */
    private void checkCursorsMatchIterators(String columnType, String write) throws Throwable
    {
        Output byIterators = compactWithDroppedData(false, columnType, write);
        assertEquals(1, byIterators.partitions);
        assertEquals(byIterators, compactWithDroppedData(true, columnType, write));
    }

    /**
     * Writes partitions 1 and 2 with data of column {@code c} only, and partition 3 with a row that has {@code v}, and
     * data of {@code c} in another row or as a static column, in two sstables with no tombstones. Then drops
     * {@code c}, compacts the sstables, by cursors or by iterators, and describes the result.
     */
    private Output compactWithDroppedData(boolean useCursors, String columnType, String write) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c " + columnType + ", PRIMARY KEY (pk, ck))");
        disableCompaction();
        String update = "UPDATE %s USING TIMESTAMP 10 SET ";
        String where = columnType.endsWith("static") ? " WHERE pk = ?" : " WHERE pk = ? AND ck = 2";
        execute(update + write + where, 1);
        execute(update + "v = 1 WHERE pk = 3 AND ck = 1");
        flush();
        execute(update + write + where, 2);
        execute(update + write + where, 3);
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP 100");
        Output output = compactWithoutTombstones(useCursors);
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(3, 1, 1));
        return output;
    }

    /**
     * Writes two partitions with a row and a static cell each, and a third with a static cell only, in two sstables
     * with no tombstones. Then drops the static column, which is the only one, older than the cells of partitions 1
     * and 3, compacts the sstables, by cursors or by iterators, describes the result, and checks that re-adding the
     * column brings no cell back.
     */
    private Output compactWithLastStaticColumnDropped(boolean useCursors) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, s int static, PRIMARY KEY (pk, ck))");
        disableCompaction();
        execute("UPDATE %s USING TIMESTAMP 200 SET s = 1 WHERE pk = 1");
        execute("UPDATE %s USING TIMESTAMP 10 SET v = 1 WHERE pk = 1 AND ck = 1");
        flush();
        execute("UPDATE %s USING TIMESTAMP 10 SET s = 2 WHERE pk = 2");
        execute("UPDATE %s USING TIMESTAMP 10 SET v = 2 WHERE pk = 2 AND ck = 1");
        execute("UPDATE %s USING TIMESTAMP 200 SET s = 3 WHERE pk = 3");
        flush();
        execute("ALTER TABLE %s DROP s USING TIMESTAMP 100");
        Output output = compactWithoutTombstones(useCursors);
        assertRows(execute("SELECT pk, ck, v FROM %s"), row(1, 1, 1), row(2, 1, 2));

        execute("ALTER TABLE %s ADD s int static");
        assertRows(execute("SELECT pk, ck, v, s FROM %s"), row(1, 1, 1, null), row(2, 1, 2, null));
        return output;
    }

    /**
     * Checks that no sstable has tombstones, compacts them all into one, by cursors or by iterators, and describes the
     * result.
     */
    private Output compactWithoutTombstones(boolean useCursors) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertTrue(cfs.getLiveSSTables().size() > 1);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertFalse(sstable.mayHaveTombstones());

        try (WithProperties properties = new WithProperties().set(ALLOW_CURSOR_COMPACTION, useCursors))
        {
            withLogAppender(CompactionTask.class, appender -> {
                compact();
                String path = useCursors ? " by cursors" : " by iterators";
                assertTrue("Compaction did not run" + path,
                           appender.list.stream().anyMatch(event -> event.getFormattedMessage().contains(path)));
            });
        }
        return new Output(Iterables.getOnlyElement(cfs.getLiveSSTables()));
    }
}
