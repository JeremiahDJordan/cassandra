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
package org.apache.cassandra.db.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A disjunction of expressions on static columns and on other columns is evaluated per row, with the values of the
 * static columns taken from the static row of the partition ({@code RowFilter#filterWithStaticRow}). The deleted and
 * expired elements of a static collection don't count as elements there, as in {@code RowFilter#filter}. Each query is
 * checked through CQL (a single replica: no reconciliation, the static row is purged before it is evaluated), and by
 * applying the filter of the query, rebuilt as one that needs reconciliation, to the unpurged rows of the table.
 */
@RunWith(Parameterized.class)
public class StaticDisjunctionDeadCellsTest extends CQLTester
{
    @Parameterized.Parameter
    public boolean flush;

    @Parameterized.Parameters(name = "flush={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[]{ false }, new Object[]{ true });
    }

    @Before
    public void setup() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, st int static, sl list<text> static, ss set<text> static, " +
                    "sm map<text, text> static, PRIMARY KEY (pk, ck))");
        // 1: a row, and static collections with a deleted element each
        execute("INSERT INTO %s (pk, ck, v, st, sl, ss, sm) VALUES (1, 1, 0, 0, ['a', 'b'], {'a', 'b'}, {'k': 'v', 'l': 'w'})");
        execute("UPDATE %s SET sl = sl - ['a'], ss = ss - {'a'} WHERE pk = 1");
        execute("DELETE sm['k'] FROM %s WHERE pk = 1");
        // 2: a row matching v = 1, without a static row
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 1)");
        // 3: only a static row, whose collections only have deleted elements
        execute("INSERT INTO %s (pk, st, sl, ss, sm) VALUES (3, 0, ['a'], {'a'}, {'k': 'v'})");
        execute("UPDATE %s SET sl = sl - ['a'], ss = ss - {'a'} WHERE pk = 3");
        execute("DELETE sm['k'] FROM %s WHERE pk = 3");
        // 4: a row, and static collections whose only elements have expired
        execute("INSERT INTO %s (pk, ck, v, st) VALUES (4, 1, 0, 0)");
        execute("UPDATE %s USING TTL 1 SET sl = sl + ['x'], ss = ss + {'x'}, sm['x'] = 'y' WHERE pk = 4");
        Thread.sleep(2100);
        if (flush)
            flush();
    }

    @Test
    public void testDeadElementsOfStaticCollectionsInDisjunctions() throws Throwable
    {
        // a deleted or expired element isn't an element of empty value, nor of its old value
        assertFiltered("v = 1 OR sl CONTAINS ''", "2/1");
        assertFiltered("v = 1 OR sl CONTAINS 'a'", "2/1");
        assertFiltered("v = 1 OR sl CONTAINS 'x'", "2/1");
        assertFiltered("v = 1 OR ss CONTAINS 'a'", "2/1");
        assertFiltered("v = 1 OR ss CONTAINS 'x'", "2/1");
        assertFiltered("v = 1 OR sm CONTAINS ''", "2/1");
        assertFiltered("v = 1 OR sm CONTAINS KEY 'k'", "2/1");
        assertFiltered("v = 1 OR sm CONTAINS KEY 'x'", "2/1");
        assertFiltered("v = 1 OR sm['k'] = ''", "2/1");
        assertFiltered("v = 1 OR sm['x'] < 'z'", "2/1");
        assertFiltered("st = 0 AND (v = 1 OR sm CONTAINS KEY 'k')");
        // the live elements still are
        assertFiltered("v = 1 OR sl CONTAINS 'b'", "1/1", "2/1");
        assertFiltered("v = 1 OR ss CONTAINS 'b'", "1/1", "2/1");
        assertFiltered("v = 1 OR sm CONTAINS KEY 'l'", "1/1", "2/1");
        assertFiltered("v = 1 OR sm['l'] = 'w'", "1/1", "2/1");
        // and for the negations, a partition whose elements are all dead holds none: partition 3 has only its static row
        assertFiltered("v = 1 OR sl NOT CONTAINS ''", "1/1", "2/1", "3/null", "4/1");
        assertFiltered("v = 1 OR ss NOT CONTAINS 'a'", "1/1", "2/1", "3/null", "4/1");
        assertFiltered("v = 1 OR sm NOT CONTAINS KEY 'k'", "1/1", "2/1", "3/null", "4/1");
        assertFiltered("v = 1 OR sm NOT CONTAINS ''", "1/1", "2/1", "3/null", "4/1");
        assertFiltered("v = 1 OR ss NOT CONTAINS 'b'", "2/1", "3/null", "4/1");
    }

    private void assertFiltered(String restriction, String... expected) throws Throwable
    {
        String query = "SELECT pk, ck FROM %s WHERE " + restriction + " ALLOW FILTERING";
        List<String> sorted = new ArrayList<>(Arrays.asList(expected));
        Collections.sort(sorted);

        // a single replica: the read doesn't need reconciliation, and the replica purges the rows it filters
        List<String> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(query))
            rows.add(row.getInt("pk") + "/" + (row.has("ck") ? row.getInt("ck") : null));
        Collections.sort(rows);
        assertEquals(restriction + ", without reconciliation", sorted, rows);

        // the same filter for a read that needs reconciliation, which filters the rows unpurged
        ReadCommand command = parseReadCommand(query);
        assertTrue(restriction, command.rowFilter().root().hasMixedDisjunction() || !restriction.contains(" OR "));
        RowFilter filter = withReconciliation(command.rowFilter());
        assertEquals(restriction + ", with reconciliation", sorted, filter(filter));
    }

    private RowFilter withReconciliation(RowFilter filter) throws Throwable
    {
        int version = MessagingService.current_version;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RowFilter.serializer.serialize(filter, out, version);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), true))
            {
                return RowFilter.serializer.deserialize(in, version, currentTableMetadata(), true);
            }
        }
    }

    /**
     * @return the rows that the given filter keeps of the unpurged rows of the table, as {@code pk/ck}; a partition
     * kept without any row is {@code pk/null}, which is how it is returned if its static row has live data
     */
    private List<String> filter(RowFilter filter)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        long nowInSec = FBUtilities.nowInSeconds();
        ReadCommand all = PartitionRangeReadCommand.allDataRead(cfs.metadata(), nowInSec);
        List<String> rows = new ArrayList<>();
        try (ReadExecutionController controller = all.executionController();
             UnfilteredPartitionIterator partitions = filter.filter(all.executeLocally(controller), nowInSec))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    int pk = Int32Type.instance.compose(partition.partitionKey().getKey());
                    int count = 0;
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (!unfiltered.isRow())
                            continue;
                        rows.add(pk + "/" + Int32Type.instance.compose(((Row) unfiltered).clustering().bufferAt(0)));
                        count++;
                    }
                    if (count == 0)
                        rows.add(pk + "/null");
                }
            }
        }
        Collections.sort(rows);
        return rows;
    }
}
