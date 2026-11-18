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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/// A read of a memtable partition with no clustering slice to read (e.g. a page that starts after the only row a
/// clustering restriction selects) returns the partition's static row with only the selected static columns, like the
/// static row of a sliced read, since the serialization header of a response only has the fetched columns.
@RunWith(Parameterized.class)
public class TrieMemtableStaticRowSelectionTest extends CQLTester
{
    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[]{ "default" },
                             new Object[]{ "trie" },
                             new Object[]{ "trie_stage1" },
                             new Object[]{ "trie_stage2" },
                             new Object[]{ "trie_stage3" },
                             new Object[]{ "skiplist" });
    }

    private void createTestTable()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, s text static, sl list<text> static, " +
                    "PRIMARY KEY (pk, ck))" +
                    ("default".equals(memtable) ? "" : " WITH memtable = '" + memtable + '\''));
        disableCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 0, 1)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        execute("UPDATE %s SET s = 'a', sl = [''] WHERE pk = 1");
    }

    @Test
    public void testPagedQueriesFetchingSomeStaticColumns() throws Throwable
    {
        createTestTable();
        // Each query selects a single row and fetches some, but not all, of the static columns: the second page
        // reads the partition with an empty slice.
        String[][] queries = { { "SELECT pk, ck FROM %s WHERE sl CONTAINS '' AND ck = 0 ALLOW FILTERING", "1:0" },
                               { "SELECT pk, ck FROM %s WHERE sl CONTAINS '' AND ck = 1 ALLOW FILTERING", "1:1" },
                               { "SELECT pk, ck FROM %s WHERE pk = 1 AND ck = 0" +
                                 " AND sl CONTAINS '' ALLOW FILTERING", "1:0" },
                               { "SELECT pk, ck FROM %s WHERE ck IN (0) AND sl CONTAINS '' ALLOW FILTERING", "1:0" },
                               { "SELECT pk, ck FROM %s WHERE ck >= 0 AND ck <= 0" +
                                 " AND sl CONTAINS '' ALLOW FILTERING", "1:0" },
                               { "SELECT pk, ck, s FROM %s WHERE ck = 0 ALLOW FILTERING", "1:0" },
                               { "SELECT pk, ck FROM %s WHERE s = 'a' AND ck = 0 ALLOW FILTERING", "1:0" } };
        beforeAndAfterFlush(() -> {
            for (String[] query : queries)
            {
                for (int pageSize : new int[]{ 1, 2, 100 })
                {
                    List<String> rows = new ArrayList<>();
                    for (com.datastax.driver.core.Row row : executeNetWithPaging(query[0], pageSize))
                        rows.add(row.getInt("pk") + ":" + row.getInt("ck"));
                    assertEquals(query[0] + " with page size " + pageSize, List.of(query[1]), rows);
                }
            }
        });
    }

    @Test
    public void testEmptySliceReturnsSelectedStaticColumns()
    {
        createTestTable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = cfs.metadata();
        ColumnMetadata sl = metadata.getColumn(ByteBufferUtil.bytes("sl"));
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1));
        ColumnFilter selection = ColumnFilter.selection(RegularAndStaticColumns.builder().add(sl).build());

        Partition partition = cfs.getCurrentMemtable().getPartition(key);
        assertNotNull(partition);
        try (UnfilteredRowIterator iterator = partition.unfilteredIterator(selection, Slices.NONE, false))
        {
            assertOnlySelectedStaticColumn(iterator, sl);
        }

        // the same with an empty set of clusterings
        NavigableSet<Clustering<?>> noClusterings = BTreeSet.empty(metadata.comparator);
        try (UnfilteredRowIterator iterator = partition.unfilteredIterator(selection, noClusterings, false))
        {
            assertOnlySelectedStaticColumn(iterator, sl);
        }
    }

    private static void assertOnlySelectedStaticColumn(UnfilteredRowIterator iterator, ColumnMetadata column)
    {
        Row staticRow = iterator.staticRow();
        assertEquals(List.of(column), new ArrayList<>(staticRow.columns()));
        assertNotNull(staticRow.getComplexColumnData(column));
        assertFalse(iterator.hasNext());
    }
}
