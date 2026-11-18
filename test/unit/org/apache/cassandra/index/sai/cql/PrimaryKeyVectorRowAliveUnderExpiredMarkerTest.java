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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.junit.Assert.assertEquals;

/**
 * ANN ordering by a vector in a primary key column: a row whose row marker was written again with a TTL and has
 * expired is still alive by older cells. The read of the ordering, with the query's column filter, drops the cells of
 * the columns the query doesn't read that are older than the marker (on the versions that add the synthetic score
 * column, ED and later, that is every regular column of a selection of primary key columns). The liveness of the row
 * that tells a stale key has to be decided from every column, and the row returned has to keep the cells that keep it
 * alive.
 */
public class PrimaryKeyVectorRowAliveUnderExpiredMarkerTest extends SAITester.Versioned
{
    private static final String[] PLANS = { " WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}",
                                            " WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'}",
                                            "" };

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.stream(new Version[]{ Version.EC, Version.ED, Version.GA }).map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Test
    public void testPartitionKeyVector() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, q int, l set<int>, PRIMARY KEY ((k1, k2), c))");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
        for (int c = 1; c <= 3; c++)
            execute("INSERT INTO %s (k1, k2, c, r, q, l) VALUES (1, [1, 0], ?, 0, 1, {1})", c);
        // the markers of rows 1 (with r) and 2 (alone) are written again with a TTL, and expire: q and l keep them alive
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 1, 2) USING TTL 1");
        execute("INSERT INTO %s (k1, k2, c) VALUES (1, [1, 0], 2) USING TTL 1");
        Thread.sleep(2500);

        for (String stage : new String[]{ "memtable", "sstable" })
        {
            if (stage.equals("sstable"))
                flush();
            for (String plan : PLANS)
            {
                for (String where : new String[]{ "", "WHERE k1 = 1 AND k2 = [1, 0] ", "WHERE c > 0 " })
                {
                    String query = "SELECT k1, c FROM %s " + where + "ORDER BY k2 ANN OF [1, 0] LIMIT 10" + plan;
                    check(stage, query, row(1, 1), row(1, 2), row(1, 3));
                }
            }
        }
    }

    @Test
    public void testClusteringVector() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c vector<float, 2>, s int static, r int, q int, l set<int>, PRIMARY KEY (k, c))");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, s) VALUES (1, 1)");
        for (int i = 1; i <= 3; i++)
            execute("INSERT INTO %s (k, c, r, q, l) VALUES (1, ?, 0, 1, {1})", vector(1f, i / 10f));
        // row [1, 0.4] was deleted, then written again (with r alone), in another sstable
        execute("DELETE FROM %s WHERE k = 1 AND c = [1, 0.4]");
        flush();
        execute("INSERT INTO %s (k, c, r) VALUES (1, [1, 0.4], 3)");
        flush();
        execute("INSERT INTO %s (k, c, r) VALUES (1, [1, 0.1], 2) USING TTL 1");
        execute("INSERT INTO %s (k, c) VALUES (1, [1, 0.2]) USING TTL 1");
        execute("INSERT INTO %s (k, c) VALUES (1, [1, 0.4]) USING TTL 1");
        Thread.sleep(2500);

        for (String stage : new String[]{ "sstable and memtable", "two sstables", "compacted" })
        {
            if (stage.equals("two sstables"))
                flush();
            else if (stage.equals("compacted"))
                compact();
            for (String plan : PLANS)
            {
                for (String where : new String[]{ "", "WHERE k = 1 ", "WHERE s = 1 " })
                {
                    String query = "SELECT k, c FROM %s " + where + "ORDER BY c ANN OF [1, 0] LIMIT 10" + plan;
                    check(stage, query, row(1, vector(1f, 0.1f)), row(1, vector(1f, 0.2f)), row(1, vector(1f, 0.3f)), row(1, vector(1f, 0.4f)));
                }
            }
        }
    }

    /**
     * Under a per-partition limit, the first row of a partition, which only its older cells keep alive, is the one
     * that comes back, on every version: it is returned with those cells, so it isn't dropped downstream as dead, and
     * its neighbour doesn't take its place.
     */
    @Test
    public void testPerPartitionLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, s int static, r int, q int, l set<int>, PRIMARY KEY ((k1, k2), c))");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k1, k2, s) VALUES (1, [1, 0], 1)");
        execute("INSERT INTO %s (k1, k2, c, r, q, l) VALUES (1, [1, 0], 1, 0, 1, {2})");
        execute("INSERT INTO %s (k1, k2, c, r, q, l) VALUES (1, [1, 0], 2, 0, 1, {2})");
        execute("INSERT INTO %s (k1, k2, c, r, q, l) VALUES (2, [1, 0.2], 1, 0, 1, {2})");
        // row 1 of partition 1: its marker and l written again with a TTL; once they expire, q keeps it alive, and it
        // satisfies l NOT CONTAINS 1
        execute("INSERT INTO %s (k1, k2, c, l) VALUES (1, [1, 0], 1, {3}) USING TTL 1");
        Thread.sleep(2500);

        for (String stage : new String[]{ "memtable", "sstable" })
        {
            if (stage.equals("sstable"))
                flush();
            for (String plan : PLANS)
            {
                String query = "SELECT k1, c FROM %s WHERE l NOT CONTAINS 1 ORDER BY k2 ANN OF [1, 0] PER PARTITION LIMIT 1 LIMIT 10" + plan;
                check(stage, query, row(1, 1), row(2, 1));
            }
        }
    }

    /**
     * A row alive only by an element of a collection that the query reads in part (an element or a slice of a map, an
     * element of a set; CQL doesn't select list elements): the row must still be returned.
     */
    @Test
    public void testCollectionSelectedInPart() throws Throwable
    {
        // the collections of the table, and the selections that read them in part: with a single collection, nothing
        // else that the query doesn't read can make the row look dropped
        String[][] cases = { { "m map<int, int>", "m[1]", "m[1..1]" },
                             { "l set<int>", "l[5]" },
                             { "m map<int, int>, l set<int>", "m[1], l[5]" } };
        for (String[] c : cases)
        {
            String columns = c[0];
            boolean map = columns.contains("m map"), set = columns.contains("l set");
            createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, r int, " + columns + ", PRIMARY KEY ((k1, k2), c))");
            disableCompaction();
            createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
            // row 1, alive only by m[2] (or by the element 6 of l): its marker and r are written later, with a TTL,
            // and expire; with both collections, row 2 is the one alive by l
            if (map)
                execute("UPDATE %s USING TIMESTAMP 10 SET m[2] = 20 WHERE k1 = 1 AND k2 = [1, 0] AND c = 1");
            else
                execute("UPDATE %s USING TIMESTAMP 10 SET l = l + {6} WHERE k1 = 1 AND k2 = [1, 0] AND c = 1");
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 1, 0) USING TIMESTAMP 20 AND TTL 1");
            if (map && set)
                execute("UPDATE %s USING TIMESTAMP 10 SET l = l + {6} WHERE k1 = 1 AND k2 = [1, 0] AND c = 2");
            else
                execute("UPDATE %s USING TIMESTAMP 10 SET r = 1 WHERE k1 = 1 AND k2 = [1, 0] AND c = 2");
            execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 2, 0) USING TIMESTAMP 20 AND TTL 1");
            // row 3, live, with the selected elements
            String inserted = "k1, k2, c, r" + (map ? ", m" : "") + (set ? ", l" : "");
            String values = "1, [1, 0], 3, 0" + (map ? ", {1: 10}" : "") + (set ? ", {5}" : "");
            execute("INSERT INTO %s (" + inserted + ") VALUES (" + values + ") USING TIMESTAMP 40");
            Thread.sleep(2500);
            // with a single collection, row 2 is dead (its r was overwritten by the TTL'd insert)
            List<Integer> live = map && set ? List.of(1, 2, 3) : List.of(1, 3);

            for (String stage : new String[]{ "memtable", "sstable" })
            {
                if (stage.equals("sstable"))
                    flush();
                for (int i = 1; i < c.length; i++)
                {
                    for (String plan : PLANS)
                    {
                        String query = "SELECT k1, c, " + c[i] + " FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 100" + plan;
                        assertEquals(query + ", " + columns + ", " + stage + ", on " + version, live, clusterings(execute(query)));
                        // the first row of the partition, alive by its older element, is the one that comes back
                        query = "SELECT k1, c, " + c[i] + " FROM %s ORDER BY k2 ANN OF [1, 0] PER PARTITION LIMIT 1 LIMIT 100" + plan;
                        assertEquals(query + ", " + columns + ", " + stage + ", on " + version, List.of(1), clusterings(execute(query)));
                    }
                }
            }
        }
    }

    /**
     * The row returned has the cells of the columns, and of the collection paths, that the query doesn't read without
     * their values, so that only the query's selection goes to the coordinator.
     */
    @Test
    public void testRowReturnedWithoutUnqueriedValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 vector<float, 2>, c int, r int, q int, b blob, m map<int, int>, PRIMARY KEY ((k1, k2), c))");
        disableCompaction();
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        // row 1, alive only by q, b and m: its marker and r are written again with a TTL, and expire
        execute("INSERT INTO %s (k1, k2, c, r, q, b, m) VALUES (1, [1, 0], 1, 0, 1, 0x0102, {1: 10, 2: 20}) USING TIMESTAMP 10");
        execute("INSERT INTO %s (k1, k2, c, r) VALUES (1, [1, 0], 1, 2) USING TIMESTAMP 20 AND TTL 1");
        // row 2, live, for comparison
        execute("INSERT INTO %s (k1, k2, c, r, q, b, m) VALUES (1, [1, 0], 2, 0, 1, 0x0102, {1: 10, 2: 20}) USING TIMESTAMP 40");
        Thread.sleep(2500);

        for (String stage : new String[]{ "memtable", "sstable" })
        {
            if (stage.equals("sstable"))
                flush();
            for (String selection : new String[]{ "r", "m[1]" })
            {
                String query = "SELECT k1, c, " + selection + " FROM %s ORDER BY k2 ANN OF [1, 0] LIMIT 10";
                String message = query + ", " + stage + ", on " + version;
                ReadCommand command = parseReadCommand(query);
                ColumnFilter filter = command.columnFilter();
                StorageAttachedIndexSearcher searcher = ((StorageAttachedIndexQueryPlan) command.indexQueryPlan()).searcherFor(command);
                List<Integer> seen = new ArrayList<>();
                try (ReadExecutionController executionController = command.executionController();
                     UnfilteredPartitionIterator partitions = searcher.search(executionController))
                {
                    while (partitions.hasNext())
                    {
                        try (UnfilteredRowIterator partition = partitions.next())
                        {
                            while (partition.hasNext())
                            {
                                Unfiltered unfiltered = partition.next();
                                if (!unfiltered.isRow())
                                    continue;
                                Row row = (Row) unfiltered;
                                seen.add(Int32Type.instance.compose(row.clustering().bufferAt(0)));
                                for (ColumnData data : row)
                                {
                                    ColumnMetadata column = data.column();
                                    if (column.isSynthetic())
                                        continue;
                                    Iterable<Cell<?>> cells = column.isSimple() ? List.of((Cell<?>) data) : (ComplexColumnData) data;
                                    for (Cell<?> cell : cells)
                                    {
                                        boolean queried = column.isSimple() ? filter.fetchedColumnIsQueried(column)
                                                                            : filter.fetchedCellIsQueried(column, cell.path());
                                        if (!queried)
                                            assertEquals(message + ": the value of " + column.name + " in row " + seen.get(seen.size() - 1),
                                                         0, cell.valueSize());
                                    }
                                }
                            }
                        }
                    }
                }
                Collections.sort(seen);
                assertEquals(message + ": rows", List.of(1, 2), seen);
            }
        }
    }

    private static List<Integer> clusterings(UntypedResultSet result)
    {
        List<Integer> clusterings = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            clusterings.add(row.getInt("c"));
        Collections.sort(clusterings);
        return clusterings;
    }

    private void check(String stage, String query, Object[]... rows) throws Throwable
    {
        assertRowsIgnoringOrder(query + ", " + stage + ", on " + version, execute(query), rows);
    }
}
