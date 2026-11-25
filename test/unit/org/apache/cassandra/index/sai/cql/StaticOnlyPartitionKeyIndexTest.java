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
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Indexes on the components of a composite partition key must find partitions that have only a static row. Every
 * query on the indexed table is checked against the same query, with {@code ALLOW FILTERING}, on an identical table
 * without indexes, in the memtable, after flush, after compaction and after an index rebuild, with paging and with
 * {@code LIMIT}. (A single-column partition key can't be indexed.)
 */
public class StaticOnlyPartitionKeyIndexTest extends SAITester.Versioned
{
    private String indexed;
    private String reference;

    @Test
    public void testCompositePartitionKeyComponentIndex()
    {
        createTables("CREATE TABLE %s (pk1 int, pk2 int, ck int, st int static, v int, PRIMARY KEY ((pk1, pk2), ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(pk1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(pk2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        writeCompositeData();

        verify(Arrays.asList("pk1 = 3",
                             "pk1 = 4",
                             "pk1 > 2",
                             "pk1 >= 3 AND pk1 <= 3",
                             "pk2 = 1",
                             "pk2 > 2",
                             "pk1 = 3 AND pk2 = 1",
                             "pk1 = 3 AND pk2 > 1",
                             "pk1 = 3 AND v = 1",
                             "pk1 = 3 AND st = 5",
                             "pk1 = 3 AND st > 0",
                             "pk1 = 3 AND (v = 1 OR st = 5)"));
    }

    @Test
    public void testCompositePartitionKeyComponentAndStaticColumnIndex()
    {
        createTables("CREATE TABLE %s (pk1 int, pk2 int, ck int, st int static, v int, PRIMARY KEY ((pk1, pk2), ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(pk1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex'");

        writeCompositeData();

        verify(Arrays.asList("pk1 = 3",
                             "st = 5",
                             "st > 0",
                             "pk1 = 3 AND st = 5",
                             "pk1 = 3 AND st > 0"));
    }

    @Test
    public void testIndexBuiltOnExistingSSTables()
    {
        createTables("CREATE TABLE %s (pk1 int, pk2 int, ck int, st int static, v int, PRIMARY KEY ((pk1, pk2), ck))");

        writeCompositeData();
        flush(KEYSPACE, indexed);
        flush(KEYSPACE, reference);

        // the index is built from the sstables, not flushed from the memtable index
        createIndex("CREATE CUSTOM INDEX ON %s(pk1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        verify(Arrays.asList("pk1 = 3",
                             "pk1 > 2",
                             "pk1 = 3 AND v = 1",
                             "pk1 = 3 AND st = 5"));
    }

    @Test
    public void testStaticAndRegularRowsInDifferentSSTables()
    {
        createTables("CREATE TABLE %s (pk1 int, pk2 int, ck int, st int static, v int, PRIMARY KEY ((pk1, pk2), ck))");
        disableCompaction(KEYSPACE, indexed);
        disableCompaction(KEYSPACE, reference);
        createIndex("CREATE CUSTOM INDEX ON %s(pk1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        for (String table : Arrays.asList(indexed, reference))
        {
            write(table, "INSERT INTO %s (pk1, pk2, st) VALUES (3, 1, 5)");
            write(table, "INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 2, 1, 1)");
            write(table, "INSERT INTO %s (pk1, pk2, st) VALUES (3, 3, 6)");
            write(table, "INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 3, 1, 1)");
        }
        flush(KEYSPACE, indexed);
        flush(KEYSPACE, reference);
        compact(KEYSPACE, indexed);

        for (String table : Arrays.asList(indexed, reference))
        {
            // regular rows for a partition whose static row is in another sstable
            write(table, "INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 1, 1, 1)");
            write(table, "INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 1, 2, 2)");
            // a static row for a partition whose regular row is in another sstable
            write(table, "INSERT INTO %s (pk1, pk2, st) VALUES (3, 2, 7)");
            // deleting the regular row of a partition whose static row is in another sstable
            write(table, "DELETE FROM %s WHERE pk1 = 3 AND pk2 = 3 AND ck = 1");
            // a static row only
            write(table, "INSERT INTO %s (pk1, pk2, st) VALUES (3, 4, 8)");
        }

        verify(Arrays.asList("pk1 = 3",
                             "pk1 > 2",
                             "pk1 = 3 AND v = 1",
                             "pk1 = 3 AND st > 0",
                             "pk1 = 3 AND (v = 1 OR st = 6)"));
    }

    @Test
    public void testClusteringColumnIndex()
    {
        createTables("CREATE TABLE %s (pk1 int, pk2 int, ck int, st int static, v int, PRIMARY KEY ((pk1, pk2), ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");

        writeCompositeData();

        verify(Arrays.asList("ck = 1",
                             "ck > 0",
                             "ck = 1 AND st = 6",
                             "ck > 0 AND st > 0"));
    }

    private void writeCompositeData()
    {
        for (String table : Arrays.asList(indexed, reference))
        {
            // static row only
            write(table, "INSERT INTO %s (pk1, pk2, st) VALUES (3, 1, 5)");
            // static and regular rows
            write(table, "INSERT INTO %s (pk1, pk2, ck, st, v) VALUES (3, 2, 1, 6, 1)");
            write(table, "INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 2, 2, 2)");
            // regular rows only
            write(table, "INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 3, 1, 1)");
            write(table, "INSERT INTO %s (pk1, pk2, ck, v) VALUES (3, 3, 2, 2)");
            // deleted static row only
            write(table, "INSERT INTO %s (pk1, pk2, st) VALUES (3, 4, 7)");
            write(table, "DELETE st FROM %s WHERE pk1 = 3 AND pk2 = 4");
            // deleted static row, with a regular row
            write(table, "INSERT INTO %s (pk1, pk2, ck, st, v) VALUES (3, 5, 1, 8, 1)");
            write(table, "DELETE st FROM %s WHERE pk1 = 3 AND pk2 = 5");
            // static row only, with a deleted regular row
            write(table, "INSERT INTO %s (pk1, pk2, ck, st, v) VALUES (3, 6, 1, 9, 1)");
            write(table, "DELETE FROM %s WHERE pk1 = 3 AND pk2 = 6 AND ck = 1");
            // static row only, on another value of pk1
            write(table, "INSERT INTO %s (pk1, pk2, st) VALUES (4, 1, 5)");
            write(table, "INSERT INTO %s (pk1, pk2, ck, v) VALUES (4, 2, 1, 1)");
        }
    }

    private void createTables(String query)
    {
        reference = createTable(query);
        indexed = createTable(query);
    }

    private void write(String table, String query)
    {
        executeFormattedQuery(String.format(query, KEYSPACE + '.' + table));
    }

    private void verify(List<String> restrictions)
    {
        verifyAll(restrictions, "memtable");
        flush(KEYSPACE, indexed);
        flush(KEYSPACE, reference);
        verifyAll(restrictions, "after flush");
        compact(KEYSPACE, indexed);
        verifyAll(restrictions, "after compaction");
        rebuildTableIndexes(KEYSPACE, indexed);
        verifyAll(restrictions, "after rebuild");
    }

    private void verifyAll(List<String> restrictions, String stage)
    {
        for (String restriction : restrictions)
            verify(restriction, stage);
    }

    private void verify(String restriction, String stage)
    {
        String select = "SELECT * FROM %s.%s WHERE " + restriction;
        List<String> expected = rows(executeFormattedQuery(String.format(select + " ALLOW FILTERING", KEYSPACE, reference)));
        String message = String.format("[%s, %s] %s", version, stage, restriction);

        List<String> actual = rows(executeFormattedQuery(String.format(select + " ALLOW FILTERING", KEYSPACE, indexed)));
        assertEquals(message, sorted(expected), sorted(actual));

        // paging returns the same rows, in the same order, as the unpaged query
        for (int pageSize : new int[]{ 1, 2, 3 })
        {
            List<String> paged = new ArrayList<>();
            for (com.datastax.driver.core.Row row : executeNetWithPaging(String.format(select + " ALLOW FILTERING", KEYSPACE, indexed), pageSize))
            {
                List<String> values = new ArrayList<>();
                for (com.datastax.driver.core.ColumnDefinitions.Definition column : row.getColumnDefinitions())
                    values.add(column.getName() + '=' + (row.isNull(column.getName()) ? "null" : row.getObject(column.getName())));
                paged.add(String.join(", ", values));
            }
            assertEquals(message + " page size " + pageSize, actual, paged);
        }

        // limit
        for (int limit : new int[]{ 1, 2, 3 })
        {
            List<String> limited = rows(executeFormattedQuery(String.format(select + " LIMIT " + limit + " ALLOW FILTERING", KEYSPACE, indexed)));
            assertEquals(message + " limit " + limit, Math.min(limit, expected.size()), limited.size());
            assertTrue(message + " limit " + limit + ": " + limited + " not in " + expected, expected.containsAll(limited));
        }
    }

    private static List<String> rows(UntypedResultSet result)
    {
        List<String> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
        {
            List<String> values = new ArrayList<>();
            for (ColumnSpecification spec : row.getColumns())
                values.add(spec.name.toString() + "=" + (row.has(spec.name.toString()) ? spec.type.getString(row.getBytes(spec.name.toString())) : "null"));
            rows.add(String.join(", ", values));
        }
        return rows;
    }

    private static List<String> sorted(List<String> rows)
    {
        List<String> copy = new ArrayList<>(rows);
        copy.sort(String::compareTo);
        return copy;
    }
}
