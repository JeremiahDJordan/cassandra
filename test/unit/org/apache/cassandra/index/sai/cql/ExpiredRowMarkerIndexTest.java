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

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests SAI queries, filtered and ordered (generic and ANN ordering), on a row whose row marker (primary key liveness
 * info) is newer than its other cells: the row is returned whether the marker is live or has expired, and with a live
 * marker, a replica doesn't return the older cells that the query doesn't select.
 */
public class ExpiredRowMarkerIndexTest extends SAITester
{
    private static final String TABLE = "CREATE TABLE %s (k int, c int, a int, r int, x int, l set<int>, v vector<float, 2>, PRIMARY KEY (k, c))";
    private static final String OLDER_WRITE = "INSERT INTO %s (k, c, a, r, l, v) VALUES (1, 1, 5, 7, {7}, [1.0, 2.0]) USING TIMESTAMP 1";

    private static final String[] QUERIES = { "SELECT k, c, x FROM %s WHERE a = 5",
                                              "SELECT k, c, x FROM %s ORDER BY v ANN OF [1.0, 2.0] LIMIT 3",
                                              "SELECT k, c, x FROM %s WHERE a = 5 ORDER BY v ANN OF [1.0, 2.0] LIMIT 3",
                                              "SELECT k, c, x FROM %s ORDER BY a LIMIT 3" };

    @Test
    public void testRowAliveByOlderCellsUnderExpiredMarker() throws Throwable
    {
        Map<String, String> tables = new LinkedHashMap<>();
        for (boolean flush : new boolean[]{ false, true })
            tables.put(flush ? "sstable" : "memtable", createIndexedTable("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 10 AND TTL 1", flush));

        Thread.sleep(2500);

        for (Map.Entry<String, String> table : tables.entrySet())
        {
            for (String query : QUERIES)
            {
                String formattedQuery = String.format(query, KEYSPACE + '.' + table.getValue());
                assertRow(table.getKey() + ": " + formattedQuery, formattedQuery);
            }
        }
    }

    @Test
    public void testOlderNotQueriedCellsSkippedWithLiveMarker() throws Throwable
    {
        for (boolean flush : new boolean[]{ false, true })
        {
            String table = createIndexedTable("INSERT INTO %s (k, c) VALUES (1, 1) USING TIMESTAMP 10", flush);
            TableMetadata metadata = currentTableMetadata();
            ColumnMetadata r = metadata.getColumn(ColumnIdentifier.getInterned("r", false));
            ColumnMetadata v = metadata.getColumn(ColumnIdentifier.getInterned("v", false));
            for (String query : QUERIES)
            {
                String formattedQuery = String.format(query, KEYSPACE + '.' + table);
                String message = (flush ? "sstable: " : "memtable: ") + formattedQuery;
                assertRow(message, formattedQuery);

                int rows = 0;
                for (Partition partition : Util.getAllUnfiltered(parseReadCommand(formattedQuery)))
                {
                    for (Row row : partition.rows())
                    {
                        // r is older than the row marker and not queried, unlike the ordering column v
                        assertNull(message, row.getCell(r));
                        if (query.contains(" v ANN "))
                            assertNotNull(message, row.getCell(v));
                        rows++;
                    }
                }
                assertEquals(message, 1, rows);
            }
        }
    }

    private void assertRow(String message, String query) throws Throwable
    {
        try
        {
            assertRows(execute(query), row(1, 1, null));
        }
        catch (AssertionError e)
        {
            throw new AssertionError(message + ": " + e.getMessage(), e);
        }
    }

    private String createIndexedTable(String markerWrite, boolean flush) throws Throwable
    {
        String table = createTable(TABLE);
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        disableCompaction();
        execute(OLDER_WRITE);
        execute(markerWrite);
        if (flush)
            flush();
        return table;
    }
}
