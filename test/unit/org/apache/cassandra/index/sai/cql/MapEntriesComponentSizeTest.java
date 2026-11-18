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

import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.schema.TableMetadata;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * An ENTRIES index holds each entry as a CompositeType value of its key and value, so a key holds at most 65534 bytes
 * and a value 65535: a client write of a larger one is rejected before it is applied, and such an entry that reaches
 * the index anyway is left out of it.
 */
public class MapEntriesComponentSizeTest extends SAITester
{
    // the largest key and value an entry term can hold: a key of 65535 bytes would start the CompositeType value with
    // 0xFFFF, its static marker
    private static final String MAX_KEY = "k".repeat(65534);
    private static final String MAX_VALUE = "k".repeat(65535);
    private static final String OVER = "k".repeat(65536);

    @Test
    public void testOversizedEntryRejected() throws Throwable
    {
        for (String staticKeyword : new String[]{ "", " static" })
        {
            createTable("CREATE TABLE %s (pk int, ck int, m map<text, text>" + staticKeyword + ", v int, " +
                        "PRIMARY KEY (pk, ck))");
            createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(m)) USING 'StorageAttachedIndex'");
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, 0, 0)");

            String where = staticKeyword.isEmpty() ? "WHERE pk = 0 AND ck = 0" : "WHERE pk = 0";
            String keyOver = "its key is 65536 bytes, more than the 65534 bytes an entry key can hold";
            String key65535 = "its key is 65535 bytes, more than the 65534 bytes an entry key can hold";
            String valueOver = "its value is 65536 bytes, more than the 65535 bytes an entry value can hold";
            String[][] updates = { { "m = {'" + OVER + "': 'v'}", keyOver },
                                   { "m = {'" + MAX_VALUE + "': 'v'}", key65535 },
                                   { "m = {'k': '" + OVER + "'}", valueOver },
                                   { "m = {'" + OVER + "': '" + OVER + "'}", keyOver + ", and " + valueOver },
                                   { "m['" + OVER + "'] = 'v'", keyOver },
                                   { "m = m + {'a': 'b', '" + OVER + "': 'v'}", keyOver } };
            for (String[] update : updates)
            {
                String statement = "UPDATE %s SET " + update[0] + ' ' + where;
                String message = "too large for the ENTRIES index";
                assertThatThrownBy(() -> execute(statement)).as(statement)
                                                            .isInstanceOf(InvalidRequestException.class)
                                                            .hasMessageContaining(message)
                                                            .hasMessageEndingWith(update[1] + '.');
                assertThatThrownBy(() -> executeNet(statement)).as(statement)
                                                               .hasMessageContaining(message)
                                                               .hasMessageEndingWith(update[1] + '.');
                // nothing was applied
                assertFalse(statement, execute("SELECT m FROM %s WHERE pk = 0").one().has("m"));
            }

            // the largest key and value are written (an index with the default max term size doesn't index them),
            // and the other entries of the map are indexed
            execute("UPDATE %s SET m = {'a': 'b', '" + MAX_KEY + "': 'v'} " + where);
            execute("UPDATE %s SET m = m + {'c': '" + MAX_VALUE + "'} " + where);
            UntypedResultSet.Row row = execute("SELECT m FROM %s WHERE pk = 0").one();
            assertEquals(3, row.getMap("m", UTF8Type.instance, UTF8Type.instance).size());
            beforeAndAfterFlush(() -> assertKeys("SELECT pk FROM %s WHERE m['a'] = 'b'", 0));
        }
    }

    /**
     * Rows written before the validation, or without it, reach the index build, the memtable index and compaction.
     */
    @Test
    public void testOversizedEntryWithoutValidationIsNotIndexed() throws Throwable
    {
        for (String staticKeyword : new String[]{ "", " static" })
        {
            createTable("CREATE TABLE %s (pk int, ck int, m map<text, text>" + staticKeyword + ", v int, " +
                        "PRIMARY KEY (pk, ck))");
            disableCompaction();

            // written before the index exists: the index build reads it from the sstable
            execute("INSERT INTO %s (pk, ck, v, m) " +
                    "VALUES (0, 0, 0, {'a': 'b', '" + OVER + "': 'v', 'k': '" + OVER + "'})");
            execute("INSERT INTO %s (pk, ck, v, m) VALUES (1, 0, 1, {'a': 'b'})");
            flush();
            createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(m)) USING 'StorageAttachedIndex'");
            assertKeys("SELECT pk FROM %s WHERE m['a'] = 'b'", 0, 1);

            // applied without the validation of a client write: the memtable index
            applyDirectly(2, Map.of("a", "b", OVER, "v", MAX_VALUE, "w"));
            applyDirectly(3, Map.of("k", OVER, "c", "d"));
            assertKeys("SELECT pk FROM %s WHERE m['a'] = 'b'", 0, 1, 2);
            assertKeys("SELECT pk FROM %s WHERE m['c'] = 'd'", 3);

            flush();
            assertKeys("SELECT pk FROM %s WHERE m['a'] = 'b'", 0, 1, 2);
            compact();
            assertKeys("SELECT pk FROM %s WHERE m['a'] = 'b'", 0, 1, 2);
            assertKeys("SELECT pk FROM %s WHERE m['c'] = 'd'", 3);

            // the rows are still all there, with their oversized entries
            assertEquals(4, execute("SELECT pk FROM %s").size());
        }
    }

    private void applyDirectly(int pk, Map<String, String> map)
    {
        TableMetadata metadata = getCurrentColumnFamilyStore().metadata();
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, pk);
        if (metadata.getColumn(ColumnIdentifier.getInterned("m", false)).isStatic())
        {
            builder.row().add("m", map);
            builder.row(0).add("v", pk);
        }
        else
        {
            builder.row(0).add("m", map).add("v", pk);
        }
        builder.buildAsMutation().apply();
    }
}
