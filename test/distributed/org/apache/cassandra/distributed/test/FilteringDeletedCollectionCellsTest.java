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

package org.apache.cassandra.distributed.test;

import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.junit.Assert.assertEquals;

/**
 * Filtering on collections with deleted and expired elements and entries gives the same results at ONE, where the
 * replicas purge the rows before filtering them, and at QUORUM and ALL, where they don't because the read needs
 * reconciliation (see RowFilter#filter), for regular and static collections, as range reads and as single partition
 * reads.
 */
public class FilteringDeletedCollectionCellsTest extends TestBaseImpl
{
    private static final int PARTITIONS = 10;

    @Test
    public void testDeletedCollectionCellsAtAllConsistencyLevels() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(config -> config.set("hinted_handoff_enabled", false)).start()))
        {
            String columns = "s text%1$s, sl list<text>%1$s, ss set<text>%1$s, sm map<text, text>%1$s";
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.st (pk int, ck int, v int, " + String.format(columns, " static") +
                                              ", PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.rt (pk int, ck int, v int, " + String.format(columns, "") +
                                              ", PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'"));
            for (int pk = 0; pk < PARTITIONS; pk++)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.st (pk, ck, v) VALUES (?, 0, 1)"), ConsistencyLevel.ALL, pk);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.rt (pk, ck, v) VALUES (?, 0, 1)"), ConsistencyLevel.ALL, pk);
            }
            String[][] writes = { { "1", "UPDATE %s SET sl = ['a', 'b'], ss = {'a', 'b'} %s" },
                                  { "1", "UPDATE %s SET sl = sl - ['a'], ss = ss - {'a'} %s" }, // deleted elements
                                  { "2", "UPDATE %s SET sl = ['a'], ss = {'a'} %s" },
                                  { "2", "DELETE sl[0] FROM %s %s" }, // deleted element by index
                                  { "3", "UPDATE %s SET sm = {'k': 'v', 'o': 'x'} %s" },
                                  { "3", "DELETE sm['k'] FROM %s %s" }, // deleted entry
                                  { "4", "UPDATE %s SET sl = ['a'], ss = {'a'}, sm = {'o': 'x'} %s" },
                                  { "4", "UPDATE %s USING TTL 1 SET sl = sl + ['x'], ss = ss + {'x'}, sm['k'] = 'v' %s" }, // expired
                                  { "5", "UPDATE %s SET sl = ['a'], ss = {'a'}, sm = {'k': 'v'} %s" },
                                  { "5", "DELETE sl, ss, sm FROM %s %s" }, // deleted collections
                                  { "6", "UPDATE %s SET s = 'a', sl = [''], ss = {''}, sm = {'k': ''} %s" }, // live empty values
                                  { "7", "UPDATE %s SET s = 'a', sl = ['a', 'b'], ss = {'b'}, sm = {'k': 'v'} %s" },
                                  { "8", "UPDATE %s SET s = 'a' %s" },
                                  { "8", "DELETE s FROM %s %s" } }; // deleted simple cell
            for (String[] write : writes)
            {
                cluster.coordinator(1).execute(String.format(write[1], KEYSPACE + ".st", "WHERE pk = " + write[0]), ConsistencyLevel.ALL);
                cluster.coordinator(1).execute(String.format(write[1], KEYSPACE + ".rt", "WHERE pk = " + write[0] + " AND ck = 0"), ConsistencyLevel.ALL);
            }
            Thread.sleep(2000); // let the TTLs expire

            String[] restrictions = { "sl CONTAINS ''", "sl CONTAINS 'a'", "sl CONTAINS 'x'",
                                      "ss CONTAINS ''", "ss CONTAINS 'a'", "ss CONTAINS 'x'",
                                      "sm CONTAINS KEY 'k'", "sm CONTAINS KEY 'o'", "sm CONTAINS ''", "sm CONTAINS 'v'",
                                      "sm['k'] = ''", "sm['k'] = 'v'", "s = ''", "s = 'a'",
                                      "sl CONTAINS '' AND v = 1", "sm CONTAINS KEY 'k' AND v = 1",
                                      // negations
                                      "sl NOT CONTAINS ''", "sl NOT CONTAINS 'b'", "ss NOT CONTAINS ''", "ss NOT CONTAINS 'x'",
                                      "sm NOT CONTAINS KEY 'k'", "sm NOT CONTAINS ''", "sm['k'] != ''", "sm['k'] != 'v'",
                                      "sm['k'] < 'w'", "s != 'a'", "sl NOT CONTAINS '' AND sm NOT CONTAINS KEY 'k'",
                                      "sl NOT CONTAINS '' AND v = 1" };
            StringBuilder in = new StringBuilder("pk IN (0");
            for (int pk = 1; pk < PARTITIONS; pk++)
                in.append(", ").append(pk);
            in.append(") AND ");
            for (boolean flushed : BOOLEANS)
            {
                if (flushed)
                    cluster.forEach(i -> i.flush(KEYSPACE));
                for (String restriction : restrictions)
                {
                    // the regular table read at ONE, where the replica purges the rows before filtering, is the reference
                    Set<Integer> expected = select(cluster, "rt", restriction, ConsistencyLevel.ONE);
                    for (String table : new String[]{ "rt", "st" })
                    {
                        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL })
                        {
                            String context = String.format("%s, %s at %s: %s", flushed ? "flushed" : "memtable", table, cl, restriction);
                            assertEquals(context, expected, select(cluster, table, restriction, cl));
                            assertEquals(context + " (IN)", expected, select(cluster, table, in + restriction, cl));

                            Set<Integer> singlePartitionReads = new TreeSet<>();
                            for (int pk = 0; pk < PARTITIONS; pk++)
                                singlePartitionReads.addAll(select(cluster, table, "pk = " + pk + " AND " + restriction, cl));
                            assertEquals(context + " (pk =)", expected, singlePartitionReads);
                        }
                    }
                }
            }
        }
    }

    private static Set<Integer> select(Cluster cluster, String table, String restriction, ConsistencyLevel cl)
    {
        Set<Integer> keys = new TreeSet<>();
        String query = "SELECT pk FROM " + KEYSPACE + '.' + table + " WHERE " + restriction + " ALLOW FILTERING";
        for (Object[] row : cluster.coordinator(1).execute(query, cl))
            keys.add((Integer) row[0]);
        return keys;
    }
}
