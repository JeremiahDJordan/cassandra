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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * A column is dropped and re-added while clients write to it. A replica that has applied the drop but not yet the
 * re-add receives writes to the re-added column from a coordinator that has it. Those writes are newer than the drop,
 * so the replica has to keep them, whatever its memtable: they are acknowledged, and they are the replica's data for
 * the column once it applies the re-add.
 */
public class DroppedColumnReplicaBehindTest extends TestBaseImpl
{
    private static final String[] MEMTABLES = { "skiplist", "trie" };
    // name, type, static, value
    private static final Object[][] KINDS = {
        { "simple", "int", false, "3" },
        { "complex", "set<int>", false, "{3}" },
        { "static_simple", "int", true, "3" },
        { "static_complex", "set<int>", true, "{3}" },
    };

    private static String describe(Object[][] rows)
    {
        StringBuilder sb = new StringBuilder();
        for (Object[] row : rows)
            sb.append(Arrays.toString(row));
        return sb.length() == 0 ? "(no rows)" : sb.toString();
    }

    @Test
    public void replicaWithTheDropButNotTheReAdd() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(config -> config.with(NETWORK)
                                                                         .set("memtable", Map.of("configurations",
                                                                                                 Map.of("skiplist", Map.of("class_name", "SkipListMemtable"),
                                                                                                        "trie", Map.of("class_name", "TrieMemtable")))))
                                             .start()))
        {
            List<String> tables = new ArrayList<>();
            List<Object[]> kinds = new ArrayList<>();
            for (String memtable : MEMTABLES)
            {
                for (Object[] kind : KINDS)
                {
                    for (boolean flushBehind : BOOLEANS)
                    {
                        String table = KEYSPACE + ".t_" + memtable + '_' + kind[0] + (flushBehind ? "_flush" : "");
                        tables.add(table);
                        kinds.add(new Object[]{ kind[1], kind[2], kind[3], flushBehind, memtable });
                        cluster.schemaChange("CREATE TABLE " + table + " (pk int, ck int, v int, s int static, c " + kind[1] + ((boolean) kind[2] ? " static" : "") +
                                             ", PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "' AND read_repair = 'NONE'");
                        cluster.coordinator(1).execute("INSERT INTO " + table + " (pk, ck, v, c) VALUES (0, 0, 1, " + kind[3] + ')', ConsistencyLevel.ALL);
                    }
                }
            }
            // the drop is newer than the inserts, and the re-add newer than the drop
            Thread.sleep(5);
            for (String table : tables)
                cluster.schemaChange("ALTER TABLE " + table + " DROP c");
            Thread.sleep(5);

            // Node 3 stays behind: it does not get the re-add.
            cluster.filters().verbs(Verb.SCHEMA_PUSH_REQ.id, Verb.SCHEMA_PULL_RSP.id, Verb.SCHEMA_VERSION_RSP.id).to(3).drop();
            for (int t = 0; t < tables.size(); t++)
            {
                String reAdd = "ALTER TABLE " + tables.get(t) + " ADD c " + kinds.get(t)[0] + ((boolean) kinds.get(t)[1] ? " static" : "");
                for (int node = 1; node <= 2; node++)
                {
                    try
                    {
                        cluster.get(node).schemaChangeInternal(reAdd);
                    }
                    catch (Throwable alreadyThere)
                    {
                        // node 1 pushed it to node 2
                    }
                }
            }

            List<String> failures = new ArrayList<>();
            for (int t = 0; t < tables.size(); t++)
            {
                String table = tables.get(t);
                String name = table.substring(table.indexOf('.') + 1);
                String value = (String) kinds.get(t)[2];
                boolean isStatic = (boolean) kinds.get(t)[1];
                boolean flushBehind = (boolean) kinds.get(t)[3];

                for (int node = 1; node <= 3; node++)
                {
                    Object[][] columns = cluster.get(node).executeInternal("SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? AND column_name = 'c'", KEYSPACE, name);
                    if ((columns.length == 1) != (node < 3))
                        failures.add(name + ": node " + node + (node < 3 ? " does not have" : " has") + " the re-added column, the test does not test what it says");
                }

                // The write to the re-added column, from a coordinator that has the re-add.
                String write = "INSERT INTO " + table + " (pk, ck, v, c) VALUES (0, 1, 2, " + value + ')';
                try
                {
                    cluster.coordinator(1).execute(write, ConsistencyLevel.ALL);
                }
                catch (Throwable e)
                {
                    failures.add(name + ": the write to the re-added column failed at ALL: " + e);
                }
                if (flushBehind)
                    cluster.get(3).flush(KEYSPACE);

                cluster.get(3).schemaChangeInternal("ALTER TABLE " + table + " ADD c " + kinds.get(t)[0] + (isStatic ? " static" : ""));
                String expectedC = value.startsWith("{") ? "[3]" : "3";
                for (int node = 1; node <= 3; node++)
                {
                    Object[][] rows = cluster.get(node).executeInternal("SELECT pk, ck, v, c FROM " + table + " WHERE pk = 0 AND ck = 1");
                    String c = rows.length == 1 && rows[0][3] != null ? String.valueOf(rows[0][3]) : "null";
                    if (rows.length != 1 || !c.equals(expectedC))
                        failures.add(name + ": node " + node + " returns " + describe(rows) + ", expected c = " + expectedC);
                }
                if (!flushBehind)
                {
                    cluster.get(3).flush(KEYSPACE);
                    Object[][] rows = cluster.get(3).executeInternal("SELECT pk, ck, v, c FROM " + table + " WHERE pk = 0 AND ck = 1");
                    String c = rows.length == 1 && rows[0][3] != null ? String.valueOf(rows[0][3]) : "null";
                    if (rows.length != 1 || !c.equals(expectedC))
                        failures.add(name + ": node 3 returns " + describe(rows) + " after a flush, expected c = " + expectedC);
                }
                // The value from before the drop stays dropped.
                Object[][] old = cluster.get(3).executeInternal("SELECT pk, ck, v, c FROM " + table + " WHERE pk = 0 AND ck = 0");
                if (!isStatic && (old.length != 1 || old[0][3] != null))
                    failures.add(name + ": node 3 returns " + describe(old) + " for the row written before the drop, expected c = null");
            }

            Assert.assertTrue(String.join("\n", failures), failures.isEmpty());
        }
    }
}
