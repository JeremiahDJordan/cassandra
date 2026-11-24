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
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

/**
 * A range combined with {@code !=} or {@code NOT IN} on a literal column that SAI filters, with or without an index on
 * the column, selects the rows that filtering without an index selects, from the coordinator and from the replica, with
 * a limit and in pages, in memtables and sstables.
 */
public class LiteralRangeNotEqualTest extends SAITester.Versioned
{
    private static final String[] RESTRICTIONS = { "n >= 0 AND t >= 't2' AND t <= 't5' AND t != 't3'",
                                                   "n >= 0 AND t > 't1' AND t < 't6' AND t NOT IN ('t3', 't4')",
                                                   "n >= 0 AND t NOT IN ('t3', 't4')",
                                                   "t >= 't2' AND t <= 't5' AND t != 't3'",
                                                   "n >= 0 AND u >= 'u2' AND u <= 'u5' AND u != 'u3'",
                                                   "n >= 0 AND u NOT IN ('u3', 'u4')",
                                                   "n >= 0 AND a >= 'a2' AND a <= 'a5' AND a != 'a3'",
                                                   "n >= 0 AND ci >= 'C2' AND ci <= 'C5' AND ci != 'C3'",
                                                   "n >= 0 AND ci >= 'C2' AND ci <= 'C5' AND ci != 'c3'",
                                                   "n >= 0 AND ci NOT IN ('C3', 'C4')",
                                                   "n >= 0 AND fd >= [2.0] AND fd <= [5.0] AND fd != [3.00]",
                                                   "n >= 0 AND b >= 0x02 AND b <= 0x05 AND b != 0x03" };

    @BeforeClass
    public static void setUpNetwork() throws Throwable
    {
        requireNetwork();
    }

    @Test
    public void testRangeWithExclusions() throws Throwable
    {
        String schema = "CREATE TABLE %s (k int PRIMARY KEY, n int, t text, u text, a ascii, ci text, fd frozen<list<decimal>>, b blob)";
        String unindexed = createTable(schema);
        String indexed = createTable(schema);
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex'");
        // != and NOT IN compare raw values with equals_behaviour_when_analyzed: unsupported, as without the index
        createIndex("CREATE CUSTOM INDEX ON %s(ci) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'case_sensitive': false, 'equals_behaviour_when_analyzed': 'unsupported' }");

        for (int k = 0; k < 8; k++)
        {
            for (String table : new String[]{ unindexed, indexed })
                execute("INSERT INTO " + KEYSPACE + '.' + table + " (k, n, t, u, a, ci, fd, b) VALUES (?, ?, ?, ?, ?, ?, [" + k + ".0], 0x0" + k + ')',
                        k, k, "t" + k, "u" + k, "a" + k, "C" + k);
        }

        beforeAndAfterFlush(() -> {
            for (String restrictions : RESTRICTIONS)
            {
                List<Integer> expected = keys(unindexed, restrictions, false);
                List<Integer> limited = keys(unindexed, restrictions + " LIMIT 2", false);
                assertEquals(restrictions, expected, keys(indexed, restrictions, false));
                assertEquals(restrictions, expected, keys(indexed, restrictions, true));
                assertEquals(restrictions, limited, keys(indexed, restrictions + " LIMIT 2", false));
                assertEquals(restrictions, limited, keys(indexed, restrictions + " LIMIT 2", true));
                List<Integer> paged = new ArrayList<>();
                executeNetWithPaging("SELECT k FROM " + KEYSPACE + '.' + indexed + " WHERE " + restrictions + " ALLOW FILTERING", 2)
                    .forEach(row -> paged.add(row.getInt("k")));
                Collections.sort(paged);
                assertEquals(restrictions, expected, paged);
            }
        });
    }

    private List<Integer> keys(String table, String restrictions, boolean internal)
    {
        String query = "SELECT k FROM " + KEYSPACE + '.' + table + " WHERE " + restrictions + " ALLOW FILTERING";
        List<Integer> keys = new ArrayList<>();
        for (UntypedResultSet.Row row : internal ? executeInternal(query) : execute(query))
            keys.add(row.getInt("k"));
        Collections.sort(keys);
        return keys;
    }
}
