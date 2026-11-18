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

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.inject.Injection;

import static org.junit.Assert.assertEquals;

/**
 * A frozen value whose canonical index term is over the maximum term size, while the value as written isn't, is
 * indexed as written when it reaches the index without the checks of a client write, and found by the values written
 * the same way. Set elements and map keys are cell paths, which the trie memtable stores in canonical form, so they are
 * only tested with a skiplist memtable.
 */
public class CanonicalTermOverMaxSizeTest extends SAITester.Versioned
{
    // 850 times 2.5: 7654 bytes as written, 8504 bytes in canonical form; 700 times: 6304 and 7004 bytes
    private static final String OVER = list(850, "2.5");
    private static final String UNDER = list(700, "2.5");
    // 300 times (2.5) for a tuple of a decimal and 9 ints: 3904 bytes as written, 15004 bytes in canonical form
    private static final String PADDED = list(300, "(2.5)");

    // the default (trie) memtable and a skiplist one
    private static final String[] BOTH = { "", " WITH memtable = 'skiplist'" };
    private static final String[] SKIPLIST = { " WITH memtable = 'skiplist'" };

    @Before
    public void setThresholds()
    {
        Guardrails.instance.setSaiFrozenTermSizeThreshold("1KiB", "8KiB");
    }

    @Test
    public void testFrozenListColumn() throws Throwable
    {
        checkIndexedAsWritten(BOTH, "frozen<list<decimal>>", "FULL(%s)", "%s", "%s = %s", OVER, UNDER);
    }

    @Test
    public void testTupleHoldingList() throws Throwable
    {
        checkIndexedAsWritten(BOTH, "frozen<tuple<frozen<list<decimal>>, int>>", "%s", "(%s, 1)", "%s = (%s, 1)",
                              OVER, UNDER);
    }

    @Test
    public void testMapValue() throws Throwable
    {
        checkIndexedAsWritten(BOTH, "map<int, frozen<list<decimal>>>", "VALUES(%s)", "{1: %s}", "%s CONTAINS %s",
                              OVER, UNDER);
    }

    @Test
    public void testSetElement() throws Throwable
    {
        checkIndexedAsWritten(SKIPLIST, "set<frozen<list<decimal>>>", "%s", "{%s}", "%s CONTAINS %s", OVER, UNDER);
    }

    @Test
    public void testMapKey() throws Throwable
    {
        checkIndexedAsWritten(SKIPLIST, "map<frozen<list<decimal>>, int>", "KEYS(%s)", "{%s: 1}", "%s CONTAINS KEY %s",
                              OVER, UNDER);
    }

    @Test
    public void testMapEntry() throws Throwable
    {
        checkIndexedAsWritten(SKIPLIST, "map<frozen<list<decimal>>, int>", "ENTRIES(%s)", "{%s: 1}", "%s[%s] = 1",
                              OVER, UNDER);
    }

    @Test
    public void testPaddedTuples() throws Throwable
    {
        checkIndexedAsWritten(BOTH, "frozen<list<frozen<tuple<decimal, int, int, int, int, int, int, int, int, int>>>>",
                              "FULL(%s)", "%s", "%s = %s", PADDED, list(10, "(2.5)"));
    }

    /**
     * @param memtables the table options selecting the memtables to test with
     * @param type the type of the indexed column
     * @param target the index target, with %s for the column
     * @param value a value of the column holding the list %s
     * @param restriction a restriction finding the value, with %s for the column and the list
     * @param over a list whose canonical form only is over the maximum term size
     * @param under a list under it in both forms
     */
    private void checkIndexedAsWritten(String[] memtables, String type, String target, String value, String restriction,
                                       String over, String under) throws Throwable
    {
        for (String memtable : memtables)
            checkIndexedAsWritten(memtable, type, target, value, restriction, over, under);
    }

    private void checkIndexedAsWritten(String memtable, String type, String target, String value, String restriction,
                                       String over, String under) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c " + type + ')' + memtable);
        createIndex("CREATE CUSTOM INDEX ON %s(" + String.format(target, "c") + ") USING 'StorageAttachedIndex'");
        disableCompaction();

        // an SSTable written by an earlier version, which indexed the values as written
        Injection old = injectTermsAsWritten("old_" + currentTable());
        execute("INSERT INTO %s (k, c) VALUES (0, " + String.format(value, over) + ')');
        flush();
        old.disable();

        // internal writes, which the term size guardrails don't apply to
        execute("INSERT INTO %s (k, c) VALUES (1, " + String.format(value, over) + ')');
        execute("INSERT INTO %s (k, c) VALUES (2, " + String.format(value, under) + ')');

        String overQuery = "SELECT k FROM %s WHERE " + String.format(restriction, "c", over);
        String underQuery = "SELECT k FROM %s WHERE " + String.format(restriction, "c", under);
        // the results of every phase, compared at the end to show all the differences
        List<String> expected = new ArrayList<>();
        List<String> actual = new ArrayList<>();
        for (String phase : new String[]{ "memtable", "flushed", "compacted", "rebuilt" })
        {
            switch (phase)
            {
                case "flushed":
                    flush();
                    break;
                case "compacted":
                    compact();
                    assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
                    break;
                case "rebuilt":
                    rebuildTableIndexes();
                    break;
            }
            expected.add(phase + ": " + List.of(0, 1) + ' ' + List.of(2));
            actual.add(phase + ": " + keys("k", overQuery) + ' ' + keys("k", underQuery));
        }
        assertEquals(memtable, expected, actual);
        assertEquals(3, execute("SELECT k FROM %s").size());
    }

    private static String list(int size, String element)
    {
        return '[' + String.join(", ", Collections.nCopies(size, element)) + ']';
    }
}
