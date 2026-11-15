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

package org.apache.cassandra.db.rows;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_CURSOR_COMPACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Reads and compactions of rows that hold cells of a dropped complex column newer than the drop, as for a write that
 * reached a replica before the drop did, or that has a client-supplied timestamp. Such cells are kept, and
 * {@link Row.Merger} merges them like any other, including when none of the columns it was created for is complex.
 * <p>
 * Each test runs with cursor and iterator compaction.
 */
@RunWith(Parameterized.class)
public class RowMergerDroppedComplexColumnTest extends CQLTester
{
    @Parameterized.Parameter
    public boolean useCursors;

    private WithProperties properties;

    @Parameterized.Parameters(name = "cursors={0}")
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(new Object[][]{ { true }, { false } });
    }

    @Before
    public void setCompactionPath()
    {
        properties = new WithProperties().set(ALLOW_CURSOR_COMPACTION, useCursors);
    }

    @After
    public void resetCompactionPath()
    {
        properties.close();
    }

    /**
     * The only complex column is dropped. One sstable has a cell of it newer than the drop, and another sstable has
     * the same row.
     */
    @Test
    public void testDroppedColumnInOneOfTwoSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c set<int>, PRIMARY KEY (pk, ck))");
        disableCompaction();
        long writeTimestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, ck, c) VALUES (1, 1, {7, 8}) USING TIMESTAMP " + writeTimestamp);
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 2)");
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (writeTimestamp - 1));

        assertReads(row(1, 1, 2));
        compactAll();
        assertReads(row(1, 1, 2));

        execute("ALTER TABLE %s ADD c set<int>");
        assertRows(execute("SELECT pk, ck, v, c FROM %s"), row(1, 1, 2, set(7, 8)));
    }

    /**
     * As above, with the other version of the row in the memtable: a single sstable is enough.
     */
    @Test
    public void testDroppedColumnInSSTableAndRowInMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c set<int>, PRIMARY KEY (pk, ck))");
        disableCompaction();
        long writeTimestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, ck, c) VALUES (1, 1, {7, 8}) USING TIMESTAMP " + writeTimestamp);
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (writeTimestamp - 1));
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 2)");

        assertReads(row(1, 1, 2));
        flush();
        compactAll();
        assertReads(row(1, 1, 2));

        execute("ALTER TABLE %s ADD c set<int>");
        assertRows(execute("SELECT pk, ck, v, c FROM %s"), row(1, 1, 2, set(7, 8)));
    }

    /**
     * The only static complex column is dropped, while a regular complex column stays: the static row is merged on its
     * own, sized for the static columns.
     */
    @Test
    public void testDroppedStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, s set<int>, sv int static, sc set<int> static, " +
                    "PRIMARY KEY (pk, ck))");
        disableCompaction();
        long writeTimestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, sc) VALUES (1, {7, 8}) USING TIMESTAMP " + writeTimestamp);
        flush();
        execute("INSERT INTO %s (pk, ck, v, s, sv) VALUES (1, 1, 2, {3}, 4)");
        flush();
        execute("ALTER TABLE %s DROP sc USING TIMESTAMP " + (writeTimestamp - 1));

        Object[] expected = row(1, 1, 4, 2);
        assertRows(execute("SELECT pk, ck, sv, v FROM %s"), expected);
        assertRows(execute("SELECT pk, ck, sv, v FROM %s WHERE pk = 1"), expected);
        assertRows(execute("SELECT pk, ck, sv, v FROM %s WHERE pk = 1 AND ck = 1"), expected);
        compactAll();
        assertRows(execute("SELECT pk, ck, sv, v FROM %s"), expected);
        assertRows(execute("SELECT pk, ck, sv, v FROM %s WHERE pk = 1"), expected);

        execute("ALTER TABLE %s ADD sc set<int> static");
        assertRows(execute("SELECT pk, ck, sv, sc, v, s FROM %s"), row(1, 1, 4, set(7, 8), 2, set(3)));
    }

    /**
     * Checks full scans, partition reads and a single-row read of the table, with columns {@code pk, ck, v}.
     */
    private void assertReads(Object[] expected) throws Throwable
    {
        assertRows(execute("SELECT * FROM %s"), expected);
        assertRows(execute("SELECT * FROM %s WHERE pk = 1"), expected);
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 1"), expected);
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 1 AND ck = 1"), expected);
    }

    /**
     * Compacts all the sstables, of which there must be several, into one, and checks that compaction took the path
     * that the test is for.
     */
    private void compactAll() throws Throwable
    {
        assertTrue(getCurrentColumnFamilyStore().getLiveSSTables().size() > 1);
        withLogAppender(CompactionTask.class, appender -> {
            compact();
            String path = useCursors ? " by cursors" : " by iterators";
            assertTrue("Compaction did not run" + path,
                       appender.list.stream().anyMatch(event -> event.getFormattedMessage().contains(path)));
        });
        assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }
}
