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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

/**
 * Reads and compactions of rows that hold cells of a dropped complex column that are newer than the drop, when no
 * remaining column of the same kind is complex.
 */
public class DroppedComplexColumnMergeTest extends CQLTester
{
    @Test
    public void testDroppedColumnInOneOfTwoSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c set<int>, PRIMARY KEY (pk, ck))");
        disableCompaction();
        long timestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, ck, c) VALUES (1, 1, {7, 8}) USING TIMESTAMP " + timestamp);
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 2)");
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (timestamp - 1));

        assertReads();
        compactAll();
        assertReads();

        execute("ALTER TABLE %s ADD c set<int>");
        assertRows(execute("SELECT pk, ck, v, c FROM %s"), row(1, 1, 2, set(7, 8)));
    }

    @Test
    public void testDroppedColumnInSSTableAndRowInMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, c set<int>, PRIMARY KEY (pk, ck))");
        disableCompaction();
        long timestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, ck, c) VALUES (1, 1, {7, 8}) USING TIMESTAMP " + timestamp);
        flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (timestamp - 1));
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 2)");

        assertReads();
        flush();
        compactAll();
        assertReads();

        execute("ALTER TABLE %s ADD c set<int>");
        assertRows(execute("SELECT pk, ck, v, c FROM %s"), row(1, 1, 2, set(7, 8)));
    }

    @Test
    public void testDroppedStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, s set<int>, sv int static, sc set<int> static, " +
                    "PRIMARY KEY (pk, ck))");
        disableCompaction();
        long timestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, sc) VALUES (1, {7, 8}) USING TIMESTAMP " + timestamp);
        flush();
        execute("INSERT INTO %s (pk, ck, v, s, sv) VALUES (1, 1, 2, {3}, 4)");
        flush();
        execute("ALTER TABLE %s DROP sc USING TIMESTAMP " + (timestamp - 1));

        Object[] expected = row(1, 1, 4, 2);
        assertRows(execute("SELECT pk, ck, sv, v FROM %s"), expected);
        assertRows(execute("SELECT pk, ck, sv, v FROM %s WHERE pk = 1"), expected);
        compactAll();
        assertRows(execute("SELECT pk, ck, sv, v FROM %s"), expected);
        assertRows(execute("SELECT pk, ck, sv, v FROM %s WHERE pk = 1"), expected);

        execute("ALTER TABLE %s ADD sc set<int> static");
        assertRows(execute("SELECT pk, ck, sv, sc, v, s FROM %s"), row(1, 1, 4, set(7, 8), 2, set(3)));
    }

    private void assertReads() throws Throwable
    {
        assertRows(execute("SELECT * FROM %s"), row(1, 1, 2));
        assertRows(execute("SELECT * FROM %s WHERE pk = 1"), row(1, 1, 2));
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 1 AND ck = 1"), row(1, 1, 2));
    }

    private void compactAll() throws Throwable
    {
        compact();
        assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }
}
