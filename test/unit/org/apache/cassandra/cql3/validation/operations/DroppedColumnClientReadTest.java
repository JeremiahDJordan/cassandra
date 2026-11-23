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

package org.apache.cassandra.cql3.validation.operations;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Client reads of partitions that hold cells of a dropped column that are newer than the drop.
 */
public class DroppedColumnClientReadTest extends CQLTester
{
    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    @Test
    public void testDroppedSimpleColumn() throws Throwable
    {
        checkDroppedColumn("int", "5", 5, false);
        checkDroppedColumn("int", "5", 5, true);
    }

    @Test
    public void testDroppedComplexColumn() throws Throwable
    {
        checkDroppedColumn("set<int>", "{5, 6}", set(5, 6), false);
        checkDroppedColumn("set<int>", "{5, 6}", set(5, 6), true);
    }

    private void checkDroppedColumn(String type, String literal, Object value, boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, l list<int>, c " + type + ", PRIMARY KEY (pk, ck))");
        disableCompaction();
        long timestamp = FBUtilities.timestampMicros();
        execute("INSERT INTO %s (pk, ck, v, c) VALUES (1, 1, 1, " + literal + ") USING TIMESTAMP " + timestamp);
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 3)");
        if (flush)
            flush();
        execute("ALTER TABLE %s DROP c USING TIMESTAMP " + (timestamp - 1));

        assertRowsNet(executeNet("SELECT * FROM %s WHERE pk = 1"), row(1, 1, null, 1), row(1, 2, null, 2));
        assertRowsNet(executeNet("SELECT * FROM %s WHERE pk = 1 AND ck = 1"), row(1, 1, null, 1));
        assertRowsNet(executeNet("SELECT pk, ck, v FROM %s WHERE pk = 1"), row(1, 1, 1), row(1, 2, 2));
        assertRowsNet(executeNet("SELECT * FROM %s"), row(1, 1, null, 1), row(1, 2, null, 2), row(2, 1, null, 3));
        assertRowsNet(executeNet("SELECT * FROM %s WHERE pk = 1 AND ck >= 1"), row(1, 1, null, 1), row(1, 2, null, 2));

        execute("ALTER TABLE %s ADD c " + type);
        assertRowsNet(executeNet("SELECT pk, ck, v, c FROM %s WHERE pk = 1"), row(1, 1, 1, value), row(1, 2, 2, null));
    }

    @Test
    public void testDroppedStaticColumn() throws Throwable
    {
        for (boolean flush : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, s int static, sc int static, PRIMARY KEY (pk, ck))");
            disableCompaction();
            long timestamp = FBUtilities.timestampMicros();
            execute("INSERT INTO %s (pk, ck, v, s, sc) VALUES (1, 1, 1, 2, 3) USING TIMESTAMP " + timestamp);
            if (flush)
                flush();
            execute("ALTER TABLE %s DROP sc USING TIMESTAMP " + (timestamp - 1));

            assertRowsNet(executeNet("SELECT * FROM %s WHERE pk = 1"), row(1, 1, 2, 1));
            assertRowsNet(executeNet("SELECT * FROM %s"), row(1, 1, 2, 1));
        }
    }
}
