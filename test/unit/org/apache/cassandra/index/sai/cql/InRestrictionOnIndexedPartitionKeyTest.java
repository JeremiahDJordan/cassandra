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

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

/**
 * An IN restriction on a partition key column that is only part of a key range query's row filter (the partition key is
 * not fully restricted) can be served by an SAI index on that column.
 */
public class InRestrictionOnIndexedPartitionKeyTest extends SAITester
{
    @Test
    public void testInOnIndexedPartitionKeyComponent() throws Throwable
    {
        createTable("CREATE TABLE %s (id1 int, id2 int, author text, time bigint, v1 text, PRIMARY KEY ((id1, id2), author, time))");
        createIndex("CREATE CUSTOM INDEX ON %s(id2) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s(id1, id2, author, time, v1) VALUES(0, 0, 'bob', 0, 'A')");
        execute("INSERT INTO %s(id1, id2, author, time, v1) VALUES(0, 1, 'bob', 2, 'C')");
        execute("INSERT INTO %s(id1, id2, author, time, v1) VALUES(0, 2, 'tom', 0, 'D')");
        assertRowsIgnoringOrder(execute("SELECT v1 FROM %s WHERE id2 IN (0, 2)"), row("A"), row("D"));
        assertRowsIgnoringOrder(execute("SELECT v1 FROM %s WHERE id2 IN (1, 3)"), row("C"));
        assertRowsIgnoringOrder(execute("SELECT v1 FROM %s WHERE id2 IN (0, 1) AND author = 'bob' ALLOW FILTERING"), row("A"), row("C"));
    }
}
