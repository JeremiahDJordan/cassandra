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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

/**
 * A varint range is also searched with its bounds as written, for the indexes written before varints were indexed
 * without their redundant sign bytes (see Expression#withUncanonicalizedBounds). Bounds in the right order can be in
 * the wrong order as written ({@code vi > 0x000001 AND vi <= 255}: as written, the lower bound has 3 bytes and the
 * upper one 2), and that search must not fail the query (the memtable index rejects such a range).
 */
public class VarintRangeBoundsAsWrittenTest extends SAITester.Versioned
{
    @Test
    public void testNonMinimalBounds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, vi varint, u varint)");
        createIndex("CREATE CUSTOM INDEX ON %s(vi) USING 'StorageAttachedIndex'");
        disableCompaction();
        int[] values = { -256, -1, 0, 1, 2, 127, 128, 255, 256, 65536 };
        for (int i = 0; i < values.length; i++)
            execute("INSERT INTO %s (k, vi, u) VALUES (?, ?, ?)", i, BigInteger.valueOf(values[i]),
                    BigInteger.valueOf(values[i]));

        String[] lowers = { "blobAsVarint(0x000001)", "blobAsVarint(0x0001)", "blobAsVarint(0xffff)", "1" };
        String[] uppers = { "255", "blobAsVarint(0x00ff)", "blobAsVarint(0x0000ff)", "2" };
        List<String> queries = new ArrayList<>();
        for (String lower : lowers)
        {
            for (String upper : uppers)
            {
                for (String lowerOp : new String[]{ ">", ">=" })
                {
                    for (String upperOp : new String[]{ "<", "<=" })
                        queries.add("%s " + lowerOp + ' ' + lower + " AND %s " + upperOp + ' ' + upper);
                }
            }
        }

        // the rows in the memtable, then in an SSTable
        for (String phase : new String[]{ "memtable", "flushed" })
        {
            if (phase.equals("flushed"))
                flush();
            for (String query : queries)
            {
                String indexed = String.format(query, "vi", "vi");
                String unindexed = String.format(query, "u", "u");
                assertEquals(phase + ": " + indexed,
                             keys("k", "SELECT k FROM %s WHERE " + unindexed + " ALLOW FILTERING"),
                             keys("k", "SELECT k FROM %s WHERE " + indexed));
            }
        }
    }
}
