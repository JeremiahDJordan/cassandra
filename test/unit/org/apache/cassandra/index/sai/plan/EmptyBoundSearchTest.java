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
package org.apache.cassandra.index.sai.plan;

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeTermIterator;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Searching a non-literal index with an expression that has an empty bound, without the query planner: the search
 * rewrites the expression itself (see {@link Expression#withoutUnindexedEmptyBounds()}).
 */
public class EmptyBoundSearchTest extends SAITester.Versioned
{
    @Test
    public void testSearchWithEmptyBound() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, v) VALUES (0, ?)", ByteBufferUtil.EMPTY_BYTE_BUFFER);
        execute("INSERT INTO %s (pk, v) VALUES (1, ?)", Integer.MIN_VALUE);
        execute("INSERT INTO %s (pk, v) VALUES (2, 2)");

        IndexContext context = getIndexContext(index);
        AbstractBounds<PartitionPosition> range = DataRange.allData(getCurrentColumnFamilyStore().getPartitioner()).keyRange();

        assertSearches(context, range);
        flush();
        assertSearches(context, range);
    }

    private void assertSearches(IndexContext context, AbstractBounds<PartitionPosition> range) throws Throwable
    {
        // the empty value sorts before every value, and isn't indexed
        assertEquals(0, count(context, range, Operator.EQ));
        assertEquals(0, count(context, range, Operator.LT));
        assertEquals(0, count(context, range, Operator.LTE));
        assertEquals(2, count(context, range, Operator.GT));
        assertEquals(2, count(context, range, Operator.GTE));
        // != returns the candidates for post-filtering (see IndexContext#getNonEqIterator)
        assertTrue(count(context, range, Operator.NEQ) >= 2);
        // the empty lower bound is dropped
        Expression emptyLowerBound = new Expression(context).add(Operator.GT, ByteBufferUtil.EMPTY_BYTE_BUFFER)
                                                            .add(Operator.LTE, Int32Type.instance.decompose(2));
        assertEquals(2, count(context, range, emptyLowerBound));
    }

    private long count(IndexContext context, AbstractBounds<PartitionPosition> range, Operator op) throws Throwable
    {
        return count(context, range, new Expression(context).add(op, ByteBufferUtil.EMPTY_BYTE_BUFFER));
    }

    private long count(IndexContext context, AbstractBounds<PartitionPosition> range, Expression expression) throws Throwable
    {
        try (QueryView view = new QueryView.Builder(context, range).build();
             KeyRangeIterator keys = KeyRangeTermIterator.build(expression, view, range, new QueryContext(), false))
        {
            long count = 0;
            while (keys.hasNext())
            {
                keys.next();
                count++;
            }
            return count;
        }
    }
}
