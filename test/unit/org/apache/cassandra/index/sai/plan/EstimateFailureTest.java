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

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.TrieMemtableIndex;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Checks that a row count estimate that fails, which the query planner uses to choose a plan, doesn't fail the query:
 * the planner then assumes that all the rows of the index match.
 */
public class EstimateFailureTest extends SAITester
{
    @Test
    public void testFailingMemtableEstimate() throws Throwable
    {
        // The failed assertions of the estimates, such as "Invalid order of range boundaries", are errors
        testFailingEstimate(true, TrieMemtableIndex.class, "estimateMatchingRowsCount",
                            "throw new java.lang.AssertionError(\"Injected failure!\", null);");
    }

    @Test
    public void testFailingSSTableEstimate() throws Throwable
    {
        testFailingEstimate(false, SegmentMetadata.class, "estimateNumRowsMatching",
                            "throw new java.lang.RuntimeException(\"Injected failure!\");");
    }

    /**
     * Makes the row count estimates of the memtable or SSTable index fail, and checks that the queries still succeed,
     * with the right rows, and that the planner then assumes that all the rows of the index match.
     */
    private void testFailingEstimate(boolean memtable, Class<?> estimator, String method, String failure) throws Throwable
    {
        expectEstimateFailures = true;
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        int rows = 100;
        for (int i = 0; i < rows; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, i);
        if (!memtable)
            flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        // what the planner expects for a restriction that all the rows match, at most the table's estimated row count
        double allRows = plannedKeys(cfs, Operator.GTE, 0);
        assertTrue(allRows > 20);
        assertTrue(plannedKeys(cfs, Operator.LT, 10) < allRows);
        assertRowCount(execute("SELECT pk FROM %s WHERE v < 10"), 10);
        assertEquals(0, estimateFailures(cfs));

        Injections.inject(Injections.newCustom("fail_estimate")
                                    .add(InvokePointBuilder.newInvokePoint().onClass(estimator).onMethod(method))
                                    .add(ActionBuilder.newActionBuilder().actions().doAction(failure))
                                    .build());

        assertRowCount(execute("SELECT pk FROM %s WHERE v < 10"), 10);
        assertRowCount(execute("SELECT pk FROM %s WHERE v >= 10 AND v < 20"), 10);
        assertRowCount(execute("SELECT pk FROM %s WHERE v != 10"), rows - 1);
        assertRowCount(execute("SELECT pk FROM %s WHERE v = 10"), 1);
        assertEquals(allRows, plannedKeys(cfs, Operator.LT, 10), 0);
        // The failures are counted, by the queries that recorded their table metrics
        assertTrue(estimateFailures(cfs) > 0);
    }

    /**
     * @return the number of keys the query planner expects the index to return for the given restriction on v
     */
    private static double plannedKeys(ColumnFamilyStore cfs, Operator op, int value)
    {
        ReadCommand command = Util.cmd(cfs).filterOn("v", op, value).build();
        QueryController controller = new QueryController(cfs,
                                                         command,
                                                         SAIUtil.currentVersion().onDiskFormat().indexFeatureSet(),
                                                         new QueryContext(),
                                                         null);
        try
        {
            Plan.RowsIteration plan = (Plan.RowsIteration) controller.buildPlan();
            Plan.KeysIteration keys = plan.firstNodeOfType(Plan.KeysIteration.class);
            assertNotNull(keys);
            return keys.expectedKeys();
        }
        finally
        {
            // releases the SSTable indexes the plan referenced
            controller.abort();
        }
    }
}
