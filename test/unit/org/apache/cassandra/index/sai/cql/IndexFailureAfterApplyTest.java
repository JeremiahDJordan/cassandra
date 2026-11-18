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

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * An SAI indexer that fails on a write after the memtable applied it: the update stays applied and the write fails.
 */
public class IndexFailureAfterApplyTest extends SAITester
{
    private static final String[] MEMTABLES = { "trie" };

    private Injection failure;

    @Before
    public void injectFailure() throws Throwable
    {
        failure = Injections.newCustom("sai_insert_fails")
                            .add(InvokePointBuilder.newInvokePoint()
                                                   .onClass("org.apache.cassandra.index.sai.StorageAttachedIndex$UpdateIndexer")
                                                   .onMethod("insertRow")
                                                   .atEntry())
                            .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, "\"Injected indexer failure\""))
                            .build();
        Injections.inject(failure);
        failure.disable();
    }

    private void failingWrite(String query)
    {
        failure.enable();
        try
        {
            execute(query);
            fail("expected the injected failure");
        }
        catch (RuntimeException e)
        {
            // expected
        }
        finally
        {
            failure.disable();
        }
    }

    /** A rebuild flushes the memtable first, so that it indexes the row of the failed write the index missed. */
    @Test
    public void testRebuildRepairsTheFailedWriteInTheMemtable()
    {
        List<String> missing = new ArrayList<>();
        for (String memtable : MEMTABLES)
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
            disableCompaction();
            String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, 0, 1)");

            failingWrite("INSERT INTO %s (pk, ck, v) VALUES (0, 1, 1)");

            // the write is applied, and the index misses it (so the failure was injected where it matters)
            assertRows(execute("SELECT ck, v FROM %s WHERE pk = 0"), row(0, 1), row(1, 1));
            assertRows(execute("SELECT ck FROM %s WHERE v = 1"), row(0));
            assertEquals(memtable, 0, getCurrentColumnFamilyStore().getLiveSSTables().size());

            rebuildIndexes(index);
            if (execute("SELECT ck FROM %s WHERE v = 1").size() != 2)
                missing.add(memtable + " after the rebuild");

            flush();
            if (execute("SELECT ck FROM %s WHERE v = 1").size() != 2)
                missing.add(memtable + " after the flush that followed the rebuild");
            assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 0"), row(0), row(1));
        }
        assertEquals("the index misses the row of the failed write", Collections.emptyList(), missing);
    }
}
