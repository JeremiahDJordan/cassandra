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

import java.util.UUID;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

/**
 * A full rebuild that rebuilds every index of an SSTable, because its per-SSTable components number the empty static
 * rows, doesn't run at the same time as another build of the SSTable, such as the initial build of a new index: both
 * would write the index's files.
 */
public class ConcurrentStaticRowIndexBuildTest extends SAITester.Versioned.WithImmutableComponents
{
    @After
    public void tearDown() throws Throwable
    {
        Injections.deleteAll();
        StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = false;
    }

    @Test
    public void testRebuildWhileAnotherIndexIsBuilt() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, v int, w int, PRIMARY KEY (pk, ck))");
        disableCompaction();
        for (int pk = 0; pk < 20; pk++)
        {
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v, w) VALUES (?, ?, ?, ?)", pk, ck, pk * 10 + ck, pk);
            // most partitions have no static row
            if (pk % 4 == 0)
                execute("INSERT INTO %s (pk, st) VALUES (?, ?)", pk, pk);
        }
        flush();

        // the indexes were built by an earlier release
        StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = true;
        String v;
        try
        {
            v = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        }
        finally
        {
            StorageAttachedIndexBuilder.NUMBER_EMPTY_STATIC_ROWS = false;
        }

        InvokePointBuilder afterNumberingCheck = InvokePointBuilder.newInvokePoint()
                                                                   .onClass(StorageAttachedIndexBuilder.class)
                                                                   .onMethod("numbersEmptyStaticRows")
                                                                   .atExit();
        Injections.inject(Injections.newBarrier("numbering_check_" + UUID.randomUUID(), 2, false).add(afterNumberingCheck).build());

        // CREATE INDEX st, and nodetool rebuild_index v while it builds
        String st = createIndexAsync("CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex'");
        rebuildIndexes(v);
        waitForIndexBuilds(KEYSPACE, st);
        Injections.deleteAll(); // removes the barrier, which the restart below would wait at

        waitForTableIndexesQueryable();
        verify();
        simulateNodeRestart();
        verify();
    }

    private void verify()
    {
        for (int pk = 0; pk < 20; pk++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v = ?", pk * 10 + 1), row(pk, 1));
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE w = ?", pk), row(pk, 0), row(pk, 1), row(pk, 2));
            if (pk % 4 == 0)
                assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE st = ?", pk), row(pk, 0), row(pk, 1), row(pk, 2));
        }
    }
}
