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
package org.apache.cassandra.db.memtable;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class MemtableIndexFailureTest extends CQLTester
{
    @Parameterized.Parameter()
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> parameters()
    {
        return ImmutableList.of("skiplist", "trie", "trie_stage1", "trie_stage2", "trie_stage3");
    }

    /**
     * A write whose index fails in {@link Index.Indexer#finish()} may already be in the memtable, with a column that no
     * earlier write of the memtable has; the memtable still flushes, and the flushed rows are the rows read before.
     */
    @Test
    public void testFlushAfterIndexFailureInFinish() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, c int) WITH memtable = '" + memtable + "'");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING '" + FinishFailingIndex.class.getName() + "'");
        execute("INSERT INTO %s (k, a) VALUES (0, 0)");

        FinishFailingIndex.fail = true;
        try
        {
            execute("INSERT INTO %s (k, a, c) VALUES (1, 1, 1)");
            fail("expected the index failure");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains(FinishFailingIndex.FAILURE));
        }
        finally
        {
            FinishFailingIndex.fail = false;
        }

        Object[][] rows = getRows(execute("SELECT k, a, c FROM %s"));
        // the skip-list memtables apply the write before the index fails
        if (memtable.toLowerCase().startsWith("skiplist"))
            assertEquals(2, rows.length);
        flush();
        assertRows(execute("SELECT k, a, c FROM %s"), rows);
        assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

    public static class FinishFailingIndex extends StubIndex
    {
        static final String FAILURE = "Injected failure of FinishFailingIndex";
        static volatile boolean fail;

        public FinishFailingIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public Indexer indexerFor(DecoratedKey key,
                                  RegularAndStaticColumns columns,
                                  long nowInSec,
                                  WriteContext ctx,
                                  IndexTransaction.Type transactionType,
                                  Memtable memtable)
        {
            return new Indexer()
            {
                @Override
                public void finish()
                {
                    if (fail)
                        throw new RuntimeException(FAILURE);
                }
            };
        }
    }
}
