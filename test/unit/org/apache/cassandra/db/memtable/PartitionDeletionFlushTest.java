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

package org.apache.cassandra.db.memtable;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;

/**
 * A partition deletion applied to a partition that has rows in the memtable must survive the flush, and so must the
 * rows written after it.
 */
@RunWith(Parameterized.class)
public class PartitionDeletionFlushTest extends CQLTester
{
    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object> parameters()
    {
        // "default" means no memtable option on the table, i.e. TrieMemtable
        return Arrays.asList("default", "trie", "trie_stage1", "trie_stage2", "trie_stage3", "skiplist");
    }

    private void createTableWithMemtable(String createTable)
    {
        createTable("default".equals(memtable) ? createTable : createTable + " WITH memtable = '" + memtable + '\'');
        disableCompaction();
    }

    private void assertRowsBeforeAndAfterFlush(String query, Object[]... rows)
    {
        assertRows(execute(query), rows);
        flush();
        assertRows(execute(query), rows);
        compact();
        assertRows(execute(query), rows);
    }

    /// The partition deletion must reach the SSTable, or the rows in the older SSTable come back.
    @Test
    public void testPartitionDeletionOverMemtableRows()
    {
        createTableWithMemtable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 0) USING TIMESTAMP 10");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 0) USING TIMESTAMP 10");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 11, 0) USING TIMESTAMP 11");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 12, 0) USING TIMESTAMP 11");
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 1");

        assertRowsBeforeAndAfterFlush("SELECT ck FROM %s WHERE pk = 1");
    }

    /// A row written in the same batch as the partition deletion must be flushed.
    @Test
    public void testPartitionDeletionAndInsertInOneBatch()
    {
        createTableWithMemtable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        for (int ck = 11; ck <= 13; ++ck)
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, 0) USING TIMESTAMP 11", ck);
        execute("BEGIN UNLOGGED BATCH " +
                "DELETE FROM %1$s USING TIMESTAMP 20 WHERE pk = 1; " +
                "INSERT INTO %1$s (pk, ck, v) VALUES (1, 30, 1) USING TIMESTAMP 30; " +
                "APPLY BATCH");

        assertRowsBeforeAndAfterFlush("SELECT ck FROM %s WHERE pk = 1", row(30));
    }

    /// A row written after the partition deletion must be flushed.
    @Test
    public void testInsertAfterPartitionDeletion()
    {
        createTableWithMemtable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 11, 0) USING TIMESTAMP 11");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 12, 0) USING TIMESTAMP 11");
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 1");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 30, 1) USING TIMESTAMP 30");

        assertRowsBeforeAndAfterFlush("SELECT ck FROM %s WHERE pk = 1", row(30));
    }

    /// A partition deletion older than the rows in the memtable deletes none of them; the rows must still be counted,
    /// and the row deletions that follow must be flushed.
    @Test
    public void testOlderPartitionDeletionThenRowDeletions()
    {
        createTableWithMemtable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 11, 0) USING TIMESTAMP 1");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 12, 0) USING TIMESTAMP 1");
        flush();
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 11, 0) USING TIMESTAMP 11");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 12, 0) USING TIMESTAMP 11");
        execute("DELETE FROM %s USING TIMESTAMP 5 WHERE pk = 1");
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 1 AND ck = 11");
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 1 AND ck = 12");

        assertRowsBeforeAndAfterFlush("SELECT ck FROM %s WHERE pk = 1");
    }

    /// The same with a static row, which the partition deletion also removes.
    @Test
    public void testPartitionDeletionWithStaticRow()
    {
        createTableWithMemtable("CREATE TABLE %s (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, s, v) VALUES (1, 1, 1, 0) USING TIMESTAMP 10");
        flush();
        execute("INSERT INTO %s (pk, ck, s, v) VALUES (1, 11, 11, 0) USING TIMESTAMP 11");
        execute("DELETE FROM %s USING TIMESTAMP 20 WHERE pk = 1");

        assertRowsBeforeAndAfterFlush("SELECT ck, s FROM %s WHERE pk = 1");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 30, 1) USING TIMESTAMP 30");
        assertRowsBeforeAndAfterFlush("SELECT ck, s FROM %s WHERE pk = 1", row(30, null));
    }
}
