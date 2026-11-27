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

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;

/**
 * Tests that the live data size of a memtable is the same for the same rows, whether a row was written once or
 * merged from several writes.
 */
@RunWith(Parameterized.class)
public class MemtableLiveDataSizeTest extends CQLTester
{
    private static final String INSERT = "INSERT INTO %s (pk, ck, v) VALUES (0, ?, 1) USING TIMESTAMP ";

    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> parameters()
    {
        return ImmutableList.of("skiplist", "trie_stage1", "trie_stage2", "trie_stage3");
    }

    @Test
    public void testTtlAddedToRowMarker()
    {
        assertEquals(liveDataSize(INSERT + "2000 AND TTL 100000"),
                     liveDataSize(INSERT + "1000", INSERT + "2000 AND TTL 100000"));
    }

    @Test
    public void testTtlRemovedFromRowMarker()
    {
        assertEquals(liveDataSize(INSERT + "2000"),
                     liveDataSize(INSERT + "1000 AND TTL 100000", INSERT + "2000"));
    }

    @Test
    public void testRowDeletionOverRowMarkerWithTtl()
    {
        String delete = "DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 0 AND ck = ?";
        assertEquals(liveDataSize(delete),
                     liveDataSize(INSERT + "1000 AND TTL 100000", delete));
    }

    @Test
    public void testRowDeletionKeepingNewerCell()
    {
        String update = "UPDATE %s USING TIMESTAMP 3000 SET v = 2 WHERE pk = 0 AND ck = ?";
        String delete = "DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 0 AND ck = ?";
        assertEquals(liveDataSize(delete, update),
                     liveDataSize(INSERT + "1000 AND TTL 100000", update, delete));
    }

    private long liveDataSize(String... writes)
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = '" + memtable + "'");
        for (int ck = 0; ck < 10; ck++)
        {
            for (String write : writes)
                execute(write, ck);
        }
        return getCurrentColumnFamilyStore().getCurrentMemtable().getLiveDataSize();
    }
}
