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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.junit.Assert.assertEquals;

/**
 * Reads paged by bytes, and aggregations, return every row when the rows have expired or deleted data that replicas
 * count before purging it and the coordinator counts after. Rows are read once their TTLs have expired, well within
 * gc_grace_seconds.
 */
public class BytePagingLiveDataSizeTest extends TestBaseImpl
{
    private static final int PARTITIONS = 2;
    private static final int ROWS = 50;
    private static final int PAGE_SIZE_IN_BYTES = 256;
    private static final int READ_DELAY_IN_SEC = 3600;

    private static Cluster cluster;
    private static int tables;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(3)
                              .withInstanceInitializer(BBHelper::install)
                              .withConfig(config -> config.set("aggregation_subpage_size_in_kb", 1))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testExpiredRowLiveness()
    {
        testPaging("a int, b int",
                   "INSERT INTO %s (pk, ck, a) VALUES (?, ?, 0) USING TTL 60",
                   "UPDATE %s SET b = 1 WHERE pk = ? AND ck = ?");
    }

    @Test
    public void testExpiredCollectionCells()
    {
        testPaging("m map<int, int>",
                   "UPDATE %s USING TTL 60 SET m = m + {1: 1, 2: 2} WHERE pk = ? AND ck = ?",
                   "UPDATE %s SET m = m + {3: 3} WHERE pk = ? AND ck = ?");
    }

    @Test
    public void testDeletedCollectionElements()
    {
        testPaging("m map<int, int>",
                   "UPDATE %s SET m = m + {1: 1, 2: 2, 3: 3} WHERE pk = ? AND ck = ?",
                   "DELETE m[1], m[2] FROM %s WHERE pk = ? AND ck = ?");
    }

    /**
     * A replica that counts more bytes for the rows it reads than the coordinator, as replicas of earlier releases do,
     * stops its pages early. It holds deletions of rows that the other replicas return after its last row, so the
     * coordinator has to read on from it.
     */
    @Test
    public void testReplicaCountingMoreBytes()
    {
        String table = createTable("m map<int, int>");
        write("UPDATE %s USING TTL 60 SET m = m + {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9, 10: 10} WHERE pk = ? AND ck = ?", table);
        write("UPDATE %s SET m = m + {11: 11} WHERE pk = ? AND ck = ?", table);
        for (int pk = 0; pk < PARTITIONS; pk++)
            for (int ck = 0; ck < ROWS; ck += 3)
                cluster.get(2).executeInternal(format("DELETE FROM %s WHERE pk = ? AND ck = ?", table), pk, ck);

        cluster.get(2).runOnInstance(() -> BBHelper.countFullRowSize.set(true));
        try
        {
            for (String query : new String[]{ "SELECT * FROM %s WHERE pk = 0", "SELECT * FROM %s" })
            {
                String select = format(query, table);
                List<String> expected = execute(select, "ALL", Integer.MAX_VALUE, false);
                assertEquals(select, (ROWS - (ROWS + 2) / 3) * (query.contains("WHERE") ? 1 : PARTITIONS), expected.size());
                assertEquals(select, expected, execute(select, "ALL", PAGE_SIZE_IN_BYTES, true));
            }

            List<String> count = execute(format("SELECT COUNT(*) FROM %s", table), "ALL", 1000, false);
            assertEquals(Collections.singletonList(Arrays.toString(new Object[]{ (long) (ROWS - (ROWS + 2) / 3) * PARTITIONS })), count);
        }
        finally
        {
            cluster.get(2).runOnInstance(() -> BBHelper.countFullRowSize.set(false));
        }
    }

    /**
     * Writes every row with the first statement and then with the second, with the data in memtables, split between
     * a memtable and an sstable, or in two sstables.
     */
    private static void testPaging(String columns, String first, String second)
    {
        for (int flushes = 0; flushes <= 2; flushes++)
        {
            String table = createTable(columns);
            write(first, table);
            if (flushes >= 1)
                cluster.forEach(node -> node.flush(KEYSPACE));
            write(second, table);
            if (flushes == 2)
                cluster.forEach(node -> node.flush(KEYSPACE));

            for (String cl : new String[]{ "ONE", "ALL" })
            {
                for (String query : new String[]{ "SELECT * FROM %s WHERE pk = 0", "SELECT * FROM %s" })
                {
                    String select = format(query, table);
                    String context = select + " at " + cl + " after " + flushes + " flushes";
                    List<String> expected = execute(select, cl, Integer.MAX_VALUE, false);
                    assertEquals(context, query.contains("WHERE") ? ROWS : ROWS * PARTITIONS, expected.size());
                    assertEquals(context, expected, execute(select, cl, PAGE_SIZE_IN_BYTES, true));
                }

                List<String> count = execute(format("SELECT COUNT(*) FROM %s", table), cl, 1000, false);
                assertEquals("COUNT(*) at " + cl + " after " + flushes + " flushes",
                             Collections.singletonList(Arrays.toString(new Object[]{ (long) ROWS * PARTITIONS })), count);
            }
        }
    }

    private static String createTable(String columns)
    {
        String table = KEYSPACE + ".t" + tables++;
        cluster.schemaChange(format("CREATE TABLE %s (pk int, ck int, %s, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'", table, columns));
        return table;
    }

    private static void write(String statement, String table)
    {
        for (int pk = 0; pk < PARTITIONS; pk++)
            for (int ck = 0; ck < ROWS; ck++)
                cluster.coordinator(1).execute(format(statement, table), ALL, pk, ck);
    }

    /**
     * Executes the query on node 1 with the given page size, in rows or bytes, as if READ_DELAY_IN_SEC later.
     */
    private static List<String> execute(String query, String consistency, int pageSize, boolean inBytes)
    {
        return cluster.get(1).callOnInstance(() -> {
            int nowInSec = FBUtilities.nowInSeconds() + READ_DELAY_IN_SEC;
            ClientState clientState = ClientState.forInternalCalls();
            SelectStatement statement = (SelectStatement) QueryProcessor.getStatement(query, clientState);
            QueryState queryState = new QueryState(clientState);
            PageSize size = inBytes ? PageSize.inBytes(pageSize) : PageSize.inRows(pageSize);

            List<String> rows = new ArrayList<>();
            PagingState pagingState = null;
            do
            {
                QueryOptions options = QueryOptions.create(ConsistencyLevel.valueOf(consistency), Collections.emptyList(), false,
                                                           size, pagingState, null, ProtocolVersion.CURRENT, null,
                                                           Long.MIN_VALUE, nowInSec);
                ResultMessage.Rows page = statement.execute(queryState, options, System.nanoTime());
                RowUtil.toIter(page).forEachRemaining(row -> rows.add(Arrays.toString(row)));
                pagingState = page.result.metadata.getPagingState();
            }
            while (pagingState != null);
            return rows;
        });
    }

    public static class BBHelper
    {
        static final AtomicBoolean countFullRowSize = new AtomicBoolean();

        static void install(ClassLoader cl, int node)
        {
            if (node != 2)
                return;
            new ByteBuddy().rebase(BTreeRow.class)
                           .method(named("liveDataSize").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static int liveDataSize(int nowInSec, @This BTreeRow row, @SuperCall Callable<Integer> zuper) throws Exception
        {
            return countFullRowSize.get() ? row.dataSize() : zuper.call();
        }
    }
}
