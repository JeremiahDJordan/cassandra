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
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wide tables with SSTables of versions up to FB and of version GA, whose primary key maps encode keys and designate
 * static rows differently: intersections that skip between them return the same rows as filtering without indexes.
 */
public class MixedVersionWideRowsTest extends SAITester
{
    @After
    public void resetVersionAndOptimizer()
    {
        SAIUtil.resetCurrentVersion();
        CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.setInt(1);
    }

    @Test
    public void testGaWithOlderVersions() throws Throwable
    {
        for (String older : new String[]{ "ba", "ec", "fb" })
        {
            assertSameRowsAsFiltering(List.of(Version.GA, Version.parse(older), Version.GA, Version.parse(older)), false, older.hashCode());
            assertSameRowsAsFiltering(List.of(Version.parse(older), Version.GA, Version.parse(older)), true, older.hashCode() + 1);
        }
    }

    /**
     * Writes the same random rows to a table with indexes on {@code a} and {@code b} (and on the static column
     * {@code st}) and to a table without, flushing once per provided version (current at the time of the flush), then
     * checks intersections on the former return the same rows as filtering the latter.
     */
    private void assertSameRowsAsFiltering(List<Version> versions, boolean withStatic, long seed) throws Throwable
    {
        String columns = "(pk int, ck int, a int, b int, " + (withStatic ? "st int static, " : "") + "PRIMARY KEY (pk, ck))";
        String plain = createTable("CREATE TABLE %s " + columns);
        disableCompaction();
        String indexed = createTable("CREATE TABLE %s " + columns);
        disableCompaction();
        createIndex("CREATE INDEX ON %s(a) USING 'sai'");
        createIndex("CREATE INDEX ON %s(b) USING 'sai'");
        if (withStatic)
            createIndex("CREATE INDEX ON %s(st) USING 'sai'");
        waitForTableIndexesQueryable();

        Random random = new Random(seed);
        for (Version version : versions)
        {
            SAIUtil.setCurrentVersion(version);
            for (int pk = 0; pk < 14; pk++)
            {
                if (withStatic && random.nextInt(4) == 0)
                    both(indexed, plain, "UPDATE %s SET st = ? WHERE pk = ?", random.nextInt(3), pk);
                for (int ck = 0; ck < 8; ck++)
                {
                    int action = random.nextInt(10);
                    if (action < 4)
                        both(indexed, plain, "INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", pk, ck, random.nextInt(4), random.nextInt(4));
                    else if (action == 4)
                        both(indexed, plain, "UPDATE %s SET a = ? WHERE pk = ? AND ck = ?", random.nextInt(4), pk, ck);
                    else if (action == 5)
                        both(indexed, plain, "UPDATE %s SET b = ? WHERE pk = ? AND ck = ?", random.nextInt(4), pk, ck);
                    else if (action == 6 && random.nextInt(3) == 0)
                        both(indexed, plain, "DELETE FROM %s WHERE pk = ? AND ck = ?", pk, ck);
                }
            }
            flush(KEYSPACE, indexed);
            flush(KEYSPACE, plain);
        }
        SAIUtil.resetCurrentVersion();

        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, indexed);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertThat(cfs.getLiveSSTables().stream().map(s -> group.descriptorFor(s).perSSTableComponents().version()))
        .containsAll(versions);

        List<String> queries = new ArrayList<>();
        String select = "SELECT pk, ck FROM %s WHERE ";
        for (int a = 0; a < 4; a++)
            for (int b = 0; b < 4; b++)
                queries.add(select + "a = " + a + " AND b = " + b);
        queries.add(select + "a > 1 AND b < 2");
        queries.add(select + "a = 1 OR b = 2");
        queries.add(select + "a != 1 AND b = 2");
        queries.add(select + "a > 0 AND b > 0 LIMIT 7");
        queries.add(select + "pk = 5 AND a = 1 AND b >= 1");
        queries.add(select + "token(pk) > 0 AND a = 1 AND b >= 1");
        queries.add(select + "pk = 5 AND ck >= 2 AND ck < 6 AND a >= 1 AND b >= 1");
        if (withStatic)
        {
            for (int st = 0; st < 3; st++)
            {
                queries.add(select + "st = " + st + " AND a = 1");
                queries.add(select + "st = " + st + " AND a >= 2 AND b <= 1");
                queries.add(select + "st = " + st + " OR b = 3");
            }
            queries.add("SELECT pk FROM %s WHERE st = 1 AND a = 2 PER PARTITION LIMIT 1");
        }

        for (int level : new int[]{ 1, 0 })
        {
            CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.setInt(level);
            for (String query : queries)
            {
                List<List<Object>> expected = rows(executeFormattedQuery(String.format(query + " ALLOW FILTERING", KEYSPACE + '.' + plain)));
                List<List<Object>> actual = rows(executeFormattedQuery(String.format(query, KEYSPACE + '.' + indexed)));
                assertThat(actual).as("%s at optimization level %s over versions %s", query, level, versions).isEqualTo(expected);
            }
        }
        CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.setInt(1);
    }

    private void both(String indexed, String plain, String statement, Object... values)
    {
        executeFormattedQuery(String.format(statement, KEYSPACE + '.' + indexed), values);
        executeFormattedQuery(String.format(statement, KEYSPACE + '.' + plain), values);
    }

    private static List<List<Object>> rows(UntypedResultSet resultSet)
    {
        List<List<Object>> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : resultSet)
        {
            List<Object> values = new ArrayList<>();
            for (var column : row.getColumns())
                values.add(row.getBytes(column.name.toString()));
            rows.add(values);
        }
        return rows;
    }
}
