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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.memory.MemtableIndex;

import static org.apache.cassandra.cql3.CQL3Type.Native.DECIMAL;
import static org.apache.cassandra.cql3.CQL3Type.Native.INT;
import static org.apache.cassandra.cql3.CQL3Type.Native.VARINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SingleRestrictionEstimatedRowCountTest extends SAITester
{
    static protected Map<Map.Entry<Version, CQL3Type.Native>, ColumnFamilyStore> tables = new HashMap<>();
    static Version[] versions = new Version[]{ Version.DB, Version.EB };
    static CQL3Type.Native[] types = new CQL3Type.Native[]{ INT, DECIMAL, VARINT };

    static protected Object getFilterValue(CQL3Type.Native type, int value)
    {
        switch (type)
        {
            case INT:
                return value;
            case DECIMAL:
                return BigDecimal.valueOf(value);
            case VARINT:
                return BigInteger.valueOf(value);
        }
        fail("Must be known type");
        return null;
    }

    static Map.Entry<Version, CQL3Type.Native> tablesEntryKey(Version version, CQL3Type.Native type)
    {
        return new AbstractMap.SimpleEntry<>(version, type);
    }

    @Test
    public void testMemtablesSAI()
    {
        // Use fixed number of shards to make the test predictable; without it, the default is
        // to use the processors count, which may vary depending on the environment where tests are run.
        TrieMemtable.SHARD_COUNT = 8;

        createTables();

        // Estimates are just estimates, they are expected to have some inaccuracy, therefore we don't check
        // them for equality. Common sources of estimate inaccuracy are:
        // - estimating number of memtable rows based on memtable live size and average row size
        // - finite resolution of histograms (applies to newer histogram based estimation in E* formats and later)
        // - taking a subset of shards for estimation for better speed
        // - running search on some shards lazily

        for (Version version : versions)
        {
            boolean[] useTermStatsTestedValues = new boolean[]{ false };
            if (version.onOrAfter(Version.EB))
                useTermStatsTestedValues = new boolean[]{ true, false };
            for (boolean useTermStats : useTermStatsTestedValues)
            {
                QueryController.QUERY_OPT_USE_TERM_STATS = useTermStats;

                RowCountTest test = new RowCountTest(Operator.NEQ, 25);
                test.doTest(version, INT, 95, 99);
                test.doTest(version, DECIMAL, 95, 99);
                test.doTest(version, VARINT, 95, 99);

                test = new RowCountTest(Operator.LT, 50);
                test.doTest(version, INT, 40, 60);
                test.doTest(version, DECIMAL, 40, 60);
                test.doTest(version, VARINT, 40, 60);

                test = new RowCountTest(Operator.LT, 150);
                test.doTest(version, INT, 95, 100);
                test.doTest(version, DECIMAL, 95, 100);
                test.doTest(version, VARINT, 95, 100);

                test = new RowCountTest(Operator.EQ, 31);
                // For older on-disk formats we expect less accurate estimates due to lack of per-index stats and due to
                // lazy search on the first shard only; in this scenario each shard iterator will report at least one row,
                // even if none are matching. We could have run the search on all shards to get more accurate estimates,
                // but search is expensive, so we accept less accurate estimates for older formats.
                int maxExpectedRows = useTermStats ? 1 : TrieMemtable.SHARD_COUNT;
                test.doTest(version, INT, 1, maxExpectedRows);
                test.doTest(version, DECIMAL, 1, maxExpectedRows);
                test.doTest(version, VARINT, 1, maxExpectedRows);
            }
        }
    }

    /**
     * The memtable estimates of NOT CONTAINS, NOT CONTAINS KEY and != count the rows of the memtable once, with the
     * first memtable shard empty or holding a single row.
     */
    @Test
    public void testMemtableNegationEstimatesWithUnevenShards()
    {
        for (int firstShardRows : new int[]{ 0, 1 })
        {
            createTable("CREATE TABLE %s (pk int PRIMARY KEY, st set<text>, mk map<text, text>, n int)");
            String setIndex = createIndex("CREATE CUSTOM INDEX ON %s(st) USING 'StorageAttachedIndex'");
            String mapIndex = createIndex("CREATE CUSTOM INDEX ON %s(KEYS(mk)) USING 'StorageAttachedIndex'");
            String intIndex = createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            ShardBoundaries boundaries = cfs.localRangeSplits(TrieMemtable.SHARD_COUNT);
            int[] shardRows = { firstShardRows, 199 };
            int[] written = new int[shardRows.length];
            int rows = 0;
            int ones = 0;
            for (int pk = 0; !Arrays.equals(written, shardRows); pk++)
            {
                int shard = boundaries.getShardForKey(cfs.decorateKey(Int32Type.instance.decompose(pk)));
                if (shard >= shardRows.length || written[shard] == shardRows[shard])
                    continue;
                written[shard]++;
                rows++;
                ones += pk % 2;
                execute("INSERT INTO %s (pk, st, mk, n) VALUES (?, {'a'}, {'a': 'x'}, ?)", pk, pk % 2);
            }

            assertEquals(rows, memtableEstimate(setIndex, Operator.NOT_CONTAINS, "b"), rows * 0.1);
            assertEquals(rows, memtableEstimate(mapIndex, Operator.NOT_CONTAINS_KEY, "b"), rows * 0.1);
            assertEquals(rows - ones, memtableEstimate(intIndex, Operator.NEQ, "1"), rows * 0.1);
        }
    }

    private long memtableEstimate(String index, Operator operator, String value)
    {
        IndexContext context = getIndexContext(index);
        MemtableIndex memtableIndex = context.getLiveMemtables().get(getCurrentColumnFamilyStore().getCurrentMemtable());
        return memtableIndex.estimateMatchingRowsCount(new Expression(context).add(operator, context.getValidator().fromString(value)));
    }

    void createTables()
    {
        for (Version version : versions)
        {
            SAIUtil.setCurrentVersion(version);
            for (CQL3Type.Native type : types)
            {
                createTable("CREATE TABLE %s (pk text PRIMARY KEY, age " + type + ')');
                createIndex("CREATE CUSTOM INDEX ON %s(age) USING 'StorageAttachedIndex'");
                tables.put(tablesEntryKey(version, type), getCurrentColumnFamilyStore());
            }
        }
        flush();
        for (ColumnFamilyStore cfs : tables.values())
            populateTable(cfs);
    }

    void populateTable(ColumnFamilyStore cfs)
    {
        // Avoid race condition of starting before flushing completed
        cfs.unsafeRunWithoutFlushing(() -> {
            for (int i = 0; i < 100; i++)
            {
                String query = String.format("INSERT INTO %s (pk, age) VALUES (?, " + i + ')',
                        cfs.keyspace.getName() + '.' + cfs.name);
                executeFormattedQuery(query, "key" + i);
            }
        });
    }

    static class RowCountTest
    {
        final Operator op;
        final int filterValue;

        RowCountTest(Operator op, int filterValue)
        {
            this.op = op;
            this.filterValue = filterValue;
        }

        void doTest(Version version, CQL3Type.Native type, double minExpectedRows, double maxExpectedRows)
        {
            ColumnFamilyStore cfs = tables.get(new AbstractMap.SimpleEntry<>(version, type));
            Object filter = getFilterValue(type, filterValue);
            ReadCommand rc = Util.cmd(cfs)
                                 .columns("age")
                                 .filterOn("age", op, filter)
                                 .build();
            QueryController controller = new QueryController(cfs,
                                                             rc,
                                                             version.onDiskFormat().indexFeatureSet(),
                                                             new QueryContext(),
                                                             null);

            long totalRows = controller.planFactory.tableMetrics.rows;
            assertEquals(0, cfs.metrics().liveSSTableCount.getValue().intValue());
            assertEquals(97, totalRows);

            Plan plan = controller.buildPlan();
            assert plan instanceof Plan.RowsIteration;
            Plan.RowsIteration root = (Plan.RowsIteration) plan;
            Plan.KeysIteration planNode = root.firstNodeOfType(Plan.KeysIteration.class);
            assertNotNull(planNode);

            double minSelectivity = minExpectedRows / totalRows;
            double maxSelectivity = maxExpectedRows / totalRows;

            assertThat(root.expectedRows()).isBetween(minExpectedRows, maxExpectedRows);
            assertThat(planNode.expectedKeys()).isBetween(minExpectedRows, maxExpectedRows);
            assertThat(planNode.selectivity()).isBetween(minSelectivity, maxSelectivity);
        }
    }
}
