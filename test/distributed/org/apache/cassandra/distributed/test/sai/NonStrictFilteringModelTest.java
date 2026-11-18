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

package org.apache.cassandra.distributed.test.sai;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.toStrings;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Model-based test of SAI queries that replicas can't strictly filter (CASSANDRA-19018, CASSANDRA-20189).
 * <p>
 * Random writes and deletes are applied to random subsets of the 3 replicas (without hints nor read repair), so that
 * rows are partially updated on different replicas, optionally with an incremental repair in the middle and more
 * divergence on top. Queries at ALL, with and without paging and LIMIT, are then compared with their predicate
 * evaluated on the reconciled contents of the table, as are their first rows of each partition with PER PARTITION
 * LIMIT, and their groups with GROUP BY, with a limit and paged, with and without a filter. Most queries restrict the
 * values of an existing row, so that the expected results include rows split across replicas. One iteration has wide
 * partitions, where single restrictions match many more rows than whole filters.
 * <p>
 * The seed is fixed, so that a failure is tied to a change. Another seed can be given with the system property
 * {@code cassandra.test.non_strict_model_seed}, e.g. {@code -Dtest.jvm.args=-Dcassandra.test.non_strict_model_seed=123}
 * in ant.
 */
public class NonStrictFilteringModelTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(NonStrictFilteringModelTest.class);

    private static Cluster CLUSTER;
    private static final String COLS = "k, c, a, b, x, y, t, s";
    private static final int K = 0, C = 1, A = 2, B = 3, X = 4, Y = 5, T = 6, S = 7;
    private static final String[] TEXTS = { "Foo", "foo", "BAR", "bar" };
    private static final String SEED_PROPERTY = "cassandra.test.non_strict_model_seed";
    private static final long DEFAULT_SEED = 595317330956668L;
    private static final int ITERATIONS = 4;
    private static final int QUERY_ROUNDS = 3;

    private int mismatches = 0;
    private int errors = 0;
    private int checks = 0;
    private int writeErrors = 0;

    @BeforeClass
    public static void setUpCluster() throws Exception
    {
        // The random flushes make many small SSTables. Replica warnings about how many of their indexes a query reads
        // would fail paged queries: the in-JVM coordinator doesn't initialize CoordinatorWarnings for them.
        CLUSTER = init(Cluster.build(3)
                              .withConfig(config -> SAIUtil.withNonStrictReplicaFiltering(config, true)
                                                           .set("hinted_handoff_enabled", false)
                                                           .set("sai_sstable_indexes_per_query_warn_threshold", -1)
                                                           .with(GOSSIP).with(NETWORK))
                              .start());
    }

    @AfterClass
    public static void tearDown()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testRandomPartialUpdates() throws Exception
    {
        // checkstyle: suppress below 'blockSystemPropertyUsage'
        long baseSeed = Long.getLong(SEED_PROPERTY, DEFAULT_SEED);
        logger.info("MODEL SEED " + baseSeed);
        for (int i = 0; i < ITERATIONS; i++)
            iteration(i, baseSeed + i);
        logger.info(String.format("MODEL SUMMARY seed=%d checks=%d mismatches=%d errors=%d writeErrors=%d",
                                  baseSeed, checks, mismatches, errors, writeErrors));
        assertThat(writeErrors).as("write errors (seed %d)", baseSeed).isZero();
        assertThat(errors).as("query errors (seed %d)", baseSeed).isZero();
        assertThat(mismatches).as("mismatches (seed %d)", baseSeed).isZero();
    }

    private void iteration(int n, long seed) throws Exception
    {
        Random random = new Random(seed);
        String table = "model_" + n;
        boolean repair = n % 3 != 0;      // 0: all unrepaired, 1: repaired + little divergence, 2: repaired + more divergence
        boolean wide = n % 2 == 0;
        boolean veryWide = n == 3;        // a consistent base of wide partitions, with divergence on top
        int partitions = veryWide ? 2 : wide ? 4 : 40;
        int clusterings = veryWide ? 300 : wide ? 8 : 2;
        int ops = 60 + random.nextInt(120);

        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (k int, c int, a int, b int, x int, y int, t text, s int static, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(b) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(s) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(t) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}"));
        SAIUtil.waitForIndexQueryableOnAllNodes(CLUSTER, KEYSPACE);

        long ts = 1;
        if (veryWide)
            ts = baseRows(random, table, partitions, clusterings, ts);
        ts = writes(random, table, partitions, clusterings, ops, ts);
        if (repair)
        {
            // make everything consistent and repaired, then add unrepaired divergence on top
            CLUSTER.forEach(i -> i.flush(KEYSPACE));
            CLUSTER.get(1).nodetoolResult("repair", KEYSPACE, table).asserts().success();
            SAIUtil.waitForRepairedSSTables(CLUSTER, KEYSPACE, table);
            ts = writes(random, table, partitions, clusterings, n % 3 == 1 ? 10 + random.nextInt(20) : ops, ts);
        }

        Object[][] truth = CLUSTER.coordinator(1).execute(withKeyspace("SELECT " + COLS + " FROM %s." + table), ConsistencyLevel.ALL);
        String ctx = String.format("iter=%d seed=%d repair=%s wide=%s veryWide=%s rows=%d writeErrors=%d", n, seed, repair, wide, veryWide, truth.length, writeErrors);
        logger.info("MODEL ITERATION " + ctx);

        for (int q = 0; q < QUERY_ROUNDS; q++)
        {
            // most of the time, query for the values of an existing reconciled row, so that the expected results
            // aren't empty and include rows that are split across replicas
            Object[] sample = truth.length > 0 && random.nextInt(10) < 7 ? truth[random.nextInt(truth.length)] : null;
            int va = pick(sample, A, 3, random), vb = pick(sample, B, 3, random), vx = pick(sample, X, 3, random), vy = pick(sample, Y, 3, random), vs = pick(sample, S, 3, random);
            int vk = pick(sample, K, partitions, random), vc = Math.max(0, pick(sample, C, clusterings, random) - random.nextInt(2));
            String vt = sample != null && sample[T] != null ? (String) sample[T] : TEXTS[random.nextInt(TEXTS.length)];

            check(ctx, table, truth, "a = " + va + " AND b = " + vb, r -> eq(r, A, va) && eq(r, B, vb), random);
            check(ctx, table, truth, "a = " + va + " AND b = " + vb + " AND x = " + vx, r -> eq(r, A, va) && eq(r, B, vb) && eq(r, X, vx), random);
            check(ctx, table, truth, "a = " + va + " AND x = " + vx, r -> eq(r, A, va) && eq(r, X, vx), random);
            check(ctx, table, truth, "a = " + va + " AND x = " + vx + " AND y = " + vy, r -> eq(r, A, va) && eq(r, X, vx) && eq(r, Y, vy), random);
            check(ctx, table, truth, "a = " + va + " AND b IN (" + vb + ", " + ((vb + 1) % 3) + ")", r -> eq(r, A, va) && (eq(r, B, vb) || eq(r, B, (vb + 1) % 3)), random);
            if (q == 0)
                check(ctx, table, truth, "a = " + va + " AND (b = " + vb + " OR x = " + vx + ")",
                      r -> eq(r, A, va) && (eq(r, B, vb) || eq(r, X, vx)), random);
            if (q == 0)
                check(ctx, table, truth, "a = " + va + " AND x IN (" + vx + ", " + ((vx + 1) % 3) + ")",
                      r -> eq(r, A, va) && (eq(r, X, vx) || eq(r, X, (vx + 1) % 3)), random);
            check(ctx, table, truth, "a != " + va + " AND b = " + vb, r -> r[A] != null && !eq(r, A, va) && eq(r, B, vb), random);
            check(ctx, table, truth, "a = " + va + " AND x != " + vx, r -> eq(r, A, va) && r[X] != null && !eq(r, X, vx), random);
            check(ctx, table, truth, "s = " + vs + " AND a = " + va, r -> eq(r, S, vs) && eq(r, A, va), random);
            check(ctx, table, truth, "s = " + vs + " AND x = " + vx, r -> eq(r, S, vs) && eq(r, X, vx), random);
            check(ctx, table, truth, "a = " + va + " AND b = " + vb + " AND s = " + vs, r -> eq(r, A, va) && eq(r, B, vb) && eq(r, S, vs), random);
            check(ctx, table, truth, "k = " + vk + " AND a = " + va + " AND b = " + vb, r -> eq(r, K, vk) && eq(r, A, va) && eq(r, B, vb), random);
            check(ctx, table, truth, "k = " + vk + " AND c > " + vc + " AND a = " + va + " AND b = " + vb, r -> eq(r, K, vk) && ((Integer) r[C]) > vc && eq(r, A, va) && eq(r, B, vb), random);
            check(ctx, table, truth, "k = " + vk + " AND s = " + vs + " AND b = " + vb, r -> eq(r, K, vk) && eq(r, S, vs) && eq(r, B, vb), random);
            check(ctx, table, truth, "a = " + va + " OR b = " + vb, r -> eq(r, A, va) || eq(r, B, vb), random);
            check(ctx, table, truth, "(a = " + va + " AND b = " + vb + ") OR (a = " + ((va + 1) % 3) + " AND b = " + ((vb + 1) % 3) + ")",
                  r -> (eq(r, A, va) && eq(r, B, vb)) || (eq(r, A, (va + 1) % 3) && eq(r, B, (vb + 1) % 3)), random);
            check(ctx, table, truth, "t = '" + vt + "' AND b = " + vb, r -> r[T] != null && ((String) r[T]).equalsIgnoreCase(vt) && eq(r, B, vb), random);
            check(ctx, table, truth, "a >= " + va + " AND b < " + vb, r -> r[A] != null && ((Integer) r[A]) >= va && r[B] != null && ((Integer) r[B]) < vb, random);
            check(ctx, table, truth, "x = " + vx + " AND y = " + vy + " AND a = " + va + " AND b = " + vb, r -> eq(r, X, vx) && eq(r, Y, vy) && eq(r, A, va) && eq(r, B, vb), random);
            check(ctx, table, truth, "a = " + va, r -> eq(r, A, va), random);
            // GROUP BY without a filter, over partitions with several rows
            groupByChecks(ctx, table, truth, null, r -> true, random);
        }
        CLUSTER.schemaChange(withKeyspace("DROP TABLE %s." + table));
    }

    /**
     * Writes all the rows of the partitions on all the replicas, with b = 9 on most of them, so that restrictions on a
     * and b together match many fewer rows than restrictions on a alone.
     */
    private long baseRows(Random random, String table, int partitions, int clusterings, long ts)
    {
        String tbl = KEYSPACE + '.' + table;
        for (int k = 0; k < partitions; k++)
        {
            for (int c = 0; c < clusterings; c++)
            {
                String stmt = String.format("INSERT INTO %s (k, c, a, b, x, y, t) VALUES (%d, %d, %d, %d, %d, %d, '%s') USING TIMESTAMP %d", tbl, k, c,
                                            random.nextInt(3), random.nextInt(30) == 0 ? random.nextInt(3) : 9, random.nextInt(3), random.nextInt(3),
                                            TEXTS[random.nextInt(TEXTS.length)], ts++);
                for (int node = 1; node <= 3; node++)
                    apply(node, stmt);
            }
        }
        return ts;
    }

    private long writes(Random random, String table, int partitions, int clusterings, int ops, long ts)
    {
        String tbl = KEYSPACE + '.' + table;
        for (int i = 0; i < ops; i++)
        {
            int k = random.nextInt(partitions), c = random.nextInt(clusterings);
            String using = " USING TIMESTAMP " + (ts++);
            String stmt;
            int kind = random.nextInt(100);
            if (kind < 20)
            {
                // a row whose columns are each written to a single, random replica
                String[] cols = { "a", "b", "x", "y" };
                for (String col : cols)
                    apply(1 + random.nextInt(3), String.format("UPDATE %s USING TIMESTAMP %d SET %s = %d WHERE k = %d AND c = %d", tbl, ts++, col, random.nextInt(3), k, c));
                stmt = String.format("UPDATE %s USING TIMESTAMP %d SET t = '%s' WHERE k = %d AND c = %d", tbl, ts++, TEXTS[random.nextInt(TEXTS.length)], k, c);
            }
            else if (kind < 30)
                stmt = String.format("INSERT INTO %s (k, c, a, b, x, y, t) VALUES (%d, %d, %d, %d, %d, %d, '%s')%s", tbl, k, c,
                                     random.nextInt(3), random.nextInt(3), random.nextInt(3), random.nextInt(3), TEXTS[random.nextInt(TEXTS.length)], using);
            else if (kind < 70)
            {
                String[] cols = { "a", "b", "x", "y" };
                stmt = String.format("UPDATE %s%s SET %s = %d WHERE k = %d AND c = %d", tbl, using, cols[random.nextInt(cols.length)], random.nextInt(3), k, c);
            }
            else if (kind < 75)
                stmt = String.format("UPDATE %s%s SET t = '%s' WHERE k = %d AND c = %d", tbl, using, TEXTS[random.nextInt(TEXTS.length)], k, c);
            else if (kind < 83)
                stmt = String.format("UPDATE %s%s SET s = %d WHERE k = %d", tbl, using, random.nextInt(3), k);
            else if (kind < 88)
                stmt = String.format("DELETE FROM %s%s WHERE k = %d AND c = %d", tbl, using, k, c);
            else if (kind < 91)
            {
                String[] cols = { "a", "b", "x", "y", "t" };
                stmt = String.format("DELETE %s FROM %s%s WHERE k = %d AND c = %d", cols[random.nextInt(cols.length)], tbl, using, k, c);
            }
            else if (kind < 93)
                stmt = String.format("DELETE s FROM %s%s WHERE k = %d", tbl, using, k);
            else
                stmt = String.format("DELETE FROM %s%s WHERE k = %d", tbl, using, k);

            // apply to a random non-empty subset of the replicas
            int mask = 1 + random.nextInt(7);
            for (int node = 1; node <= 3; node++)
                if ((mask & (1 << (node - 1))) != 0)
                    apply(node, stmt);

            // half of the partition deletions are followed by newer writes, so that some partitions have a static row
            // and rows written after their deletion, which the replicas still return with the partition
            if (kind >= 93 && random.nextBoolean())
            {
                applyToRandomReplicas(random, String.format("UPDATE %s USING TIMESTAMP %d SET s = %d WHERE k = %d", tbl, ts++, random.nextInt(3), k));
                for (int row = 0, rows = 1 + random.nextInt(2); row < rows; row++)
                    applyToRandomReplicas(random, String.format("INSERT INTO %s (k, c, a, b, x, y) VALUES (%d, %d, %d, %d, %d, %d) USING TIMESTAMP %d", tbl, k,
                                                                random.nextInt(clusterings), random.nextInt(3), random.nextInt(3), random.nextInt(3), random.nextInt(3), ts++));
            }

            if (random.nextInt(25) == 0)
                CLUSTER.get(1 + random.nextInt(3)).flush(KEYSPACE);
        }
        return ts;
    }

    private void applyToRandomReplicas(Random random, String stmt)
    {
        int mask = 1 + random.nextInt(7);
        for (int node = 1; node <= 3; node++)
            if ((mask & (1 << (node - 1))) != 0)
                apply(node, stmt);
    }

    private void apply(int node, String stmt)
    {
        try
        {
            CLUSTER.get(node).executeInternal(stmt);
        }
        catch (Throwable t)
        {
            writeErrors++;
            logger.info("MODEL WRITE ERROR node" + node + ' ' + stmt + " -> " + t);
        }
    }

    private static int pick(Object[] sample, int col, int domain, Random random)
    {
        return sample != null && sample[col] != null ? (Integer) sample[col] : random.nextInt(domain);
    }

    private static boolean eq(Object[] row, int col, int value)
    {
        return row[col] != null && ((Integer) row[col]) == value;
    }

    private void check(String ctx, String table, Object[][] truth, String where, Predicate<Object[]> predicate, Random random)
    {
        Set<String> expected = new TreeSet<>();
        for (Object[] row : truth)
            if (row[C] != null && predicate.test(row))
                expected.add(Arrays.toString(row));

        String select = withKeyspace("SELECT " + COLS + " FROM %s." + table + " WHERE " + where + " ALLOW FILTERING");

        // unlimited
        compare(ctx, select, expected,
                () -> toStrings(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL)), -1);

        // paging
        int pageSize = 1 + random.nextInt(4);
        compare(ctx, select + " /* page size " + pageSize + " */", expected,
                () -> toStrings(CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, pageSize)), -1);

        // limit
        int limit = 1 + random.nextInt(4);
        String limited = select.replace(" ALLOW FILTERING", " LIMIT " + limit + " ALLOW FILTERING");
        compare(ctx, limited, expected,
                () -> toStrings(CLUSTER.coordinator(1).execute(limited, ConsistencyLevel.ALL)), limit);

        // per-partition limit: the first matching rows of each partition, in clustering order
        int perPartitionLimit = 1 + random.nextInt(3);
        Set<String> expectedPerPartition = new TreeSet<>();
        Map<Object, Integer> matchesPerPartition = new HashMap<>();
        for (Object[] row : truth)
            if (row[C] != null && predicate.test(row) && matchesPerPartition.merge(row[K], 1, Integer::sum) <= perPartitionLimit)
                expectedPerPartition.add(Arrays.toString(row));
        String selectPerPartition = select.replace(" ALLOW FILTERING", " PER PARTITION LIMIT " + perPartitionLimit + " ALLOW FILTERING");
        compare(ctx, selectPerPartition, expectedPerPartition,
                () -> toStrings(CLUSTER.coordinator(1).execute(selectPerPartition, ConsistencyLevel.ALL)), -1);
        int perPartitionPageSize = 1 + random.nextInt(4);
        compare(ctx, selectPerPartition + " /* page size " + perPartitionPageSize + " */", expectedPerPartition,
                () -> toStrings(CLUSTER.coordinator(1).executeWithPaging(selectPerPartition, ConsistencyLevel.ALL,
                                                                         perPartitionPageSize)),
                -1);

        groupByChecks(ctx, table, truth, where, predicate, random);
    }

    /**
     * Checks GROUP BY queries: each row is a group of its primary key, and each partition a group of its partition key,
     * whose replicas return complete groups that the coordinator may filter out. They select the static column, which
     * the partition at which a replica stops can lose, and aggregate over the rows of each partition, whose values a
     * replica that stops early can leave out.
     *
     * @param where the WHERE clause, or {@code null} for queries without one, which also return the partitions that only
     *              have a static row
     */
    private void groupByChecks(String ctx, String table, Object[][] truth, String where, Predicate<Object[]> predicate, Random random)
    {
        Set<String> expectedRowGroups = new TreeSet<>();
        Map<Object, Long> counts = new TreeMap<>();
        Map<Object, Integer> maxX = new TreeMap<>();
        Map<Object, Object> statics = new TreeMap<>();
        for (Object[] row : truth)
        {
            boolean staticOnly = row[C] == null;
            if (staticOnly ? where == null : predicate.test(row))
            {
                expectedRowGroups.add(Arrays.toString(new Object[]{ row[K], row[C], row[S], 1L }));
                counts.merge(row[K], 1L, Long::sum);
                statics.put(row[K], row[S]);
                if (row[X] != null)
                    maxX.merge(row[K], (Integer) row[X], Math::max);
            }
        }
        Set<String> expectedPartitionGroups = new TreeSet<>();
        counts.forEach((k, count) -> expectedPartitionGroups.add(Arrays.toString(new Object[]{ k, statics.get(k), count, maxX.get(k) })));

        String from = withKeyspace(" FROM %s." + table) + (where == null ? "" : " WHERE " + where);
        String filtering = where == null ? "" : " ALLOW FILTERING";
        // without a filter, every small limit and page size, as the replicas stop at the partitions they reach these at
        int[] limits = where == null ? new int[]{ 1, 2, 3, 4 } : new int[]{ 1 + random.nextInt(4) };
        int[] pageSizes = where == null ? new int[]{ 1, 2, 3 } : new int[]{ 1 + random.nextInt(4) };
        for (int groupLimit : limits)
        {
            String selectRowGroups = "SELECT k, c, s, count(*)" + from + " GROUP BY k, c LIMIT " + groupLimit + filtering;
            compare(ctx, selectRowGroups, expectedRowGroups,
                    () -> toStrings(CLUSTER.coordinator(1).execute(selectRowGroups, ConsistencyLevel.ALL)), groupLimit);
            String selectPartitionGroups = "SELECT k, s, count(*), max(x)" + from + " GROUP BY k LIMIT " + groupLimit + filtering;
            compare(ctx, selectPartitionGroups, expectedPartitionGroups,
                    () -> toStrings(CLUSTER.coordinator(1).execute(selectPartitionGroups, ConsistencyLevel.ALL)),
                    groupLimit);
        }
        for (int groupPageSize : pageSizes)
        {
            String pagedRowGroups = "SELECT k, c, s, count(*)" + from + " GROUP BY k, c" + filtering;
            compare(ctx, pagedRowGroups + " /* page size " + groupPageSize + " */", expectedRowGroups,
                    () -> toStrings(CLUSTER.coordinator(1).executeWithPaging(pagedRowGroups, ConsistencyLevel.ALL,
                                                                             groupPageSize)),
                    -1);
            String pagedPartitionGroups = "SELECT k, s, count(*), max(x)" + from + " GROUP BY k" + filtering;
            compare(ctx, pagedPartitionGroups + " /* page size " + groupPageSize + " */", expectedPartitionGroups,
                    () -> toStrings(CLUSTER.coordinator(1).executeWithPaging(pagedPartitionGroups, ConsistencyLevel.ALL,
                                                                             groupPageSize)),
                    -1);
        }
    }

    private void compare(String ctx, String query, Set<String> expected, Supplier<List<String>> q, int limit)
    {
        checks++;
        List<String> actual;
        try
        {
            actual = q.get();
        }
        catch (Throwable t)
        {
            errors++;
            Throwable root = t;
            while (root.getCause() != null && root.getCause() != root)
                root = root.getCause();
            logger.info(String.format("MODEL ERROR [%s] %s -> %s: %s (expected %d rows)",
                                      ctx, query, t.getClass().getSimpleName(),
                                      Objects.toString(root.getMessage()).replace('\n', ' '), expected.size()));
            return;
        }

        Set<String> actualSet = new TreeSet<>(actual);
        boolean ok;
        if (limit < 0)
            ok = actualSet.equals(expected) && actual.size() == expected.size();
        else
            ok = expected.containsAll(actualSet) && actual.size() == Math.min(limit, expected.size()) && actualSet.size() == actual.size();

        if (!ok)
        {
            mismatches++;
            Set<String> missing = new TreeSet<>(expected);
            missing.removeAll(actualSet);
            Set<String> extra = new TreeSet<>(actualSet);
            extra.removeAll(expected);
            logger.info(String.format("MODEL MISMATCH [%s] %s -> expected=%d actual=%d missing=%s extra=%s",
                                      ctx, query, expected.size(), actual.size(),
                                      limit < 0 ? missing : "(limit " + limit + ")", extra));
        }
    }
}
