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

package org.apache.cassandra.index.sai.disk.apache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests of the SAI indexes Apache Cassandra 5.0 wrote (test/data/legacy-sai/apache-aa, see its README),
 * checked against filtering the same data without indexes, the rows read from the table, and our rebuilt indexes.
 */
public class ApacheIndexUpgradeTest extends ApacheIndexTester
{
    private static final String[] TYPES_COLUMNS = { "i", "bi", "si", "ti", "f", "d", "dec", "vi", "dt", "tm", "ts", "u", "tu", "ip", "b", "t", "a" };
    private static final String TOMBS = "CREATE TABLE %s (pk int, ck int, s text, n int, st set<text>, v int, PRIMARY KEY (pk, ck))";

    /**
     * Every type of index, checked against our rebuilt indexes, and against filtering the same data without indexes
     * (for the queries that can run without indexes).
     */
    @Test
    public void testAllTypes() throws Throwable
    {
        createTable(TYPES);
        disableCompaction();
        String[] indexes = new String[TYPES_COLUMNS.length];
        for (int i = 0; i < TYPES_COLUMNS.length; i++)
        {
            indexes[i] = "types_" + TYPES_COLUMNS[i];
            createIndex("CREATE INDEX " + indexes[i] + " ON %s(" + TYPES_COLUMNS[i] + ") USING 'sai'");
        }
        waitForTableIndexesQueryable();
        loadApacheSSTables("types");
        assertReadFromApacheComponents(indexes);
        assertMultiSegment("types_i", "types_bi", "types_dec", "types_vi", "types_ip", "types_d");

        // deleted and expired rows are neither indexed nor returned
        assertThat(execute("SELECT pk FROM %s WHERE t = 'expired'")).isEmpty();
        assertThat(execute("SELECT pk FROM %s WHERE i = 1 AND pk = 100007 ALLOW FILTERING")).isEmpty();

        UUID u5 = UUID.nameUUIDFromBytes("5".getBytes());
        Map<String, Object> results = assertSameResultsAfterRebuild(List.of("SELECT pk FROM %s WHERE i = -500",
                                              "SELECT pk FROM %s WHERE i > 400",
                                              "SELECT pk FROM %s WHERE i >= -10 AND i < 10",
                                              "SELECT pk FROM %s WHERE i != 3 AND pk < 30 ALLOW FILTERING",
                                              "SELECT pk FROM %s WHERE bi = -1989999970",
                                              "SELECT pk FROM %s WHERE bi > 0 AND bi < 300000000",
                                              "SELECT pk FROM %s WHERE bi <= -1999000000",
                                              "SELECT pk FROM %s WHERE si < -140",
                                              "SELECT pk FROM %s WHERE si = 0",
                                              "SELECT pk FROM %s WHERE ti >= 95",
                                              "SELECT pk FROM %s WHERE ti = -100",
                                              "SELECT pk FROM %s WHERE f > 700.5",
                                              "SELECT pk FROM %s WHERE f = -299.5",
                                              "SELECT pk FROM %s WHERE d <= -990.0",
                                              "SELECT pk FROM %s WHERE d > 1000.0 AND d < 1100.0",
                                              "SELECT pk FROM %s WHERE dec > 5.00",
                                              "SELECT pk FROM %s WHERE dec = -19.97",
                                              "SELECT pk FROM %s WHERE dec >= -0.5 AND dec <= 0.5",
                                              "SELECT pk FROM %s WHERE vi > 1000000000000000000000000000000",
                                              "SELECT pk FROM %s WHERE vi = -1496",
                                              "SELECT pk FROM %s WHERE vi < -100000000000000000000000000000000",
                                              "SELECT pk FROM %s WHERE dt >= '2024-11-01'",
                                              "SELECT pk FROM %s WHERE dt = '2024-10-04'",
                                              "SELECT pk FROM %s WHERE tm < '00:00:05'",
                                              "SELECT pk FROM %s WHERE ts > '2023-11-16 00:00:00+0000'",
                                              "SELECT pk FROM %s WHERE ts = '2023-11-14 22:14:20+0000'",
                                              "SELECT pk FROM %s WHERE u = " + u5,
                                              "SELECT pk FROM %s WHERE tu > maxTimeuuid('2023-11-14 22:13:21+0000') AND tu < minTimeuuid('2023-11-14 22:13:22+0000')",
                                              "SELECT pk FROM %s WHERE ip = '10.0.0.4'",
                                              "SELECT pk FROM %s WHERE ip = 'fe80::107'",
                                              "SELECT pk FROM %s WHERE ip > '10.0.3.0' AND ip < '10.0.255.255'",
                                              "SELECT pk FROM %s WHERE ip >= 'fe80::700'",
                                              "SELECT pk FROM %s WHERE b = true AND pk < 100 ALLOW FILTERING",
                                              "SELECT pk FROM %s WHERE t = 't7'",
                                              "SELECT pk FROM %s WHERE t = ''",
                                              "SELECT pk FROM %s WHERE t LIKE 't4%%'",
                                              "SELECT pk FROM %s WHERE a = 'a3'",
                                              "SELECT pk FROM %s WHERE i > 0 AND t = 't3'",
                                              "SELECT pk FROM %s WHERE bi < 0 AND b = true",
                                              "SELECT pk FROM %s WHERE (i < -490 OR d > 2200.0) AND a = 'a1'",
                                              "SELECT pk, i FROM %s ORDER BY i LIMIT 10",
                                              "SELECT pk, d FROM %s ORDER BY d DESC LIMIT 10",
                                              "SELECT pk, ts FROM %s WHERE i > 300 ORDER BY ts LIMIT 5",
                                              "SELECT pk, si FROM %s ORDER BY si LIMIT 5",
                                              "SELECT pk, t FROM %s WHERE a = 'a4' ORDER BY t DESC LIMIT 8",
                                              "SELECT pk FROM %s WHERE i IN (1, 2, 3, -500)",
                                              "SELECT pk FROM %s WHERE bi != -1989999970 AND pk < 50 ALLOW FILTERING",
                                              "SELECT pk FROM %s WHERE dec = -19.970",
                                              "SELECT pk FROM %s WHERE vi >= -3 AND vi <= 3",
                                              "SELECT pk FROM %s WHERE ip = '::ffff:10.0.0.4'",
                                              "SELECT pk FROM %s WHERE ip < '10.0.0.9'",
                                              "SELECT pk FROM %s WHERE tu < minTimeuuid('2023-11-14 22:13:21+0000')",
                                              "SELECT pk FROM %s WHERE t IN ('t1', 't2', '')",
                                              "SELECT pk FROM %s WHERE i = 1 OR t = 'expired'"),
                                      indexes);
        assertSameResultsAsUnindexed("types", TYPES, unindexable(results));
    }

    /**
     * Varints written with redundant sign bytes (e.g. {@code blobAsVarint(0x0001)} for 1). On Apache's index a literal
     * finds the rows written the same way, and with our reader the minimal ones too; after a rebuild in our format
     * every equal varint is found. Apache's own results (recorded with the data set) must be in ours.
     */
    @Test
    public void testVarintsWithRedundantSignBytes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, vi varint, v int)");
        disableCompaction();
        createIndex("CREATE INDEX varints_vi ON %s(vi) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("varints");
        assertReadFromApacheComponents("varints_vi");
        // pk 1: 1, 2: 0x0001, 3: 0x000001, 4: 255, 5: 0x0000ff, 6: -1, 7: 0xffff, 8: 2, 9: 300, 10: 0

        // query, Apache's results, ours on Apache's index, ours after a rebuild
        Object[][] queries = {
            { "vi = 1", List.of(1), List.of(1), List.of(1, 2, 3) },
            { "vi = blobAsVarint(0x0001)", List.of(2), List.of(1, 2), List.of(1, 2, 3) },
            { "vi = blobAsVarint(0x000001)", List.of(3), List.of(1, 3), List.of(1, 2, 3) },
            { "vi = 255", List.of(4), List.of(4), List.of(4, 5) },
            { "vi = blobAsVarint(0x0000ff)", List.of(5), List.of(4, 5), List.of(4, 5) },
            { "vi = -1", List.of(6), List.of(6), List.of(6, 7) },
            { "vi = blobAsVarint(0xffff)", List.of(7), List.of(6, 7), List.of(6, 7) },
            { "vi > 0 AND vi < 3", List.of(1, 8), List.of(1, 8), List.of(1, 2, 3, 8) },
            { "vi >= blobAsVarint(0x0001) AND vi <= blobAsVarint(0x000002)", List.of(2, 3), List.of(1, 2, 3, 8),
              List.of(1, 2, 3, 8) },
            { "vi >= 1", List.of(1, 2, 3, 4, 5, 8, 9), List.of(1, 2, 3, 4, 5, 8, 9), List.of(1, 2, 3, 4, 5, 8, 9) },
            { "vi <= 1", List.of(1, 6, 7, 10), List.of(1, 6, 7, 10), List.of(1, 2, 3, 6, 7, 10) },
        };
        List<String> expected = new ArrayList<>();
        List<String> actual = new ArrayList<>();
        for (Object[] query : queries)
        {
            List<Integer> keys = keys("SELECT pk FROM %s WHERE " + query[0]);
            assertThat(keys).as("%s on Apache's index: Apache's results", query[0])
                            .containsAll((Iterable<Integer>) query[1]);
            expected.add(query[0] + ": " + query[2]);
            actual.add(query[0] + ": " + keys);
        }
        assertThat(actual).as("on Apache's index").isEqualTo(expected);

        rebuildTableIndexes();
        expected.clear();
        actual.clear();
        for (Object[] query : queries)
        {
            expected.add(query[0] + ": " + query[3]);
            actual.add(query[0] + ": " + keys("SELECT pk FROM %s WHERE " + query[0]));
        }
        assertThat(actual).as("after a rebuild").isEqualTo(expected);
    }

    /**
     * @return the provided results of queries, without the queries that need an index (ORDER BY, LIKE)
     */
    private static Map<String, Object> unindexable(Map<String, Object> results)
    {
        Map<String, Object> filtered = new LinkedHashMap<>(results);
        filtered.keySet().removeIf(query -> query.contains("ORDER BY") || query.contains(" LIKE "));
        return filtered;
    }

    /**
     * DESC clustering columns. Apache stores bigint, inet, varint and decimal terms unreversed, and the other types
     * reversed. The results are checked against the rows filtered here; range queries on the DESC inet, varint and
     * decimal columns (indexed unreversed, CASSANDRA-20100) also at the level of the candidate rows the indexes return,
     * before and after a rebuild in our format.
     */
    @Test
    public void testDescClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, cb bigint, ci inet, cv varint, cd decimal, cn int, ct text, v int, " +
                    "PRIMARY KEY (pk, cb, ci, cv, cd, cn, ct)) " +
                    "WITH CLUSTERING ORDER BY (cb DESC, ci DESC, cv DESC, cd DESC, cn DESC, ct DESC)");
        disableCompaction();
        String[] indexes = { "descs_cb", "descs_ci", "descs_cv", "descs_cd", "descs_cn", "descs_ct", "descs_v" };
        for (String index : indexes)
            createIndex("CREATE INDEX " + index + " ON %s(" + index.substring(6) + ") USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("descs");
        assertReadFromApacheComponents(indexes);
        assertMultiSegment("descs_cb", "descs_ci", "descs_cv", "descs_cd", "descs_cn");

        List<DescRow> rows = descRows();
        assertThat(rows).hasSize(1000);
        long cb = rows.get(123).cb;
        String select = "SELECT pk, cb, ci, cv, cd, cn, ct FROM %s WHERE ";
        assertDescQuery(select + "cb = " + cb, rows, r -> r.cb == cb);
        assertDescQuery(select + "cb > " + cb, rows, r -> r.cb > cb);
        assertDescQuery(select + "cb <= " + cb, rows, r -> r.cb <= cb);
        assertDescQuery(select + "cb > -100000000700 AND cb < 100000000700", rows, r -> r.cb > -100000000700L && r.cb < 100000000700L);
        assertDescQuery(select + "cn = 7", rows, r -> r.cn == 7);
        assertDescQuery(select + "cn > 400", rows, r -> r.cn > 400);
        assertDescQuery(select + "cn >= -10 AND cn < 10", rows, r -> r.cn >= -10 && r.cn < 10);
        assertDescQuery(select + "ct = 'ct3'", rows, r -> r.ct.equals("ct3"));
        InetAddress ip4 = InetAddress.getByName("10.1.3.4");
        assertDescQuery(select + "ci = '10.1.3.4'", rows, r -> r.ci.equals(ip4));
        InetAddress ip6 = InetAddress.getByName("2001::50b");
        assertDescQuery(select + "ci = '2001::50b'", rows, r -> r.ci.equals(ip6));
        BigInteger cv = rows.get(321).cv;
        assertDescQuery(select + "cv = " + cv, rows, r -> r.cv.equals(cv));
        BigDecimal cd = rows.get(222).cd;
        assertDescQuery(select + "cd = " + cd, rows, r -> r.cd.compareTo(cd) == 0);
        assertDescQuery(select + "cb > " + cb + " AND cn < 300 AND v = 3", rows, r -> r.cb > cb && r.cn < 300 && r.v == 3);

        // The candidate rows the Apache indexes return for ranges, before any post-filtering. Varints and decimals are
        // truncated in the index, so their candidates can include more.
        InetAddress ci = InetAddress.getByName("10.1.10.0");
        assertCandidates("descs_ci", Operator.GT, InetAddressType.instance.decompose(ci), rows, r -> compareInet(r.ci, ci) > 0, true);
        assertCandidates("descs_ci", Operator.LTE, InetAddressType.instance.decompose(ci), rows, r -> compareInet(r.ci, ci) <= 0, true);
        InetAddress ci6 = InetAddress.getByName("2001::50b");
        assertCandidates("descs_ci", Operator.GTE, InetAddressType.instance.decompose(ci6), rows, r -> compareInet(r.ci, ci6) >= 0, true);
        assertCandidates("descs_ci", Operator.LT, InetAddressType.instance.decompose(ci6), rows, r -> compareInet(r.ci, ci6) < 0, true);
        for (BigInteger bound : new BigInteger[]{ BigInteger.valueOf(1000000), BigInteger.valueOf(-100), BigInteger.TEN.pow(20).negate() })
        {
            assertCandidates("descs_cv", Operator.GT, IntegerType.instance.decompose(bound), rows, r -> r.cv.compareTo(bound) > 0, false);
            assertCandidates("descs_cv", Operator.LTE, IntegerType.instance.decompose(bound), rows, r -> r.cv.compareTo(bound) <= 0, false);
        }
        for (BigDecimal bound : new BigDecimal[]{ BigDecimal.valueOf(-1), new BigDecimal("10.5"), new BigDecimal("-123.45") })
        {
            assertCandidates("descs_cd", Operator.LT, DecimalType.instance.decompose(bound), rows, r -> r.cd.compareTo(bound) < 0, false);
            assertCandidates("descs_cd", Operator.GTE, DecimalType.instance.decompose(bound), rows, r -> r.cd.compareTo(bound) >= 0, false);
        }
        assertCandidates("descs_cb", Operator.GT, LongType.instance.decompose(cb), rows, r -> r.cb > cb, true);
        assertCandidates("descs_cb", Operator.LTE, LongType.instance.decompose(cb), rows, r -> r.cb <= cb, true);
        assertCandidates("descs_cn", Operator.LT, Int32Type.instance.decompose(-3), rows, r -> r.cn < -3, true);
        assertCandidates("descs_cn", Operator.GTE, Int32Type.instance.decompose(-3), rows, r -> r.cn >= -3, true);

        // Range queries on the DESC inet, varint and decimal columns (indexed unreversed, CASSANDRA-20100), from the
        // Apache components and then from ours, after a rebuild.
        Map<String, Predicate<DescRow>> ranges = new LinkedHashMap<>();
        ranges.put(select + "ci > '10.1.10.0'", r -> compareInet(r.ci, ci) > 0);
        ranges.put(select + "ci <= '10.1.10.0'", r -> compareInet(r.ci, ci) <= 0);
        ranges.put(select + "ci >= '2001::50b'", r -> compareInet(r.ci, ci6) >= 0);
        ranges.put(select + "ci >= '10.1.3.4' AND ci < '10.1.10.0'", r -> compareInet(r.ci, ip4) >= 0 && compareInet(r.ci, ci) < 0);
        for (BigInteger bound : new BigInteger[]{ BigInteger.valueOf(1000000), BigInteger.valueOf(-100), BigInteger.TEN.pow(20).negate() })
        {
            ranges.put(select + "cv > " + bound, r -> r.cv.compareTo(bound) > 0);
            ranges.put(select + "cv <= " + bound, r -> r.cv.compareTo(bound) <= 0);
        }
        ranges.put(select + "cv > -100 AND cv <= 1000000", r -> r.cv.compareTo(BigInteger.valueOf(-100)) > 0 && r.cv.compareTo(BigInteger.valueOf(1000000)) <= 0);
        for (BigDecimal bound : new BigDecimal[]{ BigDecimal.valueOf(-1), new BigDecimal("10.5"), new BigDecimal("-123.45") })
        {
            ranges.put(select + "cd < " + bound, r -> r.cd.compareTo(bound) < 0);
            ranges.put(select + "cd >= " + bound, r -> r.cd.compareTo(bound) >= 0);
        }
        ranges.put(select + "cd >= -123.45 AND cd < 10.5", r -> r.cd.compareTo(new BigDecimal("-123.45")) >= 0 && r.cd.compareTo(new BigDecimal("10.5")) < 0);
        ranges.put(select + "cb > " + cb + " AND ci > '10.1.10.0' AND cv > -100 AND cd < 10.5",
                   r -> r.cb > cb && compareInet(r.ci, ci) > 0 && r.cv.compareTo(BigInteger.valueOf(-100)) > 0 && r.cd.compareTo(new BigDecimal("10.5")) < 0);
        ranges.forEach((query, filter) -> assertDescQuery(query, rows, filter));

        rebuildIndexes(indexes);
        waitForTableIndexesQueryable();
        assertNoApacheComponents(indexes);
        ranges.forEach((query, filter) -> assertDescQuery(query, rows, filter));
        assertDescQuery(select + "cd = " + cd, rows, r -> r.cd.compareTo(cd) == 0);
        assertDescQuery(select + "cv = " + cv, rows, r -> r.cv.equals(cv));
    }

    /**
     * Checks the rows the index returns for the expression, before any filtering, are those of the provided rows that
     * the filter accepts (or include them, if not exact).
     */
    private void assertCandidates(String index, Operator operator, ByteBuffer value, List<DescRow> rows, Predicate<DescRow> filter, boolean exact) throws Exception
    {
        IndexContext context = getIndexContext(index);
        Expression expression = new Expression(context).add(operator, value);
        AbstractBounds<PartitionPosition> all = DataRange.allData(getCurrentColumnFamilyStore().getPartitioner()).keyRange();
        Set<String> candidates = new HashSet<>();
        for (SSTableIndex sstableIndex : context.getView().getIndexes())
        {
            try (KeyRangeIterator keys = sstableIndex.search(expression, all, new QueryContext(), false))
            {
                while (keys.hasNext())
                {
                    // the partition key and first clustering column identify a row (the varint and decimal
                    // clustering values decoded from the keys are equal to the written ones, but not byte for byte)
                    PrimaryKey key = keys.next();
                    candidates.add(ByteBufferUtil.bytesToHex(key.partitionKey().getKey()) + '/' + ByteBufferUtil.bytesToHex(key.clustering().bufferAt(0)));
                }
            }
        }
        Set<String> expected = rows.stream().filter(filter).map(r -> r.key).collect(Collectors.toSet());
        String description = index + ' ' + operator + ' ' + ByteBufferUtil.bytesToHex(value);
        Set<String> missing = new HashSet<>(expected);
        missing.removeAll(candidates);
        assertThat(missing).as(description + " (missing candidates)").isEmpty();
        if (exact)
        {
            Set<String> extra = new HashSet<>(candidates);
            extra.removeAll(expected);
            assertThat(extra).as(description + " (extra candidates)").isEmpty();
        }
    }

    private static int compareInet(InetAddress a, InetAddress b)
    {
        return ByteBuffer.wrap(ipv6(a)).compareTo(ByteBuffer.wrap(ipv6(b)));
    }

    private static byte[] ipv6(InetAddress address)
    {
        byte[] bytes = address.getAddress();
        if (bytes.length == 16)
            return bytes;
        byte[] mapped = new byte[16];
        mapped[10] = (byte) 0xff;
        mapped[11] = (byte) 0xff;
        System.arraycopy(bytes, 0, mapped, 12, 4);
        return mapped;
    }

    @Test
    public void testAnalyzers() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, t_ci text, t_norm text, t_ascii text, t_all text, a_ci ascii)");
        disableCompaction();
        createIndex("CREATE INDEX analyzed_ci ON %s(t_ci) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}");
        createIndex("CREATE INDEX analyzed_norm ON %s(t_norm) USING 'sai' WITH OPTIONS = {'normalize': 'true'}");
        createIndex("CREATE INDEX analyzed_ascii ON %s(t_ascii) USING 'sai' WITH OPTIONS = {'ascii': 'true'}");
        createIndex("CREATE INDEX analyzed_all ON %s(t_all) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'}");
        createIndex("CREATE INDEX analyzed_aci ON %s(a_ci) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}");
        waitForTableIndexesQueryable();
        loadApacheSSTables("analyzed");
        String[] indexes = { "analyzed_ci", "analyzed_norm", "analyzed_ascii", "analyzed_all", "analyzed_aci" };
        assertReadFromApacheComponents(indexes);

        // The values are "Alpha", "alpha", "ALPHA", "Ålpha" (composed), "Ålpha" (decomposed), "Beta", "béta" (composed),
        // "béta" (decomposed), "Gamma", "gamma ray" and "", cycling over the 200 rows.
        assertThat(execute("SELECT pk FROM %s WHERE t_ci = 'alpha'")).hasSize(56);
        assertThat(execute("SELECT pk FROM %s WHERE t_all = 'ALPHA'")).hasSize(92);
        assertSameResultsAfterRebuild(List.of("SELECT pk FROM %s WHERE t_ci = 'alpha'",
                                              "SELECT pk FROM %s WHERE t_ci = 'GAMMA RAY'",
                                              "SELECT pk FROM %s WHERE t_ci = ''",
                                              "SELECT pk FROM %s WHERE t_norm = 'Ålpha'",
                                              "SELECT pk FROM %s WHERE t_norm = 'béta'",
                                              "SELECT pk FROM %s WHERE t_ascii = 'Alpha'",
                                              "SELECT pk FROM %s WHERE t_ascii = 'beta'",
                                              "SELECT pk FROM %s WHERE t_all = 'ALPHA'",
                                              "SELECT pk FROM %s WHERE t_all = 'BETA'",
                                              "SELECT pk FROM %s WHERE a_ci = 'abc3'",
                                              "SELECT pk FROM %s WHERE t_ci = 'beta' AND t_ascii = 'beta'"),
                                      indexes);
    }

    @Test
    public void testCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, l list<int>, st set<text>, m map<text, int>, fm frozen<map<text, int>>, fl frozen<list<text>>)");
        disableCompaction();
        createIndex("CREATE INDEX colls_l ON %s(l) USING 'sai'");
        createIndex("CREATE INDEX colls_st ON %s(st) USING 'sai'");
        createIndex("CREATE INDEX colls_mk ON %s(KEYS(m)) USING 'sai'");
        createIndex("CREATE INDEX colls_mv ON %s(VALUES(m)) USING 'sai'");
        createIndex("CREATE INDEX colls_me ON %s(ENTRIES(m)) USING 'sai'");
        createIndex("CREATE INDEX colls_fm ON %s(FULL(fm)) USING 'sai'");
        createIndex("CREATE INDEX colls_fl ON %s(FULL(fl)) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("colls");
        String[] indexes = { "colls_l", "colls_st", "colls_mk", "colls_mv", "colls_me", "colls_fm", "colls_fl" };
        assertReadFromApacheComponents(indexes);

        Map<String, Object> results = assertSameResultsAfterRebuild(List.of("SELECT pk FROM %s WHERE l CONTAINS 3",
                                              "SELECT pk FROM %s WHERE l CONTAINS -4",
                                              "SELECT pk FROM %s WHERE l CONTAINS 3 AND l CONTAINS 0",
                                              "SELECT pk FROM %s WHERE st CONTAINS 's2'",
                                              "SELECT pk FROM %s WHERE st CONTAINS 'x1' AND st CONTAINS 's5'",
                                              "SELECT pk FROM %s WHERE m CONTAINS KEY 'k3'",
                                              "SELECT pk FROM %s WHERE m CONTAINS KEY 'z0' AND m CONTAINS KEY 'k4'",
                                              "SELECT pk FROM %s WHERE m CONTAINS 7",
                                              "SELECT pk FROM %s WHERE m CONTAINS -150",
                                              "SELECT pk FROM %s WHERE m['k5'] = 13",
                                              "SELECT pk FROM %s WHERE m['z1'] = -100",
                                              "SELECT pk FROM %s WHERE m['k2'] > 20",
                                              "SELECT pk FROM %s WHERE m['k2'] >= 10 AND m['k2'] < 20",
                                              "SELECT pk FROM %s WHERE m['z2'] < -290",
                                              "SELECT pk FROM %s WHERE fm = {'f1': 1}",
                                              "SELECT pk FROM %s WHERE fl = ['l3', 'm1']",
                                              "SELECT pk FROM %s WHERE fl = ['l3', 'm0']",
                                              "SELECT pk FROM %s WHERE l CONTAINS 1 AND m CONTAINS KEY 'k1'",
                                              "SELECT pk FROM %s WHERE st NOT CONTAINS 's2'",
                                              "SELECT pk FROM %s WHERE m NOT CONTAINS KEY 'k3'"),
                                      indexes);
        assertSameResultsAsUnindexed("colls", "CREATE TABLE %s (pk int PRIMARY KEY, l list<int>, st set<text>, m map<text, int>, fm frozen<map<text, int>>, fl frozen<list<text>>)", results);
    }

    @Test
    public void testStaticColumnsAndPartitionKeyComponents() throws Throwable
    {
        createTable(STATICS);
        disableCompaction();
        String[] indexes = { "statics_pk1", "statics_pk2", "statics_ck", "statics_st", "statics_sti", "statics_v" };
        for (String index : indexes)
            createIndex("CREATE INDEX " + index + " ON %s(" + index.substring(8) + ") USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("statics");
        assertReadFromApacheComponents(indexes);

        // Checked against filtering without indexes: our own indexes on partition key components miss the partitions
        // that only have a static row (both in the ec and ga formats), which Apache's include.
        assertSameResultsAsUnindexed("statics",
                                     STATICS,
                                     List.of("SELECT pk1, pk2, ck FROM %s WHERE pk1 = 3",
                                              "SELECT pk1, pk2, ck FROM %s WHERE pk1 > 7",
                                              "SELECT pk1, pk2, ck FROM %s WHERE pk2 = 'p4'",
                                              "SELECT pk1, pk2, ck FROM %s WHERE pk1 = 2 AND pk2 = 'p0'",
                                              "SELECT pk1, pk2, ck FROM %s WHERE ck = 2",
                                              "SELECT pk1, pk2, ck FROM %s WHERE ck > 2 AND pk1 < 3",
                                              "SELECT pk1, pk2, ck, st FROM %s WHERE st = 'st1'",
                                              "SELECT pk1, pk2, ck, sti FROM %s WHERE sti > 3",
                                              "SELECT pk1, pk2, st FROM %s WHERE st = 'st2' PER PARTITION LIMIT 1",
                                              "SELECT pk1, pk2, ck FROM %s WHERE sti = 0 AND v > 20",
                                              "SELECT pk1, pk2, ck FROM %s WHERE st = 'st3' AND ck = 4",
                                              "SELECT pk1, pk2, ck FROM %s WHERE v = 5",
                                              "SELECT pk1, pk2, ck FROM %s WHERE v >= 30 AND pk2 = 'p9'",
                                              "SELECT pk1, pk2, ck, sti FROM %s WHERE sti < 2 LIMIT 7"));
    }

    /**
     * Apache's vector indexes (jvector 1.0.2 graphs, with product quantization, and several segments) find the same
     * neighbours as brute force, as well as ours do after a rebuild.
     */
    @Test
    public void testVectors() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v vector<float, 8>, cat int)");
        disableCompaction();
        createIndex("CREATE INDEX vectors_v ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX vectors_cat ON %s(cat) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("vectors");
        assertReadFromApacheComponents("vectors_v", "vectors_cat");
        assertMultiSegment("vectors_v");
        // both sstables have product quantization (Apache's CompressedVectors component holds more than a "no PQ" flag)
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            assertThat(group.descriptorFor(sstable).perIndexComponents(getIndexContext("vectors_v")).get(IndexComponentType.PQ).file().length()).isGreaterThan(1000);

        List<float[]> vectors = vectors();
        assertThat(vectors).hasSize(3800);
        double recall = annRecall(vectors, new Random(7));
        assertThat(recall).isGreaterThan(0.9);

        rebuildIndexes("vectors_v", "vectors_cat");
        waitForTableIndexesQueryable();
        assertNoApacheComponents("vectors_v", "vectors_cat");
        assertThat(annRecall(vectors, new Random(7))).isGreaterThan(0.9);
    }

    private double annRecall(List<float[]> vectors, Random random)
    {
        int found = 0, expected = 0;
        for (int q = 0; q < 20; q++)
        {
            float[] query = new float[8];
            for (int i = 0; i < query.length; i++)
                query[i] = random.nextFloat() * 2 - 1;
            query[0] += q % 5;
            int cat = q % 10;
            boolean filtered = q % 2 == 0;
            String cql = "SELECT pk FROM %s " + (filtered ? "WHERE cat = " + cat + ' ' : "") +
                         "ORDER BY v ANN OF " + vectorLiteral(query) + " LIMIT 10";
            Set<Integer> actual = new HashSet<>();
            for (UntypedResultSet.Row row : execute(cql))
                actual.add(row.getInt("pk"));
            List<Integer> exact = new ArrayList<>();
            for (int pk = 0; pk < vectors.size(); pk++)
                if (!filtered || pk % 10 == cat)
                    exact.add(pk);
            exact.sort(Comparator.comparingDouble(pk -> -cosine(query, vectors.get(pk))));
            assertThat(actual).hasSize(10);
            for (int pk : exact.subList(0, 10))
            {
                expected++;
                if (actual.contains(pk))
                    found++;
            }
        }
        return (double) found / expected;
    }

    /**
     * Some sstables have Apache's components and some ours: queries over both, then after compacting them all into
     * our format.
     */
    @Test
    public void testMixedFormatsAndCompaction() throws Throwable
    {
        createTable(WIDE);
        disableCompaction();
        createIndex("CREATE INDEX wide_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX wide_n ON %s(n) USING 'sai'");
        createIndex("CREATE INDEX wide_st ON %s(st) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("wide");
        assertReadFromApacheComponents("wide_s", "wide_n", "wide_st");

        // Our sstables: new partitions, rows overwriting and deleting Apache's, and a memtable on top of that.
        for (int pk = 100; pk < 130; pk++)
            for (int ck = 0; ck < 4; ck++)
                execute("INSERT INTO %s (pk, ck, st, s, n) VALUES (?, ?, ?, ?, ?)", pk, ck, pk % 5, "w" + (ck % 3), (long) (pk * 10 + ck));
        execute("UPDATE %s SET s = 'w9', n = 424242 WHERE pk = 42 AND ck = 3");
        execute("DELETE FROM %s WHERE pk = 43 AND ck = 1");
        execute("UPDATE %s SET st = 7 WHERE pk = 44");
        flush();
        execute("INSERT INTO %s (pk, ck, st, s, n) VALUES (?, ?, ?, ?, ?)", 1000, 0, 2, "w1", 5L);
        execute("DELETE FROM %s WHERE pk = 45");

        List<String> queries = List.of("SELECT pk, ck FROM %s WHERE s = 'w1'",
                                       "SELECT pk, ck FROM %s WHERE s = 'w9'",
                                       "SELECT pk, ck FROM %s WHERE n = 423",
                                       "SELECT pk, ck FROM %s WHERE n = 424242",
                                       "SELECT pk, ck FROM %s WHERE n > 1000 AND n < 1200",
                                       "SELECT pk, ck FROM %s WHERE n < 10",
                                       "SELECT pk, ck FROM %s WHERE s = 'w1' AND n > 400 AND n < 500",
                                       "SELECT pk, ck FROM %s WHERE st = 2",
                                       "SELECT pk FROM %s WHERE st = 7 PER PARTITION LIMIT 1",
                                       "SELECT pk, ck FROM %s WHERE st = 2 AND s = 'w0'",
                                       "SELECT pk, ck, n FROM %s ORDER BY n LIMIT 6",
                                       "SELECT pk, ck, n FROM %s ORDER BY n DESC LIMIT 6",
                                       "SELECT pk, ck FROM %s WHERE s = 'w2' LIMIT 11");
        List<Object> expected = new ArrayList<>(runQueries(queries).values());
        assertThat(execute("SELECT pk, ck FROM %s WHERE s = 'w9'")).hasSize(1);
        assertThat(execute("SELECT pk, ck FROM %s WHERE pk = 45")).isEmpty();
        assertThat(execute("SELECT pk FROM %s WHERE st = 7 PER PARTITION LIMIT 1")).hasSize(1);

        // paging over both formats
        assertThat(executeNetWithPaging("SELECT pk, ck FROM %s WHERE s = 'w1'", 7).all()).hasSize(((List<?>) expected.get(0)).size());
        assertThat(executeNetWithPaging("SELECT pk, ck FROM %s WHERE n > 100 AND n < 700", 5).all()).hasSize(execute("SELECT pk, ck FROM %s WHERE n > 100 AND n < 700").size());

        // the same rows from our components only
        flush();
        compact();
        waitForCompactions();
        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1);
        assertNoApacheComponents("wide_s", "wide_n", "wide_st");
        assertThat(new ArrayList<>(runQueries(queries).values())).isEqualTo(expected);
    }

    /**
     * Intersections over an sstable whose indexes use Apache's components and one whose indexes use ours, whose key
     * encodings differ, checked against filtering the same data without indexes.
     */
    @Test
    public void testIntersectionsOverMixedFormats() throws Throwable
    {
        String create = WIDE;
        createTable(create);
        disableCompaction();
        String[] indexes = { "wide_s", "wide_n", "wide_st" };
        for (String index : indexes)
            createIndex("CREATE INDEX " + index + " ON %s(" + index.substring(5) + ") USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("wide");
        assertReadFromApacheComponents(indexes);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader rebuilt = cfs.getLiveSSTables().iterator().next();
        cfs.indexManager.buildIndexesBlocking(Set.of(rebuilt), new HashSet<>(cfs.indexManager.listIndexes()), true);
        waitForTableIndexesQueryable();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertThat(group.descriptorFor(sstable).perIndexComponents(getIndexContext("wide_n")).version())
            .isEqualTo(sstable.equals(rebuilt) ? Version.current(KEYSPACE) : Version.APACHE_AA);

        // with the query optimizer (which, on so little data, mostly picks a single index), and without
        Map<String, Object> results = runQueries(List.of("SELECT pk, ck FROM %s WHERE s = 'w1' AND n < 500",
                                                         "SELECT pk, ck FROM %s WHERE s = 'w1' AND n > 500",
                                                         "SELECT pk, ck FROM %s WHERE n < 500 AND s = 'w2'",
                                                         "SELECT pk, ck FROM %s WHERE s = 'w0' AND n > 100 AND n < 900",
                                                         "SELECT pk, ck FROM %s WHERE st = 2 AND s = 'w1'",
                                                         "SELECT pk, ck FROM %s WHERE st = 2 AND n > 300",
                                                         "SELECT pk, ck FROM %s WHERE s = 'w1' OR n = 423"));
        CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.setInt(0);
        try
        {
            assertThat(runQueries(new ArrayList<>(results.keySet()))).isEqualTo(results);
        }
        finally
        {
            CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.setInt(1);
        }
        assertSameResultsAsUnindexed("wide", create, results);
    }

    /**
     * A restart reads the Apache components again without rebuilding them, and so does validation (as on a startup,
     * streaming, or nodetool verify).
     */
    @Test
    public void testRestartValidationAndSnapshot() throws Throwable
    {
        createTable(TYPES);
        disableCompaction();
        createIndex("CREATE INDEX types_t ON %s(t) USING 'sai'");
        createIndex("CREATE INDEX types_bi ON %s(bi) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("types");
        assertReadFromApacheComponents("types_t", "types_bi");
        Set<String> apacheFiles = apacheFiles();

        simulateNodeRestart();
        assertReadFromApacheComponents("types_t", "types_bi");
        reloadSSTableIndexInPlace();
        assertReadFromApacheComponents("types_t", "types_bi");
        assertThat(apacheFiles()).isEqualTo(apacheFiles);

        // checksums, including per segment
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertThat(group.validateSSTableAttachedIndexes(cfs.getLiveSSTables(), true, true)).isTrue();

        // a snapshot carries them
        TableSnapshot snapshot = cfs.snapshot("apache");
        Set<String> snapshotFiles = new HashSet<>();
        for (File directory : snapshot.getDirectories())
            Arrays.stream(directory.tryList()).map(File::name).forEach(snapshotFiles::add);
        assertThat(snapshotFiles).containsAll(apacheFiles.stream().filter(f -> !f.contains("PartitionToSize")).collect(Collectors.toSet()));

        assertThat(execute("SELECT pk FROM %s WHERE t = 't7'")).isNotEmpty();
        assertThat(execute("SELECT pk FROM %s WHERE bi > 0 AND bi < 300000000")).isNotEmpty();
    }

    /**
     * When an Apache component turns out to be corrupted, the indexes of its sstable are rebuilt in our format, once.
     */
    @Test
    public void testCorruptedComponentIsRebuilt() throws Throwable
    {
        createTable(WIDE);
        disableCompaction();
        createIndex("CREATE INDEX wide_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX wide_n ON %s(n) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("wide");
        assertReadFromApacheComponents("wide_s", "wide_n");
        List<String> queries = List.of("SELECT pk, ck FROM %s WHERE s = 'w1'", "SELECT pk, ck FROM %s WHERE n > 900");
        var before = runQueries(queries);

        // break the footer of the terms of wide_s on one sstable
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        SSTableReader corrupted = cfs.getLiveSSTables().iterator().next();
        IndexComponents.ForRead components = group.descriptorFor(corrupted).perIndexComponents(getIndexContext("wide_s"));
        breakFooter(components.get(IndexComponentType.TERMS_DATA).file());

        // validation (as on a restart) finds it, and the initial build rebuilds that sstable
        reloadSSTableIndexInPlace();
        simulateNodeRestart();
        waitForTableIndexesQueryable();
        assertThat(runQueries(queries)).isEqualTo(before);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            Version expected = sstable.equals(corrupted) ? Version.current(KEYSPACE) : Version.APACHE_AA;
            assertThat(group.descriptorFor(sstable).perSSTableComponents().version()).isEqualTo(expected);
            assertThat(group.descriptorFor(sstable).perIndexComponents(getIndexContext("wide_s")).version()).isEqualTo(expected);
            assertThat(group.descriptorFor(sstable).perIndexComponents(getIndexContext("wide_n")).version()).isEqualTo(expected);
        }

        // and nothing gets rebuilt again
        var indexes = getIndexContext("wide_s").getView().getIndexes().stream().map(SSTableIndex::getSSTable).collect(Collectors.toSet());
        simulateNodeRestart();
        reloadSSTableIndexInPlace();
        simulateNodeRestart();
        assertThat(getIndexContext("wide_s").getView().getIndexes().stream().map(SSTableIndex::getSSTable).collect(Collectors.toSet())).isEqualTo(indexes);
        assertThat(runQueries(queries)).isEqualTo(before);
    }

    /**
     * Same as {@link #testCorruptedComponentIsRebuilt}, with a corrupted Apache per-SSTable component, which the
     * segment metadata of every Apache index of the sstable reads.
     */
    @Test
    public void testCorruptedApachePerSSTableComponentIsRebuilt() throws Throwable
    {
        createTable(WIDE);
        disableCompaction();
        createIndex("CREATE INDEX wide_s ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX wide_n ON %s(n) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("wide");
        assertReadFromApacheComponents("wide_s", "wide_n");
        List<String> queries = List.of("SELECT pk, ck FROM %s WHERE s = 'w1'", "SELECT pk, ck FROM %s WHERE n > 900", "SELECT pk, ck FROM %s WHERE s = 'w1' AND n < 500");
        var before = runQueries(queries);

        // break the footer of the clustering keys of one sstable
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        SSTableReader corrupted = cfs.getLiveSSTables().iterator().next();
        IndexComponents.ForRead perSSTable = group.descriptorFor(corrupted).perSSTableComponents();
        breakFooter(perSSTable.get(IndexComponentType.CLUSTERING_KEY_BLOCKS).file());

        // opening the indexes (as on a restart) finds it, and the initial build rebuilds that sstable
        reloadSSTableIndexInPlace();
        simulateNodeRestart();
        assertThat(runQueries(queries)).isEqualTo(before);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            Version expected = sstable.equals(corrupted) ? Version.current(KEYSPACE) : Version.APACHE_AA;
            IndexDescriptor descriptor = group.descriptorFor(sstable);
            assertThat(descriptor.perSSTableComponents().version()).isEqualTo(expected);
            assertThat(descriptor.perIndexComponents(getIndexContext("wide_s")).version()).isEqualTo(expected);
            assertThat(descriptor.perIndexComponents(getIndexContext("wide_n")).version()).isEqualTo(expected);
        }
        String prefix = corrupted.descriptor.baseFile().name() + '-' + APACHE_PREFIX;
        waitForAssert(() -> assertThat(apacheFiles()).noneMatch(f -> f.startsWith(prefix)), 30, TimeUnit.SECONDS);

        simulateNodeRestart();
        assertThat(runQueries(queries)).isEqualTo(before);
    }

    /**
     * Turning the reader off (cassandra.sai.read_apache_format=false) rebuilds the indexes in our format on the next
     * start, which replaces the Apache components; turning it on again changes nothing.
     */
    @Test
    public void testReadApacheFormatProperty() throws Throwable
    {
        createTable(TYPES);
        disableCompaction();
        createIndex("CREATE INDEX types_i ON %s(i) USING 'sai'");
        createIndex("CREATE INDEX types_t ON %s(t) USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("types");
        assertReadFromApacheComponents("types_i", "types_t");
        List<String> queries = List.of("SELECT pk FROM %s WHERE i = -500", "SELECT pk FROM %s WHERE i > 400", "SELECT pk FROM %s WHERE t = 't7'");
        var before = runQueries(queries);

        CassandraRelevantProperties.SAI_READ_APACHE_FORMAT.setBoolean(false);
        try
        {
            simulateNodeRestart();
            assertThat(runQueries(queries)).isEqualTo(before);
            waitForAssert(() -> assertThat(apacheFiles()).isEmpty(), 30, TimeUnit.SECONDS);
        }
        finally
        {
            CassandraRelevantProperties.SAI_READ_APACHE_FORMAT.setBoolean(true);
        }
        assertNoApacheComponents("types_i", "types_t");

        simulateNodeRestart();
        assertNoApacheComponents("types_i", "types_t");
        assertThat(runQueries(queries)).isEqualTo(before);
    }

    /**
     * Deletions: in "tombs", the second sstable deletes and updates rows of the first in every way, which Apache
     * doesn't index where they leave no live data; checked against filtering and our rebuilt indexes.
     */
    @Test
    public void testDeletions() throws Throwable
    {
        createTable(TOMBS);
        disableCompaction();
        String[] indexes = { "tombs_s", "tombs_n", "tombs_st" };
        for (String index : indexes)
            createIndex("CREATE INDEX " + index + " ON %s(" + index.substring(6) + ") USING 'sai'");
        waitForTableIndexesQueryable();
        loadApacheSSTables("tombs");
        assertReadFromApacheComponents(indexes);

        List<String> queries = List.of("SELECT pk, ck FROM %s WHERE s = 's3'",
                                       "SELECT pk, ck FROM %s WHERE s = 's0'",
                                       "SELECT pk, ck FROM %s WHERE s = 'gone'",
                                       "SELECT pk, ck FROM %s WHERE n > 50",
                                       "SELECT pk, ck FROM %s WHERE n >= -10 AND n < 10",
                                       "SELECT pk, ck FROM %s WHERE n > 1000",
                                       "SELECT pk, ck FROM %s WHERE st CONTAINS 'a1'",
                                       "SELECT pk, ck FROM %s WHERE st CONTAINS 'b2'",
                                       "SELECT pk, ck FROM %s WHERE st NOT CONTAINS 'a1'",
                                       "SELECT pk, ck FROM %s WHERE st NOT CONTAINS 'b0' AND n > 0",
                                       "SELECT pk, ck FROM %s WHERE s = 's4' AND n < 0",
                                       "SELECT pk, ck FROM %s WHERE s = 's3' OR st CONTAINS 'a3'",
                                       "SELECT pk, ck FROM %s WHERE pk = 13 AND s = 's3'",
                                       "SELECT pk, ck FROM %s WHERE pk = 6 AND n > -100",
                                       "SELECT pk, ck FROM %s WHERE n != 5 AND pk < 3 ALLOW FILTERING");
        Map<String, Object> results = assertSameResultsAfterRebuild(queries, indexes);
        assertSameResultsAsUnindexed("tombs", TOMBS, results);
    }

    /**
     * EUCLIDEAN and DOT_PRODUCT vector indexes on vectors not of unit length, whose graphs had vectors deleted or
     * replaced ("vectors2"), find the same neighbours as brute force and no deleted row, as ours do after a rebuild.
     */
    @Test
    public void testVectorsWithDeletionsAndOtherSimilarities() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, ve vector<float, 8>, vd vector<float, 8>)");
        disableCompaction();
        createIndex("CREATE INDEX vectors2_ve ON %s(ve) USING 'sai' WITH OPTIONS = {'similarity_function': 'EUCLIDEAN'}");
        createIndex("CREATE INDEX vectors2_vd ON %s(vd) USING 'sai' WITH OPTIONS = {'similarity_function': 'DOT_PRODUCT'}");
        waitForTableIndexesQueryable();
        loadApacheSSTables("vectors2");
        assertReadFromApacheComponents("vectors2_ve", "vectors2_vd");

        Map<Integer, float[]> euclidean = vectors("ve");
        Map<Integer, float[]> dot = vectors("vd");
        // 4300 rows, minus every 10th of the first 3000, and every 20th of them from the 4th one
        assertThat(euclidean).hasSize(4300 - 300 - 150);
        assertThat(dot.keySet()).isEqualTo(euclidean.keySet());
        assertVectorQueries(euclidean, dot);

        rebuildIndexes("vectors2_ve", "vectors2_vd");
        waitForTableIndexesQueryable();
        assertNoApacheComponents("vectors2_ve", "vectors2_vd");
        assertVectorQueries(euclidean, dot);
    }

    private void assertVectorQueries(Map<Integer, float[]> euclidean, Map<Integer, float[]> dot)
    {
        Random random = new Random(3);
        int found = 0, expected = 0;
        for (int q = 0; q < 40; q++)
        {
            boolean isEuclidean = q % 2 == 0;
            float[] query = new float[8];
            for (int i = 0; i < query.length; i++)
                query[i] = (float) random.nextGaussian() * (q % 4 < 2 ? 1 : 5);
            Map<Integer, float[]> vectors = isEuclidean ? euclidean : dot;
            List<Integer> exact = bruteForce(vectors, query, 10, isEuclidean ? ApacheIndexTester::euclidean
                                                                             : ApacheIndexTester::dotProduct);
            List<Integer> actual = new ArrayList<>();
            for (UntypedResultSet.Row row : execute("SELECT pk FROM %s ORDER BY " + (isEuclidean ? "ve" : "vd") +
                                                    " ANN OF " + vectorLiteral(query) + " LIMIT 10"))
                actual.add(row.getInt("pk"));
            assertThat(actual).hasSize(10);
            assertThat(vectors.keySet()).containsAll(actual); // no deleted row
            expected += exact.size();
            found += (int) actual.stream().filter(exact::contains).count();
        }
        assertThat((double) found / expected).isGreaterThanOrEqualTo(0.95);
    }

    private void assertMultiSegment(String... indexes)
    {
        for (String index : indexes)
        {
            int maxSegments = getIndexContext(index).getView().getIndexes().stream().mapToInt(i -> i.getSegments().size()).max().orElse(0);
            assertThat(maxSegments).as("segments of %s", index).isGreaterThan(1);
        }
    }

    private List<float[]> vectors()
    {
        List<float[]> vectors = new ArrayList<>();
        UntypedResultSet rows = execute("SELECT pk, v FROM %s");
        float[][] byPk = new float[rows.size()][];
        for (UntypedResultSet.Row row : rows)
        {
            List<Float> v = row.getVector("v", FloatType.instance, 8);
            float[] array = new float[v.size()];
            for (int i = 0; i < array.length; i++)
                array[i] = v.get(i);
            byPk[row.getInt("pk")] = array;
        }
        vectors.addAll(Arrays.asList(byPk));
        return vectors;
    }

    private List<DescRow> descRows()
    {
        List<DescRow> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT pk, cb, ci, cv, cd, cn, ct, v FROM %s"))
            rows.add(new DescRow(row));
        return rows;
    }

    private void assertDescQuery(String query, List<DescRow> rows, Predicate<DescRow> filter)
    {
        Set<String> expected = rows.stream().filter(filter).map(DescRow::toString).collect(Collectors.toSet());
        Set<String> actual = new HashSet<>();
        for (UntypedResultSet.Row row : execute(query))
            actual.add(new DescRow(row).toString());
        assertThat(actual).as(query).isEqualTo(expected);
    }

    private static class DescRow
    {
        final int pk;
        final long cb;
        final InetAddress ci;
        final BigInteger cv;
        final BigDecimal cd;
        final int cn;
        final String ct;
        final int v;
        // the partition key and first clustering column bytes, in hex, which identify the row
        final String key;

        DescRow(UntypedResultSet.Row row)
        {
            pk = row.getInt("pk");
            cb = row.getLong("cb");
            ci = InetAddressType.instance.compose(row.getBytes("ci"));
            cv = IntegerType.instance.compose(row.getBytes("cv"));
            cd = DecimalType.instance.compose(row.getBytes("cd"));
            cn = row.getInt("cn");
            ct = row.getString("ct");
            v = row.has("v") ? row.getInt("v") : -1;
            key = ByteBufferUtil.bytesToHex(row.getBytes("pk")) + '/' + ByteBufferUtil.bytesToHex(row.getBytes("cb"));
        }

        @Override
        public String toString()
        {
            return pk + "/" + cb + '/' + ci.getHostAddress() + '/' + cv + '/' + cd.stripTrailingZeros().toPlainString() + '/' + cn + '/' + ct;
        }
    }

    private static void breakFooter(File file) throws IOException
    {
        try (RandomAccessFile raf = new RandomAccessFile(file.toJavaIOFile(), "rw"))
        {
            raf.seek(file.length() - 16);
            raf.write(0);
        }
    }
}
