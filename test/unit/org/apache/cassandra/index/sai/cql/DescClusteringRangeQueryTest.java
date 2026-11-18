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

package org.apache.cassandra.index.sai.cql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.junit.Assert.assertEquals;

/**
 * Range queries on SAI indexes on descending clustering columns, at every on-disk version, also with a single
 * predicate, which only the clustering column index can serve.
 */
public class DescClusteringRangeQueryTest extends SAITester.Versioned
{
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        List<Object[]> versions = new ArrayList<>();
        for (Version version : Version.ALL)
        {
            // Files at BA do not exist in production anywhere
            if (!version.equals(Version.BA))
                versions.add(new Object[]{ version });
        }
        return versions;
    }

    @After
    public void resetVersion()
    {
        SAIUtil.resetCurrentVersion();
    }

    @Test
    public void testReversedIntBetween() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c int, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai'");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 1, 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 3, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 4, 'TX')");

        runThenFlushThenCompact(() ->
        {
            ResultSet rangeRowsNet = executeNet("SELECT * FROM %s WHERE c >= 2 AND c <= 3 AND abbreviation = 'MA'");
            assertRowsNet(rangeRowsNet, row (0, 3, "MA"), row (0, 2, "MA"));
        });
    }

    @Test
    public void testReversedLongBetween() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c bigint, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai'");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 1, 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 3, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 4, 'TX')");

        runThenFlushThenCompact(() ->
        {
            ResultSet rangeRowsNet = executeNet("SELECT * FROM %s WHERE c >= 2 AND c <= 3 AND abbreviation = 'MA'");
            assertRowsNet(rangeRowsNet, row (0, 3L, "MA"), row (0, 2L, "MA"));
        });
    }

    @Test
    public void testReversedBigIntegerBetween() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c varint, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai'");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 1, 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 3, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 4, 'TX')");

        runThenFlushThenCompact(() ->
        {
            ResultSet rangeRowsNet = executeNet("SELECT * FROM %s WHERE c >= 2 AND c <= 3 AND abbreviation = 'MA'");
            assertRowsNet(rangeRowsNet, row (0, new BigInteger("3"), "MA"), row (0, new BigInteger("2"), "MA"));
        });
    }

    @Test
    public void testReversedBigDecimalBetween() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c decimal, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai'");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 1.1, 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2.1, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2.9, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 4.0, 'TX')");

        runThenFlushThenCompact(() ->
        {
            // The decimal clustering values are not returned with the scale they were written with (2.9 comes back as
            // 2.90, with or without an index), so they are compared numerically rather than with assertRowsNet.
            List<BigDecimal> values = new ArrayList<>();
            for (Row row : executeNet("SELECT * FROM %s WHERE c > 1.9 AND c < 3.0 AND abbreviation = 'MA'"))
            {
                assertEquals(0, row.getInt("p"));
                assertEquals("MA", row.getString("abbreviation"));
                values.add(row.getDecimal("c"));
            }
            assertEquals(2, values.size());
            assertEquals(0, new BigDecimal("2.9").compareTo(values.get(0)));
            assertEquals(0, new BigDecimal("2.1").compareTo(values.get(1)));
        });
    }

    @Test
    public void testReversedInetBetween() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c inet, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai'");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, '127.0.0.1', 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, '127.0.0.2', 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, '127.0.0.3', 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, '127.0.0.4', 'TX')");

        runThenFlushThenCompact(() ->
        {
            ResultSet rangeRowsNet = executeNet("SELECT * FROM %s WHERE c >= '127.0.0.2' AND c <= '127.0.0.3' AND abbreviation = 'MA'");
            assertRowsNet(rangeRowsNet, row (0, InetAddress.getByName("127.0.0.3"), "MA"), row (0, InetAddress.getByName("127.0.0.2"), "MA"));
        });
    }

    @Test
    public void testReversedIntBetweenWithAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c int, abbreviation ascii, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX clustering_test_index ON %s(c) USING 'sai'");
        createIndex("CREATE INDEX abbreviation_test_index ON %s(abbreviation) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}");

        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 1, 'CA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 2, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 3, 'MA')");
        execute("INSERT INTO %s(p, c, abbreviation) VALUES (0, 4, 'TX')");

        runThenFlushThenCompact(() ->
        {
            ResultSet rangeRowsNet = executeNet("SELECT * FROM %s WHERE c >= 2 AND c <= 3 AND abbreviation = 'MA'");
            assertRowsNet(rangeRowsNet, row (0, 3, "MA"), row (0, 2, "MA"));
        });
    }

    // The tests below only restrict the clustering column, so the query can only be served by its index.

    @Test
    public void testReversedIntIndexOnly() throws Throwable
    {
        checkIndexOnlyRanges("int", 1, 2, 3, 4);
    }

    @Test
    public void testReversedLongIndexOnly() throws Throwable
    {
        checkIndexOnlyRanges("bigint", -5L, 2L, 3L, Long.MAX_VALUE);
    }

    @Test
    public void testReversedBigIntegerIndexOnly() throws Throwable
    {
        checkIndexOnlyRanges("varint",
                            new BigInteger("-100000000000000000000"),
                            new BigInteger("2"),
                            new BigInteger("3"),
                            new BigInteger("100000000000000000000"));
    }

    @Test
    public void testReversedBigDecimalIndexOnly() throws Throwable
    {
        checkIndexOnlyRanges("decimal",
                            new BigDecimal("-1.1"),
                            new BigDecimal("2.1"),
                            new BigDecimal("2.9"),
                            new BigDecimal("400000000000000000000.5"));
    }

    @Test
    public void testReversedInetIndexOnly() throws Throwable
    {
        checkIndexOnlyRanges("inet",
                            InetAddress.getByName("10.0.0.1"),
                            InetAddress.getByName("127.0.0.2"),
                            InetAddress.getByName("127.0.0.3"),
                            InetAddress.getByName("200.0.0.4"));
    }

    /**
     * Writes the four given values, in ascending order, to a descending clustering column across two partitions, and
     * checks that range and equality queries restricting only that column find the right rows.
     */
    private void checkIndexOnlyRanges(String type, Object v1, Object v2, Object v3, Object v4) throws Throwable
    {
        createTable("CREATE TABLE %s(p int, c " + type + ", v int, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE INDEX ON %s(c) USING 'sai'");
        disableCompaction();

        execute("INSERT INTO %s(p, c, v) VALUES (0, ?, 1)", v1);
        execute("INSERT INTO %s(p, c, v) VALUES (0, ?, 2)", v2);
        execute("INSERT INTO %s(p, c, v) VALUES (1, ?, 3)", v3);
        execute("INSERT INTO %s(p, c, v) VALUES (1, ?, 4)", v4);

        runThenFlushThenCompact(() ->
        {
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c >= ? AND c <= ?", v2, v3), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c > ? AND c < ?", v1, v4), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c > ? AND c <= ?", v2, v3), row(3));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c >= ? AND c < ?", v2, v3), row(2));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c >= ?", v3), row(3), row(4));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c > ?", v3), row(4));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c <= ?", v2), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c < ?", v2), row(1));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c = ?", v2), row(2));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c = ?", v4), row(4));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c >= ? AND c <= ?", v1, v4), row(1), row(2), row(3), row(4));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE c > ? AND c < ?", v2, v3));
            assertRowsIgnoringOrder(execute("SELECT v FROM %s WHERE p = 1 AND c >= ? AND c <= ?", v2, v3), row(3));
        });
    }
}
