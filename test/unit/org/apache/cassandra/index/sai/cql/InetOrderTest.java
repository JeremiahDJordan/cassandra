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
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.Hex;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SAI queries on inet columns give the rows that filtering without an index gives. The column type orders inet values
 * by their bytes: a 4-byte IPv4 address and its 16-byte IPv4-mapped IPv6 address are different values, and IPv4
 * addresses sort among the IPv6 addresses by their first bytes.
 */
public class InetOrderTest extends SAITester.Versioned
{
    private static final String MAPPED_LOOPBACK = "blobAsInet(0x00000000000000000000ffff7f000001)";

    private static final String[] EDGE_ADDRESSES = {
    "00000000", "00000001", "0a000001", "20010db8", "7f000001", "ffffffff",
    "00000000000000000000000000000000", "00000000000000000000000000000001",
    "00000000000000000000fffeffffffff", "00000000000000000000ffff00000000", "00000000000000000000ffff7f000001",
    "00000000000000000000ffffffffffff", "00000000000000000001000000000000", "0a000001000000000000000000000000",
    "20010db8000000000000000000000000", "20010db8000000000000000000000001", "7f000001000000000000000000000000",
    "ffffffffffffffffffffffffffffffff"
    };

    @Test
    public void testMixedAddressForms() throws Throwable
    {
        requireNetwork();
        for (boolean indexInet : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, i inet, v int)");
            createIndex("CREATE INDEX ON %s(v) USING 'sai'");
            if (indexInet)
                createIndex("CREATE INDEX ON %s(i) USING 'sai'");

            execute("INSERT INTO %s (k, i, v) VALUES (1, '127.0.0.1', 1)");
            execute("INSERT INTO %s (k, i, v) VALUES (2, " + MAPPED_LOOPBACK + ", 1)");
            execute("INSERT INTO %s (k, i, v) VALUES (3, '2001:db8::1', 1)");
            execute("INSERT INTO %s (k, i, v) VALUES (4, '10.0.0.1', 1)");

            beforeAndAfterFlush(() -> {
                assertMatches("i < '127.0.0.1'", 2, 3, 4);
                assertMatches("i < '2001:db8::1'", 2, 4);
                assertMatches("i > '127.0.0.1'");
                assertMatches("i > '10.0.0.1'", 1, 3);
                assertMatches("i = '127.0.0.1'", 1);
                assertMatches("i = " + MAPPED_LOOPBACK, 2);
                assertMatches("i IN ('127.0.0.1', '2001:db8::1')", 1, 3);
                assertMatches("i != '127.0.0.1'", 2, 3, 4);
                assertMatches("i != " + MAPPED_LOOPBACK, 1, 3, 4);
                assertMatches("i NOT IN ('127.0.0.1', '10.0.0.1')", 2, 3);
                assertMatches("i >= " + MAPPED_LOOPBACK + " AND i <= '127.0.0.1'", 1, 2, 3, 4);
                assertMatches("i > " + MAPPED_LOOPBACK + " AND i < '127.0.0.1' AND i != '10.0.0.1'", 3);
                assertMatches("i >= " + MAPPED_LOOPBACK + " AND i <= '127.0.0.1' AND i != '127.0.0.1'", 2, 3, 4);
                assertMatches("i >= blobAsInet(0x)", 1, 2, 3, 4);
                assertMatches("i > blobAsInet(0x) AND i < '127.0.0.1'", 2, 3, 4);
                assertMatches("i < blobAsInet(0x)");
                assertMatches("i <= blobAsInet(0x)");
            });
        }
    }

    @Test
    public void testSSTablesOfBothTermRanges()
    {
        requireNetwork();
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i inet)");
        createIndex("CREATE INDEX ON %s(i) USING 'sai'");
        disableCompaction();
        execute("INSERT INTO %s (k, i) VALUES (1, '0.0.0.0')");
        flush();
        execute("INSERT INTO %s (k, i) VALUES (2, '::')");
        flush();
        execute("INSERT INTO %s (k, i) VALUES (3, blobAsInet(0x00000000000000000000ffff00000000))");
        flush();
        execute("INSERT INTO %s (k, i) VALUES (4, '2001:db8::1')");
        flush();

        String query = "SELECT k FROM %s WHERE i < '::1'";
        assertThat(keys(executeInternal(query))).containsExactlyInAnyOrder(1, 2);
        assertThat(keys(executeWithCoordinator(query))).containsExactlyInAnyOrder(1, 2);
        assertThat(keys(executeWithCoordinator(query + " LIMIT 1"))).hasSize(1).isSubsetOf(1, 2);
        assertThat(executeNetWithPaging(query, 1).all().stream().map(row -> row.getInt("k"))).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    public void testCollectionElementsOfBothForms() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<inet>, m map<inet, int>)");
        createIndex("CREATE INDEX ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX ON %s(keys(m)) USING 'sai'");
        execute("INSERT INTO %s (k, s, m) VALUES (1, {" + MAPPED_LOOPBACK + "}, {" + MAPPED_LOOPBACK + ": 1})");
        execute("INSERT INTO %s (k, s, m) VALUES (2, {'127.0.0.1'}, {'127.0.0.1': 1})");

        beforeAndAfterFlush(() -> {
            assertThat(keys(execute("SELECT k FROM %s WHERE s CONTAINS '127.0.0.1'"))).containsExactly(2);
            assertThat(keys(execute("SELECT k FROM %s WHERE s CONTAINS " + MAPPED_LOOPBACK))).containsExactly(1);
            assertThat(keys(execute("SELECT k FROM %s WHERE s NOT CONTAINS '127.0.0.1'"))).containsExactly(1);
            assertThat(keys(execute("SELECT k FROM %s WHERE s NOT CONTAINS " + MAPPED_LOOPBACK))).containsExactly(2);
            assertThat(keys(execute("SELECT k FROM %s WHERE m CONTAINS KEY '127.0.0.1'"))).containsExactly(2);
            assertThat(keys(execute("SELECT k FROM %s WHERE m NOT CONTAINS KEY '127.0.0.1'"))).containsExactly(1);
            assertThat(keys(execute("SELECT k FROM %s WHERE m NOT CONTAINS KEY " + MAPPED_LOOPBACK))).containsExactly(2);
        });
    }

    private void assertMatches(String restriction, Integer... keys)
    {
        String query = "SELECT k FROM %s WHERE v = 1 AND " + restriction;
        List<Integer> expected = Arrays.asList(keys);
        assertThat(keys(executeInternal(query + " ALLOW FILTERING"))).as(query).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(keys(executeWithCoordinator(query + " ALLOW FILTERING"))).as(query).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(executeNet(query + " ALLOW FILTERING").all().stream().map(row -> row.getInt("k")))
        .as(query).containsExactlyInAnyOrderElementsOf(expected);

        List<Integer> limited = keys(executeWithCoordinator(query + " LIMIT 1 ALLOW FILTERING"));
        assertThat(limited).as(query).hasSize(Math.min(1, keys.length));
        assertThat(expected).as(query).containsAll(limited);
    }

    private static List<Integer> keys(UntypedResultSet rows)
    {
        List<Integer> keys = new ArrayList<>();
        rows.forEach(row -> keys.add(row.getInt("k")));
        return keys;
    }

    /**
     * Compares random queries on inet values with the same queries on a table without indexes, in the memtable,
     * in one SSTable, in several SSTables and after compaction, on a replica and through the coordinator, with and
     * without a limit and paging.
     */
    @Test
    public void testRandomQueriesMatchFiltering()
    {
        requireNetwork();
        String schema = "CREATE TABLE %s (k int, c int, i inet, v int, s set<inet>, m map<int, inet>, f frozen<list<inet>>, " +
                        "PRIMARY KEY (k, c))";
        String indexed = createTable(schema);
        createIndex("CREATE INDEX ON %s(i) USING 'sai'");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX ON %s(entries(m)) USING 'sai'");
        String unindexedInet = createTable(schema);
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        String filtered = createTable(schema);

        List<String> addresses = randomAddresses();
        String[] tables = { indexed, unindexedInet, filtered };
        for (String table : tables)
            disableCompaction(KEYSPACE, table);

        writeRows(tables, addresses, 0, 10);
        checkQueries(indexed, unindexedInet, filtered, addresses);
        flush(KEYSPACE, indexed, unindexedInet, filtered);
        checkQueries(indexed, unindexedInet, filtered, addresses);
        writeRows(tables, addresses, 5, 15);
        flush(KEYSPACE, indexed, unindexedInet, filtered);
        writeRows(tables, addresses, 10, 20);
        checkQueries(indexed, unindexedInet, filtered, addresses);
        compact(KEYSPACE, indexed, unindexedInet, filtered);
        checkQueries(indexed, unindexedInet, filtered, addresses);
    }

    @Test
    public void testClusteringColumnMatchesFiltering()
    {
        requireNetwork();
        List<String> addresses = randomAddresses();
        for (String order : new String[]{ "ASC", "DESC" })
        {
            String schema = "CREATE TABLE %s (k int, c inet, v int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c " + order + ')';
            String indexed = createTable(schema);
            createIndex("CREATE INDEX ON %s(c) USING 'sai'");
            String filtered = createTable(schema);
            disableCompaction(KEYSPACE, indexed);
            disableCompaction(KEYSPACE, filtered);

            for (int v = 0; v < 80; v++)
            {
                if (v == 40)
                    flush(KEYSPACE, indexed, filtered);
                String insert = "INSERT INTO " + KEYSPACE + ".%s (k, c, v) VALUES (" + getRandom().nextIntBetween(0, 4) + ", " +
                                randomAddress(addresses) + ", " + v + ')';
                execute(String.format(insert, indexed));
                execute(String.format(insert, filtered));
            }

            for (int n = 0; n < 40; n++)
            {
                String restriction = randomInetRestriction("c", addresses);
                checkQuery(indexed, filtered, "v", restriction);
                checkQuery(indexed, filtered, "v", "k = 1 AND " + restriction);
            }
        }
    }

    private List<String> randomAddresses()
    {
        List<String> addresses = new ArrayList<>(Arrays.asList(EDGE_ADDRESSES));
        for (int n = 0; n < 20; n++)
        {
            String ipv4 = randomHex(4);
            addresses.add(ipv4);
            if (getRandom().nextBoolean())
                addresses.add("00000000000000000000ffff" + ipv4);
            if (getRandom().nextBoolean())
                addresses.add(ipv4 + (getRandom().nextBoolean() ? "000000000000000000000000" : randomHex(12)));
            addresses.add(randomHex(16));
        }
        return addresses.stream().map(hex -> "blobAsInet(0x" + hex + ')').collect(Collectors.toList());
    }

    private String randomHex(int size)
    {
        byte[] bytes = new byte[size];
        for (int b = 0; b < size; b++)
            bytes[b] = (byte) getRandom().nextIntBetween(0, 255);
        return Hex.bytesToHex(bytes);
    }

    private String randomAddress(List<String> addresses)
    {
        return addresses.get(getRandom().nextIntBetween(0, addresses.size() - 1));
    }

    private String randomAddresses(List<String> addresses, int max)
    {
        int size = getRandom().nextIntBetween(0, max);
        List<String> values = new ArrayList<>(size);
        for (int n = 0; n < size; n++)
            values.add(randomAddress(addresses));
        return String.join(", ", values);
    }

    private void writeRows(String[] tables, List<String> addresses, int firstKey, int lastKey)
    {
        for (int k = firstKey; k < lastKey; k++)
        {
            for (int c = 0; c < 4; c++)
            {
                String i = getRandom().nextIntBetween(0, 9) == 0 ? "null" : randomAddress(addresses);
                int v = getRandom().nextIntBetween(0, 1);
                String s = '{' + randomAddresses(addresses, 3) + '}';
                String m = "{0: " + randomAddress(addresses) + ", 1: " + randomAddress(addresses) + '}';
                String f = '[' + randomAddresses(addresses, 2) + ']';
                for (String table : tables)
                    execute(String.format("INSERT INTO %s.%s (k, c, i, v, s, m, f) VALUES (%d, %d, %s, %d, %s, %s, %s)",
                                          KEYSPACE, table, k, c, i, v, s, m, f));
            }
        }
    }

    private void checkQueries(String indexed, String unindexedInet, String filtered, List<String> addresses)
    {
        for (int n = 0; n < 40; n++)
        {
            String restriction = randomInetRestriction("i", addresses);
            checkQuery(indexed, filtered, "c", restriction);
            checkQuery(unindexedInet, filtered, "c", "v = 1 AND " + restriction);
        }
        for (int n = 0; n < 10; n++)
        {
            String address = randomAddress(addresses);
            checkQuery(indexed, filtered, "c", "s CONTAINS " + address);
            checkQuery(indexed, filtered, "c", "s NOT CONTAINS " + address);
            checkQuery(indexed, filtered, "c", "m[0] " + randomOperator() + ' ' + address);
            checkQuery(indexed, filtered, "c", "v = 1 AND f CONTAINS " + address);
        }
    }

    private String randomOperator()
    {
        return new String[]{ "=", "!=", "<", "<=", ">", ">=" }[getRandom().nextIntBetween(0, 5)];
    }

    private String randomInetRestriction(String column, List<String> addresses)
    {
        String a = randomAddress(addresses);
        switch (getRandom().nextIntBetween(0, 5))
        {
            case 0:
            case 1:
                return column + ' ' + randomOperator() + ' ' + a;
            case 2:
                return column + (getRandom().nextBoolean() ? " > " : " >= ") + a + " AND " +
                       column + (getRandom().nextBoolean() ? " < " : " <= ") + randomAddress(addresses);
            case 3:
                return column + " IN (" + a + ", " + randomAddress(addresses) + ')';
            case 4:
                return column + " NOT IN (" + a + ", " + randomAddress(addresses) + ')';
            default:
                return column + (getRandom().nextBoolean() ? " < " : " >= ") + a + " AND " + column + " != " + randomAddress(addresses);
        }
    }

    /**
     * Checks that the rows of the query, identified by their {@code k} and the given int column, are those of the
     * query of the table without indexes.
     */
    private void checkQuery(String table, String filteredTable, String id, String restriction)
    {
        String select = "SELECT k, " + id + " FROM " + KEYSPACE + ".%s WHERE " + restriction;
        List<List<Integer>> expected = rows(filteredTable, select, "", id, this::executeInternal);
        if (expected == null)
            return;

        String message = table + ": " + restriction;
        assertThat(rows(table, select, "", id, this::executeInternal)).as(message).isEqualTo(expected);
        assertThat(rows(table, select, "", id, this::executeWithCoordinator)).as(message).isEqualTo(expected);
        for (int limit : new int[]{ 1, 3 })
        {
            String limitClause = " LIMIT " + limit;
            List<List<Integer>> expectedLimited = expected.subList(0, Math.min(limit, expected.size()));
            assertThat(rows(table, select, limitClause, id, this::executeInternal)).as(message + limitClause).isEqualTo(expectedLimited);
            assertThat(rows(table, select, limitClause, id, this::executeWithCoordinator)).as(message + limitClause).isEqualTo(expectedLimited);
        }

        List<List<Integer>> paged = new ArrayList<>();
        executeNetWithPaging(String.format(select, table) + " ALLOW FILTERING", 2)
        .forEach(row -> paged.add(Arrays.asList(row.getInt("k"), row.getInt(id))));
        assertThat(paged).as(message + " paged").isEqualTo(expected);
    }

    /**
     * @return the rows of the query, or {@code null} if the query is rejected
     */
    private static List<List<Integer>> rows(String table, String select, String limit, String id, Function<String, UntypedResultSet> executor)
    {
        UntypedResultSet result;
        try
        {
            result = executor.apply(String.format(select, table) + limit + " ALLOW FILTERING");
        }
        catch (InvalidRequestException e)
        {
            return null;
        }
        List<List<Integer>> rows = new ArrayList<>();
        result.forEach(row -> rows.add(Arrays.asList(row.getInt("k"), row.getInt(id))));
        return rows;
    }
}
