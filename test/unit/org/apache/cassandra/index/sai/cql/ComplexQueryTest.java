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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ComplexQueryTest extends SAITester
{
    @Parameterized.Parameters(name = "version={0}")
    public static List<Object> data()
    {
        return Stream.of(Version.AA, SAIUtil.currentVersion(), Version.LATEST).map(v -> new Object[]{ v}).collect(Collectors.toList());
    }

    @Parameterized.Parameter
    public Version version;

    @Before
    public void setup() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Test
    public void partialUpdateTest()
    {
        createTable("CREATE TABLE %s (pk int, c1 text, c2 text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(c1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c2) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, c1, c2) VALUES (?, ?, ?)", 1, "a", "a");
        flush();
        execute("UPDATE %s SET c1 = ? WHERE pk = ?", "b", 1);
        flush();
        execute("UPDATE %s SET c2 = ? WHERE pk = ?", "c", 1);
        flush();

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE c1 = 'b' AND c2='c'");
        assertRows(resultSet, row(1));
    }

    @Test
    public void splitRowsWithBooleanLogic()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction(KEYSPACE);

        // flush a sstable with 2 partial rows
        execute("INSERT INTO %s (pk, str_val) VALUES (3, 'A')");
        execute("INSERT INTO %s (pk, val) VALUES (1, 'A')");
        flush();

        // flush another sstable with 2 more partial rows, where PK 3 is now a complete row
        execute("INSERT INTO %s (pk, val) VALUES (3, 'A')");
        execute("INSERT INTO %s (pk, str_val) VALUES (2, 'A')");
        flush();

        // pk 3 should match
        var result = execute("SELECT pk FROM %s WHERE str_val = 'A' AND val = 'A'");
        assertRows(result, row(3));
    }

    @Test
    public void basicOrTest()
    {
        createTable("CREATE TABLE %s (pk int, a int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 2, 2);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 3, 3);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE a = 1 or a = 3");

        assertRowsIgnoringOrder(resultSet, row(1), row(3) );
    }

    @Test
    public void basicInTest()
    {
        createTable("CREATE TABLE %s (pk int, a int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 2, 2);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 3, 3);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 4, 4);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 5, 5);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE a in (1, 3, 5)");

        assertRowsIgnoringOrder(resultSet, row(1), row(3), row(5));
    }

    @Test
    public void complexQueryTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, a int, b int, c int, d int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(d) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 1, 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 2, 2, 1, 1, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 3, 3, 2, 1, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 4, 4, 2, 2, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 5, 5, 3, 2, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 6, 6, 3, 2, 2);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 7, 7, 4, 3, 2);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 8, 8, 4, 3, 3);

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE (a = 1 AND c = 1) OR (b IN (3, 4) AND d = 2)"), row(1), row(7), row(6));
            // Shows that IN with an empty list produces no rows
            assertRows(execute("SELECT pk FROM %s WHERE (a = 1 AND c = 1) OR (b IN () AND d = 2)"), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE b IN () AND d = 2"));
            assertRows(execute("SELECT pk FROM %s WHERE b NOT IN () AND d = 2"), row(7), row(6));
        });

    }

    @Test
    public void disjunctionWithClusteringKey()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, PRIMARY KEY(pk, ck))");

        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 2, 2, 2);

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 1 or ck = 2"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE a = 1 or ck = 2 ALLOW FILTERING");

        assertRowsIgnoringOrder(resultSet, row(1), row(2));
    }

    @Test
    public void disjunctionWithIndexOnClusteringKey()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 2, 2, 2);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE a = 1 or ck = 2");

        assertRowsIgnoringOrder(resultSet, row(1), row(2));
    }

    @Test
    public void complexQueryWithMultipleClusterings()
    {
        createTable("CREATE TABLE %s (pk int, ck0 int, ck1 int, a int, b int, c int, d int, e int, PRIMARY KEY(pk, ck0, ck1))");
        createIndex("CREATE CUSTOM INDEX ON %s(ck0) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ck1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(d) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(e) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 1, 1, 1, 1, 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 2, 2, 2, 2, 2, 2, 2, 2);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 3, 3, 3, 3, 3, 3, 3, 3);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 4, 4, 4, 4, 4, 4, 4, 4);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 5, 5, 5, 5, 5, 5, 5, 5);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE b = 6 AND d = 6 OR (a = 6 OR (c = 3 OR ck0 = 5))");

        assertRowsIgnoringOrder(resultSet, row(3), row(5));

        resultSet = execute("SELECT pk FROM %s WHERE ck0 = 1 AND (b = 6 AND c = 6 OR (d = 6 OR e = 6))");

        assertEquals(0 , resultSet.size());

        resultSet = execute("SELECT pk FROM %s WHERE b = 4 OR a = 3 OR c = 5");

        assertRowsIgnoringOrder(resultSet, row(3), row(4), row(5));
    }

    @Test
    public void complexQueryWithPartitionKeyRestriction()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");

        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 1, 1, 5);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 2, 2, 6);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 1, 3, 7);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 2, 4, 8);


        assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s WHERE pk = 1 AND (a = 2 OR b = 7)"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);

        UntypedResultSet resultSet = execute("SELECT pk, ck FROM %s WHERE pk = 1 AND (a = 2 OR b = 7) ALLOW FILTERING");

        assertRowsIgnoringOrder(resultSet, row(1, 2));

        assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s WHERE pk = 1 OR a = 2 OR b = 7 ALLOW FILTERING"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(String.format(StatementRestrictions.PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL, "pk"));

        // Here pk = 1 is directly under AND operation, so a simple isDisjunction check on it would not be enough
        // to reject it ;)
        assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s WHERE a = 2 OR (pk = 1 AND b = 7) ALLOW FILTERING"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(String.format(StatementRestrictions.PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL, "pk"));
    }

    @Test
    public void complexQueryWithPartitionKeyRestrictionAndIndexes()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 1, 1, 5);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 2, 2, 6);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 1, 3, 7);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 2, 4, 8);

        UntypedResultSet resultSet = execute("SELECT pk, ck FROM %s WHERE pk = 1 AND (a = 2 OR b = 7)");

        assertRowsIgnoringOrder(resultSet, row(1, 2));

        assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s WHERE pk = 1 OR a = 2 OR b = 7 ALLOW FILTERING"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(String.format(StatementRestrictions.PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL, "pk"));
    }

    @Test
    public void indexNotSupportingDisjunctionTest()
    {
        createTable("CREATE TABLE %s (pk int, a int, PRIMARY KEY(pk))");
        createIndex("CREATE INDEX ON %s(a) USING 'legacy_local_table'");

        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 2, 2);

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 1 or a = 2")).isInstanceOf(InvalidRequestException.class)
                                                                                   .hasMessage(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_DISJUNCTION);

        assertRows(execute("SELECT pk FROM %s WHERE a = 1 or a = 2 ALLOW FILTERING"), row(1), row(2));
    }

    @Test
    public void testUpdatesWithLargeIndexedValues() throws Throwable
    {
        // We use wide rows to ensure the values are in the same trie memory index shard
        createTable("CREATE TABLE %s (pk int, ck int, val text, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // Only use values greater than MAX_RECURSIVE_KEY_LENGTH, and make sure they share a prefix (otherwise we
        // don't hit the error on flush)
        var aa = "a".repeat(1000);
        var ab = "a".repeat(999) + 'b';
        execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", 0, 1, aa);
        execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", 0, 1, ab);
        execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", 0, 2, aa);

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT ck FROM %s WHERE val = ?", aa), row(2));
            assertRows(execute("SELECT ck FROM %s WHERE val = ?", ab), row(1));
        });
    }

    @Test
    public void complexQueryWithMultipleNEQ()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 1, 1, 5);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 2, 2, 6);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 3, 3, 7);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 4, 4, 8);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 5, null, null);

        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a != 2 AND b != 7"), row(1), row(4));
        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a != 2 AND a != 3"), row(1), row(4));
        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a NOT IN (2, 3)"), row(1), row(4));
        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a NOT IN (2, 3) AND b NOT IN (7, 8)"), row(1));
    }

    @Test
    public void testComplexQueryWithClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");

        // Insert data with different clustering column values but the same value for a and then do some updates
        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1, 1, 10);
        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1, 2, 10);
        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1, 3, 10);

        // Update 1,2
        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1, 2, 15);

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a = 10"), row(1), row(3));
            assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a = 15"), row(2));
        });
    }

    /**
     * A null element of a filtered (c) IN ((x), (y)) matches no row, also when an index serves a restriction of the
     * query, and the other elements are matched.
     */
    @Test
    public void testMultiColumnInWithNullElement() throws Throwable
    {
        for (String type : new String[]{ "int", "text" })
        {
            Object[] values = type.equals("int") ? new Object[]{ 0, 1, 2 } : new Object[]{ "a", "b", "c" };
            String literal = type.equals("int") ? "1" : "'b'";
            for (String indexed : new String[]{ "v", "c2" })
            {
                createTable("CREATE TABLE %s (pk int, c1 int, c2 " + type + ", v int, u int, PRIMARY KEY (pk, c1, c2))");
                createIndex("CREATE CUSTOM INDEX ON %s(" + indexed + ") USING 'StorageAttachedIndex'");
                for (int i = 0; i < 3; i++)
                    execute("INSERT INTO %s (pk, c1, c2, v, u) VALUES (?, ?, ?, ?, ?)", i, i, values[i], i, i);

                beforeAndAfterFlush(() -> {
                    // v is indexed or not, u is never indexed
                    for (String column : new String[]{ "v", "u" })
                    {
                        String select = "SELECT pk FROM %s WHERE (c2) IN ";
                        String eq = " AND " + column + " = 1 ALLOW FILTERING";
                        String gt = " AND " + column + " >= 0 ALLOW FILTERING";
                        assertRows(execute(select + "((?), (?))" + eq, null, values[1]), row(1));
                        assertRows(execute(select + "((?), (?))" + eq, values[1], null), row(1));
                        assertRowsIgnoringOrder(execute(select + "((?), (?), (?))" + gt, values[1], null, values[2]), row(1), row(2));
                        assertRows(execute(select + "((?), (?))" + gt, null, null));
                        assertRows(execute(select + "((null), (" + literal + "))" + eq), row(1));
                        assertRows(execute(select + "?" + eq, list(tuple(new Object[]{ null }), tuple(values[1]))), row(1));
                        assertRows(execute("SELECT pk FROM %s WHERE pk = 1 AND (c2) IN ((?), (?))" + eq, null, values[1]), row(1));
                    }
                    assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE (c2) IN ((?), (?), (?)) ALLOW FILTERING", null, values[0], values[2]),
                                            row(0), row(2));
                });
            }
        }
    }

    /**
     * A query restricting a non-indexed column whose type is a composite with a multi-cell map in it.
     */
    @Test
    public void compositeTypeWithMapInsideQuery()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (" +
                              "pk1 frozen<map<'CompositeType(IntegerType,SimpleDateType)', 'DynamicCompositeType(Q=>LongType,I=>ByteType,6=>LexicalUUIDType)'>>," +
                              "pk2 frozen<tuple<frozen<tuple<float>>>>," +
                              "ck1 frozen<list<frozen<map<'LexicalUUIDType', ascii>>>>," +
                              "ck2 tinyint," +
                              "r1 frozen<list<'DynamicCompositeType(X=>DecimalType,y=>TimestampType,f=>BooleanType)'>> static," +
                              "r2 'DynamicCompositeType(P=>ShortType)'," +
                              "r3 'CompositeType(FrozenType(ListType(DoubleType)),FrozenType(MapType(LongType,DoubleType)),DoubleType)'," +
                              "r4 frozen<list<frozen<list<time>>>>," +
                              "r5 'CompositeType(CompositeType(ShortType,SimpleDateType,BooleanType),CompositeType(FloatType),MapType(ByteType,TimeType))'," +
                              "r6 set<smallint>," +
                              "PRIMARY KEY ((pk1, pk2), ck1, ck2))");

        createIndex("CREATE CUSTOM INDEX ON %s (FULL(ck1)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s (FULL(pk1)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s (FULL(r4)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s (r2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s (r3) USING 'StorageAttachedIndex'");

        UntypedResultSet withMultipleColumns = execute("SELECT pk1 FROM " +
                                                       "%s " +
                                                       "WHERE r5 = 0x0010000230bd00000457f0bd31000001000000000700049f647252000000260000000200000001f300000008000001c4e14bba4b00000001260000000800003f2b300d385d00" +
                                                       " AND r3 = 0x001c00000002000000083380d171eace676900000008e153bb97fdd5c22e00006d000000030000000897c5493857999fc000000013f08cc4fad0f04d0de51cff28d4ae743d2da1c40000000857108e8c372c868400000013f0cc6bca55f0ee240b27ff12c77a7b7dc3c665000000086c07d25fcdd3403500000013f0745922bdf0ac44c9b5ffd80f025ded9a211d000008200547f5da7a43aa00" +
                                                       " AND  r2 = 0x8050000255e200 " +
                                                       " AND pk2 = ((-1.2651989E-23))" +
                                                       " ALLOW FILTERING;");

        assertRowCount(withMultipleColumns, 0);

        UntypedResultSet withoutSAI = execute("SELECT pk1 FROM " +
                                              "%s " +
                                              " WHERE r5 = 0x001c00000002000000083380d171eace676900000008e153bb97fdd5c22e00006d000000030000000897c5493857999fc000000013f08cc4fad0f04d0de51cff28d4ae743d2da1c40000000857108e8c372c868400000013f0cc6bca55f0ee240b27ff12c77a7b7dc3c665000000086c07d25fcdd3403500000013f0745922bdf0ac44c9b5ffd80f025ded9a211d000008200547f5da7a43aa00" +
                                              " ALLOW FILTERING;");

        assertRowCount(withoutSAI, 0);
    }

    /**
     * A null element of IN matches nothing on a regular or a static column, with or without an index, and on a filtered
     * primary key column; it is rejected where the restriction selects the partitions or the clustering.
     */
    @Test
    public void testInWithNullElements() throws Throwable
    {
        for (String schema : new String[]{ "CREATE TABLE %s (pk int, ck int, v int, u int, PRIMARY KEY (pk, ck))",
                                           "CREATE TABLE %s (pk int, ck int, v int static, u int static, PRIMARY KEY (pk, ck))" })
        {
            // the unindexed column u holds the same values as the indexed column v
            createTable(schema);
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
            for (int i = 0; i < 3; i++)
                execute("INSERT INTO %s (pk, ck, v, u) VALUES (?, 0, ?, ?)", i, i, i);
            execute("INSERT INTO %s (pk, ck) VALUES (3, 0)");

            beforeAndAfterFlush(() -> {
                for (String column : new String[]{ "v", "u" })
                {
                    String select = "SELECT pk FROM %s WHERE " + column;
                    String filtering = " ALLOW FILTERING";
                    assertRows(execute(select + " IN (?, ?)" + filtering, 1, null), row(1));
                    assertRows(execute(select + " IN (?, ?)" + filtering, null, null));
                    assertRows(execute(select + " IN (1, null)" + filtering), row(1));
                    assertRows(execute(select + " IN ?" + filtering, list(1, null)), row(1));
                    assertRows(execute(select + " IN (?, ?) AND pk = 1" + filtering, 1, null), row(1));
                    assertRows(execute(select + " IN (?, ?) AND pk = 2" + filtering, 1, null));
                    assertRowsIgnoringOrder(execute(select + " IN (?, ?, ?) OR " + column + " = 2" + filtering, null, 0, null), row(0), row(2));
                }
                // the index serves IN without ALLOW FILTERING
                assertRows(execute("SELECT pk FROM %s WHERE v IN (?, ?)", null, 2), row(2));
                assertRows(execute("SELECT pk FROM %s WHERE v IN (?, ?)", null, null));
            });
        }

        // On a primary key column, a null element is rejected where the restriction selects the partitions or the
        // clustering, and matches nothing where the column is filtered.
        String invalid = "Invalid null value in condition for column ";
        createTable("CREATE TABLE %s (pk int, c1 int, c2 int, v int, PRIMARY KEY (pk, c1, c2))");
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (1, 2, 2, 2)");
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT c2 FROM %s WHERE c2 IN (?, ?) ALLOW FILTERING", 1, null), row(1));
            assertRows(execute("SELECT c2 FROM %s WHERE c2 IN (?, ?) ALLOW FILTERING", null, 2), row(2));
            assertRows(execute("SELECT c2 FROM %s WHERE pk = 1 AND c2 IN (?, ?) ALLOW FILTERING", 1, null), row(1));
            assertInvalidMessage(invalid + "c1", "SELECT * FROM %s WHERE pk = 1 AND c1 IN (?, ?)", 1, null);
            assertInvalidMessage(invalid + "c2", "SELECT * FROM %s WHERE pk = 1 AND c1 = 1 AND c2 IN (?, ?)", 1, null);
        });

        createTable("CREATE TABLE %s (p1 int, p2 int, v int, PRIMARY KEY ((p1, p2)))");
        execute("INSERT INTO %s (p1, p2, v) VALUES (1, 1, 1)");
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT p1 FROM %s WHERE p1 IN (?, ?) ALLOW FILTERING", 1, null), row(1));
            assertRows(execute("SELECT p1 FROM %s WHERE p2 IN (?, ?) ALLOW FILTERING", null, 1), row(1));
            assertInvalidMessage(invalid + "p2", "SELECT * FROM %s WHERE p1 = 1 AND p2 IN (?, ?)", 1, null);
        });

        // an IN on the first clustering column selects the clustering, with or without an index on it
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        assertInvalidMessage(invalid + "ck", "SELECT * FROM %s WHERE ck IN (?, ?)", 1, null);
        assertInvalidMessage(invalid + "ck", "SELECT * FROM %s WHERE ck IN (?, ?) ALLOW FILTERING", null, 1);
        assertInvalidMessage(invalid + "ck", "SELECT * FROM %s WHERE pk = 1 AND ck IN (?, ?)", 1, null);
    }
}
