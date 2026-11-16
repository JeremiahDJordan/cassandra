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
package org.apache.cassandra.cql3.validation.entities;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;

/**
 * Statements mixing values of a non-frozen UDT with values of the same UDT frozen, or with tuples. Values of a
 * non-frozen UDT are handed on in their frozen form, so these statements are accepted, and give the same results
 * (types and serialized values), as in Apache Cassandra.
 */
public class FrozenAndNonFrozenUdtMixTest extends CQLTester
{
    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    @Test
    public void testCollectionLiteralSelectorsMixingFrozenAndNonFrozen() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");
        String textType = createType("CREATE TYPE %s (a text, b int)");
        String frozen = "frozen<" + type + '>';
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ", fv " + frozen + ", " +
                    "l list<" + frozen + ">, fl frozen<list<" + frozen + ">>, " +
                    "s set<" + frozen + ">, fs frozen<set<" + frozen + ">>, " +
                    "m map<int, " + frozen + ">, fm frozen<map<int, " + frozen + ">>, " +
                    "tp frozen<tuple<int, int>>, tp3 frozen<tuple<int, int, int>>, " +
                    "vt " + textType + ", ttxt frozen<tuple<text, int>>)");
        String insert = "INSERT INTO %s (k, v, fv, l, fl, s, fs, m, fm, tp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Object u12 = userType("a", 1, "b", 2);
        Object u34 = userType("a", 3, "b", 4);
        execute(insert, 0, u12, u34, list(u12), list(u34), set(u12), set(u34), map(1, u12), map(2, u34), tuple(5, 6));
        // all the frozen and non-frozen values are equal, to compare with the selection of frozen values only
        execute(insert, 1, u12, u12, list(u12), list(u12), set(u12), set(u12), map(1, u12), map(1, u12), tuple(1, 2));
        execute("UPDATE %s SET tp3 = (7, 8, 9), vt = {a: 'x', b: 1}, ttxt = ('y', 2) WHERE k IN (0, 1)");

        String function = createFunction(KEYSPACE, "int",
                                          "CREATE FUNCTION %s (x int) CALLED ON NULL INPUT RETURNS " + type + " " +
                                          "LANGUAGE java AS 'return udfContext.newReturnUDTValue().setInt(\"a\", x).setInt(\"b\", x + 1);'");

        assertRows(execute("SELECT [v, fv] FROM %s WHERE k = 0"), row(list(u12, u34)));
        assertRows(execute("SELECT [fv, v] FROM %s WHERE k = 0"), row(list(u34, u12)));
        assertRows(execute("SELECT [v, fv, v] FROM %s WHERE k = 0"), row(list(u12, u34, u12)));
        assertRows(execute("SELECT {v, fv} FROM %s WHERE k = 0"), row(set(u12, u34)));
        assertRows(execute("SELECT {fv, v} FROM %s WHERE k = 0"), row(set(u12, u34)));
        assertRows(execute("SELECT [v, (" + type + "){a: 3, b: 4}] FROM %s WHERE k = 0"), row(list(u12, u34)));
        assertRows(execute("SELECT [" + function + "(k), fv] FROM %s WHERE k = 0"), row(list(userType("a", 0, "b", 1), u34)));
        assertRows(execute("SELECT [l, fl] FROM %s WHERE k = 0"), row(list(list(u12), list(u34))));
        assertRows(execute("SELECT [fl, l] FROM %s WHERE k = 0"), row(list(list(u34), list(u12))));
        assertRows(execute("SELECT {s, fs} FROM %s WHERE k = 0"), row(set(set(u12), set(u34))));
        assertRows(execute("SELECT {fs, s} FROM %s WHERE k = 0"), row(set(set(u12), set(u34))));
        assertRows(execute("SELECT [m, fm] FROM %s WHERE k = 0"), row(list(map(1, u12), map(2, u34))));
        assertRows(execute("SELECT [fm, m] FROM %s WHERE k = 0"), row(list(map(2, u34), map(1, u12))));

        // An element of unknown type, like a column between parentheses, is expected to be of the other elements' type,
        // which may be a non-frozen UDT or collection, as in Apache Cassandra.
        assertRows(execute("SELECT [v, (v)], {v, (v)}, [l, (l)], {s, (s)} FROM %s WHERE k = 0"),
                   row(list(u12, u12), set(u12), list(list(u12), list(u12)), set(set(u12))));
        // A frozen value between parentheses is of that type too.
        assertRows(execute("SELECT [v, (fv)], {v, (fv)}, [l, (fl)] FROM %s WHERE k = 0"),
                   row(list(u12, u34), set(u12, u34), list(list(u12), list(u34))));
        // A value of another type isn't.
        assertInvalidMessage("(k) is not of the expected type", "SELECT [v, (k)] FROM %s WHERE k = 0");
        assertInvalidMessage("(k) is not of the expected type", "SELECT {v, (k)} FROM %s WHERE k = 0");
        // STAR-561 checks that such an element is of that type, and a non-frozen value isn't of a frozen type. Apache
        // Cassandra doesn't check it, and accepts this, as it accepts [v, (k)].
        assertInvalidMessage("(v) is not of the expected type", "SELECT [fv, (v)] FROM %s WHERE k = 0");

        // A UDT and a tuple of the same shape can be mixed; the element type is that of the first element.
        assertRows(execute("SELECT [v, tp] FROM %s WHERE k = 0"), row(list(u12, userType("a", 5, "b", 6))));
        assertRows(execute("SELECT [tp, v] FROM %s WHERE k = 0"), row(list(tuple(5, 6), tuple(1, 2))));
        assertRows(execute("SELECT {v, tp} FROM %s WHERE k = 0"), row(set(u12, userType("a", 5, "b", 6))));

        // The client gets the same types and serialized values as for frozen values only.
        assertSameNetResult("SELECT [v, fv] FROM %s WHERE k = 1", "SELECT [fv, fv] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [fv, v] FROM %s WHERE k = 1", "SELECT [fv, fv] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT {v, fv} FROM %s WHERE k = 1", "SELECT {fv, fv} FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [l, fl] FROM %s WHERE k = 1", "SELECT [fl, fl] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT {s, fs} FROM %s WHERE k = 1", "SELECT {fs, fs} FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [m, fm] FROM %s WHERE k = 1", "SELECT [fm, fm] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [v, (v)] FROM %s WHERE k = 1", "SELECT [fv, fv] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [v, tp] FROM %s WHERE k = 1", "SELECT [fv, fv] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [fv, tp] FROM %s WHERE k = 1", "SELECT [fv, fv] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [tp, v] FROM %s WHERE k = 1", "SELECT [tp, tp] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [tp, fv] FROM %s WHERE k = 1", "SELECT [tp, tp] FROM %s WHERE k = 1");

        // As STAR-561 intends, a later element of a wider type (a tuple with more components) gives the element type,
        // so that the client can decode all the elements. Apache Cassandra keeps the first element's type here.
        assertRows(execute("SELECT [v, tp3] FROM %s WHERE k = 0"), row(list(tuple(1, 2), tuple(7, 8, 9))));
        assertEquals(executeNet("SELECT [tp3, tp3] FROM %s WHERE k = 1").getColumnDefinitions().getType(0),
                     executeNet("SELECT [v, tp3] FROM %s WHERE k = 1").getColumnDefinitions().getType(0));

        // Elements whose types can't read each other's values are still rejected (STAR-561), including when one is
        // not frozen.
        String incompatible = "Invalid collection literal: all selectors must have the same CQL type inside collection literals";
        assertInvalidMessage(incompatible, "SELECT [v, k] FROM %s WHERE k = 0");
        assertInvalidMessage(incompatible, "SELECT [l, s] FROM %s WHERE k = 0");
        assertInvalidMessage(incompatible, "SELECT [v, vt] FROM %s WHERE k = 0");
        assertInvalidMessage(incompatible, "SELECT [v, ttxt] FROM %s WHERE k = 0");
    }

    @Test
    public void testTupleReceiversAcceptNonFrozenUdtValues() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ", fv frozen<" + type + ">, tp frozen<tuple<int, int>>, i int)");
        execute("INSERT INTO %s (k, v, fv, tp) VALUES (0, {a: 1, b: 2}, {a: 1, b: 2}, (5, 6))");

        assertRows(execute("SELECT (tuple<int, int>) v FROM %s"), row(tuple(1, 2)));
        assertRows(execute("SELECT (frozen<tuple<int, int>>) v FROM %s"), row(tuple(1, 2)));
        assertRows(execute("SELECT (tuple<int, int, int>) v FROM %s"), row(tuple(1, 2)));
        assertSameNetResult("SELECT (tuple<int, int>) v FROM %s", "SELECT (tuple<int, int>) fv FROM %s");
        assertEquals(executeNet("SELECT tp FROM %s").getColumnDefinitions().getType(0),
                     executeNet("SELECT (tuple<int, int>) v FROM %s").getColumnDefinitions().getType(0));

        String function = createFunction(KEYSPACE, "tuple<int, int>",
                                         "CREATE FUNCTION %s (x tuple<int, int>) CALLED ON NULL INPUT RETURNS int " +
                                         "LANGUAGE java AS 'return x == null ? null : x.getInt(0);'");
        assertRows(execute("SELECT " + function + "(v), " + function + "(fv), " + function + "(tp) FROM %s"), row(1, 1, 5));

        String listFunction = createFunction(KEYSPACE, "list<frozen<tuple<int, int>>>",
                                             "CREATE FUNCTION %s (x list<frozen<tuple<int, int>>>) CALLED ON NULL INPUT RETURNS int " +
                                             "LANGUAGE java AS 'return x == null ? null : x.get(0).getInt(0) + x.get(1).getInt(0);'");
        assertRows(execute("SELECT " + listFunction + "([v, tp]), " + listFunction + "([tp, v]) FROM %s"), row(6, 6));

        // A UDT receiver doesn't accept a non-frozen UDT value, and a tuple receiver with fewer components doesn't either.
        assertInvalidMessage("Cannot assign value", "SELECT (frozen<" + type + ">) v FROM %s");
        assertInvalidMessage("Cannot assign value", "SELECT (" + type + ") v FROM %s");
        assertInvalidMessage("Cannot assign value", "SELECT (tuple<int>) v FROM %s");

        // The same for a function returning the non-frozen UDT in a selection. As a term (in writes, restrictions and
        // schema) it is not accepted where a tuple is expected, as before: some terms, like the WHERE clause of a
        // materialized view, are stored as CQL text and prepared again by every node, including nodes without this
        // change. Apache Cassandra accepts these terms.
        String udtFunction = createFunction(KEYSPACE, "int",
                                            "CREATE FUNCTION %s (x int) CALLED ON NULL INPUT RETURNS " + type + " " +
                                            "LANGUAGE java AS 'return udfContext.newReturnUDTValue().setInt(\"a\", x).setInt(\"b\", x + 1);'");
        String identity = createFunction(KEYSPACE, "tuple<int, int>",
                                         "CREATE FUNCTION %s (x tuple<int, int>) CALLED ON NULL INPUT RETURNS tuple<int, int> " +
                                         "LANGUAGE java AS 'return x;'");
        assertRows(execute("SELECT (tuple<int, int>) " + udtFunction + "(1), " + function + '(' + udtFunction + "(3)) FROM %s"),
                   row(tuple(1, 2), 3));
        assertInvalidMessage("cannot be passed as argument 0",
                             "INSERT INTO %s (k, tp) VALUES (1, " + identity + '(' + udtFunction + "(1)))");
        assertInvalidMessage("cannot be passed as argument 0",
                             "INSERT INTO %s (k, i) VALUES (1, " + function + '(' + udtFunction + "(3)))");
        assertInvalidMessage("Cannot cast value", "INSERT INTO %s (k, tp) VALUES (2, (tuple<int, int>) " + udtFunction + "(5))");
        assertInvalidMessage("Cannot cast value", "INSERT INTO %s (k, fv) VALUES (3, (frozen<" + type + ">) " + udtFunction + "(1))");

        createTable("CREATE TABLE %s (k int, c frozen<tuple<int, int>>, x int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)");
        execute("INSERT INTO %s (k, c, x) VALUES (0, (1, 2), 1)");
        assertInvalidMessage("cannot be passed as argument 0",
                             "SELECT x FROM %s WHERE k = 0 AND c = " + identity + '(' + udtFunction + "(1))");
        assertInvalidMessage("Cannot cast value", "SELECT x FROM %s WHERE k = 0 AND c = (tuple<int, int>) " + udtFunction + "(1)");
        assertInvalidMessage("cannot be passed as argument 0",
                             "SELECT x FROM %s WHERE k = 0 AND c IN (" + identity + '(' + udtFunction + "(1)), (3, 4))");

        boolean viewsEnabled = DatabaseDescriptor.getMaterializedViewsEnabled();
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
        try
        {
            createTable("CREATE TABLE %s (pk frozen<tuple<int, int>>, c int, x int, PRIMARY KEY (pk, c))");
            assertInvalidMessage("cannot be passed as argument 0",
                                 "CREATE MATERIALIZED VIEW " + KEYSPACE + ".view_with_function AS SELECT * FROM %s " +
                                 "WHERE pk = " + identity + '(' + udtFunction + "(1)) AND c IS NOT NULL PRIMARY KEY (c, pk)");
        }
        finally
        {
            DatabaseDescriptor.setMaterializedViewsEnabled(viewsEnabled);
        }
    }

    /**
     * Generic native functions infer the type of a collection literal argument from its elements. When several element
     * types could be used, the first element's is, so the outcome doesn't depend on hashing. Asserting both element
     * orders catches an unordered choice in any JVM: it would make the same choice for both.
     */
    @Test
    public void testFunctionArgumentTypeFollowsElementOrder() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ", fv frozen<" + type + ">, tp frozen<tuple<int, int>>)");
        execute("INSERT INTO %s (k, v, fv, tp) VALUES (0, {a: 1, b: 2}, {a: 1, b: 2}, (5, 6))");

        assertRows(execute("SELECT toJson([tp, v]), toJson([tp, fv]) FROM %s"), row("[[5, 6], [1, 2]]", "[[5, 6], [1, 2]]"));
        // Apache Cassandra uses the tuple type here, as only the tuple type accepts both elements there; on this line a
        // frozen UDT also accepts a tuple, so the first element's type is used.
        String udtJson = "[{\"a\": 1, \"b\": 2}, {\"a\": 5, \"b\": 6}]";
        assertRows(execute("SELECT toJson([fv, tp]), toJson([v, tp]) FROM %s"), row(udtJson, udtJson));

        assertEquals(executeNet("SELECT fv FROM %s").getColumnDefinitions().getType(0),
                     executeNet("SELECT collection_min([fv, tp]) FROM %s").getColumnDefinitions().getType(0));
        assertEquals(executeNet("SELECT fv FROM %s").getColumnDefinitions().getType(0),
                     executeNet("SELECT collection_min([v, tp]) FROM %s").getColumnDefinitions().getType(0));
        assertEquals(executeNet("SELECT tp FROM %s").getColumnDefinitions().getType(0),
                     executeNet("SELECT collection_min([tp, fv]) FROM %s").getColumnDefinitions().getType(0));
        assertEquals(executeNet("SELECT tp FROM %s").getColumnDefinitions().getType(0),
                     executeNet("SELECT collection_min([tp, v]) FROM %s").getColumnDefinitions().getType(0));
    }

    /**
     * Generic native functions, like {@code toJson} or {@code collection_max}, take the type of a collection literal
     * argument from its elements. That type has frozen element types, so a literal of non-frozen values, like
     * {@code [v, v]}, isn't assignable to it, but it is accepted as the argument it was inferred from, as in Apache
     * Cassandra. A collection type with frozen elements that is declared, or taken from another argument, still doesn't
     * accept non-frozen values.
     */
    @Test
    public void testGenericFunctionsAcceptCollectionLiteralsOfNonFrozenValues() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");
        String frozen = "frozen<" + type + '>';
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ", fv " + frozen + ", " +
                    "l list<" + frozen + ">, fl frozen<list<" + frozen + ">>, s set<" + frozen + ">, fs frozen<set<" + frozen + ">>, " +
                    "m map<int, " + frozen + ">, fm frozen<map<int, " + frozen + ">>, li list<int>, fli frozen<list<int>>, " +
                    "tp frozen<tuple<int, int>>)");
        String insert = "INSERT INTO %s (k, v, fv, l, fl, s, fs, m, fm, li, fli, tp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Object u12 = userType("a", 1, "b", 2);
        Object u34 = userType("a", 3, "b", 4);
        execute(insert, 0, u12, u34, list(u12), list(u34), set(u12), set(u34), map(1, u12), map(2, u34), list(1), list(2), tuple(5, 6));
        // all the frozen and non-frozen values are equal, to compare with the same calls on frozen values only
        execute(insert, 1, u12, u12, list(u12), list(u12), set(u12), set(u12), map(1, u12), map(1, u12), list(1), list(1), tuple(1, 2));
        String mk = createFunction(KEYSPACE, "int",
                                   "CREATE FUNCTION %s (x int) CALLED ON NULL INPUT RETURNS " + type + " " +
                                   "LANGUAGE java AS 'return udfContext.newReturnUDTValue().setInt(\"a\", x).setInt(\"b\", x + 1);'");

        String json12 = "{\"a\": 1, \"b\": 2}";
        String json34 = "{\"a\": 3, \"b\": 4}";
        assertRows(execute("SELECT toJson([v, v]), toJson([v, fv]), toJson([fv, v]), toJson({v, fv}) FROM %s WHERE k = 0"),
                   row('[' + json12 + ", " + json12 + ']', '[' + json12 + ", " + json34 + ']',
                       '[' + json34 + ", " + json12 + ']', '[' + json12 + ", " + json34 + ']'));
        assertRows(execute("SELECT to_json([l, fl]), to_json({s, fs}), to_json([m, fm]), to_json([li, fli]) FROM %s WHERE k = 0"),
                   row("[[" + json12 + "], [" + json34 + "]]", "[[" + json12 + "], [" + json34 + "]]",
                       "[{\"1\": " + json12 + "}, {\"2\": " + json34 + "}]", "[[1], [2]]"));
        assertRows(execute("SELECT toJson([" + mk + "(1), " + mk + "(3)]), toJson([" + mk + "(3), v]) FROM %s WHERE k = 0"),
                   row('[' + json12 + ", " + json34 + ']', '[' + json34 + ", " + json12 + ']'));
        assertRows(execute("SELECT collection_count([v, v]), collection_count({v, fv}), collection_min([v, fv]), " +
                           "collection_max([fv, v]), collection_max([l, fl]) FROM %s WHERE k = 0"),
                   row(2, 2, u12, u34, list(u34)));
        assertRows(execute("SELECT max([v, fv]), min([fv, v]) FROM %s"), row(list(u12, u34), list(u12, u12)));
        assertRows(execute("SELECT toJson(max([v, fv])), toJson(collection_max([v, fv])) FROM %s WHERE k = 0"),
                   row('[' + json12 + ", " + json34 + ']', json34));
        assertRows(execute("SELECT mask_null([v, fv]), mask_replace([v], [fv]), mask_replace([v], fl) FROM %s WHERE k = 0"),
                   row(null, list(u34), list(u34)));

        // The client gets the same types and serialized values as for frozen values only.
        assertSameNetResult("SELECT toJson([v, fv]) FROM %s WHERE k = 1", "SELECT toJson([fv, fv]) FROM %s WHERE k = 1");
        assertSameNetResult("SELECT max([v, fv]) FROM %s WHERE k = 1", "SELECT max([fv, fv]) FROM %s WHERE k = 1");
        assertSameNetResult("SELECT max([l, fl]) FROM %s WHERE k = 1", "SELECT max([fl, fl]) FROM %s WHERE k = 1");
        assertSameNetResult("SELECT collection_max([v, fv]) FROM %s WHERE k = 1", "SELECT collection_max([fv, fv]) FROM %s WHERE k = 1");
        assertSameNetResult("SELECT collection_min({fv, v}) FROM %s WHERE k = 1", "SELECT collection_min({fv, fv}) FROM %s WHERE k = 1");
        assertSameNetResult("SELECT mask_default([v, fv]) FROM %s WHERE k = 1", "SELECT mask_default([fv, fv]) FROM %s WHERE k = 1");
        assertSameNetResult("SELECT mask_hash([v, fv]) FROM %s WHERE k = 1", "SELECT mask_hash([fv, fv]) FROM %s WHERE k = 1");

        // The same when prepared.
        PreparedStatement prepared = sessionNet().prepare(formatQuery("SELECT toJson([v, fv]), collection_max([v, fv]) FROM %s WHERE k = ?"));
        Row row = sessionNet().execute(prepared.bind(0)).one();
        assertEquals('[' + json12 + ", " + json34 + ']', row.getString(0));
        assertEquals(executeNet("SELECT fv FROM %s WHERE k = 0").getColumnDefinitions().getType(0), row.getColumnDefinitions().getType(1));

        // A tuple type has frozen components, in Apache Cassandra too, so a non-frozen value in a tuple literal is still
        // rejected where the tuple type is inferred. A collection literal of non-frozen values in a tuple is accepted.
        assertInvalidMessage("cannot be passed as argument 0 of function system.", "SELECT to_json((v, k)) FROM %s");
        assertInvalidMessage("cannot be passed as argument 0 of function system.collection_count",
                             "SELECT collection_count([(v, k)]) FROM %s");
        assertInvalidMessage("cannot be passed as argument 0 of function system.max", "SELECT max((l, fl)) FROM %s");
        assertRows(execute("SELECT to_json((k, [v, fv])) FROM %s WHERE k = 0"), row("[0, [" + json12 + ", " + json34 + "]]"));
        // A value with a type hint isn't assignable to the non-frozen element type, as in Apache Cassandra.
        assertInvalidMessage("cannot be passed as argument 0 of function system.collection_count",
                             "SELECT collection_count([v, (" + frozen + ") fv]) FROM %s");

        // A nested literal of non-frozen values next to a non-frozen collection column is tested against the column's
        // type, which is declared for it, as in Apache Cassandra. A nested literal of frozen values is accepted there.
        assertInvalidMessage("cannot be passed as argument 0 of function system.to_json", "SELECT to_json([[v], l]) FROM %s");
        assertInvalidMessage("cannot be passed as argument 0 of function system.collection_count",
                             "SELECT collection_count([{v}, s]) FROM %s");
        assertRows(execute("SELECT to_json([l, [fv]]) FROM %s WHERE k = 0"), row("[[" + json12 + "], [" + json34 + "]]"));
        // Two nested literals infer the same type, so this is accepted. Apache Cassandra accepts it in some JVM runs and
        // rejects it in others, depending on which of two qualifying element types a HashSet gives first.
        assertRows(execute("SELECT to_json([[v], [fv]]) FROM %s WHERE k = 0"), row("[[" + json12 + "], [" + json34 + "]]"));

        // The literal is an exact match for the native function, which is preferred to a user function of the same
        // name that accepts it as a list of tuples.
        createFunctionOverload(KEYSPACE + ".to_json", "list<frozen<tuple<int, int>>>",
                               "CREATE FUNCTION %s (x list<frozen<tuple<int, int>>>) CALLED ON NULL INPUT RETURNS text " +
                               "LANGUAGE java AS 'return \"udf\";'");
        assertRows(execute("SELECT to_json([v, fv]), to_json([v, v]) FROM %s WHERE k = 0"),
                   row('[' + json12 + ", " + json34 + ']', '[' + json12 + ", " + json12 + ']'));
        // A literal mixing a UDT with a tuple is only weakly assignable to both, so the call is ambiguous, as in Apache
        // Cassandra.
        assertInvalidMessage("Ambiguous call to function to_json", "SELECT to_json([v, tp]) FROM %s WHERE k = 0");

        // With a user function of the same name, the native function is the only one accepting the argument.
        createFunctionOverload(KEYSPACE + ".tojson", "list<frozen<" + type + ">>",
                               "CREATE FUNCTION %s (x list<frozen<" + type + ">>) CALLED ON NULL INPUT RETURNS text " +
                               "LANGUAGE java AS 'return \"udf\";'");
        assertRows(execute("SELECT toJson([v, fv]), tojson(l) FROM %s WHERE k = 0"), row('[' + json12 + ", " + json34 + ']', "udf"));

        // A declared collection type with frozen elements doesn't accept non-frozen values, as in Apache Cassandra.
        String listFunction = createFunction(KEYSPACE, "list<frozen<" + type + ">>",
                                             "CREATE FUNCTION %s (x list<frozen<" + type + ">>) CALLED ON NULL INPUT RETURNS int " +
                                             "LANGUAGE java AS 'return x == null ? null : x.size();'");
        assertRows(execute("SELECT " + listFunction + "([fv, fv]) FROM %s WHERE k = 0"), row(2));
        assertInvalidMessage("cannot be passed as argument 0 of function " + listFunction,
                             "SELECT " + listFunction + "([v, fv]) FROM %s WHERE k = 0");
        assertInvalidMessage("cannot be passed as argument 0 of function " + listFunction,
                             "SELECT " + listFunction + "([" + mk + "(1), " + mk + "(3)]) FROM %s WHERE k = 0");
        assertInvalidMessage("Cannot assign value", "SELECT (list<" + frozen + ">)[v, fv] FROM %s WHERE k = 0");
        assertInvalidMessage("Cannot assign value", "SELECT toJson((list<" + frozen + ">)[v, fv]) FROM %s WHERE k = 0");

        // Neither does the type of another argument: mask_replace takes the type of the masked value.
        assertInvalidMessage("cannot be passed as argument 1 of function system.mask_replace", "SELECT mask_replace(fl, [v]) FROM %s");
        assertInvalidMessage("cannot be passed as argument 1 of function system.mask_replace", "SELECT mask_replace([fv], [v]) FROM %s");
        // Known difference from Apache Cassandra, which accepts this: the type of the second argument is the type
        // inferred from the first one, whose element type is frozen here.
        assertInvalidMessage("cannot be passed as argument 1 of function system.mask_replace", "SELECT mask_replace([v], [v]) FROM %s");
        // A null value in the literal is a null element, as in Apache Cassandra.
        execute("INSERT INTO %s (k, fv) VALUES (2, ?)", u34);
        assertRows(execute("SELECT collection_count([fv, v]), collection_count([v, fv]) FROM %s WHERE k = 2"), row(2, 2));

        // Known difference from Apache Cassandra, which accepts this: a set literal argument in a term is prepared with
        // the type inferred from it, whose element type is frozen here, so it is tested against that type.
        assertInvalidMessage("cannot be passed as argument 0 of function system.collection_count",
                             "SELECT k FROM %s WHERE k = collection_count({" + mk + "(1), " + mk + "(3)})");
    }

    /**
     * Writes and restrictions mixing frozen and non-frozen UDT values. These were never affected, and are checked
     * against Apache Cassandra's behaviour.
     */
    @Test
    public void testWritesAndRestrictionsMixingFrozenAndNonFrozenValues() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");
        String frozen = "frozen<" + type + '>';
        Object u12 = userType("a", 1, "b", 2);
        Object u34 = userType("a", 3, "b", 4);
        String function = createFunction(KEYSPACE, "int",
                                         "CREATE FUNCTION %s (x int) CALLED ON NULL INPUT RETURNS " + type + " " +
                                         "LANGUAGE java AS 'return udfContext.newReturnUDTValue().setInt(\"a\", x).setInt(\"b\", x + 1);'");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ", fv " + frozen + ", l list<" + frozen + ">, m map<int, " + frozen + ">)");
        execute("INSERT INTO %s (k, v, fv, l, m) VALUES (0, ?, ?, ?, ?)", u12, u34, list(u12, u34), map(1, u12));
        execute("INSERT INTO %s (k, v, fv, l, m) VALUES (1, (" + type + "){a: 1, b: 2}, {a: 3, b: 4}, [{a: 1, b: 2}, {a: 3, b: 4}], {1: {a: 1, b: 2}})");
        execute("UPDATE %s SET v = " + function + "(1), fv = " + function + "(3), l = l + [{a: 1, b: 2}], m = m + {2: {a: 3, b: 4}} WHERE k = 2");
        execute("UPDATE %s SET l = l + ?, m = m + ? WHERE k = 2", list(u34), map(3, u12));
        execute("UPDATE %s SET m[4] = " + function + "(3) WHERE k = 2");
        assertRows(execute("SELECT k, v, fv, l, m FROM %s WHERE k IN (0, 1, 2)"),
                   row(0, u12, u34, list(u12, u34), map(1, u12)),
                   row(1, u12, u34, list(u12, u34), map(1, u12)),
                   row(2, u12, u34, list(u12, u34), map(2, u34, 3, u12, 4, u34)));
        assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE fv = ? ALLOW FILTERING", u34), row(0), row(1), row(2));
        assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE l CONTAINS " + function + "(3) ALLOW FILTERING"), row(0), row(1), row(2));

        createTable("CREATE TABLE %s (pk " + frozen + ", c " + frozen + ", x int, PRIMARY KEY (pk, c))");
        execute("INSERT INTO %s (pk, c, x) VALUES ({a: 1, b: 2}, {a: 1, b: 2}, 1)");
        execute("INSERT INTO %s (pk, c, x) VALUES (?, ?, 2)", u34, u12);
        execute("INSERT INTO %s (pk, c, x) VALUES (" + function + "(1), " + function + "(3), 3)");
        assertRowsIgnoringOrder(execute("SELECT x FROM %s WHERE pk IN ({a: 1, b: 2}, {a: 3, b: 4})"), row(1), row(3), row(2));
        assertRowsIgnoringOrder(execute("SELECT x FROM %s WHERE pk IN ?", list(u12, u34)), row(1), row(3), row(2));
        assertRowsIgnoringOrder(execute("SELECT x FROM %s WHERE pk IN (?, ?)", u12, u34), row(1), row(3), row(2));
        assertRows(execute("SELECT x FROM %s WHERE pk = " + function + "(1)"), row(1), row(3));
        assertRows(execute("SELECT x FROM %s WHERE pk = ? AND c IN (?, ?)", u12, u12, u34), row(1), row(3));
        assertRows(execute("SELECT x FROM %s WHERE pk = ? AND c > " + function + "(1)", u12), row(3));
    }

    /**
     * Checks that both queries return the same column type and the same serialized value to the client.
     */
    private void assertSameNetResult(String query, String expectedQuery) throws Throwable
    {
        ResultSet expected = executeNet(expectedQuery);
        ResultSet actual = executeNet(query);
        assertEquals(query, expected.getColumnDefinitions().getType(0), actual.getColumnDefinitions().getType(0));
        Row expectedRow = expected.one();
        Row actualRow = actual.one();
        assertEquals(query, expectedRow.getBytesUnsafe(0), actualRow.getBytesUnsafe(0));
    }
}
