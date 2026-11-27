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
package org.apache.cassandra.cql3.validation.entities;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;

/**
 * Statements mixing values of a non-frozen UDT with values of the same UDT frozen. Values of a non-frozen UDT are
 * handed on in their frozen form, so these statements are accepted, and give the same results (types and serialized
 * values) as with frozen values.
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
                    "tp frozen<tuple<int, int>>, " +
                    "vt " + textType + ", ttxt frozen<tuple<text, int>>)");
        String insert = "INSERT INTO %s (k, v, fv, l, fl, s, fs, m, fm, tp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Object u12 = userType("a", 1, "b", 2);
        Object u34 = userType("a", 3, "b", 4);
        execute(insert, 0, u12, u34, list(u12), list(u34), set(u12), set(u34), map(1, u12), map(2, u34), tuple(5, 6));
        // all the frozen and non-frozen values are equal, to compare with the selection of frozen values only
        execute(insert, 1, u12, u12, list(u12), list(u12), set(u12), set(u12), map(1, u12), map(1, u12), tuple(1, 2));
        execute("UPDATE %s SET vt = {a: 'x', b: 1}, ttxt = ('y', 2) WHERE k IN (0, 1)");

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
        // which may be a non-frozen UDT or collection.
        assertRows(execute("SELECT [v, (v)], {v, (v)}, [l, (l)], {s, (s)} FROM %s WHERE k = 0"),
                   row(list(u12, u12), set(u12), list(list(u12), list(u12)), set(set(u12))));
        // A frozen value between parentheses is of that type too.
        assertRows(execute("SELECT [v, (fv)], {v, (fv)}, [l, (fl)] FROM %s WHERE k = 0"),
                   row(list(u12, u34), set(u12, u34), list(list(u12), list(u34))));
        // A non-frozen value between parentheses is of the frozen type of the other elements, as it is returned in its
        // frozen form.
        assertRows(execute("SELECT [fv, (v)], {fv, (v)}, [fl, (l)], {fs, (s)}, [fm, (m)] FROM %s WHERE k = 0"),
                   row(list(u34, u12), set(u12, u34), list(list(u34), list(u12)), set(set(u12), set(u34)),
                       list(map(2, u34), map(1, u12))));
        assertSameNetResult("SELECT [fv, (v)] FROM %s WHERE k = 1", "SELECT [fv, fv] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT {fs, (s)} FROM %s WHERE k = 1", "SELECT {fs, fs} FROM %s WHERE k = 1");
        assertRows(execute("SELECT [fv, ((v))], [fl, ((l))] FROM %s WHERE k = 0"), row(list(u34, u12), list(list(u34), list(u12))));
        // A value of another type isn't, as the client couldn't decode the result.
        assertInvalidMessage("(k) is not of the expected type", "SELECT [v, (k)] FROM %s WHERE k = 0");
        assertInvalidMessage("(k) is not of the expected type", "SELECT {v, (k)} FROM %s WHERE k = 0");
        assertInvalidMessage("(k) is not of the expected type", "SELECT [fv, (k)] FROM %s WHERE k = 0");
        assertInvalidMessage("(s) is not of the expected type", "SELECT [fl, (s)] FROM %s WHERE k = 0");
        assertInvalidMessage("(vt) is not of the expected type", "SELECT [fv, (vt)] FROM %s WHERE k = 0");

        // The client gets the same types and serialized values as for frozen values only.
        assertSameNetResult("SELECT [v, fv] FROM %s WHERE k = 1", "SELECT [fv, fv] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [fv, v] FROM %s WHERE k = 1", "SELECT [fv, fv] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT {v, fv} FROM %s WHERE k = 1", "SELECT {fv, fv} FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [l, fl] FROM %s WHERE k = 1", "SELECT [fl, fl] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT {s, fs} FROM %s WHERE k = 1", "SELECT {fs, fs} FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [m, fm] FROM %s WHERE k = 1", "SELECT [fm, fm] FROM %s WHERE k = 1");
        assertSameNetResult("SELECT [v, (v)] FROM %s WHERE k = 1", "SELECT [fv, fv] FROM %s WHERE k = 1");

        // Elements whose types can't read each other's values are rejected, including when one is not frozen.
        String incompatible = "Invalid collection literal: all selectors must have the same CQL type inside collection literals";
        assertInvalidMessage(incompatible, "SELECT [v, k] FROM %s WHERE k = 0");
        assertInvalidMessage(incompatible, "SELECT [l, s] FROM %s WHERE k = 0");
        assertInvalidMessage(incompatible, "SELECT [v, vt] FROM %s WHERE k = 0");
        assertInvalidMessage(incompatible, "SELECT [v, ttxt] FROM %s WHERE k = 0");
        assertInvalidMessage(incompatible, "SELECT [v, tp] FROM %s WHERE k = 0");
    }

    /**
     * Generic native functions, like {@code toJson} or {@code max}, take the type of a collection literal argument from
     * its elements. That type has frozen element types, so a literal of non-frozen values, like {@code [v, v]}, isn't
     * assignable to it, but it is accepted as the argument it was inferred from. A collection type with frozen elements
     * that is declared doesn't accept non-frozen values.
     */
    @Test
    public void testGenericFunctionsAcceptCollectionLiteralsOfNonFrozenValues() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");
        String frozen = "frozen<" + type + '>';
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ", fv " + frozen + ", " +
                    "l list<" + frozen + ">, fl frozen<list<" + frozen + ">>, s set<" + frozen + ">, fs frozen<set<" + frozen + ">>, " +
                    "m map<int, " + frozen + ">, fm frozen<map<int, " + frozen + ">>, li list<int>, fli frozen<list<int>>)");
        String insert = "INSERT INTO %s (k, v, fv, l, fl, s, fs, m, fm, li, fli) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Object u12 = userType("a", 1, "b", 2);
        Object u34 = userType("a", 3, "b", 4);
        execute(insert, 0, u12, u34, list(u12), list(u34), set(u12), set(u34), map(1, u12), map(2, u34), list(1), list(2));
        // all the frozen and non-frozen values are equal, to compare with the same calls on frozen values only
        execute(insert, 1, u12, u12, list(u12), list(u12), set(u12), set(u12), map(1, u12), map(1, u12), list(1), list(1));
        String mk = createFunction(KEYSPACE, "int",
                                   "CREATE FUNCTION %s (x int) CALLED ON NULL INPUT RETURNS " + type + " " +
                                   "LANGUAGE java AS 'return udfContext.newReturnUDTValue().setInt(\"a\", x).setInt(\"b\", x + 1);'");

        String json12 = "{\"a\": 1, \"b\": 2}";
        String json34 = "{\"a\": 3, \"b\": 4}";
        assertRows(execute("SELECT toJson([v, v]), toJson([v, fv]), toJson([fv, v]), toJson({v, fv}) FROM %s WHERE k = 0"),
                   row('[' + json12 + ", " + json12 + ']', '[' + json12 + ", " + json34 + ']',
                       '[' + json34 + ", " + json12 + ']', '[' + json12 + ", " + json34 + ']'));
        assertRows(execute("SELECT toJson([l, fl]), toJson({s, fs}), toJson([m, fm]), toJson([li, fli]) FROM %s WHERE k = 0"),
                   row("[[" + json12 + "], [" + json34 + "]]", "[[" + json12 + "], [" + json34 + "]]",
                       "[{\"1\": " + json12 + "}, {\"2\": " + json34 + "}]", "[[1], [2]]"));
        assertRows(execute("SELECT toJson([" + mk + "(1), " + mk + "(3)]), toJson([" + mk + "(3), v]) FROM %s WHERE k = 0"),
                   row('[' + json12 + ", " + json34 + ']', '[' + json34 + ", " + json12 + ']'));
        assertRows(execute("SELECT max([v, fv]), min([fv, v]) FROM %s"), row(list(u12, u34), list(u12, u12)));
        assertRows(execute("SELECT toJson(max([v, fv])) FROM %s WHERE k = 0"), row('[' + json12 + ", " + json34 + ']'));

        // The client gets the same types and serialized values as for frozen values only.
        assertSameNetResult("SELECT toJson([v, fv]) FROM %s WHERE k = 1", "SELECT toJson([fv, fv]) FROM %s WHERE k = 1");
        assertSameNetResult("SELECT max([v, fv]) FROM %s WHERE k = 1", "SELECT max([fv, fv]) FROM %s WHERE k = 1");
        assertSameNetResult("SELECT max([l, fl]) FROM %s WHERE k = 1", "SELECT max([fl, fl]) FROM %s WHERE k = 1");

        // The same when prepared.
        PreparedStatement prepared = sessionNet().prepare(formatQuery("SELECT toJson([v, fv]), max([v, fv]) FROM %s WHERE k = ?"));
        Row row = sessionNet().execute(prepared.bind(0)).one();
        assertEquals('[' + json12 + ", " + json34 + ']', row.getString(0));
        assertEquals(executeNet("SELECT [fv, fv] FROM %s WHERE k = 0").getColumnDefinitions().getType(0), row.getColumnDefinitions().getType(1));

        // A tuple type has frozen components, so a non-frozen value in a tuple literal is rejected where the tuple type
        // is inferred. A collection literal of non-frozen values in a tuple is accepted.
        assertInvalidMessage("cannot be passed as argument 0 of function system.tojson", "SELECT toJson((v, k)) FROM %s");
        assertInvalidMessage("cannot be passed as argument 0 of function system.max", "SELECT max((l, fl)) FROM %s");
        assertRows(execute("SELECT toJson((k, [v, fv])) FROM %s WHERE k = 0"), row("[0, [" + json12 + ", " + json34 + "]]"));
        // A value with a type hint isn't assignable to the non-frozen element type.
        assertInvalidMessage("cannot be passed as argument 0 of function system.tojson",
                             "SELECT toJson([v, (" + frozen + ") fv]) FROM %s");
        // An element that isn't a value of the UDT, like an empty list, is rejected.
        assertInvalidMessage("cannot be passed as argument 0 of function system.max", "SELECT max([v, []]) FROM %s");

        // A nested literal of non-frozen values next to a non-frozen collection column is tested against the column's
        // type, which is declared for it. A nested literal of frozen values is accepted there.
        assertInvalidMessage("cannot be passed as argument 0 of function system.tojson", "SELECT toJson([[v], l]) FROM %s");
        assertInvalidMessage("cannot be passed as argument 0 of function system.tojson", "SELECT toJson([{v}, s]) FROM %s");
        assertRows(execute("SELECT toJson([l, [fv]]) FROM %s WHERE k = 0"), row("[[" + json12 + "], [" + json34 + "]]"));
        // Two nested literals infer the same type, so this is accepted.
        assertRows(execute("SELECT toJson([[v], [fv]]) FROM %s WHERE k = 0"), row("[[" + json12 + "], [" + json34 + "]]"));

        // With a user function of the same name, the native function is the only one accepting the argument.
        createFunctionOverload(KEYSPACE + ".tojson", "list<frozen<" + type + ">>",
                               "CREATE FUNCTION %s (x list<frozen<" + type + ">>) CALLED ON NULL INPUT RETURNS text " +
                               "LANGUAGE java AS 'return \"udf\";'");
        assertRows(execute("SELECT toJson([v, fv]), tojson(l) FROM %s WHERE k = 0"), row('[' + json12 + ", " + json34 + ']', "udf"));

        // A declared collection type with frozen elements doesn't accept non-frozen values.
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
