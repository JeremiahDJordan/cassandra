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

package org.apache.cassandra.cql3.selection;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TermSelectionTest extends CQLTester
{
    // Helper method for testSelectLiteral()
    private void assertConstantResult(UntypedResultSet result, Object constant)
    {
        assertRows(result,
                   row(1, "one", constant),
                   row(2, "two", constant),
                   row(3, "three", constant));
    }

    @Test
    public void testSelectLiteral() throws Throwable
    {
        long timestampInMicros = System.currentTimeMillis() * 1000;
        createTable("CREATE TABLE %s (pk int, ck int, t text, PRIMARY KEY (pk, ck) )");
        execute("INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?) USING TIMESTAMP ?", 1, 1, "one", timestampInMicros);
        execute("INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?) USING TIMESTAMP ?", 1, 2, "two", timestampInMicros);
        execute("INSERT INTO %s (pk, ck, t) VALUES (?, ?, ?) USING TIMESTAMP ?", 1, 3, "three", timestampInMicros);

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, 'a const' FROM %s");
        assertConstantResult(execute("SELECT ck, t, (text)'a const' FROM %s"), "a const");

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, 42 FROM %s");
        assertConstantResult(execute("SELECT ck, t, (smallint)42 FROM %s"), (short) 42);

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, (1, 'foo') FROM %s");
        assertConstantResult(execute("SELECT ck, t, (tuple<int, text>)(1, 'foo') FROM %s"), tuple(1, "foo"));

        assertInvalidMessage("Cannot infer type for term ((1)) in selection clause", "SELECT ck, t, ((1)) FROM %s");
        // We cannot differentiate a tuple containing a tuple from a tuple between parentheses.
        assertInvalidMessage("Cannot infer type for term ((tuple<int>)(1))", "SELECT ck, t, ((tuple<int>)(1)) FROM %s");
        assertConstantResult(execute("SELECT ck, t, (tuple<tuple<int>>)((1)) FROM %s"), tuple(tuple(1)));

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, [1, 2, 3] FROM %s");
        assertConstantResult(execute("SELECT ck, t, (list<int>)[1, 2, 3] FROM %s"), list(1, 2, 3));

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, {1, 2, 3} FROM %s");
        assertConstantResult(execute("SELECT ck, t, (set<int>){1, 2, 3} FROM %s"), set(1, 2, 3));

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, {1: 'foo', 2: 'bar', 3: 'baz'} FROM %s");
        assertConstantResult(execute("SELECT ck, t, (map<int, text>){1: 'foo', 2: 'bar', 3: 'baz'} FROM %s"), map(1, "foo", 2, "bar", 3, "baz"));

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, {} FROM %s");
        assertConstantResult(execute("SELECT ck, t, (map<int, text>){} FROM %s"), map());
        assertConstantResult(execute("SELECT ck, t, (set<int>){} FROM %s"), set());

        assertColumnNames(execute("SELECT ck, t, (int)42, (int)43 FROM %s"), "ck", "t", "(int)42", "(int)43");
        assertRows(execute("SELECT ck, t, (int) 42, (int) 43 FROM %s"),
                   row(1, "one", 42, 43),
                   row(2, "two", 42, 43),
                   row(3, "three", 42, 43));

        assertRows(execute("SELECT min(ck), max(ck), [min(ck), max(ck)] FROM %s"), row(1, 3, list(1, 3)));
        assertRows(execute("SELECT [min(ck), max(ck)] FROM %s"), row(list(1, 3)));
        assertRows(execute("SELECT {min(ck), max(ck)} FROM %s"), row(set(1, 3)));

        // We need to use a cast to differentiate between a map and an UDT
        assertInvalidMessage("Cannot infer type for term {'min': system.min(ck), 'max': system.max(ck)}",
                             "SELECT {'min' : min(ck), 'max' : max(ck)} FROM %s");
        assertRows(execute("SELECT (map<text, int>){'min' : min(ck), 'max' : max(ck)} FROM %s"), row(map("min", 1, "max", 3)));

        assertRows(execute("SELECT [1, min(ck), max(ck)] FROM %s"), row(list(1, 1, 3)));
        assertRows(execute("SELECT {1, min(ck), max(ck)} FROM %s"), row(set(1, 1, 3)));
        assertRows(execute("SELECT (map<text, int>) {'litteral' : 1, 'min' : min(ck), 'max' : max(ck)} FROM %s"), row(map("litteral", 1, "min", 1, "max", 3)));

        // Test List nested within Lists
        assertRows(execute("SELECT [[], [min(ck), max(ck)]] FROM %s"),
                   row(list(list(), list(1, 3))));
        assertRows(execute("SELECT [[], [CAST(pk AS BIGINT), CAST(ck AS BIGINT), WRITETIME(t)]] FROM %s"),
                   row(list(list(), list(1L, 1L, timestampInMicros))),
                   row(list(list(), list(1L, 2L, timestampInMicros))),
                   row(list(list(), list(1L, 3L, timestampInMicros))));
        assertRows(execute("SELECT [[min(ck)], [max(ck)]] FROM %s"),
                   row(list(list(1), list(3))));
        assertRows(execute("SELECT [[min(ck)], ([max(ck)])] FROM %s"),
                   row(list(list(1), list(3))));
        assertRows(execute("SELECT [[pk], [ck]] FROM %s"),
                   row(list(list(1), list(1))),
                   row(list(list(1), list(2))),
                   row(list(list(1), list(3))));
        assertRows(execute("SELECT [[pk], [ck]] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(list(list(1), list(3))),
                   row(list(list(1), list(2))),
                   row(list(list(1), list(1))));

        // Test Sets nested within Lists
        assertRows(execute("SELECT [{}, {min(ck), max(ck)}] FROM %s"),
                   row(list(set(), set(1, 3))));
        assertRows(execute("SELECT [{}, {CAST(pk AS BIGINT), CAST(ck AS BIGINT), WRITETIME(t)}] FROM %s"),
                   row(list(set(), set(1L, 1L, timestampInMicros))),
                   row(list(set(), set(1L, 2L, timestampInMicros))),
                   row(list(set(), set(1L, 3L, timestampInMicros))));
        assertRows(execute("SELECT [{min(ck)}, {max(ck)}] FROM %s"),
                   row(list(set(1), set(3))));
        assertRows(execute("SELECT [{min(ck)}, ({max(ck)})] FROM %s"),
                   row(list(set(1), set(3))));
        assertRows(execute("SELECT [{pk}, {ck}] FROM %s"),
                   row(list(set(1), set(1))),
                   row(list(set(1), set(2))),
                   row(list(set(1), set(3))));
        assertRows(execute("SELECT [{pk}, {ck}] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(list(set(1), set(3))),
                   row(list(set(1), set(2))),
                   row(list(set(1), set(1))));

        // Test Maps nested within Lists
        assertRows(execute("SELECT [{}, (map<text, int>){'min' : min(ck), 'max' : max(ck)}] FROM %s"),
                   row(list(map(), map("min", 1, "max", 3))));
        assertRows(execute("SELECT [{}, (map<text, bigint>){'pk' : CAST(pk AS BIGINT), 'ck' : CAST(ck AS BIGINT), 'writetime' : WRITETIME(t)}] FROM %s"),
                   row(list(map(), map("pk", 1L, "ck", 1L, "writetime", timestampInMicros))),
                   row(list(map(), map("pk", 1L, "ck", 2L, "writetime", timestampInMicros))),
                   row(list(map(), map("pk", 1L, "ck", 3L, "writetime", timestampInMicros))));
        assertRows(execute("SELECT [{}, (map<text, int>){'pk' : pk, 'ck' : ck}] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(list(map(), map("pk", 1, "ck", 3))),
                   row(list(map(), map("pk", 1, "ck", 2))),
                   row(list(map(), map("pk", 1, "ck", 1))));

        // Test Tuples nested within Lists
        assertRows(execute("SELECT [(pk, ck, WRITETIME(t))] FROM %s"),
                   row(list(tuple(1, 1, timestampInMicros))),
                   row(list(tuple(1, 2, timestampInMicros))),
                   row(list(tuple(1, 3, timestampInMicros))));
        assertRows(execute("SELECT [(min(ck), max(ck))] FROM %s"),
                   row(list(tuple(1, 3))));
        assertInvalidMessage("Invalid list literal [(cast(pk as bigint), cast(ck as bigint)), (t, writetime(t))]: " +
                             "value (t, writetime(t)) is not of type frozen<tuple<bigint, bigint>>",
                             "SELECT [(CAST(pk AS BIGINT), CAST(ck AS BIGINT)), (t, WRITETIME(t))] FROM %s");

        // Test UDTs nested within Lists
        String type = createType("CREATE TYPE %s(a int, b int, c bigint)");
        assertRows(execute("SELECT [(" + type + "){a : min(ck), b: max(ck)}] FROM %s"),
                   row(list(userType("a", 1, "b", 3, "c", null))));
        assertRows(execute("SELECT [(" + type + "){a : pk, b : ck, c : WRITETIME(t)}] FROM %s"),
                   row(list(userType("a", 1, "b", 1, "c", timestampInMicros))),
                   row(list(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(list(userType("a", 1, "b", 3, "c", timestampInMicros))));
        assertRows(execute("SELECT [(" + type + "){a : pk, b : ck, c : WRITETIME(t)}] FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(list(userType("a", 1, "b", 3, "c", timestampInMicros))),
                   row(list(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(list(userType("a", 1, "b", 1, "c", timestampInMicros))));

        // Test Lists nested within Sets
        assertRows(execute("SELECT {[], [min(ck), max(ck)]} FROM %s"),
                   row(set(list(), list(1, 3))));
        assertRows(execute("SELECT {[], [pk, ck]} FROM %s LIMIT 2"),
                   row(set(list(), list(1, 1))),
                   row(set(list(), list(1, 2))));
        assertRows(execute("SELECT {[], [pk, ck]} FROM %s WHERE pk = 1 ORDER BY ck DESC LIMIT 2"),
                   row(set(list(), list(1, 3))),
                   row(set(list(), list(1, 2))));
        assertRows(execute("SELECT {[min(ck)], ([max(ck)])} FROM %s"),
                   row(set(list(1), list(3))));
        assertRows(execute("SELECT {[pk], ([ck])} FROM %s"),
                   row(set(list(1), list(1))),
                   row(set(list(1), list(2))),
                   row(set(list(1), list(3))));
        assertRows(execute("SELECT {([min(ck)]), [max(ck)]} FROM %s"),
                   row(set(list(1), list(3))));

        // Test Sets nested within Sets
        assertRows(execute("SELECT {{}, {min(ck), max(ck)}} FROM %s"),
                   row(set(set(), set(1, 3))));
        assertRows(execute("SELECT {{}, {pk, ck}} FROM %s LIMIT 2"),
                   row(set(set(), set(1, 1))),
                   row(set(set(), set(1, 2))));
        assertRows(execute("SELECT {{}, {pk, ck}} FROM %s WHERE pk = 1 ORDER BY ck DESC LIMIT 2"),
                   row(set(set(), set(1, 3))),
                   row(set(set(), set(1, 2))));
        assertRows(execute("SELECT {{min(ck)}, ({max(ck)})} FROM %s"),
                   row(set(set(1), set(3))));
        assertRows(execute("SELECT {{pk}, ({ck})} FROM %s"),
                   row(set(set(1), set(1))),
                   row(set(set(1), set(2))),
                   row(set(set(1), set(3))));
        assertRows(execute("SELECT {({min(ck)}), {max(ck)}} FROM %s"),
                   row(set(set(1), set(3))));

        // Test Maps nested within Sets
        assertRows(execute("SELECT {{}, (map<text, int>){'min' : min(ck), 'max' : max(ck)}} FROM %s"),
                   row(set(map(), map("min", 1, "max", 3))));
        assertRows(execute("SELECT {{}, (map<text, int>){'pk' : pk, 'ck' : ck}} FROM %s"),
                   row(set(map(), map("pk", 1, "ck", 1))),
                   row(set(map(), map("pk", 1, "ck", 2))),
                   row(set(map(), map("pk", 1, "ck", 3))));

        // Test Tuples nested within Sets
        assertRows(execute("SELECT {(pk, ck, WRITETIME(t))} FROM %s"),
                   row(set(tuple(1, 1, timestampInMicros))),
                   row(set(tuple(1, 2, timestampInMicros))),
                   row(set(tuple(1, 3, timestampInMicros))));
        assertRows(execute("SELECT {(min(ck), max(ck))} FROM %s"),
                   row(set(tuple(1, 3))));

        // Test UDTs nested within Sets
        assertRows(execute("SELECT {(" + type + "){a : min(ck), b: max(ck)}} FROM %s"),
                   row(set(userType("a", 1, "b", 3, "c", null))));
        assertRows(execute("SELECT {(" + type + "){a : pk, b : ck, c : WRITETIME(t)}} FROM %s"),
                   row(set(userType("a", 1, "b", 1, "c", timestampInMicros))),
                   row(set(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(set(userType("a", 1, "b", 3, "c", timestampInMicros))));
        assertRows(execute("SELECT {(" + type + "){a : pk, b : ck, c : WRITETIME(t)}} FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(set(userType("a", 1, "b", 3, "c", timestampInMicros))),
                   row(set(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(set(userType("a", 1, "b", 1, "c", timestampInMicros))));

        // Test Lists nested within Maps
        assertRows(execute("SELECT (map<frozen<list<int>>, frozen<list<int>>>){[min(ck)]:[max(ck)]} FROM %s"),
                   row(map(list(1), list(3))));
        assertRows(execute("SELECT (map<frozen<list<int>>, frozen<list<int>>>){[pk]: [ck]} FROM %s"),
                   row(map(list(1), list(1))),
                   row(map(list(1), list(2))),
                   row(map(list(1), list(3))));

        // Test Sets nested within Maps
        assertRows(execute("SELECT (map<frozen<set<int>>, frozen<set<int>>>){{min(ck)} : {max(ck)}} FROM %s"),
                   row(map(set(1), set(3))));
        assertRows(execute("SELECT (map<frozen<set<int>>, frozen<set<int>>>){{pk} : {ck}} FROM %s"),
                   row(map(set(1), set(1))),
                   row(map(set(1), set(2))),
                   row(map(set(1), set(3))));

        // Test Maps nested within Maps
        assertRows(execute("SELECT (map<frozen<map<text, int>>, frozen<map<text, int>>>){{'min' : min(ck)} : {'max' : max(ck)}} FROM %s"),
                   row(map(map("min", 1), map("max", 3))));
        assertRows(execute("SELECT (map<frozen<map<text, int>>, frozen<map<text, int>>>){{'pk' : pk} : {'ck' : ck}} FROM %s"),
                   row(map(map("pk", 1), map("ck", 1))),
                   row(map(map("pk", 1), map("ck", 2))),
                   row(map(map("pk", 1), map("ck", 3))));

        // Test Tuples nested within Maps
        assertRows(execute("SELECT (map<frozen<tuple<int, int>>, frozen<tuple<bigint>>>){(pk, ck) : (WRITETIME(t))} FROM %s"),
                   row(map(tuple(1, 1), tuple(timestampInMicros))),
                   row(map(tuple(1, 2), tuple(timestampInMicros))),
                   row(map(tuple(1, 3), tuple(timestampInMicros))));
        assertRows(execute("SELECT (map<frozen<tuple<int>> , frozen<tuple<int>>>){(min(ck)) : (max(ck))} FROM %s"),
                   row(map(tuple(1), tuple(3))));

        // Test UDTs nested within Maps
        assertRows(execute("SELECT (map<int, frozen<" + type + ">>){ck : {a : min(ck), b: max(ck)}} FROM %s"),
                   row(map(1, userType("a", 1, "b", 3, "c", null))));
        assertRows(execute("SELECT (map<int, frozen<" + type + ">>){ck : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s"),
                   row(map(1, userType("a", 1, "b", 1, "c", timestampInMicros))),
                   row(map(2, userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(map(3, userType("a", 1, "b", 3, "c", timestampInMicros))));
        assertRows(execute("SELECT (map<int, frozen<" + type + ">>){ck : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(map(3, userType("a", 1, "b", 3, "c", timestampInMicros))),
                   row(map(2, userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(map(1, userType("a", 1, "b", 1, "c", timestampInMicros))));

        // Test Lists nested within Tuples
        assertRows(execute("SELECT ([min(ck)], [max(ck)]) FROM %s"),
                   row(tuple(list(1), list(3))));
        assertRows(execute("SELECT ([pk], [ck]) FROM %s"),
                   row(tuple(list(1), list(1))),
                   row(tuple(list(1), list(2))),
                   row(tuple(list(1), list(3))));

        // Test Sets nested within Tuples
        assertRows(execute("SELECT ({min(ck)}, {max(ck)}) FROM %s"),
                   row(tuple(set(1), set(3))));
        assertRows(execute("SELECT ({pk}, {ck}) FROM %s"),
                   row(tuple(set(1), set(1))),
                   row(tuple(set(1), set(2))),
                   row(tuple(set(1), set(3))));

        // Test Maps nested within Tuples
        assertRows(execute("SELECT ((map<text, int>){'min' : min(ck)}, (map<text, int>){'max' : max(ck)}) FROM %s"),
                   row(tuple(map("min", 1), map("max", 3))));
        assertRows(execute("SELECT ((map<text, int>){'pk' : pk}, (map<text, int>){'ck' : ck}) FROM %s"),
                   row(tuple(map("pk", 1), map("ck", 1))),
                   row(tuple(map("pk", 1), map("ck", 2))),
                   row(tuple(map("pk", 1), map("ck", 3))));

        // Test Tuples nested within Tuples
        assertRows(execute("SELECT (tuple<tuple<int, int, bigint>>)((pk, ck, WRITETIME(t))) FROM %s"),
                   row(tuple(tuple(1, 1, timestampInMicros))),
                   row(tuple(tuple(1, 2, timestampInMicros))),
                   row(tuple(tuple(1, 3, timestampInMicros))));
        assertRows(execute("SELECT (tuple<tuple<int, int, bigint>>)((min(ck), max(ck))) FROM %s"),
                   row(tuple(tuple(1, 3))));

        assertRows(execute("SELECT ((t, WRITETIME(t)), (CAST(pk AS BIGINT), CAST(ck AS BIGINT))) FROM %s"),
                   row(tuple(tuple("one", timestampInMicros), tuple(1L, 1L))),
                   row(tuple(tuple("two", timestampInMicros), tuple(1L, 2L))),
                   row(tuple(tuple("three", timestampInMicros), tuple(1L, 3L))));

        // Test UDTs nested within Tuples
        assertRows(execute("SELECT (tuple<" + type + ">)({a : min(ck), b: max(ck)}) FROM %s"),
                   row(tuple(userType("a", 1, "b", 3, "c", null))));
        assertRows(execute("SELECT (tuple<" + type + ">)({a : pk, b : ck, c : WRITETIME(t)}) FROM %s"),
                   row(tuple(userType("a", 1, "b", 1, "c", timestampInMicros))),
                   row(tuple(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(tuple(userType("a", 1, "b", 3, "c", timestampInMicros))));
        assertRows(execute("SELECT (tuple<" + type + ">)({a : pk, b : ck, c : WRITETIME(t)}) FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(tuple(userType("a", 1, "b", 3, "c", timestampInMicros))),
                   row(tuple(userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(tuple(userType("a", 1, "b", 1, "c", timestampInMicros))));

        // Test Lists nested within UDTs
        String containerType = createType("CREATE TYPE %s(l list<int>)");
        assertRows(execute("SELECT (" + containerType + "){l : [min(ck), max(ck)]} FROM %s"),
                   row(userType("l", list(1, 3))));
        assertRows(execute("SELECT (" + containerType + "){l : [pk, ck]} FROM %s"),
                   row(userType("l", list(1, 1))),
                   row(userType("l", list(1, 2))),
                   row(userType("l", list(1, 3))));

        // Test Sets nested within UDTs
        containerType = createType("CREATE TYPE %s(s set<int>)");
        assertRows(execute("SELECT (" + containerType + "){s : {min(ck), max(ck)}} FROM %s"),
                   row(userType("s", set(1, 3))));
        assertRows(execute("SELECT (" + containerType + "){s : {pk, ck}} FROM %s"),
                   row(userType("s", set(1))),
                   row(userType("s", set(1, 2))),
                   row(userType("s", set(1, 3))));

        // Test Maps nested within UDTs
        containerType = createType("CREATE TYPE %s(m map<text, int>)");
        assertRows(execute("SELECT (" + containerType + "){m : {'min' : min(ck), 'max' : max(ck)}} FROM %s"),
                   row(userType("m", map("min", 1, "max", 3))));
        assertRows(execute("SELECT (" + containerType + "){m : {'pk' : pk, 'ck' : ck}} FROM %s"),
                   row(userType("m", map("pk", 1, "ck", 1))),
                   row(userType("m", map("pk", 1, "ck", 2))),
                   row(userType("m", map("pk", 1, "ck", 3))));

        // Test Tuples nested within UDTs
        containerType = createType("CREATE TYPE %s(t tuple<int, int>, w tuple<bigint>)");
        assertRows(execute("SELECT (" + containerType + "){t : (pk, ck), w : (WRITETIME(t))} FROM %s"),
                   row(userType("t", tuple(1, 1), "w", tuple(timestampInMicros))),
                   row(userType("t", tuple(1, 2), "w", tuple(timestampInMicros))),
                   row(userType("t", tuple(1, 3), "w", tuple(timestampInMicros))));

        // Test UDTs nested within Maps
        containerType = createType("CREATE TYPE %s(t frozen<" + type + ">)");
        assertRows(execute("SELECT (" + containerType + "){t : {a : min(ck), b: max(ck)}} FROM %s"),
                   row(userType("t", userType("a", 1, "b", 3, "c", null))));
        assertRows(execute("SELECT (" + containerType + "){t : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s"),
                   row(userType("t", userType("a", 1, "b", 1, "c", timestampInMicros))),
                   row(userType("t", userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(userType("t", userType("a", 1, "b", 3, "c", timestampInMicros))));
        assertRows(execute("SELECT (" + containerType + "){t : {a : pk, b : ck, c : WRITETIME(t)}} FROM %s WHERE pk = 1 ORDER BY ck DESC"),
                   row(userType("t", userType("a", 1, "b", 3, "c", timestampInMicros))),
                   row(userType("t", userType("a", 1, "b", 2, "c", timestampInMicros))),
                   row(userType("t", userType("a", 1, "b", 1, "c", timestampInMicros))));


        // Test Litteral Set with Duration elements
        assertInvalidMessage("Durations are not allowed inside sets: set<duration>",
                             "SELECT pk, ck, (set<duration>){2d, 1mo} FROM %s");

        assertInvalidMessage("Invalid field selection: system.min(ck) of type int is not a user type",
                             "SELECT min(ck).min FROM %s");
        assertInvalidMessage("Invalid field selection: (map<text, int>){'min': system.min(ck), 'max': system.max(ck)} of type frozen<map<text, int>> is not a user type",
                             "SELECT (map<text, int>) {'min' : min(ck), 'max' : max(ck)}.min FROM %s");
    }

    @Test
    public void testCollectionLiteralsWithDurations() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, d1 duration, d2 duration, PRIMARY KEY (pk, ck) )");
        execute("INSERT INTO %s (pk, ck, d1, d2) VALUES (1, 1, 15h, 13h)");
        execute("INSERT INTO %s (pk, ck, d1, d2) VALUES (1, 2, 10h, 12h)");
        execute("INSERT INTO %s (pk, ck, d1, d2) VALUES (1, 3, 11h, 13h)");

        assertRows(execute("SELECT [d1, d2] FROM %s"),
                   row(list(Duration.from("15h"), Duration.from("13h"))),
                   row(list(Duration.from("10h"), Duration.from("12h"))),
                   row(list(Duration.from("11h"), Duration.from("13h"))));

        assertInvalidMessage("Durations are not allowed inside sets: frozen<set<duration>>", "SELECT {d1, d2} FROM %s");

        assertRows(execute("SELECT (map<int, duration>){ck : d1} FROM %s"),
                   row(map(1, Duration.from("15h"))),
                   row(map(2, Duration.from("10h"))),
                   row(map(3, Duration.from("11h"))));

        assertInvalidMessage("Durations are not allowed as map keys: map<duration, int>",
                             "SELECT (map<duration, int>){d1 : ck, d2 :ck} FROM %s");
    }

    @Test
    public void testSelectUDTLiteral() throws Throwable
    {
        String type = createType("CREATE TYPE %s(a int, b text)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ")");

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", 0, userType("a", 3, "b", "foo"));

        assertInvalidMessage("Cannot infer type for term", "SELECT { a: 4, b: 'bar'} FROM %s");

        assertRows(execute("SELECT k, v, (" + type + "){ a: 4, b: 'bar'} FROM %s"),
            row(0, userType("a", 3, "b", "foo"), userType("a", 4, "b", "bar"))
        );

        assertRows(execute("SELECT k, v, (" + type + ")({ a: 4, b: 'bar'}) FROM %s"),
            row(0, userType("a", 3, "b", "foo"), userType("a", 4, "b", "bar"))
        );

        assertRows(execute("SELECT k, v, ((" + type + "){ a: 4, b: 'bar'}).a FROM %s"),
                   row(0, userType("a", 3, "b", "foo"), 4)
        );

        assertRows(execute("SELECT k, v, (" + type + "){ a: 4, b: 'bar'}.a FROM %s"),
                   row(0, userType("a", 3, "b", "foo"), 4)
        );

        assertInvalidMessage("Cannot infer type for term", "SELECT { a: 4} FROM %s");

        assertRows(execute("SELECT k, v, (" + type + "){ a: 4} FROM %s"),
            row(0, userType("a", 3, "b", "foo"), userType("a", 4, "b", null))
        );

        assertRows(execute("SELECT k, v, (" + type + "){ b: 'bar'} FROM %s"),
                   row(0, userType("a", 3, "b", "foo"), userType("a", null, "b", "bar"))
        );

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", 1, userType("a", 5, "b", "foo"));
        assertRows(execute("SELECT (" + type + "){ a: max(v.a) , b: 'max'} FROM %s"),
                   row(userType("a", 5, "b", "max"))
        );
        assertRows(execute("SELECT (" + type + "){ a: min(v.a) , b: 'min'} FROM %s"),
                   row(userType("a", 3, "b", "min"))
        );
    }

    @Test
    public void testCollectionLiteralElementTypes() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int, j int, bi bigint, vi varint, t text, " +
                    "v " + type + ", fv frozen<" + type + ">, l list<int>, fl frozen<list<int>>, tp tuple<int, int>)");
        execute("INSERT INTO %s (k, i, j, bi, vi, t, v, fv, l, fl, tp) " +
                "VALUES (0, 7, 8, 9, 10, 'x', {a: 1, b: 2}, {a: 3, b: 4}, [1], [2], (5, 6))");

        // elements that are not of the element type of the collection, between parentheses or not
        assertInvalidMessage("Invalid list literal [v, (i)]: value (i) is not of type " + type, "SELECT [v, (i)] FROM %s");
        assertInvalidMessage("Invalid list literal [v, i]: value i is not of type " + type, "SELECT [v, i] FROM %s");
        assertInvalidMessage("Invalid list literal [i, t]: value t is not of type int", "SELECT [i, t] FROM %s");
        assertInvalidMessage("Invalid list literal [i, (t)]: value (t) is not of type int", "SELECT [i, (t)] FROM %s");
        assertInvalidMessage("Invalid list literal [i, bi]: value bi is not of type int", "SELECT [i, bi] FROM %s");
        assertInvalidMessage("Invalid list literal [i, writetime(i)]: value writetime(i) is not of type int",
                             "SELECT [i, writetime(i)] FROM %s");
        assertInvalidMessage("Invalid list literal [l, (v)]: value (v) is not of type list<int>", "SELECT [l, (v)] FROM %s");
        assertInvalidMessage("Invalid list literal [t]: value t is not of type int", "SELECT [[i], [t]] FROM %s");
        assertInvalidMessage("Invalid set literal {i, (t)}: value (t) is not of type int", "SELECT {i, (t)} FROM %s");
        assertInvalidMessage("Invalid set literal {v, (i)}: value (i) is not of type " + type, "SELECT {v, (i)} FROM %s");
        assertInvalidMessage("Invalid map literal {i: t}: value t is not of type int",
                             "SELECT [(map<int, int>){i: j}, {i: t}] FROM %s");
        assertInvalidMessage("Invalid map literal {t: j}: key t is not of type int",
                             "SELECT [(map<int, int>){i: j}, {t: j}] FROM %s");
        assertInvalidMessage("Invalid user type literal {a: t}: field t is not of type int", "SELECT [v, {a: t}] FROM %s");
        assertInvalidMessage("Invalid list literal [tp, (i, t)]: value (i, t) is not of type frozen<tuple<int, int>>",
                             "SELECT [tp, (i, t)] FROM %s");
        assertInvalidMessage("Invalid tuple literal (1, t): component t is not of type int", "SELECT [tp, (1, t)] FROM %s");

        // elements of the element type of the collection, or whose values are values of it
        Object fv = userType("a", 3, "b", 4);
        assertRows(execute("SELECT [fv, (fv)] FROM %s"), row(list(fv, fv)));
        assertRows(execute("SELECT [(int) 1, i] FROM %s"), row(list(1, 7)));
        assertRows(execute("SELECT [i, (j)] FROM %s"), row(list(7, 8)));
        assertRows(execute("SELECT [i, 1, null] FROM %s"), row(list(7, 1, null)));
        assertRows(execute("SELECT [vi, i] FROM %s"), row(list(BigInteger.valueOf(10), BigInteger.valueOf(7))));
        assertRows(execute("SELECT {i, (j)} FROM %s"), row(set(7, 8)));
        assertRows(execute("SELECT [l, fl] FROM %s"), row(list(list(1), list(2))));
        assertRows(execute("SELECT [fl, l] FROM %s"), row(list(list(2), list(1))));
        assertRows(execute("SELECT [[i], [(j)]] FROM %s"), row(list(list(7), list(8))));
        assertRows(execute("SELECT [(map<int, int>){i: j}, {j: (i)}] FROM %s"), row(list(map(7, 8), map(8, 7))));
        assertRows(execute("SELECT [tp, (i, j)] FROM %s"), row(list(tuple(5, 6), tuple(7, 8))));
        assertRows(execute("SELECT [fv, {a: i, b: 1}] FROM %s"), row(list(fv, userType("a", 7, "b", 1))));
        for (String literal : new String[]{ "[v, fv]", "[fv, v]", "[v, (v)]", "[fv, (v)]", "[v, tp]", "[[v], [fv]]", "[[fv], [v]]" })
            assertRowCount(execute("SELECT " + literal + " FROM %s"), 1);
        assertRows(execute("SELECT to_json([v, (fv)]) FROM %s"), row("[{\"a\": 1, \"b\": 2}, {\"a\": 3, \"b\": 4}]"));
    }

    @Test
    public void testNestedCollectionLiteralsAsFunctionArguments() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int, bi bigint, vi varint, bl blob, " +
                    "tp frozen<tuple<int, int>>, tp3 frozen<tuple<int, int, int>>)");
        execute("INSERT INTO %s (k, i, bi, vi, bl, tp, tp3) VALUES (0, 7, 9, 10, 0xcafe, (5, 6), (1, 2, 3))");
        String varintLists = createFunction(KEYSPACE, "list<frozen<list<varint>>>",
                                            "CREATE FUNCTION %s (x list<frozen<list<varint>>>) CALLED ON NULL INPUT " +
                                            "RETURNS text LANGUAGE java AS 'return String.valueOf(x);'");
        String tupleLists = createFunction(KEYSPACE, "list<frozen<list<frozen<tuple<int, int, int>>>>>",
                                           "CREATE FUNCTION %s (x list<frozen<list<frozen<tuple<int, int, int>>>>>) " +
                                           "CALLED ON NULL INPUT RETURNS text LANGUAGE java AS 'return String.valueOf(x);'");
        String tuplesOfLists = createFunction(KEYSPACE, "list<frozen<tuple<int, frozen<list<varint>>>>>",
                                              "CREATE FUNCTION %s (x list<frozen<tuple<int, frozen<list<varint>>>>>) " +
                                              "CALLED ON NULL INPUT RETURNS text LANGUAGE java AS 'return String.valueOf(x);'");

        // the elements of nested literals are of the element type of the function parameter or of the cast
        assertRowsNet(executeNet("SELECT " + varintLists + "([[i], [bi]]) FROM %s"), row("[[7], [9]]"));
        assertRowsNet(executeNet("SELECT " + varintLists + "([[i], [vi]]) FROM %s"), row("[[7], [10]]"));
        assertRowsNet(executeNet("SELECT " + varintLists + "([[i], [bi, 1]]) FROM %s"), row("[[7], [9, 1]]"));
        // the bind marker keeps the type it has without the function, the element type of the literal, int
        assertRowsNet(executeNet("SELECT " + varintLists + "([[i], [?]]) FROM %s", 11), row("[[7], [11]]"));
        assertRowsNet(executeNet("SELECT " + tupleLists + "([[tp], [tp3]]) FROM %s"), row("[[(5,6,NULL)], [(1,2,3)]]"));
        assertRowsNet(executeNet("SELECT " + tuplesOfLists + "([(i, [i, bi])]) FROM %s"), row("[(7,[7,9])]"));
        assertRowsNet(executeNet("SELECT collection_count([[tp], [tp3]]) FROM %s"), row(2));
        assertEquals(list(list(BigInteger.valueOf(7)), list(BigInteger.valueOf(9))),
                     executeNet("SELECT (list<frozen<list<varint>>>)[[i], [bi]] FROM %s").one().getObject(0));

        // without a function parameter or a cast, they are of the element type of the enclosing literal
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  "Invalid list literal [bi]: value bi is not of type int",
                                  InvalidQueryException.class,
                                  "SELECT [[i], [bi]] FROM %s");
        assertRowsNet(executeNet("SELECT [[bl], [i, 1]] FROM %s"),
                      row(list(list(ByteBufferUtil.hexToBytes("cafe")), list(ByteBufferUtil.bytes(7), ByteBufferUtil.bytes(1)))));
    }

    @Test
    public void testSetLiteralWithCastOrFunctionParameterType() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int, vi varint)");
        execute("INSERT INTO %s (k, i, vi) VALUES (0, 30, 32)");
        String varintSet = createFunction(KEYSPACE, "set<varint>",
                                          "CREATE FUNCTION %s (x set<varint>) CALLED ON NULL INPUT " +
                                          "RETURNS text LANGUAGE java AS 'return String.valueOf(x);'");

        // a set is sorted and deduplicated with the type its elements are serialized with
        assertRowsNet(executeNet("SELECT collection_count((set<varint>){i, 30}) FROM %s"), row(1));
        assertRowsNet(executeNet("SELECT to_json((set<varint>){i, 1}) FROM %s"), row("[1, 30]"));
        assertRowsNet(executeNet("SELECT " + varintSet + "({i, 1}) FROM %s"), row("[1, 30]"));
        assertRowsNet(executeNet("SELECT collection_count((set<varint>){i, vi, 30}) FROM %s"), row(2));
        assertRowsNet(executeNet("SELECT to_json((set<varint>){i, vi, 1}) FROM %s"), row("[1, 30, 32]"));
    }

    @Test
    public void testBindMarkersInCollectionLiteralWithCastOrFunctionParameterType() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        execute("INSERT INTO %s (k, i) VALUES (0, 30)");
        String varintSet = createFunction(KEYSPACE, "set<varint>",
                                          "CREATE FUNCTION %s (x set<varint>) CALLED ON NULL INPUT " +
                                          "RETURNS text LANGUAGE java AS 'return String.valueOf(x);'");
        String varintLists = createFunction(KEYSPACE, "list<frozen<list<varint>>>",
                                            "CREATE FUNCTION %s (x list<frozen<list<varint>>>) CALLED ON NULL INPUT " +
                                            "RETURNS text LANGUAGE java AS 'return String.valueOf(x);'");

        // bind markers keep the element type of the literal if all its elements are of it, as without a cast or function
        for (String selector : new String[]{ "(list<varint>)[i, ?]", varintSet + "({i, ?})", varintLists + "([[i], [?]])" })
        {
            PreparedStatement prepared = sessionNet().prepare(formatQuery("SELECT " + selector + " FROM %s"));
            assertEquals(selector, "int", prepared.getVariables().getType(0).toString());
            assertRowCountNet(sessionNet().execute(prepared.bind(5)), 1);
        }
    }

    @Test
    public void testCollectionLiteralOfCounterAndBigint() throws Throwable
    {
        // counter values are serialized as bigint values
        createTable("CREATE TABLE %s (k bigint PRIMARY KEY, c counter)");
        execute("UPDATE %s SET c = c + 5 WHERE k = 0");
        assertRowsNet(executeNet("SELECT [k, c] FROM %s"), row(list(0L, 5L)));
        assertRowsNet(executeNet("SELECT [k, (c)] FROM %s"), row(list(0L, 5L)));
    }

    @Test
    public void testTupleLiteralWithTooManyComponents() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int, v " + type + ", " +
                    "tp frozen<tuple<int, int>>, tp3 frozen<tuple<int, int, int>>)");
        execute("INSERT INTO %s (k, i, v, tp, tp3) VALUES (0, 7, {a: 1, b: 2}, (5, 6), (1, 2, 3))");
        String tuples = createFunction(KEYSPACE, "list<frozen<tuple<int, int, int>>>",
                                       "CREATE FUNCTION %s (x list<frozen<tuple<int, int, int>>>) CALLED ON NULL INPUT " +
                                       "RETURNS text LANGUAGE java AS 'return String.valueOf(x);'");

        String message = "Invalid tuple literal (1, 2, 3): too many elements. Type frozen<tuple<int, int>> expects 2 but got 3";
        assertInvalidTupleLiteral(message, "SELECT [tp, (1, 2, 3)] FROM %s");
        assertInvalidTupleLiteral(message, "SELECT {tp, (1, 2, 3)} FROM %s");
        assertInvalidTupleLiteral(message, "SELECT [[tp], [(1, 2, 3)]] FROM %s");
        assertInvalidTupleLiteral(message, "SELECT [(tp, tp), (tp, (1, 2, 3))] FROM %s");
        assertInvalidTupleLiteral(message, "SELECT [(map<int, frozen<tuple<int, int>>>){1: tp}, {1: (1, 2, 3)}] FROM %s");
        assertInvalidTupleLiteral("Invalid tuple literal (1, 2, 3, 4): too many elements. Type frozen<tuple<int, int, int>> expects 3 but got 4",
                                  "SELECT [tp, tp3, (1, 2, 3, 4)] FROM %s");
        assertInvalidTupleLiteral("Invalid tuple literal (1, 2, 3): too many elements. Type " + type + " expects 2 but got 3",
                                  "SELECT [v, (1, 2, 3)] FROM %s");

        // a tuple literal may have fewer components than its type, and as many as the type of the function parameter
        assertRows(execute("SELECT [tp3, (1, 2)] FROM %s"), row(list(tuple(1, 2, 3), tuple(1, 2))));
        assertRows(execute("SELECT [tp3, (i, i)] FROM %s"), row(list(tuple(1, 2, 3), tuple(7, 7))));
        assertRowsNet(executeNet("SELECT " + tuples + "([tp, (1, 2, 3)]) FROM %s"), row("[(5,6,NULL), (1,2,3)]"));
    }

    private void assertInvalidTupleLiteral(String message, String query) throws Throwable
    {
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT), message, InvalidQueryException.class, query);
    }

    @Test
    public void testCollectionLiteralTypeOfWiderElement() throws Throwable
    {
        requireNetwork();
        String type = createType("CREATE TYPE %s (a int, b int)");
        String type3 = createType("CREATE TYPE %s (a int, b int, c int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int, bi bigint, vi varint, v " + type + ", fv3 frozen<" + type3 + ">, " +
                    "tp frozen<tuple<int, int>>, tp3 frozen<tuple<int, int, int>>, tt frozen<tuple<text, int>>, " +
                    "l list<frozen<tuple<int, int>>>, l3 list<frozen<tuple<int, int, int>>>)");
        execute("INSERT INTO %s (k, i, bi, vi, v, fv3, tp, tp3, tt, l, l3) VALUES " +
                "(0, 1, 2, 3, {a: 4, b: 5}, {a: 6, b: 7, c: 8}, (9, 10), (11, 12, 13), ('x', 14), [(15, 16)], [(17, 18, 19)])");

        // the type of the literal is the type of the first element of which all the elements are, whatever their order
        String tupleList = "(list<frozen<tuple<int, int, int>>>)";
        assertSameResultNet("SELECT [tp, tp3] FROM %s", "SELECT " + tupleList + "[tp, tp3] FROM %s");
        assertSameResultNet("SELECT [tp3, tp] FROM %s", "SELECT " + tupleList + "[tp3, tp] FROM %s");
        assertSameResultNet("SELECT [tp, null, tp3] FROM %s", "SELECT " + tupleList + "[tp, null, tp3] FROM %s");
        assertSameResultNet("SELECT [v, tp3] FROM %s", "SELECT " + tupleList + "[v, tp3] FROM %s");
        assertSameResultNet("SELECT [tp, v, tp3] FROM %s", "SELECT " + tupleList + "[tp, v, tp3] FROM %s");
        assertSameResultNet("SELECT {tp, tp3} FROM %s", "SELECT (set<frozen<tuple<int, int, int>>>){tp, tp3} FROM %s");
        assertSameResultNet("SELECT [i, vi] FROM %s", "SELECT (list<varint>)[i, vi] FROM %s");
        assertEquals(executeNet("SELECT [l3, l3] FROM %s").getColumnDefinitions().getType(0),
                     executeNet("SELECT [l, l3] FROM %s").getColumnDefinitions().getType(0));
        assertSameResultNet("SELECT [[tp], [tp3]] FROM %s",
                            "SELECT (list<frozen<list<frozen<tuple<int, int, int>>>>>)[[tp], [tp3]] FROM %s");

        List<com.datastax.driver.core.TupleValue> tuples = executeNet("SELECT [tp, tp3] FROM %s").one()
                                                                    .getList(0, com.datastax.driver.core.TupleValue.class);
        assertEquals(10, tuples.get(0).getInt(1));
        assertTrue(tuples.get(0).isNull(2));
        assertEquals(13, tuples.get(1).getInt(2));

        com.datastax.driver.core.ResultSet result = executeNet("SELECT [v, fv3] FROM %s");
        assertEquals(executeNet("SELECT [fv3, fv3] FROM %s").getColumnDefinitions().getType(0),
                     result.getColumnDefinitions().getType(0));
        List<com.datastax.driver.core.UDTValue> values = result.one().getList(0, com.datastax.driver.core.UDTValue.class);
        assertEquals(5, values.get(0).getInt("b"));
        assertTrue(values.get(0).isNull("c"));
        assertEquals(8, values.get(1).getInt("c"));

        assertRowsNet(executeNet("SELECT collection_count([[tp], [tp3]]) FROM %s"), row(2));

        // the first element type is kept when all the elements are of it
        assertEquals(executeNet("SELECT [tp, tp] FROM %s").getColumnDefinitions().getType(0),
                     executeNet("SELECT [tp, v] FROM %s").getColumnDefinitions().getType(0));
        assertEquals(executeNet("SELECT [v, v] FROM %s").getColumnDefinitions().getType(0),
                     executeNet("SELECT [v, tp] FROM %s").getColumnDefinitions().getType(0));

        // no element type of which all the elements are
        assertInvalidMessage("Invalid list literal [tp, tt, tp3]: value tt is not of type frozen<tuple<int, int>>",
                             "SELECT [tp, tt, tp3] FROM %s");
        assertInvalidMessage("Invalid list literal [i, bi]: value bi is not of type int", "SELECT [i, bi] FROM %s");
        assertInvalidMessage("Invalid list literal [tp3, tt]: value tt is not of type frozen<tuple<int, int, int>>",
                             "SELECT [tp3, tt] FROM %s");
    }

    @Test
    public void testCollectionLiteralTypeOfWiderNativeElement() throws Throwable
    {
        requireNetwork();
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int, b bigint, vi varint, at ascii, txt text, bl blob, " +
                    "d date, tm time, ts timestamp, u uuid, tu timeuuid)");
        execute("INSERT INTO %s (k, i, b, vi, at, txt, bl, d, tm, ts, u, tu) VALUES (0, 1, 2, 3, 'a', 'b', 0xcafe, " +
                "'2020-01-01', '01:00:00', '2020-01-01 00:00:00+0000', 3e5e9a3a-3f6e-4d0a-9a39-7c2b5e3f4a10, " +
                "6d3d1c40-c25b-11ee-8000-000000000000)");

        // a text reads the values of an ascii, a blob those of any type, a varint those of an int or a bigint and a
        // uuid those of a timeuuid
        assertSameResultNet("SELECT [at, txt] FROM %s", "SELECT (list<text>)[at, txt] FROM %s");
        assertSameResultNet("SELECT {at, txt} FROM %s", "SELECT (set<text>){at, txt} FROM %s");
        assertSameResultNet("SELECT [i, bl] FROM %s", "SELECT (list<blob>)[i, bl] FROM %s");
        assertSameResultNet("SELECT [at, txt, bl] FROM %s", "SELECT (list<blob>)[at, txt, bl] FROM %s");
        assertSameResultNet("SELECT [b, vi] FROM %s", "SELECT (list<varint>)[b, vi] FROM %s");
        assertSameResultNet("SELECT [tu, u] FROM %s", "SELECT (list<uuid>)[tu, u] FROM %s");

        // a type whose values are those of another type but mean other values doesn't widen it
        assertInvalidMessage("Invalid list literal [i, d]: value d is not of type int", "SELECT [i, d] FROM %s");
        assertInvalidMessage("Invalid list literal [b, tm]: value tm is not of type bigint", "SELECT [b, tm] FROM %s");
        assertInvalidMessage("Invalid list literal [ts, vi]: value vi is not of type timestamp", "SELECT [ts, vi] FROM %s");
        assertInvalidMessage("Invalid list literal [d]: value d is not of type int", "SELECT [[i], [d]] FROM %s");
        assertInvalidMessage("Invalid list literal [(i, i), (d, i)]: value (d, i) is not of type frozen<tuple<int, int>>",
                             "SELECT [(i, i), (d, i)] FROM %s");
    }

    private void assertSameResultNet(String query, String expectedQuery)
    {
        com.datastax.driver.core.ResultSet result = executeNet(query);
        com.datastax.driver.core.ResultSet expected = executeNet(expectedQuery);
        assertEquals(expected.getColumnDefinitions().getType(0), result.getColumnDefinitions().getType(0));
        com.datastax.driver.core.Row row = result.one();
        com.datastax.driver.core.Row expectedRow = expected.one();
        assertEquals(expectedRow.getBytesUnsafe(0), row.getBytesUnsafe(0));
        assertEquals(expectedRow.getObject(0), row.getObject(0));
    }

    @Test
    public void testInvalidSelect() throws Throwable
    {
        // Creates a table just so we can reference it in the (invalid) SELECT below
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");

        assertInvalidMessage("Cannot infer type for term", "SELECT ? FROM %s");
        assertInvalidMessage("Cannot infer type for term", "SELECT k, ? FROM %s");

        assertInvalidMessage("Cannot infer type for term", "SELECT k, null FROM %s");
    }

    private void assertColumnSpec(ColumnSpecification spec, String expectedName, AbstractType<?> expectedType)
    {
        assertEquals(expectedName, spec.name.toString());
        assertEquals(expectedType, spec.type);
    }

    @Test
    public void testSelectPrepared() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, t text, PRIMARY KEY (pk, ck) )");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 1, 'one')");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 2, 'two')");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 3, 'three')");

        String query = "SELECT (int)?, (decimal):adecimal, (text)?, (tuple<int,text>):atuple, pk, ck, t FROM %s WHERE pk = ?";
        ResultMessage.Prepared prepared = prepare(query);

        List<ColumnSpecification> boundNames = prepared.metadata.names;

        // 5 bound variables
        assertEquals(5, boundNames.size());
        assertColumnSpec(boundNames.get(0), "[selection]", Int32Type.instance);
        assertColumnSpec(boundNames.get(1), "adecimal", DecimalType.instance);
        assertColumnSpec(boundNames.get(2), "[selection]", UTF8Type.instance);
        assertColumnSpec(boundNames.get(3), "atuple", TypeParser.parse("TupleType(Int32Type,UTF8Type)"));
        assertColumnSpec(boundNames.get(4), "pk", Int32Type.instance);


        List<ColumnSpecification> resultNames = prepared.resultMetadata.names;

        // 7 result "columns"
        assertEquals(7, resultNames.size());
        assertColumnSpec(resultNames.get(0), "(int)?", Int32Type.instance);
        assertColumnSpec(resultNames.get(1), "(decimal)?", DecimalType.instance);
        assertColumnSpec(resultNames.get(2), "(text)?", UTF8Type.instance);
        assertColumnSpec(resultNames.get(3), "(tuple<int, text>)?", TypeParser.parse("TupleType(Int32Type,UTF8Type)"));
        assertColumnSpec(resultNames.get(4), "pk", Int32Type.instance);
        assertColumnSpec(resultNames.get(5), "ck", Int32Type.instance);
        assertColumnSpec(resultNames.get(6), "t", UTF8Type.instance);

        assertRows(execute(query, 88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"), 1),
                   row(88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"),
                       1, 1, "one"),
                   row(88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"),
                       1, 2, "two"),
                   row(88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"),
                       1, 3, "three"));
    }

    @Test
    public void testConstantFunctionArgs() throws Throwable
    {
        String fInt = createFunction(KEYSPACE,
                                     "int,int",
                                     "CREATE FUNCTION %s (val1 int, val2 int) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS int " +
                                     "LANGUAGE java\n" +
                                     "AS 'return Math.max(val1, val2);';");
        String fFloat = createFunction(KEYSPACE,
                                       "float,float",
                                       "CREATE FUNCTION %s (val1 float, val2 float) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS float " +
                                       "LANGUAGE java\n" +
                                       "AS 'return Math.max(val1, val2);';");
        String fText = createFunction(KEYSPACE,
                                      "text,text",
                                      "CREATE FUNCTION %s (val1 text, val2 text) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE java\n" +
                                      "AS 'return val2;';");
        String fAscii = createFunction(KEYSPACE,
                                       "ascii,ascii",
                                       "CREATE FUNCTION %s (val1 ascii, val2 ascii) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS ascii " +
                                       "LANGUAGE java\n" +
                                       "AS 'return val2;';");
        String fTimeuuid = createFunction(KEYSPACE,
                                          "timeuuid,timeuuid",
                                          "CREATE FUNCTION %s (val1 timeuuid, val2 timeuuid) " +
                                          "CALLED ON NULL INPUT " +
                                          "RETURNS timeuuid " +
                                          "LANGUAGE java\n" +
                                          "AS 'return val2;';");

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, valInt int, valFloat float, valText text, valAscii ascii, valTimeuuid timeuuid)");
        execute("INSERT INTO %s (pk, valInt, valFloat, valText, valAscii, valTimeuuid) " +
                "VALUES (1, 10, 10.0, '100', '100', 2deb23e0-96b5-11e5-b26d-a939dd1405a3)");

        assertRows(execute("SELECT pk, " + fInt + "(valInt, 100) FROM %s"),
                   row(1, 100));
        assertRows(execute("SELECT pk, " + fInt + "(valInt, (int)100) FROM %s"),
                   row(1, 100));
        assertInvalidMessage("Type error: (bigint)100 cannot be passed as argument 1 of function",
                             "SELECT pk, " + fInt + "(valInt, (bigint)100) FROM %s");
        assertRows(execute("SELECT pk, " + fFloat + "(valFloat, (float)100.00) FROM %s"),
                   row(1, 100f));
        assertRows(execute("SELECT pk, " + fText + "(valText, 'foo') FROM %s"),
                   row(1, "foo"));
        assertRows(execute("SELECT pk, " + fAscii + "(valAscii, (ascii)'foo') FROM %s"),
                   row(1, "foo"));
        assertRows(execute("SELECT pk, " + fTimeuuid + "(valTimeuuid, (timeuuid)34617f80-96b5-11e5-b26d-a939dd1405a3) FROM %s"),
                   row(1, UUID.fromString("34617f80-96b5-11e5-b26d-a939dd1405a3")));

        // ambiguous

        String fAmbiguousFunc1 = createFunction(KEYSPACE,
                                                "int,bigint",
                                                "CREATE FUNCTION %s (val1 int, val2 bigint) " +
                                                "CALLED ON NULL INPUT " +
                                                "RETURNS bigint " +
                                                "LANGUAGE java\n" +
                                                "AS 'return Math.max((long)val1, val2);';");
        assertRows(execute("SELECT pk, " + fAmbiguousFunc1 + "(valInt, 100) FROM %s"),
                   row(1, 100L));
        createFunctionOverload(fAmbiguousFunc1, "int,int",
                                                "CREATE FUNCTION %s (val1 int, val2 int) " +
                                                "CALLED ON NULL INPUT " +
                                                "RETURNS int " +
                                                "LANGUAGE java\n" +
                                                "AS 'return Math.max(val1, val2);';");
        assertRows(execute("SELECT pk, " + fAmbiguousFunc1 + "(valInt, 100) FROM %s"),
                   row(1, 100));
    }

    @Test
    public void testPreparedFunctionArgs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, t text, i int, PRIMARY KEY (pk, ck) )");
        execute("INSERT INTO %s (pk, ck, t, i) VALUES (1, 1, 'one', 50)");
        execute("INSERT INTO %s (pk, ck, t, i) VALUES (1, 2, 'two', 100)");
        execute("INSERT INTO %s (pk, ck, t, i) VALUES (1, 3, 'three', 150)");

        String fIntMax = createFunction(KEYSPACE,
                                        "int,int",
                                        "CREATE FUNCTION %s (val1 int, val2 int) " +
                                        "CALLED ON NULL INPUT " +
                                        "RETURNS int " +
                                        "LANGUAGE java\n" +
                                        "AS 'return Math.max(val1, val2);';");

        // weak typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s", 100),
                   row(1, 1, 100),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s", 200),
                   row(1, 1, 200),
                   row(1, 2, 200),
                   row(1, 3, 200));

        // explicit typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s", 100),
                   row(1, 1, 100),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s", 200),
                   row(1, 1, 200),
                   row(1, 2, 200),
                   row(1, 3, 200));

        // weak typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(1,1)", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(2,1)", 0));

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(?,1)", 0, 1),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(?,1)", 0, 2));

        // explicit typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)1,(int)1)", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)2,(int)1)", 0));

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)?,(int)1)", 0, 1),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)?,(int)1)", 0, 2));

        assertInvalidMessage("Invalid unset value for argument", "SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)1,(int)1)", unset());
    }

    @Test
    public void testInsertUpdateDelete() throws Throwable
    {
        String fIntMax = createFunction(KEYSPACE,
                                        "int,int",
                                        "CREATE FUNCTION %s (val1 int, val2 int) " +
                                        "CALLED ON NULL INPUT " +
                                        "RETURNS int " +
                                        "LANGUAGE java\n" +
                                        "AS 'return Math.max(val1, val2);';");

        createTable("CREATE TABLE %s (pk int, ck int, t text, i int, PRIMARY KEY (pk, ck) )");

        execute("UPDATE %s SET i = " + fIntMax + "(100, 200) WHERE pk = 1 AND ck = 1");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 1"),
                   row(200));

        execute("UPDATE %s SET i = " + fIntMax + "(100, 300) WHERE pk = 1 AND ck = " + fIntMax + "(1,2)");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 2"),
                   row(300));

        execute("DELETE FROM %s WHERE pk = 1 AND ck = " + fIntMax + "(1,2)");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 2"));

        execute("INSERT INTO %s (pk, ck, i) VALUES (1, " + fIntMax + "(1,2), " + fIntMax + "(100, 300))");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 2"),
                   row(300));
    }
}
