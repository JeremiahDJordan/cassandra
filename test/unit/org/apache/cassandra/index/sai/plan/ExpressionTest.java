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
import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

public class ExpressionTest
{

    @Test
    public void testBoundHashCode()
    {
        ByteBuffer buf1 = UTF8Type.instance.decompose("blah");
        Expression.Bound b1 = new Expression.Bound(buf1, UTF8Type.instance, true);
        ByteBuffer buf2 = UTF8Type.instance.decompose("blah");
        Expression.Bound b2 = new Expression.Bound(buf2, UTF8Type.instance, true);
        assertEquals(b1, b2);
        assertEquals(b1.hashCode(), b2.hashCode());
    }

    @Test
    public void testNotMatchingBoundHashCode()
    {
        ByteBuffer buf1 = UTF8Type.instance.decompose("blah");
        Expression.Bound b1 = new Expression.Bound(buf1, UTF8Type.instance, true);
        ByteBuffer buf2 = UTF8Type.instance.decompose("blah2");
        Expression.Bound b2 = new Expression.Bound(buf2, UTF8Type.instance, true);
        assertNotEquals(b1, b2);
        assertNotEquals(b1.hashCode(), b2.hashCode());
    }

    @Test
    public void toStringRedactionTest()
    {
        // test a few operators, just enough to see different variants to the string representations
        Expression expression = makeExpression(Operator.EQ);
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: EQ, lower: (?, true), upper: (?, true), exclusions: []}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: EQ, lower: (sensitive, true), upper: (sensitive, true), exclusions: []}");

        expression = makeExpression(Operator.GT);
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (?, false), upper: (null, false), exclusions: []}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (sensitive, false), upper: (null, false), exclusions: []}");

        expression = makeExpression(Operator.GTE);
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (?, true), upper: (null, false), exclusions: []}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (sensitive, true), upper: (null, false), exclusions: []}");

        expression = makeExpression(Operator.LT);
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (null, false), upper: (?, false), exclusions: []}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (null, false), upper: (sensitive, false), exclusions: []}");

        expression = makeExpression(Operator.LTE);
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (null, false), upper: (?, true), exclusions: []}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (null, false), upper: (sensitive, true), exclusions: []}");

        // test exclusions, which also need redaction
        expression = new Expression(SAITester.createIndexContext("col", UTF8Type.instance));
        expression.add(Operator.GT, UTF8Type.instance.decompose("sensitive_1"));
        expression.add(Operator.NEQ, UTF8Type.instance.decompose("sensitive_2"));
        assertThat(expression.toString(true))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (?, false), upper: (null, false), exclusions: [?]}");
        assertThat(expression.toString(false))
                .isEqualTo("Expression{name: col, op: RANGE, lower: (sensitive_1, false), upper: (null, false), exclusions: [sensitive_2]}");
    }

    @Test
    public void testWildcardFreePatternsAreEqualities()
    {
        // wildcard-free LIKE patterns (LIKE_MATCHES without '%' or '_') match a single term: served as equalities
        // on their unescaped literal
        assertEquals(Expression.Op.EQ, makeExpression(Operator.LIKE_MATCHES, "abc").getOp());
        assertEquals(makeExpression(Operator.EQ, "abc"), makeExpression(Operator.LIKE_MATCHES, "abc"));
        assertEquals(makeExpression(Operator.EQ, "é日😀_%\\"), makeExpression(Operator.LIKE_MATCHES, "é日😀\\_\\%\\\\"));
        assertEquals(Expression.Op.AUTOMATON, makeExpression(Operator.LIKE_MATCHES, "a%c").getOp());
        assertEquals(Expression.Op.AUTOMATON, makeExpression(Operator.LIKE_MATCHES, "a_c").getOp());
        assertEquals(Expression.Op.AUTOMATON, makeExpression(Operator.LIKE_MATCHES, "a\\\\_").getOp());
        assertEquals(Expression.Op.AUTOMATON, makeExpression(Operator.LIKE_MATCHES, "%abc%").getOp());
        // the other pattern operators carry the literal left once their leading/trailing wildcards were trimmed
        assertEquals(Expression.Op.AUTOMATON, makeExpression(Operator.LIKE_SUFFIX, "abc").getOp());
        assertEquals(Expression.Op.AUTOMATON, makeExpression(Operator.LIKE_CONTAINS, "abc").getOp());
        assertEquals(Expression.Op.PREFIX, makeExpression(Operator.LIKE_PREFIX, "abc").getOp());
    }

    @Test
    public void testPostFilterDecimalVarintAndInetValues() throws Exception
    {
        // values equal to the bound but serialized differently: 2.50 for 2.5, and 1 with a redundant sign byte
        DecimalType decimal = DecimalType.instance;
        assertPostFilter(decimal, decimal.decompose(new BigDecimal("2.5")), decimal.decompose(new BigDecimal("2.50")),
                         decimal.decompose(new BigDecimal("1")), decimal.decompose(new BigDecimal("4")));
        IntegerType varint = IntegerType.instance;
        assertPostFilter(varint, varint.decompose(BigInteger.ONE), ByteBuffer.wrap(new byte[]{ 0, 1 }),
                         varint.decompose(BigInteger.valueOf(-300)), varint.decompose(BigInteger.valueOf(300)));
        InetAddressType inet = InetAddressType.instance;
        assertPostFilter(inet, inet.decompose(InetAddress.getByName("127.0.0.2")), inet.decompose(InetAddress.getByName("127.0.0.2")),
                         inet.decompose(InetAddress.getByName("127.0.0.1")), inet.decompose(InetAddress.getByName("127.0.0.3")));
    }

    private static void assertPostFilter(AbstractType<?> type, ByteBuffer bound, ByteBuffer value, ByteBuffer lower, ByteBuffer upper)
    {
        assertTrue(makeExpression(type, Operator.EQ, bound).isSatisfiedBy(value));
        assertFalse(makeExpression(type, Operator.NEQ, bound).isSatisfiedBy(value));
        assertTrue(makeExpression(type, Operator.GTE, bound).isSatisfiedBy(value));
        assertFalse(makeExpression(type, Operator.GT, bound).isSatisfiedBy(value));
        assertTrue(makeExpression(type, Operator.LTE, bound).isSatisfiedBy(value));
        assertFalse(makeExpression(type, Operator.LT, bound).isSatisfiedBy(value));
        assertTrue(makeExpression(type, Operator.GT, lower).add(Operator.LT, upper).isSatisfiedBy(value));
        assertFalse(makeExpression(type, Operator.GT, upper).isSatisfiedBy(value));
        assertFalse(makeExpression(type, Operator.GT, lower).add(Operator.LT, upper).add(Operator.NEQ, bound).isSatisfiedBy(value));
        assertTrue(makeExpression(type, Operator.GT, lower).add(Operator.LT, upper).add(Operator.NEQ, upper).isSatisfiedBy(value));
    }

    private static Expression makeExpression(AbstractType<?> type, Operator op, ByteBuffer value)
    {
        Expression expression = new Expression(SAITester.createIndexContext("col", type));
        expression.add(op, value);
        return expression;
    }

    private static Expression makeExpression(Operator op)
    {
        return makeExpression(op, "sensitive");
    }

    private static Expression makeExpression(Operator op, String value)
    {
        Expression expression = new Expression(SAITester.createIndexContext("col", UTF8Type.instance));
        expression.add(op, UTF8Type.instance.decompose(value));
        return expression;
    }
}
