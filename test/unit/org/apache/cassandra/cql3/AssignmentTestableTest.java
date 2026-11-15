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

package org.apache.cassandra.cql3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.config.CassandraRelevantProperties.VECTOR_FLOAT_ONLY;

public class AssignmentTestableTest
{
    @BeforeClass
    public static void setupClass()
    {
        VECTOR_FLOAT_ONLY.setBoolean(false);
    }

    @Test
    public void testGetPreferredCompatibleType()
    {
        testGetPreferredCompatibleType(null);

        testGetPreferredCompatibleType(AsciiType.instance, AsciiType.instance);
        testGetPreferredCompatibleType(UTF8Type.instance, UTF8Type.instance);
        testGetPreferredCompatibleType(AsciiType.instance, AsciiType.instance, AsciiType.instance);
        testGetPreferredCompatibleType(UTF8Type.instance, UTF8Type.instance, UTF8Type.instance);
        testGetPreferredCompatibleType(UTF8Type.instance, AsciiType.instance, UTF8Type.instance);
        testGetPreferredCompatibleType(UTF8Type.instance, UTF8Type.instance, AsciiType.instance);

        testGetPreferredCompatibleType(Int32Type.instance, Int32Type.instance);
        testGetPreferredCompatibleType(LongType.instance, LongType.instance);
        testGetPreferredCompatibleType(Int32Type.instance, Int32Type.instance, Int32Type.instance);
        testGetPreferredCompatibleType(LongType.instance, LongType.instance, LongType.instance);
        testGetPreferredCompatibleType(null, Int32Type.instance, LongType.instance); // not assignable
        testGetPreferredCompatibleType(null, LongType.instance, Int32Type.instance); // not assignable
    }

    /**
     * When the element types are all compatible with each other, the type of the first element is preferred, whatever
     * the hash codes of the types.
     */
    @Test
    public void testGetPreferredCompatibleTypeOfMutuallyCompatibleTypes()
    {
        UserType udt = new UserType("ks", ByteBufferUtil.bytes("t"), Collections.singletonList(FieldIdentifier.forUnquoted("a")),
                                    Collections.singletonList(Int32Type.instance), true);
        AbstractType<?> type1 = ListType.getInstance(udt, false);
        AbstractType<?> type2 = ListType.getInstance(udt.freeze(), false);
        List<AbstractType<?>> types12 = Arrays.asList(type1, type2);
        List<AbstractType<?>> types21 = Arrays.asList(type2, type1);
        Assert.assertEquals(type1, AssignmentTestable.getCompatibleTypeIfKnown(types12));
        Assert.assertEquals(type2, AssignmentTestable.getCompatibleTypeIfKnown(types21));

        Assert.assertEquals(ListType.getInstance(type1, false), Lists.getPreferredCompatibleType(types12, t -> t));
        Assert.assertEquals(ListType.getInstance(type2, false), Lists.getPreferredCompatibleType(types21, t -> t));
        Assert.assertEquals(SetType.getInstance(type1, false), Sets.getPreferredCompatibleType(types12, t -> t));
        Assert.assertEquals(SetType.getInstance(type2, false), Sets.getPreferredCompatibleType(types21, t -> t));
        Assert.assertEquals(MapType.getInstance(type1, type2, false),
                            Maps.getPreferredCompatibleType(Arrays.asList(Pair.create(type1, type2), Pair.create(type2, type1)), t -> t));
        Assert.assertEquals(MapType.getInstance(type2, type1, false),
                            Maps.getPreferredCompatibleType(Arrays.asList(Pair.create(type2, type1), Pair.create(type1, type2)), t -> t));

        // frozen user types of several hash codes and a tuple of the same shape
        AbstractType<?> tuple = new TupleType(Collections.singletonList(Int32Type.instance));
        for (int i = 0; i < 10; i++)
        {
            AbstractType<?> frozenUdt = new UserType("ks", ByteBufferUtil.bytes("t" + i), Collections.singletonList(FieldIdentifier.forUnquoted("a")),
                                                     Collections.singletonList(Int32Type.instance), false);
            List<AbstractType<?>> udtFirst = Arrays.asList(frozenUdt, tuple);
            List<AbstractType<?>> tupleFirst = Arrays.asList(tuple, frozenUdt);
            Assert.assertEquals(VectorType.getInstance(frozenUdt, 2), Vectors.getPreferredCompatibleType(udtFirst, t -> t));
            Assert.assertEquals(VectorType.getInstance(tuple, 2), Vectors.getPreferredCompatibleType(tupleFirst, t -> t));
        }
    }

    public void testGetPreferredCompatibleType(AbstractType<?> type, AbstractType<?>... types)
    {
        Assert.assertEquals(type, AssignmentTestable.getCompatibleTypeIfKnown(Arrays.asList(types)));
    }
}
