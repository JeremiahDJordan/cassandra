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

package org.apache.cassandra.schema;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ColumnMetadata#validateCell} for tuple and user type columns. A multi-cell tuple, which is the type of a
 * dropped non-frozen UDT column, has one cell per field, as a non-frozen UDT has: the cell is valid if its value is
 * valid for the field that its path names.
 */
public class ColumnMetadataValidateCellTest
{
    private static final ImmutableList<AbstractType<?>> FIELD_TYPES = ImmutableList.of(Int32Type.instance, UTF8Type.instance);
    private static final ByteBuffer INT = Int32Type.instance.decompose(7);
    private static final ByteBuffer TEXT = UTF8Type.instance.decompose("x");
    private static final ByteBuffer WHOLE = TupleType.buildValue(INT, TEXT);
    private static final ByteBuffer THREE_BYTES = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
    private static final ByteBuffer NOT_UTF8 = ByteBuffer.wrap(new byte[]{ (byte) 0xff, (byte) 0xfe });

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testMultiCellTuple()
    {
        testMultiCell(dropped(new TupleType(FIELD_TYPES, true)));
    }

    @Test
    public void testNonFrozenUserType()
    {
        testMultiCell(ColumnMetadata.regularColumn("ks", "t", "c", userType(true)));
    }

    @Test
    public void testFrozenTuple()
    {
        testFrozen(dropped(new TupleType(FIELD_TYPES, false)));
    }

    @Test
    public void testFrozenUserType()
    {
        testFrozen(ColumnMetadata.regularColumn("ks", "t", "c", userType(false)));
    }

    private static void testMultiCell(ColumnMetadata column)
    {
        BufferCell.live(column, 1, INT, field(0)).validate();
        BufferCell.live(column, 1, ByteBufferUtil.EMPTY_BYTE_BUFFER, field(0)).validate();
        BufferCell.live(column, 1, TEXT, field(1)).validate();
        BufferCell.tombstone(column, 1, 1, field(1)).validate();

        assertInvalid(BufferCell.live(column, 1, THREE_BYTES, field(0)));
        assertInvalid(BufferCell.live(column, 1, NOT_UTF8, field(1)));
        // the value of the whole tuple is not a field's value
        assertInvalid(BufferCell.live(column, 1, WHOLE, field(0)));
        assertInvalid(BufferCell.live(column, 1, INT, field(2)));
        assertInvalid(BufferCell.live(column, 1, INT, field(-1)));
        assertInvalid(BufferCell.live(column, 1, INT, CellPath.create(THREE_BYTES)));
        assertInvalid(BufferCell.tombstone(column, 1, 1, CellPath.create(THREE_BYTES)));
    }

    private static void testFrozen(ColumnMetadata column)
    {
        BufferCell.live(column, 1, WHOLE).validate();
        BufferCell.live(column, 1, ByteBufferUtil.EMPTY_BYTE_BUFFER).validate();
        BufferCell.tombstone(column, 1, 1).validate();

        assertInvalid(BufferCell.live(column, 1, THREE_BYTES));
        assertInvalid(BufferCell.tombstone(column, 1, 1, field(0)));
    }

    private static void assertInvalid(Cell<?> cell)
    {
        assertThatThrownBy(cell::validate).isInstanceOf(MarshalException.class);
    }

    private static CellPath field(int position)
    {
        return CellPath.create(ShortType.instance.decompose((short) position));
    }

    private static ColumnMetadata dropped(AbstractType<?> type)
    {
        return ColumnMetadata.droppedColumn("ks", "t", ColumnIdentifier.getInterned("c", false), type, ColumnMetadata.Kind.REGULAR, null);
    }

    private static UserType userType(boolean multiCell)
    {
        return new UserType("ks",
                            ByteBufferUtil.bytes("u"),
                            Arrays.asList(FieldIdentifier.forUnquoted("a"), FieldIdentifier.forUnquoted("b")),
                            FIELD_TYPES,
                            multiCell);
    }
}
