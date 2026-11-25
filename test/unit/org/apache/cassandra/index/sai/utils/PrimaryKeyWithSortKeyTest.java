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

package org.apache.cassandra.index.sai.utils;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrimaryKeyWithSortKeyTest
{
    private static final int NOW_IN_SEC = 0;

    private ColumnMetadata column;
    private PrimaryKeyWithSortKey key;
    private Row row;

    @Before
    public void setUp()
    {
        column = ColumnMetadata.regularColumn("ks", "table", "value", Int32Type.instance);
        IndexContext context = mock(IndexContext.class);
        when(context.getDefinition()).thenReturn(column);
        SSTableId<?> source = mock(SSTableId.class);
        key = new PrimaryKeyWithScore(context, source, mock(PrimaryKey.class), 1.0f, false);
        row = mock(Row.class);
    }

    @Test
    public void testMissingCellIsInvalid()
    {
        when(row.getCell(column)).thenReturn(null);

        assertFalse(key.isIndexDataValid(row, NOW_IN_SEC));
    }

    @Test
    public void testNonLiveCellIsInvalid()
    {
        Cell<?> cell = mock(Cell.class);
        when(cell.isLive(NOW_IN_SEC)).thenReturn(false);
        doReturn(cell).when(row).getCell(column);

        assertFalse(key.isIndexDataValid(row, NOW_IN_SEC));
    }

    /**
     * The terms of an index of version aa on sstables of byte-comparable version OSS50 are preencoded in OSS50, those
     * of the other indexes and of the older sstables in OSS41, with the same bytes.
     */
    @Test
    public void testTermsPreencodedInDifferentVersions()
    {
        IndexContext context = mock(IndexContext.class);
        doReturn(Int32Type.instance).when(context).getValidator();
        when(context.getDefinition()).thenReturn(column);
        PrimaryKeyWithByteComparable one = keyWithTerm(context, 1, ByteComparable.Version.OSS41);
        PrimaryKeyWithByteComparable two = keyWithTerm(context, 2, ByteComparable.Version.OSS50);

        assertTrue(one.compareTo(two) < 0);
        assertTrue(two.compareTo(one) > 0);
        assertEquals(0, two.compareTo(keyWithTerm(context, 2, ByteComparable.Version.OSS41)));
        Cell<?> cell = mock(Cell.class);
        when(cell.isLive(NOW_IN_SEC)).thenReturn(true);
        when(cell.buffer()).thenReturn(Int32Type.instance.decompose(2));
        doReturn(cell).when(row).getCell(column);
        assertTrue(two.isIndexDataValid(row, NOW_IN_SEC));
        assertFalse(one.isIndexDataValid(row, NOW_IN_SEC));
    }

    /**
     * A DESC bigint column has terms of the same fixed length in every byte-comparable version, and an aa index decodes
     * them in either.
     */
    @Test
    public void testReversedLongTermsInEveryVersion()
    {
        ReversedType<Long> type = ReversedType.getInstance(LongType.instance);
        ByteBuffer value = type.decompose(-42L);
        byte[] oss41 = ByteSourceInverse.readBytes(TypeUtil.asComparableBytes(value, type, ByteComparable.Version.OSS41));
        assertEquals(Long.BYTES, oss41.length);
        assertArrayEquals(oss41, ByteSourceInverse.readBytes(TypeUtil.asComparableBytes(value, type, ByteComparable.Version.OSS50)));
        assertEquals(value, Version.AA.onDiskFormat().decodeFromTrie(ByteComparable.preencoded(ByteComparable.Version.OSS50, oss41), type));
    }

    private static PrimaryKeyWithByteComparable keyWithTerm(IndexContext context, int value, ByteComparable.Version version)
    {
        ByteBuffer term = Int32Type.instance.decompose(value);
        return new PrimaryKeyWithByteComparable(context, mock(SSTableId.class), mock(PrimaryKey.class),
                                                ByteComparable.preencoded(version, TypeUtil.asComparableBytes(term, Int32Type.instance).asByteComparableArray(version)));
    }
}
