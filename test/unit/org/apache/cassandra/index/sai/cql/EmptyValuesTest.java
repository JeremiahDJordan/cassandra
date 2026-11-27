/*
 * Copyright DataStax, Inc.
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

import org.junit.Test;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.quicktheories.QuickTheory.qt;

/**
 * Tests that empty values are only indexed for literal indexes. See CNDB-12774 for more details.
 */
public class EmptyValuesTest extends SAITester
{
    @Test
    public void testEmptyValues()
    {
        qt().forAll(AbstractTypeGenerators.primitiveTypeGen()).checkAssert(type -> {

            CQL3Type cql3Type = type.asCQL3Type();
            if (type.allowsEmpty() && StorageAttachedIndex.SUPPORTED_TYPES.contains(cql3Type))
            {
                testEmptyValues(cql3Type);
            }
        });
    }

    @Test
    public void testEmptyFieldInClusteringRejected()
    {
        String udt = createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (pk int, ck frozen<tuple<int, int>>, ck2 frozen<" + udt + ">, v int, PRIMARY KEY (pk, ck, ck2)) WITH memtable = {'class': 'SkipListMemtable'}");
        String index = createIndex(String.format(CREATE_INDEX_TEMPLATE, 'v'));
        String error = String.format(StorageAttachedIndex.EMPTY_CLUSTERING_FIELD_ERROR, index);

        assertInvalidMessage(error, "INSERT INTO %s (pk, ck, ck2, v) VALUES (0, (blobAsInt(0x), 1), {a: 1, b: 1}, 1)");
        assertInvalidMessage(error, "INSERT INTO %s (pk, ck, ck2, v) VALUES (0, (1, 1), {a: 1, b: blobAsInt(0x)}, 1)");
        assertInvalidMessage(error, "DELETE FROM %s WHERE pk = 0 AND ck = (blobAsInt(0x), 1) AND ck2 = {a: 1, b: 1}");

        execute("INSERT INTO %s (pk, ck, ck2, v) VALUES (0, (null, 1), {a: 1, b: null}, 1)");
        assertRows(execute("SELECT ck FROM %s WHERE v = 1"), row(tuple(null, 1)));
    }

    @Test
    public void testEmptyFieldInCollectionInClusteringRejected()
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<list<frozen<tuple<int, int>>>>, v int, PRIMARY KEY (pk, ck)) WITH memtable = {'class': 'SkipListMemtable'}");
        String index = createIndex(String.format(CREATE_INDEX_TEMPLATE, 'v'));

        assertInvalidMessage(String.format(StorageAttachedIndex.EMPTY_CLUSTERING_FIELD_ERROR, index),
                             "INSERT INTO %s (pk, ck, v) VALUES (0, [(blobAsInt(0x), 1)], 1)");
    }

    @Test
    public void testEmptyFieldInClusteringWrittenBeforeIndex()
    {
        for (String order : new String[]{ "ASC", "DESC" })
        {
            createTable("CREATE TABLE %s (pk int, ck frozen<tuple<int, int>>, v int, PRIMARY KEY (pk, ck)) WITH memtable = {'class': 'SkipListMemtable'} AND CLUSTERING ORDER BY (ck " + order + ")");
            disableCompaction();
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, (null, 1), 1)");
            flush();
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, (blobAsInt(0x), 1), 1)");
            flush();
            createIndex(String.format(CREATE_INDEX_TEMPLATE, 'v'));

            assertThatThrownBy(this::compact).hasStackTraceContaining("its clustering has an empty value");
        }
    }

    private void testEmptyValues(CQL3Type type)
    {
        createTable(String.format("CREATE TABLE %%s (k int PRIMARY KEY, v %s)", type));
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", EMPTY_BYTE_BUFFER);
        flush();
        createIndex(String.format(CREATE_INDEX_TEMPLATE, 'v'));

        boolean indexed = TypeUtil.isLiteral(type.getType());

        Assertions.assertThat(execute("SELECT * FROM %s WHERE v = ?", EMPTY_BYTE_BUFFER)).hasSize(indexed ? 1 : 0);

        execute("INSERT INTO %s (k, v) VALUES (1, ?)", EMPTY_BYTE_BUFFER);
        Assertions.assertThat(execute("SELECT * FROM %s WHERE v = ?", EMPTY_BYTE_BUFFER)).hasSize(indexed ? 2 : 0);

        flush();
        Assertions.assertThat(execute("SELECT * FROM %s WHERE v = ?", EMPTY_BYTE_BUFFER)).hasSize(indexed ? 2 : 0);
    }
}
