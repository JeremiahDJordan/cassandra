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

package org.apache.cassandra.cql3.terms;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.fail;

public class InMarkerTest extends CQLTester
{
    @Test
    public void testNotEnoughBytesThrowsInvalidRequest()
    {
        assertInMarkerRejectsMalformedValue(new byte[]{ 0, 0, 0, 1 });
    }

    @Test
    public void testExtraneousBytesThrowsInvalidRequest()
    {
        assertInMarkerRejectsMalformedValue(new byte[]{ 0, 0, 0, 0, 9, 9 });
    }

    @Test
    public void testMalformedElementThrowsInvalidRequest()
    {
        assertInMarkerRejectsMalformedValue(new byte[]{ 0, 0, 0, 1, 0, 0, 0, 2, 1, 2 });
    }

    @Test
    public void testEmptyElement() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, blobAsInt(0x), 0)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 1, blobAsInt(0x))");
        execute("INSERT INTO %s (pk, ck) VALUES (0, 2)");

        ByteBuffer values = ListType.getInstance(Int32Type.instance, false).pack(List.of(EMPTY_BYTE_BUFFER));
        assertRows(execute("SELECT ck FROM %s WHERE pk = 0 AND ck IN ?", values), row(EMPTY_BYTE_BUFFER));
        assertRows(execute("SELECT ck FROM %s WHERE v IN ? ALLOW FILTERING", values), row(1));
        assertRows(execute("UPDATE %s SET v = 3 WHERE pk = 0 AND ck = 2 IF v IN ?", values), row(false, null));
        assertRows(execute("UPDATE %s SET v = 3 WHERE pk = 0 AND ck = 1 IF v IN ?", values), row(true));
        execute("DELETE FROM %s WHERE pk = 0 AND ck IN ?", values);
        assertRows(execute("SELECT ck, v FROM %s WHERE pk = 0"), row(1, 3), row(2, null));
    }

    @Test
    public void testUnsortedSetElement() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck frozen<set<int>>, v vector<frozen<set<int>>, 1>, PRIMARY KEY (pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, {1, 2}, [{1, 2}])");

        SetType<Integer> setType = SetType.getInstance(Int32Type.instance, false);
        ByteBuffer unsorted = setType.pack(List.of(bytes(2), bytes(1), bytes(2)));
        ByteBuffer values = ListType.getInstance(setType, false).pack(List.of(unsorted));
        assertRows(execute("SELECT ck FROM %s WHERE pk = 0 AND ck IN ?", values), row(set(1, 2)));

        VectorType<Set<Integer>> vectorType = VectorType.getInstance(setType, 1);
        values = ListType.getInstance(vectorType, false).pack(List.of(vectorType.pack(List.of(unsorted))));
        assertRows(execute("SELECT ck FROM %s WHERE v IN ? ALLOW FILTERING", values), row(set(1, 2)));
    }

    private void assertInMarkerRejectsMalformedValue(byte[] malformedListBytes)
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        SelectStatement select = (SelectStatement) parseStatement("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE pk IN ?");

        QueryOptions options = QueryOptions.forInternalCalls(Collections.singletonList(ByteBuffer.wrap(malformedListBytes)));
        try
        {
            select.getQuery(options, QueryState.forInternalCalls().getNowInSeconds());
            fail("Expected InvalidRequestException to be thrown for a malformed IN marker value");
        }
        catch (InvalidRequestException e)
        {
            // expected
        }
    }
}