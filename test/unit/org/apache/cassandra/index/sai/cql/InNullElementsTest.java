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
package org.apache.cassandra.index.sai.cql;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.serializers.ListSerializer;

import static org.junit.Assert.assertEquals;

/**
 * Null elements of an IN filter. The coordinators of this version leave them out of the filter or reject them (see
 * SingleColumnRestriction and MultiColumnRestriction), but a coordinator of Apache Cassandra 5.0 sends them to the
 * replicas: a null element matches nothing, and doesn't hide the elements after it.
 */
public class InNullElementsTest extends SAITester
{
    private static final ByteBuffer ONE = Int32Type.instance.decompose(1);
    private static final ByteBuffer TWO = Int32Type.instance.decompose(2);

    /**
     * A filter with null elements, as a replica receives it from an Apache Cassandra 5.0 coordinator, on an indexed
     * and on an unindexed column, alone and with a restriction on another index.
     */
    @Test
    public void testReplicaFilterWithNullElements() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int, u int, w int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (pk, v, u, w) VALUES (?, ?, ?, 1)", i, i, i);

        beforeAndAfterFlush(() -> {
            assertReplicaFilter(Arrays.asList(ONE, TWO), 1, 2);
            assertReplicaFilter(Arrays.asList(ONE, null), 1);
            assertReplicaFilter(Arrays.asList(null, ONE), 1);
            assertReplicaFilter(Arrays.asList(ONE, null, TWO), 1, 2);
            assertReplicaFilter(Arrays.asList(null, null, TWO), 2);
            assertReplicaFilter(Arrays.asList((ByteBuffer) null));
        });
    }

    private void assertReplicaFilter(List<ByteBuffer> elements, Integer... expected)
    {
        Set<Integer> expectedKeys = new TreeSet<>(Arrays.asList(expected));
        ByteBuffer packed = ListSerializer.pack(elements, elements.size());
        for (String column : new String[]{ "v", "u" })
        {
            for (boolean withOtherIndex : new boolean[]{ false, true })
            {
                AbstractReadCommandBuilder builder = Util.cmd(getCurrentColumnFamilyStore()).filterOn(column, Operator.IN, packed.duplicate());
                if (withOtherIndex)
                    builder.filterOn("w", Operator.EQ, 1);
                Set<Integer> keys = new TreeSet<>();
                for (FilteredPartition partition : Util.getAll(builder.build()))
                    keys.add(Int32Type.instance.compose(partition.partitionKey().getKey()));
                assertEquals(String.format("%s IN %s%s", column, elements, withOtherIndex ? " AND w = 1" : ""), expectedKeys, keys);
            }
        }
    }

    /**
     * The multi-column form of IN on a single clustering column, (c) IN ((x), (y)), treats a null element like the
     * single column IN does: where it filters the column, the null matches nothing, and where it selects the
     * clustering, it is rejected, with or without an index. A null tuple, (c) IN (?, ?) with a null bound for a tuple,
     * is a tuple of null elements.
     */
    @Test
    public void testMultiColumnInWithNullElement() throws Throwable
    {
        for (boolean indexed : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int, c1 int, c2 int, v int, PRIMARY KEY (pk, c1, c2))");
            if (indexed)
                createIndex("CREATE CUSTOM INDEX ON %s(c2) USING 'StorageAttachedIndex'");
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (1, 1, 1, 1)");
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (1, 2, 2, 1)");

            beforeAndAfterFlush(() -> {
                // c2 is filtered (c1 isn't restricted): a null element matches nothing
                assertRows(execute("SELECT c2 FROM %s WHERE (c2) IN ((?), (?)) ALLOW FILTERING", 1, 2), row(1), row(2));
                assertRows(execute("SELECT c2 FROM %s WHERE (c2) IN ((?), (?)) ALLOW FILTERING", 1, null), row(1));
                assertRows(execute("SELECT c2 FROM %s WHERE (c2) IN ((?), (?)) ALLOW FILTERING", null, 2), row(2));
                assertRows(execute("SELECT c2 FROM %s WHERE (c2) IN ((?), (?), (?)) ALLOW FILTERING", 1, null, 2), row(1), row(2));
                assertRows(execute("SELECT c2 FROM %s WHERE (c2) IN ((?), (?)) ALLOW FILTERING", null, null));
                // an IN with a single element is an equality, which rejects a null value
                assertInvalidMessage("Unsupported null value for column c2",
                                     "SELECT c2 FROM %s WHERE (c2) IN ((?)) ALLOW FILTERING", (Object) null);
                assertRows(execute("SELECT c2 FROM %s WHERE (c2) IN ((1), (null)) ALLOW FILTERING"), row(1));
                assertRows(execute("SELECT c2 FROM %s WHERE pk = 1 AND (c2) IN ((?), (?)) ALLOW FILTERING", null, 1), row(1));
                assertRows(execute("SELECT c2 FROM %s WHERE (c2) IN ((?), (?)) AND v = 1 ALLOW FILTERING", 2, null), row(2));
                if (indexed)
                    assertRows(execute("SELECT c2 FROM %s WHERE (c2) IN ((?), (?))", 1, null), row(1));

                // the restriction selects the clustering: a null element is rejected
                assertInvalidMessage("Invalid null value in condition for columns: [c1]",
                                     "SELECT c2 FROM %s WHERE pk = 1 AND (c1) IN ((?), (?))", 1, null);
                assertInvalidMessage("Invalid null value in condition for columns: [c1, c2]",
                                     "SELECT c2 FROM %s WHERE pk = 1 AND (c1, c2) IN ((?, ?), (?, ?))", 1, 1, 2, null);

                // a null tuple is a tuple of null elements
                assertRows(execute("SELECT c2 FROM %s WHERE (c2) IN (?, ?) ALLOW FILTERING", tuple(1), null), row(1));
                assertInvalidMessage("Invalid null value in condition for columns: [c1, c2]",
                                     "SELECT c2 FROM %s WHERE pk = 1 AND (c1, c2) IN (?, ?)", tuple(1, 1), null);
            });
        }
    }
}
