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
 * SingleColumnRestriction), but a coordinator of Apache Cassandra 5.0 sends them to the replicas: a null element
 * matches nothing, and doesn't hide the elements after it.
 */
public class InNullElementsTest extends SAITester
{
    private static final ByteBuffer ONE = Int32Type.instance.decompose(1);
    private static final ByteBuffer TWO = Int32Type.instance.decompose(2);

    /**
     * A filter with null elements, as a replica receives it from an Apache Cassandra 5.0 coordinator, on an indexed
     * column, alone and with a restriction on another index.
     */
    @Test
    public void testReplicaFilterWithNullElements() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int, w int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(w) USING 'StorageAttachedIndex'");
        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (pk, v, w) VALUES (?, ?, 1)", i, i);

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
        for (boolean withOtherIndex : new boolean[]{ false, true })
        {
            AbstractReadCommandBuilder builder = Util.cmd(getCurrentColumnFamilyStore()).filterOn("v", Operator.IN, packed.duplicate());
            if (withOtherIndex)
                builder.filterOn("w", Operator.EQ, 1);
            Set<Integer> keys = new TreeSet<>();
            for (FilteredPartition partition : Util.getAll(builder.build()))
                keys.add(Int32Type.instance.compose(partition.partitionKey().getKey()));
            assertEquals(String.format("v IN %s%s", elements, withOtherIndex ? " AND w = 1" : ""), expectedKeys, keys);
        }
    }
}
