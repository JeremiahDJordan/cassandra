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

package org.apache.cassandra.index.sai.view;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.IntervalTree;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_INTERVAL_TREE_EXPENSIVE_CHECKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RangeTermTreeTest
{
    static
    {
        // read once, when IntervalTree is loaded
        TEST_INTERVAL_TREE_EXPENSIVE_CHECKS.setBoolean(true);
    }

    private static IndexContext context;

    @BeforeClass
    public static void setup()
    {
        SchemaLoader.prepareServer();
        context = indexContext();
    }

    /**
     * The expensive checks of IntervalTree compare the center of a node, which is an endpoint of one interval, with
     * the endpoint at the same position in the sorted endpoints, which can be an equal endpoint of another interval
     * (CASSANDRA-20866).
     */
    @Test
    public void testIndexesSharingATerm()
    {
        assertTrue("IntervalTree expensive checks must be enabled", IntervalTree.EXPENSIVE_CHECKS);

        SSTableIndex a = index(0, 10);
        SSTableIndex b = index(10, 20);
        SSTableIndex c = index(10, 10);
        SSTableIndex d = index(20, 30);
        TermTree tree = tree(a, b, c, d);

        assertEquals(Set.of(a), tree.search(expression(Operator.EQ, 5)));
        assertEquals(Set.of(a, b, c), tree.search(expression(Operator.EQ, 10)));
        assertEquals(Set.of(b, d), tree.search(expression(Operator.EQ, 20)));
        assertEquals(Set.of(), tree.search(expression(Operator.EQ, 31)));
    }

    @Test
    public void testTermEquality()
    {
        RangeTermTree.Term term = term(1, Version.LATEST);
        assertEquals(term, term(1, Version.LATEST));
        assertEquals(term.hashCode(), term(1, Version.LATEST).hashCode());
        assertNotEquals(term, term(2, Version.LATEST));
        assertNotEquals(term, new RangeTermTree.Term(null, Int32Type.instance, Version.LATEST));
        assertEquals(new RangeTermTree.Term(null, Int32Type.instance, Version.LATEST),
                     new RangeTermTree.Term(null, Int32Type.instance, Version.LATEST));
        if (Version.LATEST != Version.AA)
            assertNotEquals(term, term(1, Version.AA));
    }

    private static RangeTermTree.Term term(int value, Version version)
    {
        return new RangeTermTree.Term(Int32Type.instance.decompose(value), Int32Type.instance, version);
    }

    private static TermTree tree(SSTableIndex... indexes)
    {
        TermTree.Builder builder = new RangeTermTree.Builder(context.getValidator());
        for (SSTableIndex index : indexes)
            builder.add(index);
        return builder.build();
    }

    private static SSTableIndex index(int minTerm, int maxTerm)
    {
        SSTableIndex index = mock(SSTableIndex.class);
        when(index.minTerm()).thenReturn(Int32Type.instance.decompose(minTerm));
        when(index.maxTerm()).thenReturn(Int32Type.instance.decompose(maxTerm));
        when(index.getVersion()).thenReturn(Version.LATEST);
        when(index.getIndexContext()).thenReturn(context);
        // read by the trace logging of RangeTermTree.Builder#addIndex
        when(index.getSSTable()).thenReturn(mock(SSTableReader.class));
        return index;
    }

    private static Expression expression(Operator operator, int value)
    {
        ByteBuffer term = Int32Type.instance.decompose(value);
        return new Expression(context).add(operator, term);
    }

    private static IndexContext indexContext()
    {
        TableMetadata table = TableMetadata.builder("ks", "tb")
                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                           .addRegularColumn("v", Int32Type.instance)
                                           .partitioner(Murmur3Partitioner.instance)
                                           .build();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        options.put("target", "v");
        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata("v_index", IndexMetadata.Kind.CUSTOM, options);
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(table, indexMetadata);
        return new IndexContext(table.keyspace,
                                table.name,
                                table.id,
                                table.partitionKeyType,
                                table.comparator,
                                target.left,
                                target.right,
                                indexMetadata,
                                MockSchema.newCFS(table));
    }
}
