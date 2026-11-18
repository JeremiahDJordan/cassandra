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

package org.apache.cassandra.index.sai.disk.v9;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Stopwatch;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The order of the partitions that {@link V9SSTableComponentsWriter} accepts: the SSTable order, by token and then by
 * key for distinct keys that share a token.
 */
public class V9SSTableComponentsWriterTest extends SaiRandomizedTest
{
    private static final TableMetadata TABLE = TableMetadata.builder("test", "test")
                                                            .partitioner(Murmur3Partitioner.instance)
                                                            .addPartitionKeyColumn("pk", BytesType.instance)
                                                            .addRegularColumn("v", BytesType.instance)
                                                            .build();

    private final V9RowAwarePrimaryKeyFactory factory = new V9RowAwarePrimaryKeyFactory(TABLE.comparator);

    @Test
    public void testKeysWithTheSameToken() throws Throwable
    {
        // three keys with one token, then a key with a greater token
        List<DecoratedKey> keys = Util.murmurCollisions(Murmur3Partitioner.instance, "key", 3);
        DecoratedKey other = keys.get(0);
        for (int i = 0; other.getToken().compareTo(keys.get(0).getToken()) <= 0; i++)
            other = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(i));
        keys.add(other);

        V9SSTableComponentsWriter writer = newWriter();
        for (DecoratedKey key : keys)
            add(writer, key);
        writer.complete(Stopwatch.createStarted());
    }

    @Test
    public void testDuplicateKey() throws Throwable
    {
        DecoratedKey key = Util.murmurCollisions(Murmur3Partitioner.instance, "key", 1).get(0);
        V9SSTableComponentsWriter writer = newWriter();
        add(writer, key);
        assertRejected(writer, key);
        writer.abort(null);
    }

    @Test
    public void testKeysWithTheSameTokenOutOfOrder() throws Throwable
    {
        List<DecoratedKey> keys = Util.murmurCollisions(Murmur3Partitioner.instance, "key", 2);
        V9SSTableComponentsWriter writer = newWriter();
        add(writer, keys.get(1));
        assertRejected(writer, keys.get(0));
        writer.abort(null);
    }

    @Test
    public void testTokensOutOfOrder() throws Throwable
    {
        List<DecoratedKey> keys = new ArrayList<>();
        for (int i = 0; i < 2; i++)
            keys.add(Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(i)));
        keys.sort(Comparator.naturalOrder());
        V9SSTableComponentsWriter writer = newWriter();
        add(writer, keys.get(1));
        assertRejected(writer, keys.get(0));
        writer.abort(null);
    }

    private V9SSTableComponentsWriter newWriter() throws Throwable
    {
        IndexDescriptor descriptor = newClusteringIndexDescriptor(TABLE);
        return new V9SSTableComponentsWriter(descriptor.newPerSSTableComponentsForWrite());
    }

    private void add(V9SSTableComponentsWriter writer, DecoratedKey key) throws Throwable
    {
        writer.startPartition(key, -1);
        writer.nextRow(factory.create(key, Clustering.EMPTY));
    }

    private void assertRejected(V9SSTableComponentsWriter writer, DecoratedKey key)
    {
        assertThatThrownBy(() -> add(writer, key)).isInstanceOf(IllegalArgumentException.class)
                                                  .hasMessageContaining("Partition keys must be in ascending order");
    }
}
