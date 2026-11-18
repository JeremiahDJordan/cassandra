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

package org.apache.cassandra.index.sai.disk;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ToLongFunction;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v9.V9OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v9.V9SSTableComponentsWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lookups of {@link PrimaryKeyMap} on an SSTable whose distinct partition keys share tokens, for its keys and for keys
 * of the same tokens that it doesn't have, checked against a model of the rows, with the default partition key block
 * size and with blocks of two keys, so that the keys of a token span blocks.
 */
public class RowAwareTokenCollisionPrimaryKeyMapTest extends SAITester.Versioned.RowAware
{
    private static final int[] PARTITION_BLOCK_SHIFTS = { V9SSTableComponentsWriter.DEFAULT_PARTITION_BLOCK_SHIFT, 1 };

    private static final int NO_CLUSTERING = Integer.MIN_VALUE;
    private static final int STATIC = Integer.MIN_VALUE + 1;
    private static final int[] CLUSTERINGS = { 1, 3, 5 };
    private static final int[] TARGET_CLUSTERINGS = { 0, 1, 2, 3, 5, 6 };

    @After
    public void resetPartitionBlockShift()
    {
        V9SSTableComponentsWriter.setPartitionBlockShift(V9SSTableComponentsWriter.DEFAULT_PARTITION_BLOCK_SHIFT);
    }

    @Test
    public void testSkinny() throws Throwable
    {
        for (int shift : PARTITION_BLOCK_SHIFTS)
            test(shift, false, false);
    }

    @Test
    public void testWide() throws Throwable
    {
        for (int shift : PARTITION_BLOCK_SHIFTS)
            test(shift, true, false);
    }

    @Test
    public void testWideWithStaticRows() throws Throwable
    {
        for (int shift : PARTITION_BLOCK_SHIFTS)
            test(shift, true, true);
    }

    private void test(int partitionBlockShift, boolean wide, boolean withStatic) throws Throwable
    {
        V9SSTableComponentsWriter.setPartitionBlockShift(partitionBlockShift);
        if (wide)
            createTable("CREATE TABLE %s (pk blob, ck int, " + (withStatic ? "s int static, " : "") +
                        "int_value int, PRIMARY KEY (pk, ck))");
        else
            createTable("CREATE TABLE %s (pk blob PRIMARY KEY, int_value int)");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(int_value) USING 'StorageAttachedIndex'");
        disableCompaction();

        IPartitioner partitioner = getCurrentColumnFamilyStore().getPartitioner();
        Data data = new Data(partitioner);

        int value = 0;
        for (DecoratedKey key : data.present)
        {
            if (!wide)
            {
                execute("INSERT INTO %s (pk, int_value) VALUES (?, ?)", key.getKey(), value++);
                continue;
            }
            if (withStatic)
                execute("INSERT INTO %s (pk, s) VALUES (?, ?)", key.getKey(), value++);
            for (int ck : CLUSTERINGS)
                execute("INSERT INTO %s (pk, ck, int_value) VALUES (?, ?, ?)", key.getKey(), ck, value++);
        }
        flush();

        // the model of the rows of the SSTable, in their order
        List<Row> rows = new ArrayList<>();
        for (DecoratedKey key : data.present)
        {
            if (!wide)
            {
                rows.add(new Row(key, NO_CLUSTERING));
                continue;
            }
            if (withStatic)
                rows.add(new Row(key, STATIC));
            for (int ck : CLUSTERINGS)
                rows.add(new Row(key, ck));
        }

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        IndexContext context = SAITester.createIndexContext(index, Int32Type.instance);
        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Set.of(context));
        IndexComponents.ForRead components = indexDescriptor.perSSTableComponents();
        PrimaryKey.Factory pkFactory = components.onDiskFormat().newPrimaryKeyFactory(cfs.metadata.get().comparator);
        boolean v9 = components.onDiskFormat() instanceof V9OnDiskFormat;

        try (PrimaryKeyMap.Factory factory = components.onDiskFormat().newPrimaryKeyMapFactory(components, pkFactory, sstable))
        {
            String description = String.format("version %s, partition block shift %d", components.version(), partitionBlockShift);

            try (PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
            {
                assertThat(map.count()).as(description).isEqualTo(rows.size());
                for (int rowId = 0; rowId < rows.size(); rowId++)
                {
                    PrimaryKey key = map.primaryKeyFromRowId(rowId);
                    Row row = rows.get(rowId);
                    assertThat(key.partitionKey()).as("%s, partition key of row %d", description, rowId).isEqualTo(row.key);
                    if (row.clustering >= 0)
                        assertThat(key.clustering()).as("%s, clustering of row %d", description, rowId)
                                                    .isEqualTo(clustering(row.clustering));
                    else
                        assertThat(key.hasClustering()).as("%s, clustering of row %d", description, rowId).isFalse();
                }
            }

            List<Target> targets = new ArrayList<>();
            for (Token token : data.targetTokens())
                targets.add(new Target(pkFactory.createTokenOnly(token), token, null, NO_CLUSTERING));
            for (DecoratedKey key : data.all)
            {
                targets.add(new Target(pkFactory.create(key, Clustering.EMPTY), key.getToken(), key, NO_CLUSTERING));
                if (withStatic)
                    targets.add(new Target(pkFactory.create(key, Clustering.STATIC_CLUSTERING), key.getToken(), key, NO_CLUSTERING));
                if (wide)
                    for (int ck : TARGET_CLUSTERINGS)
                        targets.add(new Target(pkFactory.create(key, clustering(ck)), key.getToken(), key, ck));
            }

            for (Target target : targets)
            {
                String targetDescription = String.format("%s, target %s", description, target);
                long ceiling = -1;
                long floor = -1;
                long firstEqual = -1;
                for (int rowId = 0; rowId < rows.size(); rowId++)
                {
                    int cmp = target.compareTo(rows.get(rowId));
                    if (cmp <= 0 && ceiling < 0)
                        ceiling = rowId;
                    if (cmp == 0 && firstEqual < 0)
                        firstEqual = rowId;
                    if (cmp >= 0)
                        floor = rowId;
                }
                long notFound = ceiling < 0 ? Long.MIN_VALUE : ~ceiling;

                assertThat(lookup(factory, m -> m.ceiling(target.key))).as("ceiling, %s", targetDescription).isEqualTo(ceiling);
                assertThat(lookup(factory, m -> m.floor(target.key))).as("floor, %s", targetDescription).isEqualTo(floor);

                // A key that designates several rows (a token, or a partition of a wide table) is found by the V9 maps
                // at its first row, and not found by the others, where only full keys have exact matches.
                boolean prefix = target.partitionKey == null || (wide && target.clustering == NO_CLUSTERING);
                if (!prefix || v9)
                    assertThat(lookup(factory, m -> m.exactRowIdOrInvertedCeiling(target.key)))
                    .as("exact, %s", targetDescription)
                    .isEqualTo(firstEqual >= 0 ? firstEqual : notFound);
            }
        }
    }

    // The token lookups only go forward, so each lookup gets a new map
    private static long lookup(PrimaryKeyMap.Factory factory, ToLongFunction<PrimaryKeyMap> lookup) throws Exception
    {
        try (PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            return lookup.applyAsLong(map);
        }
    }

    private static Clustering<?> clustering(int ck)
    {
        return Clustering.make(Int32Type.instance.decompose(ck));
    }

    private static class Row
    {
        final DecoratedKey key;
        final int clustering; // NO_CLUSTERING for a skinny table, STATIC for a static row

        Row(DecoratedKey key, int clustering)
        {
            this.key = key;
            this.clustering = clustering;
        }
    }

    private static class Target
    {
        final PrimaryKey key;
        final Token token;
        final DecoratedKey partitionKey; // null for a token
        final int clustering; // NO_CLUSTERING for a token or a partition (or the static row of a partition)

        Target(PrimaryKey key, Token token, DecoratedKey partitionKey, int clustering)
        {
            this.key = key;
            this.token = token;
            this.partitionKey = partitionKey;
            this.clustering = clustering;
        }

        // the order of the primary keys: a key without clustering designates all the rows of its partition
        int compareTo(Row row)
        {
            int cmp = token.compareTo(row.key.getToken());
            if (cmp != 0 || partitionKey == null)
                return cmp;
            cmp = partitionKey.compareTo(row.key);
            if (cmp != 0 || clustering == NO_CLUSTERING || row.clustering == NO_CLUSTERING)
                return cmp;
            // a static row comes before the other rows of its partition
            return row.clustering == STATIC ? 1 : Integer.compare(clustering, row.clustering);
        }

        @Override
        public String toString()
        {
            return String.format("%s (token %s%s)", key, token,
                                 partitionKey == null ? ", token only" : ", key " + ByteBufferUtil.bytesToHex(partitionKey.getKey()) +
                                                                         (clustering == NO_CLUSTERING ? "" : ", clustering " + clustering));
        }
    }

    private static class Data
    {
        final List<DecoratedKey> present = new ArrayList<>();
        final List<DecoratedKey> all = new ArrayList<>();

        Data(IPartitioner partitioner)
        {
            List<List<DecoratedKey>> groups = new ArrayList<>();
            for (String prefix : new String[]{ "group0", "group1", "group2" })
            {
                List<DecoratedKey> group = Util.murmurCollisions(partitioner, prefix, 8);
                for (DecoratedKey key : group)
                    assertThat(key.getToken()).isEqualTo(group.get(0).getToken());
                groups.add(group);
            }
            groups.sort(Comparator.comparing(g -> g.get(0).getToken()));
            Token low = groups.get(0).get(0).getToken();
            Token middle = groups.get(1).get(0).getToken();
            Token high = groups.get(2).get(0).getToken();

            // the keys of each group that are in the SSTable: not the first, fifth and last ones
            for (List<DecoratedKey> group : groups)
                for (int i : new int[]{ 1, 2, 3, 5, 6 })
                    present.add(group.get(i));

            present.addAll(Util.keysBetweenTokens(partitioner, low, middle, 2));
            present.addAll(Util.keysBetweenTokens(partitioner, middle, high, 2));
            present.sort(Comparator.naturalOrder());

            for (List<DecoratedKey> group : groups)
                all.addAll(group);
            for (DecoratedKey key : present)
                if (!all.contains(key))
                    all.add(key);
            all.sort(Comparator.naturalOrder());
        }

        Set<Token> targetTokens()
        {
            Set<Token> tokens = new TreeSet<>();
            for (DecoratedKey key : all)
            {
                long token = key.getToken().getLongValue();
                tokens.add(key.getToken());
                tokens.add(key.getToken().getPartitioner().getTokenFactory().fromLongValue(token - 1));
                tokens.add(key.getToken().getPartitioner().getTokenFactory().fromLongValue(token + 1));
            }
            return tokens;
        }
    }
}
