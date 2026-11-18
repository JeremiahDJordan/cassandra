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

package org.apache.cassandra.index.sai.memory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.v2.V2RowAwarePrimaryKeyFactory;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link TrieMemoryIndex.SortedSetKeyRangeIterator#skipTo} on the key sets of an index on a partition key
 * column, which holds both STATIC keys (from static row updates) and WIDE keys (from regular row updates).
 */
public class SortedSetKeyRangeIteratorTest
{
    // CLUSTERING ORDER BY (ck DESC), so the higher clustering value sorts first
    private static final ClusteringComparator COMPARATOR = new ClusteringComparator(ReversedType.getInstance(Int32Type.instance));
    private static final PrimaryKey.Factory FACTORY = new V2RowAwarePrimaryKeyFactory(COMPARATOR);

    // partition keys in token order
    private static DecoratedKey a, b, c;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        List<DecoratedKey> keys = new ArrayList<>();
        for (int i = 0; i < 3; i++)
            keys.add(Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(i)));
        keys.sort(Comparator.naturalOrder());
        a = keys.get(0);
        b = keys.get(1);
        c = keys.get(2);
    }

    /**
     * The first key of the set is a STATIC key of a lower partition, but the target partition has no STATIC key.
     * Skipping must not land on a WIDE key of the target partition that sorts before the target.
     */
    @Test
    public void testSkipToPartitionWithoutStaticKey()
    {
        PrimaryKey bHigh = wide(b, 2);
        PrimaryKey bLow = wide(b, 1);
        PrimaryKeys set = keys(staticKey(a), bHigh, bLow);

        TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(set);
        iterator.skipTo(bLow);
        assertNextIs(iterator, bLow);
        assertFalse(iterator.hasNext());
    }

    /**
     * Same as {@link #testSkipToPartitionWithoutStaticKey}, after the iterator has already returned the STATIC key.
     */
    @Test
    public void testSkipToPartitionWithoutStaticKeyAfterNext()
    {
        PrimaryKey aStatic = staticKey(a);
        PrimaryKey bHigh = wide(b, 2);
        PrimaryKey bLow = wide(b, 1);
        PrimaryKeys set = keys(aStatic, bHigh, bLow, staticKey(c));

        TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(set);
        assertNextIs(iterator, aStatic);
        iterator.skipTo(bLow);
        assertNextIs(iterator, bLow);
        assertTrue(iterator.next().isStaticRow());
        assertFalse(iterator.hasNext());
    }

    /**
     * When the target partition has a STATIC key, skipping to a WIDE key of that partition must land on the STATIC key,
     * which matches the whole partition. This is what an index on a static column relies on.
     */
    @Test
    public void testSkipToPartitionWithStaticKey()
    {
        PrimaryKey bStatic = staticKey(b);
        PrimaryKeys set = keys(staticKey(a), bStatic, wide(b, 2), wide(b, 1), wide(c, 1));

        TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(set);
        iterator.skipTo(wide(b, 1));
        assertNextIs(iterator, bStatic);
    }

    /**
     * An index on a static column only holds STATIC keys.
     */
    @Test
    public void testSkipToOnlyStaticKeys()
    {
        PrimaryKey bStatic = staticKey(b);
        PrimaryKey cStatic = staticKey(c);
        PrimaryKeys set = keys(staticKey(a), bStatic, cStatic);

        TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(set);
        iterator.skipTo(wide(b, 5));
        assertNextIs(iterator, bStatic);
        iterator.skipTo(wide(c, 5));
        assertNextIs(iterator, cStatic);
        assertFalse(iterator.hasNext());
    }

    /**
     * Consecutive skips without reading a key in between, the second one to a key whose STATIC form is below the first
     * target. This must not fail with "key out of range" in {@code tailSet}.
     */
    @Test
    public void testConsecutiveSkipsInPartitionWithoutStaticKey()
    {
        PrimaryKey bHigh = wide(b, 3);
        PrimaryKey bMiddle = wide(b, 2);
        PrimaryKey bLow = wide(b, 1);
        PrimaryKeys set = keys(staticKey(a), bHigh, bMiddle, bLow, staticKey(c));

        TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(set);
        iterator.skipTo(bMiddle);
        iterator.skipTo(bLow);
        assertNextIs(iterator, bLow);

        iterator = iterator(set);
        iterator.skipTo(bLow);
        // backwards: a no-op
        iterator.skipTo(bHigh);
        assertNextIs(iterator, bLow);
    }

    /**
     * A skip to a WIDE key, then, without reading a key in between, a skip to the STATIC key of the same partition,
     * which the set orders before the WIDE keys but which compares equal to the first target: the second skip is a
     * no-op.
     */
    @Test
    public void testSkipToWideKeyThenStaticKeyOfSamePartition()
    {
        PrimaryKey bMiddle = wide(b, 2);
        PrimaryKey bLow = wide(b, 1);

        // a regular column index: no STATIC keys
        PrimaryKeys regularKeys = keys(wide(b, 3), bMiddle, bLow, wide(c, 1));
        TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(regularKeys, false);
        iterator.skipTo(bMiddle);
        iterator.skipTo(staticKey(b));
        assertNextIs(iterator, bMiddle);
        assertNextIs(iterator, bLow);

        // a partition key column index whose target partition has no STATIC key
        iterator = iterator(keys(staticKey(a), wide(b, 3), bMiddle, bLow, staticKey(c)));
        iterator.skipTo(bMiddle);
        iterator.skipTo(staticKey(b));
        assertNextIs(iterator, bMiddle);
        assertNextIs(iterator, bLow);

        // after returning the STATIC key of the target partition
        PrimaryKey bStatic = staticKey(b);
        iterator = iterator(keys(bStatic, wide(b, 3), bMiddle, bLow));
        assertNextIs(iterator, bStatic);
        iterator.skipTo(bMiddle);
        iterator.skipTo(bStatic);
        assertNextIs(iterator, bMiddle);
    }

    /**
     * The searcher first skips to the token-only key of the start of the query range, which compares equal to every key
     * with the same token. A following skip to a WIDE key with that token must still skip within the partition.
     */
    @Test
    public void testSkipToTokenOnlyKeyThenWideKey()
    {
        PrimaryKey bLow = wide(b, 1);
        PrimaryKeys set = keys(wide(b, 3), wide(b, 2), bLow, wide(c, 1));

        TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(set);
        iterator.skipTo(FACTORY.createTokenOnly(b.getToken()));
        iterator.skipTo(bLow);
        assertNextIs(iterator, bLow);
    }

    /**
     * Skipping to a partition with no keys in the set lands on the first key of the next partition.
     */
    @Test
    public void testSkipToMissingPartition()
    {
        PrimaryKey cHigh = wide(c, 2);
        PrimaryKeys set = keys(staticKey(a), cHigh, wide(c, 1));

        TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(set);
        iterator.skipTo(wide(b, 1));
        assertNextIs(iterator, cHigh);
    }

    /**
     * After reading into a partition past its STATIC key, a skip within the partition must not go back to the STATIC
     * key.
     */
    @Test
    public void testSkipWithinPartitionDoesNotRewindToStaticKey()
    {
        PrimaryKey bStatic = staticKey(b);
        PrimaryKey bLow = wide(b, 1);
        PrimaryKeys set = keys(wide(a, 1), bStatic, wide(b, 3), wide(b, 2), bLow, wide(c, 1));

        TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(set);
        assertNextIs(iterator, wide(a, 1));
        assertNextIs(iterator, bStatic);
        assertNextIs(iterator, wide(b, 3));
        iterator.skipTo(bLow);
        assertNextIs(iterator, bLow);
        assertNextIs(iterator, wide(c, 1));
        assertFalse(iterator.hasNext());
    }

    /**
     * After returning the STATIC key of a partition, a skip within the partition must skip. The STATIC key compares equal
     * to the target in the natural order of the keys, but sorts before it in the set.
     */
    @Test
    public void testSkipWithinPartitionAfterStaticKey()
    {
        PrimaryKey bStatic = staticKey(b);
        PrimaryKey bLow = wide(b, 1);
        PrimaryKeys set = keys(bStatic, wide(b, 3), wide(b, 2), bLow, wide(c, 1));

        TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(set);
        assertNextIs(iterator, bStatic);
        iterator.skipTo(bLow);
        assertNextIs(iterator, bLow);
    }

    /**
     * Skipping to a key of a partition whose token isn't in the set must not load the target key, which for an SSTable
     * key means reading it from the primary key map. That holds whether the set can hold STATIC keys or not.
     */
    @Test
    public void testSkipToAbsentTokenDoesNotLoadTarget()
    {
        PrimaryKeys set = keys(staticKey(a), wide(a, 1), wide(c, 2), wide(c, 1));

        for (boolean mayHaveStaticKeys : new boolean[]{ true, false })
        {
            AtomicInteger loads = new AtomicInteger();
            PrimaryKey target = FACTORY.createDeferred(b.getToken(), () -> {
                loads.incrementAndGet();
                return wide(b, 1);
            });

            TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(set, mayHaveStaticKeys);
            iterator.skipTo(target);
            assertNextIs(iterator, wide(c, 2));
            assertEquals("loads of the target with mayHaveStaticKeys=" + mayHaveStaticKeys, 0, loads.get());
        }
    }

    /**
     * Without STATIC keys in the set, skipping goes to the target itself.
     */
    @Test
    public void testSkipWithoutStaticKeys()
    {
        PrimaryKey cLow = wide(c, 1);
        PrimaryKeys set = keys(wide(a, 1), wide(c, 2), cLow);

        TrieMemoryIndex.SortedSetKeyRangeIterator iterator = iterator(set, false);
        iterator.skipTo(cLow);
        assertNextIs(iterator, cLow);
        assertFalse(iterator.hasNext());
    }

    private static TrieMemoryIndex.SortedSetKeyRangeIterator iterator(PrimaryKeys set)
    {
        return iterator(set, true);
    }

    private static TrieMemoryIndex.SortedSetKeyRangeIterator iterator(PrimaryKeys set, boolean mayHaveStaticKeys)
    {
        return new TrieMemoryIndex.SortedSetKeyRangeIterator(set.keys(), mayHaveStaticKeys, FACTORY);
    }

    private static void assertNextIs(TrieMemoryIndex.SortedSetKeyRangeIterator iterator, PrimaryKey expected)
    {
        assertTrue(iterator.hasNext());
        PrimaryKey actual = iterator.next();
        assertEquals(expected.partitionKey(), actual.partitionKey());
        assertEquals(expected.clustering(), actual.clustering());
    }

    private static PrimaryKeys keys(PrimaryKey... keys)
    {
        PrimaryKeys set = new PrimaryKeys();
        for (PrimaryKey key : keys)
            set.addAndIncrementFrequency(key);
        return set;
    }

    private static PrimaryKey staticKey(DecoratedKey partitionKey)
    {
        return FACTORY.create(partitionKey, Clustering.STATIC_CLUSTERING);
    }

    private static PrimaryKey wide(DecoratedKey partitionKey, int clustering)
    {
        return FACTORY.create(partitionKey, Clustering.make(Int32Type.instance.decompose(clustering)));
    }
}
