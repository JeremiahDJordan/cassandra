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

package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PrimaryKeyMapIteratorSkipToTest extends SAITester
{
    /**
     * A key without clustering (the key of a static row) compares equal to every key of its partition: a skip to it
     * never goes back into rows of its partition that were already returned.
     */
    @Test
    public void testSkipToStaticKeyOfCurrentPartition() throws IOException
    {
        createTable("CREATE TABLE %s (k int, c int, s int static, v int, PRIMARY KEY (k, c))");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, c, s, v) VALUES (0, 1, 0, 0)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 2, 0)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 3, 0)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableIndex sstableIndex = getIndexContext(index).getView().getIndexes().iterator().next();
        PartitionPosition min = cfs.getPartitioner().getMinimumToken().minKeyBound();
        AbstractBounds<PartitionPosition> all = new Range<>(min, min);

        try (KeyRangeIterator iterator = PrimaryKeyMapIterator.create(sstableIndex.getSSTableContext(), all))
        {
            PrimaryKey first = iterator.next();
            assertEquals(1, clustering(first));
            PrimaryKey staticKey = sstableIndex.getIndexContext().keyFactory().create(first.partitionKey(),
                                                                                      Clustering.STATIC_CLUSTERING);
            iterator.skipTo(staticKey);
            assertEquals(2, clustering(iterator.next()));
            iterator.skipTo(staticKey);
            iterator.skipTo(first);
            assertEquals(3, clustering(iterator.next()));
            iterator.skipTo(staticKey);
            assertFalse(iterator.hasNext());
        }
    }

    private static int clustering(PrimaryKey key)
    {
        return Int32Type.instance.compose(key.clustering().bufferAt(0));
    }
}
