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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.assertj.core.api.Assertions.assertThat;

public class PrimaryKeyListUtilTest extends SAITester
{
    private PrimaryKey.Factory factory;
    private final List<DecoratedKey> partitions = new ArrayList<>();
    private final List<PrimaryKey> keys = new ArrayList<>();

    /**
     * A static row key compares equal to every row of its partition. As a bound, it must keep all of them.
     */
    @Test
    public void testGetKeysInRangeWithStaticRowBounds()
    {
        createKeys();

        assertThat(PrimaryKeyListUtil.getKeysInRange(keys, staticRowKey(0), staticRowKey(2))).isEqualTo(keys);
        assertThat(PrimaryKeyListUtil.getKeysInRange(keys, staticRowKey(1), staticRowKey(1))).isEqualTo(keys.subList(3, 6));
        assertThat(PrimaryKeyListUtil.getKeysInRange(keys, keys.get(1), staticRowKey(1))).isEqualTo(keys.subList(1, 6));
        assertThat(PrimaryKeyListUtil.getKeysInRange(keys, staticRowKey(1), keys.get(7))).isEqualTo(keys.subList(3, 8));
        assertThat(PrimaryKeyListUtil.getKeysInRange(keys, keys.get(4), keys.get(4))).isEqualTo(keys.subList(4, 5));
    }

    @Test
    public void testToStaticRowKeys()
    {
        createKeys();

        List<PrimaryKey> someKeys = List.of(keys.get(0), keys.get(2), keys.get(5), keys.get(6), keys.get(7), keys.get(8));
        List<PrimaryKey> staticKeys = PrimaryKeyListUtil.toStaticRowKeys(someKeys);
        assertThat(staticKeys).hasSize(3);
        for (int i = 0; i < 3; i++)
        {
            assertThat(staticKeys.get(i).isStaticRow()).isTrue();
            assertThat(staticKeys.get(i).partitionKey()).isEqualTo(partitions.get(i));
        }
    }

    /**
     * Creates the row keys of 3 partitions with 3 rows each, in primary key order.
     */
    private void createKeys()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY(pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        factory = PrimaryKey.factory(cfs.metadata().comparator,
                                     Version.current(KEYSPACE).onDiskFormat().indexFeatureSet());
        for (int pk = 0; pk < 3; pk++)
            partitions.add(cfs.getPartitioner().decorateKey(Int32Type.instance.decompose(pk)));
        partitions.sort(DecoratedKey::compareTo);
        for (DecoratedKey partition : partitions)
            for (int ck = 0; ck < 3; ck++)
                keys.add(factory.create(partition, Clustering.make(Int32Type.instance.decompose(ck))));
    }

    private PrimaryKey staticRowKey(int partition)
    {
        return factory.create(partitions.get(partition), Clustering.STATIC_CLUSTERING);
    }
}
