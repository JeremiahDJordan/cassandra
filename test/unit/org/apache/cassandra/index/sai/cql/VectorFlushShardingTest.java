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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.UCS_L0_SHARDS_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests flushing a table with a vector index when the unified compaction strategy splits flushes into several SSTables
 * ({@code unified_compaction.l0_shards_enabled}, which is off by default). The vector index of a memtable can only be
 * written once, so a table with a vector index must flush to a single SSTable (CASSANDRA-19661).
 */
public class VectorFlushShardingTest extends VectorTester
{
    private static final String UCS_TWO_SHARDS = " WITH compaction = {'class': 'UnifiedCompactionStrategy', " +
                                                 "'base_shard_count': '2', 'min_sstable_size': '0MiB'}";
    private static final int ROWS = 100;

    private static WithProperties properties;

    @BeforeClass
    public static void enableL0Shards()
    {
        // read when the compaction strategy of a table is created
        properties = new WithProperties().set(UCS_L0_SHARDS_ENABLED, true);
    }

    @AfterClass
    public static void resetL0Shards()
    {
        properties.close();
    }

    /**
     * Without a vector index, the flush is split, which shows the flush sharding is active in this test.
     */
    @Test
    public void testFlushWithoutVectorIndexIsSharded()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)" + UCS_TWO_SHARDS);
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        disableCompaction();

        for (int k = 0; k < ROWS; k++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, k);
        flush();

        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(2);
        assertRowCount(execute("SELECT * FROM %s WHERE v >= 0"), ROWS);
    }

    @Test
    public void testFlushWithVectorIndexIsNotSharded()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, vec vector<float, 2>)" + UCS_TWO_SHARDS);
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        disableCompaction();

        for (int k = 0; k < ROWS; k++)
            execute("INSERT INTO %s (k, v, vec) VALUES (?, ?, ?)", k, k, vector((float) k + 1, 1f));
        flush();

        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1);
        assertRowCount(execute("SELECT * FROM %s WHERE v >= 0"), ROWS);
        assertRowCount(execute("SELECT k FROM %s ORDER BY vec ANN OF [100, 1] LIMIT 10"), 10);
    }
}
