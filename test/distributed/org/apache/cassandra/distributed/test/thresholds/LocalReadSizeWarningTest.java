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

package org.apache.cassandra.distributed.test.thresholds;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LocalReadSizeWarningTest extends AbstractClientSizeWarning
{
    @BeforeClass
    public static void setupClass() throws IOException
    {
        AbstractClientSizeWarning.setupClass();

        // setup threshold after init to avoid driver issues loading
        // the test uses a rather small limit, which causes driver to fail while loading metadata
        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> {
            // disable coordinator version
            DatabaseDescriptor.setCoordinatorReadSizeWarnThreshold(null);
            DatabaseDescriptor.setCoordinatorReadSizeFailThreshold(null);

            DatabaseDescriptor.setLocalReadSizeWarnThreshold(new DataStorageSpec.LongBytesBound(1, KIBIBYTES));
            DatabaseDescriptor.setLocalReadSizeFailThreshold(new DataStorageSpec.LongBytesBound(2, KIBIBYTES));
        }));
    }

    @Test
    public void warnThresholdSinglePartitionWithPaging()
    {
        warnThresholdWithPaging("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1");
    }

    @Test
    public void warnThresholdScanWithPaging()
    {
        warnThresholdWithPaging("SELECT * FROM " + KEYSPACE + ".tbl");
    }

    private void warnThresholdWithPaging(String cql)
    {
        // with a page size of 2, the first page reads two empty values, and the second page two values of 300 bytes,
        // over the warn threshold (1 KiB) and under the fail threshold (2 KiB) with the size of the rest of the page
        insertTwoPages(1, 300);

        enable(true);
        // the pages after the first are fetched by this thread while iterating
        Iterator<Object[]> rows = CLUSTER.coordinator(1).executeWithPaging(cql, ConsistencyLevel.ALL, 2);
        assertThat(Iterators.size(rows)).isEqualTo(4);
        assertWarnAborts(1, 0, 0);
    }

    @Test
    public void failThresholdWithPaging()
    {
        String cql = "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1";

        // with a page size of 2, the first page reads two empty values, and the second page two values of 1200 bytes,
        // over the fail threshold (2 KiB)
        insertTwoPages(1, 1200);

        enable(true);
        // the second page is fetched by this thread while iterating, and aborts
        Iterator<Object[]> rows = CLUSTER.coordinator(1).executeWithPaging(cql, ConsistencyLevel.ALL, 2);
        assertThat(rows.next()[1]).isEqualTo(1);
        assertThat(rows.next()[1]).isEqualTo(2);
        assertThatThrownBy(rows::hasNext)
        .hasMessageContaining("bytes and aborted the query SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?")
        .hasMessageContaining("(see local_read_size_fail_threshold)");
        assertWarnAborts(0, 1, 1);

        // the first page is fetched before executeWithPaging returns, and aborts
        assertThatThrownBy(() -> CLUSTER.coordinator(1).executeWithPaging(cql + " AND ck > 2", ConsistencyLevel.ALL, 2))
        .hasMessageContaining("bytes and aborted the query SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?")
        .hasMessageContaining("(see local_read_size_fail_threshold)");
        assertWarnAborts(0, 2, 1);

        // the aborted pages left no warnings state on the threads that executed them
        insertTwoPages(2, 0);
        rows = CLUSTER.coordinator(1).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 2", ConsistencyLevel.ALL, 1);
        assertThat(Iterators.size(rows)).isEqualTo(4);
        assertWarnAborts(0, 2, 0);
    }

    private static void insertTwoPages(int pk, int secondPageValueSize)
    {
        for (int ck = 1; ck <= 4; ck++)
            CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)", ConsistencyLevel.ALL,
                                           pk, ck, bytes(ck <= 2 ? 0 : secondPageValueSize));
    }

    @Override
    protected void assertWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("(see local_read_size_warn_threshold)").contains("and issued local read size warnings for query");
    }

    @Override
    protected void assertAbortWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("(see local_read_size_fail_threshold)").contains("aborted the query");
    }

    @Override
    protected long[] getHistogram()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.LocalReadSize." + KEYSPACE)).toArray();
    }

    @Override
    protected long totalWarnings()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.LocalReadSizeWarnings." + KEYSPACE)).sum();
    }

    @Override
    protected long totalAborts()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.LocalReadSizeAborts." + KEYSPACE)).sum();
    }
}
