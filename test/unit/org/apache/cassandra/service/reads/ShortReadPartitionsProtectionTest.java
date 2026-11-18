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

package org.apache.cassandra.service.reads;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

public class ShortReadPartitionsProtectionTest extends CQLTester
{
    /**
     * With endpoint grouping, a replica reads all its ranges in one command and stops once it reaches the limit, so
     * short read protection can be asked for more partitions of a range the replica returned nothing for (see
     * {@code EndpointGroupingCoordinator.EndpointShortReadResponseProtection}). It must then read that range from its
     * beginning, since there is no last partition to continue after.
     */
    @Test
    public void testMorePartitionsOfRangeWithoutPartitions()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        for (int k = 0; k < 10; k++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, k);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        PartitionRangeReadCommand command = (PartitionRangeReadCommand) Util.cmd(cfs).withLimit(2).build();

        // the replica has already returned as many rows as the limit allows, in another range
        DataLimits.Counter singleResultCounter = command.limits().newCounter(command.nowInSec(), false, command.selectsFullPartition(), false);
        try (ReadExecutionController controller = command.executionController();
             PartitionIterator partitions = singleResultCounter.applyTo(command.executeInternal(controller)))
        {
            PartitionIterators.consume(partitions);
        }
        assertThat(command.limits().isCounterBelowLimits(singleResultCounter)).isFalse();

        DataLimits.Counter mergedResultCounter = command.limits().newCounter(command.nowInSec(), true, command.selectsFullPartition(), false).onlyCount();

        ShortReadPartitionsProtection protection = new ShortReadPartitionsProtection(command,
                                                                                     ReplicaUtils.full(FBUtilities.getBroadcastAddressAndPort()),
                                                                                     null,
                                                                                     () -> {},
                                                                                     singleResultCounter,
                                                                                     mergedResultCounter,
                                                                                     Dispatcher.RequestTime.forImmediateExecution())
        {
            @Override
            public boolean rangeExhausted()
            {
                // as for a range that an endpoint-grouped replica didn't read
                return false;
            }
        };

        List<Integer> keys = new ArrayList<>();
        try (UnfilteredPartitionIterator partitions = protection.moreContents())
        {
            assertThat(partitions).isNotNull();
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    keys.add(partition.partitionKey().getKey().getInt(0));
                }
            }
        }

        // the first partitions of the range, in token order
        List<Integer> expected = new ArrayList<>();
        for (Object[] row : getRows(execute("SELECT k FROM %s LIMIT 2")))
            expected.add((Integer) row[0]);
        assertThat(keys).isEqualTo(expected);
    }
}
