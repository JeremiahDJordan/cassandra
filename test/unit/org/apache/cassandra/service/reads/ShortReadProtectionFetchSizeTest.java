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

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ReadCommand;

import static org.apache.cassandra.service.reads.ShortReadProtection.MAX_NON_STRICT_FETCH_SIZE;
import static org.apache.cassandra.service.reads.ShortReadProtection.fetchSize;
import static org.assertj.core.api.Assertions.assertThat;

public class ShortReadProtectionFetchSizeTest extends CQLTester
{
    /**
     * Short read protection asks for the missing rows, and for growing numbers of rows when the read is non-strict
     * (CASSANDRA-19018) or its replica filtering protection reads whole partitions.
     */
    @Test
    public void testFetchSize()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");

        // the command builder makes reads that need reconciliation
        ReadCommand strict = Util.cmd(getCurrentColumnFamilyStore(), 0).filterOn("a", Operator.EQ, 1).withLimit(1).build();
        ReadCommand unfiltered = Util.cmd(getCurrentColumnFamilyStore()).withLimit(1).build();
        ReadCommand strictOverPartitions = Util.cmd(getCurrentColumnFamilyStore()).filterOn("a", Operator.EQ, 1).withLimit(1).build();
        ReadCommand nonStrict = Util.cmd(getCurrentColumnFamilyStore()).filterOn("a", Operator.EQ, 1).filterOn("b", Operator.EQ, 2).withLimit(1).build();
        assertThat(strict.rowFilter().isStrict()).isTrue();
        assertThat(strictOverPartitions.rowFilter().isStrict()).isTrue();
        assertThat(nonStrict.rowFilter().isStrict()).isFalse();

        // strict filters ask for the missing rows, as do reads without a filter
        for (ReadCommand command : new ReadCommand[]{ strict, unfiltered })
        {
            assertThat(fetchSize(command, 1, 1)).isEqualTo(1);
            assertThat(fetchSize(command, 1, 100)).isEqualTo(1);
            assertThat(fetchSize(command, 7, 1)).isEqualTo(7);
        }

        // unless replica filtering protection reads whole partitions, as it does for reads of several partitions
        assertThat(fetchSize(strictOverPartitions, 1, 1)).isEqualTo(2);
        assertThat(fetchSize(strictOverPartitions, 1, 1000)).isEqualTo(2000);

        // non-strict filters double the previous request, up to a maximum, and never ask for fewer than the missing rows
        assertThat(fetchSize(nonStrict, 1, 0)).isEqualTo(1);
        assertThat(fetchSize(nonStrict, 1, 1)).isEqualTo(2);
        assertThat(fetchSize(nonStrict, 1, 2)).isEqualTo(4);
        assertThat(fetchSize(nonStrict, 1, 1000)).isEqualTo(2000);
        assertThat(fetchSize(nonStrict, 1, MAX_NON_STRICT_FETCH_SIZE)).isEqualTo(MAX_NON_STRICT_FETCH_SIZE);
        assertThat(fetchSize(nonStrict, 1, Integer.MAX_VALUE)).isEqualTo(MAX_NON_STRICT_FETCH_SIZE);
        assertThat(fetchSize(nonStrict, 3, 1)).isEqualTo(3);
        assertThat(fetchSize(nonStrict, 10000, 10000)).isEqualTo(10000);
    }
}
