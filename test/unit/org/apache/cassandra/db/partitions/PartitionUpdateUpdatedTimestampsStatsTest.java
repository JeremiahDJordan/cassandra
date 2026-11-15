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

package org.apache.cassandra.db.partitions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * {@link PartitionUpdate#withUpdatedTimestamps} must leave {@link PartitionUpdate#stats()} describing the updated
 * timestamps, for every memtable's partition update implementation: a proposal's update has its timestamps set to the
 * proposing ballot ({@link Commit.Proposal#of}), and {@link Commit#isReproposalOf} reads that ballot back from
 * {@code stats().minTimestamp}.
 */
@RunWith(Parameterized.class)
public class PartitionUpdateUpdatedTimestampsStatsTest extends CQLTester
{
    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object> parameters()
    {
        return Arrays.asList("skiplist", "trie", "trie_stage1", "trie_stage2", "trie_stage3");
    }

    private TableMetadata createTestTable()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text, l list<int>) WITH memtable = '" + memtable + "'");
        return currentTableMetadata();
    }

    /** v = 'a' plus l appended with [1], or assigned [1] (which carries a complex deletion) if {@code assignList} */
    private static PartitionUpdate update(TableMetadata metadata, long timestamp, boolean assignList)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 1).timestamp(timestamp);
        Row.SimpleBuilder row = builder.row().add("v", "a");
        if (assignList)
            row.add("l", Collections.singletonList(1));
        else
            row.appendAll("l", Collections.singletonList(1));
        return builder.build();
    }

    @Test
    public void testStatsFollowUpdatedTimestamps()
    {
        TableMetadata metadata = createTestTable();

        // live data only: every timestamp becomes the new one
        PartitionUpdate update = update(metadata, 1000, false);
        assertEquals(1000, update.stats().minTimestamp);
        assertEquals(5000, update.withUpdatedTimestamps(5000).stats().minTimestamp);
        assertEquals(500, update.withUpdatedTimestamps(500).stats().minTimestamp);

        // a collection assignment carries a complex deletion, which becomes newTimestamp - 1
        PartitionUpdate withDeletion = update(metadata, 1000, true);
        assertEquals(4999, withDeletion.withUpdatedTimestamps(5000).stats().minTimestamp);
        assertEquals(499, withDeletion.withUpdatedTimestamps(500).stats().minTimestamp);
    }

    @Test
    public void testNewProposalIsNotAReproposalOfAnEarlierCommit()
    {
        TableMetadata metadata = createTestTable();

        Ballot older = Ballot.atUnixMicrosWithLsb(2000, 1, Ballot.Flag.NONE);
        Commit.Committed committed = new Commit.Committed(older, update(metadata, 2000, false));

        // A new update whose statement timestamp equals the earlier commit's ballot (ClientState hands out both from one
        // sequence), proposed with a later ballot.
        PartitionUpdate update = update(metadata, 2000, false);
        Commit.Proposal proposal = Commit.Proposal.of(Ballot.atUnixMicrosWithLsb(3000, 2, Ballot.Flag.NONE), update);

        assertEquals(3000, proposal.update.stats().minTimestamp);
        assertFalse(proposal.isReproposalOf(committed));
    }
}
