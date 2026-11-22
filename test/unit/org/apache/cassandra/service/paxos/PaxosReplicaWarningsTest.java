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

package org.apache.cassandra.service.paxos;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.ReplicaWarnings;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.index.sai.NotIndexedValues.NGRAM_ANALYZER;
import static org.apache.cassandra.index.sai.NotIndexedValues.NGRAM_VALUE;
import static org.apache.cassandra.service.paxos.Ballot.Flag.NONE;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.nextBallot;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the client warnings of the Paxos v2 commits a replica applies: those of the commit of the current proposal go
 * to the coordinator with the response, those of another proposer's commit, applied with a prepare or a refresh, are
 * dropped.
 */
public class PaxosReplicaWarningsTest extends CQLTester
{
    private final List<Message<?>> responses = new CopyOnWriteArrayList<>();
    private String index;

    @Before
    public void setUp()
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text)");
        index = createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex' WITH OPTIONS = " + NGRAM_ANALYZER);
        PaxosV2TestHelper.localElectorate();

        MessagingService.instance().outboundSink.add((message, to) -> {
            responses.add(message);
            return false;
        });
        ClientWarn.instance.captureWarnings();
    }

    @After
    public void tearDown()
    {
        ClientWarn.instance.resetWarnings();
        MessagingService.instance().outboundSink.clear();
    }

    @Test
    public void testPrepareRefreshDropsWarnings()
    {
        PartitionUpdate update = update("0");
        PaxosPrepareRefresh.Request request = new PaxosPrepareRefresh.Request(nextBallot(NONE), new Commit.Committed(nextBallot(NONE), update));
        PaxosPrepareRefresh.RequestHandler.execute(request, FBUtilities.getBroadcastAddressAndPort());

        assertRows(execute("SELECT k FROM %s WHERE k = '0'"), row("0"));
        assertThat(ClientWarn.instance.getWarnings()).isNull();
    }

    @Test
    public void testCommitAndPrepareDropsWarnings()
    {
        PartitionUpdate update = update("1");
        SinglePartitionReadCommand read = SinglePartitionReadCommand.fullPartitionRead(update.metadata(), FBUtilities.nowInSeconds(), update.partitionKey());
        PaxosCommitAndPrepare.Request request = new PaxosCommitAndPrepare.Request(new Commit.Agreed(nextBallot(NONE), update), nextBallot(NONE),
                                                                                  PaxosV2TestHelper.localElectorate(), read, true);
        PaxosCommitAndPrepare.requestHandler.doVerb(Message.builder(Verb.PAXOS2_COMMIT_AND_PREPARE_REQ, request).build());

        assertRows(execute("SELECT k FROM %s WHERE k = '1'"), row("1"));
        assertThat(ClientWarn.instance.getWarnings()).isNull();
        assertThat(responses).hasSize(1);
        assertThat(ReplicaWarnings.fromResponse(responses.get(0))).isNull();
    }

    @Test
    public void testCommitSendsWarnings()
    {
        Commit.Agreed agreed = new Commit.Agreed(nextBallot(NONE), update("2"));
        PaxosCommit.requestHandler.doVerb(Message.builder(Verb.PAXOS_COMMIT_REQ, agreed).build());

        assertRows(execute("SELECT k FROM %s WHERE k = '2'"), row("2"));
        assertThat(ClientWarn.instance.getWarnings()).isNull();
        assertThat(responses).hasSize(1);
        assertThat(ReplicaWarnings.fromResponse(responses.get(0)))
        .containsExactly(String.format(IndexContext.ANALYZED_VALUE_NOT_INDEXED_WARNING, "v", keyspace(), index));
    }

    private PartitionUpdate update(String key)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        return new RowUpdateBuilder(cfs.metadata(), FBUtilities.timestampMicros(), key).add("v", NGRAM_VALUE).buildUpdate();
    }
}
