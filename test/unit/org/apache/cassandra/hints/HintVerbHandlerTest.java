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
package org.apache.cassandra.hints;

import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.exceptions.RequestFailureReason.INCOMPATIBLE_SCHEMA;
import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.apache.cassandra.net.Verb.FAILURE_RSP;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.net.Verb.HINT_RSP;
import static org.junit.Assert.assertEquals;

public class HintVerbHandlerTest
{
    private static final String KEYSPACE = "hint_verb_handler_test";
    private static final String TABLE = "table";

    private static InetAddressAndPort sender;

    @BeforeClass
    public static void setup() throws UnknownHostException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
        sender = InetAddressAndPort.getByName("127.0.0.2");
        Gossiper.instance.initializeNodeUnsafe(sender, UUID.randomUUID(), 1);
    }

    @After
    public void cleanup()
    {
        MockMessagingService.cleanup();
    }

    @Test
    public void testAcknowledgesHintForDroppedTable()
    {
        setSenderSchemaVersion(Schema.instance.getVersion());
        assertAcknowledged(unknownTableHint(sender, TableId.generate()));
    }

    @Test
    public void testFailsHintForTableCreatedAfterLocalSchema() throws Exception
    {
        setSenderSchemaVersion(UUID.randomUUID());
        assertFailed(unknownTableHint(sender, TableId.generate()));
    }

    @Test
    public void testFailsHintForTableCreatedSinceTheHintWasReceived() throws Exception
    {
        setSenderSchemaVersion(Schema.instance.getVersion());
        assertFailed(unknownTableHint(sender, Schema.instance.getTableMetadata(KEYSPACE, TABLE).id));
    }

    private static void setSenderSchemaVersion(UUID version)
    {
        Gossiper.instance.injectApplicationState(sender, ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(version));
    }

    private static Message<HintMessage> unknownTableHint(InetAddressAndPort from, TableId unknownTableId)
    {
        return Message.out(HINT_REQ, new HintMessage(UUID.randomUUID(), unknownTableId)).withFrom(from);
    }

    private static void assertAcknowledged(Message<HintMessage> message)
    {
        assertResponses(message, 1, 0);
    }

    private static void assertFailed(Message<HintMessage> message) throws Exception
    {
        MockMessagingSpy failures = assertResponses(message, 0, 1);
        assertEquals(INCOMPATIBLE_SCHEMA, failures.captureMessageOut().get(1, TimeUnit.SECONDS).payload);
    }

    private static MockMessagingSpy assertResponses(Message<HintMessage> message, int acks, int failures)
    {
        MockMessagingSpy ackSpy = MockMessagingService.when(verb(HINT_RSP)).dontReply();
        MockMessagingSpy failureSpy = MockMessagingService.when(verb(FAILURE_RSP)).dontReply();
        new HintVerbHandler().doVerb(message);
        assertEquals(acks, ackSpy.messagesIntercepted());
        assertEquals(failures, failureSpy.messagesIntercepted());
        return failureSpy;
    }
}
