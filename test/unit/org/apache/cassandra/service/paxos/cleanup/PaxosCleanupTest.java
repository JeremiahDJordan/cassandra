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

package org.apache.cassandra.service.paxos.cleanup;

import java.io.IOException;
import java.util.Collections;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.IGossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class PaxosCleanupTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testLocalStateChangeAfterStartPrepare() throws IOException
    {
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        VersionedValue.VersionedValueFactory values = new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance);
        EndpointState localState = new EndpointState(HeartBeatState.empty());
        localState.addApplicationState(ApplicationState.SEVERITY, values.severity(0.0));

        IGossiper gossiper = Mockito.mock(IGossiper.class);
        Mockito.doReturn(localState).when(gossiper).getEndpointStateForEndpoint(self);
        MessageDelivery messaging = Mockito.mock(MessageDelivery.class);
        SharedContext ctx = Mockito.mock(SharedContext.class);
        Mockito.doReturn(self).when(ctx).broadcastAddressAndPort();
        Mockito.doReturn(gossiper).when(ctx).gossiper();
        Mockito.doReturn(messaging).when(ctx).messaging();

        TableMetadata table = TableMetadata.builder("ks", "tbl").addPartitionKeyColumn("k", Int32Type.instance).build();
        Token token = Murmur3Partitioner.instance.getMinimumToken();
        PaxosCleanup.cleanup(ctx, Collections.singleton(self), table, Collections.singleton(new Range<>(token, token)), false, Runnable::run);

        ArgumentCaptor<Message<?>> sent = ArgumentCaptor.forClass(Message.class);
        Mockito.verify(messaging).sendWithCallback(sent.capture(), Mockito.eq(self), Mockito.any());
        Message<?> message = sent.getValue();
        int size = message.serializedSize(MessagingService.current_version);

        localState.addApplicationState(ApplicationState.SEVERITY, values.severity(123.456));

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(message, out, MessagingService.current_version);
            Assert.assertEquals(size, out.getLength());
        }
    }
}
