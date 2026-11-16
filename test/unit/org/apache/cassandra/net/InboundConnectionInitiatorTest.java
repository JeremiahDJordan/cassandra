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

package org.apache.cassandra.net;

import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InboundConnectionInitiatorTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testHandshakeTimeoutWithoutInitiateMessage()
    {
        EmbeddedChannel channel = new EmbeddedChannel(new InboundConnectionInitiator.Handler(new InboundConnectionSettings()));
        assertHandshakeTimesOut(channel);
    }

    @Test
    public void testHandshakeTimeoutWithUnsupportedInitiateMessage()
    {
        EmbeddedChannel channel = new EmbeddedChannel(new InboundConnectionInitiator.Handler(new InboundConnectionSettings()));
        // the first message of a peer on a messaging version before 4.0, which is not decoded
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(Message.PROTOCOL_MAGIC);
        buf.writeInt(MessagingService.VERSION_30 << 8);
        buf.writeInt(0);
        channel.writeInbound(buf);
        assertHandshakeTimesOut(channel);
    }

    private static void assertHandshakeTimesOut(EmbeddedChannel channel)
    {
        assertTrue(channel.isOpen());
        channel.advanceTimeBy(HandshakeProtocol.TIMEOUT_MILLIS + 1, MILLISECONDS);
        channel.runScheduledPendingTasks();
        assertFalse(channel.isOpen());
    }
}
