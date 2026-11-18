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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.MessagingService.VERSION_DS_20;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_21;
import static org.junit.Assert.assertEquals;

/**
 * The serialized size of a received mutation matches what is written when it's sent again, for mutations that don't
 * serialize again to the bytes they were received as.
 */
public class MutationSerializedSizeTest extends CQLTester
{
    private static final int[][] RECEIVED_AND_SENT_VERSIONS = { { VERSION_DS_20, VERSION_DS_20 },
                                                                { VERSION_DS_20, VERSION_DS_21 },
                                                                { VERSION_DS_21, VERSION_DS_20 },
                                                                { VERSION_DS_21, VERSION_DS_21 } };

    @Test
    public void testCounterUpdate() throws IOException
    {
        // the local shard of a counter update is cleared when the leader receives it
        createTable("CREATE TABLE %s (k int, c int, total counter, PRIMARY KEY (k, c))");
        for (int[] versions : RECEIVED_AND_SENT_VERSIONS)
        {
            Mutation update = new RowUpdateBuilder(currentTableMetadata(), FBUtilities.timestampMicros(), 0)
                              .clustering(1)
                              .add("total", CounterContext.instance().createUpdate(1))
                              .build();
            Message<CounterMutation> sent = Message.out(Verb.COUNTER_MUTATION_REQ,
                                                        new CounterMutation(update, ConsistencyLevel.ONE));
            Message<CounterMutation> received = deserialize(serialize(sent, versions[0]), versions[0]);
            assertSerializedSize(received, versions);
        }
    }

    @Test
    public void testDroppedColumn() throws IOException
    {
        // a node that has dropped the column skips its cells, but forwards or replays the mutation as it received it
        createTable("CREATE TABLE %s (k int, c int, v int, w int, PRIMARY KEY (k, c))");
        List<byte[]> sent = new ArrayList<>();
        for (int[] versions : RECEIVED_AND_SENT_VERSIONS)
        {
            Mutation mutation = new RowUpdateBuilder(currentTableMetadata(), 1, 0).clustering(1)
                                                                                  .add("v", 1)
                                                                                  .add("w", 1)
                                                                                  .build();
            sent.add(serialize(Message.out(Verb.MUTATION_REQ, mutation), versions[0]));
        }
        execute("ALTER TABLE %s DROP w");

        for (int i = 0; i < RECEIVED_AND_SENT_VERSIONS.length; i++)
        {
            int[] versions = RECEIVED_AND_SENT_VERSIONS[i];
            Message<Mutation> received = deserialize(sent.get(i), versions[0]);
            assertSerializedSize(Message.out(Verb.MUTATION_REQ, received.payload), versions);
        }
    }

    private static byte[] serialize(Message<?> message, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(message, out, version);
            return out.toByteArray();
        }
    }

    private static <T> Message<T> deserialize(byte[] bytes, int version) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            return Message.serializer.deserialize(in, FBUtilities.getBroadcastAddressAndPort(), version);
        }
    }

    private static void assertSerializedSize(Message<?> message, int[] versions) throws IOException
    {
        int size = message.serializedSize(versions[1]);
        assertEquals("received at " + versions[0] + ", sent at " + versions[1],
                     size, serialize(message, versions[1]).length);
    }
}
