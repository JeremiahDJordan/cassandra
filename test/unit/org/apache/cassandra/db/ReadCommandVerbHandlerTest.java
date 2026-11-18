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

package org.apache.cassandra.db;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.Verb.*;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadCommandVerbHandlerTest
{
    private final static Random random = new Random();

    private static ReadCommandVerbHandler handler;
    private static TableMetadata metadata;
    private static TableMetadata metadata_with_transient;
    private static DecoratedKey KEY;

    private static final String TEST_NAME = "read_command_vh_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace_replicated";
    private static final String KEYSPACE_WITH_TRANSIENT = TEST_NAME + "ks_with_transient";
    private static final String TABLE = "table1";

    @BeforeClass
    public static void init() throws Throwable
    {
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(TEST_NAME);
        metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        metadata_with_transient = Schema.instance.getTableMetadata(KEYSPACE_WITH_TRANSIENT, TABLE);
        KEY = key(metadata, 1);

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(KEY.getToken(), InetAddressAndPort.getByName("127.0.0.2"));
        tmd.updateNormalToken(key(metadata, 2).getToken(), InetAddressAndPort.getByName("127.0.0.3"));
        tmd.updateNormalToken(key(metadata, 3).getToken(), FBUtilities.getBroadcastAddressAndPort());
    }

    @Before
    public void setup()
    {
        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().outboundSink.add((message, to) -> false);
        MessagingService.instance().inboundSink.add((message) -> false);

        handler = new ReadCommandVerbHandler();
    }

    @Test
    public void setRepairedDataTrackingFlagIfHeaderPresent()
    {
        TrackingSinglePartitionReadCommand command = new TrackingSinglePartitionReadCommand(metadata);
        assertFalse(command.isTrackingRepairedData());
        handler.doVerb(Message.builder(READ_REQ, (ReadCommand) command)
                              .from(peer())
                              .withFlag(MessageFlag.TRACK_REPAIRED_DATA)
                              .withId(messageId())
                              .build());
        assertTrue(command.isTrackingRepairedData());
    }

    @Test
    public void dontSetRepairedDataTrackingFlagUnlessHeaderPresent()
    {
        TrackingSinglePartitionReadCommand command = new TrackingSinglePartitionReadCommand(metadata);
        assertFalse(command.isTrackingRepairedData());
        handler.doVerb(Message.builder(READ_REQ, (ReadCommand) command)
                              .from(peer())
                              .withId(messageId())
                              .withParam(ParamType.TRACE_SESSION, nextTimeUUID())
                              .build());
        assertFalse(command.isTrackingRepairedData());
    }

    @Test
    public void dontSetRepairedDataTrackingFlagIfHeadersEmpty()
    {
        TrackingSinglePartitionReadCommand command = new TrackingSinglePartitionReadCommand(metadata);
        assertFalse(command.isTrackingRepairedData());
        handler.doVerb(Message.builder(READ_REQ, (ReadCommand) command)
                              .withId(messageId())
                              .from(peer())
                              .build());
        assertFalse(command.isTrackingRepairedData());
    }

    /**
     * Replicas relax the filtering of non-strict reads (CASSANDRA-19018) only for coordinators that ask for it with
     * the custom message parameter, whatever their messaging version.
     */
    @Test
    public void coordinatorAsksForNonStrictFilteringOnlyWithTheParameter() throws IOException
    {
        ReadCommand command = new TrackingSinglePartitionReadCommand(metadata);

        Message<ReadCommand> withoutParam = Message.builder(READ_REQ, command).from(peer()).withId(messageId()).build();
        assertFalse(ReadCommandVerbHandler.coordinatorAsksForNonStrictFiltering(withoutParam));
        Message<ReadCommand> withParam = ReadCommand.withNonStrictFilteringParam(withoutParam);
        assertTrue(ReadCommandVerbHandler.coordinatorAsksForNonStrictFiltering(withParam));

        // the parameter goes along with other custom parameters, and survives serialization
        Message<ReadCommand> withOtherParam = Message.builder(READ_REQ, command).from(peer()).withId(messageId()).withCustomParam("other", new byte[]{ 2 }).build();
        assertFalse(ReadCommandVerbHandler.coordinatorAsksForNonStrictFiltering(withOtherParam));
        Message<ReadCommand> withBothParams = ReadCommand.withNonStrictFilteringParam(withOtherParam);
        assertArrayEquals(new byte[]{ 2 }, withBothParams.header.customParams().get("other"));
        Message<ReadCommand> deserialized = serializeRoundTrip(withBothParams);
        assertTrue(ReadCommandVerbHandler.coordinatorAsksForNonStrictFiltering(deserialized));
        assertArrayEquals(new byte[]{ 2 }, deserialized.header.customParams().get("other"));

        // a read from this node needs the parameter too
        Message<ReadCommand> fromSelf = Message.builder(READ_REQ, command)
                                               .from(FBUtilities.getBroadcastAddressAndPort())
                                               .withId(messageId())
                                               .build();
        assertFalse(ReadCommandVerbHandler.coordinatorAsksForNonStrictFiltering(fromSelf));
        Message<ReadCommand> fromSelfWithParam = ReadCommand.withNonStrictFilteringParam(fromSelf);
        assertTrue(ReadCommandVerbHandler.coordinatorAsksForNonStrictFiltering(fromSelfWithParam));

        // a coordinator on a messaging version below DS 10 needs the parameter too
        MessagingService.instance().versions.set(peer(), MessagingService.VERSION_50);
        try
        {
            assertFalse(ReadCommandVerbHandler.coordinatorAsksForNonStrictFiltering(withoutParam));
            assertTrue(ReadCommandVerbHandler.coordinatorAsksForNonStrictFiltering(withParam));
        }
        finally
        {
            MessagingService.instance().versions.reset(peer());
        }
    }

    private static Message<ReadCommand> serializeRoundTrip(Message<ReadCommand> message) throws IOException
    {
        int version = MessagingService.current_version;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(message, out, version);
            try (DataInputBuffer in = new DataInputBuffer(out.unsafeGetBufferAndFlip(), false))
            {
                return Message.serializer.deserialize(in, message.from(), version);
            }
        }
    }

    @Test (expected = InvalidRequestException.class)
    public void rejectsRequestWithNonMatchingTransientness()
    {
        ReadCommand command = new TrackingSinglePartitionReadCommand(metadata_with_transient);
        handler.doVerb(Message.builder(READ_REQ, command)
                              .from(peer())
                              .withId(messageId())
                              .build());
    }

    private static int messageId()
    {
        return random.nextInt();
    }

    private static InetAddressAndPort peer()
    {
        try
        {
            return InetAddressAndPort.getByAddress(new byte[]{ 127, 0, 0, 9});
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static class TrackingSinglePartitionReadCommand extends SinglePartitionReadCommand
    {
        private boolean trackingRepairedData = false;
        
        TrackingSinglePartitionReadCommand(TableMetadata metadata)
        {
            super(false,
                  0,
                  false,
                  metadata,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(metadata),
                  RowFilter.none(),
                  DataLimits.NONE,
                  KEY,
                  new ClusteringIndexSliceFilter(Slices.ALL, false),
                  null,
                  false,
                  null);
        }

        @Override
        public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
        {
            trackingRepairedData = executionController.isTrackingRepairedStatus();
            return super.executeLocally(executionController);
        }

        public boolean isTrackingRepairedData()
        {
            return trackingRepairedData;
        }
    }

    private static DecoratedKey key(TableMetadata metadata, int key)
    {
        return metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key));
    }
}
