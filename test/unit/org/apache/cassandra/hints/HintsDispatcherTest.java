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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.IAccordService.IAccordResult;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.FAILURE;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.RETRY_DIFFERENT_SYSTEM;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.SUCCESS;
import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.TIMEOUT;
import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.net.Verb.HINT_RSP;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HintsDispatcherTest
{
    private static final String KEYSPACE = "hints_dispatcher_test";
    private static final String TABLE = "table";
    private static final String DROPPED_TABLE = "dropped_table";
    private static final UUID HOST_ID = UUID.randomUUID();
    private static InetAddressAndPort address;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        // the tests hold hint responses for a few seconds
        DatabaseDescriptor.setWriteRpcTimeout(TimeUnit.SECONDS.toMillis(10));
        address = FBUtilities.getBroadcastAddressAndPort();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE),
                                    SchemaLoader.standardCFMD(KEYSPACE, DROPPED_TABLE));
    }

    @After
    public void cleanup()
    {
        MockMessagingService.cleanup();
    }

    @Test
    public void testKeepPageWhenRetriedHintIsRejectedAgain() throws Exception
    {
        File directory = new File(Files.createTempDirectory(null));
        try
        {
            long now = System.currentTimeMillis();
            File file = writeHints(directory, hint(TABLE, "a", now, 3600));
            List<HintMessage> sent = new CopyOnWriteArrayList<>();
            rejectingSpy(m -> true, sent, m -> {});

            try (HintsDispatcher dispatcher = dispatcher(file))
            {
                assertFalse(dispatcher.dispatch());
            }
            assertEquals(Arrays.asList("a", "a"), keysOf(sent));
        }
        finally
        {
            directory.deleteRecursive();
        }
    }

    @Test
    public void testRetryRejectedHintAfterExpiredHint() throws Exception
    {
        File directory = new File(Files.createTempDirectory(null));
        try
        {
            long now = System.currentTimeMillis();
            File file = writeHints(directory,
                                   hint(TABLE, "a", now, 3),
                                   hint(TABLE, "b", now, 3600),
                                   hint(TABLE, "c", now, 3600));
            AtomicInteger rejections = new AtomicInteger(1);
            List<HintMessage> sent = new CopyOnWriteArrayList<>();
            // "a" expires before the rejected hint is sent again
            rejectingSpy(m -> "b".equals(keyOf(m)) && rejections.getAndDecrement() > 0, sent, m -> {
                if ("a".equals(keyOf(m)))
                    sleepUntil(now + 3100);
            });

            try (HintsDispatcher dispatcher = dispatcher(file))
            {
                assertTrue(dispatcher.dispatch());
            }
            assertRetried(sent, Arrays.asList("a", "b", "c"), "b");
        }
        finally
        {
            directory.deleteRecursive();
        }
    }

    @Test
    public void testDropRejectedHintThatExpired() throws Exception
    {
        File directory = new File(Files.createTempDirectory(null));
        try
        {
            long now = System.currentTimeMillis();
            File file = writeHints(directory, hint(TABLE, "a", now, 3));
            List<HintMessage> sent = new CopyOnWriteArrayList<>();
            // "a" expires before it is sent again
            rejectingSpy(m -> true, sent, m -> sleepUntil(now + 3100));

            try (HintsDispatcher dispatcher = dispatcher(file))
            {
                assertTrue(dispatcher.dispatch());
            }
            assertEquals(Arrays.asList("a"), keysOf(sent));
        }
        finally
        {
            directory.deleteRecursive();
        }
    }

    @Test
    public void testDropRejectedHintForDroppedTable() throws Exception
    {
        File directory = new File(Files.createTempDirectory(null));
        try
        {
            long now = System.currentTimeMillis();
            File file = writeHints(directory, hint(DROPPED_TABLE, "a", now, 3600));
            List<HintMessage> sent = new CopyOnWriteArrayList<>();
            // the table is dropped before the rejected hint is sent again
            rejectingSpy(m -> true, sent, m -> SchemaTestUtil.announceTableDrop(KEYSPACE, DROPPED_TABLE));

            try (HintsDispatcher dispatcher = dispatcher(file))
            {
                assertTrue(dispatcher.dispatch());
            }
            assertEquals(Arrays.asList("a"), keysOf(sent));
        }
        finally
        {
            directory.deleteRecursive();
        }
    }

    @Test
    public void testSplitHintFailsWhenOneLegFails()
    {
        assertEquals(FAILURE, splitHintOutcome(RETRY_DIFFERENT_SYSTEM, new RuntimeException()));
        assertEquals(TIMEOUT, splitHintOutcome(RETRY_DIFFERENT_SYSTEM, new WriteTimeoutException(WriteType.SIMPLE, ConsistencyLevel.ONE, 0, 1)));
        assertEquals(FAILURE, splitHintOutcome(FAILURE, new RetryOnDifferentSystemException()));
        assertEquals(RETRY_DIFFERENT_SYSTEM, splitHintOutcome(SUCCESS, new RetryOnDifferentSystemException()));
    }

    private static HintsDispatcher.Callback.Outcome splitHintOutcome(HintsDispatcher.Callback.Outcome normal, Throwable accordFailure)
    {
        AtomicReference<BiConsumer<? super TxnResult, Throwable>> accordCallback = new AtomicReference<>();
        IAccordResult<TxnResult> accordResult = new IAccordResult<>()
        {
            public TxnResult success() { return null; }
            public Throwable fail() { return accordFailure; }
            public TxnResult awaitAndGet() { throw new UnsupportedOperationException(); }
            public IAccordResult<TxnResult> addCallback(BiConsumer<? super TxnResult, Throwable> callback)
            {
                accordCallback.set(callback);
                return this;
            }
        };
        HintsDispatcher.Callback callback = new HintsDispatcher.Callback(address, System.currentTimeMillis(), accordResult);
        if (normal == SUCCESS)
            callback.onResponse(null);
        else
            callback.onFailure(address, RequestFailure.forReason(normal == FAILURE ? RequestFailureReason.UNKNOWN : RequestFailureReason.RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM));
        accordCallback.get().accept(null, accordFailure);
        return callback.await();
    }

    private static Hint hint(String tableName, String key, long creationTime, int gcgs)
    {
        TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, tableName);
        Mutation mutation = new RowUpdateBuilder(table, TimeUnit.MILLISECONDS.toMicros(creationTime), bytes(key))
                            .clustering("column")
                            .add("val", "value")
                            .build();
        return Hint.create(mutation, creationTime, gcgs);
    }

    private static File writeHints(File directory, Hint... hints) throws Exception
    {
        HintsDescriptor descriptor = new HintsDescriptor(HOST_ID, System.currentTimeMillis());
        try (HintsWriter writer = HintsWriter.create(directory, descriptor);
             HintsWriter.Session session = writer.newSession(ByteBuffer.allocate(1024)))
        {
            for (Hint hint : hints)
                session.append(hint);
        }
        return new File(directory, descriptor.fileName());
    }

    /**
     * Answers every hint with success, except the hints {@code rejected} selects, which are asked to retry on the other
     * transaction system. Each hint, as the peer reads it, is added to {@code sent} and passed to {@code beforeResponse}.
     */
    private static void rejectingSpy(Predicate<HintMessage> rejected, List<HintMessage> sent, Consumer<HintMessage> beforeResponse)
    {
        MockMessagingService.when(verb(HINT_REQ)).respond((message, to) -> {
            HintMessage hint = received(message);
            sent.add(hint);
            beforeResponse.accept(hint);
            if (!rejected.test(hint))
                return Message.internalResponse(HINT_RSP, NoPayload.noPayload);

            MessagingService.instance().callbacks.remove(message.id(), to).callback.onFailure(to, RequestFailure.forReason(RequestFailureReason.RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM));
            return null;
        });
    }

    private static HintMessage received(Message<?> message)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            HintMessage.serializer.serialize((SerializableHintMessage) message.payload, out, MessagingService.current_version);
            return HintMessage.serializer.deserialize(new DataInputBuffer(out.buffer(), false), MessagingService.current_version);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static String keyOf(HintMessage message)
    {
        try
        {
            return message.hint == null ? null : ByteBufferUtil.string(message.hint.mutation.key().getKey());
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    /** The first pass sends {@code page} in any order, then {@code retried} is sent again. */
    private static void assertRetried(List<HintMessage> sent, List<String> page, String retried)
    {
        List<String> keys = keysOf(sent);
        assertEquals(keys.toString(), page.size() + 1, keys.size());
        assertEquals(new HashSet<>(page), new HashSet<>(keys.subList(0, page.size())));
        assertEquals(retried, keys.get(page.size()));
    }

    private static List<String> keysOf(List<HintMessage> messages)
    {
        return messages.stream().map(HintsDispatcherTest::keyOf).collect(Collectors.toList());
    }

    private static void sleepUntil(long millis)
    {
        Uninterruptibles.sleepUninterruptibly(Math.max(0, millis - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
    }

    private static HintsDispatcher dispatcher(File file)
    {
        return HintsDispatcher.create(file, RateLimiter.create(1_000_000_000), address, HOST_ID, () -> false);
    }
}
