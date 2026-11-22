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

package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.net.MessagingService.VERSION_50;
import static org.apache.cassandra.net.MessagingService.VERSION_DSE_68;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_10;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_20;
import static org.apache.cassandra.net.MessagingService.VERSION_DS_21;
import static org.apache.cassandra.service.ReplicaWarnings.MAX_ENCODED_BYTES;
import static org.apache.cassandra.service.ReplicaWarnings.MAX_WARNINGS;
import static org.apache.cassandra.service.ReplicaWarnings.MAX_WARNING_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReplicaWarningsTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testNestedCapture()
    {
        assertFalse(ReplicaWarnings.isCapturing());
        assertFalse(ReplicaWarnings.add("not captured"));

        Object outer = ReplicaWarnings.capture();
        try
        {
            assertTrue(ReplicaWarnings.add("outer"));

            Object inner = ReplicaWarnings.capture();
            try
            {
                assertNull(ReplicaWarnings.drain());
                ReplicaWarnings.add("inner");
                assertEquals(Collections.singletonList("inner"), ReplicaWarnings.drain());
                assertNull(ReplicaWarnings.drain());
                ReplicaWarnings.add("dropped");
            }
            finally
            {
                ReplicaWarnings.restore(inner);
            }

            assertEquals(Collections.singletonList("outer"), ReplicaWarnings.drain());
        }
        finally
        {
            ReplicaWarnings.restore(outer);
        }
        assertFalse(ReplicaWarnings.isCapturing());
    }

    @Test
    public void testClientWarnIsCaptured()
    {
        ClientWarn.instance.captureWarnings();
        try
        {
            Object previous = ReplicaWarnings.capture();
            try
            {
                ClientWarn.instance.warn("a");
                ClientWarn.instance.warn("a", "key");
                ClientWarn.instance.warn("b", "key");
                assertEquals(Arrays.asList("a", "b"), ReplicaWarnings.drain());
            }
            finally
            {
                ReplicaWarnings.restore(previous);
            }

            assertNull(ClientWarn.instance.getWarnings());
            ClientWarn.instance.warn("c");
            assertEquals(Collections.singletonList("c"), ClientWarn.instance.getWarnings());
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    @Test
    public void testCaptureLimit()
    {
        Object previous = ReplicaWarnings.capture();
        try
        {
            for (int i = 0; i < MAX_WARNINGS + 5; i++)
            {
                ReplicaWarnings.add("w" + i);
                ReplicaWarnings.add("w" + i);
            }
            List<String> warnings = ReplicaWarnings.drain();
            assertThat(warnings).hasSize(MAX_WARNINGS);
            assertEquals("w0", warnings.get(0));
            assertEquals("w" + (MAX_WARNINGS - 1), warnings.get(MAX_WARNINGS - 1));
        }
        finally
        {
            ReplicaWarnings.restore(previous);
        }
    }

    @Test
    public void testTruncation()
    {
        String ascii = "a".repeat(MAX_WARNING_BYTES);
        assertEquals(ascii, ReplicaWarnings.truncate(ascii));
        assertEquals(ascii, ReplicaWarnings.truncate(ascii + "b"));

        // 2-byte characters: the last one would end past the limit
        String twoBytes = "a" + "é".repeat(MAX_WARNING_BYTES / 2);
        assertEquals("a" + "é".repeat(MAX_WARNING_BYTES / 2 - 1), ReplicaWarnings.truncate(twoBytes));

        // a surrogate pair (4 bytes) is never split
        String pairs = "ab" + "😀".repeat(MAX_WARNING_BYTES / 4);
        String truncated = ReplicaWarnings.truncate(pairs);
        assertEquals("ab" + "😀".repeat(MAX_WARNING_BYTES / 4 - 1), truncated);
        assertThat(truncated.getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(MAX_WARNING_BYTES);

        // the truncated text is what's captured, so its duplicates are dropped
        Object previous = ReplicaWarnings.capture();
        try
        {
            ReplicaWarnings.add(pairs);
            ReplicaWarnings.add(pairs + "x");
            assertEquals(Collections.singletonList(truncated), ReplicaWarnings.drain());
        }
        finally
        {
            ReplicaWarnings.restore(previous);
        }
    }

    @Test
    public void testEncodeDecode()
    {
        List<String> warnings = Arrays.asList("a", "", "é😀", "b".repeat(MAX_WARNING_BYTES));
        assertEquals(warnings, ReplicaWarnings.decode(ReplicaWarnings.encode(warnings)));

        List<String> max = new ArrayList<>();
        for (int i = 0; i < MAX_WARNINGS; i++)
            max.add("w" + i);
        assertEquals(max, ReplicaWarnings.decode(ReplicaWarnings.encode(max)));
    }

    @Test
    public void testEncodedSizeLimit()
    {
        List<String> warnings = new ArrayList<>();
        for (int i = 0; i < MAX_WARNINGS; i++)
            warnings.add(String.format("%02d", i) + "x".repeat(MAX_WARNING_BYTES - 2));

        // 3 warnings of the max size fit, with their lengths and count
        byte[] encoded = ReplicaWarnings.encode(warnings);
        assertThat(encoded.length).isLessThanOrEqualTo(MAX_ENCODED_BYTES);
        assertEquals(warnings.subList(0, 3), ReplicaWarnings.decode(encoded));

        // the warnings after one that doesn't fit aren't sent either
        List<String> tooLarge = Arrays.asList("x".repeat(MAX_ENCODED_BYTES), "a");
        assertNull(ReplicaWarnings.encode(tooLarge));
    }

    @Test
    public void testDecodeIgnoresWarningsOverLimit()
    {
        List<String> first = new ArrayList<>();
        for (int i = 0; i < MAX_WARNINGS; i++)
            first.add("w" + i);

        // a count of 17 with only 16 entries, and with a 17th entry, malformed or not
        assertEquals(first, ReplicaWarnings.decode(withCount(MAX_WARNINGS + 1, ReplicaWarnings.encode(first))));
        List<String> more = new ArrayList<>(first);
        more.add("w16");
        assertEquals(first, ReplicaWarnings.decode(ReplicaWarnings.encode(more)));
        byte[] trailing = Arrays.copyOf(ReplicaWarnings.encode(more), ReplicaWarnings.encode(more).length + 3);
        trailing[trailing.length - 3] = (byte) 0xff;
        assertEquals(first, ReplicaWarnings.decode(trailing));
    }

    @Test
    public void testDecodeMalformed()
    {
        // count 0
        assertNull(ReplicaWarnings.decode(new byte[]{ 0 }));
        // empty, and truncated vint
        assertNull(ReplicaWarnings.decode(new byte[0]));
        assertNull(ReplicaWarnings.decode(new byte[]{ (byte) 0x80 }));
        // count out of the range of an int
        assertNull(ReplicaWarnings.decode(vint(Integer.MAX_VALUE + 1L)));
        assertNull(ReplicaWarnings.decode(vint(-1L)));
        // a length over the max, or over the remaining bytes
        assertNull(ReplicaWarnings.decode(concat(vint(1), vint(MAX_WARNING_BYTES + 1), new byte[MAX_WARNING_BYTES + 1])));
        assertNull(ReplicaWarnings.decode(concat(vint(1), vint(-1L))));
        assertNull(ReplicaWarnings.decode(concat(vint(1), vint(3), new byte[]{ 'a', 'b' })));
        // invalid UTF-8
        assertNull(ReplicaWarnings.decode(concat(vint(1), vint(2), new byte[]{ (byte) 0xc3, 'a' })));
        // trailing bytes
        assertNull(ReplicaWarnings.decode(concat(ReplicaWarnings.encode(Collections.singletonList("a")), new byte[]{ 0 })));
        // fewer entries than counted
        assertNull(ReplicaWarnings.decode(withCount(2, ReplicaWarnings.encode(Collections.singletonList("a")))));
    }

    @Test
    public void testNoParamWithoutWarnings()
    {
        Message<NoPayload> request = Message.out(Verb.ECHO_REQ, NoPayload.noPayload);
        Message.Builder<NoPayload> response = request.emptyResponseBuilder();
        ReplicaWarnings.addToResponse(null, response, FBUtilities.getBroadcastAddressAndPort());
        assertNull(response.build().header.customParams());

        response = request.emptyResponseBuilder();
        ReplicaWarnings.addToResponse(Collections.emptyList(), response, FBUtilities.getBroadcastAddressAndPort());
        assertNull(response.build().header.customParams());

        response = request.emptyResponseBuilder();
        ReplicaWarnings.addToResponse(Collections.singletonList("w"), response, FBUtilities.getBroadcastAddressAndPort());
        assertEquals(Collections.singletonList("w"), ReplicaWarnings.fromResponse(response.build()));
    }

    @Test
    public void testResponseSizeLimit()
    {
        int maxMessageSize = DatabaseDescriptor.getInternodeMaxMessageSizeInBytes();
        DatabaseDescriptor.setInternodeMaxMessageSizeInBytes(16 * 1024);
        try
        {
            // 16 warnings of 1020 bytes fit the size limit of the warnings, but not a response of 16KiB at most
            List<String> warnings = new ArrayList<>();
            for (int i = 0; i < MAX_WARNINGS; i++)
                warnings.add(String.format("%02d", i) + "x".repeat(1018));

            int previous = MAX_WARNINGS;
            for (int otherParamSize : new int[]{ 0, 8 * 1024, 16 * 1024 - 64 })
            {
                Message.Builder<NoPayload> response = Message.out(Verb.ECHO_REQ, NoPayload.noPayload).emptyResponseBuilder();
                if (otherParamSize > 0)
                    response.withCustomParam("other", new byte[otherParamSize]);
                ReplicaWarnings.addToResponse(warnings, response, FBUtilities.getBroadcastAddressAndPort());
                Message<NoPayload> built = response.build();

                List<String> sent = ReplicaWarnings.fromResponse(built);
                int count = sent == null ? 0 : sent.size();
                assertThat(count).as("with another param of %d bytes", otherParamSize).isLessThan(previous);
                if (sent != null)
                    assertEquals(warnings.subList(0, count), sent);
                assertThat(built.serializedSize(MessagingService.current_version)).isLessThanOrEqualTo(16 * 1024);
                previous = count;
            }
            assertEquals(0, previous);
        }
        finally
        {
            DatabaseDescriptor.setInternodeMaxMessageSizeInBytes(maxMessageSize);
        }
    }

    @Test
    public void testShouldSend()
    {
        int current = MessagingService.current_version;
        assertTrue(MessagingService.isDataStax50OrLater(current));

        assertFalse(ReplicaWarnings.shouldSend(current, null, false, false));
        assertFalse(ReplicaWarnings.shouldSend(current, VERSION_DSE_68, false, false));
        assertFalse(ReplicaWarnings.shouldSend(current, VERSION_DS_10, false, false));
        assertFalse(ReplicaWarnings.shouldSend(current, VERSION_50, false, false));
        assertTrue(ReplicaWarnings.shouldSend(current, VERSION_DS_20, false, false));
        assertTrue(ReplicaWarnings.shouldSend(current, VERSION_DS_21, false, false));

        // a DataStax 5.0 node with an open connection of an older version
        assertFalse(ReplicaWarnings.shouldSend(current, VERSION_DS_21, true, false));

        // this node pinned to an older version
        assertFalse(ReplicaWarnings.shouldSend(VERSION_DS_10, VERSION_DS_21, false, false));
        assertFalse(ReplicaWarnings.shouldSend(VERSION_50, null, false, true));

        // this node, whose version may not be recorded
        assertTrue(ReplicaWarnings.shouldSend(current, null, false, true));
        assertTrue(ReplicaWarnings.shouldSend(VERSION_DS_20, null, false, true));
    }

    private static byte[] vint(long value)
    {
        ByteBuffer buffer = ByteBuffer.allocate(VIntCoding.computeUnsignedVIntSize(value));
        VIntCoding.writeUnsignedVInt(value, buffer);
        return buffer.array();
    }

    private static byte[] concat(byte[]... arrays)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Arrays.stream(arrays).mapToInt(a -> a.length).sum());
        for (byte[] array : arrays)
            buffer.put(array);
        return buffer.array();
    }

    // the encoded warnings with the given count instead of their own
    private static byte[] withCount(int count, byte[] encoded)
    {
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        int countSize = VIntCoding.computeUnsignedVIntSize(buffer, 0);
        return concat(vint(count), Arrays.copyOfRange(encoded, countSize, encoded.length));
    }
}
