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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundConnections;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * The client warnings raised while a replica applies a client's write, sent back to the coordinator with the write
 * response so that the coordinator adds them to the warnings of the client's request.
 * <p>
 * On the replica, a write is applied between {@link #capture()} and {@link #restore(Object)}: every
 * {@link ClientWarn#warn} call in between is kept on the thread (see {@link #add(String)}) instead of going to the
 * thread's {@link ClientWarn.State}, and {@link #drain()} returns what was kept. The warnings travel in the custom
 * params of the response under {@link #KEY}, for the coordinator's response handler to keep in a {@link Received}
 * until the request thread publishes them to its {@link ClientWarn.State}.
 */
public final class ReplicaWarnings
{
    private static final Logger logger = LoggerFactory.getLogger(ReplicaWarnings.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public static final String KEY = "REPLICA_WARNINGS";

    /** The maximum number of warnings a replica sends for a write, and a response handler keeps. */
    public static final int MAX_WARNINGS = 16;

    /** The maximum size of a warning in UTF-8 bytes; longer warnings are truncated. */
    public static final int MAX_WARNING_BYTES = 4096;

    /**
     * The maximum size of the warnings of a response, encoded; the warnings past it aren't sent. The custom params are
     * in the message header, which a large message must have whole in its first frame. A response also gets only the
     * warnings that keep it within the large message threshold and the max internode message size.
     */
    public static final int MAX_ENCODED_BYTES = 16 * 1024;

    // the most adding the param to a response takes besides its value: the key, the value's length, and the growth of
    // the custom params map, of its param and of the params count
    private static final int PARAM_OVERHEAD_BYTES = 32;

    // capturing, nothing captured yet
    private static final Object CAPTURING = new Object();

    // null (not capturing), CAPTURING, or the List<String> of the captured warnings
    private static final FastThreadLocal<Object> STATE = new FastThreadLocal<>();

    private ReplicaWarnings()
    {
    }

    /**
     * Starts capturing the client warnings of this thread.
     *
     * @return the previous state of the thread, to pass to {@link #restore(Object)}
     */
    public static Object capture()
    {
        Object previous = STATE.get();
        STATE.set(CAPTURING);
        return previous;
    }

    /**
     * Restores the state {@link #capture()} returned, dropping what wasn't drained.
     */
    public static void restore(Object previous)
    {
        STATE.set(previous);
    }

    public static boolean isCapturing()
    {
        return STATE.get() != null;
    }

    /**
     * Captures the given warning if this thread is capturing.
     *
     * @return {@code true} if the warning was captured or dropped as a duplicate or over the limit, {@code false} if
     * this thread isn't capturing
     */
    @SuppressWarnings("unchecked")
    public static boolean add(String warning)
    {
        Object state = STATE.get();
        if (state == null)
            return false;

        if (state == CAPTURING)
        {
            List<String> warnings = new ArrayList<>();
            warnings.add(truncate(warning));
            STATE.set(warnings);
            return true;
        }

        List<String> warnings = (List<String>) state;
        if (warnings.size() < MAX_WARNINGS)
        {
            String truncated = truncate(warning);
            if (!warnings.contains(truncated))
                warnings.add(truncated);
        }
        return true;
    }

    /**
     * Returns the warnings captured since {@link #capture()} or the last call of this method, and keeps capturing.
     *
     * @return the warnings, or {@code null} if there are none
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public static List<String> drain()
    {
        Object state = STATE.get();
        if (state == null || state == CAPTURING)
            return null;

        STATE.set(CAPTURING);
        return (List<String>) state;
    }

    /**
     * Returns the warning as is if it fits {@link #MAX_WARNING_BYTES} in UTF-8, or its longest prefix of whole code
     * points that does.
     */
    @VisibleForTesting
    static String truncate(String warning)
    {
        if (warning.length() <= MAX_WARNING_BYTES / 3)
            return warning;

        int bytes = 0;
        for (int i = 0; i < warning.length(); )
        {
            int codePoint = warning.codePointAt(i);
            bytes += utf8Length(codePoint);
            if (bytes > MAX_WARNING_BYTES)
                return warning.substring(0, i);
            i += Character.charCount(codePoint);
        }
        return warning;
    }

    private static int utf8Length(int codePoint)
    {
        if (codePoint < 0x80)
            return 1;
        if (codePoint < 0x800)
            return 2;
        if (codePoint < 0x10000)
            return 3;
        return 4;
    }

    /**
     * Adds the given warnings to a response sent to the given node, if that node reads them, as many as fit
     * {@link #MAX_ENCODED_BYTES} and keep the response within the large message threshold and the max internode
     * message size.
     */
    public static void addToResponse(@Nullable List<String> warnings, Message.Builder<?> response, InetAddressAndPort destination)
    {
        if (warnings == null || !shouldSend(destination))
            return;

        int maxResponseSize = Math.min(OutboundConnections.LARGE_MESSAGE_THRESHOLD, DatabaseDescriptor.getInternodeMaxMessageSizeInBytes());
        int available = maxResponseSize - response.currentSerializedSize(MessagingService.current_version) - PARAM_OVERHEAD_BYTES;
        byte[] encoded = encode(warnings, Math.min(MAX_ENCODED_BYTES, available));
        if (encoded != null)
            response.withCustomParam(KEY, encoded);
    }

    private static boolean shouldSend(InetAddressAndPort destination)
    {
        MessagingService messaging = MessagingService.instance();
        return shouldSend(MessagingService.current_version,
                          messaging.versions.getRawIfKnown(destination),
                          messaging.hasConnectionWithVersionNotMatching(destination, MessagingService::isDataStax50OrLater),
                          destination.equals(FBUtilities.getBroadcastAddressAndPort()));
    }

    /**
     * Whether a node with the given messaging version sends replica warnings to a node, which reads them only from
     * DataStax 5.0 on (older versions ignore the param). A node whose version isn't known gets none.
     *
     * @param currentVersion the messaging version of this node
     * @param peerVersion the messaging version the node announced, or {@code null} if it isn't known
     * @param incompatibleConnection whether a connection to the node uses a version older than DataStax 5.0
     * @param self whether the node is this node
     */
    @VisibleForTesting
    static boolean shouldSend(int currentVersion, @Nullable Integer peerVersion, boolean incompatibleConnection, boolean self)
    {
        if (!MessagingService.isDataStax50OrLater(currentVersion))
            return false;

        if (self)
            return true;

        return peerVersion != null && MessagingService.isDataStax50OrLater(peerVersion) && !incompatibleConnection;
    }

    @Nullable
    @VisibleForTesting
    public static byte[] encode(List<String> warnings)
    {
        return encode(warnings, MAX_ENCODED_BYTES);
    }

    /**
     * Encodes the first of the given warnings that fit the given size: their number, then each as an unsigned vint
     * length followed by its UTF-8 bytes.
     *
     * @return the encoded warnings, or {@code null} if not even the first one fits
     */
    @Nullable
    private static byte[] encode(List<String> warnings, int maxBytes)
    {
        List<byte[]> encoded = new ArrayList<>(warnings.size());
        int entriesSize = 0;
        for (String warning : warnings)
        {
            byte[] bytes = warning.getBytes(StandardCharsets.UTF_8);
            int entrySize = VIntCoding.computeUnsignedVIntSize(bytes.length) + bytes.length;
            if (VIntCoding.computeUnsignedVIntSize(encoded.size() + 1) + entriesSize + entrySize > maxBytes)
                break;
            encoded.add(bytes);
            entriesSize += entrySize;
        }

        if (encoded.isEmpty())
            return null;

        ByteBuffer buffer = ByteBuffer.allocate(VIntCoding.computeUnsignedVIntSize(encoded.size()) + entriesSize);
        VIntCoding.writeUnsignedVInt32(encoded.size(), buffer);
        for (byte[] bytes : encoded)
        {
            VIntCoding.writeUnsignedVInt32(bytes.length, buffer);
            buffer.put(bytes);
        }
        return buffer.array();
    }

    /**
     * The encoded warnings of the given response, if it has some.
     */
    @Nullable
    public static byte[] encoded(Message<?> response)
    {
        Map<String, byte[]> params = response.header.customParams();
        return params == null ? null : params.get(KEY);
    }

    /**
     * The warnings of the given response.
     *
     * @return the warnings, or {@code null} if the response has none, or if they are malformed
     */
    @Nullable
    public static List<String> fromResponse(Message<?> response)
    {
        byte[] encoded = encoded(response);
        return encoded == null ? null : decode(encoded);
    }

    /**
     * Decodes warnings {@link #encode encoded} by a replica. At most {@link #MAX_WARNINGS} are read: the bytes after
     * them are ignored, for a later version to send more.
     *
     * @return the warnings, or {@code null} if the value is malformed
     */
    @Nullable
    public static List<String> decode(byte[] encoded)
    {
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        long count = readUnsignedVInt(buffer);
        if (count <= 0 || count > Integer.MAX_VALUE)
            return malformed();

        int toRead = (int) Math.min(count, MAX_WARNINGS);
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                                                       .onMalformedInput(CodingErrorAction.REPORT)
                                                       .onUnmappableCharacter(CodingErrorAction.REPORT);
        List<String> warnings = new ArrayList<>(toRead);
        for (int i = 0; i < toRead; i++)
        {
            long length = readUnsignedVInt(buffer);
            if (length < 0 || length > MAX_WARNING_BYTES || length > buffer.remaining())
                return malformed();

            ByteBuffer bytes = buffer.slice();
            bytes.limit((int) length);
            try
            {
                warnings.add(decoder.decode(bytes).toString());
            }
            catch (CharacterCodingException e)
            {
                return malformed();
            }
            buffer.position(buffer.position() + (int) length);
        }

        if (count <= MAX_WARNINGS && buffer.hasRemaining())
            return malformed();

        return warnings;
    }

    // the unsigned vint at the position of the buffer, moving past it, or -1 if it's truncated or over a long's range
    private static long readUnsignedVInt(ByteBuffer buffer)
    {
        int size = VIntCoding.computeUnsignedVIntSize(buffer, buffer.position());
        if (size < 0)
            return -1;

        long value = VIntCoding.getUnsignedVInt(buffer, buffer.position());
        if (value < 0)
            return -1;

        buffer.position(buffer.position() + size);
        return value;
    }

    private static List<String> malformed()
    {
        noSpamLogger.warn("Ignored malformed replica warnings in a write response");
        return null;
    }

    /**
     * Decodes the warnings of a response (see {@link #encoded(Message)}) and keeps them in the given field of a
     * response handler, unless it's closed or they are malformed.
     */
    public static <T> void keep(AtomicReferenceFieldUpdater<T, Received> updater, T handler, byte[] encoded)
    {
        List<String> warnings = decode(encoded);
        if (warnings != null)
            keep(updater, handler, warnings);
    }

    /**
     * Keeps the given warnings in the given field of a response handler, unless it's closed.
     */
    public static <T> void keep(AtomicReferenceFieldUpdater<T, Received> updater, T handler, List<String> warnings)
    {
        while (true)
        {
            Received received = updater.get(handler);
            if (received == Received.CLOSED)
                return;

            if (received == null)
            {
                if (updater.compareAndSet(handler, null, new Received(warnings)))
                    return;
                continue;
            }

            received.add(warnings);
            return;
        }
    }

    /**
     * Closes the given field of a response handler to further warnings. This is idempotent.
     */
    public static <T> void seal(AtomicReferenceFieldUpdater<T, Received> updater, T handler)
    {
        sealed(updater, handler);
    }

    /**
     * Closes the given field of a response handler to further warnings, like {@link #seal}.
     *
     * @return the warnings the handler keeps, or {@code null} if there are none
     */
    @Nullable
    public static <T> List<String> close(AtomicReferenceFieldUpdater<T, Received> updater, T handler)
    {
        Received received = sealed(updater, handler);
        return received == null ? null : received.snapshot();
    }

    // the closed warnings of the field, or null if it keeps none
    @Nullable
    private static <T> Received sealed(AtomicReferenceFieldUpdater<T, Received> updater, T handler)
    {
        while (true)
        {
            Received received = updater.get(handler);
            if (received == Received.CLOSED)
                return null;

            if (received == null)
            {
                if (updater.compareAndSet(handler, null, Received.CLOSED))
                    return null;
                continue;
            }

            received.seal();
            return received;
        }
    }

    /**
     * Drops the warnings of the given field of a response handler, and closes it to further warnings.
     */
    public static <T> void discard(AtomicReferenceFieldUpdater<T, Received> updater, T handler)
    {
        updater.getAndSet(handler, Received.CLOSED);
    }

    /**
     * Adds the given warnings to the client warnings of this thread, keyed by their text.
     */
    public static void publish(@Nullable List<String> warnings)
    {
        if (warnings == null)
            return;

        for (String warning : warnings)
            ClientWarn.instance.warn(warning, warning);
    }

    /**
     * The warnings a response handler received from replicas, at most {@link #MAX_WARNINGS} distinct texts, until
     * it's closed.
     */
    public static final class Received
    {
        /** A closed handler that keeps no warnings. */
        static final Received CLOSED = new Received();

        private final Set<String> warnings = new LinkedHashSet<>();
        private boolean closed;

        private Received()
        {
            this.closed = true;
        }

        private Received(List<String> warnings)
        {
            addAll(warnings);
        }

        private synchronized void add(List<String> warnings)
        {
            if (!closed)
                addAll(warnings);
        }

        private void addAll(List<String> warnings)
        {
            for (String warning : warnings)
            {
                if (this.warnings.size() >= MAX_WARNINGS)
                    return;
                this.warnings.add(warning);
            }
        }

        private synchronized void seal()
        {
            closed = true;
        }

        private synchronized List<String> snapshot()
        {
            return new ArrayList<>(warnings);
        }
    }
}
