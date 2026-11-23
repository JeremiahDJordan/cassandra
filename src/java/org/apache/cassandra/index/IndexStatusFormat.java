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

package org.apache.cassandra.index;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * The formats of the index status map gossiped as {@link org.apache.cassandra.gms.ApplicationState#INDEX_STATUS}.
 * <ul>
 *     <li>The legacy format maps each fully qualified index name to the name of its status:
 *     {@code {"ks1.idx1": "BUILD_SUCCEEDED"}}. It is the only format understood by HCD 1.2 and Cassandra 5.0.0-5.0.2.</li>
 *     <li>The compact format groups the indexes by keyspace and uses the numeric status codes:
 *     {@code {"ks1": {"idx1": 3}}}.</li>
 * </ul>
 * Nodes gossip the compact format, and the gossip serializer rewrites it in the legacy format for peers whose messaging
 * version is older than {@link org.apache.cassandra.net.MessagingService#VERSION_DS_20}, see
 * {@link #toLegacySerializedFormat(String)}.
 * <p>
 * This class has no state, so the gossip serializer can use it without initializing {@link IndexStatusManager}.
 */
public final class IndexStatusFormat
{
    private static final Logger logger = LoggerFactory.getLogger(IndexStatusFormat.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);

    /**
     * The longest gossiped value, in modified UTF-8 bytes, that {@link java.io.DataOutput#writeUTF(String)} can write.
     */
    private static final int MAX_GOSSIP_VALUE_LENGTH = 0xFFFF;

    private IndexStatusFormat() {}

    /**
     * Serializes as a JSON string the status of the indexes in the provided map, using the compact format.
     * <p>
     * For example, the map...
     * <pre>
     * {
     *     ks1.cf1_idx1=FULL_REBUILD_STARTED,
     *     ks1.cf1_idx2=FULL_REBUILD_STARTED,
     *     system.PaxosUncommittedIndex=BUILD_SUCCEEDED
     * }
     * </pre>
     * ...will be converted to the string...
     * <pre>
     * {
     *     "system": {"PaxosUncommittedIndex": 3},
     *     "ks1": {"cf1_idx1": 1, "cf1_idx2": 1}
     * }
     * </pre>
     */
    public static String toSerializedFormat(Map<String, Index.Status> indexStatusMap)
    {
        Map<String, Map<String, Integer>> serialized = new HashMap<>();

        for (Map.Entry<String, Index.Status> e : indexStatusMap.entrySet())
        {
            String[] keyspaceAndIndex = e.getKey().split("\\.");
            serialized.computeIfAbsent(keyspaceAndIndex[0], ignore -> new HashMap<>())
                      .put(keyspaceAndIndex[1], e.getValue().code);
        }

        return JsonUtils.writeAsJsonString(serialized);
    }

    /**
     * Serializes as a JSON string the status of the indexes in the provided map, using the legacy format.
     * <p>
     * For example, the map...
     * <pre>
     * {
     *     ks1.cf1_idx1=FULL_REBUILD_STARTED,
     *     system.PaxosUncommittedIndex=BUILD_SUCCEEDED
     * }
     * </pre>
     * ...will be converted to the string...
     * <pre>
     * {
     *     "ks1.cf1_idx1": "FULL_REBUILD_STARTED",
     *     "system.PaxosUncommittedIndex": "BUILD_SUCCEEDED"
     * }
     * </pre>
     */
    public static String toLegacySerializedFormat(Map<String, Index.Status> indexStatusMap)
    {
        Map<String, String> serialized = new HashMap<>();
        for (Map.Entry<String, Index.Status> e : indexStatusMap.entrySet())
            serialized.put(e.getKey(), e.getValue().name());
        return JsonUtils.writeAsJsonString(serialized);
    }

    /**
     * Rewrites a gossiped index status value in the legacy format, for peers that cannot parse the compact one.
     * A value already in the legacy format is returned unchanged, and so is a value that is not valid JSON: the peer
     * would not be able to read it in any format.
     * <p>
     * The compact format exists because the legacy one may not fit in a gossiped value. A value in the compact format
     * whose legacy rewrite is longer than {@link #MAX_GOSSIP_VALUE_LENGTH}, or that has a status code this node doesn't
     * know, gives {@code null}: some of the peers that need the legacy format fail on a compact value without catching
     * the failure (Apache Cassandra 5.0.0-5.0.2 casts each value to a string), so it must not be sent to them.
     *
     * @return the value in the legacy format, or {@code null} if it must not be sent to peers that need that format
     */
    @Nullable
    public static String toLegacySerializedFormat(String serialized)
    {
        // Only the compact format nests JSON objects: keyspace and index names cannot contain '{', so anything else
        // is either in the legacy format already or not valid, and is returned without parsing it.
        if (serialized.indexOf('{', 1) < 0)
            return serialized;

        Map<String, Object> parsed;
        try
        {
            parsed = JsonUtils.fromJsonMap(serialized);
        }
        catch (RuntimeException e)
        {
            noSpamLogger.debug("Unable to parse index status {}: {}", serialized, e.getMessage());
            return serialized;
        }
        if (parsed.values().stream().noneMatch(v -> v instanceof Map))
            return serialized;

        try
        {
            String legacy = toLegacySerializedFormat(fromParsedFormat(parsed));
            if (TypeSizes.encodedUTF8Length(legacy) <= MAX_GOSSIP_VALUE_LENGTH)
                return legacy;

            noSpamLogger.warn("The index status in the legacy format is too long to be gossiped ({} characters), " +
                              "it is not sent to peers that can only parse that format", legacy.length());
        }
        catch (RuntimeException e)
        {
            noSpamLogger.debug("Unable to convert index status {} to the legacy format: {}", serialized, e.getMessage());
        }
        return null;
    }

    /**
     * Parses an index status map in either the legacy or the compact format.
     *
     * @return a map from "keyspace.index" to the status of that index
     */
    public static Map<String, Index.Status> fromSerializedFormat(String serialized)
    {
        return fromParsedFormat(JsonUtils.fromJsonMap(serialized));
    }

    private static Map<String, Index.Status> fromParsedFormat(Map<String, Object> peerStatus)
    {
        Map<String, Index.Status> indexStatusMap = new HashMap<>();

        for (Map.Entry<String, Object> endpointStatus : peerStatus.entrySet())
        {
            String keyspaceOrIndex = endpointStatus.getKey();
            Object keyspaceOrIndexStatus = endpointStatus.getValue();

            if (keyspaceOrIndexStatus instanceof String)
            {
                // This is the legacy format: (fully qualified index name -> enum string)
                Index.Status status = Index.Status.valueOf(keyspaceOrIndexStatus.toString());
                indexStatusMap.put(keyspaceOrIndex, status);
            }
            else if (keyspaceOrIndexStatus instanceof Map)
            {
                // This is the new format. (keyspace -> (index -> numeric enum code))
                @SuppressWarnings("unchecked")
                Map<String, Integer> keyspaceIndexStatusMap = (Map<String, Integer>) keyspaceOrIndexStatus;

                for (Map.Entry<String, Integer> indexStatus : keyspaceIndexStatusMap.entrySet())
                {
                    Index.Status status = Index.Status.fromCode(indexStatus.getValue());
                    indexStatusMap.put(keyspaceOrIndex + '.' + indexStatus.getKey(), status);
                }
            }
            else
            {
                throw new MarshalException("Invalid index status format: " + endpointStatus);
            }
        }
        return indexStatusMap;
    }
}
