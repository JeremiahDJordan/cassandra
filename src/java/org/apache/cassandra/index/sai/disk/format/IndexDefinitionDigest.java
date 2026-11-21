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

package org.apache.cassandra.index.sai.disk.format;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.schema.IndexMetadata;

/**
 * The content of the completion marker of the per-index components of an SSTable: a digest of the definition of the
 * index they were built for, so that an index doesn't take the components of another index of the same name for its
 * own. Two indexes of the same definition build the same components from the same SSTable.
 * <p>
 * The content is a format byte ({@link #FORMAT}) followed by the SHA-256 of the definition: the kind of the index, then
 * its options sorted by name, where the class name is the one its alias expands to (see
 * {@link IndexMetadata#expandAliases}). Each string is written as the int length of its UTF-8 bytes followed by
 * them, and the number of options precedes them. Markers written before the digest are empty.
 */
public final class IndexDefinitionDigest
{
    static final byte FORMAT = 1;

    private static final int SIZE = 1 + 32;

    private IndexDefinitionDigest()
    {
    }

    /**
     * @return the content of the completion marker of the components built for the provided index
     */
    public static byte[] markerContent(IndexMetadata index)
    {
        Map<String, String> options = new TreeMap<>(index.options);
        String className = options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
        if (className != null)
            options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, IndexMetadata.expandAliases(className));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes))
        {
            writeString(out, index.kind.name());
            out.writeInt(options.size());
            for (Map.Entry<String, String> option : options.entrySet())
            {
                writeString(out, option.getKey());
                writeString(out, option.getValue());
            }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }

        byte[] content = new byte[SIZE];
        content[0] = FORMAT;
        System.arraycopy(sha256(bytes.toByteArray()), 0, content, 1, SIZE - 1);
        return content;
    }

    /**
     * @return whether the provided content of a completion marker is that of components built for the provided index,
     * or of components built before the digest was written (an empty marker)
     */
    public static boolean matches(byte[] markerContent, IndexMetadata index)
    {
        return markerContent.length == 0 || Arrays.equals(markerContent, markerContent(index));
    }

    private static void writeString(DataOutputStream out, String value) throws IOException
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] sha256(byte[] bytes)
    {
        try
        {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new AssertionError(e);
        }
    }
}
