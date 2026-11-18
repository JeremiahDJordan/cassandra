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

import java.nio.file.Files;
import java.util.Arrays;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.Hex;

import static org.assertj.core.api.Assertions.assertThat;

public class IndexDefinitionDigestTest
{
    private static final String CLASS_NAME = "org.apache.cassandra.index.sai.StorageAttachedIndex";

    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testMarkerContent()
    {
        IndexMetadata index = IndexMetadata.fromSchemaMetadata("idx", IndexMetadata.Kind.CUSTOM,
                                                               ImmutableMap.of("target", "v", "class_name", CLASS_NAME));
        assertThat(Hex.bytesToHex(IndexDefinitionDigest.markerContent(index)))
        .isEqualTo("0169f4e7d502e80211f2a4c4293246cf46eb36b15c9a27463d0fff183dcf721d9f");

        IndexMetadata analyzed = IndexMetadata.fromSchemaMetadata("idx", IndexMetadata.Kind.CUSTOM,
                                                                  ImmutableMap.of("target", "v",
                                                                                  "index_analyzer", "standard",
                                                                                  "class_name", CLASS_NAME,
                                                                                  "case_sensitive", "false"));
        assertThat(Hex.bytesToHex(IndexDefinitionDigest.markerContent(analyzed)))
        .isEqualTo("0169490565ed0fe5ec22e844db26a62c69829eb57414c5440d1c3c67559a8e492c");
    }

    @Test
    public void testClassNameAlias()
    {
        IndexMetadata alias = IndexMetadata.fromSchemaMetadata("idx", IndexMetadata.Kind.CUSTOM,
                                                               ImmutableMap.of("target", "v", "class_name", "StorageAttachedIndex"));
        IndexMetadata full = IndexMetadata.fromSchemaMetadata("other_name", IndexMetadata.Kind.CUSTOM,
                                                              ImmutableMap.of("target", "v", "class_name", CLASS_NAME));
        assertThat(IndexDefinitionDigest.markerContent(alias)).isEqualTo(IndexDefinitionDigest.markerContent(full));
    }

    @Test
    public void testMatches()
    {
        IndexMetadata index = IndexMetadata.fromSchemaMetadata("idx", IndexMetadata.Kind.CUSTOM,
                                                               ImmutableMap.of("target", "v", "class_name", CLASS_NAME));
        IndexMetadata other = IndexMetadata.fromSchemaMetadata("idx", IndexMetadata.Kind.CUSTOM,
                                                               ImmutableMap.of("target", "w", "class_name", CLASS_NAME));
        byte[] content = IndexDefinitionDigest.markerContent(index);

        assertThat(IndexDefinitionDigest.matches(content, index)).isTrue();
        assertThat(IndexDefinitionDigest.matches(new byte[0], index)).isTrue();
        assertThat(IndexDefinitionDigest.matches(content, other)).isFalse();

        byte[] otherFormat = content.clone();
        otherFormat[0] = 2;
        assertThat(IndexDefinitionDigest.matches(otherFormat, index)).isFalse();
    }

    @Test
    public void testReadMarker() throws Exception
    {
        IndexMetadata index = IndexMetadata.fromSchemaMetadata("idx", IndexMetadata.Kind.CUSTOM,
                                                               ImmutableMap.of("target", "v", "class_name", CLASS_NAME));
        byte[] content = IndexDefinitionDigest.markerContent(index);
        byte[] longer = new byte[100];
        for (int i = 0; i < longer.length; i++)
            longer[i] = (byte) i;
        File file = FileUtils.createTempFile("marker", ".db");
        try
        {
            Files.write(file.toPath(), content);
            assertThat(IndexDefinitionDigest.readMarker(file)).isEqualTo(content);

            Files.write(file.toPath(), new byte[0]);
            assertThat(IndexDefinitionDigest.readMarker(file)).isEmpty();

            // a longer file reads as one byte more than a valid marker, so that it doesn't match
            Files.write(file.toPath(), longer);
            assertThat(IndexDefinitionDigest.readMarker(file)).isEqualTo(Arrays.copyOf(longer, content.length + 1));
        }
        finally
        {
            FileUtils.deleteWithConfirm(file);
        }
    }

    @Test
    public void testApacheMarker()
    {
        byte[] index = Hex.hexToBytes("3fd76c1702616100c02893e80000000000000000af214a74");
        byte[] empty = Hex.hexToBytes("3fd76c1702616101c02893e800000000000000005b1f9c3c");
        assertThat(IndexDefinitionDigest.isApacheMarker(index)).isTrue();
        assertThat(IndexDefinitionDigest.isApacheMarker(empty)).isTrue();

        assertThat(IndexDefinitionDigest.isApacheMarker(new byte[0])).isFalse();
        assertThat(IndexDefinitionDigest.isApacheMarker(Arrays.copyOf(index, index.length - 1))).isFalse();
        byte[] otherChecksum = index.clone();
        otherChecksum[otherChecksum.length - 1] ^= 1;
        assertThat(IndexDefinitionDigest.isApacheMarker(otherChecksum)).isFalse();
    }
}
