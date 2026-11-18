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
package org.apache.cassandra.index.sai.disk.format;

import java.util.Optional;
import javax.annotation.Nullable;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionTest
{
    @BeforeClass
    public static void initialise() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testVersionsSorted()
    {
        Version previous = null;
        for (Version version : Version.ALL)
        {
            if (previous != null)
            {
                assertTrue(previous.onOrAfter(version));
                assertTrue(previous.after(version));
                assertTrue(!version.onOrAfter(previous));
                assertTrue(!version.after(previous));
            }
            previous = version;
        }
    }

    @Test
    public void supportedVersionsWillParse()
    {
        assertEquals(Version.AA, Version.parse("aa"));
        assertEquals(Version.BA, Version.parse("ba"));
        assertEquals(Version.CA, Version.parse("ca"));
    }

    @Test
    public void unsupportedOrInvalidVersionsDoNotParse()
    {
        assertThatThrownBy(() -> Version.parse(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("ab")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("abc")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testAfterMethod()
    {
        // Do some basic checks, doesn't need to be updated for each new format.
        assertTrue(Version.ED.after(Version.EC));
        assertTrue(Version.EC.after(Version.EB));
        assertTrue(Version.EB.after(Version.DC));
        assertTrue(Version.DC.after(Version.DB));
        assertTrue(Version.DB.after(Version.CA));
        assertTrue(Version.CA.after(Version.BA));
        assertTrue(Version.BA.after(Version.AA));

        assertFalse(Version.AA.after(Version.BA));
        assertFalse(Version.AA.after(Version.AA));
    }

    @Test
    public void testOnOrAfterMethod()
    {
        // Do some basic checks, doesn't need to be updated for each new format.
        assertTrue(Version.ED.onOrAfter(Version.ED));
        assertTrue(Version.ED.onOrAfter(Version.EC));
        assertTrue(Version.CA.onOrAfter(Version.BA));
        assertTrue(Version.BA.onOrAfter(Version.AA));

        assertFalse(Version.AA.onOrAfter(Version.BA));
        assertFalse(Version.BA.onOrAfter(Version.CA));
    }

    // Specific test for AA because it doesn't use the same file formatter than other versions.
    @Test
    public void testAAFileFormatting()
    {
        assertFileFormatting("filename-SAI_GroupMeta.db", Version.AA, 0, IndexComponentType.GROUP_META, null);
        assertFileFormatting("filename-SAI_TokenValues_1.db", Version.AA, 1, IndexComponentType.TOKEN_VALUES, null);

        assertFileFormatting("filename-SAI_myIndex_Vector.db", Version.AA, 0, IndexComponentType.VECTOR, "myIndex");
        assertFileFormatting("filename-SAI_saiIndex_TermsData_42.db", Version.AA, 42, IndexComponentType.TERMS_DATA, "saiIndex");
    }

    @Test
    public void testFileFormatting()
    {
        for (Version version : Version.ALL)
        {
            // AA is tested separately above.
            if (version == Version.AA)
                continue;

            assertFileFormatting("filename-SAI+" + version + "+GroupMeta.db", version, 0, IndexComponentType.GROUP_META, null);
            assertFileFormatting("filename-SAI+" + version + "+1+TokenValues.db", version, 1, IndexComponentType.TOKEN_VALUES, null);

            assertFileFormatting("filename-SAI+" + version + "+myIndex+Vector.db", version, 0, IndexComponentType.VECTOR, "myIndex");
            assertFileFormatting("filename-SAI+" + version + "+42+saiIndex+TermsData.db", version, 42, IndexComponentType.TERMS_DATA, "saiIndex");
        }
    }

    @Test
    public void testInvalidFileFormatting()
    {
        assertThat(Version.tryParseFileName("")).isEmpty();
        assertThat(Version.tryParseFileName("filename-SAI_")).isEmpty();
        assertThat(Version.tryParseFileName("filemame-SAI_.db")).isEmpty();
        assertThat(Version.tryParseFileName("filemame-SAI.db")).isEmpty();
        assertThat(Version.tryParseFileName("filemame-SAI+foo.db")).isEmpty();
    }

    @Test
    public void testApacheFileNames()
    {
        // Apache's "aa" files ("SAI+aa+" names) are APACHE_AA, not our AA ("SAI_" names)
        assertApacheFileName("nb-1-big-SAI+aa+GroupComplete.db", IndexComponentType.GROUP_COMPLETION_MARKER, null);
        assertApacheFileName("nb-1-big-SAI+aa+PartitionKeyBlocks.db", IndexComponentType.PARTITION_KEY_BLOCKS, null);
        assertApacheFileName("nb-1-big-SAI+aa+ClusteringKeyBlockOffsets.db", IndexComponentType.CLUSTERING_KEY_BLOCK_OFFSETS, null);
        assertApacheFileName("nb-1-big-SAI+aa+myIndex+Meta.db", IndexComponentType.META, "myIndex");
        assertApacheFileName("nb-1-big-SAI+aa+myIndex+BalancedTree.db", IndexComponentType.BALANCED_TREE, "myIndex");
        assertApacheFileName("SAI+aa+myIndex+CompressedVectors.db", IndexComponentType.PQ, "myIndex");
        assertApacheFileName("SAI+aa+my_index+TermsData.db", IndexComponentType.TERMS_DATA, "my_index");

        // our names for the components Apache names differently aren't Apache's
        assertThat(Version.tryParseFileName("nb-1-big-SAI+aa+PKBlocks.db")).isEmpty();
        assertThat(Version.tryParseFileName("nb-1-big-SAI+aa+myIndex+KDTree.db")).isEmpty();
        // nor are generations
        assertThat(Version.tryParseFileName("nb-1-big-SAI+aa+1+myIndex+Meta.db")).isEmpty();

        for (Version version : Version.ALL)
        {
            for (IndexComponentType type : new IndexComponentType[]{ IndexComponentType.GROUP_COMPLETION_MARKER, IndexComponentType.META })
            {
                String name = "nb-1-big-" + version.fileNameFormatter().format(type, type == IndexComponentType.META ? "myIndex" : null, 0);
                assertFalse(name, Version.isApacheComponentFileName(name));
                assertThat(Version.tryParseFileName(name).get().buildId.version()).as(name).isEqualTo(version);
            }
        }
        assertFalse(Version.isApacheComponentFileName("nb-1-big-Data.db"));
    }

    @Test
    public void testApacheFileNamesIgnoredWhenNotReadingApacheFormat()
    {
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.SAI_READ_APACHE_FORMAT, false))
        {
            for (String name : new String[]{ "nb-1-big-SAI+aa+GroupComplete.db", "nb-1-big-SAI+aa+myIndex+Meta.db" })
            {
                assertThat(Version.tryParseFileName(name)).as(name).isEmpty();
                assertTrue(name, Version.isApacheComponentFileName(name));
            }
        }
    }

    @Test
    public void testApacheVersion()
    {
        assertEquals("aa", Version.APACHE_AA.versionString());
        assertEquals("aa(apache)", Version.APACHE_AA.toString());
        assertEquals("aa", Version.AA.toString());
        assertEquals("aa(apache)@0", ComponentsBuildId.of(Version.APACHE_AA, 0).toString());
        assertTrue(Version.APACHE_AA.isApache());
        assertFalse(Version.AA.isApache());
        assertThat(Version.APACHE_AA).isNotEqualTo(Version.AA);
        assertThat(Version.ALL).doesNotContain(Version.APACHE_AA);
        assertEquals(Version.AA, Version.parse("aa"));
        assertThat(ComponentsBuildId.of(Version.APACHE_AA, 0)).isNotEqualTo(ComponentsBuildId.of(Version.AA, 0));

        // ordered before all our versions, consistently
        for (Version version : Version.ALL)
        {
            assertTrue(version.after(Version.APACHE_AA));
            assertFalse(Version.APACHE_AA.onOrAfter(version));
            assertThat(Version.APACHE_AA.compareTo(version)).isNegative();
            assertThat(version.compareTo(Version.APACHE_AA)).isPositive();
            assertThat(ComponentsBuildId.of(version, 0).compareTo(ComponentsBuildId.of(Version.APACHE_AA, 0))).isPositive();
            assertThat(ComponentsBuildId.of(Version.APACHE_AA, 0).compareTo(ComponentsBuildId.of(version, 0))).isNegative();
        }
        assertEquals(0, Version.APACHE_AA.compareTo(Version.APACHE_AA));
        assertEquals(ByteComparable.Version.OSS50, Version.APACHE_AA.byteComparableVersionFor(IndexComponentType.PARTITION_KEY_BLOCKS, null));

        assertThatThrownBy(() -> Version.APACHE_AA.fileNameFormatter().format(IndexComponentType.META, "myIndex", 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.APACHE_AA.fileNameFormatter().format(IndexComponentType.KD_TREE, "myIndex", 0)).isInstanceOf(IllegalArgumentException.class);
    }

    private void assertApacheFileName(String name, IndexComponentType componentType, @Nullable String indexName)
    {
        assertTrue(name, Version.isApacheComponentFileName(name));
        Version.ParsedFileName parsed = Version.tryParseFileName(name).orElseThrow(() -> new AssertionError(name));
        assertEquals(ComponentsBuildId.of(Version.APACHE_AA, 0), parsed.buildId);
        assertEquals(componentType, parsed.component);
        assertEquals(indexName, parsed.indexName);

        // and back
        String componentPart = name.substring(name.lastIndexOf('-') + 1);
        assertEquals(componentPart, Version.APACHE_AA.fileNameFormatter().format(componentType, indexName, 0));
    }

    private void assertFileFormatting(String componentStr, Version version, int generation, IndexComponentType componentType, @Nullable String indexName)
    {
        Optional<Version.ParsedFileName> opt = Version.tryParseFileName(componentStr);
        assertThat(opt).isPresent();

        Version.ParsedFileName parsed = opt.get();
        assertEquals(version, parsed.buildId.version());
        assertEquals(generation, parsed.buildId.generation());
        assertEquals(componentType, parsed.component);
        assertEquals(indexName, parsed.indexName);

        assertThat(componentStr).endsWith(version.fileNameFormatter().format(componentType, indexName, generation));
    }
}
