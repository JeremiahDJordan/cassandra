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

package org.apache.cassandra.io.sstable;

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;

import static org.assertj.core.api.Assertions.assertThat;

public class ApacheReadabilityTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testUnreadable()
    {
        assertThat(ApacheReadability.unreadable(BtiFormat.getInstance().getVersion("da"), ImmutableSet.of(Components.DATA, Components.DIGEST), null)).isEmpty();
        assertThat(ApacheReadability.unreadable(BigFormat.getInstance().getVersion("nb"), ImmutableSet.of(Components.DATA, Components.DIGEST), null)).isEmpty();
        assertThat(ApacheReadability.unreadable(BtiFormat.getInstance().getVersion("cc"), ImmutableSet.of(Components.DATA, Components.DIGEST), null))
        .containsExactly("bti version cc, while Apache Cassandra 5.0 reads only da");
        assertThat(ApacheReadability.unreadable(BigFormat.getInstance().getVersion("oa"), ImmutableSet.of(Components.DATA, Components.DIGEST_CRC32C), null))
        .containsExactly("checksum type CRC32C");
        assertThat(ApacheReadability.unreadable(BigFormat.getInstance().getVersion("oa"), ImmutableSet.of(Components.DATA), null))
        .containsExactly("no digest that names its checksum type");
    }
}
