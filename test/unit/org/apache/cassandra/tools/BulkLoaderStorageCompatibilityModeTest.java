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

package org.apache.cassandra.tools;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BulkLoaderStorageCompatibilityModeTest extends OfflineToolUtils
{
    /**
     * sstableloader writes for the storage_compatibility_mode of its --conf-path file, NONE as Apache Cassandra's
     * clients, and doesn't take stream_entire_sstables from a file in one of its modes.
     */
    @Test(expected = NoHostAvailableException.class)
    public void testModeOfConfiguration() throws Throwable
    {
        File config = new File(Paths.get(".", "test", "conf", "cassandra.yaml").normalize());
        Path upgrading = Files.createTempFile("cassandra", ".yaml");
        upgrading.toFile().deleteOnExit();
        Files.write(upgrading, (new String(Files.readAllBytes(config.toPath()), StandardCharsets.UTF_8) + "\nstorage_compatibility_mode: UPGRADING\nstream_entire_sstables: false\n")
                               .replace("stream_entire_sstables: true\n", "")
                               .getBytes(StandardCharsets.UTF_8));
        ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                 "-d",
                                                 "127.9.9.1:9041",
                                                 "-f",
                                                 upgrading.toString(),
                                                 OfflineToolUtils.sstableDirName("legacy_sstables", "legacy_ma_simple"));
        assertEquals(-1, tool.getExitCode());
        assertEquals(StorageCompatibilityMode.NONE, DatabaseDescriptor.getStorageCompatibilityModeForWrites());
        assertEquals("oa", BigFormat.getInstance().getLatestVersion().version);
        // as in Apache Cassandra, the file doesn't set stream_entire_sstables
        assertTrue(DatabaseDescriptor.streamEntireSSTables());
        if (!CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.isPresent())
            assertEquals("da", BtiFormat.getInstance().getLatestVersion().version);
        throw tool.getException().getCause().getCause().getCause();
    }
}
