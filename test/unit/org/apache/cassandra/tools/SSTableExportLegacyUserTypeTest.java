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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.commons.io.FileUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * sstabledump and sstablemetadata of the legacy 3.x sstables whose headers do not mark their frozen user type columns
 * as frozen (see CASSANDRA-15035).
 */
public class SSTableExportLegacyUserTypeTest extends OfflineToolUtils
{
    private static final String VALUE = "{\"name\":[\"abc\",\"def\"]}";

    @Test
    public void testFrozenUserTypesNotMarkedFrozenInHeader() throws IOException
    {
        assertFrozenUserTypeColumns(dump("mb", "legacy_mb_tuple", "mb-307-big-Data.db"), "val3");
        assertFrozenUserTypeColumns(dump("me", "legacy_me_tuple", "me-31111-big-Data.db"), "val3", "val4");
        assertFrozenUserTypeColumns(dump("me", "legacy_me_tuple_compact", "me-31111-big-Data.db"), "val3", "val4");
    }

    @Test
    public void testMetadataViewerOnFrozenUserTypesNotMarkedFrozenInHeader() throws IOException
    {
        for (String[] sstable : new String[][]{ { "mb", "legacy_mb_tuple", "mb-307-big-Data.db" },
                                                { "me", "legacy_me_tuple", "me-31111-big-Data.db" },
                                                { "me", "legacy_me_tuple_compact", "me-31111-big-Data.db" } })
        {
            String path = copy(sstable[0], sstable[1], sstable[2]);

            ToolResult metadata = ToolRunner.invokeClass(SSTableMetadataViewer.class, "-s", path);
            metadata.assertOnCleanExit();
            assertTrue(metadata.getStdout(), metadata.getStdout().contains("Rows: 5"));
        }
    }

    private static String copy(String version, String table, String dataFile) throws IOException
    {
        File dir = new File("build/test/cassandra/data/legacy_" + version + '/' + table);
        FileUtils.copyDirectory(new File("test/data/legacy-sstables/" + version + "/legacy_tables/" + table).toJavaIOFile(),
                                dir.toJavaIOFile());
        return new File(dir, dataFile).absolutePath();
    }

    private static List<JsonNode> dump(String version, String table, String dataFile) throws IOException
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, copy(version, table, dataFile));
        tool.assertOnCleanExit();

        List<JsonNode> rows = new ArrayList<>();
        JsonNode partitions = JsonUtils.JSON_OBJECT_MAPPER.readTree(tool.getStdout());
        for (JsonNode partition : partitions)
        {
            for (JsonNode row : partition.get("rows"))
                rows.add(row);
        }
        assertEquals(5, rows.size());
        return rows;
    }

    private static void assertFrozenUserTypeColumns(List<JsonNode> rows, String... columns)
    {
        for (JsonNode row : rows)
        {
            for (String column : columns)
            {
                JsonNode cell = cell(row, column);
                assertTrue(cell.toString(), !cell.has("path"));
                assertEquals(VALUE, cell.get("value").toString());
            }
        }
    }

    private static JsonNode cell(JsonNode row, String column)
    {
        for (JsonNode cell : row.get("cells"))
        {
            if (column.equals(cell.get("name").asText()))
                return cell;
        }
        throw new AssertionError("No cell for " + column + " in " + row);
    }
}
