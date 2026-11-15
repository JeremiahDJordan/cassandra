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
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * sstabledump of the legacy sstables of {@code legacy_tables.legacy_<version>_tuple}, whose frozen tuple column
 * {@code val} is recorded in the header without a frozen wrapper by some versions.
 */
@RunWith(Parameterized.class)
public class SSTableExportLegacyTupleTest extends OfflineToolUtils
{
    @Parameterized.Parameter
    public String version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> versions()
    {
        List<Object[]> versions = new ArrayList<>();
        for (String version : new String[]{ "ma", "mb", "mc", "md", "me", "na", "nb", "oa" })
        {
            if (new File("test/data/legacy-sstables/" + version + "/legacy_tables/legacy_" + version + "_tuple").isDirectory())
                versions.add(new Object[]{ version });
        }
        return versions;
    }

    @Test
    public void testDumpTupleColumns() throws IOException
    {
        String table = "legacy_" + version + "_tuple";
        File target = new File("build/test/cassandra/data/legacy_tuple_" + version + '/' + table);
        if (target.exists())
            FileUtils.deleteRecursive(target);
        target.tryCreateDirectories();
        org.apache.commons.io.FileUtils.copyDirectory(new File("test/data/legacy-sstables/" + version + "/legacy_tables/" + table).toJavaIOFile(),
                                                      target.toJavaIOFile());
        File[] data = target.tryList(file -> file.isFile() && file.name().endsWith("-Data.db"));
        assertThat(data).isNotEmpty();
        for (File sstable : data)
        {
            ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable.absolutePath());
            assertThat(tool.getCleanedStderr()).as(sstable.name()).doesNotContain("Exception");
            assertThat(tool.getExitCode()).as(sstable.name()).isZero();
            String json = tool.getStdout();
            assertThat(json.split("\"partition\" :", -1)).as(sstable.name()).hasSize(6);
            // every partition has the same frozen tuple value
            assertThat(json.split("\\{ \"name\" : \"val\", \"value\" : \\[\\[1, 2, 3\\], \\[\"a\", \"b\", \"c\"\\]\\] \\}", -1)).as(sstable.name()).hasSize(6);
        }
    }
}
