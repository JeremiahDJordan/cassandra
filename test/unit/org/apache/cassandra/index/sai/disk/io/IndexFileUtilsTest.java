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

package org.apache.cassandra.index.sai.disk.io;

import java.nio.file.Files;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexFileUtilsTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void openBlockingInputClosesHandleWhenReaderCreationFails()
    {
        FileHandle fileHandle = mock(FileHandle.class);
        RuntimeException failure = new RuntimeException("Injected failure");
        when(fileHandle.createReader()).thenThrow(failure);

        assertThatThrownBy(() -> IndexFileUtils.instance().openBlockingInput(fileHandle)).isSameAs(failure);

        verify(fileHandle).close();
    }

    @Test
    public void openBlockingInputClosesHandleWithInput() throws Exception
    {
        File file = new File(Files.createTempFile("IndexFileUtilsTest", ".db"));
        try
        {
            Files.write(file.toPath(), new byte[]{ 1, 2, 3, 4 });
            FileHandle fileHandle = new FileHandle.Builder(file).complete();

            IndexInputReader input = IndexFileUtils.instance().openBlockingInput(fileHandle);
            assertFalse(fileHandle.isCleanedUp());
            assertEquals(1, input.readByte());

            input.close();
            assertTrue(fileHandle.isCleanedUp());
        }
        finally
        {
            file.tryDelete();
        }
    }
}
