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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.Test;

import net.openhft.chronicle.core.util.ThrowingFunction;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SimpleCachedBufferPool;
import org.apache.cassandra.utils.Generators;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.creation.MockSettingsImpl;
import sun.nio.ch.DirectBuffer;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.quicktheories.QuickTheory.qt;

public class DirectIOSegmentTest
{
    @BeforeClass
    public static void beforeClass() throws IOException
    {
        File commitLogDir = new File(Files.createTempDirectory("commitLogDir"));
        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            config.commitlog_directory = commitLogDir.toString();
            return config;
        });
    }

    @Test
    public void testFlushBuffer()
    {
        int fsBlockSize = 32;
        int bufSize = 4 * fsBlockSize;

        SimpleCachedBufferPool bufferPool = mock(SimpleCachedBufferPool.class);
        AbstractCommitLogSegmentManager manager = mock(AbstractCommitLogSegmentManager.class,
                                                       new MockSettingsImpl<>().useConstructor(CommitLog.instance, DatabaseDescriptor.getCommitLogLocation()));
        doReturn(bufferPool).when(manager).getBufferPool();
        doCallRealMethod().when(manager).getConfiguration();
        when(bufferPool.createBuffer()).thenReturn(ByteBuffer.allocate(bufSize + fsBlockSize));
        doNothing().when(manager).addSize(anyLong());
        long idBase = currentTimeMillis();
        AtomicInteger nextId = new AtomicInteger(1);
        doReturn(idBase + nextId.getAndIncrement()).when(manager).getNextId();

        qt().forAll(Generators.forwardRanges(0, bufSize))
            .checkAssert(startEnd -> {
                int start = startEnd.lowerEndpoint();
                int end = startEnd.upperEndpoint();
                FileChannel channel = mock(FileChannel.class);
                ThrowingFunction<Path, FileChannel, IOException> channelFactory = path -> channel;
                DirectIOSegment seg = new DirectIOSegment(manager, channelFactory, fsBlockSize);
                List<ByteBuffer[]> writes = recordWrites(channel, () -> {});
                seg.lastSyncedOffset = start;
                seg.flush(start, end);
                assertThat(writes).hasSize(1);

                // the first buffer is a view of the segment buffer, so its position is the file position
                ByteBuffer[] written = writes.get(0);
                int position = written[0].position();
                int length = 0;
                for (ByteBuffer buf : written)
                {
                    // assert that each buffer is aligned to the fs block size
                    assertThat(buf.remaining() % fsBlockSize).isZero();
                    if (buf.isDirect())
                        assertThat(buf.alignmentOffset(buf.position(), fsBlockSize)).isZero();
                    length += buf.remaining();
                }

                // assert that the entire buffer is written
                assertThat(position).isLessThanOrEqualTo(start);
                assertThat(position + length).isGreaterThanOrEqualTo(end);

                // assert that the buffer is aligned to the fs block size
                assertThat(position % fsBlockSize).isZero();

                // assert that the buffer is unnecessarily large
                assertThat(position).isGreaterThan(start - fsBlockSize);
                assertThat(position + length).isLessThan(end + fsBlockSize);

                assertThat(seg.lastWritten).isEqualTo(position + length);
            });
    }

    @Test
    public void testFlushedTailUnchangedByConcurrentAppend()
    {
        int fsBlockSize = 32;
        int bufSize = 4 * fsBlockSize;

        SimpleCachedBufferPool bufferPool = mock(SimpleCachedBufferPool.class);
        AbstractCommitLogSegmentManager manager = mock(AbstractCommitLogSegmentManager.class,
                                                       new MockSettingsImpl<>().useConstructor(CommitLog.instance, DatabaseDescriptor.getCommitLogLocation()));
        doReturn(bufferPool).when(manager).getBufferPool();
        doCallRealMethod().when(manager).getConfiguration();
        when(bufferPool.createBuffer()).thenReturn(ByteBuffer.allocate(bufSize + fsBlockSize));
        doNothing().when(manager).addSize(anyLong());
        long idBase = currentTimeMillis();
        AtomicInteger nextId = new AtomicInteger(1);
        doReturn(idBase + nextId.getAndIncrement()).when(manager).getNextId();

        FileChannel channel = mock(FileChannel.class);
        DirectIOSegment seg = new DirectIOSegment(manager, path -> channel, fsBlockSize);
        int start = fsBlockSize;
        int end = 2 * fsBlockSize + fsBlockSize / 2;
        int blockEnd = 3 * fsBlockSize;
        for (int i = start; i < blockEnd; i++)
            seg.buffer.put(i, (byte) i);

        // an append after the next sync marker fills the rest of the last block while the write is in progress
        int appendStart = end + CommitLogSegment.SYNC_MARKER_SIZE;
        List<ByteBuffer[]> writes = recordWrites(channel, () -> {
            for (int i = appendStart; i < blockEnd; i++)
                seg.buffer.put(i, (byte) -1);
        });
        seg.lastSyncedOffset = start;
        seg.flush(start, end);
        assertThat(writes).hasSize(1);

        ByteBuffer bytes = ByteBuffer.allocate(blockEnd - start);
        for (ByteBuffer buf : writes.get(0))
            bytes.put(buf.duplicate());
        assertThat(bytes.hasRemaining()).isFalse();
        for (int i = start; i < blockEnd; i++)
            assertThat(bytes.get(i - start)).isEqualTo((byte) i);
    }

    @Test
    public void testFlushSize()
    {
        int fsBlockSize = 32;
        int bufSize = 4 * fsBlockSize;

        SimpleCachedBufferPool bufferPool = mock(SimpleCachedBufferPool.class);
        AbstractCommitLogSegmentManager manager = mock(AbstractCommitLogSegmentManager.class,
                                                       new MockSettingsImpl<>().useConstructor(CommitLog.instance, DatabaseDescriptor.getCommitLogLocation()));
        doReturn(bufferPool).when(manager).getBufferPool();
        doCallRealMethod().when(manager).getConfiguration();
        when(bufferPool.createBuffer()).thenReturn(ByteBuffer.allocate(bufSize + fsBlockSize));
        doNothing().when(manager).addSize(anyLong());
        long idBase = currentTimeMillis();
        AtomicInteger nextId = new AtomicInteger(1);
        doReturn(idBase + nextId.getAndIncrement()).when(manager).getNextId();

        FileChannel channel = mock(FileChannel.class);
        ThrowingFunction<Path, FileChannel, IOException> channelFactory = path -> channel;
        ArgumentCaptor<ByteBuffer> bufCap = ArgumentCaptor.forClass(ByteBuffer.class);
        DirectIOSegment seg = new DirectIOSegment(manager, channelFactory, fsBlockSize);

        AtomicLong size = new AtomicLong();
        doAnswer(i -> size.addAndGet(i.getArgument(0, Long.class))).when(manager).addSize(anyLong());

        for (int start = 0; start < bufSize - 1; start++)
        {
            int end = start + 1;
            seg.lastSyncedOffset = start;
            seg.flush(start, end);
            assertThat(size.get()).isGreaterThanOrEqualTo(end);
        }

        assertThat(size.get()).isEqualTo(bufSize);
    }

    @Test
    public void testBuilder()
    {
        AbstractCommitLogSegmentManager manager = mock(AbstractCommitLogSegmentManager.class,
                                                       new MockSettingsImpl<>().useConstructor(CommitLog.instance, DatabaseDescriptor.getCommitLogLocation()));
        DirectIOSegment.DirectIOSegmentBuilder builder = new DirectIOSegment.DirectIOSegmentBuilder(manager, 4096);
        assertThat(builder.fsBlockSize).isGreaterThan(0);

        int segmentSize = Math.max(5 << 20, builder.fsBlockSize * 5);
        DatabaseDescriptor.setCommitLogSegmentSize(segmentSize >> 20);

        SimpleCachedBufferPool pool = builder.createBufferPool();
        ByteBuffer buf = pool.createBuffer();
        try
        {
            assertThat(buf.remaining()).isEqualTo(segmentSize);
            assertThat(buf.alignmentOffset(buf.position(), builder.fsBlockSize)).isEqualTo(0);
            assertThat(buf).isInstanceOf(DirectBuffer.class);
            assertThat(((DirectBuffer) buf).attachment()).isNotNull();
        }
        finally
        {
            pool.releaseBuffer(buf);
        }
    }

    /**
     * Records views of the buffers passed to each call of the channel's write methods, running {@code duringWrite}
     * before each write returns.
     */
    private static List<ByteBuffer[]> recordWrites(FileChannel channel, Runnable duringWrite)
    {
        List<ByteBuffer[]> writes = new ArrayList<>();
        try
        {
            when(channel.write(any(ByteBuffer.class))).thenAnswer(invocation -> {
                duringWrite.run();
                ByteBuffer src = invocation.getArgument(0);
                int length = src.remaining();
                writes.add(new ByteBuffer[]{ record(src) });
                return length;
            });
            when(channel.write(any(ByteBuffer[].class))).thenAnswer(invocation -> {
                duringWrite.run();
                ByteBuffer[] srcs = invocation.getArgument(0);
                ByteBuffer[] written = new ByteBuffer[srcs.length];
                long length = 0;
                for (int i = 0; i < srcs.length; i++)
                {
                    length += srcs[i].remaining();
                    written[i] = record(srcs[i]);
                }
                writes.add(written);
                return length;
            });
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return writes;
    }

    private static ByteBuffer record(ByteBuffer src)
    {
        ByteBuffer written = src.duplicate();
        src.position(src.limit());
        return written;
    }
}
