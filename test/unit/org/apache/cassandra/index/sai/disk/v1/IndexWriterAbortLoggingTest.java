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

package org.apache.cassandra.index.sai.disk.v1;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * An abort without a cause is a normal abort of the SSTable write (for example, an interrupted compaction), so the
 * index writers must not log it as a warning (CASSANDRA-20695).
 */
public class IndexWriterAbortLoggingTest
{
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void attachAppender()
    {
        appender.start();
        for (Class<?> writerClass : new Class<?>[]{ SSTableIndexWriter.class, MemtableIndexWriter.class })
        {
            Logger logger = (Logger) LoggerFactory.getLogger(writerClass);
            logger.setLevel(Level.DEBUG);
            logger.addAppender(appender);
        }
    }

    @After
    public void detachAppender()
    {
        for (Class<?> writerClass : new Class<?>[]{ SSTableIndexWriter.class, MemtableIndexWriter.class })
        {
            Logger logger = (Logger) LoggerFactory.getLogger(writerClass);
            logger.detachAppender(appender);
            logger.setLevel(null);
        }
        appender.stop();
    }

    @Test
    public void sstableIndexWriterAbortWithoutCause()
    {
        sstableIndexWriter().abort(null);
        assertAbortLoggedAt(Level.DEBUG, "Aborting SSTable index flush");
    }

    @Test
    public void sstableIndexWriterAbortWithCause()
    {
        sstableIndexWriter().abort(new RuntimeException("Injected failure"));
        assertAbortLoggedAt(Level.WARN, "Aborting SSTable index flush");
    }

    @Test
    public void memtableIndexWriterAbortWithoutCause()
    {
        memtableIndexWriter().abort(null);
        assertAbortLoggedAt(Level.DEBUG, "Aborting index memtable flush");
    }

    @Test
    public void memtableIndexWriterAbortWithCause()
    {
        memtableIndexWriter().abort(new RuntimeException("Injected failure"));
        assertAbortLoggedAt(Level.WARN, "Aborting index memtable flush");
    }

    private void assertAbortLoggedAt(Level level, String message)
    {
        List<ILoggingEvent> aborts = appender.list.stream()
                                                  .filter(e -> e.getMessage() != null && e.getMessage().contains(message))
                                                  .collect(Collectors.toList());
        assertThat(aborts).hasSize(1);
        assertThat(aborts.get(0).getLevel()).isEqualTo(level);
        // the cause, if any, is only logged with the warning
        assertThat(aborts.get(0).getThrowableProxy() != null).isEqualTo(level == Level.WARN);
    }

    private static PerIndexWriter sstableIndexWriter()
    {
        IndexContext context = mock(IndexContext.class);
        when(context.getIndexMetrics()).thenReturn(Optional.empty());
        return new SSTableIndexWriter(components(context), new NamedMemoryLimiter(1 << 20, "test"), () -> false, () -> false, 0);
    }

    private static PerIndexWriter memtableIndexWriter()
    {
        return new MemtableIndexWriter(mock(MemtableIndex.class),
                                       components(mock(IndexContext.class)),
                                       mock(PrimaryKey.Factory.class),
                                       mock(RowMapping.class),
                                       () -> false);
    }

    private static IndexComponents.ForWrite components(IndexContext context)
    {
        IndexComponents.ForWrite components = mock(IndexComponents.ForWrite.class);
        when(components.context()).thenReturn(context);
        when(components.logMessage(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
        return components;
    }
}
