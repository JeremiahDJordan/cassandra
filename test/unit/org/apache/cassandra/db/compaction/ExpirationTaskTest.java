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

package org.apache.cassandra.db.compaction;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.UUIDGen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Notifying indexes about the rows of fully expired SSTables is best-effort: whatever happens, the expiration task must
 * still drop the SSTables, or every later attempt would fail the same way and leave them stuck.
 */
public class ExpirationTaskTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void dropsSSTablesWhenRealmHasNoIndexManager() throws Exception
    {
        CompactionRealm realm = mock(CompactionRealm.class);
        when(realm.getIndexManager()).thenReturn(null);
        // A realm without an index manager just has no indexes to notify, so there is nothing to warn about
        assertThat(testSSTablesAreDropped(realm)).isEmpty();
    }

    @Test
    public void dropsSSTablesWhenListingIndexesFails() throws Exception
    {
        CompactionRealm realm = mock(CompactionRealm.class);
        when(realm.getIndexManager()).thenThrow(new RuntimeException("Injected failure"));
        assertThat(testSSTablesAreDropped(realm)).hasSize(1);
    }

    /**
     * @return the warnings logged while notifying the indexes
     */
    private static List<ILoggingEvent> testSSTablesAreDropped(CompactionRealm realm) throws Exception
    {
        ILifecycleTransaction transaction = mock(ILifecycleTransaction.class);
        when(transaction.isOffline()).thenReturn(true);
        when(transaction.opId()).thenReturn(UUIDGen.getTimeUUID());
        when(transaction.originals()).thenReturn(Collections.singleton(mock(SSTableReader.class)));

        Logger logger = (Logger) LoggerFactory.getLogger(FullyExpiredSSTablesIndexNotifier.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            new ExpirationTask(realm, transaction).runMayThrow();
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
        }

        var order = inOrder(transaction);
        order.verify(transaction).obsoleteOriginals();
        order.verify(transaction).prepareToCommit();
        order.verify(transaction).commit();

        return appender.list.stream().filter(e -> e.getLevel() == Level.WARN).collect(Collectors.toList());
    }
}
