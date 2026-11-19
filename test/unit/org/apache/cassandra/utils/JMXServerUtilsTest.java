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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_RMI_PORT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class JMXServerUtilsTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testNotificationBufferLifecycle() throws Exception
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("org.apache.cassandra.test:type=CountingNotifier");
        CountingNotifier notifier = new CountingNotifier();
        mbs.registerMBean(notifier, name);
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try
        {
            // the buffer listens to broadcasters from the start until the server stops
            JMXConnectorServer server = JMXServerUtils.createJMXServer(CQLTester.getAutomaticallyAllocatedPort(loopback), "localhost", true);
            assertEquals(1, notifier.listeners.get());
            server.stop();
            assertEquals(0, notifier.listeners.get());

            // and is released when the server fails to start
            try (ServerSocket taken = new ServerSocket(0, 1, loopback);
                 WithProperties properties = new WithProperties().set(COM_SUN_MANAGEMENT_JMXREMOTE_RMI_PORT, taken.getLocalPort()))
            {
                int port = CQLTester.getAutomaticallyAllocatedPort(loopback);
                assertThatThrownBy(() -> JMXServerUtils.createJMXServer(port, "localhost", true)).isInstanceOf(IOException.class);
            }
            assertEquals(0, notifier.listeners.get());
        }
        finally
        {
            mbs.unregisterMBean(name);
        }
    }

    public interface CountingNotifierMBean
    {
    }

    public static class CountingNotifier extends NotificationBroadcasterSupport implements CountingNotifierMBean
    {
        final AtomicInteger listeners = new AtomicInteger();

        @Override
        public void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
        {
            super.addNotificationListener(listener, filter, handback);
            listeners.incrementAndGet();
        }

        @Override
        public void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException
        {
            super.removeNotificationListener(listener);
            listeners.decrementAndGet();
        }

        @Override
        public void removeNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) throws ListenerNotFoundException
        {
            super.removeNotificationListener(listener, filter, handback);
            listeners.decrementAndGet();
        }
    }
}
