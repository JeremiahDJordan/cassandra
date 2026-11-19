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

package org.apache.cassandra.auth.jmx;

import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.rmi.server.ExportException;
import java.rmi.server.RMISocketFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import com.google.common.collect.ImmutableSet;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraPrincipal;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.StubAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.JMXServerUtils;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_AUTHORIZER;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_REMOTE_LOGIN_CONFIG;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_SECURITY_AUTH_LOGIN_CONFIG;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * JMX authorization uses the Subject of the connection making each call.
 */
public class JMXAuthConcurrentSubjectsTest extends CQLTester
{
    private static final int CALLS = 50;
    private static final String ALLOWED = "allowed_role";
    private static final String DENIED = "denied_role";

    private static JMXConnectorServer jmxServer;
    private static int port;
    private static IAuthorizer originalAuthorizer;
    private static int originalPermissionsValidity;
    private static WithProperties properties;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        originalAuthorizer = DatabaseDescriptor.getAuthorizer();
        originalPermissionsValidity = DatabaseDescriptor.getPermissionsValidity();

        DatabaseDescriptor.setAuthorizer(new StubAuthorizer());
        DatabaseDescriptor.setPermissionsValidity(0);

        String config = Paths.get(ClassLoader.getSystemResource("auth/cassandra-test-jaas.conf").toURI()).toString();
        properties = new WithProperties().set(COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE, true)
                                         .set(JAVA_SECURITY_AUTH_LOGIN_CONFIG, config)
                                         .set(CASSANDRA_JMX_REMOTE_LOGIN_CONFIG, "MultiRoleLogin")
                                         .set(CASSANDRA_JMX_AUTHORIZER, JMXAuthTest.NoSuperUserAuthorizationProxy.class.getName());
        // another process can take the allocated port before the registry binds it
        for (int attempt = 1; jmxServer == null; attempt++)
        {
            port = getAutomaticallyAllocatedPort(InetAddress.getLoopbackAddress());
            try
            {
                jmxServer = JMXServerUtils.createJMXServer(port, "localhost", true);
            }
            catch (ExportException e)
            {
                if (attempt == 3 || !(e.getCause() instanceof BindException))
                    throw e;
            }
        }
    }

    @AfterClass
    public static void tearDownJmx() throws Exception
    {
        try
        {
            if (jmxServer != null)
                jmxServer.stop();
        }
        finally
        {
            properties.close();
            DatabaseDescriptor.setPermissionsValidity(originalPermissionsValidity);
            DatabaseDescriptor.setAuthorizer(originalAuthorizer);
        }
    }

    @Test
    public void testConcurrentConnectionsUseTheirOwnSubject() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        JMXResource tableMBean = JMXResource.mbean(String.format("org.apache.cassandra.db:type=Tables,keyspace=%s,table=%s", KEYSPACE, table));
        DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER, ImmutableSet.of(Permission.SELECT),
                                                 tableMBean, RoleResource.role(ALLOWED));

        ObjectName name = ObjectName.getInstance(tableMBean.getObjectName());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (JMXConnector allowed = connect(ALLOWED); JMXConnector denied = connect(DENIED))
        {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> calls = new ArrayList<>();
            for (Pair<String, JMXConnector> connection : Arrays.asList(Pair.create(ALLOWED, allowed), Pair.create(DENIED, denied)))
            {
                String role = connection.left;
                ColumnFamilyStoreMBean proxy = JMX.newMBeanProxy(connection.right.getMBeanServerConnection(), name,
                                                                 ColumnFamilyStoreMBean.class);
                calls.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < CALLS; i++)
                    {
                        try
                        {
                            assertEquals(table, proxy.getTableName());
                            assertEquals(ALLOWED, role);
                        }
                        catch (SecurityException e)
                        {
                            assertEquals(DENIED, role);
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> call : calls)
                call.get(1, TimeUnit.MINUTES);
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    @Test
    public void testFirstNotificationSubscriberIsNotSuperuser() throws Throwable
    {
        ObjectName name = new ObjectName("org.apache.cassandra.test:type=Notifier");
        Notifier notifier = new Notifier();
        ManagementFactory.getPlatformMBeanServer().registerMBean(notifier, name);
        try
        {
            DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER, ImmutableSet.of(Permission.DESCRIBE),
                                                     JMXResource.mbean(name.getCanonicalName()), RoleResource.role(ALLOWED));
            BlockingQueue<Notification> received = new LinkedBlockingQueue<>();
            NotificationListener listener = (notification, handback) -> received.add(notification);
            try (JMXConnector connector = connect(ALLOWED))
            {
                MBeanServerConnection connection = connector.getMBeanServerConnection();
                connection.addNotificationListener(name, listener, null, null);
                notifier.sendNotification(new Notification("test", name, 1));
                Notification notification = received.poll(1, TimeUnit.MINUTES);
                assertNotNull(notification);
                assertEquals("test", notification.getType());
                connection.removeNotificationListener(name, listener);
            }
        }
        finally
        {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
        }
    }

    @Test
    public void testNotificationSubscriptionRequiresDescribeFromJava24() throws Throwable
    {
        Assume.assumeTrue(Runtime.version().feature() >= 24);
        ObjectName name = new ObjectName("org.apache.cassandra.test:type=UndescribedNotifier");
        ManagementFactory.getPlatformMBeanServer().registerMBean(new Notifier(), name);
        try (JMXConnector connector = connect(DENIED))
        {
            MBeanServerConnection connection = connector.getMBeanServerConnection();
            assertThatThrownBy(() -> connection.addNotificationListener(name, (notification, handback) -> {}, null, null))
            .isInstanceOf(SecurityException.class)
            .hasMessage("Access Denied");
        }
        finally
        {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
        }
    }

    public interface NotifierMBean
    {
    }

    public static class Notifier extends NotificationBroadcasterSupport implements NotifierMBean
    {
    }

    private static JMXConnector connect(String role) throws Exception
    {
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:" + port + "/jmxrmi");
        Map<String, Object> env = new HashMap<>();
        env.put("com.sun.jndi.rmi.factory.socket", RMISocketFactory.getDefaultSocketFactory());
        env.put(JMXConnector.CREDENTIALS, new String[]{ role, "unused" });
        return JMXConnectorFactory.connect(url, env);
    }

    /** Authenticates every login as the role named by the username. */
    public static class UserNameLoginModule implements LoginModule
    {
        private Subject subject;
        private CallbackHandler callbackHandler;
        private CassandraPrincipal principal;

        public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options)
        {
            this.subject = subject;
            this.callbackHandler = callbackHandler;
        }

        public boolean login() throws LoginException
        {
            NameCallback name = new NameCallback("username");
            PasswordCallback password = new PasswordCallback("password", false);
            try
            {
                callbackHandler.handle(new Callback[]{ name, password });
            }
            catch (Exception e)
            {
                throw new LoginException(e.toString());
            }
            principal = new CassandraPrincipal(name.getName());
            return true;
        }

        public boolean commit()
        {
            subject.getPrincipals().add(principal);
            return true;
        }

        public boolean abort()
        {
            return true;
        }

        public boolean logout()
        {
            subject.getPrincipals().remove(principal);
            return true;
        }
    }
}
