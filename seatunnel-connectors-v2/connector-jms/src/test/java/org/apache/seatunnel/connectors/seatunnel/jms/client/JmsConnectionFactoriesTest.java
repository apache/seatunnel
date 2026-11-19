/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jms.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorException;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;

import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JmsConnectionFactoriesTest {

    private static final String SECRET = "s3cr3t-value";

    @Test
    void activemqClassicPropertiesIncludingADottedKeyAreApplied() {
        Map<String, String> properties = new HashMap<>();
        properties.put("brokerURL", "tcp://127.0.0.1:61616");
        properties.put("prefetchPolicy.queuePrefetch", "7");
        properties.put("useCompression", "TRUE");
        properties.put("closeTimeout", " 1234 ");

        org.apache.activemq.ActiveMQConnectionFactory factory =
                (org.apache.activemq.ActiveMQConnectionFactory)
                        JmsConnectionFactories.fromClass(
                                "org.apache.activemq.ActiveMQConnectionFactory", properties);

        assertEquals("tcp://127.0.0.1:61616", factory.getBrokerURL());
        assertEquals(7, factory.getPrefetchPolicy().getQueuePrefetch());
        assertTrue(factory.isUseCompression());
        assertEquals(1234, factory.getCloseTimeout());
    }

    @Test
    void artemisSettersThatReturnTheFactoryAreApplied() {
        Map<String, String> properties = new HashMap<>();
        properties.put("brokerURL", "tcp://127.0.0.1:61616");
        properties.put("user", "reader");
        properties.put("consumerWindowSize", "0");

        org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory factory =
                (org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory)
                        JmsConnectionFactories.fromClass(
                                "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory",
                                properties);

        assertEquals("reader", factory.getUser());
        assertEquals(0, factory.getConsumerWindowSize());
    }

    @Test
    void qpidPropertiesIncludingADottedKeyAreApplied() {
        Map<String, String> properties = new HashMap<>();
        properties.put("remoteURI", "amqp://127.0.0.1:5672");
        properties.put("prefetchPolicy.queuePrefetch", "3");

        JmsConnectionFactory factory =
                (JmsConnectionFactory)
                        JmsConnectionFactories.fromClass(
                                "org.apache.qpid.jms.JmsConnectionFactory", properties);

        assertEquals("amqp://127.0.0.1:5672", factory.getRemoteURI());
        assertEquals(
                3, ((JmsDefaultPrefetchPolicy) factory.getPrefetchPolicy()).getQueuePrefetch());
    }

    @Test
    void unknownPropertyNamesTheKeyButNotTheValue() {
        JmsConnectorException error =
                assertFactoryError(
                        "org.apache.activemq.ActiveMQConnectionFactory",
                        Collections.singletonMap("noSuchOption", SECRET));
        assertTrue(error.getMessage().contains("noSuchOption"), error.getMessage());
    }

    @Test
    void unknownLeadingPartOfADottedKeyIsRejected() {
        JmsConnectorException error =
                assertFactoryError(
                        "org.apache.activemq.ActiveMQConnectionFactory",
                        Collections.singletonMap("noSuchPolicy.queuePrefetch", SECRET));
        assertTrue(error.getMessage().contains("noSuchPolicy.queuePrefetch"), error.getMessage());
    }

    @Test
    void emptyPartOfAKeyIsRejected() {
        assertFactoryError(
                "org.apache.activemq.ActiveMQConnectionFactory",
                Collections.singletonMap("prefetchPolicy.", SECRET));
    }

    @Test
    void valueThatDoesNotMatchTheSetterTypeIsRejected() {
        JmsConnectorException error =
                assertFactoryError(
                        "org.apache.activemq.ActiveMQConnectionFactory",
                        Collections.singletonMap("closeTimeout", SECRET));
        assertTrue(error.getMessage().contains("closeTimeout"), error.getMessage());
    }

    @Test
    void setterFailureDoesNotExposeTheValue() {
        JmsConnectorException error =
                assertFactoryError(
                        ThrowingSetterFactory.class.getName(),
                        Collections.singletonMap("url", SECRET));
        assertTrue(error.getMessage().contains("IllegalArgumentException"), error.getMessage());
    }

    @Test
    void constructorFailureDoesNotExposeItsMessage() {
        assertFactoryError(ThrowingConstructorFactory.class.getName(), Collections.emptyMap());
    }

    @Test
    void classWithoutANoArgumentConstructorIsRejected() {
        JmsConnectorException error =
                assertFactoryError(
                        NoDefaultConstructorFactory.class.getName(), Collections.emptyMap());
        assertTrue(error.getMessage().contains("no-argument constructor"), error.getMessage());
    }

    @Test
    void classThatIsNotAConnectionFactoryIsRejected() {
        JmsConnectorException error =
                assertFactoryError("java.lang.String", Collections.emptyMap());
        assertTrue(error.getMessage().contains("javax.jms.ConnectionFactory"), error.getMessage());
    }

    @Test
    void missingClassPointsToThePluginDirectory() {
        JmsConnectorException error =
                assertFactoryError("com.example.MissingConnectionFactory", Collections.emptyMap());
        assertTrue(error.getMessage().contains("plugins/connector-jms"), error.getMessage());
    }

    @Test
    void jndiLookupOfAnActivemqClassicFactory() {
        Map<String, Object> options = new HashMap<>();
        options.put(
                "jndi_initial_context_factory",
                "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        options.put("jndi_provider_url", "tcp://127.0.0.1:61616");
        options.put("jndi_connection_factory_name", "ConnectionFactory");

        ConnectionFactory factory = JmsConnectionFactories.create(ReadonlyConfig.fromMap(options));

        assertEquals(
                "tcp://127.0.0.1:61616",
                ((org.apache.activemq.ActiveMQConnectionFactory) factory).getBrokerURL());
    }

    @Test
    void jndiNameBoundToSomethingElseIsRejected() {
        Map<String, Object> options = artemisJndi("queue.orders", "orders");
        options.put("jndi_connection_factory_name", "orders");

        JmsConnectorException error =
                assertThrows(
                        JmsConnectorException.class,
                        () -> JmsConnectionFactories.create(ReadonlyConfig.fromMap(options)));
        assertTrue(error.getMessage().contains("not to a javax.jms.ConnectionFactory"));
    }

    @Test
    void failedJndiLookupDoesNotExposeTheEnvironment() {
        Map<String, Object> options = artemisJndi("java.naming.security.credentials", SECRET);
        options.put("jndi_connection_factory_name", "missing");

        JmsConnectorException error =
                assertThrows(
                        JmsConnectorException.class,
                        () -> JmsConnectionFactories.create(ReadonlyConfig.fromMap(options)));
        assertTrue(error.getMessage().contains("NameNotFoundException"), error.getMessage());
        assertFalse(error.toString().contains(SECRET), error.toString());
        assertNull(error.getCause());
    }

    @Test
    void credentialsAreUsedOnlyWhenAUsernameIsSet() throws Exception {
        RecordingFactory factory = new RecordingFactory();

        JmsConnectionFactories.createConnection(factory, ReadonlyConfig.fromMap(new HashMap<>()));
        Map<String, Object> options = new HashMap<>();
        options.put("username", "reader");
        options.put("password", SECRET);
        JmsConnectionFactories.createConnection(factory, ReadonlyConfig.fromMap(options));

        assertEquals(2, factory.calls.size());
        assertEquals("createConnection()", factory.calls.get(0));
        assertEquals("createConnection(reader, " + SECRET + ")", factory.calls.get(1));
    }

    private static Map<String, Object> artemisJndi(String key, String value) {
        Map<String, Object> options = new HashMap<>();
        options.put(
                "jndi_initial_context_factory",
                "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
        options.put("jndi_properties", Collections.singletonMap(key, value));
        return options;
    }

    private static JmsConnectorException assertFactoryError(
            String className, Map<String, String> properties) {
        JmsConnectorException error =
                assertThrows(
                        JmsConnectorException.class,
                        () -> JmsConnectionFactories.fromClass(className, properties));
        assertEquals(
                JmsConnectorErrorCode.CREATE_CONNECTION_FACTORY_FAILED,
                error.getSeaTunnelErrorCode());
        assertFalse(error.toString().contains(SECRET), error.toString());
        assertNull(error.getCause());
        return error;
    }

    /** Records which createConnection overload is called. */
    private static final class RecordingFactory implements ConnectionFactory {
        private final List<String> calls = new ArrayList<>();

        @Override
        public Connection createConnection() {
            calls.add("createConnection()");
            return null;
        }

        @Override
        public Connection createConnection(String userName, String password) {
            calls.add("createConnection(" + userName + ", " + password + ")");
            return null;
        }
    }

    /** A factory whose setter rejects its value with a message that repeats it. */
    public static final class ThrowingSetterFactory extends RecordingFactoryBase {
        public void setUrl(String url) {
            throw new IllegalArgumentException("bad url " + url);
        }
    }

    /** A factory whose constructor fails with a message that must not be exposed. */
    public static final class ThrowingConstructorFactory extends RecordingFactoryBase {
        public ThrowingConstructorFactory() {
            throw new IllegalStateException(SECRET);
        }
    }

    /** A factory without a no-argument constructor. */
    public static final class NoDefaultConstructorFactory extends RecordingFactoryBase {
        public NoDefaultConstructorFactory(String url) {}
    }

    /** A connection factory that never connects. */
    public static class RecordingFactoryBase implements ConnectionFactory {
        @Override
        public Connection createConnection() {
            return null;
        }

        @Override
        public Connection createConnection(String userName, String password) {
            return null;
        }
    }
}
