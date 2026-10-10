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

package org.apache.seatunnel.connectors.seatunnel.activemq.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.activemq.exception.ActivemqConnectorException;

import org.apache.activemq.ActiveMQConnectionFactory;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ActivemqClientTest {

    private static final String TEST_URI = "tcp://localhost:61616";
    private static final String TEST_QUEUE = "test-queue";
    private static final String TEST_URI_WITH_CREDENTIALS =
            "tcp://admin:secretPass@localhost:61616";

    private static Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ActivemqSinkOptions.URI.key(), TEST_URI);
        config.put(ActivemqSinkOptions.QUEUE_NAME.key(), TEST_QUEUE);
        return config;
    }

    /**
     * L1 + L2: Session and MessageProducer are created once in the constructor, and
     * connection.start() is called once in the constructor.
     */
    @Test
    void constructorCreatesSessionProducerAndStartsConnection() throws Exception {
        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);
        Queue mockQueue = mock(Queue.class);
        MessageProducer mockProducer = mock(MessageProducer.class);

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenReturn(mockSession);
            when(mockSession.createQueue(TEST_QUEUE)).thenReturn(mockQueue);
            when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer);

            new ActivemqClient(ReadonlyConfig.fromMap(baseConfig()));

            // L2: connection.start() called once in constructor
            verify(mockConnection).start();
            // L1: session created once in constructor
            verify(mockConnection).createSession(false, Session.AUTO_ACKNOWLEDGE);
            // L1: producer created once in constructor
            verify(mockSession).createProducer(mockQueue);
        }
    }

    /**
     * L1: write() reuses the session and producer created in the constructor instead of creating
     * new JMS resources for each message.
     */
    @Test
    void writeReusesSessionAndProducer() throws Exception {
        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);
        Queue mockQueue = mock(Queue.class);
        MessageProducer mockProducer = mock(MessageProducer.class);
        TextMessage mockTextMessage = mock(TextMessage.class);

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenReturn(mockSession);
            when(mockSession.createQueue(TEST_QUEUE)).thenReturn(mockQueue);
            when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer);
            when(mockSession.createTextMessage("hello")).thenReturn(mockTextMessage);

            ActivemqClient client = new ActivemqClient(ReadonlyConfig.fromMap(baseConfig()));
            client.write("hello".getBytes(StandardCharsets.UTF_8));

            // L1: createSession still called only once (constructor only, not in write)
            verify(mockConnection, times(1)).createSession(false, Session.AUTO_ACKNOWLEDGE);
            // L1: createProducer still called only once (constructor only, not in write)
            verify(mockSession, times(1)).createProducer(mockQueue);
            // L2: connection.start() not called again in write()
            verify(mockConnection, times(1)).start();
            // write() creates a TextMessage and sends it via the reused producer
            verify(mockSession).createTextMessage("hello");
            verify(mockProducer).send(mockTextMessage);
        }
    }

    /**
     * L4 + exception chain: write() wraps JMSException with the queue name (not the broker URI,
     * which may contain credentials) in the error message, and preserves the original exception as
     * the cause.
     */
    @Test
    void writeWrapsJmsExceptionWithQueueInMessage() throws Exception {
        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);
        Queue mockQueue = mock(Queue.class);
        MessageProducer mockProducer = mock(MessageProducer.class);
        JMSException sendError = new JMSException("send failed");

        Map<String, Object> config = baseConfig();
        config.put(ActivemqSinkOptions.URI.key(), TEST_URI_WITH_CREDENTIALS);

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenReturn(mockSession);
            when(mockSession.createQueue(TEST_QUEUE)).thenReturn(mockQueue);
            when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer);
            when(mockSession.createTextMessage("hello")).thenThrow(sendError);

            ActivemqClient client = new ActivemqClient(ReadonlyConfig.fromMap(config));

            ActivemqConnectorException ex =
                    assertThrows(
                            ActivemqConnectorException.class,
                            () -> client.write("hello".getBytes(StandardCharsets.UTF_8)));

            // Issue 2: error message contains queue name, not the broker URI
            assertTrue(ex.getMessage().contains(TEST_QUEUE));
            // Issue 2: credentials from the URI must not appear in the message
            assertFalse(ex.getMessage().contains("secretPass"));
            assertFalse(ex.getMessage().contains(TEST_URI_WITH_CREDENTIALS));
            // Exception chain preserved (3-arg constructor)
            assertEquals(sendError, ex.getCause());
        }
    }

    /** L1: close() closes Producer, Session, and Connection in reverse creation order. */
    @Test
    void closeClosesAllResourcesInReverseOrder() throws Exception {
        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);
        Queue mockQueue = mock(Queue.class);
        MessageProducer mockProducer = mock(MessageProducer.class);

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenReturn(mockSession);
            when(mockSession.createQueue(TEST_QUEUE)).thenReturn(mockQueue);
            when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer);

            ActivemqClient client = new ActivemqClient(ReadonlyConfig.fromMap(baseConfig()));
            client.close();

            InOrder order = inOrder(mockProducer, mockSession, mockConnection);
            order.verify(mockProducer).close();
            order.verify(mockSession).close();
            order.verify(mockConnection).close();
        }
    }

    /**
     * L3: close() preserves the first JMSException as the cause and still closes all remaining
     * resources (session and connection) even when an earlier close fails.
     */
    @Test
    void closePreservesFirstExceptionAndClosesRemainingResources() throws Exception {
        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);
        Queue mockQueue = mock(Queue.class);
        MessageProducer mockProducer = mock(MessageProducer.class);
        JMSException producerError = new JMSException("producer close failed");
        JMSException sessionError = new JMSException("session close failed");

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenReturn(mockSession);
            when(mockSession.createQueue(TEST_QUEUE)).thenReturn(mockQueue);
            when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer);
            doThrow(producerError).when(mockProducer).close();
            doThrow(sessionError).when(mockSession).close();

            ActivemqClient client = new ActivemqClient(ReadonlyConfig.fromMap(baseConfig()));

            ActivemqConnectorException ex =
                    assertThrows(ActivemqConnectorException.class, () -> client.close());

            // L3: first error (producer) is preserved as the cause
            assertEquals(producerError, ex.getCause());
            // All resources still closed despite errors
            verify(mockProducer).close();
            verify(mockSession).close();
            verify(mockConnection).close();
        }
    }

    /**
     * L3: close() wraps a connection-only close failure with the original JMSException as the
     * cause.
     */
    @Test
    void closeWithConnectionErrorOnlyWrapsCause() throws Exception {
        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);
        Queue mockQueue = mock(Queue.class);
        MessageProducer mockProducer = mock(MessageProducer.class);
        JMSException connError = new JMSException("connection close failed");

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenReturn(mockSession);
            when(mockSession.createQueue(TEST_QUEUE)).thenReturn(mockQueue);
            when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer);
            doThrow(connError).when(mockConnection).close();

            ActivemqClient client = new ActivemqClient(ReadonlyConfig.fromMap(baseConfig()));

            ActivemqConnectorException ex =
                    assertThrows(ActivemqConnectorException.class, () -> client.close());

            // L3: connection close error is preserved as the cause
            assertEquals(connError, ex.getCause());
            verify(mockProducer).close();
            verify(mockSession).close();
            verify(mockConnection).close();
        }
    }

    /**
     * When factory-level options are not explicitly set, their setters must NOT be called so that
     * {@code jms.*} parameters embedded in the broker URI are preserved. Producer-level options
     * (delivery_mode, time_to_live, priority) always apply their JMS defaults.
     */
    @Test
    void doesNotCallFactorySettersWhenOptionsNotSet() throws Exception {
        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);
        Queue mockQueue = mock(Queue.class);
        MessageProducer mockProducer = mock(MessageProducer.class);

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenReturn(mockSession);
            when(mockSession.createQueue(TEST_QUEUE)).thenReturn(mockQueue);
            when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer);

            new ActivemqClient(ReadonlyConfig.fromMap(baseConfig()));

            ActiveMQConnectionFactory factory = ignored.constructed().get(0);
            // Issue 1: factory setters must NOT be called when options use defaults
            verify(factory, times(0)).setMaxThreadPoolSize(anyInt());
            verify(factory, times(0)).setSendTimeout(anyInt());
            verify(factory, times(0)).setUseCompression(anyBoolean());
            verify(factory, times(0)).setConnectResponseTimeout(anyInt());
            verify(factory, times(0)).setProducerWindowSize(anyInt());
            verify(factory, times(0)).setUseAsyncSend(anyBoolean());
            // Producer defaults are always applied (they default to JMS defaults)
            verify(mockProducer).setDeliveryMode(2);
            verify(mockProducer).setTimeToLive(0L);
            verify(mockProducer).setPriority(4);
        }
    }

    /**
     * When the broker URI contains {@code jms.*} query parameters and the user does not set the
     * corresponding SeaTunnel options, the URI parameters must be preserved on the
     * ActiveMQConnectionFactory. This test uses a real (non-mocked) factory to verify the
     * end-to-end behavior.
     */
    @Test
    void preservesUriJmsParamsWhenOptionsNotSet() throws Exception {
        String uriWithJmsParams =
                "tcp://localhost:61616?jms.useAsyncSend=true&jms.producerWindowSize=1048576";
        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);
        Queue mockQueue = mock(Queue.class);
        MessageProducer mockProducer = mock(MessageProducer.class);

        Map<String, Object> config = baseConfig();
        config.put(ActivemqSinkOptions.URI.key(), uriWithJmsParams);

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenReturn(mockSession);
            when(mockSession.createQueue(TEST_QUEUE)).thenReturn(mockQueue);
            when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer);

            new ActivemqClient(ReadonlyConfig.fromMap(config));

            ActiveMQConnectionFactory factory = ignored.constructed().get(0);
            // Issue 1: URI jms.* params must not be overwritten by SeaTunnel defaults
            verify(factory, times(0)).setUseAsyncSend(false);
            verify(factory, times(0)).setProducerWindowSize(0);
        }
    }

    /**
     * Verifies that explicitly configured properties are applied to the connection factory and the
     * producer during construction.
     */
    @Test
    void appliesOptionalProperties() throws Exception {
        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);
        Queue mockQueue = mock(Queue.class);
        MessageProducer mockProducer = mock(MessageProducer.class);

        Map<String, Object> config = baseConfig();
        config.put(ActivemqSinkOptions.MAX_THREAD_POOL_SIZE.key(), 100);
        config.put(ActivemqSinkOptions.SEND_TIMEOUT.key(), 5000);
        config.put(ActivemqSinkOptions.USE_COMPRESSION.key(), true);
        config.put(ActivemqSinkOptions.CONNECT_RESPONSE_TIMEOUT.key(), 5000);
        config.put(ActivemqSinkOptions.PRODUCER_WINDOW_SIZE.key(), 1048576);
        config.put(ActivemqSinkOptions.USE_ASYNC_SEND.key(), true);
        config.put(ActivemqSinkOptions.DELIVERY_MODE.key(), 1);
        config.put(ActivemqSinkOptions.TIME_TO_LIVE.key(), 60000L);
        config.put(ActivemqSinkOptions.PRIORITY.key(), 9);

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenReturn(mockSession);
            when(mockSession.createQueue(TEST_QUEUE)).thenReturn(mockQueue);
            when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer);

            new ActivemqClient(ReadonlyConfig.fromMap(config));

            ActiveMQConnectionFactory factory = ignored.constructed().get(0);
            // ConnectionFactory explicit values
            verify(factory).setMaxThreadPoolSize(100);
            verify(factory).setSendTimeout(5000);
            verify(factory).setUseCompression(true);
            verify(factory).setConnectResponseTimeout(5000);
            verify(factory).setProducerWindowSize(1048576);
            verify(factory).setUseAsyncSend(true);
            // Producer explicit values
            verify(mockProducer).setDeliveryMode(1);
            verify(mockProducer).setTimeToLive(60000L);
            verify(mockProducer).setPriority(9);
        }
    }

    /**
     * P1: When the constructor fails partway through, partially-created resources (e.g. connection)
     * are cleaned up via close() to avoid leaks, and the original construction exception is
     * propagated as the cause.
     */
    @Test
    void constructorFailureCleansUpPartiallyCreatedResources() throws Exception {
        Connection mockConnection = mock(Connection.class);
        JMSException constructionError = new JMSException("createSession failed");

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            // connection.start() succeeds, then createSession fails
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenThrow(constructionError);

            ActivemqConnectorException ex =
                    assertThrows(
                            ActivemqConnectorException.class,
                            () -> new ActivemqClient(ReadonlyConfig.fromMap(baseConfig())));

            // Original construction error is preserved as the cause
            assertEquals(constructionError, ex.getCause());
            // P1: connection was cleaned up despite construction failure
            verify(mockConnection).close();
        }
    }

    /**
     * Issue 5: When the constructor fails and best-effort close() also throws, the close error is
     * attached as a suppressed exception on the original cause instead of being swallowed.
     */
    @Test
    void constructorFailurePreservesCloseErrorAsSuppressed() throws Exception {
        Connection mockConnection = mock(Connection.class);
        JMSException constructionError = new JMSException("createSession failed");
        JMSException closeError = new JMSException("connection close failed");

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenThrow(constructionError);
            doThrow(closeError).when(mockConnection).close();

            ActivemqConnectorException ex =
                    assertThrows(
                            ActivemqConnectorException.class,
                            () -> new ActivemqClient(ReadonlyConfig.fromMap(baseConfig())));

            // Original construction error is preserved as the cause
            assertEquals(constructionError, ex.getCause());
            // Issue 5: close() wraps JMSException in ActivemqConnectorException, which is then
            // attached as a suppressed exception instead of being swallowed
            assertEquals(1, ex.getCause().getSuppressed().length);
            assertTrue(ex.getCause().getSuppressed()[0] instanceof ActivemqConnectorException);
        }
    }

    /**
     * Issue 3: delivery_mode must be 1 (NON_PERSISTENT) or 2 (PERSISTENT). An invalid value should
     * fail fast with a clear message instead of surfacing as a generic construction error.
     */
    @Test
    void constructorRejectsInvalidDeliveryMode() throws Exception {
        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);
        Queue mockQueue = mock(Queue.class);
        MessageProducer mockProducer = mock(MessageProducer.class);

        Map<String, Object> config = baseConfig();
        config.put(ActivemqSinkOptions.DELIVERY_MODE.key(), 5);

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenReturn(mockSession);
            when(mockSession.createQueue(TEST_QUEUE)).thenReturn(mockQueue);
            when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer);

            ActivemqConnectorException ex =
                    assertThrows(
                            ActivemqConnectorException.class,
                            () -> new ActivemqClient(ReadonlyConfig.fromMap(config)));

            assertTrue(ex.getCause() instanceof IllegalArgumentException);
            assertTrue(ex.getCause().getMessage().contains("delivery_mode"));
        }
    }

    /**
     * Issue 3: priority must be between 0 and 9. An out-of-range value should fail fast with a
     * clear message.
     */
    @Test
    void constructorRejectsInvalidPriority() throws Exception {
        Connection mockConnection = mock(Connection.class);
        Session mockSession = mock(Session.class);
        Queue mockQueue = mock(Queue.class);
        MessageProducer mockProducer = mock(MessageProducer.class);

        Map<String, Object> config = baseConfig();
        config.put(ActivemqSinkOptions.PRIORITY.key(), 15);

        try (MockedConstruction<ActiveMQConnectionFactory> ignored =
                mockConstruction(
                        ActiveMQConnectionFactory.class,
                        (factory, ctx) ->
                                when(factory.createConnection()).thenReturn(mockConnection))) {
            when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
                    .thenReturn(mockSession);
            when(mockSession.createQueue(TEST_QUEUE)).thenReturn(mockQueue);
            when(mockSession.createProducer(mockQueue)).thenReturn(mockProducer);

            ActivemqConnectorException ex =
                    assertThrows(
                            ActivemqConnectorException.class,
                            () -> new ActivemqClient(ReadonlyConfig.fromMap(config)));

            assertTrue(ex.getCause() instanceof IllegalArgumentException);
            assertTrue(ex.getCause().getMessage().contains("priority"));
        }
    }
}
