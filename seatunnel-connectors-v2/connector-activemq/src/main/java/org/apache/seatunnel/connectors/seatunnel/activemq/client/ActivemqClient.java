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
import org.apache.seatunnel.connectors.seatunnel.activemq.exception.ActivemqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.activemq.exception.ActivemqConnectorException;

import org.apache.activemq.ActiveMQConnectionFactory;

import lombok.extern.slf4j.Slf4j;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.nio.charset.StandardCharsets;

import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.ALWAYS_SESSION_ASYNC;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.ALWAYS_SYNC_SEND;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.CHECK_FOR_DUPLICATE;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.CLOSE_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.CONNECT_RESPONSE_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.CONSUMER_EXPIRY_CHECK_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.DELIVERY_MODE;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.DISPATCH_ASYNC;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.MAX_THREAD_POOL_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.NESTED_MAP_AND_LIST_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.PRIORITY;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.PRODUCER_WINDOW_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.QUEUE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.SEND_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.TIME_TO_LIVE;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.URI;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.USE_ASYNC_SEND;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.USE_COMPRESSION;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT;

@Slf4j
public class ActivemqClient {
    private final ReadonlyConfig config;
    private final ActiveMQConnectionFactory connectionFactory;
    private final Connection connection;
    private final Session session;
    private final MessageProducer producer;

    public ActivemqClient(ReadonlyConfig config) {
        this(config, createClientResources(config));
    }

    ActivemqClient(
            ReadonlyConfig config,
            ActiveMQConnectionFactory connectionFactory,
            Connection connection) {
        this(config, createProducerResources(config, connectionFactory, connection));
    }

    private ActivemqClient(ReadonlyConfig config, ClientResources resources) {
        this.config = config;
        this.connectionFactory = resources.connectionFactory;
        this.connection = resources.connection;
        this.session = resources.session;
        this.producer = resources.producer;
    }

    public ActiveMQConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    static ActiveMQConnectionFactory createConnectionFactory(ReadonlyConfig config) {
        log.info("broker url : " + config.get(URI));
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(config.get(URI));

        if (config.get(ALWAYS_SESSION_ASYNC) != null) {
            factory.setAlwaysSessionAsync(config.get(ALWAYS_SESSION_ASYNC));
        }

        if (config.get(CLIENT_ID) != null) {
            factory.setClientID(config.get(CLIENT_ID));
        }

        if (config.get(ALWAYS_SYNC_SEND) != null) {
            factory.setAlwaysSyncSend(config.get(ALWAYS_SYNC_SEND));
        }

        if (config.get(CHECK_FOR_DUPLICATE) != null) {
            factory.setCheckForDuplicates(config.get(CHECK_FOR_DUPLICATE));
        }

        if (config.get(CLOSE_TIMEOUT) != null) {
            factory.setCloseTimeout(config.get(CLOSE_TIMEOUT));
        }

        if (config.get(CONSUMER_EXPIRY_CHECK_ENABLED) != null) {
            factory.setConsumerExpiryCheckEnabled(config.get(CONSUMER_EXPIRY_CHECK_ENABLED));
        }
        if (config.get(DISPATCH_ASYNC) != null) {
            factory.setDispatchAsync(config.get(DISPATCH_ASYNC));
        }
        if (config.get(WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT) != null) {
            factory.setWarnAboutUnstartedConnectionTimeout(
                    config.get(WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT));
        }

        if (config.get(NESTED_MAP_AND_LIST_ENABLED) != null) {
            factory.setNestedMapAndListEnabled(config.get(NESTED_MAP_AND_LIST_ENABLED));
        }

        factory.setMaxThreadPoolSize(config.get(MAX_THREAD_POOL_SIZE));
        factory.setSendTimeout(config.get(SEND_TIMEOUT));
        factory.setUseCompression(config.get(USE_COMPRESSION));
        factory.setConnectResponseTimeout(config.get(CONNECT_RESPONSE_TIMEOUT));
        factory.setProducerWindowSize(config.get(PRODUCER_WINDOW_SIZE));
        factory.setUseAsyncSend(config.get(USE_ASYNC_SEND));
        return factory;
    }

    public void write(byte[] msg) {
        try {
            String messageBody = new String(msg, StandardCharsets.UTF_8);
            TextMessage objectMessage = session.createTextMessage(messageBody);
            producer.send(objectMessage);

        } catch (JMSException e) {
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.SEND_MESSAGE_FAILED,
                    String.format(
                            "Cannot send AMQ message %s at %s",
                            config.get(QUEUE_NAME), config.get(CLIENT_ID)),
                    e);
        }
    }

    public void close() {
        JMSException e = closeResources(producer, session, connection);
        if (e != null) {
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.CLOSE_CONNECTION_FAILED,
                    String.format(
                            "Error while closing AMQ connection with  %s", config.get(QUEUE_NAME)),
                    e);
        }
    }

    private static ClientResources createClientResources(ReadonlyConfig config) {
        ActiveMQConnectionFactory factory;
        Connection connection;
        try {
            factory = createConnectionFactory(config);
            log.info("connection factory created");
            connection = createConnection(config, factory);
        } catch (Exception e) {
            log.error("Error while creating AMQ client", e);
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.CREATE_ACTIVEMQ_CLIENT_FAILED,
                    "Error while create AMQ client ",
                    e);
        }
        log.info("connection created");
        return createProducerResources(config, factory, connection);
    }

    private static ClientResources createProducerResources(
            ReadonlyConfig config, ActiveMQConnectionFactory factory, Connection connection) {
        Session session = null;
        MessageProducer producer = null;
        try {
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue(config.get(QUEUE_NAME));
            producer = session.createProducer(destination);
            producer.setDeliveryMode(config.get(DELIVERY_MODE));
            producer.setTimeToLive(config.get(TIME_TO_LIVE));
            producer.setPriority(config.get(PRIORITY));
            return new ClientResources(factory, connection, session, producer);
        } catch (Exception e) {
            JMSException closeException = closeResources(producer, session, connection);
            if (closeException != null) {
                e.addSuppressed(closeException);
            }
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.CREATE_ACTIVEMQ_CLIENT_FAILED,
                    "Error while create AMQ client ",
                    e);
        }
    }

    private static JMSException closeResources(
            MessageProducer producer, Session session, Connection connection) {
        JMSException exception = null;
        if (producer != null) {
            exception = closeResource(producer::close, exception);
        }
        if (session != null) {
            exception = closeResource(session::close, exception);
        }
        if (connection != null) {
            exception = closeResource(connection::close, exception);
        }
        return exception;
    }

    private static JMSException closeResource(CloseAction action, JMSException previous) {
        try {
            action.close();
        } catch (JMSException e) {
            if (previous == null) {
                return e;
            }
            previous.addSuppressed(e);
        }
        return previous;
    }

    @FunctionalInterface
    private interface CloseAction {
        void close() throws JMSException;
    }

    private static Connection createConnection(
            ReadonlyConfig config, ActiveMQConnectionFactory connectionFactory)
            throws JMSException {
        if (config.get(USERNAME) != null && config.get(PASSWORD) != null) {
            return connectionFactory.createConnection(config.get(USERNAME), config.get(PASSWORD));
        }
        return connectionFactory.createConnection();
    }

    private static class ClientResources {
        private final ActiveMQConnectionFactory connectionFactory;
        private final Connection connection;
        private final Session session;
        private final MessageProducer producer;

        private ClientResources(
                ActiveMQConnectionFactory connectionFactory,
                Connection connection,
                Session session,
                MessageProducer producer) {
            this.connectionFactory = connectionFactory;
            this.connection = connection;
            this.session = session;
            this.producer = producer;
        }
    }
}
