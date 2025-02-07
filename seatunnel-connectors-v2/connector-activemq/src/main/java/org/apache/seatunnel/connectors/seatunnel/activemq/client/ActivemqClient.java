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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.nio.charset.StandardCharsets;

import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqOptions.CHECK_FOR_DUPLICATE;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqOptions.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqOptions.CLOSE_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqOptions.DISABLE_TIMESTAMP_BY_DEFAULT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqOptions.QUEUE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqOptions.URI;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqOptions.WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.ALWAYS_SESSION_ASYNC;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.ALWAYS_SYNC_SEND;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.COPY_MESSAGE_ON_SEND;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.NESTED_MAP_AND_LIST_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.USE_COMPRESSION;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSourceOptions.CONSUMER_EXPIRY_CHECK_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSourceOptions.DISPATCH_ASYNC;

@Slf4j
@AllArgsConstructor
public class ActivemqClient {
    private final ReadonlyConfig config;
    private final ActiveMQConnectionFactory connectionFactory;
    private final Connection connection;

    public ActivemqClient(ReadonlyConfig config) {
        this.config = config;
        try {
            this.connectionFactory = getConnectionFactory();
            log.info("connection factory created");
            this.connection = createConnection(config);
            log.info("connection created");

        } catch (Exception e) {
            e.printStackTrace();
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.CREATE_ACTIVEMQ_CLIENT_FAILED,
                    "Error while create AMQ client ");
        }
    }

    public ActiveMQConnectionFactory getConnectionFactory() {
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

        if (config.get(USE_COMPRESSION) != null) {
            factory.setUseCompression(config.get(USE_COMPRESSION));
        }

        if (config.get(COPY_MESSAGE_ON_SEND) != null) {
            factory.setCopyMessageOnSend(config.get(COPY_MESSAGE_ON_SEND));
        }

        if (config.get(DISABLE_TIMESTAMP_BY_DEFAULT) != null) {
            factory.setDisableTimeStampsByDefault(config.get(DISABLE_TIMESTAMP_BY_DEFAULT));
        }

        return factory;
    }

    public void write(byte[] msg) {
        try {
            Session session = this.getSession();
            Destination destination = session.createQueue(config.get(QUEUE_NAME));
            MessageProducer producer = session.createProducer(destination);
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

    public Session getSession() {
        try {
            this.connection.start();
            return this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.CLOSE_SESSION_FAILED, e);
        }
    }

    public MessageConsumer getConsumer() {
        try {
            return this.getSession()
                    .createConsumer(this.getSession().createQueue(config.get(QUEUE_NAME)));
        } catch (JMSException e) {
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.INITIALIZE_CONSUME_FAILED, e);
        }
    }

    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException e) {
            throw new ActivemqConnectorException(
                    ActivemqConnectorErrorCode.CLOSE_CONNECTION_FAILED,
                    String.format(
                            "Error while closing AMQ connection with  %s", config.get(QUEUE_NAME)));
        }
    }

    private Connection createConnection(ReadonlyConfig config) throws JMSException {
        if (config.get(USERNAME) != null && config.get(PASSWORD) != null) {
            return connectionFactory.createConnection(config.get(USERNAME), config.get(PASSWORD));
        }
        return connectionFactory.createConnection();
    }
}
