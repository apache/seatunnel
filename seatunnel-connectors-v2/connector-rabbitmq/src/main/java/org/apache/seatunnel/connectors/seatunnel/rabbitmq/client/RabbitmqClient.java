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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.client;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.DeliveryMessage;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.CLOSE_CONNECTION_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.CREATE_RABBITMQ_CLIENT_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.INIT_SSL_CONTEXT_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.PARSE_URI_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.SEND_MESSAGE_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.SETUP_SSL_FACTORY_FAILED;

/** RabbitMQ client for interaction with the broker. */
@Slf4j
public class RabbitmqClient implements AutoCloseable {
    private final RabbitmqConfig config;
    private final ConnectionFactory connectionFactory;
    private final Connection connection;
    private final Channel channel;

    /**
     * Constructor for RabbitmqClient.
     *
     * @param config RabbitMQ configuration
     */
    public RabbitmqClient(RabbitmqConfig config) {
        this.config = config;
        try {
            this.connectionFactory = createConnectionFactory();
            this.connection = connectionFactory.newConnection();
            this.channel = connection.createChannel();

            // set channel prefetch count
            if (config.getPrefetchCount() != null) {
                channel.basicQos(config.getPrefetchCount(), true);
            }

        } catch (Exception e) {
            throw new RabbitmqConnectorException(
                    CREATE_RABBITMQ_CLIENT_FAILED,
                    String.format(
                            "Error while creating RMQ client with queue %s at %s",
                            config.getQueueName(), config.getHost()),
                    e);
        }
    }

    /**
     * Get the current RabbitMQ channel.
     *
     * @return RabbitMQ channel
     */
    public Channel getChannel() {
        return channel;
    }

    /**
     * Create a new QueueingConsumer for the given queue and split.
     *
     * @param queue blocking queue
     * @param splitId split id
     * @return consumer instance
     */
    public DefaultConsumer getQueueingConsumer(
            BlockingQueue<DeliveryMessage> queue, String splitId) {
        return new QueueingConsumer(channel, queue, splitId);
    }

    private ConnectionFactory createConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        if (StringUtils.isNotEmpty(config.getUri())) {
            try {
                factory.setUri(config.getUri());
            } catch (URISyntaxException e) {
                throw new RabbitmqConnectorException(PARSE_URI_FAILED, e);
            } catch (KeyManagementException e) {
                // this should never happen
                throw new RabbitmqConnectorException(INIT_SSL_CONTEXT_FAILED, e);
            } catch (NoSuchAlgorithmException e) {
                // this should never happen
                throw new RabbitmqConnectorException(SETUP_SSL_FACTORY_FAILED, e);
            }
        } else {
            factory.setHost(config.getHost());
            factory.setPort(config.getPort());
            if (StringUtils.isNotEmpty(config.getVirtualHost())) {
                factory.setVirtualHost(config.getVirtualHost());
            }
            factory.setUsername(config.getUsername());
            factory.setPassword(config.getPassword());
        }

        if (config.getAutomaticRecovery() != null) {
            factory.setAutomaticRecoveryEnabled(config.getAutomaticRecovery());
        }
        if (config.getConnectionTimeout() != null) {
            factory.setConnectionTimeout(config.getConnectionTimeout());
        }
        if (config.getNetworkRecoveryInterval() != null) {
            factory.setNetworkRecoveryInterval(config.getNetworkRecoveryInterval());
        }
        if (config.getRequestedHeartbeat() != null) {
            factory.setRequestedHeartbeat(config.getRequestedHeartbeat());
        }
        if (config.getTopologyRecovery() != null) {
            factory.setTopologyRecoveryEnabled(config.getTopologyRecovery());
        }
        if (config.getRequestedChannelMax() != null) {
            factory.setRequestedChannelMax(config.getRequestedChannelMax());
        }
        if (config.getRequestedFrameMax() != null) {
            factory.setRequestedFrameMax(config.getRequestedFrameMax());
        }
        return factory;
    }

    /**
     * Write data to RabbitMQ.
     *
     * @param msg message body
     */
    public void write(byte[] msg) {
        try {
            if (StringUtils.isEmpty(config.getRoutingKey())) {
                channel.basicPublish("", config.getQueueName(), null, msg);
            } else {
                // not support set returnListener
                channel.basicPublish(
                        config.getExchange(), config.getRoutingKey(), false, false, null, msg);
            }
        } catch (IOException e) {
            if (config.isLogFailuresOnly()) {
                log.error(
                        "Cannot send RMQ message to queue {} at host {}",
                        config.getQueueName(),
                        config.getHost(),
                        e);
            } else {
                throw new RabbitmqConnectorException(
                        SEND_MESSAGE_FAILED,
                        String.format(
                                "Cannot send RMQ message to queue %s at host %s",
                                config.getQueueName(), config.getHost()),
                        e);
            }
        }
    }

    public void close() {
        Exception t = null;
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException e) {
            t = e;
        }

        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (IOException e) {
            if (t != null) {
                log.warn(
                        "Both channel and connection closing failed. Logging channel exception and failing with connection exception",
                        t);
            }
            t = e;
        }
        if (t != null) {
            throw new RabbitmqConnectorException(
                    CLOSE_CONNECTION_FAILED,
                    String.format(
                            "Error while closing RMQ connection with queue %s at %s",
                            config.getQueueName(), config.getHost()),
                    t);
        }
    }

    /** Declare the queue using configuration defaults. */
    public void setupQueue() throws IOException {
        if (StringUtils.isNotEmpty(config.getQueueName())) {
            declareQueueDefaults(channel, config);
        }
    }

    /** Declare a specific queue */
    public void setupQueue(String queueName) throws IOException {
        if (StringUtils.isNotEmpty(queueName)) {
            channel.queueDeclare(
                    queueName,
                    config.getDurable(),
                    config.getExclusive(),
                    config.getAutoDelete(),
                    null);
        }
    }

    private void declareQueueDefaults(Channel channel, RabbitmqConfig config) throws IOException {
        channel.queueDeclare(
                config.getQueueName(),
                config.getDurable(),
                config.getExclusive(),
                config.getAutoDelete(),
                null);
    }
}
