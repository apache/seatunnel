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

import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.CLOSE_CONNECTION_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.CREATE_RABBITMQ_CLIENT_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.INIT_SSL_CONTEXT_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.PARSE_URI_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.SEND_MESSAGE_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorErrorCode.SETUP_SSL_FACTORY_FAILED;

import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

@Slf4j
@AllArgsConstructor
public class RabbitmqClient {
    private final RabbitmqConfig config;
    private final ConnectionFactory connectionFactory;
    private final Connection connection;
    private final Channel channel;

    public RabbitmqClient(RabbitmqConfig config) {
        this.config = config;
        try {
            this.connectionFactory = getConnectionFactory();
            this.connection = connectionFactory.newConnection();
            this.channel = connection.createChannel();
            //set channel prefetch count
            if (config.getPrefetchCount() != null) {
                channel.basicQos(config.getPrefetchCount(), true);
            }
            setupQueue();
        } catch (Exception e) {
            throw new RabbitmqConnectorException(CREATE_RABBITMQ_CLIENT_FAILED, String.format("Error while create RMQ client with %s at %s", config.getQueueName(), config.getHost()), e);
        }

    }

    public Channel getChannel() {
        return channel;
    }

    public DefaultConsumer getQueueingConsumer(Handover<Delivery> handover) {
        DefaultConsumer consumer = new QueueingConsumer(channel, handover);
        return consumer;
    }

    public ConnectionFactory getConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        if (!StringUtils.isEmpty(config.getUri())) {
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
            factory.setVirtualHost(config.getVirtualHost());
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

    public void write(byte[] msg) {
        try {
            if (StringUtils.isEmpty(config.getRoutingKey())) {
                channel.basicPublish("", config.getQueueName(), null, msg);
            } else {
                //not support set returnListener
                channel.basicPublish(
                        config.getExchange(),
                        config.getRoutingKey(),
                        false,
                        false,
                        null,
                        msg);
            }
        } catch (IOException e) {
            if (config.isLogFailuresOnly()) {
                log.error("Cannot send RMQ message {} at {}", config.getQueueName(), config.getHost(), e);
            } else {
                throw new RabbitmqConnectorException(SEND_MESSAGE_FAILED, String.format("Cannot send RMQ message %s at %s", config.getQueueName(), config.getHost()), e);
            }
        }
    }

    public void close() {
        Exception t = null;
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException | TimeoutException e) {
            t = e;
        }

        try {
            if (connection != null) {
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
            throw new RabbitmqConnectorException(CLOSE_CONNECTION_FAILED, String.format("Error while closing RMQ connection with  %s at %s", config.getQueueName(), config.getHost()), t);
        }
    }

    protected void setupQueue() throws IOException {
        if (config.getQueueName() != null) {
            declareQueueDefaults(channel, config.getQueueName());
        }
    }

    private void declareQueueDefaults(Channel channel, String queueName) throws IOException {
        channel.queueDeclare(queueName, true, false, false, null);
    }

}
