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

import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;

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
    private RabbitmqConfig config;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Channel channel;

    public RabbitmqClient(RabbitmqConfig config) {
        this.config = config;
        try {
            this.connectionFactory = getConnectionFactory();
            this.connection = connectionFactory.newConnection();
            this.channel = connection.createChannel();
            setupQueue();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error while create RMQ client with "
                            + this.config.getQueueName()
                            + " at "
                            + this.config.getHost(),
                    e);
        }

    }

    public Channel getChannel() {
        return channel;
    }

    public DefaultConsumer getQueueingConsumer(Handover<Delivery> handover) {
        DefaultConsumer consumer = new QueueingConsumer(channel, handover);
        return consumer;
    }

    public ConnectionFactory getConnectionFactory()
            throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        ConnectionFactory factory = new ConnectionFactory();
        if (!StringUtils.isEmpty(config.getUri())) {
            try {
                factory.setUri(config.getUri());
            } catch (URISyntaxException e) {
                log.error("Failed to parse uri", e);
                throw e;
            } catch (KeyManagementException e) {
                // this should never happen
                log.error("Failed to initialize ssl context.", e);
                throw e;
            } catch (NoSuchAlgorithmException e) {
                // this should never happen
                log.error("Failed to setup ssl factory.", e);
                throw e;
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
                log.error(
                        "Cannot send RMQ message {} at {}",
                        config.getQueueName(),
                        config.getHost(),
                        e);
            } else {
                throw new RuntimeException(
                        "Cannot send RMQ message "
                                + config.getQueueName()
                                + " at "
                                + config.getHost(),
                        e);
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
            throw new RuntimeException(
                    "Error while closing RMQ connection with "
                            + this.config.getQueueName()
                            + " at "
                            + this.config.getHost(),
                    t);
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
