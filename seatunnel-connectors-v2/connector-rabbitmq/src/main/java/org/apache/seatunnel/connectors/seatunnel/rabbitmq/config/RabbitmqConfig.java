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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.config;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.config.CheckConfigUtil;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
@AllArgsConstructor
public class RabbitmqConfig implements Serializable {
    private String host;
    private Integer port;
    private String virtualHost;
    private String queueName;
    private String username;
    private String password;
    private String uri;
    private Integer networkRecoveryInterval;
    private Boolean automaticRecovery;
    private Boolean topologyRecovery;
    private Integer connectionTimeout;
    private Boolean durable;
    private Boolean exclusive;
    private Boolean autoDelete;
    private String routingKey;
    private boolean logFailuresOnly = false;
    private String exchange = "";
    private boolean usesCorrelationId = false;
    private boolean forE2ETesting = false;
    private Integer prefetchCount;
    private Integer requestedChannelMax;
    private Integer requestedFrameMax;
    private Integer requestedHeartbeat;
    private long deliveryTimeout;

    private final Map<String, Object> sinkOptionProps = new HashMap<>();

    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the default host to use for connections");

    public static final Option<Integer> PORT =
            Options.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the default port to use for connections");

    public static final Option<String> VIRTUAL_HOST =
            Options.key("virtual_host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the virtual host to use when connecting to the broker");

    public static final Option<String> QUEUE_NAME =
            Options.key("queue_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the queue to write the message to");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the AMQP user name to use when connecting to the broker");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the password to use when connecting to the broker");

    public static final Option<Map<String, String>> RABBITMQ_CONFIG =
            Options.key("rabbitmq.config")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "In addition to the above parameters that must be specified by the RabbitMQ client, the user can also specify multiple non-mandatory parameters for the client, "
                                    + "covering [all the parameters specified in the official RabbitMQ document](https://www.rabbitmq.com/configure.html).");

    private void parseSinkOptionProperties(Config pluginConfig) {
        if (CheckConfigUtil.isValidParam(pluginConfig, RABBITMQ_CONFIG.key())) {
            pluginConfig
                    .getObject(RABBITMQ_CONFIG.key())
                    .forEach(
                            (key, value) -> {
                                final String configKey = key.toLowerCase();
                                this.sinkOptionProps.put(configKey, value.unwrapped());
                            });
        }
    }

    public RabbitmqConfig(Config config) {
        this.host = config.getString(HOST.key());
        this.port = config.getInt(PORT.key());
        this.queueName = config.getString(QUEUE_NAME.key());
        if (config.hasPath(USERNAME.key())) {
            this.username = config.getString(USERNAME.key());
        }
        if (config.hasPath(PASSWORD.key())) {
            this.password = config.getString(PASSWORD.key());
        }
        if (config.hasPath(VIRTUAL_HOST.key())) {
            this.virtualHost = config.getString(VIRTUAL_HOST.key());
        }

        if (config.hasPath(RabbitmqBaseOptions.NETWORK_RECOVERY_INTERVAL.key())) {
            this.networkRecoveryInterval =
                    config.getInt(RabbitmqBaseOptions.NETWORK_RECOVERY_INTERVAL.key());
        }
        if (config.hasPath(RabbitmqBaseOptions.AUTOMATIC_RECOVERY_ENABLED.key())) {
            this.automaticRecovery =
                    config.getBoolean(RabbitmqBaseOptions.AUTOMATIC_RECOVERY_ENABLED.key());
        }
        if (config.hasPath(RabbitmqBaseOptions.TOPOLOGY_RECOVERY_ENABLED.key())) {
            this.topologyRecovery =
                    config.getBoolean(RabbitmqBaseOptions.TOPOLOGY_RECOVERY_ENABLED.key());
        }
        if (config.hasPath(RabbitmqBaseOptions.CONNECTION_TIMEOUT.key())) {
            this.connectionTimeout = config.getInt(RabbitmqBaseOptions.CONNECTION_TIMEOUT.key());
        }

        if (config.hasPath(RabbitmqBaseOptions.ROUTING_KEY.key())) {
            this.routingKey = config.getString(RabbitmqBaseOptions.ROUTING_KEY.key());
        }
        if (config.hasPath(RabbitmqBaseOptions.EXCHANGE.key())) {
            this.exchange = config.getString(RabbitmqBaseOptions.EXCHANGE.key());
        }
        if (config.hasPath(RabbitmqBaseOptions.FOR_E2E_TESTING.key())) {
            this.forE2ETesting = config.getBoolean(RabbitmqBaseOptions.FOR_E2E_TESTING.key());
        }
        if (config.hasPath(RabbitmqBaseOptions.USE_CORRELATION_ID.key())) {
            this.usesCorrelationId =
                    config.getBoolean(RabbitmqBaseOptions.USE_CORRELATION_ID.key());
        }
        if (config.hasPath(RabbitmqBaseOptions.DURABLE.key())) {
            this.durable = config.getBoolean(RabbitmqBaseOptions.DURABLE.key());
        }
        if (config.hasPath(RabbitmqBaseOptions.EXCLUSIVE.key())) {
            this.exclusive = config.getBoolean(RabbitmqBaseOptions.EXCLUSIVE.key());
        }
        if (config.hasPath(RabbitmqBaseOptions.PREFETCH_COUNT.key())) {
            this.prefetchCount = config.getInt(RabbitmqBaseOptions.PREFETCH_COUNT.key());
        }

        if (config.hasPath(RabbitmqSourceOptions.REQUESTED_CHANNEL_MAX.key())) {
            this.requestedChannelMax =
                    config.getInt(RabbitmqSourceOptions.REQUESTED_CHANNEL_MAX.key());
        }
        if (config.hasPath(RabbitmqSourceOptions.REQUESTED_FRAME_MAX.key())) {
            this.requestedFrameMax = config.getInt(RabbitmqSourceOptions.REQUESTED_FRAME_MAX.key());
        }
        if (config.hasPath(RabbitmqSourceOptions.REQUESTED_HEARTBEAT.key())) {
            this.requestedHeartbeat =
                    config.getInt(RabbitmqSourceOptions.REQUESTED_HEARTBEAT.key());
        }

        if (config.hasPath(RabbitmqSourceOptions.DELIVERY_TIMEOUT.key())) {
            this.deliveryTimeout = config.getInt(RabbitmqSourceOptions.DELIVERY_TIMEOUT.key());
        }

        parseSinkOptionProperties(config);
    }

    @VisibleForTesting
    public RabbitmqConfig() {}
}
