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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
@AllArgsConstructor
public class RabbitmqConfig implements Serializable {
    private String host;
    private Integer port;
    private String virtualHost;
    private String username;
    private String password;
    private String uri;
    private Integer networkRecoveryInterval;
    private Boolean automaticRecovery;
    private Boolean topologyRecovery;
    private Integer connectionTimeout;
    private Integer requestedChannelMax;
    private Integer requestedFrameMax;
    private Integer requestedHeartbeat;
    private Integer prefetchCount;
    private  long deliveryTimeout;
    private String queueName;
    private String routingKey;
    private boolean logFailuresOnly = false;
    private String exchange = "";

    private boolean forE2ETesting = false;

    public static final String RABBITMQ_SINK_CONFIG_PREFIX = "rabbitmq.properties.";

    private final Map<String, Object> sinkOptionProps = new HashMap<>();

    public static final Option<String> HOST = Options.key("host")
            .stringType()
            .noDefaultValue()
            .withDescription("the default host to use for connections");

    public static final Option<Integer> PORT = Options.key("port")
            .intType()
            .noDefaultValue()
            .withDescription("the default port to use for connections");

    public static final Option<String> VIRTUAL_HOST = Options.key("virtual_host")
            .stringType()
            .noDefaultValue()
            .withDescription("the virtual host to use when connecting to the broker");

    public static final Option<String> USERNAME = Options.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("the AMQP user name to use when connecting to the broker");

    public static final Option<String> PASSWORD = Options.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("the password to use when connecting to the broker");


    public static final Option<String> QUEUE_NAME = Options.key("queue_name")
            .stringType()
            .noDefaultValue()
            .withDescription("the queue to write the message to");

    public static final Option<String> URL = Options.key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("convenience method for setting the fields in an AMQP URI: host, port, username, password and virtual host");

    public static final Option<Integer> NETWORK_RECOVERY_INTERVAL = Options.key("network_recovery_interval")
            .intType()
            .noDefaultValue()
            .withDescription("how long will automatic recovery wait before attempting to reconnect, in ms");

    public static final Option<Boolean> AUTOMATIC_RECOVERY_ENABLED = Options.key("AUTOMATIC_RECOVERY_ENABLED")
            .booleanType()
            .noDefaultValue()
            .withDescription("if true, enables connection recovery");

    public static final Option<Boolean> TOPOLOGY_RECOVERY_ENABLED = Options.key("topology_recovery_enabled")
            .booleanType()
            .noDefaultValue()
            .withDescription("if true, enables topology recovery");

    public static final Option<Integer> CONNECTION_TIMEOUT = Options.key("connection_timeout")
            .intType()
            .noDefaultValue()
            .withDescription("connection TCP establishment timeout in milliseconds");


    public static final Option<Integer> REQUESTED_CHANNEL_MAX = Options.key("requested_channel_max")
            .intType()
            .noDefaultValue()
            .withDescription("initially requested maximum channel number");

    public static final Option<Integer> REQUESTED_FRAME_MAX = Options.key("requested_frame_max")
            .intType()
            .noDefaultValue()
            .withDescription("the requested maximum frame size");

    public static final Option<Integer> REQUESTED_HEARTBEAT = Options.key("requested_heartbeat")
            .intType()
            .noDefaultValue()
            .withDescription("the requested heartbeat timeout");

    public static final Option<Long> PREFETCH_COUNT = Options.key("prefetch_count")
            .longType()
            .noDefaultValue()
            .withDescription("prefetchCount the max number of messages to receive without acknowledgement\n");

    public static final Option<Integer> DELIVERY_TIMEOUT = Options.key("delivery_timeout")
            .intType()
            .noDefaultValue()
            .withDescription("deliveryTimeout maximum wait time");

    public static final Option<String> ROUTING_KEY = Options.key("routing_key")
            .stringType()
            .noDefaultValue()
            .withDescription("the routing key to publish the message to");

    public static final Option<String> EXCHANGE = Options.key("exchange")
            .stringType()
            .noDefaultValue()
            .withDescription("the exchange to publish the message to");

    public static final Option<Boolean> FOR_E2E_TESTING = Options.key("for_e2e_testing")
            .booleanType()
            .noDefaultValue()
            .withDescription("use to recognize E2E mode");

    private void parseSinkOptionProperties(Config pluginConfig) {
        Config sinkOptionConfig = TypesafeConfigUtils.extractSubConfig(pluginConfig,
                RABBITMQ_SINK_CONFIG_PREFIX, false);
        sinkOptionConfig.entrySet().forEach(entry -> {
            final String configKey = entry.getKey().toLowerCase();
            this.sinkOptionProps.put(configKey, entry.getValue().unwrapped());
        });
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
        if (config.hasPath(NETWORK_RECOVERY_INTERVAL.key())) {
            this.networkRecoveryInterval = config.getInt(NETWORK_RECOVERY_INTERVAL.key());
        }
        if (config.hasPath(AUTOMATIC_RECOVERY_ENABLED.key())) {
            this.automaticRecovery = config.getBoolean(AUTOMATIC_RECOVERY_ENABLED.key());
        }
        if (config.hasPath(TOPOLOGY_RECOVERY_ENABLED.key())) {
            this.topologyRecovery = config.getBoolean(TOPOLOGY_RECOVERY_ENABLED.key());
        }
        if (config.hasPath(CONNECTION_TIMEOUT.key())) {
            this.connectionTimeout = config.getInt(CONNECTION_TIMEOUT.key());
        }
        if (config.hasPath(REQUESTED_CHANNEL_MAX.key())) {
            this.requestedChannelMax = config.getInt(REQUESTED_CHANNEL_MAX.key());
        }
        if (config.hasPath(REQUESTED_FRAME_MAX.key())) {
            this.requestedFrameMax = config.getInt(REQUESTED_FRAME_MAX.key());
        }
        if (config.hasPath(REQUESTED_HEARTBEAT.key())) {
            this.requestedHeartbeat = config.getInt(REQUESTED_HEARTBEAT.key());
        }
        if (config.hasPath(PREFETCH_COUNT.key())) {
            this.prefetchCount = config.getInt(PREFETCH_COUNT.key());
        }
        if (config.hasPath(DELIVERY_TIMEOUT.key())) {
            this.deliveryTimeout = config.getInt(DELIVERY_TIMEOUT.key());
        }
        if (config.hasPath(ROUTING_KEY.key())) {
            this.routingKey = config.getString(ROUTING_KEY.key());
        }
        if (config.hasPath(EXCHANGE.key())) {
            this.exchange = config.getString(EXCHANGE.key());
        }
        if (config.hasPath(FOR_E2E_TESTING.key())) {
            this.forE2ETesting = config.getBoolean(FOR_E2E_TESTING.key());
        }
        parseSinkOptionProperties(config);
    }

    @VisibleForTesting
    public RabbitmqConfig() {

    }
}
