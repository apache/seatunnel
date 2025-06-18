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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
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
    private long deliveryTimeout;
    private String queueName;
    private Boolean durable;
    private Boolean exclusive;
    private Boolean autoDelete;
    private String routingKey;
    private boolean logFailuresOnly = false;
    private String exchange = "";

    private boolean forE2ETesting = false;
    private boolean usesCorrelationId = false;

    private Map<String, String> sinkOptionProps = new HashMap<>();

    public RabbitmqConfig(ReadonlyConfig config) {
        this.host = config.get(RabbitmqBaseOptions.HOST);
        this.port = config.get(RabbitmqBaseOptions.PORT);
        this.queueName = config.get(RabbitmqBaseOptions.QUEUE_NAME);
        if (config.getOptional(RabbitmqBaseOptions.USERNAME).isPresent()) {
            this.username = config.get(RabbitmqBaseOptions.USERNAME);
        }
        if (config.getOptional(RabbitmqBaseOptions.PASSWORD).isPresent()) {
            this.password = config.get(RabbitmqBaseOptions.PASSWORD);
        }
        if (config.getOptional(RabbitmqBaseOptions.VIRTUAL_HOST).isPresent()) {
            this.virtualHost = config.get(RabbitmqBaseOptions.VIRTUAL_HOST);
        }
        if (config.getOptional(RabbitmqBaseOptions.NETWORK_RECOVERY_INTERVAL).isPresent()) {
            this.networkRecoveryInterval =
                    config.get(RabbitmqBaseOptions.NETWORK_RECOVERY_INTERVAL);
        }
        if (config.getOptional(RabbitmqBaseOptions.AUTOMATIC_RECOVERY_ENABLED).isPresent()) {
            this.automaticRecovery = config.get(RabbitmqBaseOptions.AUTOMATIC_RECOVERY_ENABLED);
        }
        if (config.getOptional(RabbitmqBaseOptions.TOPOLOGY_RECOVERY_ENABLED).isPresent()) {
            this.topologyRecovery = config.get(RabbitmqBaseOptions.TOPOLOGY_RECOVERY_ENABLED);
        }
        if (config.getOptional(RabbitmqBaseOptions.CONNECTION_TIMEOUT).isPresent()) {
            this.connectionTimeout = config.get(RabbitmqBaseOptions.CONNECTION_TIMEOUT);
        }
        if (config.getOptional(RabbitmqSourceOptions.REQUESTED_CHANNEL_MAX).isPresent()) {
            this.requestedChannelMax = config.get(RabbitmqSourceOptions.REQUESTED_CHANNEL_MAX);
        }
        if (config.getOptional(RabbitmqSourceOptions.REQUESTED_FRAME_MAX).isPresent()) {
            this.requestedFrameMax = config.get(RabbitmqSourceOptions.REQUESTED_FRAME_MAX);
        }
        if (config.getOptional(RabbitmqSourceOptions.REQUESTED_HEARTBEAT).isPresent()) {
            this.requestedHeartbeat = config.get(RabbitmqSourceOptions.REQUESTED_HEARTBEAT);
        }
        if (config.getOptional(RabbitmqSourceOptions.PREFETCH_COUNT).isPresent()) {
            this.prefetchCount = config.get(RabbitmqSourceOptions.PREFETCH_COUNT);
        }
        if (config.getOptional(RabbitmqSourceOptions.DELIVERY_TIMEOUT).isPresent()) {
            this.deliveryTimeout = config.get(RabbitmqSourceOptions.DELIVERY_TIMEOUT);
        }
        if (config.getOptional(RabbitmqBaseOptions.ROUTING_KEY).isPresent()) {
            this.routingKey = config.get(RabbitmqBaseOptions.ROUTING_KEY);
        }
        if (config.getOptional(RabbitmqBaseOptions.EXCHANGE).isPresent()) {
            this.exchange = config.get(RabbitmqBaseOptions.EXCHANGE);
        }
        if (config.getOptional(RabbitmqBaseOptions.FOR_E2E_TESTING).isPresent()) {
            this.forE2ETesting = config.get(RabbitmqBaseOptions.FOR_E2E_TESTING);
        }
        if (config.getOptional(RabbitmqSourceOptions.USE_CORRELATION_ID).isPresent()) {
            this.usesCorrelationId = config.get(RabbitmqSourceOptions.USE_CORRELATION_ID);
        }
        if (config.getOptional(RabbitmqBaseOptions.DURABLE).isPresent()) {
            this.durable = config.get(RabbitmqBaseOptions.DURABLE);
        }
        if (config.getOptional(RabbitmqBaseOptions.EXCLUSIVE).isPresent()) {
            this.exclusive = config.get(RabbitmqBaseOptions.EXCLUSIVE);
        }
        if (config.getOptional(RabbitmqBaseOptions.AUTO_DELETE).isPresent()) {
            this.autoDelete = config.get(RabbitmqBaseOptions.AUTO_DELETE);
        }
        this.sinkOptionProps = config.get(RabbitmqSinkOptions.RABBITMQ_CONFIG);
    }
}
