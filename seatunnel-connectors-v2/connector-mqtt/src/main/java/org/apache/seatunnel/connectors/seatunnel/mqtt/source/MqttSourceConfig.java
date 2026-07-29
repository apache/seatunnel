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

package org.apache.seatunnel.connectors.seatunnel.mqtt.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.util.UUID;

public class MqttSourceConfig {

    private static final String CLIENT_ID_PREFIX = "seatunnel_mqtt_source_";

    private final String url;
    private final String topic;
    private final String username;
    private final String password;
    private final int qos;
    private final String format;
    private final String fieldDelimiter;
    private final String clientId;
    private final boolean cleanSession;
    private final int connectionTimeout;
    private final int keepAliveInterval;
    private final int reconnectTimeout;
    private final int maxQueueSize;

    public MqttSourceConfig(ReadonlyConfig config) {
        this.url = config.get(MqttSourceOptions.URL);
        this.topic = config.get(MqttSourceOptions.TOPIC);
        this.username = config.get(MqttSourceOptions.USERNAME);
        this.password = config.get(MqttSourceOptions.PASSWORD);
        this.qos = config.get(MqttSourceOptions.QOS);
        this.format = config.get(MqttSourceOptions.FORMAT);
        this.fieldDelimiter = config.get(MqttSourceOptions.FIELD_DELIMITER);
        this.cleanSession = config.get(MqttSourceOptions.CLEAN_SESSION);
        this.connectionTimeout = config.get(MqttSourceOptions.CONNECTION_TIMEOUT);
        this.keepAliveInterval = config.get(MqttSourceOptions.KEEP_ALIVE_INTERVAL);
        this.reconnectTimeout = config.get(MqttSourceOptions.RECONNECT_TIMEOUT);
        this.maxQueueSize = config.get(MqttSourceOptions.MAX_QUEUE_SIZE);

        String configuredClientId = config.get(MqttSourceOptions.CLIENT_ID);
        if (!cleanSession && isBlank(configuredClientId)) {
            throw new IllegalArgumentException(
                    "client_id is required when clean_session=false for MQTT source");
        }
        this.clientId =
                isBlank(configuredClientId)
                        ? CLIENT_ID_PREFIX + UUID.randomUUID().toString()
                        : configuredClientId;

        validate();
    }

    private void validate() {
        if (qos < 0 || qos > 1) {
            throw new IllegalArgumentException("MQTT source qos must be 0 or 1, got: " + qos);
        }
        if (!"json".equalsIgnoreCase(format) && !"text".equalsIgnoreCase(format)) {
            throw new IllegalArgumentException("Unsupported MQTT source format: " + format);
        }
        if (reconnectTimeout <= 0) {
            throw new IllegalArgumentException(
                    "reconnect_timeout must be greater than 0, got: " + reconnectTimeout);
        }
        if (maxQueueSize <= 0) {
            throw new IllegalArgumentException(
                    "max_queue_size must be greater than 0, got: " + maxQueueSize);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public String getUrl() {
        return url;
    }

    public String getTopic() {
        return topic;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getQos() {
        return qos;
    }

    public String getFormat() {
        return format;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public String getClientId() {
        return clientId;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public int getReconnectTimeout() {
        return reconnectTimeout;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }
}
