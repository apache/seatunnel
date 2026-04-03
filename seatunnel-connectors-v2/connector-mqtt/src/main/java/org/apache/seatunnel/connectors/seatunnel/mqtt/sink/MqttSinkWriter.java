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

package org.apache.seatunnel.connectors.seatunnel.mqtt.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mqtt.exception.MqttConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.mqtt.exception.MqttConnectorException;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * MQTT sink writer that publishes each {@link SeaTunnelRow} as an MQTT message. Uses Eclipse Paho
 * with in-memory persistence to avoid container disk I/O. Each parallel subtask gets a unique
 * client ID to prevent connection conflicts.
 */
@Slf4j
public class MqttSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void>, MqttCallback {

    private static final String CLIENT_ID_PREFIX = "seatunnel_mqtt_sink_task_";
    private static final long RETRY_BACKOFF_MS = 200L;

    private final String topic;
    private final int qos;
    private final int retryTimeoutMs;
    private final int batchSize;
    private final SerializationSchema serializationSchema;
    private final List<MqttMessage> messageBuffer;
    private MqttClient mqttClient;

    public MqttSinkWriter(
            SinkWriter.Context context, SeaTunnelRowType rowType, ReadonlyConfig pluginConfig) {
        this.topic = pluginConfig.get(MqttSinkOptions.TOPIC);
        this.qos = pluginConfig.get(MqttSinkOptions.QOS);
        if (this.qos < 0 || this.qos > 1) {
            throw new IllegalArgumentException(
                    "MQTT QoS must be 0 (at-most-once) or 1 (at-least-once), got: " + this.qos);
        }
        this.retryTimeoutMs = pluginConfig.get(MqttSinkOptions.RETRY_TIMEOUT);
        this.batchSize = pluginConfig.get(MqttSinkOptions.BATCH_SIZE);
        if (this.batchSize < 1) {
            throw new IllegalArgumentException("batch_size must be >= 1, got: " + this.batchSize);
        }
        this.messageBuffer = new ArrayList<>(this.batchSize);
        this.serializationSchema = createSerializationSchema(rowType, pluginConfig);

        // Each subtask appends its index and a random UUID to guarantee a globally unique client
        // ID,
        // preventing mutual disconnections and connection hijacking when running parallel jobs.
        String clientId =
                CLIENT_ID_PREFIX
                        + context.getIndexOfSubtask()
                        + "-"
                        + java.util.UUID.randomUUID().toString();

        try {
            // MemoryPersistence avoids file-system I/O; ideal for containerized deployments.
            this.mqttClient =
                    new MqttClient(
                            pluginConfig.get(MqttSinkOptions.URL),
                            clientId,
                            new MemoryPersistence());
            this.mqttClient.setCallback(this);

            MqttConnectOptions options = buildConnectOptions(pluginConfig);
            this.mqttClient.connect(options);
            log.info(
                    "MQTT sink writer [{}] connected to {}",
                    clientId,
                    pluginConfig.get(MqttSinkOptions.URL));
        } catch (MqttException e) {
            if (this.mqttClient != null) {
                try {
                    this.mqttClient.close();
                } catch (MqttException ignored) {
                    // Best-effort cleanup; the original exception is more important.
                }
            }
            throw new MqttConnectorException(
                    MqttConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to connect MQTT client [" + clientId + "]",
                    e);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        byte[] payload = serializationSchema.serialize(element);
        MqttMessage message = new MqttMessage(payload);
        message.setQos(qos);

        messageBuffer.add(message);
        if (messageBuffer.size() >= batchSize) {
            flushBuffer();
        }
    }

    @Override
    public Optional<Void> prepareCommit() throws IOException {
        flushBuffer();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
        // Stateless sink — nothing to roll back.
    }

    @Override
    public void close() throws IOException {
        try {
            flushBuffer();
        } finally {
            if (mqttClient != null) {
                try {
                    if (mqttClient.isConnected()) {
                        mqttClient.disconnect();
                    }
                    mqttClient.close();
                    log.info("MQTT sink writer closed");
                } catch (MqttException e) {
                    throw new IOException("Error closing MQTT client", e);
                }
            }
        }
    }

    // ---- MqttCallback implementation ----

    @Override
    public void connectionLost(Throwable cause) {
        // Auto-reconnect is enabled; log for observability but do not throw.
        log.warn("MQTT connection lost, auto-reconnect will attempt recovery", cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        // Sink-only client — inbound messages are not expected.
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // QoS acknowledgement received from broker.
    }

    // ---- private helpers ----

    private void flushBuffer() throws IOException {
        if (messageBuffer.isEmpty()) {
            return;
        }
        for (MqttMessage message : messageBuffer) {
            publishWithRetry(message);
        }
        messageBuffer.clear();
    }

    private void publishWithRetry(MqttMessage message) throws IOException {
        long deadline = System.currentTimeMillis() + retryTimeoutMs;
        MqttException lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (mqttClient.isConnected()) {
                    mqttClient.publish(topic, message);
                    return;
                }
            } catch (MqttException e) {
                lastException = e;
                log.warn("Transient MQTT publish failure, retrying...", e);
            }
            try {
                Thread.sleep(RETRY_BACKOFF_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during MQTT publish retry", ie);
            }
        }
        throw new IOException(
                new MqttConnectorException(
                                MqttConnectorErrorCode.PUBLISH_FAILED,
                                "Failed to publish MQTT message after " + retryTimeoutMs + "ms")
                        .getMessage(),
                lastException);
    }

    private static MqttConnectOptions buildConnectOptions(ReadonlyConfig config) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        boolean cleanSession = config.get(MqttSinkOptions.CLEAN_SESSION);
        options.setCleanSession(cleanSession);
        if (!cleanSession) {
            log.warn(
                    "clean_session=false may cause broker-side state accumulation. Ensure proper clientId management.");
        }
        options.setConnectionTimeout(config.get(MqttSinkOptions.CONNECTION_TIMEOUT));

        String username = config.get(MqttSinkOptions.USERNAME);
        if (username != null && !username.isEmpty()) {
            options.setUserName(username);
        }
        String password = config.get(MqttSinkOptions.PASSWORD);
        if (password != null && !password.isEmpty()) {
            options.setPassword(password.toCharArray());
        }
        return options;
    }

    private static SerializationSchema createSerializationSchema(
            SeaTunnelRowType rowType, ReadonlyConfig config) {
        String format = config.get(MqttSinkOptions.FORMAT);
        switch (format.toLowerCase()) {
            case "json":
                return new JsonSerializationSchema(rowType);
            case "text":
                String delimiter = config.get(MqttSinkOptions.FIELD_DELIMITER);
                return TextSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(delimiter)
                        .build();
            default:
                throw new IllegalArgumentException("Unsupported MQTT sink format: " + format);
        }
    }
}
