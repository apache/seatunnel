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
    private final SerializationSchema serializationSchema;
    private MqttClient mqttClient;

    public MqttSinkWriter(
            SinkWriter.Context context, SeaTunnelRowType rowType, ReadonlyConfig pluginConfig) {
        this.topic = pluginConfig.get(MqttSinkFactory.TOPIC);
        this.qos = pluginConfig.get(MqttSinkFactory.QOS);
        this.retryTimeoutMs = pluginConfig.get(MqttSinkFactory.RETRY_TIMEOUT);
        this.serializationSchema = createSerializationSchema(rowType, pluginConfig);

        // Each subtask appends its index to guarantee a unique MQTT client ID,
        // preventing connection hijacking when running parallel tasks.
        String clientId = CLIENT_ID_PREFIX + context.getIndexOfSubtask();

        try {
            // MemoryPersistence avoids file-system I/O; ideal for containerized deployments.
            this.mqttClient =
                    new MqttClient(
                            pluginConfig.get(MqttSinkFactory.URL),
                            clientId,
                            new MemoryPersistence());
            this.mqttClient.setCallback(this);

            MqttConnectOptions options = buildConnectOptions(pluginConfig);
            this.mqttClient.connect(options);
            log.info(
                    "MQTT sink writer [{}] connected to {}",
                    clientId,
                    pluginConfig.get(MqttSinkFactory.URL));
        } catch (MqttException e) {
            throw new RuntimeException("Failed to connect MQTT client [" + clientId + "]", e);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        byte[] payload = serializationSchema.serialize(element);
        MqttMessage message = new MqttMessage(payload);
        message.setQos(qos);

        // Localized retry loop with backoff to isolate the pipeline from transient
        // network disruptions. Polls isConnected() to let auto-reconnect recover.
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
                "Failed to publish MQTT message after " + retryTimeoutMs + "ms", lastException);
    }

    @Override
    public Optional<Void> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
        // Stateless sink — nothing to roll back.
    }

    @Override
    public void close() throws IOException {
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

    private static MqttConnectOptions buildConnectOptions(ReadonlyConfig config) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(config.get(MqttSinkFactory.CONNECTION_TIMEOUT));

        String username = config.get(MqttSinkFactory.USERNAME);
        if (username != null && !username.isEmpty()) {
            options.setUserName(username);
        }
        String password = config.get(MqttSinkFactory.PASSWORD);
        if (password != null && !password.isEmpty()) {
            options.setPassword(password.toCharArray());
        }
        return options;
    }

    private static SerializationSchema createSerializationSchema(
            SeaTunnelRowType rowType, ReadonlyConfig config) {
        String format = config.get(MqttSinkFactory.FORMAT);
        switch (format.toLowerCase()) {
            case "json":
                return new JsonSerializationSchema(rowType);
            case "text":
                return TextSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(",")
                        .build();
            default:
                throw new IllegalArgumentException("Unsupported MQTT sink format: " + format);
        }
    }
}
