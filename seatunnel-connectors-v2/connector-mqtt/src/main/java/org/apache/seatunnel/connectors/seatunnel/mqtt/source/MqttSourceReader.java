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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.mqtt.exception.MqttConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.mqtt.exception.MqttConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public class MqttSourceReader extends AbstractSingleSplitReader<SeaTunnelRow>
        implements MqttCallbackExtended {

    private static final Logger LOG = LoggerFactory.getLogger(MqttSourceReader.class);
    private static final long POLL_TIMEOUT_MS = 1000L;
    private static final long QUEUE_OFFER_TIMEOUT_MS = 1000L;

    private final MqttSourceConfig sourceConfig;
    private final CatalogTable catalogTable;
    private final BlockingQueue<byte[]> messageQueue;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final LongSupplier currentTimeMillis;

    private MqttClient mqttClient;
    private volatile Throwable receiveException;
    private volatile long disconnectedSinceMs = -1L;
    private volatile Throwable disconnectCause;

    public MqttSourceReader(MqttSourceConfig sourceConfig, CatalogTable catalogTable) {
        this(sourceConfig, catalogTable, System::currentTimeMillis);
    }

    MqttSourceReader(
            MqttSourceConfig sourceConfig,
            CatalogTable catalogTable,
            LongSupplier currentTimeMillis) {
        this.sourceConfig = sourceConfig;
        this.catalogTable = catalogTable;
        this.currentTimeMillis = currentTimeMillis;
        this.messageQueue = new LinkedBlockingQueue<>(sourceConfig.getMaxQueueSize());
        this.deserializationSchema = createDeserializationSchema(sourceConfig, catalogTable);
    }

    @Override
    public void open() {
        try {
            this.mqttClient =
                    new MqttClient(
                            sourceConfig.getUrl(),
                            sourceConfig.getClientId(),
                            new MemoryPersistence());
            this.mqttClient.setCallback(this);
            this.mqttClient.connect(buildConnectOptions(sourceConfig));
            subscribeTopic();
            LOG.info(
                    "MQTT source reader [{}] subscribed to topic [{}]",
                    sourceConfig.getClientId(),
                    sourceConfig.getTopic());
        } catch (MqttException e) {
            closeClientQuietly();
            throw new MqttConnectorException(
                    MqttConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to connect MQTT source client [" + sourceConfig.getClientId() + "]",
                    e);
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        checkReceiveException();
        checkReconnectTimeout();

        byte[] payload = messageQueue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (payload == null) {
            return;
        }

        SeaTunnelRow row = deserializationSchema.deserialize(payload);
        if (row == null) {
            return;
        }

        row.setTableId(catalogTable.getTablePath().toString());
        synchronized (output.getCheckpointLock()) {
            output.collect(row);
        }
    }

    @Override
    public void close() throws IOException {
        if (mqttClient == null) {
            return;
        }
        try {
            if (mqttClient.isConnected()) {
                if (sourceConfig.isCleanSession()) {
                    mqttClient.unsubscribe(sourceConfig.getTopic());
                }
                mqttClient.disconnect();
            } else {
                mqttClient.disconnectForcibly();
            }
            mqttClient.close();
            LOG.info("MQTT source reader [{}] closed", sourceConfig.getClientId());
        } catch (MqttException e) {
            throw new IOException("Error closing MQTT source client", e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        disconnectedSinceMs = currentTimeMillis.getAsLong();
        disconnectCause = cause;
        LOG.warn(
                "MQTT source connection lost for client [{}], auto-reconnect will attempt recovery",
                sourceConfig.getClientId(),
                cause);
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        if (!reconnect) {
            return;
        }
        try {
            subscribeTopic();
            disconnectedSinceMs = -1L;
            disconnectCause = null;
            LOG.info(
                    "MQTT source reader [{}] resubscribed to topic [{}] after reconnect to [{}]",
                    sourceConfig.getClientId(),
                    sourceConfig.getTopic(),
                    serverURI);
        } catch (MqttException e) {
            receiveException = e;
            LOG.error(
                    "Failed to resubscribe MQTT source reader [{}] to topic [{}] "
                            + "after reconnect to [{}]",
                    sourceConfig.getClientId(),
                    sourceConfig.getTopic(),
                    serverURI,
                    e);
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        if (message == null || message.getPayload() == null) {
            return;
        }
        byte[] payload = Arrays.copyOf(message.getPayload(), message.getPayload().length);
        try {
            if (!messageQueue.offer(payload, QUEUE_OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                MqttConnectorException exception =
                        new MqttConnectorException(
                                MqttConnectorErrorCode.RECEIVE_FAILED,
                                "MQTT source message queue is full. Increase max_queue_size "
                                        + "or reduce MQTT message throughput.");
                receiveException = exception;
                throw exception;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            receiveException = e;
            throw new MqttConnectorException(
                    MqttConnectorErrorCode.RECEIVE_FAILED,
                    "Interrupted while buffering MQTT source message",
                    e);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Source-only client — outbound delivery acknowledgements are not expected.
    }

    void subscribeTopic() throws MqttException {
        mqttClient.subscribe(sourceConfig.getTopic(), sourceConfig.getQos());
    }

    private void checkReceiveException() {
        if (receiveException == null) {
            return;
        }
        throw new MqttConnectorException(
                MqttConnectorErrorCode.RECEIVE_FAILED,
                "Failed to receive MQTT source messages from topic ["
                        + sourceConfig.getTopic()
                        + "] with client ["
                        + sourceConfig.getClientId()
                        + "]",
                receiveException);
    }

    private void checkReconnectTimeout() {
        long disconnectedAt = disconnectedSinceMs;
        if (disconnectedAt < 0) {
            return;
        }
        long elapsedMs = currentTimeMillis.getAsLong() - disconnectedAt;
        long timeoutMs = TimeUnit.SECONDS.toMillis(sourceConfig.getReconnectTimeout());
        if (elapsedMs < timeoutMs) {
            return;
        }

        MqttConnectorException exception =
                new MqttConnectorException(
                        MqttConnectorErrorCode.RECEIVE_FAILED,
                        "MQTT source client ["
                                + sourceConfig.getClientId()
                                + "] remained disconnected from topic ["
                                + sourceConfig.getTopic()
                                + "] for more than reconnect_timeout="
                                + sourceConfig.getReconnectTimeout()
                                + " seconds",
                        disconnectCause);
        receiveException = exception;
        throw exception;
    }

    private static MqttConnectOptions buildConnectOptions(MqttSourceConfig sourceConfig) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(sourceConfig.isCleanSession());
        options.setConnectionTimeout(sourceConfig.getConnectionTimeout());
        options.setKeepAliveInterval(sourceConfig.getKeepAliveInterval());

        String username = sourceConfig.getUsername();
        if (username != null && !username.isEmpty()) {
            options.setUserName(username);
        }
        String password = sourceConfig.getPassword();
        if (password != null && !password.isEmpty()) {
            options.setPassword(password.toCharArray());
        }
        return options;
    }

    private static DeserializationSchema<SeaTunnelRow> createDeserializationSchema(
            MqttSourceConfig sourceConfig, CatalogTable catalogTable) {
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        switch (sourceConfig.getFormat().toLowerCase()) {
            case "json":
                return new JsonDeserializationSchema(catalogTable, false, false);
            case "text":
                return TextDeserializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(sourceConfig.getFieldDelimiter())
                        .setCatalogTable(catalogTable)
                        .build();
            default:
                throw new IllegalArgumentException(
                        "Unsupported MQTT source format: " + sourceConfig.getFormat());
        }
    }

    private void closeClientQuietly() {
        if (mqttClient == null) {
            return;
        }
        try {
            mqttClient.close();
        } catch (MqttException ignored) {
            // Best-effort cleanup; the original connection exception is more important.
        }
    }
}
