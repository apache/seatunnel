/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.mqtt.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mqtt.exception.MqttConnectorException;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MqttSinkWriterTest {

    @Mock private SinkWriter.Context context;

    private SeaTunnelRowType rowType;
    private ReadonlyConfig validConfig;

    @BeforeEach
    void setUp() {
        rowType =
                new SeaTunnelRowType(
                        new String[] {"field1"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "tcp://localhost:1883");
        configMap.put("topic", "test");
        configMap.put("qos", 1);
        validConfig = ReadonlyConfig.fromMap(configMap);
    }

    @Test
    void testInvalidQosThrowsException() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "tcp://localhost:1883");
        configMap.put("topic", "test");
        configMap.put("qos", 2); // Invalid value

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new MqttSinkWriter(context, rowType, config));

        Assertions.assertTrue(ex.getMessage().contains("MQTT QoS must be 0"));
    }

    @Test
    void testInvalidFormatThrowsException() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "tcp://localhost:1883");
        configMap.put("topic", "test");
        configMap.put("format", "xml"); // Invalid format

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new MqttSinkWriter(context, rowType, config));
    }

    @Test
    void testConnectionFailureThrowsWrappedException() {
        try (MockedConstruction<MqttClient> mocked =
                Mockito.mockConstruction(
                        MqttClient.class,
                        (mock, ctx) -> {
                            doThrow(
                                            new MqttException(
                                                    MqttException.REASON_CODE_SERVER_CONNECT_ERROR))
                                    .when(mock)
                                    .connect(any(MqttConnectOptions.class));
                        })) {
            MqttConnectorException ex =
                    Assertions.assertThrows(
                            MqttConnectorException.class,
                            () -> new MqttSinkWriter(context, rowType, validConfig));

            Assertions.assertEquals("MQTT-01", ex.getSeaTunnelErrorCode().getCode());
        }
    }

    @Test
    void testWriteWithRetrySuccess() throws Exception {
        try (MockedConstruction<MqttClient> mocked = Mockito.mockConstruction(MqttClient.class)) {
            MqttSinkWriter writer = new MqttSinkWriter(context, rowType, validConfig);
            MqttClient mockClient = mocked.constructed().get(0);

            when(mockClient.isConnected()).thenReturn(true);
            doThrow(new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT))
                    .doNothing()
                    .when(mockClient)
                    .publish(anyString(), any(MqttMessage.class));

            SeaTunnelRow mockRow = Mockito.mock(SeaTunnelRow.class);
            writer.write(mockRow);
            // Default batch_size=1, so write triggers immediate flush
            verify(mockClient, times(2)).publish(anyString(), any(MqttMessage.class));
        }
    }

    @Test
    void testBatchWriteFlushesOnThreshold() throws Exception {
        Map<String, Object> batchConfig = new HashMap<>();
        batchConfig.put("url", "tcp://localhost:1883");
        batchConfig.put("topic", "test");
        batchConfig.put("qos", 1);
        batchConfig.put("batch_size", 3);
        ReadonlyConfig config = ReadonlyConfig.fromMap(batchConfig);

        try (MockedConstruction<MqttClient> mocked = Mockito.mockConstruction(MqttClient.class)) {
            MqttSinkWriter writer = new MqttSinkWriter(context, rowType, config);
            MqttClient mockClient = mocked.constructed().get(0);

            when(mockClient.isConnected()).thenReturn(true);

            // Write 2 rows — below batch threshold, no publish yet
            writer.write(new SeaTunnelRow(new Object[] {"a"}));
            writer.write(new SeaTunnelRow(new Object[] {"b"}));
            verify(mockClient, times(0)).publish(anyString(), any(MqttMessage.class));

            // 3rd write reaches threshold, all 3 flushed
            writer.write(new SeaTunnelRow(new Object[] {"c"}));
            verify(mockClient, times(3)).publish(anyString(), any(MqttMessage.class));
        }
    }

    @Test
    void testPrepareCommitFlushesBuffer() throws Exception {
        Map<String, Object> batchConfig = new HashMap<>();
        batchConfig.put("url", "tcp://localhost:1883");
        batchConfig.put("topic", "test");
        batchConfig.put("qos", 1);
        batchConfig.put("batch_size", 10);
        ReadonlyConfig config = ReadonlyConfig.fromMap(batchConfig);

        try (MockedConstruction<MqttClient> mocked = Mockito.mockConstruction(MqttClient.class)) {
            MqttSinkWriter writer = new MqttSinkWriter(context, rowType, config);
            MqttClient mockClient = mocked.constructed().get(0);

            when(mockClient.isConnected()).thenReturn(true);

            writer.write(new SeaTunnelRow(new Object[] {"a"}));
            writer.write(new SeaTunnelRow(new Object[] {"b"}));
            verify(mockClient, times(0)).publish(anyString(), any(MqttMessage.class));

            // prepareCommit forces flush of remaining buffered messages
            writer.prepareCommit();
            verify(mockClient, times(2)).publish(anyString(), any(MqttMessage.class));
        }
    }

    @Test
    void testWriteTimeoutAfterRetries() throws Exception {
        Map<String, Object> shortTimeoutConfig = new HashMap<>();
        shortTimeoutConfig.put("url", "tcp://localhost:1883");
        shortTimeoutConfig.put("topic", "test");
        shortTimeoutConfig.put("qos", 1);
        shortTimeoutConfig.put("retry_timeout", 500);
        ReadonlyConfig config = ReadonlyConfig.fromMap(shortTimeoutConfig);

        try (MockedConstruction<MqttClient> mocked = Mockito.mockConstruction(MqttClient.class)) {
            MqttSinkWriter writer = new MqttSinkWriter(context, rowType, config);
            MqttClient mockClient = mocked.constructed().get(0);

            when(mockClient.isConnected()).thenReturn(true);
            doThrow(new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT))
                    .when(mockClient)
                    .publish(anyString(), any(MqttMessage.class));

            SeaTunnelRow mockRow = Mockito.mock(SeaTunnelRow.class);

            Assertions.assertThrows(IOException.class, () -> writer.write(mockRow));
        }
    }

    @Test
    void testCustomFieldDelimiter() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "tcp://localhost:1883");
        configMap.put("topic", "test");
        configMap.put("format", "text");
        configMap.put("field_delimiter", "|");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        try (MockedConstruction<MqttClient> mocked = Mockito.mockConstruction(MqttClient.class)) {
            MqttSinkWriter writer = new MqttSinkWriter(context, rowType, config);
            MqttClient mockClient = mocked.constructed().get(0);

            when(mockClient.isConnected()).thenReturn(true);

            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"hello"});
            writer.write(row);

            verify(mockClient).publish(anyString(), any(MqttMessage.class));
        }
    }
}
