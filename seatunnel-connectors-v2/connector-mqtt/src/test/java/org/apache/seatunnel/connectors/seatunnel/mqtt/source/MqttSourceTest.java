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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.mqtt.exception.MqttConnectorException;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class MqttSourceTest {

    @Test
    void testSourceMetadataAndBoundedness() {
        MqttSource source = new MqttSource(ReadonlyConfig.fromMap(baseConfig()));

        Assertions.assertEquals(MqttSourceOptions.CONNECTOR_IDENTITY, source.getPluginName());
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
    }

    @Test
    void testStreamingJobModeIsAllowed() {
        MqttSource source = new MqttSource(ReadonlyConfig.fromMap(baseConfig()));

        source.setJobContext(new JobContext().setJobMode(JobMode.STREAMING));

        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
    }

    @Test
    void testBatchJobModeFails() {
        MqttSource source = new MqttSource(ReadonlyConfig.fromMap(baseConfig()));

        source.setJobContext(new JobContext().setJobMode(JobMode.BATCH));

        Assertions.assertThrows(MqttConnectorException.class, source::getBoundedness);
    }

    @Test
    void testProducedCatalogTables() {
        MqttSource source = new MqttSource(ReadonlyConfig.fromMap(baseConfig()));

        List<CatalogTable> catalogTables = source.getProducedCatalogTables();

        Assertions.assertEquals(1, catalogTables.size());
        Assertions.assertArrayEquals(
                new String[] {"id"}, catalogTables.get(0).getTableSchema().getFieldNames());
    }

    @Test
    void testCreateReader() throws Exception {
        MqttSource source = new MqttSource(ReadonlyConfig.fromMap(baseConfig()));

        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        AbstractSingleSplitReader<?> reader = source.createReader(readerContext);

        Assertions.assertInstanceOf(MqttSourceReader.class, reader);
    }

    @Test
    void testReaderCollectsArrivedJsonMessage() throws Exception {
        MqttSource source = new MqttSource(ReadonlyConfig.fromMap(baseConfig()));
        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        MqttSourceReader reader = (MqttSourceReader) source.createReader(readerContext);
        RecordingCollector collector = new RecordingCollector();

        reader.messageArrived("users", mqttMessage("{\"id\":1}"));
        reader.pollNext(collector);

        Assertions.assertNotNull(collector.record);
        Assertions.assertEquals(1, collector.record.getField(0));
        Assertions.assertEquals(
                source.getProducedCatalogTables().get(0).getTablePath().toString(),
                collector.record.getTableId());
    }

    @Test
    void testReaderFailsWhenReconnectTimeoutExceeded() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put("reconnect_timeout", 1);
        MqttSource source = new MqttSource(ReadonlyConfig.fromMap(config));
        MqttSourceConfig sourceConfig = new MqttSourceConfig(ReadonlyConfig.fromMap(config));
        AtomicLong currentTimeMillis = new AtomicLong(0L);
        MqttSourceReader reader =
                new MqttSourceReader(
                        sourceConfig,
                        source.getProducedCatalogTables().get(0),
                        currentTimeMillis::get);

        RuntimeException cause = new RuntimeException("broker unavailable");
        reader.connectionLost(cause);
        currentTimeMillis.set(1001L);

        Assertions.assertThrows(
                MqttConnectorException.class, () -> reader.pollNext(new RecordingCollector()));
    }

    @Test
    void testReaderFailsWhenResubscribeAfterReconnectFails() throws Exception {
        MqttSource source = new MqttSource(ReadonlyConfig.fromMap(baseConfig()));
        MqttSourceConfig sourceConfig = new MqttSourceConfig(ReadonlyConfig.fromMap(baseConfig()));
        MqttSourceReader reader =
                new MqttSourceReader(
                        sourceConfig,
                        source.getProducedCatalogTables().get(0),
                        System::currentTimeMillis);

        try (org.mockito.MockedConstruction<MqttClient> mocked =
                Mockito.mockConstruction(MqttClient.class)) {
            reader.open();
            MqttClient mockClient = mocked.constructed().get(0);
            Mockito.doThrow(new MqttException(MqttException.REASON_CODE_CLIENT_EXCEPTION))
                    .when(mockClient)
                    .subscribe("users", 1);

            reader.connectionLost(new RuntimeException("connection lost"));

            Assertions.assertDoesNotThrow(
                    () -> reader.connectComplete(true, "tcp://localhost:1883"));
            Assertions.assertThrows(
                    MqttConnectorException.class, () -> reader.pollNext(new RecordingCollector()));
        }
    }

    @Test
    void testReaderCloseForciblyDisconnectsWhenClientIsNotConnected() throws Exception {
        MqttSource source = new MqttSource(ReadonlyConfig.fromMap(baseConfig()));
        MqttSourceConfig sourceConfig = new MqttSourceConfig(ReadonlyConfig.fromMap(baseConfig()));
        MqttSourceReader reader =
                new MqttSourceReader(
                        sourceConfig,
                        source.getProducedCatalogTables().get(0),
                        System::currentTimeMillis);

        try (org.mockito.MockedConstruction<MqttClient> mocked =
                Mockito.mockConstruction(MqttClient.class)) {
            reader.open();
            MqttClient mockClient = mocked.constructed().get(0);
            Mockito.when(mockClient.isConnected()).thenReturn(false);

            reader.close();

            Mockito.verify(mockClient).disconnectForcibly();
            Mockito.verify(mockClient).close();
            Mockito.verify(mockClient, Mockito.never()).disconnect();
        }
    }

    @Test
    void testReaderFailsWhenMessageQueueIsFull() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put("max_queue_size", 1);
        MqttSource source = new MqttSource(ReadonlyConfig.fromMap(config));
        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        MqttSourceReader reader = (MqttSourceReader) source.createReader(readerContext);

        reader.messageArrived("users", mqttMessage("{\"id\":1}"));

        Assertions.assertThrows(
                MqttConnectorException.class,
                () -> reader.messageArrived("users", mqttMessage("{\"id\":2}")));
        Assertions.assertThrows(
                MqttConnectorException.class, () -> reader.pollNext(new RecordingCollector()));
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "tcp://localhost:1883");
        config.put("topic", "users");
        config.put("schema", schemaConfig());
        return config;
    }

    private static Map<String, Object> schemaConfig() {
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", Collections.singletonMap("id", "int"));
        return schema;
    }

    private static MqttMessage mqttMessage(String payload) {
        return new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
    }

    private static class RecordingCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private SeaTunnelRow record;

        @Override
        public void collect(SeaTunnelRow record) {
            this.record = record;
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }
}
