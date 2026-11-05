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

package org.apache.seatunnel.connectors.seatunnel.influxdb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorException;

import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InfluxDBSinkWriterTest {

    private static SinkConfig sinkConfig(int maxRetries) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "http://127.0.0.1:8086");
        SinkConfig sinkConfig = new SinkConfig(ReadonlyConfig.fromMap(configMap));
        sinkConfig.setBatchSize(100);
        sinkConfig.setMaxRetries(maxRetries);
        sinkConfig.setRetryBackoffMultiplierMs(0);
        sinkConfig.setMaxRetryBackoffMs(0);
        return sinkConfig;
    }

    private static InfluxDBSinkWriter writerWithClient(InfluxDB client, SinkConfig sinkConfig)
            throws Exception {
        return new InfluxDBSinkWriter(
                sinkConfig,
                new SeaTunnelRowType(
                        new String[] {"time", "value"},
                        new SeaTunnelDataType[] {BasicType.LONG_TYPE, BasicType.DOUBLE_TYPE})) {
            @Override
            protected InfluxDB createClient() {
                return client;
            }
        };
    }

    private static InfluxDB healthyClient() {
        InfluxDB client = mock(InfluxDB.class);
        when(client.version()).thenReturn("1.8.0");
        Pong pong = new Pong();
        pong.setVersion("1.8.0");
        when(client.ping()).thenReturn(pong);
        return client;
    }

    private static Point point(long time) {
        return Point.measurement("cpu")
                .time(time, TimeUnit.MILLISECONDS)
                .addField("value", 1.0d)
                .build();
    }

    @Test
    public void shouldWriteBatchExactlyOnceWhenFirstAttemptSucceeds() throws Exception {
        InfluxDB client = healthyClient();
        InfluxDBSinkWriter writer = writerWithClient(client, sinkConfig(3));

        writer.write(point(1L));
        writer.write(point(2L));
        writer.flush();

        ArgumentCaptor<BatchPoints> captor = ArgumentCaptor.forClass(BatchPoints.class);
        verify(client, times(1)).write(captor.capture());
        Assertions.assertEquals(2, captor.getValue().getPoints().size());
    }

    @Test
    public void shouldNotAccumulatePointsAcrossRetryAttempts() throws Exception {
        InfluxDB client = healthyClient();
        doThrow(new RuntimeException("first attempt fails"))
                .doAnswer(invocation -> null)
                .when(client)
                .write(any(BatchPoints.class));
        InfluxDBSinkWriter writer = writerWithClient(client, sinkConfig(3));

        writer.write(point(1L));
        writer.write(point(2L));
        writer.flush();

        ArgumentCaptor<BatchPoints> captor = ArgumentCaptor.forClass(BatchPoints.class);
        verify(client, times(2)).write(captor.capture());
        for (BatchPoints batch : captor.getAllValues()) {
            Assertions.assertEquals(2, batch.getPoints().size());
        }
    }

    @Test
    public void shouldThrowAfterRetriesExhausted() throws Exception {
        InfluxDB client = healthyClient();
        doThrow(new RuntimeException("influxdb unavailable"))
                .when(client)
                .write(any(BatchPoints.class));
        InfluxDBSinkWriter writer = writerWithClient(client, sinkConfig(2));

        writer.write(point(1L));
        Assertions.assertThrows(InfluxdbConnectorException.class, writer::flush);

        verify(client, times(3)).write(any(BatchPoints.class));
    }
}
