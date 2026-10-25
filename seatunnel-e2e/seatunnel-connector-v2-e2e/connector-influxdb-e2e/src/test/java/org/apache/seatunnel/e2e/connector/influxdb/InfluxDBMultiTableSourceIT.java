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
package org.apache.seatunnel.e2e.connector.influxdb;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.influxdb.source.InfluxDBSource;
import org.apache.seatunnel.connectors.seatunnel.influxdb.source.InfluxDBSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.influxdb.source.InfluxDBSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.influxdb.state.InfluxDBSourceState;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Exercises the connector lifecycle against InfluxDB without requiring an engine distribution. */
class InfluxDBMultiTableSourceIT {
    private static GenericContainer<?> server;
    private static String url;
    private static final String TEMPERATURE =
            "{ database=telemetry, sql=\"SELECT value, sequence FROM readings WHERE sequence >= 0\", lower_bound=0, upper_bound=4, partition_num=3, split_column=sequence, schema { table=temperatures, fields { value=DOUBLE, sequence=INT } } }";
    private static final String ALARMS =
            "{ database=operations, sql=\"select label, active from alerts tz('Asia/Shanghai')\", schema { table=alerts, fields { active=BOOLEAN, label=STRING } } }";
    private static final String EMPTY =
            "{ database=telemetry, sql=\"select missing from empty_measurement\", schema { table=empty, fields { missing=STRING } } }";

    @BeforeAll
    static void startServer() {
        server =
                new GenericContainer<>(DockerImageName.parse("influxdb:1.8"))
                        .withExposedPorts(8086)
                        .waitingFor(Wait.forHttp("/ping").forStatusCode(204));
        server.start();
        url = "http://" + server.getHost() + ":" + server.getMappedPort(8086);
        try (InfluxDB client = InfluxDBFactory.connect(url)) {
            client.createDatabase("telemetry");
            client.createDatabase("operations");
            BatchPoints temperature = BatchPoints.database("telemetry").build();
            for (int i = 0; i < 5; i++) {
                temperature.point(
                        Point.measurement("readings")
                                .time(i + 1, TimeUnit.SECONDS)
                                .addField("value", i + 0.5D)
                                .addField("sequence", i)
                                .build());
            }
            client.write(temperature);
            client.write(
                    BatchPoints.database("operations")
                            .point(
                                    Point.measurement("alerts")
                                            .time(1, TimeUnit.SECONDS)
                                            .addField("active", true)
                                            .addField("label", "high")
                                            .build())
                            .point(
                                    Point.measurement("alerts")
                                            .time(2, TimeUnit.SECONDS)
                                            .addField("active", false)
                                            .addField("label", "normal")
                                            .build())
                            .build());
        }
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void restoresAndReadsDifferentSchemasDatabasesUnevenRangesAndEmptyTables() throws Exception {
        InfluxDBSource original =
                source("tables_configs=[" + TEMPERATURE + "," + ALARMS + "," + EMPTY + "]");
        List<InfluxDBSourceSplit> splits = roundTrip(enumerate(original));
        Assertions.assertEquals(5, splits.size());
        InfluxDBSource reordered =
                source("tables_configs=[" + EMPTY + "," + ALARMS + "," + TEMPERATURE + "]");
        List<SeaTunnelRow> rows = read(reordered, splits);
        Assertions.assertEquals(7, rows.size());
        List<SeaTunnelRow> temperatures =
                rows.stream()
                        .filter(r -> "temperatures".equals(r.getTableId()))
                        .collect(Collectors.toList());
        Assertions.assertEquals(5, temperatures.size());
        Assertions.assertEquals(
                5, temperatures.stream().map(r -> r.getField(1)).distinct().count());
        for (SeaTunnelRow row : temperatures) {
            Assertions.assertEquals(((Integer) row.getField(1)) + 0.5D, row.getField(0));
        }
        List<String> alerts =
                rows.stream()
                        .filter(r -> "alerts".equals(r.getTableId()))
                        .map(r -> Arrays.toString(r.getFields()))
                        .sorted()
                        .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("[false, normal]", "[true, high]"), alerts);
        Assertions.assertFalse(rows.stream().anyMatch(r -> "empty".equals(r.getTableId())));
    }

    @Test
    void preservesLegacySingleTableReadAndTimezoneProbe() throws Exception {
        InfluxDBSource source =
                source(
                        "database=operations\nsql=\"select label, active from alerts tz('Asia/Shanghai')\"\nschema { fields { active=BOOLEAN, label=STRING } }");
        List<SeaTunnelRow> rows = read(source, enumerate(source));
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(Boolean.TRUE, rows.get(0).getField(0));
        Assertions.assertEquals("high", rows.get(0).getField(1));
    }

    private static InfluxDBSource source(String tableConfig) {
        ReadonlyConfig config =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString("url=\"" + url + "\"\n" + tableConfig));
        return (InfluxDBSource)
                new InfluxDBSourceFactory()
                        .<SeaTunnelRow, InfluxDBSourceSplit, InfluxDBSourceState>createSource(
                                new TableSourceFactoryContext(
                                        config, InfluxDBMultiTableSourceIT.class.getClassLoader()))
                        .createSource();
    }

    private static List<InfluxDBSourceSplit> enumerate(InfluxDBSource source) throws Exception {
        SourceSplitEnumerator.Context<InfluxDBSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.currentParallelism()).thenReturn(1);
        Mockito.when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        try (SourceSplitEnumerator<InfluxDBSourceSplit, InfluxDBSourceState> enumerator =
                source.createEnumerator(context)) {
            enumerator.open();
            enumerator.run();
        }
        ArgumentCaptor<List<InfluxDBSourceSplit>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(context).assignSplit(Mockito.eq(0), captor.capture());
        return captor.getValue();
    }

    private static List<SeaTunnelRow> read(InfluxDBSource source, List<InfluxDBSourceSplit> splits)
            throws Exception {
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        List<SeaTunnelRow> rows = new ArrayList<>();
        try (SourceReader<SeaTunnelRow, InfluxDBSourceSplit> reader =
                source.createReader(context)) {
            reader.open();
            reader.addSplits(splits);
            reader.handleNoMoreSplits();
            reader.pollNext(
                    new Collector<SeaTunnelRow>() {
                        public void collect(SeaTunnelRow row) {
                            rows.add(row);
                        }

                        public Object getCheckpointLock() {
                            return this;
                        }
                    });
        }
        Mockito.verify(context).signalNoMoreElement();
        return rows;
    }

    private static <T> T roundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) input.readObject();
        }
    }
}
