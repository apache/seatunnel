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

package org.apache.seatunnel.e2e.connector.iotdb;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.iotdb.source.IoTDBSource;
import org.apache.seatunnel.connectors.seatunnel.iotdb.source.IoTDBSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.iotdb.source.IoTDBSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.iotdb.state.IoTDBSourceState;

import org.apache.iotdb.session.Session;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Exercises the source lifecycle against IoTDB independently of the engine containers. */
class IoTDBMultiTableSourceIT {
    @Test
    void readsDifferentSchemasAndNonOverlappingTimePartitions() throws Exception {
        try (GenericContainer<?> server =
                new GenericContainer<>("apache/iotdb:0.13.1-node").withExposedPorts(6667)) {
            server.start();
            Session session =
                    new Session.Builder()
                            .host(server.getHost())
                            .port(server.getMappedPort(6667))
                            .username("root")
                            .password("root")
                            .build();
            try {
                await().atMost(Duration.ofSeconds(60))
                        .ignoreExceptions()
                        .untilAsserted(session::open);
                session.executeNonQueryStatement(
                        "CREATE TIMESERIES root.multi.weather.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN");
                session.executeNonQueryStatement(
                        "CREATE TIMESERIES root.multi.status.enabled WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
                for (int timestamp = 0; timestamp <= 100; timestamp++) {
                    session.executeNonQueryStatement(
                            "INSERT INTO root.multi.weather(timestamp, temperature) VALUES("
                                    + timestamp
                                    + ", 12.5)");
                }
                session.executeNonQueryStatement(
                        "INSERT INTO root.multi.status(timestamp, enabled) VALUES(3, true)");
                String connection =
                        "node_urls = \""
                                + server.getHost()
                                + ":"
                                + server.getMappedPort(6667)
                                + "\"\nusername = root\npassword = root\n";
                IoTDBSource source =
                        source(
                                connection
                                        + "tables_configs = ["
                                        + "{sql = \"SELECT temperature FROM root.multi.weather\", lower_bound = 0, upper_bound = 100, num_partitions = 4, schema {table = weather, fields {ts = bigint, temperature = float}}},"
                                        + "{sql = \"SELECT enabled FROM root.multi.status\", schema {table = status, fields {ts = bigint, enabled = boolean}}}]");
                List<SeaTunnelRow> rows = read(source);
                List<SeaTunnelRow> weather =
                        rows.stream()
                                .filter(row -> "weather".equals(row.getTableId()))
                                .collect(Collectors.toList());
                List<SeaTunnelRow> status =
                        rows.stream()
                                .filter(row -> "status".equals(row.getTableId()))
                                .collect(Collectors.toList());
                Assertions.assertEquals(101, weather.size());
                Set<Long> timestamps = new HashSet<>();
                for (SeaTunnelRow row : weather) {
                    Assertions.assertTrue(timestamps.add((Long) row.getField(0)));
                    Assertions.assertEquals(12.5F, row.getField(1));
                }
                Assertions.assertEquals(1, status.size());
                Assertions.assertArrayEquals(new Object[] {3L, true}, status.get(0).getFields());
                Assertions.assertEquals(
                        101,
                        read(source(
                                        connection
                                                + "sql = \"SELECT temperature FROM root.multi.weather\"\nschema {fields {ts = bigint, temperature = float}}"))
                                .size());
            } finally {
                session.close();
            }
        }
    }

    private static IoTDBSource source(String text) {
        ReadonlyConfig config = ReadonlyConfig.fromConfig(ConfigFactory.parseString(text));
        Object result =
                new IoTDBSourceFactory()
                        .createSource(
                                new TableSourceFactoryContext(
                                        config, IoTDBMultiTableSourceIT.class.getClassLoader()))
                        .createSource();
        return (IoTDBSource) result;
    }

    private static List<SeaTunnelRow> read(IoTDBSource source) throws Exception {
        SourceSplitEnumerator.Context<IoTDBSourceSplit> enumeratorContext =
                mock(SourceSplitEnumerator.Context.class);
        when(enumeratorContext.currentParallelism()).thenReturn(2);
        when(enumeratorContext.registeredReaders()).thenReturn(Collections.emptySet());
        SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> enumerator =
                source.createEnumerator(enumeratorContext);
        enumerator.run();
        IoTDBSourceState state = enumerator.snapshotState(1);
        List<SeaTunnelRow> rows = new ArrayList<>();
        Collector<SeaTunnelRow> output =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow row) {
                        rows.add(row);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                };
        for (List<IoTDBSourceSplit> splits : state.getPendingSplit().values()) {
            SourceReader.Context readerContext = mock(SourceReader.Context.class);
            when(readerContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);
            try (SourceReader<SeaTunnelRow, IoTDBSourceSplit> reader =
                    source.createReader(readerContext)) {
                reader.open();
                reader.addSplits(splits);
                reader.handleNoMoreSplits();
                reader.pollNext(output);
            }
        }
        enumerator.close();
        return rows;
    }
}
