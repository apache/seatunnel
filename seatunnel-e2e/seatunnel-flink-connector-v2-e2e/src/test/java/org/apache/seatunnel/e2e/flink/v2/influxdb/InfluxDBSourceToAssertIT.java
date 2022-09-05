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

package org.apache.seatunnel.e2e.flink.v2.influxdb;

import org.apache.seatunnel.connectors.seatunnel.influxdb.client.InfluxDBClient;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig;
import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class InfluxDBSourceToAssertIT extends FlinkContainer {

    private static final String INFLUXDB_DOCKER_IMAGE = "influxdb:1.8";
    private static final String INFLUXDB_CONTAINER_HOST = "influxdb-host";
    private static final String INFLUXDB_HOST = "localhost";

    private static final int INFLUXDB_PORT = 8764;
    private static final int INFLUXDB_CONTAINER_PORT = 8086;
    private static final String INFLUXDB_CONNECT_URL = String.format("http://%s:%s", INFLUXDB_HOST, INFLUXDB_PORT);
    private static final String INFLUXDB_DATABASE = "test";
    private static final String INFLUXDB_MEASUREMENT = "test";

    private GenericContainer<?> influxDBServer;

    private  InfluxDB influxDB;

    @BeforeEach
    public void startInfluxDBContainer() throws ClassNotFoundException, SQLException, ConnectException {
        influxDBServer = new GenericContainer<>(INFLUXDB_DOCKER_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases(INFLUXDB_CONTAINER_HOST)
                .withLogConsumer(new Slf4jLogConsumer(log));
        influxDBServer.setPortBindings(Lists.newArrayList(
                String.format("%s:%s", INFLUXDB_PORT, INFLUXDB_CONTAINER_PORT)));
        Startables.deepStart(Stream.of(influxDBServer)).join();
        log.info("influxdb container started");
        initializeInfluxDBClient();
        batchInsertData();
    }

    @Test
    public void testInfluxDBSource() throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/influxdb/influxdb_source_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    private void initializeInfluxDBClient() throws SQLException, ClassNotFoundException, ConnectException {
        InfluxDBConfig influxDBConfig = new InfluxDBConfig(INFLUXDB_CONNECT_URL);
        influxDB = InfluxDBClient.getInfluxDB(influxDBConfig);
    }

    public void batchInsertData() {
        influxDB.createDatabase(INFLUXDB_DATABASE);
        BatchPoints batchPoints = BatchPoints
                .database(INFLUXDB_DATABASE)
                .build();
        for (int i = 0; i < 100; i++) {
            Point point = Point.measurement(INFLUXDB_MEASUREMENT)
                    .time(new Date().getTime(), TimeUnit.NANOSECONDS)
                    .tag("label", String.format("label_%s", i))
                    .addField("value", i)
                    .addField("rt", String.format("rt_%s", i))
                    .build();
            batchPoints.point(point);
        }
        influxDB.write(batchPoints);
    }

    @AfterEach
    public void closeIoTDBContainer() {
        if (influxDBServer != null) {
            influxDBServer.stop();
        }
    }
}
