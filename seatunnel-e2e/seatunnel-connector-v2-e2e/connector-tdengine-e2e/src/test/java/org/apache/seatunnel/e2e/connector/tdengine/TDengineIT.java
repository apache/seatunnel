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

package org.apache.seatunnel.e2e.connector.tdengine;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class TDengineIT extends TestSuiteBase implements TestResource {
    private static final String DOCKER_IMAGE = "tdengine/tdengine:3.0.2.1";
    private static final String NETWORK_ALIASES1 = "flink_e2e_tdengine_src";
    private static final String NETWORK_ALIASES2 = "flink_e2e_tdengine_sink";
    private static final int PORT = 6041;

    private GenericContainer<?> tdengineServer1;
    private GenericContainer<?> tdengineServer2;
    private Connection connection1;
    private Connection connection2;
    private int testDataCount;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        tdengineServer1 =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(NETWORK_ALIASES1)
                        .withExposedPorts(PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        tdengineServer2 =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(NETWORK_ALIASES2)
                        .withExposedPorts(PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        Startables.deepStart(Stream.of(tdengineServer1)).join();
        Startables.deepStart(Stream.of(tdengineServer2)).join();
        log.info("TDengine container started");
        connection1 = createConnect(tdengineServer1);
        connection2 = createConnect(tdengineServer2);
        // wait for TDengine fully start
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        Boolean.TRUE,
                                        connection1.isValid(100) & connection2.isValid(100)));
        testDataCount = generateTestDataSet();
        log.info("tdengine testDataCount=" + testDataCount); // rowCount=8
    }

    @SneakyThrows
    private int generateTestDataSet() {
        int rowCount;
        try (Statement stmt = connection1.createStatement()) {
            stmt.execute("CREATE DATABASE power KEEP 3650");
            stmt.execute(
                    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, off BOOL, nc NCHAR(10)) "
                            + "TAGS (location BINARY(64), groupId INT)");
            String sql = getSQL();
            rowCount = stmt.executeUpdate(sql);
        }
        try (Statement stmt = connection2.createStatement()) {
            stmt.execute("CREATE DATABASE power2 KEEP 3650");
            stmt.execute(
                    "CREATE STABLE power2.meters2 (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, off BOOL, nc NCHAR(10)) "
                            + "TAGS (location BINARY(64), groupId INT)");
        }
        return rowCount;
    }

    @TestTemplate
    public void testTDengine(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/tdengine/tdengine_source_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        long rowCountInserted = readSinkDataset();
        Assertions.assertEquals(rowCountInserted, testDataCount);
    }

    @SneakyThrows
    private long readSinkDataset() {
        long rowCount;
        try (Statement stmt = connection2.createStatement();
                ResultSet resultSet = stmt.executeQuery("select count(1) from power2.meters2;"); ) {
            resultSet.next();
            rowCount = resultSet.getLong(1);
        }
        return rowCount;
    }

    @SneakyThrows
    private Connection createConnect(GenericContainer<?> tdengineServer) {
        String jdbcUrl =
                "jdbc:TAOS-RS://"
                        + tdengineServer.getHost()
                        + ":"
                        + tdengineServer.getFirstMappedPort()
                        + "?user=root&password=taosdata";
        Connection conn = DriverManager.getConnection(jdbcUrl);
        log.info("TDengine Connected! " + jdbcUrl);
        return conn;
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (connection1 != null) {
            connection1.close();
        }
        if (connection2 != null) {
            connection2.close();
        }
        if (tdengineServer1 != null) {
            tdengineServer1.stop();
        }
        if (tdengineServer2 != null) {
            tdengineServer2.stop();
        }
    }

    /**
     * The generated SQL is: INSERT INTO power.d1001 USING power.meters
     * TAGS(California.SanFrancisco, 2) VALUES('2018-10-03 14:38:05.000',10.30000,219,0.31000, true)
     * power.d1001 USING power.meters TAGS(California.SanFrancisco, 2) VALUES('2018-10-03
     * 14:38:15.000',12.60000,218,0.33000, false) power.d1001 USING power.meters
     * TAGS(California.SanFrancisco, 2) VALUES('2018-10-03 14:38:16.800',12.30000,221,0.31000, true)
     * power.d1002 USING power.meters TAGS(California.SanFrancisco, 3) VALUES('2018-10-03
     * 14:38:16.650',10.30000,218,0.25000, true) power.d1003 USING power.meters
     * TAGS(California.LosAngeles, 2) VALUES('2018-10-03 14:38:05.500',11.80000,221,0.28000, true)
     * power.d1003 USING power.meters TAGS(California.LosAngeles, 2) VALUES('2018-10-03
     * 14:38:16.600',13.40000,223,0.29000, true) power.d1004 USING power.meters
     * TAGS(California.LosAngeles, 3) VALUES('2018-10-03 14:38:05.000',10.80000,223,0.29000, true)
     * power.d1004 USING power.meters TAGS(California.LosAngeles, 3) VALUES('2018-10-03
     * 14:38:06.500',11.50000,221,0.35000, false)
     */
    private static String getSQL() {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        for (String line : getRawData()) {
            String[] ps = line.split(",");
            sb.append("power." + ps[0])
                    .append(" USING power.meters TAGS(")
                    .append(ps[5])
                    .append(", ") // tag: location
                    .append(ps[6]) // tag: groupId
                    .append(") VALUES(")
                    .append('\'')
                    .append(ps[1])
                    .append('\'')
                    .append(",") // ts
                    .append(ps[2])
                    .append(",") // current
                    .append(ps[3])
                    .append(",") // voltage
                    .append(ps[4])
                    .append(",") // off
                    .append(ps[7])
                    .append(",") // nc
                    .append(ps[8])
                    .append(") "); // phase
        }
        return sb.toString();
    }

    private static List<String> getRawData() {
        return Arrays.asList(
                "d1001,2018-10-03 14:38:05.000,10.30000,219,0.31000,'California.SanFrancisco',2,true,'nc'",
                "d1001,2018-10-03 14:38:15.000,12.60000,218,0.33000,'California.SanFrancisco',2,false,'nc'",
                "d1001,2018-10-03 14:38:16.800,12.30000,221,0.31000,'California.SanFrancisco',2,true,'nc'",
                "d1002,2018-10-03 14:38:16.650,10.30000,218,0.25000,'California.SanFrancisco',3,true,'nc'",
                "d1003,2018-10-03 14:38:05.500,11.80000,221,0.28000,'California.LosAngeles',2,true,'nc'",
                "d1003,2018-10-03 14:38:16.600,13.40000,223,0.29000,'California.LosAngeles',2,true,'nc'",
                "d1004,2018-10-03 14:38:05.000,10.80000,223,0.29000,'California.LosAngeles',3,true,'nc'",
                "d1004,2018-10-03 14:38:06.500,11.50000,221,0.35000,'California.LosAngeles',3,false,'nc'");
    }
}
