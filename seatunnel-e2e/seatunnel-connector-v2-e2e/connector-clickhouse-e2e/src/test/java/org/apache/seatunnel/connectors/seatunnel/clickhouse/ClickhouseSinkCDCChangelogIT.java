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

package org.apache.seatunnel.connectors.seatunnel.clickhouse;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Spark engine will lose the row kind of record")
@Slf4j
public class ClickhouseSinkCDCChangelogIT extends TestSuiteBase implements TestResource {
    private static final String CLICKHOUSE_DOCKER_IMAGE = "clickhouse/clickhouse-server:23.3.13.6";
    private static final String HOST = "clickhouse";
    private static final String DRIVER_CLASS = "com.clickhouse.jdbc.ClickHouseDriver";
    private static final String DATABASE = "default";
    private static final String SINK_TABLE = "sink_table";
    private ClickHouseContainer container;
    private Connection connection;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.container =
                new ClickHouseContainer(CLICKHOUSE_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withExposedPorts(8123)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CLICKHOUSE_DOCKER_IMAGE)));
        Startables.deepStart(Stream.of(this.container)).join();
        log.info("Clickhouse container started");
        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(360L, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (this.connection != null) {
            this.connection.close();
        }
        if (this.container != null) {
            this.container.stop();
        }
    }

    @TestTemplate
    public void testClickhouseMergeTreeTable(TestContainer container) throws Exception {
        initializeClickhouseMergeTreeTable();

        Container.ExecResult execResult =
                container.executeJob("/clickhouse_sink_cdc_changelog_case1.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        checkSinkTableRows();
        dropSinkTable();
    }

    @TestTemplate
    public void testClickhouseMergeTreeTableWithEnableDelete(TestContainer container)
            throws Exception {
        initializeClickhouseMergeTreeTable();

        Container.ExecResult execResult =
                container.executeJob("/clickhouse_sink_cdc_changelog_case2.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atLeast(100L, TimeUnit.MILLISECONDS)
                .atMost(20L, TimeUnit.SECONDS)
                .untilAsserted(this::checkSinkTableRows);
        dropSinkTable();
    }

    @TestTemplate
    public void testClickhouseReplacingMergeTreeTable(TestContainer container) throws Exception {
        initializeClickhouseReplacingMergeTreeTable();

        Container.ExecResult execResult =
                container.executeJob("/clickhouse_sink_cdc_changelog_case1.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        checkSinkTableRows();
        dropSinkTable();
    }

    @TestTemplate
    public void testClickhouseReplacingMergeTreeTableWithEnableDelete(TestContainer container)
            throws Exception {
        initializeClickhouseReplacingMergeTreeTable();

        Container.ExecResult execResult =
                container.executeJob("/clickhouse_sink_cdc_changelog_case2.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        checkSinkTableRows();
        dropSinkTable();
    }

    private void initConnection() throws Exception {
        final Properties info = new Properties();
        info.put("user", this.container.getUsername());
        info.put("password", this.container.getPassword());
        this.connection =
                ((Driver) Class.forName(DRIVER_CLASS).newInstance())
                        .connect(this.container.getJdbcUrl(), info);
    }

    private void initializeClickhouseMergeTreeTable() {
        try {
            Statement statement = this.connection.createStatement();
            String sql =
                    String.format(
                            "create table if not exists %s.%s(\n"
                                    + "    `pk_id`         Int64,\n"
                                    + "    `name`          String,\n"
                                    + "    `score`         Int32\n"
                                    + ")engine=MergeTree ORDER BY(pk_id) PRIMARY KEY(pk_id)",
                            DATABASE, SINK_TABLE);
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Clickhouse table failed!", e);
        }
    }

    private void initializeClickhouseReplacingMergeTreeTable() {
        try {
            Statement statement = this.connection.createStatement();
            String sql =
                    String.format(
                            "create table if not exists %s.%s(\n"
                                    + "    `pk_id`         Int64,\n"
                                    + "    `name`          String,\n"
                                    + "    `score`         Int32\n"
                                    + ")engine=ReplacingMergeTree ORDER BY(pk_id) PRIMARY KEY(pk_id)",
                            DATABASE, SINK_TABLE);
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Clickhouse table failed!", e);
        }
    }

    private void checkSinkTableRows() throws SQLException {
        Set<List<Object>> actual = new HashSet<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet =
                    statement.executeQuery(
                            String.format("select * from %s.%s", DATABASE, SINK_TABLE));
            while (resultSet.next()) {
                List<Object> row =
                        Arrays.asList(
                                resultSet.getLong("pk_id"),
                                resultSet.getString("name"),
                                resultSet.getInt("score"));
                actual.add(row);
            }
        }
        Set<List<Object>> expected =
                Stream.<List<Object>>of(Arrays.asList(1L, "A_1", 100), Arrays.asList(3L, "C", 100))
                        .collect(Collectors.toSet());
        if (!Arrays.equals(actual.toArray(), expected.toArray())) {
            throw new IllegalStateException(
                    String.format(
                            "Actual results %s not equal expected results %s",
                            Arrays.toString(actual.toArray()),
                            Arrays.toString(expected.toArray())));
        }
    }

    private void dropSinkTable() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format("drop table if exists %s.%s sync", DATABASE, SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("Test clickhouse server image error", e);
        }
    }
}
