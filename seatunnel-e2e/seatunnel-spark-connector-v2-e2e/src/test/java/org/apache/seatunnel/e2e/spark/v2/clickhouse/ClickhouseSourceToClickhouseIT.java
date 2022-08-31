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

package org.apache.seatunnel.e2e.spark.v2.clickhouse;

import org.apache.seatunnel.e2e.spark.SparkContainer;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class ClickhouseSourceToClickhouseIT extends SparkContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickhouseSourceToClickhouseIT.class);
    private ClickHouseContainer clickhouse;

    private List<String> data;

    @BeforeEach
    public void startClickhouseContainer() throws InterruptedException, ClassNotFoundException {
        clickhouse = new ClickHouseContainer(DockerImageName.parse("yandex/clickhouse-server:22.1.3.7"))
                .withNetwork(NETWORK)
                .withNetworkAliases("clickhouse")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER));
        //clickhouse.setPortBindings(Lists.newArrayList("8123:8123"));
        Startables.deepStart(Stream.of(clickhouse)).join();
        data = generateData();
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        Awaitility.given().ignoreExceptions().await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> initializeClickhouseTable());
        LOGGER.info("Clickhouse container started");
        batchInsertData();
    }

    @Test
    public void testFakeSourceToClickhouseSink() throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/clickhouse/clickhousesource_to_clickhouse.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        try (Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl(), clickhouse.getUsername(), clickhouse.getPassword());
             Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select * from default.sink");
            List<String> result = Lists.newArrayList();
            while (resultSet.next()) {
                result.add(resultSet.getString("name"));
            }
            Assertions.assertEquals(data, result);
        }
    }

    private void initializeClickhouseTable() {
        try (Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl(), clickhouse.getUsername(), clickhouse.getPassword());
             Statement stmt = connection.createStatement()) {
            String sourceTableSql = "CREATE TABLE default.source" +
                    "(" +
                    "    `name` Nullable(String)" +
                    ")ENGINE = Memory";
            String sinkTableSql = "CREATE TABLE default.sink" +
                    "(" +
                    "    `name` Nullable(String)" +
                    ")ENGINE = Memory";
            stmt.execute(sourceTableSql);
            stmt.execute(sinkTableSql);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing clickhouse table failed", e);
        }
    }

    private List<String> generateData() {
        List<String> data = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            data.add("Mike");
        }
        return data;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void batchInsertData() {
        try (Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl(), clickhouse.getUsername(), clickhouse.getPassword())) {
            String sql = "insert into default.source(name) values(?)";
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < data.size(); i++) {
                preparedStatement.setString(1, data.get(i));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Batch insert data failed!", e);
        }
    }

    @AfterEach
    public void closeClickhouseContainer() {
        if (clickhouse != null) {
            clickhouse.stop();
        }
    }
}
