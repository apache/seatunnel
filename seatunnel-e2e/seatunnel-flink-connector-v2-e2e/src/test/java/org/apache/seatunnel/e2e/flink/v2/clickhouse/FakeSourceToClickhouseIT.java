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

package org.apache.seatunnel.e2e.flink.v2.clickhouse;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

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
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Stream;

public class FakeSourceToClickhouseIT extends FlinkContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(FakeSourceToClickhouseIT.class);
    private ClickHouseContainer clickhouse;

    @BeforeEach
    public void startClickhouseContainer() throws InterruptedException, ClassNotFoundException {
        clickhouse = new ClickHouseContainer(DockerImageName.parse("yandex/clickhouse-server:22.1.3.7"))
                .withNetwork(NETWORK)
                .withNetworkAliases("clickhouse")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER));
        //clickhouse.setPortBindings(Lists.newArrayList("8123:8123"));
        Startables.deepStart(Stream.of(clickhouse)).join();
        LOGGER.info("Clickhouse container started");
        Thread.sleep(5000L);
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        initializeClickhouseTable();
    }

    @Test
    public void testFakeSourceToClickhouseSink() throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/clickhouse/fakesource_to_clickhouse.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        try (Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl(), clickhouse.getUsername(), clickhouse.getPassword());
             Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select * from default.test");
            List<String> result = Lists.newArrayList();
            while (resultSet.next()) {
                result.add(resultSet.getString("name"));
            }
            Assertions.assertFalse(result.isEmpty());
        }
    }

    private void initializeClickhouseTable() {
        try (Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl(), clickhouse.getUsername(), clickhouse.getPassword());
             Statement stmt = connection.createStatement()) {
            String initializeTableSql = "CREATE TABLE default.test" +
                    "(" +
                    "    `name` Nullable(String)" +
                    ")ENGINE = Memory";
            stmt.execute(initializeTableSql);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing clickhouse table failed", e);
        }
    }

    @AfterEach
    public void closeClickhouseContainer() {
        if (clickhouse != null) {
            clickhouse.stop();
        }
    }
}
