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

package org.apache.seatunnel.e2e.flink.v2.jdbc;

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.core.starter.config.ConfigBuilder;
import org.apache.seatunnel.e2e.flink.FlinkContainer;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class JdbcMysqlIT extends FlinkContainer {
    private static final String DOCKER_IMAGE = "bitnami/mysql:8.0.29";
    private MySQLContainer<?> mc;
    private Config config;
    private static final String THIRD_PARTY_PLUGINS_URL = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";

    @SuppressWarnings("checkstyle:MagicNumber")
    @BeforeEach
    public void startMySqlContainer() throws Exception {
        // Non-root users need to grant XA_RECOVER_ADMIN permission on is_exactly_once = "true"
        mc = new MySQLContainer<>(DockerImageName.parse(DOCKER_IMAGE).asCompatibleSubstituteFor("mysql"))
            .withNetwork(NETWORK)
            .withNetworkAliases("mysql")
            .withUsername("root")
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        Startables.deepStart(Stream.of(mc)).join();
        log.info("Mysql container started");
        Class.forName(mc.getDriverClassName());
        given().ignoreExceptions()
            .await()
            .atLeast(100, TimeUnit.MILLISECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(5, TimeUnit.SECONDS)
            .untilAsserted(this::initializeJdbcTable);
        batchInsertData();
    }

    private void initializeJdbcTable() throws URISyntaxException {
        URI resource = Objects.requireNonNull(FlinkContainer.class.getResource("/jdbc/init_sql/mysql_init.conf")).toURI();

        config = new ConfigBuilder(Paths.get(resource)).getConfig();

        CheckConfigUtil.checkAllExists(this.config, "source_table", "sink_table", "type_source_table",
            "type_sink_table", "insert_type_source_table_sql", "check_type_sink_table_sql");

        try (Connection connection = DriverManager.getConnection(mc.getJdbcUrl(), mc.getUsername(), mc.getPassword())) {
            Statement statement = connection.createStatement();
            statement.execute(config.getString("source_table"));
            statement.execute(config.getString("sink_table"));
            statement.execute(config.getString("type_source_table"));
            statement.execute(config.getString("type_sink_table"));
            statement.execute(config.getString("insert_type_source_table_sql"));
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Mysql table failed!", e);
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void batchInsertData() {
        String sql = "insert into source(name, age) values(?,?)";
        try (Connection connection = DriverManager.getConnection(mc.getJdbcUrl(), mc.getUsername(), mc.getPassword())) {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (List row : generateTestDataset()) {
                preparedStatement.setString(1, (String) row.get(0));
                preparedStatement.setInt(2, (Integer) row.get(1));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Batch insert data failed!", e);
        }
    }

    @Test
    public void testJdbcMysqlSourceAndSink() throws Exception {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/jdbc_mysql_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        Assertions.assertIterableEquals(generateTestDataset(), queryResult());
    }

    @Test
    public void testJdbcMysqlSourceAndSinkParallel() throws Exception {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/jdbc_mysql_source_and_sink_parallel.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        //Sorting is required, because it is read in parallel, so there will be out of order
        List<List> sortedResult = queryResult().stream().sorted(Comparator.comparing(list -> (Integer) list.get(1)))
            .collect(Collectors.toList());
        Assertions.assertIterableEquals(generateTestDataset(), sortedResult);
    }

    @Test
    public void testJdbcMysqlSourceAndSinkParallelUpperLower() throws Exception {
        Container.ExecResult execResult =
            executeSeaTunnelFlinkJob("/jdbc/jdbc_mysql_source_and_sink_parallel_upper_lower.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        //Sorting is required, because it is read in parallel, so there will be out of order
        List<List> sortedResult = queryResult().stream().sorted(Comparator.comparing(list -> (Integer) list.get(1)))
            .collect(Collectors.toList());

        //lower=1 upper=50
        List<List> limit50 = generateTestDataset().stream().limit(50).collect(Collectors.toList());
        Assertions.assertIterableEquals(limit50, sortedResult);
    }

    @Test
    public void testJdbcMysqlSourceAndSinkXA() throws Exception {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/jdbc_mysql_source_and_sink_xa.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        Assertions.assertIterableEquals(generateTestDataset(), queryResult());
    }

    @Test
    public void testJdbcMysqlSourceAndSinkDataType() throws Exception {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/jdbc_mysql_source_and_sink_datatype.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        checkSinkDataTypeTable();
    }

    private void checkSinkDataTypeTable() throws Exception {
        try (Connection connection = DriverManager.getConnection(mc.getJdbcUrl(), mc.getUsername(), mc.getPassword())) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(config.getString("check_type_sink_table_sql"));
            resultSet.next();
            Assertions.assertEquals(resultSet.getInt(1), 2);
        }
    }

    private List<List> queryResult() {
        List<List> result = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(mc.getJdbcUrl(), mc.getUsername(), mc.getPassword())) {
            Statement statement = connection.createStatement();
            String sql = "select name , age from sink ";
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                result.add(
                    Arrays.asList(
                        resultSet.getString(1),
                        resultSet.getInt(2)
                    )
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException("Query result data failed!", e);
        }
        return result;
    }

    private static List<List> generateTestDataset() {
        List<List> rows = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            rows.add(
                Arrays.asList(
                    String.format("user_%s", i),
                    i
                ));
        }
        return rows;
    }

    @AfterEach
    public void closeMySqlContainer() {
        if (mc != null) {
            mc.stop();
        }
    }

    @Override
    protected void executeExtraCommands(GenericContainer<?> container) throws IOException, InterruptedException {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + THIRD_PARTY_PLUGINS_URL);
        Assertions.assertEquals(0, extraCommands.getExitCode());
    }
}
