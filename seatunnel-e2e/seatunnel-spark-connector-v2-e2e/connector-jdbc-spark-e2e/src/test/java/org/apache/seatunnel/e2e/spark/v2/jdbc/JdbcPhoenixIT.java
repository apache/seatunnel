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

package org.apache.seatunnel.e2e.spark.v2.jdbc;

import org.apache.seatunnel.e2e.spark.SparkContainer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class JdbcPhoenixIT extends SparkContainer {

    private static final String PHOENIX_DOCKER_IMAGE = "iteblog/hbase-phoenix-docker:1.0";

    private static final String PHOENIX_CONTAINER_HOST = "spark_e2e_phoenix_sink";

    private static final int PHOENIX_PORT = 8763;
    private static final int PHOENIX_CONTAINER_PORT = 8765;

    private static final String PHOENIX_CONNECT_URL = "jdbc:phoenix:thin:url=http://%s:%s;serialization=PROTOBUF";
    private static final String PHOENIX_JDBC_DRIVER = "org.apache.phoenix.queryserver.client.Driver";

    private GenericContainer<?> phoenixServer;

    private Connection connection;
    private static final String THIRD_PARTY_PLUGINS_URL = "https://repo1.maven.org/maven2/com/aliyun/phoenix/ali-phoenix-shaded-thin-client/5.2.5-HBase-2.x/ali-phoenix-shaded-thin-client-5.2.5-HBase-2.x.jar";

    @BeforeEach
    public void startPhoenixContainer() throws ClassNotFoundException, SQLException {
        phoenixServer = new GenericContainer<>(PHOENIX_DOCKER_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(PHOENIX_CONTAINER_HOST)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PHOENIX_DOCKER_IMAGE)));
        phoenixServer.setPortBindings(Lists.newArrayList(
            String.format("%s:%s", PHOENIX_PORT, PHOENIX_CONTAINER_PORT)));
        Startables.deepStart(Stream.of(phoenixServer)).join();
        initializeJdbcConnection();
        log.info("phoenix container started");
        initializePhoenixTable();
        batchInsertData();
    }

    @Test
    public void testJdbcPhoenixSourceAndSink() throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/jdbc/jdbc_phoenix_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // query result
        String sql = "select f1, f2, f3, f4, f5, f6, f7 from test.sink order by f5 asc";
        List<List> result = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                result.add(Arrays.asList(
                    resultSet.getString(1),
                    resultSet.getBoolean(2),
                    resultSet.getDouble(3),
                    resultSet.getFloat(4),
                    resultSet.getShort(5),
                    resultSet.getInt(6),
                    resultSet.getInt(7)));
            }
        }
        Assertions.assertIterableEquals(generateTestDataset(), result);
    }

    private void initializeJdbcConnection() throws SQLException, ClassNotFoundException {
        Class.forName(PHOENIX_JDBC_DRIVER);
        connection = DriverManager.getConnection(String.format(PHOENIX_CONNECT_URL, phoenixServer.getHost(), PHOENIX_PORT));
    }

    private void initializePhoenixTable() {
        try {
            Statement statement = connection.createStatement();
            String createSource = "CREATE TABLE test.source (\n" +
                "\tf1 VARCHAR PRIMARY KEY,\n" +
                "\tf2 BOOLEAN,\n" +
                "\tf3 UNSIGNED_DOUBLE,\n" +
                "\tf4 UNSIGNED_FLOAT,\n" +
                "\tf5 UNSIGNED_SMALLINT,\n" +
                "\tf6 INTEGER,\n" +
                "\tf7 UNSIGNED_INT\n" +
                ")";
            String createSink = "CREATE TABLE test.sink (\n" +
                "\tf1 VARCHAR PRIMARY KEY,\n" +
                "\tf2 BOOLEAN,\n" +
                "\tf3 UNSIGNED_DOUBLE,\n" +
                "\tf4 UNSIGNED_FLOAT,\n" +
                "\tf5 UNSIGNED_SMALLINT,\n" +
                "\tf6 INTEGER,\n" +
                "\tf7 UNSIGNED_INT\n" +
                ")";
            statement.execute(createSource);
            statement.execute(createSink);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing  table failed!", e);
        }
    }

    @AfterEach
    public void closePhoenixContainer() throws SQLException {
        if (phoenixServer != null) {
            phoenixServer.stop();
        }
    }

    private static List<List> generateTestDataset() {
        List<List> rows = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            rows.add(Arrays.asList(String.format("test_%s", i),
                i % 2 == 0,
                Double.valueOf(i + 1),
                Float.valueOf(i + 2),
                (short) (i + 3),
                Integer.valueOf(i + 4),
                i + 5
            ));
        }
        return rows;
    }

    private void batchInsertData() throws SQLException, ClassNotFoundException {
        String sql = "upsert into test.source(f1, f2, f3, f4, f5, f6, f7) values(?, ?, ?, ?, ?, ?, ?)";

        try {
            connection.setAutoCommit(false);
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                for (List row : generateTestDataset()) {
                    preparedStatement.setString(1, (String) row.get(0));
                    preparedStatement.setBoolean(2, (Boolean) row.get(1));
                    preparedStatement.setDouble(3, (Double) row.get(2));
                    preparedStatement.setFloat(4, (Float) row.get(3));
                    preparedStatement.setShort(5, (Short) row.get(4));
                    preparedStatement.setInt(6, (Integer) row.get(5));
                    preparedStatement.setInt(7, (Integer) row.get(6));
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    @Override
    protected void executeExtraCommands(GenericContainer<?> container) throws IOException, InterruptedException {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + THIRD_PARTY_PLUGINS_URL);
        Assertions.assertEquals(0, extraCommands.getExitCode());
    }
}
