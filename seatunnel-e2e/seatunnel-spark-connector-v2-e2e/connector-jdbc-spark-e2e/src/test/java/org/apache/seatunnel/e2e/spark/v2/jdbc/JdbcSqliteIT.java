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

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class JdbcSqliteIT extends SparkContainer {
    private static final String SQLITE_USER = "";
    private static final String SQLITE_PASSWORD = "";
    private static final String SQLITE_DRIVER = "org.sqlite.JDBC";
    private String tmpdir;
    private static final List<List<Object>> TEST_DATASET = generateTestDataset();
    private static final String THIRD_PARTY_PLUGINS_URL = "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.39.3.0/sqlite-jdbc-3.39.3.0.jar";

    private Connection jdbcConnection;

    private void initTestDb() throws ClassNotFoundException, SQLException {
        tmpdir = System.getProperty("java.io.tmpdir");
        Class.forName(SQLITE_DRIVER);
        jdbcConnection = DriverManager.getConnection("jdbc:sqlite:" + tmpdir + "test.db", SQLITE_USER, SQLITE_PASSWORD);
        initializeJdbcTable();
        batchInsertData();
    }

    private void initializeJdbcTable() throws SQLException {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS source");
            statement.execute("DROP TABLE IF EXISTS sink");
            String createSource = "CREATE TABLE source (\n" +
                    "age INT NOT NULL,\n" +
                    "name VARCHAR(255) NOT NULL\n" +
                    ")";
            String createSink = "CREATE TABLE sink (\n" +
                    "age INT NOT NULL,\n" +
                    "name VARCHAR(255) NOT NULL\n" +
                    ")";
            statement.execute(createSource);
            statement.execute(createSink);
        }
    }

    private void batchInsertData() throws SQLException {
        String sql = "insert into source(age, name) values(?, ?)";

        try {
            jdbcConnection.setAutoCommit(false);
            try (PreparedStatement preparedStatement = jdbcConnection.prepareStatement(sql)) {
                for (List row : TEST_DATASET) {
                    preparedStatement.setInt(1, (Integer) row.get(0));
                    preparedStatement.setString(2, (String) row.get(1));
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            jdbcConnection.commit();
        } catch (SQLException e) {
            jdbcConnection.rollback();
            throw e;
        }
    }

    private static List<List<Object>> generateTestDataset() {
        List<List<Object>> rows = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            rows.add(Arrays.asList(i, String.format("test_%s", i)));
        }
        return rows;
    }

    @Test
    public void testJdbcSqliteSourceAndSink() throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/jdbc/jdbc_sqlite_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        master.copyFileFromContainer(Paths.get(SEATUNNEL_HOME, "data", "test.db").toString(), new File(tmpdir + "test.db").toPath().toString());
        // query result
        String sql = "select age, name from sink order by age asc";
        List<List> result = new ArrayList<>();
        try (Statement statement = jdbcConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                result.add(Arrays.asList(
                        resultSet.getInt(1),
                        resultSet.getString(2)));
            }
        }
        Assertions.assertIterableEquals(TEST_DATASET, result);
    }

    @AfterEach
    public void closeGreenplumContainer() throws SQLException, IOException {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        // remove the temp test.db file
        Files.delete(new File(tmpdir + "test.db").toPath());
    }

    @Override
    protected void executeExtraCommands(GenericContainer<?> container) throws IOException, InterruptedException {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + THIRD_PARTY_PLUGINS_URL);
        Assertions.assertEquals(0, extraCommands.getExitCode());

        try {
            initTestDb();
            // copy db file to container, dist file path in container is /tmp/seatunnel/data/test.db
            Path path = new File(tmpdir + "test.db").toPath();
            byte[] bytes = Files.readAllBytes(path);
            container.copyFileToContainer(Transferable.of(bytes), Paths.get(SEATUNNEL_HOME, "data", "test.db").toString());
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

}
