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
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class JdbcOracleIT extends SparkContainer {

    private OracleContainer oracleContainer;

    @BeforeEach
    public void startOracleContainer() throws ClassNotFoundException {
        oracleContainer = new OracleContainer(DockerImageName.parse("gvenzl/oracle-xe:18.4.0-slim"))
            .withNetwork(NETWORK)
            .withNetworkAliases("oracle")
            .withLogConsumer(new Slf4jLogConsumer(log));
        Startables.deepStart(Stream.of(oracleContainer)).join();
        log.info("Oracle container started");
        Class.forName(oracleContainer.getDriverClassName());
        Awaitility.given().ignoreExceptions()
            .await()
            .atMost(Duration.ofSeconds(60))
            .untilAsserted(this::initializeJdbcTable);
        batchInsertData();
    }

    private void initializeJdbcTable() {
        try (Connection connection = DriverManager.getConnection(oracleContainer.getJdbcUrl(), oracleContainer.getUsername(), oracleContainer.getPassword())) {
            Statement statement = connection.createStatement();
            String sourceSql = "CREATE TABLE source\n" +
                "   (\"ID\" NUMBER(20,0) NOT NULL ENABLE, \n" +
                "\t\"BINARY_DOUBLE1\" BINARY_DOUBLE, \n" +
                "\t\"BINARY_FLOAT1\" BINARY_FLOAT, \n" +
                "\t\"BLOB1\" BLOB, \n" +
                "\t\"CHAR1\" CHAR(100), \n" +
                "\t\"CLOB1\" CLOB, \n" +
                "\t\"DATE1\" DATE, \n" +
                "\t\"DOUBLE_PRECISION1\" FLOAT(10), \n" +
                "\t\"FLOAT1\" FLOAT(10), \n" +
                "\t\"INT1\" NUMBER(10,0), \n" +
                "\t\"INTEGER1\" NUMBER(20,0), \n" +
                "\t\"NCLOB1\" NCLOB, \n" +
                "\t\"NUMBER1\" NUMBER(30,0), \n" +
                "\t\"SMALLINT1\" NUMBER(10,0), \n" +
                "\t\"TIMESTAMP1\" TIMESTAMP (6) DEFAULT NULL, \n" +
                "\t\"VAR11\" VARCHAR2(255) DEFAULT NULL\n" +
                "   )";
            String sinkSql = "CREATE TABLE sink\n" +
                "   (\"ID\" NUMBER(20,0) NOT NULL ENABLE, \n" +
                "\t\"BINARY_DOUBLE1\" BINARY_DOUBLE, \n" +
                "\t\"BINARY_FLOAT1\" BINARY_FLOAT, \n" +
                "\t\"BLOB1\" BLOB, \n" +
                "\t\"CHAR1\" CHAR(100), \n" +
                "\t\"CLOB1\" CLOB, \n" +
                "\t\"DATE1\" DATE, \n" +
                "\t\"DOUBLE_PRECISION1\" FLOAT(10), \n" +
                "\t\"FLOAT1\" FLOAT(10), \n" +
                "\t\"INT1\" NUMBER(10,0), \n" +
                "\t\"INTEGER1\" NUMBER(20,0), \n" +
                "\t\"NCLOB1\" NCLOB, \n" +
                "\t\"NUMBER1\" NUMBER(30,0), \n" +
                "\t\"SMALLINT1\" NUMBER(10,0), \n" +
                "\t\"TIMESTAMP1\" TIMESTAMP (6) DEFAULT NULL, \n" +
                "\t\"VAR11\" VARCHAR2(255) DEFAULT NULL\n" +
                "   )";
            statement.execute(sourceSql);
            statement.execute(sinkSql);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Oracle table failed!", e);
        }
    }

    @SuppressWarnings("checkstyle:RegexpSingleline")
    private void batchInsertData() {
        try (Connection connection = DriverManager.getConnection(oracleContainer.getJdbcUrl(), oracleContainer.getUsername(), oracleContainer.getPassword())) {
            String sql =
                "INSERT INTO source (ID, BINARY_DOUBLE1, BINARY_FLOAT1, BLOB1, CHAR1, CLOB1, DATE1, DOUBLE_PRECISION1, FLOAT1, INT1, INTEGER1, NCLOB1, NUMBER1, SMALLINT1, TIMESTAMP1, VAR11) " +
                    "VALUES(1, 12.12, 123.123, '56415243484152', 'char1中', 'clob中文1234', TIMESTAMP '2022-06-01 10:57:52.000000', 123.1234, 12.123, 12, 1234, 'clob中文1234阿斯顿', -1344, 11, TIMESTAMP '2022-06-01 10:59:15.000000', 'varchar1234和总共')";
            Statement statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Batch insert data failed!", e);
        }
    }

    @Test
    public void testOracleSourceAndSink() throws SQLException, IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/jdbc/jdbc_oracle_source_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        // query result
        String sql = "select * from sink";
        try (Connection connection = DriverManager.getConnection(oracleContainer.getJdbcUrl(), oracleContainer.getUsername(), oracleContainer.getPassword())) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            List<String> result = Lists.newArrayList();
            while (resultSet.next()) {
                result.add(resultSet.getString("ID"));
            }
            Assertions.assertFalse(result.isEmpty());
        }
    }

    @AfterEach
    public void closeOracleContainer() {
        if (oracleContainer != null) {
            oracleContainer.stop();
        }
    }

}
