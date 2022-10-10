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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.shaded.com.google.common.collect.Lists;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JdbcGbse8adbIT extends AbstractJdbcIT {

    private static final String DOCKER_IMAGE = "shihd/gbase8a:1.0";
    private static final String NETWORK_ALIASES = "e2e_gbase8aDb";
    private static final String DRIVER_CLASS = "com.gbase.jdbc.Driver";
    private static final int PORT = 5258;
    private static final String URL = "jdbc:gbase://" + HOST + ":%s/%s?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final String DATABASE = "gbase";
    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String DRIVER_JAR = "https://www.gbase8.cn/wp-content/uploads/2020/10/gbase-connector-java-8.3.81.53-build55.5.7-bin_min_mix.jar";
    private static final String CONFIG_FILE = "/jdbc_gbase8a_source_to_sink.conf";
    private static final String DDL_SOURCE = "CREATE TABLE " + SOURCE_TABLE + "(\n" +
        "  \"varchar_10_col\" varchar(10) DEFAULT NULL,\n" +
        "  \"char_10_col\" char(10) DEFAULT NULL,\n" +
        "  \"text_col\" text,\n" +
        "  \"decimal_col\" decimal(10,0) DEFAULT NULL,\n" +
        "  \"float_col\" float(12,0) DEFAULT NULL,\n" +
        "  \"int_col\" int(11) DEFAULT NULL,\n" +
        "  \"tinyint_col\" tinyint(4) DEFAULT NULL,\n" +
        "  \"smallint_col\" smallint(6) DEFAULT NULL,\n" +
        "  \"double_col\" double(22,0) DEFAULT NULL,\n" +
        "  \"bigint_col\" bigint(20) DEFAULT NULL,\n" +
        "  \"date_col\" date DEFAULT NULL,\n" +
        "  \"time_col\" time DEFAULT NULL,\n" +
        "  \"timestamp_col\" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
        "  \"datetime_col\" datetime DEFAULT NULL,\n" +
        "  \"blob_col\" blob\n" +
        ")";
    private static final String DDL_SINK = "CREATE TABLE " + SINK_TABLE + "(\n" +
        "  \"varchar_10_col\" varchar(10) DEFAULT NULL,\n" +
        "  \"char_10_col\" char(10) DEFAULT NULL,\n" +
        "  \"text_col\" text,\n" +
        "  \"decimal_col\" decimal(10,0) DEFAULT NULL,\n" +
        "  \"float_col\" float(12,0) DEFAULT NULL,\n" +
        "  \"int_col\" int(11) DEFAULT NULL,\n" +
        "  \"tinyint_col\" tinyint(4) DEFAULT NULL,\n" +
        "  \"smallint_col\" smallint(6) DEFAULT NULL,\n" +
        "  \"double_col\" double(22,0) DEFAULT NULL,\n" +
        "  \"bigint_col\" bigint(20) DEFAULT NULL,\n" +
        "  \"date_col\" date DEFAULT NULL,\n" +
        "  \"time_col\" time DEFAULT NULL,\n" +
        "  \"timestamp_col\" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
        "  \"datetime_col\" datetime DEFAULT NULL,\n" +
        "  \"blob_col\" blob\n" +
        ")";
    private static final String INIT_DATA_SQL = "insert into " + SOURCE_TABLE + " (\n" +
        "  varchar_10_col,\n" +
        "  char_10_col,\n" +
        "  text_col,\n" +
        "  decimal_col,\n" +
        "  float_col,\n" +
        "  int_col,\n" +
        "  tinyint_col,\n" +
        "  smallint_col,\n" +
        "  double_col,\n" +
        "  bigint_col,\n" +
        "  date_col,\n" +
        "  time_col,\n" +
        "  timestamp_col,\n" +
        "  datetime_col,\n" +
        "  blob_col\n" +
        ")values(\n" +
        "\t?,?,?,?,?,?,?,?,?,?,?,?,?,?,?\n" +
        ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(URL, PORT, DATABASE);
        return JdbcCase.builder().dockerImage(DOCKER_IMAGE).networkAliases(NETWORK_ALIASES).containerEnv(containerEnv).driverClass(DRIVER_CLASS)
            .host(HOST).port(PORT).jdbcUrl(jdbcUrl).userName(USERNAME).password(PASSWORD).dataBase(DATABASE)
            .sourceTable(SOURCE_TABLE).sinkTable(SINK_TABLE).driverJar(DRIVER_JAR)
            .ddlSource(DDL_SOURCE).ddlSink(DDL_SINK).initDataSql(INIT_DATA_SQL).configFile(CONFIG_FILE).seaTunnelRow(initTestData()).build();
    }

    @Override
    void compareResult() {
        String sourceSql = "select * from " + SOURCE_TABLE;
        String sinkSql = "select * from " + SINK_TABLE;
        List<String> columns = Lists.newArrayList("varchar_10_col", "char_10_col", "text_col", "decimal_col", "float_col", "int_col", "tinyint_col", "smallint_col", "double_col", "bigint_col", "date_col", "time_col", "timestamp_col", "datetime_col", "blob_col");
        try (Connection connection = initializeJdbcConnection(String.format(URL, PORT, "test"))) {
            Statement sourceStatement = connection.createStatement();
            Statement sinkStatement = connection.createStatement();
            ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                    for (String column : columns) {
                        Object source = sourceResultSet.getObject(column);
                        Object sink = sinkResultSet.getObject(column);
                        if (!Objects.deepEquals(source, sink)) {

                            InputStream sourceAsciiStream = sourceResultSet.getBinaryStream(column);
                            InputStream sinkAsciiStream = sinkResultSet.getBinaryStream(column);
                            String sourceValue = IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
                            String sinkValue = IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
                            Assertions.assertEquals(sourceValue, sinkValue);
                        }
                        Assertions.assertTrue(true);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("get gbase8a connection error", e);
        }
        clearSinkTable();
    }

    @Override
    void clearSinkTable() {
        try (Statement statement = initializeJdbcConnection(String.format(URL, PORT, "test")).createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s", SINK_TABLE));
        } catch (Exception e) {
            throw new RuntimeException("test gbase8a server image error", e);
        }
    }

    @Override
    SeaTunnelRow initTestData() {
        return new SeaTunnelRow(
            new Object[]{"varchar", "char10col1", "text_col".getBytes(StandardCharsets.UTF_8), 122, 122.0, 122, 100, 1212, 122.0,
                3112121, LocalDate.now(), LocalDateTime.now(), LocalDateTime.now(), LocalDateTime.now(), "blob".getBytes(StandardCharsets.UTF_8)});
    }

    protected Connection createAndChangeDatabase(Connection connection) {
        try {
            connection.prepareStatement("CREATE DATABASE test").executeUpdate();
            jdbcCase.setDataBase("test");
            connection.close();
            return initializeJdbcConnection(String.format(URL, PORT, jdbcCase.getDataBase()));
        } catch (Exception e) {
            throw new RuntimeException("create database error", e);
        }

    }
}
