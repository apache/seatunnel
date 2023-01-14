/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcOracleIT extends AbstractJdbcIT {

    private static final String ORACLE_IMAGE = "gvenzl/oracle-xe:21-slim-faststart";
    private static final String ORACLE_NETWORK_ALIASES = "e2e_oracleDb";
    private static final String DRIVER_CLASS = "oracle.jdbc.OracleDriver";
    private static final int ORACLE_PORT = 1521;
    private static final String ORACLE_URL = "jdbc:oracle:thin:@" + HOST + ":%s/%s";
    private static final String USERNAME = "testUser";
    private static final String PASSWORD = "testPassword";
    private static final String DATABASE = "testDB";
    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String CONFIG_FILE = "/jdbc_oracle_source_to_sink.conf";

    private static final String CREATE_SQL = "create table %s\n" +
        "(\n" +
        "    varchar_10_col                varchar2(10),\n" +
        "    char_10_col                   char(10),\n" +
        "    clob_col                      clob,\n" +
        "    number_3_sf_2_dp              number(3, 2),\n" +
        "    integer_col                   integer,\n" +
        "    float_col                     float(10),\n" +
        "    real_col                      real,\n" +
        "    binary_float_col              binary_float,\n" +
        "    binary_double_col             binary_double,\n" +
        "    date_col                      date,\n" +
        "    timestamp_with_3_frac_sec_col timestamp(3),\n" +
        "    timestamp_with_local_tz       timestamp with local time zone,\n" +
        "    raw_col                       raw(1000),\n" +
        "    blob_col                      blob\n" +
        ");";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        containerEnv.put("ORACLE_PASSWORD", PASSWORD);
        containerEnv.put("APP_USER", USERNAME);
        containerEnv.put("APP_USER_PASSWORD", PASSWORD);
        String jdbcUrl = String.format(ORACLE_URL, ORACLE_PORT, DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(DATABASE, SOURCE_TABLE, fieldNames);

        return JdbcCase.builder()
            .dockerImage(ORACLE_IMAGE)
            .networkAliases(ORACLE_NETWORK_ALIASES)
            .containerEnv(containerEnv)
            .driverClass(DRIVER_CLASS)
            .host(HOST)
            .port(ORACLE_PORT)
            .localPort(ORACLE_PORT)
            .jdbcTemplate(ORACLE_URL)
            .jdbcUrl(jdbcUrl)
            .userName(USERNAME)
            .password(PASSWORD)
            .database(DATABASE)
            .sourceTable(SOURCE_TABLE)
            .sinkTable(SINK_TABLE)
            .createSql(CREATE_SQL)
            .configFile(CONFIG_FILE)
            .insertSql(insertSql)
            .testData(testDataSet)
            .build();
    }

    @Override
    void compareResult() throws SQLException, IOException {

    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/12.2.0.1/ojdbc8-12.2.0.1.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames = new String[] {
            "varchar_10_col",
            "char_10_col",
            "clob_col",
            "number_3_sf_2_dp",
            "integer_col",
            "float_col",
            "real_col",
            "binary_float_col",
            "binary_double_col",
            "date_col",
            "timestamp_with_3_frac_sec_col",
            "timestamp_with_local_tz",
            "raw_col",
            "blob_col"
        };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row = new SeaTunnelRow(
                new Object[] {
                    String.format("f%s", i),
                    String.format("f%s", i),
                    String.format("f%s", i),
                    BigDecimal.valueOf(1.1),
                    i,
                    Float.parseFloat("2.2"),
                    Float.parseFloat("2.2"),
                    Float.parseFloat("22.2"),
                    Double.parseDouble("2.2"),
                    Date.valueOf(LocalDate.now()),
                    Timestamp.valueOf(LocalDateTime.now()),
                    Timestamp.valueOf(LocalDateTime.now()),
                    "raw",
                    "blob".getBytes()
                });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(ORACLE_IMAGE);

        GenericContainer<?> container = new OracleContainer(imageName)
            .withDatabaseName(DATABASE)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withNetwork(NETWORK)
            .withNetworkAliases(ORACLE_NETWORK_ALIASES)
            .withExposedPorts(ORACLE_PORT)
            .waitingFor(Wait.forHealthcheck())
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(ORACLE_IMAGE)));

        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", ORACLE_PORT, ORACLE_PORT)));

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }
}
