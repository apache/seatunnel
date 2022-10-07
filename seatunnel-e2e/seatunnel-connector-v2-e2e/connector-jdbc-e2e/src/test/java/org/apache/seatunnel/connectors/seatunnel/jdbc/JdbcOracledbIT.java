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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcOracledbIT extends AbstractJdbcIT {

    private static final String DOCKER_IMAGE = "gvenzl/oracle-xe:18.4.0-slim";
    private static final String NETWORK_ALIASES = "e2e_oracleDb";
    private static final String DRIVER_CLASS = "oracle.jdbc.OracleDriver";
    private static final int PORT = 1521;
    private static final String URL = "jdbc:oracle:thin:@" + HOST + ":%s/%s";
    private static final String USERNAME = "test";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "xepdb1";
    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String DRIVER_JAR = "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/12.2.0.1/ojdbc8-12.2.0.1.jar";
    private static final String CONFIG_FILE = "/jdbc_oracle_source_to_sink.conf";
    private static final String DDL_SOURCE = "create table " + SOURCE_TABLE + " (\n" +
        "  varchar_10_col   varchar2(10),\n" +
        "  char_10_col      char(10),\n" +
        "  clob_col         clob,\n" +
        "  number_3_sf_2_dp  number(3, 2),\n" +
        "  integer_col       integer,\n" +
        "  float_col         float(10),\n" +
        "  real_col          real,\n" +
        "  binary_float_col  binary_float,\n" +
        "  binary_double_col binary_double,\n" +
        "  date_col                      date,\n" +
        "  timestamp_with_3_frac_sec_col timestamp(3),\n" +
        "  timestamp_with_tz             timestamp with time zone,\n" +
        "  timestamp_with_local_tz       timestamp with local time zone,\n" +
        "  raw_col  raw(1000),\n" +
        "  blob_col blob\n" +
        ")";
    private static final String DDL_SINK = "create table " + SINK_TABLE + "(\n" +
        "  varchar_10_col   varchar2(10),\n" +
        "  char_10_col      char(10),\n" +
        "  clob_col         clob,\n" +
        "  number_3_sf_2_dp  number(3, 2),\n" +
        "  integer_col       integer,\n" +
        "  float_col         float(10),\n" +
        "  real_col          real,\n" +
        "  binary_float_col  binary_float,\n" +
        "  binary_double_col binary_double,\n" +
        "  date_col                      date,\n" +
        "  timestamp_with_3_frac_sec_col timestamp(3),\n" +
        "  timestamp_with_tz             timestamp with time zone,\n" +
        "  timestamp_with_local_tz       timestamp with local time zone,\n" +
        "  raw_col  raw(1000),\n" +
        "  blob_col blob\n" +
        ")";
    private static final String INIT_DATA_SQL = "insert into " + SOURCE_TABLE + " (\n" +
        "  varchar_10_col,\n" +
        "  char_10_col,\n" +
        "  clob_col,\n" +
        "  number_3_sf_2_dp,\n" +
        "  integer_col,\n" +
        "  float_col,\n" +
        "  real_col,\n" +
        "  binary_float_col,\n" +
        "  binary_double_col,\n" +
        "  date_col,\n" +
        "  timestamp_with_3_frac_sec_col,\n" +
        "  timestamp_with_tz,\n" +
        "  timestamp_with_local_tz,\n" +
        "  raw_col,\n" +
        "  blob_col\n" +
        ")values(\n" +
        "\t?,?,?,?,?,?,?,?,?,?,?,?,?,rawtohex(?),rawtohex(?)\n" +
        ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        containerEnv.put("ORACLE_PASSWORD", PASSWORD);
        containerEnv.put("APP_USER", USERNAME);
        containerEnv.put("APP_USER_PASSWORD", PASSWORD);
        String jdbcUrl = String.format(URL, PORT, DATABASE);
        return JdbcCase.builder().dockerImage(DOCKER_IMAGE).networkAliases(NETWORK_ALIASES).containerEnv(containerEnv).driverClass(DRIVER_CLASS)
            .host(HOST).port(PORT).jdbcUrl(jdbcUrl).userName(USERNAME).password(PASSWORD).dataBase(DATABASE)
            .sourceTable(SOURCE_TABLE).sinkTable(SINK_TABLE).driverJar(DRIVER_JAR)
            .ddlSource(DDL_SOURCE).ddlSink(DDL_SINK).initDataSql(INIT_DATA_SQL).configFile(CONFIG_FILE).seaTunnelRow(initTestData()).build();
    }

    @Override
    void compareResult() throws SQLException {
        String sql = "select a,b,c from " + SINK_TABLE;
        List<Object> result = new ArrayList<>();
        try (Statement statement = jdbcConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                for (int index = 0; index < this.getJdbcCase().getSeaTunnelRow().getFields().length; index++) {
                    result.add(resultSet.getObject(index + 1));
                }
            }
        }
        Assertions.assertIterableEquals(Arrays.asList(this.getJdbcCase().getSeaTunnelRow().getFields()), result);
        clearSinkTable();
    }

    @Override
    void clearSinkTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s", SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("test oracle server image error", e);
        }
    }

    @Override
    SeaTunnelRow initTestData() {
        return new SeaTunnelRow(
            new Object[]{"varchar", "char10col", "clobS", 1.12, 2022, 1.2222, 1.22222, 1.22222, 1.22222,
                LocalDate.now(),
                LocalDateTime.now(),
                LocalDateTime.now(),
                LocalDateTime.now(),
                "raw", "blob"
            });
    }
}
