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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcOracledbIT extends AbstractJdbcIT {

    private static final String DOCKER_IMAGE = "gvenzl/oracle-xe:18.4.0-slim";
    private static final String DRIVER_CLASS = "oracle.jdbc.OracleDriver";
    private static final int PORT = 1521;
    private static final String URL = "jdbc:oracle:thin:@" + HOST + ":%s/%s";
    private static final String USERNAME = "test";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "xepdb1";
    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String DRIVER_JAR = "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/12.2.0.1/ojdbc8-12.2.0.1.jar";
    private static final String CONFIG_FILE = "/jdbc_oracle_source_and_sink.conf";
    private static final String DDL_SOURCE = "CREATE TABLE source (a VARCHAR(5),\n" +
        "                b VARCHAR(30),\n" +
        "                c VARCHAR(20));  ";
    private static final String DDL_SINK = "CREATE TABLE sink (a VARCHAR(5),\n" +
        "                b VARCHAR(30),\n" +
        "                c VARCHAR(20));  ";
    private static final String INIT_DATA_SQL = "insert into source values(?,?,?);";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        containerEnv.put("ORACLE_PASSWORD", PASSWORD);
        containerEnv.put("APP_USER", USERNAME);
        containerEnv.put("APP_USER_PASSWORD", PASSWORD);
        String jdbcUrl = String.format(URL, PORT, DATABASE);
        return JdbcCase.builder().dockerImage(DOCKER_IMAGE).containerEnv(containerEnv).driverClass(DRIVER_CLASS)
            .host(HOST).port(PORT).jdbcUrl(jdbcUrl).userName(USERNAME).password(PASSWORD).dataBase(DATABASE)
            .sourceTable(SOURCE_TABLE).sinkTable(SINK_TABLE).driverJar(DRIVER_JAR)
            .ddlSource(DDL_SOURCE).ddlSink(DDL_SINK).initDataSql(INIT_DATA_SQL).configFile(CONFIG_FILE).seaTunnelRow(initTestData()).build();
    }

    @Override
    int compareResult() throws SQLException {
        String sql = "select a,b,c from sinks";
        List<Object> result = new ArrayList<>();
        try (Statement statement = jdbcConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                for (int index = 0; index < this.getJdbcCase().getSeaTunnelRow().getFields().length; index++) {
                    result.add(resultSet.getObject(index));
                }
            }
        }
        Assertions.assertIterableEquals(Arrays.asList(this.getJdbcCase().getSeaTunnelRow().getFields()), result);
        return 0;
    }

    @Override
    SeaTunnelRow initTestData() {
        return new SeaTunnelRow(new Object[]{"a", "b", "c"});
    }
}
