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
import org.apache.seatunnel.connectors.seatunnel.jdbc.util.JdbcCompareUtil;

import org.junit.jupiter.api.Assertions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class JdbcStarRocksdbIT extends AbstractJdbcIT {

    private static final String DOCKER_IMAGE = "d87904488/starrocks-starter:2.2.1";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String NETWORK_ALIASES = "e2e_starRocksdb";
    private static final int SR_PORT = 9030;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String DATABASE = "test";
    private static final String URL = "jdbc:mysql://" + HOST + ":" + SR_PORT + "/" + DATABASE + "?createDatabaseIfNotExist=true";

    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String SR_DRIVER_JAR = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    private static final String COLUMN_STRING = "BIGINT_COL, LARGEINT_COL, SMALLINT_COL, TINYINT_COL, BOOLEAN_COL, DECIMAL_COL, DOUBLE_COL, FLOAT_COL, INT_COL, CHAR_COL, VARCHAR_11_COL, STRING_COL, DATETIME_COL, DATE_COL";
    private static final String CONFIG_FILE = "/jdbc_starrocks_source_to_sink.conf";

    private static final String DDL_SOURCE = "create table " + DATABASE + "." + SOURCE_TABLE + " (\n" +
        "  BIGINT_COL     BIGINT,\n" +
        "  LARGEINT_COL   LARGEINT,\n" +
        "  SMALLINT_COL   SMALLINT,\n" +
        "  TINYINT_COL    TINYINT,\n" +
        "  BOOLEAN_COL    BOOLEAN,\n" +
        "  DECIMAL_COL    DECIMAL,\n" +
        "  DOUBLE_COL     DOUBLE,\n" +
        "  FLOAT_COL      FLOAT,\n" +
        "  INT_COL        INT,\n" +
        "  CHAR_COL       CHAR,\n" +
        "  VARCHAR_11_COL VARCHAR(11),\n" +
        "  STRING_COL     STRING,\n" +
        "  DATETIME_COL   DATETIME,\n" +
        "  DATE_COL       DATE\n" +
        ")ENGINE=OLAP\n" +
        "DUPLICATE KEY(`BIGINT_COL`)\n" +
        "DISTRIBUTED BY HASH(`BIGINT_COL`) BUCKETS 1\n" +
        "PROPERTIES (\n" +
        "\"replication_num\" = \"1\",\n" +
        "\"in_memory\" = \"false\"," +
        "\"storage_format\" = \"DEFAULT\"" +
        ")";


    private static final String DDL_SINK = "create table " + DATABASE + "." + SINK_TABLE + " (\n" +
        "  BIGINT_COL     BIGINT,\n" +
        "  LARGEINT_COL   LARGEINT,\n" +
        "  SMALLINT_COL   SMALLINT,\n" +
        "  TINYINT_COL    TINYINT,\n" +
        "  BOOLEAN_COL    BOOLEAN,\n" +
        "  DECIMAL_COL    DECIMAL,\n" +
        "  DOUBLE_COL     DOUBLE,\n" +
        "  FLOAT_COL      FLOAT,\n" +
        "  INT_COL        INT,\n" +
        "  CHAR_COL       CHAR,\n" +
        "  VARCHAR_11_COL VARCHAR(11),\n" +
        "  STRING_COL     STRING,\n" +
        "  DATETIME_COL   DATETIME,\n" +
        "  DATE_COL       DATE\n" +
        ")ENGINE=OLAP\n" +
        "DUPLICATE KEY(`BIGINT_COL`)\n" +
        "DISTRIBUTED BY HASH(`BIGINT_COL`) BUCKETS 1\n" +
        "PROPERTIES (\n" +
        "\"replication_num\" = \"1\",\n" +
        "\"in_memory\" = \"false\"," +
        "\"storage_format\" = \"DEFAULT\"" +
        ")";

    private static final String INIT_DATA_SQL = "insert into " + DATABASE + "." + SOURCE_TABLE + " (\n" +
        "  BIGINT_COL,\n" +
        "  LARGEINT_COL,\n" +
        "  SMALLINT_COL,\n" +
        "  TINYINT_COL,\n" +
        "  BOOLEAN_COL,\n" +
        "  DECIMAL_COL,\n" +
        "  DOUBLE_COL,\n" +
        "  FLOAT_COL,\n" +
        "  INT_COL,\n" +
        "  CHAR_COL,\n" +
        "  VARCHAR_11_COL,\n" +
        "  STRING_COL,\n" +
        "  DATETIME_COL,\n" +
        "  DATE_COL\n" +
        ")values(\n" +
        "\t?,?,?,?,?,?,?,?,?,?,?,?,?,?\n" +
        ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(URL, SR_PORT);
        return JdbcCase.builder().dockerImage(DOCKER_IMAGE).networkAliases(NETWORK_ALIASES).containerEnv(containerEnv).driverClass(DRIVER_CLASS)
            .host(HOST).jdbcTemplate(URL).dataBase(DATABASE).port(SR_PORT).localPort(SR_PORT).jdbcUrl(jdbcUrl).userName(USERNAME).password(PASSWORD).dataBase(DATABASE)
            .sourceTable(SOURCE_TABLE).sinkTable(SINK_TABLE).driverJar(SR_DRIVER_JAR)
            .ddlSource(DDL_SOURCE).ddlSink(DDL_SINK).initDataSql(INIT_DATA_SQL).configFile(CONFIG_FILE).seaTunnelRow(initTestData()).build();
    }

    @Override
    void compareResult() {
        try (Connection connection = initializeJdbcConnection(URL)) {
            assertHasData(SOURCE_TABLE);
            assertHasData(SINK_TABLE);
            JdbcCompareUtil.compare(connection, String.format("select * from %s.%s limit 1", DATABASE, SOURCE_TABLE),
                String.format("select * from %s.%s limit 1", DATABASE, SINK_TABLE), COLUMN_STRING);
        } catch (Exception e) {
            throw new RuntimeException("get starRocks connection error", e);
        }
        clearSinkTable();
    }

    @Override
    void clearSinkTable() {
        try (Statement statement = initializeJdbcConnection(URL).createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s", DATABASE + "." + SINK_TABLE));
        } catch (Exception e) {
            throw new RuntimeException("test starrocks server image error", e);
        }
    }

    @Override
    SeaTunnelRow initTestData() {
        return new SeaTunnelRow(
            new Object[]{1234, 1123456, 12, 1, 0, 2222243.2222243, 2222243.22222, 1.22222, 12, "a", "VARCHAR_COL", "STRING_COL", "2022-08-13 17:35:59", "2022-08-13"});
    }

    private void assertHasData(String table) {
        try (Statement statement = initializeJdbcConnection(URL).createStatement()) {
            String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next());
        } catch (Exception e) {
            throw new RuntimeException("test starrocks server image error", e);
        }
    }
}
