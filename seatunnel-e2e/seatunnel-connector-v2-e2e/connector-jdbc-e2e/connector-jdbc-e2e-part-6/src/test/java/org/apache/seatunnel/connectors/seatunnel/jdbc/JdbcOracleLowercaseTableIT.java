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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleURLParser;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcOracleLowercaseTableIT extends AbstractJdbcIT {

    private static final String ORACLE_IMAGE = "gvenzl/oracle-xe:21-slim-faststart";
    private static final String ORACLE_NETWORK_ALIASES = "e2e_oracleDb";
    private static final String DRIVER_CLASS = "oracle.jdbc.OracleDriver";
    private static final int ORACLE_PORT = 1521;
    private static final String ORACLE_URL = "jdbc:oracle:thin:@" + HOST + ":%s/%s";
    private static final String USERNAME = "TESTUSER";
    private static final String PASSWORD = "testPassword";
    private static final String DATABASE = "XE";
    private static final String SCHEMA = USERNAME;
    private static final String SOURCE_TABLE = "E2E_TABLE_SOURCE_LOWER";
    private static final String SINK_TABLE = "E2E_TABLE_SINK_LOWER";
    private static final String CATALOG_TABLE = "e2e_table_catalog_lower";
    // no execute conf just test lower oracle create table
    private static final List<String> CONFIG_FILE = Lists.newArrayList();

    private static final String CREATE_SQL =
            "create table %s\n"
                    + "(\n"
                    + "    VARCHAR_10_COL                varchar2(10),\n"
                    + "    CHAR_10_COL                   char(10),\n"
                    + "    CLOB_COL                      clob,\n"
                    + "    NUMBER_3_SF_2_DP              number(3, 2),\n"
                    + "    INTEGER_COL                   integer,\n"
                    + "    FLOAT_COL                     float(10),\n"
                    + "    REAL_COL                      real,\n"
                    + "    BINARY_FLOAT_COL              binary_float,\n"
                    + "    BINARY_DOUBLE_COL             binary_double,\n"
                    + "    DATE_COL                      date,\n"
                    + "    TIMESTAMP_WITH_3_FRAC_SEC_COL timestamp(3),\n"
                    + "    TIMESTAMP_WITH_LOCAL_TZ       timestamp with local time zone,\n"
                    + "    constraint PK_T_COL1 primary key (INTEGER_COL)"
                    + ")";

    private static final String SINK_CREATE_SQL =
            "create table %s\n"
                    + "(\n"
                    + "    VARCHAR_10_COL                varchar2(10),\n"
                    + "    CHAR_10_COL                   char(10),\n"
                    + "    CLOB_COL                      clob,\n"
                    + "    NUMBER_3_SF_2_DP              number(3, 2),\n"
                    + "    INTEGER_COL                   integer,\n"
                    + "    FLOAT_COL                     float(10),\n"
                    + "    REAL_COL                      real,\n"
                    + "    BINARY_FLOAT_COL              binary_float,\n"
                    + "    BINARY_DOUBLE_COL             binary_double,\n"
                    + "    DATE_COL                      date,\n"
                    + "    TIMESTAMP_WITH_3_FRAC_SEC_COL timestamp(3),\n"
                    + "    TIMESTAMP_WITH_LOCAL_TZ       timestamp with local time zone\n"
                    + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        containerEnv.put("ORACLE_PASSWORD", PASSWORD);
        containerEnv.put("APP_USER", USERNAME);
        containerEnv.put("APP_USER_PASSWORD", PASSWORD);
        String jdbcUrl = String.format(ORACLE_URL, ORACLE_PORT, SCHEMA);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(SCHEMA, SOURCE_TABLE, fieldNames);

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
                .schema(SCHEMA)
                .sourceTable(SOURCE_TABLE)
                .sinkTable(SINK_TABLE)
                .catalogDatabase(DATABASE)
                .catalogSchema(SCHEMA)
                .catalogTable(CATALOG_TABLE)
                .createSql(CREATE_SQL)
                .sinkCreateSql(SINK_CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/12.2.0.1/ojdbc8-12.2.0.1.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "VARCHAR_10_COL",
                    "CHAR_10_COL",
                    "CLOB_COL",
                    "NUMBER_3_SF_2_DP",
                    "INTEGER_COL",
                    "FLOAT_COL",
                    "REAL_COL",
                    "BINARY_FLOAT_COL",
                    "BINARY_DOUBLE_COL",
                    "DATE_COL",
                    "TIMESTAMP_WITH_3_FRAC_SEC_COL",
                    "TIMESTAMP_WITH_LOCAL_TZ"
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
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
                                Timestamp.valueOf(LocalDateTime.now())
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(ORACLE_IMAGE);

        GenericContainer<?> container =
                new OracleContainer(imageName)
                        .withDatabaseName(SCHEMA)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("sql/oracle_init.sql"),
                                "/container-entrypoint-startdb.d/init.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(ORACLE_NETWORK_ALIASES)
                        .withExposedPorts(ORACLE_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(ORACLE_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", ORACLE_PORT, ORACLE_PORT)));

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    protected void clearTable(String database, String schema, String table) {
        clearTable(schema, table);
    }

    @Override
    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(schema, table);
    }

    @Override
    protected void initCatalog() {
        String jdbcUrl = jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost());
        catalog =
                new OracleCatalog(
                        "oracle",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        OracleURLParser.parse(jdbcUrl),
                        SCHEMA);
        catalog.open();
    }

    @Test
    public void testCatalog() {
        TablePath tablePathOracle = TablePath.of("XE", "TESTUSER", "E2E_TABLE_SOURCE_LOWER");
        TablePath tablePathOracleCreateTablePath =
                TablePath.of("XE", "TESTUSER", "E2E_TABLE_SOURCE_LOWER_AUTO");
        OracleCatalog oracleCatalog =
                new OracleCatalog(
                        "Oracle",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        OracleURLParser.parse(
                                jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost())),
                        SCHEMA);
        oracleCatalog.open();
        catalog.executeSql(
                tablePathOracle,
                "comment on column E2E_TABLE_SOURCE_LOWER.CHAR_10_COL is '\"#¥%……&*（）;;'',,..``````//''@特殊注释''\\''\"'");
        Assertions.assertTrue(oracleCatalog.tableExists(tablePathOracle));
        Assertions.assertEquals(
                oracleCatalog
                        .getTable(tablePathOracle)
                        .getTableSchema()
                        .getColumns()
                        .get(1)
                        .getComment(),
                "\"#¥%……&*（）;;',,..``````//'@特殊注释'\\'\"");
        oracleCatalog.truncateTable(tablePathOracle, true);
        Assertions.assertFalse(oracleCatalog.isExistsData(tablePathOracle));
        // create table with comment
        Assertions.assertFalse(oracleCatalog.tableExists(tablePathOracleCreateTablePath));
        oracleCatalog.createTable(
                tablePathOracleCreateTablePath, oracleCatalog.getTable(tablePathOracle), true);
        Assertions.assertTrue(oracleCatalog.tableExists(tablePathOracleCreateTablePath));
        final CatalogTable table = oracleCatalog.getTable(tablePathOracleCreateTablePath);
        Assertions.assertEquals(
                table.getTableSchema().getColumns().get(1).getComment(),
                "\"#¥%……&*（）;;',,..``````//'@特殊注释'\\'\"");
        testTableOfQuery(oracleCatalog);
        oracleCatalog.close();
    }

    @SneakyThrows
    private void testTableOfQuery(OracleCatalog oracleCatalog) {
        String querySql = "select * from TESTUSER.E2E_TABLE_SOURCE_LOWER";
        CatalogTable tableOfQuery = oracleCatalog.getTable(querySql);
        final List<Column> columns = tableOfQuery.getTableSchema().getColumns();
        Assertions.assertEquals(columns.get(0).getColumnLength(), 40);
        Assertions.assertEquals(columns.get(1).getColumnLength(), 40);
    }
}
