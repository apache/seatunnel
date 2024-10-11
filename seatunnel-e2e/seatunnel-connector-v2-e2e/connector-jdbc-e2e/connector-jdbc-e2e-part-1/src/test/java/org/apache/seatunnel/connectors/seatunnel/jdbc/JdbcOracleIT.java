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

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleURLParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Statement;
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
    private static final String USERNAME = "TESTUSER";
    private static final String PASSWORD = "testPassword";
    private static final String DATABASE = "XE";
    private static final String SCHEMA = USERNAME;
    private static final String SOURCE_TABLE = "E2E_TABLE_SOURCE";
    private static final String SINK_TABLE = "E2E_TABLE_SINK";
    private static final String CATALOG_TABLE = "E2E_TABLE_CATALOG";
    private static final List<String> CONFIG_FILE =
            Lists.newArrayList(
                    "/jdbc_oracle_source_to_sink.conf",
                    "/jdbc_oracle_source_to_sink_use_select1.conf",
                    "/jdbc_oracle_source_to_sink_use_select2.conf",
                    "/jdbc_oracle_source_to_sink_use_select3.conf");

    private static final String CREATE_SQL =
            "create table %s\n"
                    + "(\n"
                    + "    VARCHAR_10_COL                varchar2(10),\n"
                    + "    CHAR_10_COL                   char(10),\n"
                    + "    CLOB_COL                      clob,\n"
                    + "    NUMBER_1             number(1),\n"
                    + "    NUMBER_6             number(6),\n"
                    + "    NUMBER_10             number(10),\n"
                    + "    NUMBER_3_SF_2_DP              number(3, 2),\n"
                    + "    NUMBER_7_SF_N2_DP             number(7, -2),\n"
                    + "    INTEGER_COL                   integer,\n"
                    + "    FLOAT_COL                     float(10),\n"
                    + "    REAL_COL                      real,\n"
                    + "    BINARY_FLOAT_COL              binary_float,\n"
                    + "    BINARY_DOUBLE_COL             binary_double,\n"
                    + "    DATE_COL                      date,\n"
                    + "    TIMESTAMP_WITH_3_FRAC_SEC_COL timestamp(3),\n"
                    + "    TIMESTAMP_WITH_LOCAL_TZ       timestamp with local time zone,\n"
                    + "    XML_TYPE_COL                  \"SYS\".\"XMLTYPE\",\n"
                    + "    constraint PK_T_COL primary key (INTEGER_COL)"
                    + ")";

    private static final String SINK_CREATE_SQL =
            "create table %s\n"
                    + "(\n"
                    + "    VARCHAR_10_COL                varchar2(10),\n"
                    + "    CHAR_10_COL                   char(10),\n"
                    + "    CLOB_COL                      clob,\n"
                    + "    NUMBER_1             number(1),\n"
                    + "    NUMBER_6             number(6),\n"
                    + "    NUMBER_10             number(10),\n"
                    + "    NUMBER_3_SF_2_DP              number(3, 2),\n"
                    + "    NUMBER_7_SF_N2_DP             number(7, -2),\n"
                    + "    INTEGER_COL                   integer,\n"
                    + "    FLOAT_COL                     float(10),\n"
                    + "    REAL_COL                      real,\n"
                    + "    BINARY_FLOAT_COL              binary_float,\n"
                    + "    BINARY_DOUBLE_COL             binary_double,\n"
                    + "    DATE_COL                      date,\n"
                    + "    TIMESTAMP_WITH_3_FRAC_SEC_COL timestamp(3),\n"
                    + "    TIMESTAMP_WITH_LOCAL_TZ       timestamp with local time zone,\n"
                    + "    XML_TYPE_COL                  \"SYS\".\"XMLTYPE\"\n"
                    + ")";

    private static final String[] fieldNames =
            new String[] {
                "VARCHAR_10_COL",
                "CHAR_10_COL",
                "CLOB_COL",
                "NUMBER_1",
                "NUMBER_6",
                "NUMBER_10",
                "NUMBER_3_SF_2_DP",
                "NUMBER_7_SF_N2_DP",
                "INTEGER_COL",
                "FLOAT_COL",
                "REAL_COL",
                "BINARY_FLOAT_COL",
                "BINARY_DOUBLE_COL",
                "DATE_COL",
                "TIMESTAMP_WITH_3_FRAC_SEC_COL",
                "TIMESTAMP_WITH_LOCAL_TZ",
                "XML_TYPE_COL"
            };

    @Test
    public void testSampleDataFromColumnSuccess() throws Exception {
        JdbcDialect dialect = new OracleDialect();
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of(null, SCHEMA, SOURCE_TABLE))
                        .build();
        dialect.sampleDataFromColumn(connection, table, "INTEGER_COL", 1, 1024);

        table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of(null, SCHEMA, SOURCE_TABLE))
                        .query(
                                "select * from "
                                        + quoteIdentifier(SOURCE_TABLE)
                                        + " where INTEGER_COL = 1")
                        .build();
        dialect.sampleDataFromColumn(connection, table, "INTEGER_COL", 1, 1024);
    }

    @TestTemplate
    public void testOracleWithoutDecimalTypeNarrowing(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob(
                        "/jdbc_oracle_source_to_sink_without_decimal_type_narrowing.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

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
                // oracle jdbc not support getTables/getCatalog/getSchema , is empty
                .tablePathFullName(TablePath.DEFAULT.getFullName())
                .build();
    }

    @Override
    void checkResult(String executeKey, TestContainer container, Container.ExecResult execResult) {
        defaultCompare(executeKey, fieldNames, "INTEGER_COL");
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/12.2.0.1/ojdbc8-12.2.0.1.jar && wget https://repo1.maven.org/maven2/com/oracle/database/xml/xdb6/12.2.0.1/xdb6-12.2.0.1.jar && wget https://repo1.maven.org/maven2/com/oracle/database/xml/xmlparserv2/12.2.0.1/xmlparserv2-12.2.0.1.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                String.format("f%s", i),
                                String.format("f%s", i),
                                String.format("f%s", i),
                                1,
                                i * 10,
                                i * 1000,
                                BigDecimal.valueOf(1.1),
                                BigDecimal.valueOf(2400),
                                i,
                                Float.parseFloat("2.2"),
                                Float.parseFloat("2.2"),
                                Float.parseFloat("22.2"),
                                Double.parseDouble("2.2"),
                                Date.valueOf(LocalDate.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><project xmlns=\"http://maven.apache.org/POM/4.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd\"><name>SeaTunnel : E2E : Connector V2 : Oracle XMLType</name></project>"
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

    @BeforeAll
    @Override
    public void startUp() {
        super.startUp();
        // analyzeTable before execute job
        String analyzeTable =
                String.format(
                        "analyze table "
                                + quoteIdentifier(SOURCE_TABLE)
                                + " compute statistics for table");
        log.info("analyze table {}", analyzeTable);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(analyzeTable);
        } catch (Exception e) {
            log.error("Error when analyze table", e);
        }
    }
}
