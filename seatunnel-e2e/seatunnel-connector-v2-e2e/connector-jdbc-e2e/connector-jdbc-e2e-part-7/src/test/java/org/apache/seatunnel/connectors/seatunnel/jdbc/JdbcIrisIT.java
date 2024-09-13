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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.iris.IrisCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.iris.IrisDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class JdbcIrisIT extends AbstractJdbcIT {
    private static final String IRIS_IMAGE = "intersystems/iris-community:2023.1";
    private static final String IRIS_NETWORK_ALIASES = "e2e_irisDb";
    private static final String DRIVER_CLASS = "com.intersystems.jdbc.IRISDriver";
    private static final int IRIS_PORT = 1972;
    private static final String IRIS_URL = "jdbc:IRIS://" + HOST + ":%s/%s";
    private static final String USERNAME = "_SYSTEM";
    private static final String PASSWORD = "Seatunnel";
    private static final String DATABASE = "%SYS";
    private static final String SCHEMA = "test";
    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String CATALOG_TABLE = "e2e_table_catalog";
    private static final Integer GEN_ROWS = 100;
    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_iris_source_to_sink_with_full_type.conf");

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    private static final String CREATE_SQL =
            "create table %s\n"
                    + "(\n"
                    + "    BIGINT_COL                         BIGINT  primary key,\n"
                    + "    BIGINT_10_COL                      BIGINT(10),\n"
                    + "    BINARY_COL                         BINARY,\n"
                    + "    BINARY_10_COL                      BINARY(10),\n"
                    + "    BINARY_VARYING_COL                 BINARY VARYING,\n"
                    + "    BINARY_VARYING_10_COL              BINARY VARYING(10),\n"
                    + "    BIT_COL                            BIT,\n"
                    + "    BLOB_COL                           BLOB,\n"
                    + "    CHAR_COL                           CHAR,\n"
                    + "    CHAR_255_COL                       CHAR(255),\n"
                    + "    CHAR_VARYING_COL                   CHAR VARYING,\n"
                    + "    CHAR_VARYING_255_COL               CHAR VARYING(255),\n"
                    + "    CHARACTER_COL                      CHARACTER,\n"
                    + "    CHARACTER_120_COL                  CHARACTER(120),\n"
                    + "    CHARACTER_VARYING_COL              CHARACTER VARYING,\n"
                    + "    CHARACTER_VARYING_155_COL          CHARACTER VARYING(155),\n"
                    + "    CLOB_COL                           CLOB,\n"
                    + "    DATE_COL                           DATE,\n"
                    + "    DATETIME_COL                       DATETIME,\n"
                    + "    DATETIME2_COL                      DATETIME2,\n"
                    + "    DEC_COL                            DEC,\n"
                    + "    DEC_3_COL                          DEC(3),\n"
                    + "    DEC_3_2_COL                        DEC(3,2),\n"
                    + "    DECIMAL_COL                        DECIMAL,\n"
                    + "    DECIMAL_6_COL                      DECIMAL(6),\n"
                    + "    DECIMAL_6_2_COL                    DECIMAL(6,2),\n"
                    + "    DOUBLE_COL                         DOUBLE,\n"
                    + "    DOUBLE_PRECISION_COL               DOUBLE PRECISION,\n"
                    + "    FLOAT_COL                          FLOAT,\n"
                    + "    FLOAT_2_COL                        FLOAT(2),\n"
                    + "    IMAGE_COL                          IMAGE,\n"
                    + "    INT_COL                            INT,\n"
                    + "    INT_10_COL                         INT(10),\n"
                    + "    INTEGER_COL                        INTEGER,\n"
                    + "    LONG_COL                           LONG,\n"
                    + "    LONG_BINARY_COL                    LONG BINARY,\n"
                    + "    LONG_RAW_COL                       LONG RAW,\n"
                    + "    LONG_VARCHAR_COL                   LONG VARCHAR,\n"
                    + "    LONG_VARCHAR_10_COL                LONG VARCHAR(10),\n"
                    + "    LONGTEXT_COL                       LONGTEXT,\n"
                    + "    LONGVARBINARY_COL                  LONGVARBINARY,\n"
                    + "    LONGVARBINARY_10_COL               LONGVARBINARY(10),\n"
                    + "    LONGVARCHAR_COL                    LONGVARCHAR,\n"
                    + "    LONGVARCHAR_20_COL                 LONGVARCHAR(20),\n"
                    + "    MEDIUMINT_COL                      MEDIUMINT,\n"
                    + "    MEDIUMINT_10_COL                   MEDIUMINT(10),\n"
                    + "    MEDIUMTEXT_COL                     MEDIUMTEXT,\n"
                    + "    MONEY_COL                          MONEY,\n"
                    + "    NATIONAL_CHAR_COL                  NATIONAL CHAR,\n"
                    + "    NATIONAL_CHAR_200_COL              NATIONAL CHAR(200),\n"
                    + "    NATIONAL_CHAR_VARYING_COL          NATIONAL CHAR VARYING,\n"
                    + "    NATIONAL_CHAR_VARYING_100_COL      NATIONAL CHAR VARYING(100),\n"
                    + "    NATIONAL_CHARACTER_COL             NATIONAL CHARACTER,\n"
                    + "    NATIONAL_CHARACTER_233_COL         NATIONAL CHARACTER(233),\n"
                    + "    NCHAR_COL                          NCHAR,\n"
                    + "    NCHAR_22_COL                       NCHAR(22),\n"
                    + "    NTEXT_COL                          NTEXT,\n"
                    + "    NUMBER_COL                         NUMBER,\n"
                    + "    NUMBER_5_COL                       NUMBER(5),\n"
                    + "    NUMBER_5_3_COL                     NUMBER(5,3),\n"
                    + "    NUMERIC_COL                        NUMERIC,\n"
                    + "    NUMERIC_6_COL                      NUMERIC(6),\n"
                    + "    NUMERIC_6_3_COL                    NUMERIC(6,3),\n"
                    + "    NVARCHAR_COL                       NVARCHAR,\n"
                    + "    NVARCHAR_7_COL                     NVARCHAR(7),\n"
                    + "    NVARCHAR_7_3_COL                   NVARCHAR(7,3),\n"
                    + "    POSIXTIME_COL                      POSIXTIME,\n"
                    + "    RAW_10_COL                         RAW(10),\n"
                    + "    REAL_COL                           REAL,\n"
                    + "    SERIAL_COL                         SERIAL,\n"
                    + "    SMALLDATETIME_COL                  SMALLDATETIME,\n"
                    + "    SMALLINT_COL                       SMALLINT,\n"
                    + "    SMALLINT_3_COL                     SMALLINT(3),\n"
                    + "    SMALLMONEY_COL                     SMALLMONEY,\n"
                    + "    SYSNAME_COL                        SYSNAME,\n"
                    + "    TEXT_COL                           TEXT,\n"
                    + "    TIME_COL                           TIME,\n"
                    + "    TIME_3_COL                         TIME(3),\n"
                    + "    TIMESTAMP_COL                      TIMESTAMP,\n"
                    + "    TIMESTAMP2_COL                     TIMESTAMP2,\n"
                    + "    TINYINT_COL                        TINYINT,\n"
                    + "    TINYINT_10_COL                     TINYINT(10),\n"
                    + "    UNIQUEIDENTIFIER_COL               UNIQUEIDENTIFIER,\n"
                    + "    VARBINARY_COL                      VARBINARY,\n"
                    + "    VARBINARY_10_COL                   VARBINARY(10),\n"
                    + "    VARCHAR_COL                        VARCHAR,\n"
                    + "    VARCHAR_254_COL                    VARCHAR(254),\n"
                    + "    VARCHAR_254_10_COL                 VARCHAR(254,10),\n"
                    + "    VARCHAR2_10_COL                    VARCHAR2(10)\n"
                    + ")";

    private static final String[] fieldNames =
            new String[] {
                "BIGINT_COL",
                "BIGINT_10_COL",
                "BINARY_COL",
                "BINARY_10_COL",
                "BINARY_VARYING_COL",
                "BINARY_VARYING_10_COL",
                "BIT_COL",
                "BLOB_COL",
                "CHAR_COL",
                "CHAR_255_COL",
                "CHAR_VARYING_COL",
                "CHAR_VARYING_255_COL",
                "CHARACTER_COL",
                "CHARACTER_120_COL",
                "CHARACTER_VARYING_COL",
                "CHARACTER_VARYING_155_COL",
                "CLOB_COL",
                "DATE_COL",
                "DATETIME_COL",
                "DATETIME2_COL",
                "DEC_COL",
                "DEC_3_COL",
                "DEC_3_2_COL",
                "DECIMAL_COL",
                "DECIMAL_6_COL",
                "DECIMAL_6_2_COL",
                "DOUBLE_COL",
                "DOUBLE_PRECISION_COL",
                "FLOAT_COL",
                "FLOAT_2_COL",
                "IMAGE_COL",
                "INT_COL",
                "INT_10_COL",
                "INTEGER_COL",
                "LONG_COL",
                "LONG_BINARY_COL",
                "LONG_RAW_COL",
                "LONG_VARCHAR_COL",
                "LONG_VARCHAR_10_COL",
                "LONGTEXT_COL",
                "LONGVARBINARY_COL",
                "LONGVARBINARY_10_COL",
                "LONGVARCHAR_COL",
                "LONGVARCHAR_20_COL",
                "MEDIUMINT_COL",
                "MEDIUMINT_10_COL",
                "MEDIUMTEXT_COL",
                "MONEY_COL",
                "NATIONAL_CHAR_COL",
                "NATIONAL_CHAR_200_COL",
                "NATIONAL_CHAR_VARYING_COL",
                "NATIONAL_CHAR_VARYING_100_COL",
                "NATIONAL_CHARACTER_COL",
                "NATIONAL_CHARACTER_233_COL",
                "NCHAR_COL",
                "NCHAR_22_COL",
                "NTEXT_COL",
                "NUMBER_COL",
                "NUMBER_5_COL",
                "NUMBER_5_3_COL",
                "NUMERIC_COL",
                "NUMERIC_6_COL",
                "NUMERIC_6_3_COL",
                "NVARCHAR_COL",
                "NVARCHAR_7_COL",
                "NVARCHAR_7_3_COL",
                "POSIXTIME_COL",
                "RAW_10_COL",
                "REAL_COL",
                "SERIAL_COL",
                "SMALLDATETIME_COL",
                "SMALLINT_COL",
                "SMALLINT_3_COL",
                "SMALLMONEY_COL",
                "SYSNAME_COL",
                "TEXT_COL",
                "TIME_COL",
                "TIME_3_COL",
                "TIMESTAMP_COL",
                "TIMESTAMP2_COL",
                "TINYINT_COL",
                "TINYINT_10_COL",
                "UNIQUEIDENTIFIER_COL",
                "VARBINARY_COL",
                "VARBINARY_10_COL",
                "VARCHAR_COL",
                "VARCHAR_254_COL",
                "VARCHAR_254_10_COL",
                "VARCHAR2_10_COL"
            };

    @Test
    public void testSampleDataFromColumnSuccess() throws Exception {
        JdbcDialect dialect = new IrisDialect();
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of(DATABASE, SCHEMA, SOURCE_TABLE))
                        .build();
        Object[] bigintCols =
                dialect.sampleDataFromColumn(connection, table, "BIGINT_COL", 1, 1024);
        Assertions.assertEquals(GEN_ROWS, bigintCols.length);
    }

    @Test
    @Override
    public void testCatalog() {
        if (catalog == null) {
            return;
        }
        TablePath sourceTablePath =
                new TablePath(
                        jdbcCase.getDatabase(), jdbcCase.getSchema(), jdbcCase.getSourceTable());
        TablePath targetTablePath =
                new TablePath(
                        jdbcCase.getCatalogDatabase(),
                        jdbcCase.getCatalogSchema(),
                        jdbcCase.getCatalogTable());

        CatalogTable catalogTable = catalog.getTable(sourceTablePath);
        catalog.createTable(targetTablePath, catalogTable, false);
        Assertions.assertTrue(catalog.tableExists(targetTablePath));

        catalog.dropTable(targetTablePath, false);
        Assertions.assertFalse(catalog.tableExists(targetTablePath));
    }

    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    @TestTemplate
    public void testUpsert(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/jdbc_iris_upsert.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        try (Statement statement = connection.createStatement();
                ResultSet sink =
                        statement.executeQuery(
                                "SELECT * FROM test.e2e_upsert_table_sink ORDER BY pk_id")) {
            String[] fieldNames = new String[] {"pk_id", "name", "score"};
            Object[] sinkResult = toArrayResult(sink, fieldNames);
            Assertions.assertEquals(2, sinkResult.length);
            Assertions.assertEquals(3, ((Object[]) sinkResult[0]).length);
            Assertions.assertEquals("A_1", ((Object[]) sinkResult[0])[1]);
        } catch (SQLException | IOException e) {
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.DATA_COMPARISON_FAILED, e);
        }
    }

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        containerEnv.put("IRIS_PASSWORD", PASSWORD);
        containerEnv.put("APP_USER", USERNAME);
        containerEnv.put("APP_USER_PASSWORD", PASSWORD);
        String jdbcUrl = String.format(IRIS_URL, IRIS_PORT, DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(SCHEMA, SOURCE_TABLE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(IRIS_IMAGE)
                .networkAliases(IRIS_NETWORK_ALIASES)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(IRIS_PORT)
                .localPort(IRIS_PORT)
                .jdbcTemplate(IRIS_URL)
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
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    protected void createNeededTables() {
        try (Statement statement = connection.createStatement()) {
            String createTemplate = jdbcCase.getCreateSql();

            String createSource =
                    String.format(
                            createTemplate,
                            buildTableInfoWithSchema(
                                    jdbcCase.getDatabase(),
                                    jdbcCase.getSchema(),
                                    jdbcCase.getSourceTable()));
            statement.execute(createSource);

            String upsertSinkSql =
                    "CREATE TABLE test.e2e_upsert_table_sink (\n"
                            + "\"pk_id\" INT PRIMARY KEY,\n"
                            + "\"name\" VARCHAR(50),\n"
                            + "\"score\" INT\n"
                            + ");";
            statement.execute(upsertSinkSql);

            connection.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
    }

    @Override
    public String insertTable(String schema, String table, String... fields) {
        String columns =
                Arrays.stream(fields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(fields).map(f -> "?").collect(Collectors.joining(", "));

        return "INSERT OR UPDATE "
                + buildTableInfoWithSchema(schema, table)
                + " ("
                + columns
                + " )"
                + " VALUES ("
                + placeholders
                + ")";
    }

    @Override
    void checkResult(String executeKey, TestContainer container, Container.ExecResult execResult) {
        defaultCompare(executeKey, fieldNames, "BIGINT_COL");
    }

    @Override
    String driverUrl() {
        // reference: https://intersystems-community.github.io/iris-driver-distribution/
        return "https://raw.githubusercontent.com/intersystems-community/iris-driver-distribution/main/JDBC/JDK18/intersystems-jdbc-3.8.4.jar";
    }

    @Override
    protected Class<?> loadDriverClass() {
        return super.loadDriverClassFromUrl();
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 1; i <= GEN_ROWS; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                Long.valueOf(i),
                                Long.valueOf(i),
                                "*".getBytes(StandardCharsets.UTF_8),
                                "123456".getBytes(StandardCharsets.UTF_8),
                                "*".getBytes(StandardCharsets.UTF_8),
                                "123456".getBytes(StandardCharsets.UTF_8),
                                i % 10 == 0 ? 1 : 0,
                                String.valueOf(i).getBytes(StandardCharsets.UTF_8),
                                "*",
                                String.valueOf(i),
                                "*",
                                String.valueOf(i),
                                "*",
                                String.valueOf(i),
                                "*",
                                String.valueOf(i),
                                String.valueOf(i),
                                Date.valueOf(LocalDate.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 2),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 2),
                                Double.parseDouble("1.111"),
                                Double.parseDouble("1.111111"),
                                Float.parseFloat("1.1"),
                                Float.parseFloat("1.11"),
                                String.valueOf(i).getBytes(),
                                i,
                                i,
                                i,
                                Long.valueOf(i),
                                String.valueOf(i).getBytes(),
                                String.valueOf(i).getBytes(),
                                String.valueOf(i),
                                String.valueOf(i),
                                String.valueOf(i),
                                String.valueOf(i).getBytes(),
                                String.valueOf(i).getBytes(),
                                String.valueOf(i),
                                String.valueOf(i),
                                i,
                                i,
                                String.valueOf(i),
                                i,
                                "*",
                                String.valueOf(i),
                                "*",
                                String.valueOf(i),
                                "*",
                                String.valueOf(i),
                                "*",
                                String.valueOf(i),
                                String.valueOf(i),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 3),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 3),
                                "1",
                                "1",
                                "1.111",
                                Time.valueOf(LocalTime.now()),
                                "10".getBytes(),
                                Double.parseDouble("1.11"),
                                Long.valueOf(i),
                                Timestamp.valueOf(LocalDateTime.now()),
                                i,
                                i,
                                i,
                                "F4526E29-8B4A-4449-AA90-2A7DF971F221",
                                String.valueOf(i),
                                Time.valueOf(LocalTime.now()),
                                Time.valueOf(LocalTime.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                i,
                                i,
                                "3E8B5AC7-D63A-4202-83E1-A576EBE11557",
                                "*".getBytes(),
                                String.valueOf(i).getBytes(),
                                "*",
                                String.valueOf(i),
                                "1.11",
                                String.valueOf(i)
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer(IRIS_IMAGE)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("password/password.txt"),
                                "/tmp/password.txt")
                        .withCommand("--password-file /tmp/password.txt")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(IRIS_NETWORK_ALIASES)
                        .withExposedPorts(IRIS_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IRIS_IMAGE)));

        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", IRIS_PORT, IRIS_PORT)));

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
                new IrisCatalog(
                        "iris",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        JdbcUrlUtil.getUrlInfo(jdbcUrl));
        // set connection
        ((IrisCatalog) catalog).setConnection(jdbcUrl, connection);
        catalog.open();
    }
}
