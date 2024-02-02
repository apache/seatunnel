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

package org.apache.seatunnel.e2e.connector.doris;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.doris.util.DorisCatalogUtil;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class DorisIT extends AbstractDorisIT {
    private static final String TABLE = "doris_e2e_table";
    private static final String ALL_TYPE_TABLE = "doris_all_type_table";
    private static final String FAKESOURCE_ALL_TYPE_TABLE = "fake_all_type_table";
    private static final String DRIVER_JAR =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";

    private static final String sourceDB = "e2e_source";
    private static final String sinkDB = "e2e_sink";
    private Connection conn;

    private Map<String, String> checkColumnTypeMap = null;

    private static final String INIT_DATA_SQL =
            "insert into "
                    + sourceDB
                    + "."
                    + TABLE
                    + " (\n"
                    + "  F_ID,\n"
                    + "  F_INT,\n"
                    + "  F_BIGINT,\n"
                    + "  F_TINYINT,\n"
                    + "  F_SMALLINT,\n"
                    + "  F_DECIMAL,\n"
                    + "  F_LARGEINT,\n"
                    + "  F_BOOLEAN,\n"
                    + "  F_DOUBLE,\n"
                    + "  F_FLOAT,\n"
                    + "  F_CHAR,\n"
                    + "  F_VARCHAR_11,\n"
                    + "  F_STRING,\n"
                    + "  F_DATETIME_P,\n"
                    + "  F_DATETIME,\n"
                    + "  F_DATE\n"
                    + ")values(\n"
                    + "\t?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?\n"
                    + ")";

    private static final String INIT_ALL_TYPE_DATA_SQL =
            "insert into "
                    + sourceDB
                    + "."
                    + ALL_TYPE_TABLE
                    + " (\n"
                    + "  F_ID,\n"
                    + "  F_INT,\n"
                    + "  F_BIGINT,\n"
                    + "  F_TINYINT,\n"
                    + "  F_SMALLINT,\n"
                    + "  F_DECIMAL,\n"
                    + "  F_DECIMAL_V3,\n"
                    + "  F_LARGEINT,\n"
                    + "  F_BOOLEAN,\n"
                    + "  F_DOUBLE,\n"
                    + "  F_FLOAT,\n"
                    + "  F_CHAR,\n"
                    + "  F_VARCHAR_11,\n"
                    + "  F_STRING,\n"
                    + "  F_DATETIME_P,\n"
                    + "  F_DATETIME_V2,\n"
                    + "  F_DATETIME,\n"
                    + "  F_DATE,\n"
                    + "  F_DATE_V2,\n"
                    + "  F_JSON,\n"
                    + "  F_JSONB,\n"
                    + "  F_ARRAY_BOOLEAN,\n"
                    + "  F_ARRAY_BYTE,\n"
                    + "  F_ARRAY_SHOT,\n"
                    + "  F_ARRAY_INT,\n"
                    + "  F_ARRAY_BIGINT,\n"
                    + "  F_ARRAY_FLOAT,\n"
                    + "  F_ARRAY_DOUBLE,\n"
                    + "  F_ARRAY_STRING_CHAR,\n"
                    + "  F_ARRAY_STRING_VARCHAR,\n"
                    + "  F_ARRAY_STRING_LARGEINT,\n"
                    + "  F_ARRAY_STRING_STRING\n"
                    + ")values(\n"
                    + "\t?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?\n"
                    + ")";

    private final String COLUMN_STRING =
            "F_ID, F_INT, F_BIGINT, F_TINYINT, F_SMALLINT, F_DECIMAL, F_LARGEINT, F_BOOLEAN, F_DOUBLE, F_FLOAT, "
                    + "F_CHAR, F_VARCHAR_11, F_STRING, F_DATETIME_P, F_DATETIME, F_DATE";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/jdbc/lib && cd /tmp/seatunnel/plugins/jdbc/lib && wget "
                                        + DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @TestTemplate
    public void testDoris(TestContainer container) throws IOException, InterruptedException {
        initializeJdbcTable();
        batchInsertData();

        Container.ExecResult execResult = container.executeJob("/doris_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        checkSinkData();

        batchInsertData();
        Container.ExecResult execResult2 =
                container.executeJob("/doris_source_and_sink_2pc_false.conf");
        Assertions.assertEquals(0, execResult2.getExitCode());
        checkSinkData();

        batchInsertAllTypeData();
        Container.ExecResult execResult3 =
                container.executeJob("/doris_source_to_doris_sink_type_convertor.conf");
        Assertions.assertEquals(0, execResult3.getExitCode());
        checkAllTypeSinkData();

        Container.ExecResult execResult4 =
                container.executeJob("/fake_source_to_doris_type_convertor.conf");
        Assertions.assertEquals(0, execResult4.getExitCode());
        checkFakeSourceAllTypeSinkData();
    }

    private void checkAllTypeSinkData() {
        try {
            assertHasData(sourceDB, ALL_TYPE_TABLE);

            try (PreparedStatement ps =
                    conn.prepareStatement(DorisCatalogUtil.TABLE_SCHEMA_QUERY)) {
                ps.setString(1, sinkDB);
                ps.setString(2, ALL_TYPE_TABLE);
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    String columnType = resultSet.getString("COLUMN_TYPE");
                    Assertions.assertEquals(
                            checkColumnTypeMap.get(columnName).toUpperCase(Locale.ROOT),
                            columnType.toUpperCase(Locale.ROOT));
                }
            }

            String sourceSql =
                    String.format("select * from %s.%s order by F_ID ", sourceDB, ALL_TYPE_TABLE);
            String sinkSql =
                    String.format("select * from %s.%s order by F_ID", sinkDB, ALL_TYPE_TABLE);
            List<String> columnList =
                    Arrays.stream(COLUMN_STRING.split(","))
                            .map(x -> x.trim())
                            .collect(Collectors.toList());
            Statement sourceStatement =
                    conn.createStatement(
                            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            Statement sinkStatement =
                    conn.createStatement(
                            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            Assertions.assertEquals(
                    sourceResultSet.getMetaData().getColumnCount(),
                    sinkResultSet.getMetaData().getColumnCount());
            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                    for (String column : columnList) {
                        Object source = sourceResultSet.getObject(column);
                        Object sink = sinkResultSet.getObject(column);
                        if (!Objects.deepEquals(source, sink)) {
                            InputStream sourceAsciiStream = sourceResultSet.getBinaryStream(column);
                            InputStream sinkAsciiStream = sinkResultSet.getBinaryStream(column);
                            String sourceValue =
                                    IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
                            String sinkValue =
                                    IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
                            Assertions.assertEquals(sourceValue, sinkValue);
                        }
                    }
                }
            }
            // Check the row numbers is equal
            sourceResultSet.last();
            sinkResultSet.last();
            Assertions.assertEquals(sourceResultSet.getRow(), sinkResultSet.getRow());
        } catch (Exception e) {
            throw new RuntimeException("Doris connection error", e);
        }
    }

    private void checkFakeSourceAllTypeSinkData() {
        try {
            Map<String, String> fakeTypeMap = new LinkedHashMap<>();
            fakeTypeMap.put("c_bigint", "bigint(20)");
            fakeTypeMap.put("c_array", "ARRAY<INT(11)>");
            fakeTypeMap.put("c_string", "string");
            fakeTypeMap.put("c_boolean", "tinyint(1)");
            fakeTypeMap.put("c_tinyint", "tinyint(4)");
            fakeTypeMap.put("c_smallint", "smallint(6)");
            fakeTypeMap.put("c_int", "int(11)");
            fakeTypeMap.put("c_float", "float");
            fakeTypeMap.put("c_double", "double");
            fakeTypeMap.put("c_bytes", "string");
            fakeTypeMap.put("c_date", "date");
            fakeTypeMap.put("c_decimal", "decimalv3(10, 2)");
            fakeTypeMap.put("c_timestamp", "datetime(6)");
            fakeTypeMap.put("c_map", "json");
            try (PreparedStatement ps =
                    conn.prepareStatement(DorisCatalogUtil.TABLE_SCHEMA_QUERY)) {
                ps.setString(1, sinkDB);
                ps.setString(2, FAKESOURCE_ALL_TYPE_TABLE);
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    String columnType = resultSet.getString("COLUMN_TYPE");
                    Assertions.assertEquals(
                            fakeTypeMap.get(columnName).toUpperCase(Locale.ROOT),
                            columnType.toUpperCase(Locale.ROOT));
                }
            }

            BigDecimal bigDecimal = BigDecimal.valueOf(1091, 2);
            List<Map<String, Object>> fakeSourceTestData = new ArrayList<>();
            Map<String, Object> row1 = new HashMap<>();
            row1.put("c_bigint", 1L);
            row1.put("c_array", "[1, 2, 3]");
            row1.put("c_string", "1");
            row1.put("c_boolean", true);
            row1.put("c_tinyint", 1);
            row1.put("c_smallint", 2);
            row1.put("c_int", 1);
            row1.put("c_float", 1.0);
            row1.put("c_double", 1.0);
            row1.put("c_bytes", null);
            row1.put("c_date", "2023-04-22");
            row1.put("c_decimal", bigDecimal.toString());
            row1.put("c_timestamp", "2023-04-22T23:20:58");
            row1.put("c_map", "{\"1\":\"v\"}");

            Map<String, Object> row2 = new HashMap<>();
            row2.put("c_bigint", 1L);
            row2.put("c_array", "[1, 2, 3]");
            row2.put("c_string", "1");
            row2.put("c_boolean", true);
            row2.put("c_tinyint", 1);
            row2.put("c_smallint", 2);
            row2.put("c_int", 1);
            row2.put("c_float", 1.0);
            row2.put("c_double", 1.0);
            row2.put("c_bytes", null);
            row2.put("c_date", "2023-04-22");
            row2.put("c_decimal", bigDecimal.toString());
            row2.put("c_timestamp", "2023-04-22T23:20:58");
            row2.put("c_map", "{\"1\":\"v\"}");

            fakeSourceTestData.add(row1);
            fakeSourceTestData.add(row2);

            String sinkSql =
                    String.format(
                            "select * from %s.%s order by c_bigint",
                            sinkDB, FAKESOURCE_ALL_TYPE_TABLE);
            List<String> columnList =
                    fakeTypeMap.keySet().stream().map(x -> x.trim()).collect(Collectors.toList());
            Statement sinkStatement =
                    conn.createStatement(
                            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            for (Map<String, Object> row : fakeSourceTestData) {
                if (sinkResultSet.next()) {
                    for (String column : columnList) {
                        Object source = row.get(column);
                        if (column.equalsIgnoreCase("c_array")) {
                            String value = sinkResultSet.getString(column);
                            Assertions.assertEquals(source, value);
                            continue;
                        }
                        if (column.equalsIgnoreCase("c_bytes")) {
                            Object sink = sinkResultSet.getObject(column);
                            Assertions.assertEquals(source, sink);
                            continue;
                        }
                        Object sink = sinkResultSet.getObject(column);
                        Assertions.assertEquals(source.toString(), sink.toString());
                    }
                }
            }

            // Check the row numbers is equal
            sinkResultSet.last();
        } catch (Exception e) {
            throw new RuntimeException("Doris connection error", e);
        }
    }

    private void checkSinkData() {
        try {
            assertHasData(sourceDB, TABLE);

            String sourceSql =
                    String.format(
                            "select * from %s.%s where F_ID > 50 order by F_ID ", sourceDB, TABLE);
            String sinkSql = String.format("select * from %s.%s order by F_ID", sinkDB, TABLE);
            List<String> columnList =
                    Arrays.stream(COLUMN_STRING.split(","))
                            .map(String::trim)
                            .collect(Collectors.toList());
            Statement sourceStatement =
                    conn.createStatement(
                            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            Statement sinkStatement =
                    conn.createStatement(
                            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            Assertions.assertEquals(
                    sourceResultSet.getMetaData().getColumnCount(),
                    sinkResultSet.getMetaData().getColumnCount());
            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                    for (String column : columnList) {
                        Object source = sourceResultSet.getObject(column);
                        Object sink = sinkResultSet.getObject(column);
                        if (!Objects.deepEquals(source, sink)) {
                            InputStream sourceAsciiStream = sourceResultSet.getBinaryStream(column);
                            InputStream sinkAsciiStream = sinkResultSet.getBinaryStream(column);
                            String sourceValue =
                                    IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
                            String sinkValue =
                                    IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
                            Assertions.assertEquals(sourceValue, sinkValue);
                        }
                    }
                }
            }
            // Check the row numbers is equal
            sourceResultSet.last();
            sinkResultSet.last();
            Assertions.assertEquals(sourceResultSet.getRow(), sinkResultSet.getRow());
            clearSinkTable();
        } catch (Exception e) {
            throw new RuntimeException("Doris connection error", e);
        }
    }

    private void assertHasData(String db, String table) {
        try (Statement statement = conn.createStatement()) {
            String sql = String.format("select * from %s.%s limit 1", db, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next());
        } catch (Exception e) {
            throw new RuntimeException("test doris server image error", e);
        }
    }

    private void clearSinkTable() {
        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s.%s", sourceDB, TABLE));
            statement.execute(String.format("TRUNCATE TABLE %s.%s", sinkDB, TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("test doris server image error", e);
        }
    }

    private void initializeJdbcTable() {
        try {
            URLClassLoader urlClassLoader =
                    new URLClassLoader(
                            new URL[] {new URL(DRIVER_JAR)}, DorisIT.class.getClassLoader());
            Thread.currentThread().setContextClassLoader(urlClassLoader);
            Driver driver = (Driver) urlClassLoader.loadClass(DRIVER_CLASS).newInstance();
            Properties props = new Properties();
            props.put("user", USERNAME);
            props.put("password", PASSWORD);
            conn = driver.connect(String.format(URL, container.getHost()), props);
            try (Statement statement = conn.createStatement()) {
                // create test databases
                statement.execute(createDatabase(sourceDB));
                statement.execute(createDatabase(sinkDB));
                log.info("create source and sink database succeed");
                // create source and sink table
                statement.execute(createTableForTest(sourceDB));
                statement.execute(createTableForTest(sinkDB));
                statement.execute(createAllTypeTableForTest(sourceDB));
            } catch (SQLException e) {
                throw new RuntimeException("Initializing table failed!", e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Initializing jdbc failed!", e);
        }
    }

    private String createDatabase(String db) {
        return String.format("CREATE DATABASE IF NOT EXISTS %s ;", db);
    }

    private String createTableForTest(String db) {
        String createTableSql =
                "create table if not exists `%s`.`%s`(\n"
                        + "F_ID bigint null,\n"
                        + "F_INT int null,\n"
                        + "F_BIGINT bigint null,\n"
                        + "F_TINYINT tinyint null,\n"
                        + "F_SMALLINT smallint null,\n"
                        + "F_DECIMAL decimal(18,6) null,\n"
                        + "F_LARGEINT largeint null,\n"
                        + "F_BOOLEAN boolean null,\n"
                        + "F_DOUBLE double null,\n"
                        + "F_FLOAT float null,\n"
                        + "F_CHAR char null,\n"
                        + "F_VARCHAR_11 varchar(11) null,\n"
                        + "F_STRING string null,\n"
                        + "F_DATETIME_P datetime(6),\n"
                        + "F_DATETIME datetime,\n"
                        + "F_DATE date\n"
                        + ")\n"
                        + "UNIQUE KEY(`F_ID`)\n"
                        + "DISTRIBUTED BY HASH(`F_ID`) BUCKETS 1\n"
                        + "properties(\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\""
                        + ");";
        return String.format(createTableSql, db, TABLE);
    }

    private String createAllTypeTableForTest(String db) {
        String createTableSql =
                "create table if not exists `%s`.`%s`(\n"
                        + "F_ID bigint null,\n"
                        + "F_INT int null,\n"
                        + "F_BIGINT bigint null,\n"
                        + "F_TINYINT tinyint null,\n"
                        + "F_SMALLINT smallint null,\n"
                        + "F_DECIMAL decimal(18,6) null,\n"
                        + "F_DECIMAL_V3 decimalv3(28,10) null,\n"
                        + "F_LARGEINT largeint null,\n"
                        + "F_BOOLEAN boolean null,\n"
                        + "F_DOUBLE double null,\n"
                        + "F_FLOAT float null,\n"
                        + "F_CHAR char null,\n"
                        + "F_VARCHAR_11 varchar(11) null,\n"
                        + "F_STRING string null,\n"
                        + "F_DATETIME_P datetime(6),\n"
                        + "F_DATETIME_V2 datetimev2(6),\n"
                        + "F_DATETIME datetime,\n"
                        + "F_DATE date,\n"
                        + "F_DATE_V2 datev2,\n"
                        + "F_JSON json,\n"
                        + "F_JSONB jsonb,\n"
                        + "F_ARRAY_BOOLEAN ARRAY<boolean>,\n"
                        + "F_ARRAY_BYTE ARRAY<tinyint>,\n"
                        + "F_ARRAY_SHOT ARRAY<smallint>,\n"
                        + "F_ARRAY_INT ARRAY<int>,\n"
                        + "F_ARRAY_BIGINT ARRAY<bigint>,\n"
                        + "F_ARRAY_FLOAT ARRAY<float>,\n"
                        + "F_ARRAY_DOUBLE ARRAY<double>,\n"
                        + "F_ARRAY_STRING_CHAR ARRAY<char(10)>,\n"
                        + "F_ARRAY_STRING_VARCHAR ARRAY<varchar(100)>,\n"
                        + "F_ARRAY_STRING_LARGEINT ARRAY<largeint>,\n"
                        + "F_ARRAY_STRING_STRING ARRAY<string>\n"
                        + ")\n"
                        + "Duplicate KEY(`F_ID`)\n"
                        + "DISTRIBUTED BY HASH(`F_ID`) BUCKETS 1\n"
                        + "properties(\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\""
                        + ");";
        checkColumnTypeMap = new HashMap<>();
        checkColumnTypeMap.put("F_ID", "bigint(20)");
        checkColumnTypeMap.put("F_INT", "int(11)");
        checkColumnTypeMap.put("F_BIGINT", "bigint(20)");
        checkColumnTypeMap.put("F_TINYINT", "tinyint(4)");
        checkColumnTypeMap.put("F_SMALLINT", "smallint(6)");
        checkColumnTypeMap.put("F_DECIMAL", "decimalv3(18, 6)");
        checkColumnTypeMap.put("F_DECIMAL_V3", "decimalv3(28, 10)");
        checkColumnTypeMap.put("F_LARGEINT", "largeint");
        checkColumnTypeMap.put("F_BOOLEAN", "tinyint(1)");
        checkColumnTypeMap.put("F_DOUBLE", "double");
        checkColumnTypeMap.put("F_FLOAT", "float");
        checkColumnTypeMap.put("F_CHAR", "char(1)");
        checkColumnTypeMap.put("F_VARCHAR_11", "varchar(11)");
        checkColumnTypeMap.put("F_STRING", "string");
        checkColumnTypeMap.put("F_DATETIME_P", "datetime(6)");
        checkColumnTypeMap.put("F_DATETIME_V2", "datetime(6)");
        checkColumnTypeMap.put("F_DATETIME", "datetime(6)");
        checkColumnTypeMap.put("F_DATE", "date");
        checkColumnTypeMap.put("F_DATE_V2", "date");
        checkColumnTypeMap.put("F_JSON", "json");
        checkColumnTypeMap.put("F_JSONB", "json");
        checkColumnTypeMap.put("F_ARRAY_BOOLEAN", "ARRAY<tinyint(1)>");
        checkColumnTypeMap.put("F_ARRAY_BYTE", "ARRAY<tinyint(4)>");
        checkColumnTypeMap.put("F_ARRAY_SHOT", "ARRAY<smallint(6)>");
        checkColumnTypeMap.put("F_ARRAY_INT", "ARRAY<int(11)>");
        checkColumnTypeMap.put("F_ARRAY_BIGINT", "ARRAY<bigint(20)>");
        checkColumnTypeMap.put("F_ARRAY_FLOAT", "ARRAY<float>");
        checkColumnTypeMap.put("F_ARRAY_DOUBLE", "ARRAY<double>");
        checkColumnTypeMap.put("F_ARRAY_STRING_CHAR", "ARRAY<string>");
        checkColumnTypeMap.put("F_ARRAY_STRING_VARCHAR", "ARRAY<string>");
        checkColumnTypeMap.put("F_ARRAY_STRING_LARGEINT", "ARRAY<string>");
        checkColumnTypeMap.put("F_ARRAY_STRING_STRING", "ARRAY<string>");

        return String.format(createTableSql, db, ALL_TYPE_TABLE);
    }

    private void batchInsertData() {
        List<SeaTunnelRow> rows = genDorisTestData(100L);
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement preparedStatement = conn.prepareStatement(INIT_DATA_SQL)) {
                for (int i = 0; i < rows.size(); i++) {
                    for (int index = 0; index < rows.get(i).getFields().length; index++) {
                        preparedStatement.setObject(index + 1, rows.get(i).getFields()[index]);
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            conn.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new RuntimeException("get connection error", exception);
        }
        log.info("insert data succeed");
    }

    private void batchInsertAllTypeData() {
        List<SeaTunnelRow> rows = genDorisAllTypeTestData(100L);
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement preparedStatement =
                    conn.prepareStatement(INIT_ALL_TYPE_DATA_SQL)) {
                for (int i = 0; i < rows.size(); i++) {
                    for (int index = 0; index < rows.get(i).getFields().length; index++) {
                        preparedStatement.setObject(index + 1, rows.get(i).getFields()[index]);
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            conn.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new RuntimeException("get connection error", exception);
        }
        log.info("insert all type data succeed");
    }

    private List<SeaTunnelRow> genDorisTestData(Long nums) {
        List<SeaTunnelRow> datas = new ArrayList<>();
        for (int i = 0; i < nums; i++) {
            datas.add(
                    new SeaTunnelRow(
                            new Object[] {
                                Long.valueOf(i),
                                GenerateTestData.genInt(),
                                GenerateTestData.genBigint(),
                                GenerateTestData.genTinyint(),
                                GenerateTestData.genSmallint(),
                                GenerateTestData.genBigDecimal(18, 6),
                                GenerateTestData.genBigInteger(126),
                                GenerateTestData.genBoolean(),
                                GenerateTestData.genDouble(),
                                GenerateTestData.genFloat(0, 1000),
                                GenerateTestData.genString(1),
                                GenerateTestData.genString(11),
                                GenerateTestData.genString(12),
                                GenerateTestData.genDatetimeString(true),
                                GenerateTestData.genDatetimeString(false),
                                GenerateTestData.genDateString()
                            }));
        }
        log.info("generate test data succeed");
        return datas;
    }

    private List<SeaTunnelRow> genDorisAllTypeTestData(Long nums) {
        HashMap<String, String> stringStringHashMap = new HashMap<>();
        stringStringHashMap.put("1", "1");
        stringStringHashMap.put("2", "2");

        HashMap<String, Integer> stringIntHashMap = new HashMap<>();
        stringIntHashMap.put("1", 1);
        stringIntHashMap.put("2", 2);
        List<SeaTunnelRow> datas = new ArrayList<>();
        for (int i = 0; i < nums; i++) {
            datas.add(
                    new SeaTunnelRow(
                            new Object[] {
                                Long.valueOf(i),
                                GenerateTestData.genInt(),
                                GenerateTestData.genBigint(),
                                GenerateTestData.genTinyint(),
                                GenerateTestData.genSmallint(),
                                GenerateTestData.genBigDecimal(18, 6),
                                GenerateTestData.genBigDecimal(28, 10),
                                GenerateTestData.genBigInteger(126),
                                GenerateTestData.genBoolean(),
                                GenerateTestData.genDouble(),
                                GenerateTestData.genFloat(0, 1000),
                                GenerateTestData.genString(1),
                                GenerateTestData.genString(11),
                                GenerateTestData.genString(12),
                                GenerateTestData.genDatetimeString(false),
                                GenerateTestData.genDatetimeString(false),
                                GenerateTestData.genDatetimeString(true),
                                GenerateTestData.genDateString(),
                                GenerateTestData.genDateString(),
                                GenerateTestData.genJsonString(),
                                GenerateTestData.genJsonString(),
                                (new boolean[] {true, true, false}).toString(),
                                (new int[] {1, 2, 3}).toString(),
                                (new int[] {1, 2, 3}).toString(),
                                (new int[] {1, 2, 3}).toString(),
                                (new long[] {1L, 2L, 3L}).toString(),
                                (new float[] {1.0F, 1.0F, 1.0F}).toString(),
                                (new double[] {1.0, 1.0, 1.0}).toString(),
                                (new String[] {"1", "1"}).toString(),
                                (new String[] {"1", "1"}).toString(),
                                (new String[] {"1", "1"}).toString(),
                                (new String[] {"1", "1"}).toString()
                            }));
        }
        log.info("generate test data succeed");
        return datas;
    }

    @AfterAll
    public void close() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }
}
