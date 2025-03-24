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
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.doris.util.DorisCatalogUtil;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class DorisIT extends AbstractDorisIT {
    private static final String UNIQUE_TABLE = "doris_e2e_unique_table";
    private static final String DUPLICATE_TABLE = "doris_duplicate_table";
    private static final String sourceDB = "e2e_source";
    private static final String sinkDB = "e2e_sink";
    private Connection conn;

    private Map<String, String> checkColumnTypeMap = null;

    private static final String INIT_UNIQUE_TABLE_DATA_SQL =
            "insert into "
                    + sourceDB
                    + "."
                    + UNIQUE_TABLE
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
                    + "  F_DATE,\n"
                    + "  MAP_VARCHAR_BOOLEAN,\n"
                    + "  MAP_CHAR_TINYINT,\n"
                    + "  MAP_STRING_SMALLINT,\n"
                    + "  MAP_INT_INT,\n"
                    + "  MAP_TINYINT_BIGINT,\n"
                    + "  MAP_SMALLINT_LARGEINT,\n"
                    + "  MAP_BIGINT_FLOAT,\n"
                    + "  MAP_LARGEINT_DOUBLE,\n"
                    + "  MAP_STRING_DECIMAL,\n"
                    + "  MAP_DECIMAL_DATE,\n"
                    + "  MAP_DATE_DATETIME,\n"
                    + "  MAP_DATETIME_CHAR,\n"
                    + "  MAP_CHAR_VARCHAR,\n"
                    + "  MAP_VARCHAR_STRING\n"
                    + ")values(\n"
                    + "\t?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?\n"
                    + ")";

    private static final String INIT_DUPLICATE_TABLE_DATA_SQL =
            "insert into "
                    + sourceDB
                    + "."
                    + DUPLICATE_TABLE
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
                    + "  F_ARRAY_STRING_STRING,\n"
                    + "  F_ARRAY_DECIMAL,\n"
                    + "  F_ARRAY_DATE,\n"
                    + "  F_ARRAY_DATETIME\n"
                    + ")values(\n"
                    + "\t?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?\n"
                    + ")";

    private final String DUPLICATE_TABLE_COLUMN_STRING =
            "F_ID, F_INT, F_BIGINT, F_TINYINT, F_SMALLINT, F_DECIMAL, F_DECIMAL_V3, F_LARGEINT, F_BOOLEAN, F_DOUBLE, F_FLOAT, F_CHAR, F_VARCHAR_11, F_STRING, F_DATETIME_P, F_DATETIME_V2, F_DATETIME, F_DATE, F_DATE_V2, F_JSON, F_JSONB, F_ARRAY_BOOLEAN, F_ARRAY_BYTE, F_ARRAY_SHOT, F_ARRAY_INT, F_ARRAY_BIGINT, F_ARRAY_FLOAT, F_ARRAY_DOUBLE, F_ARRAY_STRING_CHAR, F_ARRAY_STRING_VARCHAR, F_ARRAY_STRING_LARGEINT, F_ARRAY_STRING_STRING, F_ARRAY_DECIMAL, F_ARRAY_DATE, F_ARRAY_DATETIME";

    private final String UNIQUE_TABLE_COLUMN_STRING =
            "F_ID, F_INT, F_BIGINT, F_TINYINT, F_SMALLINT, F_DECIMAL, F_LARGEINT, F_BOOLEAN, F_DOUBLE, F_FLOAT, F_CHAR, F_VARCHAR_11, F_STRING, F_DATETIME_P, F_DATETIME, F_DATE, MAP_VARCHAR_BOOLEAN, MAP_CHAR_TINYINT, MAP_STRING_SMALLINT, MAP_INT_INT, MAP_TINYINT_BIGINT, MAP_SMALLINT_LARGEINT, MAP_BIGINT_FLOAT, MAP_LARGEINT_DOUBLE, MAP_STRING_DECIMAL, MAP_DECIMAL_DATE, MAP_DATE_DATETIME, MAP_DATETIME_CHAR, MAP_CHAR_VARCHAR, MAP_VARCHAR_STRING";

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
    public void testCustomSql(TestContainer container) throws IOException, InterruptedException {
        initializeJdbcTable();
        Container.ExecResult execResult =
                container.executeJob("/doris_source_and_sink_with_custom_sql.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(101, tableCount(sinkDB, UNIQUE_TABLE));
        clearUniqueTable();
    }

    @TestTemplate
    public void testDoris(TestContainer container) throws IOException, InterruptedException {
        initializeJdbcTable();
        batchInsertUniqueTableData();

        Container.ExecResult execResult = container.executeJob("/doris_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        checkSinkData();

        batchInsertUniqueTableData();
        Container.ExecResult execResult2 =
                container.executeJob("/doris_source_and_sink_2pc_false.conf");
        Assertions.assertEquals(0, execResult2.getExitCode());
        checkSinkData();

        batchInsertDuplicateTableData();
        Container.ExecResult execResult3 =
                container.executeJob("/doris_source_to_doris_sink_type_convertor.conf");
        Assertions.assertEquals(0, execResult3.getExitCode());
        checkAllTypeSinkData();
    }

    @TestTemplate
    public void testNoSchemaDoris(TestContainer container)
            throws IOException, InterruptedException {
        initializeJdbcTable();
        batchInsertUniqueTableData();
        Container.ExecResult execResult1 = container.executeJob("/doris_source_no_schema.conf");
        Assertions.assertEquals(0, execResult1.getExitCode());
        checkSinkData();
    }

    private void checkAllTypeSinkData() {
        try {
            assertHasData(sourceDB, DUPLICATE_TABLE);

            try (PreparedStatement ps =
                    conn.prepareStatement(DorisCatalogUtil.TABLE_SCHEMA_QUERY)) {
                ps.setString(1, sinkDB);
                ps.setString(2, DUPLICATE_TABLE);
                try (ResultSet resultSet = ps.executeQuery()) {
                    while (resultSet.next()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        String columnType = resultSet.getString("COLUMN_TYPE");
                        Assertions.assertEquals(
                                checkColumnTypeMap.get(columnName).toUpperCase(Locale.ROOT),
                                columnType.toUpperCase(Locale.ROOT));
                    }
                }
            }

            String sourceSql =
                    String.format("select * from %s.%s order by F_ID ", sourceDB, DUPLICATE_TABLE);
            String sinkSql =
                    String.format("select * from %s.%s order by F_ID", sinkDB, DUPLICATE_TABLE);
            checkSourceAndSinkTableDate(sourceSql, sinkSql, DUPLICATE_TABLE_COLUMN_STRING);
            clearDuplicateTable();
        } catch (Exception e) {
            throw new RuntimeException("Doris connection error", e);
        }
    }

    protected void checkSinkData() {
        try {
            assertHasData(sourceDB, UNIQUE_TABLE);
            assertHasData(sinkDB, UNIQUE_TABLE);

            PreparedStatement sourcePre =
                    conn.prepareStatement(DorisCatalogUtil.TABLE_SCHEMA_QUERY);
            sourcePre.setString(1, sourceDB);
            sourcePre.setString(2, UNIQUE_TABLE);
            ResultSet sourceResultSet = sourcePre.executeQuery();

            PreparedStatement sinkPre = conn.prepareStatement(DorisCatalogUtil.TABLE_SCHEMA_QUERY);
            sinkPre.setString(1, sinkDB);
            sinkPre.setString(2, UNIQUE_TABLE);
            ResultSet sinkResultSet = sinkPre.executeQuery();

            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                    String sourceColumnType = sourceResultSet.getString("COLUMN_TYPE");
                    String sinkColumnType = sinkResultSet.getString("COLUMN_TYPE");
                    // because seatunnel type can not save the scale and length of the key type and
                    // value type in the MapType,
                    // so we use the longest scale on the doris sink to prevent data overflow.
                    if (sourceColumnType.equalsIgnoreCase("map<varchar(200),tinyint(1)>")) {
                        Assertions.assertEquals("map<string,tinyint(1)>", sinkColumnType);
                        continue;
                    }

                    if (sourceColumnType.equalsIgnoreCase("map<char(1),tinyint(4)>")) {
                        Assertions.assertEquals("map<string,tinyint(4)>", sinkColumnType);
                        continue;
                    }

                    if (sourceColumnType.equalsIgnoreCase("map<smallint(6),largeint>")) {
                        Assertions.assertEquals(
                                "map<smallint(6),decimalv3(20, 0)>", sinkColumnType);
                        continue;
                    }

                    if (sourceColumnType.equalsIgnoreCase("map<largeint,double>")) {
                        Assertions.assertEquals("map<decimalv3(20, 0),double>", sinkColumnType);
                        continue;
                    }

                    if (sourceColumnType.equalsIgnoreCase("map<date,datetime>")) {
                        Assertions.assertEquals("map<date,datetime(6)>", sinkColumnType);
                        continue;
                    }

                    if (sourceColumnType.equalsIgnoreCase("map<datetime,char(20)>")) {
                        Assertions.assertEquals("map<datetime(6),string>", sinkColumnType);
                        continue;
                    }

                    if (sourceColumnType.equalsIgnoreCase("map<char(20),varchar(255)>")) {
                        Assertions.assertEquals("map<string,string>", sinkColumnType);
                        continue;
                    }

                    if (sourceColumnType.equalsIgnoreCase("map<varchar(255),string>")) {
                        Assertions.assertEquals("map<string,string>", sinkColumnType);
                        continue;
                    }

                    Assertions.assertEquals(
                            sourceColumnType.toUpperCase(Locale.ROOT),
                            sinkColumnType.toUpperCase(Locale.ROOT));
                }
            }

            String sourceSql =
                    String.format(
                            "select * from %s.%s where F_ID > 50 order by F_ID ",
                            sourceDB, UNIQUE_TABLE);
            String sinkSql =
                    String.format("select * from %s.%s order by F_ID", sinkDB, UNIQUE_TABLE);
            checkSourceAndSinkTableDate(sourceSql, sinkSql, UNIQUE_TABLE_COLUMN_STRING);
            clearUniqueTable();
        } catch (Exception e) {
            throw new RuntimeException("Doris connection error", e);
        }
    }

    private void checkSourceAndSinkTableDate(String sourceSql, String sinkSql, String columnsString)
            throws Exception {
        List<String> columnList =
                Arrays.stream(columnsString.split(","))
                        .map(x -> x.trim())
                        .collect(Collectors.toList());
        Statement sourceStatement =
                conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        Statement sinkStatement =
                conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
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
                        // source read map<xx,datetime> will create map<xx,datetime(6)> in doris
                        // sink, because seatunnel type can not save the scale in MapType
                        // so we use the longest scale on the doris sink to prevent data overflow.
                        String sinkStr = sink.toString().replaceAll(".000000", "");
                        Assertions.assertEquals(source, sinkStr);
                    }
                }
            }
        }
        // Check the row numbers is equal
        sourceResultSet.last();
        sinkResultSet.last();
        Assertions.assertEquals(sourceResultSet.getRow(), sinkResultSet.getRow());
    }

    private Integer tableCount(String db, String table) {
        try (Statement statement = conn.createStatement()) {
            String sql = String.format("select count(*) from %s.%s", db, table);
            ResultSet source = statement.executeQuery(sql);
            if (source.next()) {
                int rowCount = source.getInt(1);
                return rowCount;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to check data in Doris server", e);
        }
        return -1;
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

    private void clearUniqueTable() {
        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s.%s", sourceDB, UNIQUE_TABLE));
            statement.execute(String.format("TRUNCATE TABLE %s.%s", sinkDB, UNIQUE_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("test doris server image error", e);
        }
    }

    private void clearDuplicateTable() {
        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s.%s", sourceDB, DUPLICATE_TABLE));
            statement.execute(String.format("TRUNCATE TABLE %s.%s", sinkDB, DUPLICATE_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("test doris server image error", e);
        }
    }

    protected void initializeJdbcTable() {
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
                statement.execute(createUniqueTableForTest(sourceDB));
                statement.execute(createDuplicateTableForTest(sourceDB));
                log.info("create source and sink table succeed");
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

    private String createUniqueTableForTest(String db) {
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
                        + "F_DATE date,\n"
                        + "MAP_VARCHAR_BOOLEAN map<varchar(200),boolean>,\n"
                        + "MAP_CHAR_TINYINT MAP<CHAR, TINYINT>,\n"
                        + "MAP_STRING_SMALLINT MAP<STRING, SMALLINT>,\n"
                        + "MAP_INT_INT MAP<INT, INT>,\n"
                        + "MAP_TINYINT_BIGINT MAP<TINYINT, BIGINT>,\n"
                        + "MAP_SMALLINT_LARGEINT MAP<SMALLINT, LARGEINT>,\n"
                        + "MAP_BIGINT_FLOAT MAP<BIGINT, FLOAT>,\n"
                        + "MAP_LARGEINT_DOUBLE MAP<LARGEINT, DOUBLE>,\n"
                        + "MAP_STRING_DECIMAL MAP<STRING, DECIMAL(10,2)>,\n"
                        + "MAP_DECIMAL_DATE MAP<DECIMAL(10,2), DATE>,\n"
                        + "MAP_DATE_DATETIME MAP<DATE, DATETIME>,\n"
                        + "MAP_DATETIME_CHAR MAP<DATETIME, CHAR(20)>,\n"
                        + "MAP_CHAR_VARCHAR MAP<CHAR(20), VARCHAR(255)>,\n"
                        + "MAP_VARCHAR_STRING MAP<VARCHAR(255), STRING>\n"
                        + ")\n"
                        + "UNIQUE KEY(`F_ID`)\n"
                        + "DISTRIBUTED BY HASH(`F_ID`) BUCKETS 1\n"
                        + "properties(\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\""
                        + ");";
        return String.format(createTableSql, db, UNIQUE_TABLE);
    }

    private String createDuplicateTableForTest(String db) {
        String createDuplicateTableSql =
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
                        + "F_ARRAY_STRING_STRING ARRAY<string>,\n"
                        + "F_ARRAY_DECIMAL ARRAY<decimalv3(10,2)>,\n"
                        + "F_ARRAY_DATE ARRAY<date>,\n"
                        + "F_ARRAY_DATETIME ARRAY<datetime>\n"
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
        checkColumnTypeMap.put("F_DATETIME", "datetime");
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
        checkColumnTypeMap.put("F_ARRAY_STRING_LARGEINT", "ARRAY<decimalv3(20, 0)>");
        checkColumnTypeMap.put("F_ARRAY_STRING_STRING", "ARRAY<string>");
        checkColumnTypeMap.put("F_ARRAY_DECIMAL", "ARRAY<decimalv3(10, 2)>");
        checkColumnTypeMap.put("F_ARRAY_DATE", "ARRAY<date>");
        checkColumnTypeMap.put("F_ARRAY_DATETIME", "ARRAY<datetime>");

        return String.format(createDuplicateTableSql, db, DUPLICATE_TABLE);
    }

    protected void batchInsertUniqueTableData() {
        List<SeaTunnelRow> rows = genUniqueTableTestData(100L);
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement preparedStatement =
                    conn.prepareStatement(INIT_UNIQUE_TABLE_DATA_SQL)) {
                for (int i = 0; i < rows.size(); i++) {
                    if (i % 10 == 0) {
                        for (int index = 0; index < rows.get(i).getFields().length; index++) {
                            preparedStatement.setObject(index + 1, null);
                        }
                    } else {
                        for (int index = 0; index < rows.get(i).getFields().length; index++) {
                            preparedStatement.setObject(index + 1, rows.get(i).getFields()[index]);
                        }
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            conn.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            String message = ExceptionUtils.getMessage(exception);
            getErrorUrl(message);
            throw new RuntimeException("get connection error", exception);
        }
        log.info("insert data succeed");
    }

    private void batchInsertDuplicateTableData() {
        List<SeaTunnelRow> rows = genDuplicateTableTestData(100L);
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement preparedStatement =
                    conn.prepareStatement(INIT_DUPLICATE_TABLE_DATA_SQL)) {
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

    private List<SeaTunnelRow> genUniqueTableTestData(Long nums) {
        List<SeaTunnelRow> datas = new ArrayList<>();
        Map<String, Boolean> varcharBooleanMap = new HashMap<>();
        varcharBooleanMap.put("aa", true);

        Map<String, Byte> charTinyintMap = new HashMap<>();
        charTinyintMap.put("a", (byte) 1);

        Map<String, Short> stringSmallintMap = new HashMap<>();
        stringSmallintMap.put("aa", Short.valueOf("1"));

        Map<Integer, Integer> intIntMap = new HashMap<>();
        intIntMap.put(1, 1);

        Map<Byte, Long> tinyintBigintMap = new HashMap<>();
        tinyintBigintMap.put((byte) 1, 1L);

        Map<Short, Long> smallintLargeintMap = new HashMap<>();
        smallintLargeintMap.put(Short.valueOf("1"), Long.valueOf("11"));

        Map<Long, Float> bigintFloatMap = new HashMap<>();
        bigintFloatMap.put(Long.valueOf("1"), Float.valueOf("11.1"));

        Map<Long, Double> largeintDoubtMap = new HashMap<>();
        largeintDoubtMap.put(11L, Double.valueOf("11.1"));

        String stringDecimalMap = "{\"11\":\"10.2\"}";

        String decimalDateMap = "{\"10.02\":\"2020-02-01\"}";

        String dateDatetimeMap = "{\"2020-02-01\":\"2020-02-01 12:00:00\"}";

        String datetimeCharMap = "{\"2020-02-01 12:00:00\":\"1\"}";

        String charVarcharMap = "{\"1\":\"11\"}";

        String varcharStringMap = "{\"11\":\"11\"}";
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
                                GenerateTestData.genDatetimeString(false),
                                GenerateTestData.genDatetimeString(true),
                                GenerateTestData.genDateString(),
                                JsonUtils.toJsonString(varcharBooleanMap),
                                JsonUtils.toJsonString(charTinyintMap),
                                JsonUtils.toJsonString(stringSmallintMap),
                                JsonUtils.toJsonString(intIntMap),
                                JsonUtils.toJsonString(tinyintBigintMap),
                                JsonUtils.toJsonString(smallintLargeintMap),
                                JsonUtils.toJsonString(bigintFloatMap),
                                JsonUtils.toJsonString(largeintDoubtMap),
                                stringDecimalMap,
                                decimalDateMap,
                                dateDatetimeMap,
                                datetimeCharMap,
                                charVarcharMap,
                                varcharStringMap
                            }));
        }
        log.info("generate test data succeed");
        return datas;
    }

    private List<SeaTunnelRow> genDuplicateTableTestData(Long nums) {
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
                                (new String[] {"1", "1"}).toString(),
                                (new BigDecimal[] {
                                            new BigDecimal("10.02"), new BigDecimal("10.03")
                                        })
                                        .toString(),
                                (new String[] {"2020-06-09", "2020-06-10"}).toString(),
                                (new String[] {"2020-06-09 12:02:02", "2020-06-10 12:02:02"})
                                        .toString()
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

    public void getErrorUrl(String message) {
        // Using regular expressions to match URLs
        Pattern pattern = Pattern.compile("http://[\\w./?=&-_]+");
        Matcher matcher = pattern.matcher(message);
        String urlString = null;
        if (matcher.find()) {
            log.error("Found URL: " + matcher.group());
            urlString = matcher.group();
        } else {
            log.error("No URL found.");
            return;
        }

        try {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // Set the request method
            connection.setRequestMethod("GET");

            // Set the connection timeout
            connection.setConnectTimeout(5000);
            // Set the read timeout
            connection.setReadTimeout(5000);

            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in =
                        new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
            } else {
                log.error("GET request not worked");
            }
        } catch (Exception e) {
            log.error(ExceptionUtils.getMessage(e));
        }
    }
}
