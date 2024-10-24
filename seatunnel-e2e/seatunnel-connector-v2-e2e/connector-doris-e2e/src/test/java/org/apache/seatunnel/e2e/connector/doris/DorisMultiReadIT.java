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
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
public class DorisMultiReadIT extends AbstractDorisIT {
    private static final String UNIQUE_TABLE_0 = "doris_e2e_unique_table_0";
    private static final String UNIQUE_TABLE_1 = "doris_e2e_unique_table_1";
    private static final String SOURCE_DB_0 = "e2e_source_0";
    private static final String SOURCE_DB_1 = "e2e_source_1";
    private static final String sinkDB = "e2e_sink";
    private Connection conn;

    private static final String INIT_UNIQUE_TABLE_DATA_SQL =
            "insert into %s.%s"
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
    public void testDorisMultiRead(TestContainer container)
            throws IOException, InterruptedException {
        initializeJdbcTable();
        // init table_0
        batchInsertUniqueTableData(SOURCE_DB_0, UNIQUE_TABLE_0);
        // init table_1
        batchInsertUniqueTableData(SOURCE_DB_1, UNIQUE_TABLE_1);
        // test assert row num
        Container.ExecResult execResult =
                container.executeJob("/doris_multi_source_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        // execute multi read with 2pc enable
        execResult = container.executeJob("/doris_multi_source_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        checkSinkData(SOURCE_DB_0, UNIQUE_TABLE_0, "where F_ID >= 50");
        checkSinkData(SOURCE_DB_1, UNIQUE_TABLE_1, "where F_ID < 40");
        // clean sink database data
        clearSinkUniqueTable();
        // execute multi read without 2pc enable
        Container.ExecResult execResult2 =
                container.executeJob("/doris_multi_source_to_sink_2pc_false.conf");
        Assertions.assertEquals(0, execResult2.getExitCode());
        checkSinkData(SOURCE_DB_0, UNIQUE_TABLE_0, "where F_ID >= 50");
        checkSinkData(SOURCE_DB_1, UNIQUE_TABLE_1, "where F_ID < 40");
        // clean all data
        clearSourceUniqueTable();
        clearSinkUniqueTable();
    }

    protected void checkSinkData(String database, String tableName, String sqlCondition) {
        try {
            assertHasData(database, tableName);
            assertHasData(sinkDB, tableName);

            PreparedStatement sourcePre =
                    conn.prepareStatement(DorisCatalogUtil.TABLE_SCHEMA_QUERY);
            sourcePre.setString(1, database);
            sourcePre.setString(2, tableName);
            ResultSet sourceResultSet = sourcePre.executeQuery();

            PreparedStatement sinkPre = conn.prepareStatement(DorisCatalogUtil.TABLE_SCHEMA_QUERY);
            sinkPre.setString(1, sinkDB);
            sinkPre.setString(2, tableName);
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
                            "select * from %s.%s %s order by F_ID ",
                            database, tableName, sqlCondition);
            String sinkSql = String.format("select * from %s.%s order by F_ID", sinkDB, tableName);
            checkSourceAndSinkTableDate(sourceSql, sinkSql, UNIQUE_TABLE_COLUMN_STRING);
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

    private void clearSourceUniqueTable() {
        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s.%s", SOURCE_DB_0, UNIQUE_TABLE_0));
            statement.execute(String.format("TRUNCATE TABLE %s.%s", SOURCE_DB_1, UNIQUE_TABLE_1));
        } catch (SQLException e) {
            throw new RuntimeException("test doris server image error", e);
        }
    }

    private void clearSinkUniqueTable() {
        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s.%s", sinkDB, UNIQUE_TABLE_0));
            statement.execute(String.format("TRUNCATE TABLE %s.%s", sinkDB, UNIQUE_TABLE_1));
        } catch (SQLException e) {
            throw new RuntimeException("test doris server image error", e);
        }
    }

    protected void initializeJdbcTable() {
        try {
            URLClassLoader urlClassLoader =
                    new URLClassLoader(
                            new URL[] {new URL(DRIVER_JAR)},
                            DorisMultiReadIT.class.getClassLoader());
            Thread.currentThread().setContextClassLoader(urlClassLoader);
            Driver driver = (Driver) urlClassLoader.loadClass(DRIVER_CLASS).newInstance();
            Properties props = new Properties();
            props.put("user", USERNAME);
            props.put("password", PASSWORD);
            conn = driver.connect(String.format(URL, container.getHost()), props);
            try (Statement statement = conn.createStatement()) {
                // create test databases
                statement.execute(createDatabase(SOURCE_DB_0));
                statement.execute(createDatabase(SOURCE_DB_1));
                statement.execute(createDatabase(sinkDB));
                log.info("create source and sink database succeed");
                // create source and sink table
                statement.execute(createUniqueTableForTest(SOURCE_DB_0, UNIQUE_TABLE_0));
                statement.execute(createUniqueTableForTest(SOURCE_DB_1, UNIQUE_TABLE_1));
                statement.execute(createUniqueTableForTest(sinkDB, UNIQUE_TABLE_0));
                statement.execute(createUniqueTableForTest(sinkDB, UNIQUE_TABLE_1));
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

    private String createUniqueTableForTest(String db, String table) {
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
        return String.format(createTableSql, db, table);
    }

    protected void batchInsertUniqueTableData(String database, String tableName) {
        List<SeaTunnelRow> rows = genUniqueTableTestData(100L);
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement preparedStatement =
                    conn.prepareStatement(
                            String.format(INIT_UNIQUE_TABLE_DATA_SQL, database, tableName))) {
                for (SeaTunnelRow row : rows) {
                    for (int index = 0; index < row.getFields().length; index++) {
                        preparedStatement.setObject(index + 1, row.getFields()[index]);
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

    @AfterAll
    public void close() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    public void getErrorUrl(String message) {
        // 使用正则表达式匹配URL
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

            // 设置请求方法
            connection.setRequestMethod("GET");

            // 设置连接超时时间
            connection.setConnectTimeout(5000);
            // 设置读取超时时间
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
