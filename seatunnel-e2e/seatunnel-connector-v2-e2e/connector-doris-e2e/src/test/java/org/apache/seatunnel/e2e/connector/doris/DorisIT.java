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
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class DorisIT extends AbstractDorisIT {
    private static final String TABLE = "doris_e2e_table";
    private static final String DRIVER_JAR =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";

    private static final String sourceDB = "e2e_source";
    private static final String sinkDB = "e2e_sink";
    private Connection conn;

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
                        + "duplicate KEY(`F_ID`)\n"
                        + "DISTRIBUTED BY HASH(`F_ID`) BUCKETS 1\n"
                        + "properties(\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\""
                        + ");";
        return String.format(createTableSql, db, TABLE);
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

    @AfterAll
    public void close() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }
}
