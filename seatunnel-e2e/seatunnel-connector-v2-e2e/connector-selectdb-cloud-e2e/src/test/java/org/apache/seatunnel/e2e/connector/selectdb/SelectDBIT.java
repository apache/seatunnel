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

package org.apache.seatunnel.e2e.connector.selectdb;

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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class SelectDBIT extends AbstractSelectDBIT {
    private static final String UNIQUE_TABLE = "selectdb_e2e_unique_table";
    private static final String DUPLICATE_TABLE = "selectdb_duplicate_table";
    private static final String sourceDB = "e2e_source";
    private static final String sinkDB = "e2e_sink";
    private Connection conn;

    private Map<String, String> checkColumnTypeMap = null;

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
                container.executeJob("/selectdb_source_and_sink_with_custom_sql.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(101, tableCount(sinkDB, UNIQUE_TABLE));
        clearUniqueTable();
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

    private void clearUniqueTable() {
        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s.%s", sourceDB, UNIQUE_TABLE));
            statement.execute(String.format("TRUNCATE TABLE %s.%s", sinkDB, UNIQUE_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("test doris server image error", e);
        }
    }

    protected void initializeJdbcTable() {
        try {
            URLClassLoader urlClassLoader =
                    new URLClassLoader(
                            new URL[] {new URL(DRIVER_JAR)}, SelectDBIT.class.getClassLoader());
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

    @AfterAll
    public void close() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }
}
