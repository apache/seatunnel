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

import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Slf4j
public class JdbcDuckDBIT extends AbstractJdbcIT {

    private static final String DRIVER_CLASS = "org.duckdb.DuckDBDriver";
    private static final String DUCKDB_USER = "duckdb";
    private static final String SCHEMA = "";
    private static final String DATABASE = "main";
    private static final String SOURCE_TABLE = "source";
    private static final String SINK_TABLE = "sink";
    private static final List<String> CONFIG_FILE =
            Collections.singletonList("/jdbc_duckdb_source_and_sink.conf");
    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS %s (\n"
                    + "  id INTEGER,\n"
                    + "  name VARCHAR,\n"
                    + "  amount DECIMAL(10,2),\n"
                    + "  score DOUBLE,\n"
                    + "  active BOOLEAN,\n"
                    + "  created_date DATE,\n"
                    + "  created_ts TIMESTAMP\n"
                    + ")";

    private static final String HOST_MOUNT_PATH = "/tmp";

    private final Path duckdbBaseDir =
            Paths.get(HOST_MOUNT_PATH, "duckdb-e2e", UUID.randomUUID().toString());
    private Path duckdbDatabasePath;

    @Override
    @BeforeAll
    public void startUp() {
        try {
            Files.createDirectories(duckdbBaseDir);
            duckdbDatabasePath = duckdbBaseDir.resolve("seatunnel_duckdb_e2e.db");
            jdbcCase = getJdbcCase();
            initializeJdbcConnection(jdbcCase.getJdbcUrl());
            createNeededTables();
            insertTestData();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start DuckDB embedded test", e);
        }
    }

    @Override
    JdbcCase getJdbcCase() {
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String insertSql = insertTable(SCHEMA, SOURCE_TABLE, testDataSet.getKey());
        return JdbcCase.builder()
                .driverClass(DRIVER_CLASS)
                .userName(DUCKDB_USER)
                .database(DATABASE)
                .schema(SCHEMA)
                .sourceTable(SOURCE_TABLE)
                .sinkTable(SINK_TABLE)
                .jdbcUrl(buildJdbcUrl())
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .useSaveModeCreateTable(true)
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/1.3.1.0/duckdb_jdbc-1.3.1.0.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "id", "name", "amount", "score", "active", "created_date", "created_ts"
                };
        List<SeaTunnelRow> rows = new ArrayList<>();
        rows.add(
                new SeaTunnelRow(
                        new Object[] {
                            1,
                            "Alice",
                            new BigDecimal("12.50"),
                            98.5,
                            true,
                            Date.valueOf(LocalDate.of(2024, 1, 1)),
                            Timestamp.valueOf(LocalDateTime.of(2024, 1, 1, 8, 30))
                        }));
        rows.add(
                new SeaTunnelRow(
                        new Object[] {
                            2,
                            "Bob",
                            new BigDecimal("20.00"),
                            76.3,
                            false,
                            Date.valueOf(LocalDate.of(2024, 2, 15)),
                            Timestamp.valueOf(LocalDateTime.of(2024, 2, 15, 12, 0))
                        }));
        rows.add(
                new SeaTunnelRow(
                        new Object[] {
                            3,
                            "Carol",
                            new BigDecimal("5.75"),
                            88.0,
                            true,
                            Date.valueOf(LocalDate.of(2024, 3, 3)),
                            Timestamp.valueOf(LocalDateTime.of(2024, 3, 3, 18, 45))
                        }));
        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        return null;
    }

    @Override
    public void clearTable(String schema, String table) {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM " + buildTableInfoWithSchema(schema, table));
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clear table " + table, e);
        }
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @TestTemplate
    @Override
    public void testJdbcDb(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        List<String> variables = Collections.singletonList("DUCKDB_URL=" + buildJdbcUrl());
        for (String configFile : jdbcCase.getConfigFile()) {
            try {
                Container.ExecResult execResult = container.executeJob(configFile, variables);
                Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
                defaultCompare(configFile, jdbcCase.getTestData().getKey(), "id");
            } finally {
                clearTable(jdbcCase.getSchema(), jdbcCase.getSinkTable());
            }
        }
    }

    private String buildJdbcUrl() {
        return "jdbc:duckdb:" + duckdbDatabasePath.toString();
    }
}
