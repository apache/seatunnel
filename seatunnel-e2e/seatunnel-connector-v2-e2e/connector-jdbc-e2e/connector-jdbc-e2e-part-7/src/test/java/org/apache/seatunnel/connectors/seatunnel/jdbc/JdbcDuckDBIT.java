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
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class JdbcDuckDBIT extends AbstractJdbcIT {

    private static final String DUCKDB_IMAGE = "openjdk:11-jre-slim";
    private static final String DUCKDB_CONTAINER_HOST = "duckdb-e2e";
    private static final String DUCKDB_DATABASE = "main"; // Use main schema
    private static final String DUCKDB_SOURCE = "source";
    private static final String DUCKDB_SINK = "sink";
    private static final String DUCKDB_USERNAME = "duckdb"; // Set non-empty username
    private static final String DUCKDB_PASSWORD = "";
    private static final int DUCKDB_PORT = 0;
    private static final String DUCKDB_URL_TEMPLATE = "jdbc:duckdb:/tmp/test_%s.db";
    private String duckdbUrl;
    private static final String DRIVER_CLASS = "org.duckdb.DuckDBDriver";

    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS %s ("
                    + "id BIGINT PRIMARY KEY, "
                    + "c_bit_1 BOOLEAN, "
                    + "c_bit_8 TINYINT, "
                    + "c_boolean BOOLEAN, "
                    + "c_tinyint TINYINT, "
                    + "c_tinyint_unsigned SMALLINT, "
                    + "c_smallint SMALLINT, "
                    + "c_smallint_unsigned INTEGER, "
                    + "c_mediumint INTEGER, "
                    + "c_mediumint_unsigned INTEGER, "
                    + "c_int INTEGER, "
                    + "c_int_unsigned BIGINT, "
                    + "c_integer INTEGER, "
                    + "c_bigint BIGINT, "
                    + "c_bigint_unsigned DECIMAL(20,0), "
                    + "c_decimal DECIMAL(20,0), "
                    + "c_float REAL, "
                    + "c_double DOUBLE, "
                    + "c_char VARCHAR(1), "
                    + "c_tinytext VARCHAR, "
                    + "c_mediumtext VARCHAR, "
                    + "c_text VARCHAR, "
                    + "c_varchar VARCHAR(255), "
                    + "c_json VARCHAR, "
                    + "c_longtext VARCHAR, "
                    + "c_date DATE, "
                    + "c_datetime TIMESTAMP, "
                    + "c_timestamp TIMESTAMP, "
                    + "c_tinyblob BLOB, "
                    + "c_mediumblob BLOB, "
                    + "c_blob BLOB, "
                    + "c_longblob BLOB, "
                    + "c_varbinary BLOB, "
                    + "c_binary BLOB, "
                    + "c_year INTEGER, "
                    + "c_int_unsigned_zerofill BIGINT, "
                    + "c_bigint_30 BIGINT"
                    + ");";

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                // DuckDB is an embedded database, minimal setup required
                container.execInContainer("mkdir", "-p", "/tmp/seatunnel/plugins/Jdbc/lib");
            };

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    public void startUp() {
        log.info("Starting DuckDB IT test");
    }

    @Override
    public void tearDown() {
        log.info("DuckDB IT test completed");
    }

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();

        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(DUCKDB_DATABASE, DUCKDB_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(DUCKDB_IMAGE)
                .networkAliases(DUCKDB_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(DUCKDB_PORT)
                .localPort(DUCKDB_PORT)
                .jdbcTemplate(DUCKDB_URL_TEMPLATE.replace("%s", "test"))
                .jdbcUrl(DUCKDB_URL_TEMPLATE.replace("%s", "test"))
                .userName(DUCKDB_USERNAME)
                .password(DUCKDB_PASSWORD)
                .database(DUCKDB_DATABASE)
                .sourceTable(DUCKDB_SOURCE)
                .sinkTable(DUCKDB_SINK)
                .createSql(CREATE_SQL)
                .configFile(Arrays.asList("/jdbc_duckdb_source_and_sink.conf"))
                .insertSql(insertSql)
                .testData(testDataSet)
                .catalogDatabase(DUCKDB_DATABASE)
                .catalogTable(DUCKDB_SINK)
                .tablePathFullName(DUCKDB_SOURCE)
                .build();
    }

    @Override
    String driverUrl() {
        // DuckDB driver should be provided via Maven dependencies
        return "";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "id",
                    "c_bit_1",
                    "c_bit_8",
                    "c_boolean",
                    "c_tinyint",
                    "c_tinyint_unsigned",
                    "c_smallint",
                    "c_smallint_unsigned",
                    "c_mediumint",
                    "c_mediumint_unsigned",
                    "c_int",
                    "c_int_unsigned",
                    "c_integer",
                    "c_bigint",
                    "c_bigint_unsigned",
                    "c_decimal",
                    "c_float",
                    "c_double",
                    "c_char",
                    "c_tinytext",
                    "c_mediumtext",
                    "c_text",
                    "c_varchar",
                    "c_json",
                    "c_longtext",
                    "c_date",
                    "c_datetime",
                    "c_timestamp",
                    "c_tinyblob",
                    "c_mediumblob",
                    "c_blob",
                    "c_longblob",
                    "c_varbinary",
                    "c_binary",
                    "c_year",
                    "c_int_unsigned_zerofill",
                    "c_bigint_30"
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(
                    new SeaTunnelRow(
                            new Object[] {
                                (long) (i + 1), // id
                                true, // c_bit_1
                                (byte) 1, // c_bit_8
                                true, // c_boolean
                                (byte) 1, // c_tinyint
                                (short) 1, // c_tinyint_unsigned
                                (short) 1, // c_smallint
                                1, // c_smallint_unsigned
                                1, // c_mediumint
                                1, // c_mediumint_unsigned
                                1, // c_int
                                1L, // c_int_unsigned
                                1, // c_integer
                                1L, // c_bigint
                                new BigDecimal("1"), // c_bigint_unsigned
                                new BigDecimal("1"), // c_decimal
                                1.0f, // c_float
                                1.0d, // c_double
                                "a", // c_char
                                "tinytext", // c_tinytext
                                "mediumtext", // c_mediumtext
                                "text", // c_text
                                "varchar", // c_varchar
                                "{\"key\": \"value\"}", // c_json
                                "longtext", // c_longtext
                                Date.valueOf(LocalDate.of(2023, 1, 1)), // c_date
                                Timestamp.valueOf(
                                        LocalDateTime.of(2023, 1, 1, 0, 0, 0)), // c_datetime
                                Timestamp.valueOf(
                                        LocalDateTime.of(2023, 1, 1, 0, 0, 0)), // c_timestamp
                                "tinyblob".getBytes(), // c_tinyblob
                                "mediumblob".getBytes(), // c_mediumblob
                                "blob".getBytes(), // c_blob
                                "longblob".getBytes(), // c_longblob
                                "varbinary".getBytes(), // c_varbinary
                                "binary".getBytes(), // c_binary
                                2023, // c_year
                                1L, // c_int_unsigned_zerofill
                                1L // c_bigint_30
                            }));
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    protected GenericContainer<?> initContainer() {
        // DuckDB is an embedded database, create a minimal container for testing framework
        return new GenericContainer<>(DockerImageName.parse(DUCKDB_IMAGE))
                .withNetwork(NETWORK)
                .withNetworkAliases(DUCKDB_CONTAINER_HOST)
                .withCommand("tail", "-f", "/dev/null");
    }

    @Override
    protected void checkResult(
            String executeKey, TestContainer container, Container.ExecResult execResult) {
        if (execResult.getExitCode() != 0) {
            String stdout = execResult.getStdout();
            String stderr = execResult.getStderr();

            log.error("Test [{}] failed with exit code: {}", executeKey, execResult.getExitCode());
            if (stdout != null && !stdout.trim().isEmpty()) {
                log.error("Stdout: {}", stdout);
            }
            if (stderr != null && !stderr.trim().isEmpty()) {
                log.error("Stderr: {}", stderr);
            }

            Assertions.fail(
                    String.format(
                            "Test [%s] failed with exit code %d.\nStdout: %s\nStderr: %s",
                            executeKey,
                            execResult.getExitCode(),
                            stdout != null ? stdout : "<empty>",
                            stderr != null ? stderr : "<empty>"));
        } else {
            log.info("Test [{}] completed successfully", executeKey);
        }
    }

    @Override
    protected void createNeededTables() {
        try (Statement stmt = connection.createStatement()) {
            String createSourceSql = String.format(CREATE_SQL, quoteIdentifier(DUCKDB_SOURCE));
            stmt.execute(createSourceSql);
            log.debug("Created source table: {}", DUCKDB_SOURCE);

            String createSinkSql = String.format(CREATE_SQL, quoteIdentifier(DUCKDB_SINK));
            stmt.execute(createSinkSql);
            log.debug("Created sink table: {}", DUCKDB_SINK);

            connection.commit();
            log.info("DuckDB tables created successfully");
        } catch (SQLException e) {
            log.error("Failed to create DuckDB tables: {}", e.getMessage(), e);
            try {
                connection.rollback();
                log.info("Transaction rolled back due to table creation failure");
            } catch (SQLException rollbackException) {
                log.error(
                        "Failed to rollback transaction: {}",
                        rollbackException.getMessage(),
                        rollbackException);
            }
            throw new RuntimeException("Failed to create tables: " + e.getMessage(), e);
        }
    }

    @Override
    protected void clearTable(String database, String schema, String table) {
        if (table == null || table.trim().isEmpty()) {
            log.warn("Table name is null or empty, skipping clear operation");
            return;
        }

        try (Statement stmt = connection.createStatement()) {
            // Use schema-qualified table name for DuckDB
            String schemaName = (schema == null || schema.trim().isEmpty()) ? "main" : schema;
            String qualifiedTable = quoteIdentifier(schemaName) + "." + quoteIdentifier(table);
            String clearSql = String.format("DELETE FROM %s", qualifiedTable);

            int deletedRows = stmt.executeUpdate(clearSql);
            connection.commit();
            log.info("Cleared table: {}, deleted {} rows", qualifiedTable, deletedRows);
        } catch (SQLException e) {
            log.error("Failed to clear table {}: {}", table, e.getMessage(), e);
            try {
                connection.rollback();
                log.info("Transaction rolled back due to table clear failure");
            } catch (SQLException rollbackException) {
                log.error(
                        "Failed to rollback transaction: {}",
                        rollbackException.getMessage(),
                        rollbackException);
            }
            throw new RuntimeException("Failed to clear table " + table + ": " + e.getMessage(), e);
        }
    }

    @TestTemplate
    public void testDuckDBSourceAndSink(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        String testId = UUID.randomUUID().toString().replace("-", "");
        log.info("Starting DuckDB source and sink test with ID: {}", testId);

        try {
            // Initialize unique database URL to avoid concurrent conflicts
            duckdbUrl = String.format(DUCKDB_URL_TEMPLATE, testId);
            log.info("Using unique DuckDB URL: {}", duckdbUrl);

            // Create test data first
            log.info("Creating test data for DuckDB test");
            createTestData();
            log.info("Test data creation completed successfully");

            // Set system property for dynamic URL substitution in configuration
            System.setProperty("DUCKDB_URL", duckdbUrl);
            log.debug("Set system property DUCKDB_URL to: {}", duckdbUrl);

            // Execute SeaTunnel job
            log.info(
                    "Executing SeaTunnel job with configuration: /jdbc_duckdb_source_and_sink.conf");
            Container.ExecResult execResult =
                    container.executeJob("/jdbc_duckdb_source_and_sink.conf");

            if (execResult.getExitCode() != 0) {
                log.error("SeaTunnel job failed with exit code: {}", execResult.getExitCode());
                log.error("Job stderr: {}", execResult.getStderr());
                log.error("Job stdout: {}", execResult.getStdout());
            }
            Assertions.assertEquals(
                    0, execResult.getExitCode(), "SeaTunnel job failed: " + execResult.getStderr());
            log.info("SeaTunnel job completed successfully");

            // Verify results
            log.info("Starting result verification");
            verifyResults();
            log.info("Result verification completed successfully");

        } catch (Exception e) {
            log.error("DuckDB test failed with exception: {}", e.getMessage(), e);
            throw e;
        } finally {
            // Clean up system property
            System.clearProperty("DUCKDB_URL");
            log.debug("Cleared system property DUCKDB_URL");

            log.info("DuckDB source and sink test completed for ID: {}", testId);
        }
    }

    private void createTestData() throws SQLException {
        log.info("Creating DuckDB test data");
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();
        List<SeaTunnelRow> rows = testDataSet.getValue();

        try (Connection conn = DriverManager.getConnection(duckdbUrl);
                Statement stmt = conn.createStatement()) {

            // Create source table
            String createSourceSql = String.format(CREATE_SQL, quoteIdentifier(DUCKDB_SOURCE));
            stmt.execute(createSourceSql);

            // Insert test data using prepared statement for better performance and data consistency
            String fieldList =
                    String.join(
                            ", ",
                            Arrays.stream(fieldNames)
                                    .map(this::quoteIdentifier)
                                    .toArray(String[]::new));
            String placeholders = String.join(", ", Collections.nCopies(fieldNames.length, "?"));
            String insertSql =
                    String.format(
                            "INSERT INTO %s (%s) VALUES (%s)",
                            quoteIdentifier(DUCKDB_SOURCE), fieldList, placeholders);

            try (java.sql.PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                for (SeaTunnelRow row : rows) {
                    Object[] fields = row.getFields();
                    for (int i = 0; i < fields.length; i++) {
                        pstmt.setObject(i + 1, fields[i]);
                    }
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            log.info("DuckDB test data created successfully with {} rows", rows.size());
        }
    }

    private void verifyResults() throws SQLException {
        log.info("Verifying DuckDB test results");

        try (Connection conn = DriverManager.getConnection(duckdbUrl)) {
            log.debug("Connected to DuckDB for result verification: {}", duckdbUrl);

            try (Statement stmt = conn.createStatement()) {
                // Check if sink table exists and has data
                String countSql = "SELECT COUNT(*) FROM " + quoteIdentifier(DUCKDB_SINK);
                log.debug("Executing count query: {}", countSql);

                try (ResultSet rs = stmt.executeQuery(countSql)) {
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        log.info("DuckDB sink table contains {} rows", count);

                        if (count <= 0) {
                            log.error("Sink table is empty, expected > 0 rows");
                            Assertions.fail(
                                    "Sink table should contain data but found " + count + " rows");
                        }
                    } else {
                        log.error("Failed to get count from sink table");
                        Assertions.fail("Could not retrieve row count from sink table");
                    }
                }

                // Verify specific field values to ensure data integrity
                String selectSql =
                        "SELECT id, c_boolean, c_integer, c_varchar, c_decimal FROM "
                                + quoteIdentifier(DUCKDB_SINK)
                                + " ORDER BY id LIMIT 1";
                log.info("Executing data verification query: {}", selectSql);

                try (ResultSet rs = stmt.executeQuery(selectSql)) {
                    if (rs.next()) {
                        long idValue = rs.getLong("id");
                        boolean boolValue = rs.getBoolean("c_boolean");
                        int intValue = rs.getInt("c_integer");
                        String varcharValue = rs.getString("c_varchar");

                        log.debug(
                                "Retrieved values: id={}, boolean={}, integer={}, varchar={}",
                                idValue,
                                boolValue,
                                intValue,
                                varcharValue);

                        // Verify expected values based on our test data
                        Assertions.assertTrue(
                                idValue > 0, "ID should be positive, got: " + idValue);
                        Assertions.assertTrue(
                                boolValue, "Boolean value should be true, got: " + boolValue);
                        Assertions.assertEquals(
                                1, intValue, "Integer value should be 1, got: " + intValue);
                        Assertions.assertEquals(
                                "varchar",
                                varcharValue,
                                "Varchar value should be 'varchar', got: " + varcharValue);

                        log.info(
                                "Data integrity verification successful: id={}, boolean={}, integer={}, varchar={}",
                                idValue,
                                boolValue,
                                intValue,
                                varcharValue);
                    } else {
                        log.error("No data found in sink table for verification");
                        Assertions.fail("Sink table contains no data for verification");
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Failed to verify results due to SQL exception: {}", e.getMessage(), e);
            throw new SQLException("Result verification failed: " + e.getMessage(), e);
        }

        log.info("DuckDB test results verification completed successfully");
    }
}
