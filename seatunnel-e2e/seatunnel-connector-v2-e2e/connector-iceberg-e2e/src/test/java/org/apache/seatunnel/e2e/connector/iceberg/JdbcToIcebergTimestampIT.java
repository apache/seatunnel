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

package org.apache.seatunnel.e2e.connector.iceberg;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.stream.Stream;

/**
 * E2E test verifying that NTZ (No Time Zone) and LTZ (Local Time Zone) timestamp columns from JDBC
 * sources are stored with the correct Iceberg timestamp type:
 *
 * <ul>
 *   <li>NTZ → Iceberg {@code TimestampType.withoutZone()} (e.g. MySQL DATETIME, PG timestamp)
 *   <li>LTZ → Iceberg {@code TimestampType.withZone()} (e.g. MySQL TIMESTAMP, PG timestamptz)
 * </ul>
 *
 * <p>Covers the fix for https://github.com/apache/seatunnel/issues/10685
 */
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason =
                "Spark engine does not support TIMESTAMP_TZ (OffsetDateTime) natively; "
                        + "TIMESTAMP_TZ is serialized as a custom Decimal struct in Spark translation layer, "
                        + "which is incompatible with standard Sink connectors. "
                        + "Tested on Zeta and Flink engines only.")
@DisabledOnOs(OS.WINDOWS)
public class JdbcToIcebergTimestampIT extends TestSuiteBase implements TestResource {

    private static final Logger log = LoggerFactory.getLogger(JdbcToIcebergTimestampIT.class);

    // -------------------------------------------------------------------------
    // Catalog directories (inside the SeaTunnel container)
    // -------------------------------------------------------------------------
    private static final String MYSQL_CATALOG_DIR = "/tmp/seatunnel_mnt/iceberg/hadoop-ts-mysql/";

    private static final String PG_CATALOG_DIR = "/tmp/seatunnel_mnt/iceberg/hadoop-ts-pg/";

    // -------------------------------------------------------------------------
    // MySQL container
    // -------------------------------------------------------------------------
    private static final String MYSQL_IMAGE = "mysql:8.0";
    private static final String MYSQL_HOST = "mysql_timestamp_e2e";
    private static final String MYSQL_DATABASE = "ts_test";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";

    private static final MySQLContainer<?> MYSQL_CONTAINER =
            new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
                    .withDatabaseName(MYSQL_DATABASE)
                    .withUsername(MYSQL_USER)
                    .withPassword(MYSQL_PASSWORD)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(MYSQL_HOST)
                    .withLogConsumer(
                            new Slf4jLogConsumer(
                                    DockerLoggerFactory.getLogger("mysql-timestamp-image")));

    // -------------------------------------------------------------------------
    // PostgreSQL container
    // -------------------------------------------------------------------------
    private static final String PG_IMAGE = "postgres:14-alpine";
    private static final String PG_HOST = "pg_timestamp_e2e";
    private static final String PG_DATABASE = "ts_test";
    private static final String PG_USER = "postgres";
    private static final String PG_PASSWORD = "postgres";

    private static final PostgreSQLContainer<?> PG_CONTAINER =
            new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                    .withDatabaseName(PG_DATABASE)
                    .withUsername(PG_USER)
                    .withPassword(PG_PASSWORD)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(PG_HOST)
                    .withLogConsumer(
                            new Slf4jLogConsumer(
                                    DockerLoggerFactory.getLogger("pg-timestamp-image")));

    // -------------------------------------------------------------------------
    // Driver / plugin JARs downloaded into the SeaTunnel container
    // -------------------------------------------------------------------------
    private static final String MYSQL_DRIVER_URL =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";

    private static final String PG_DRIVER_URL =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";

    private static final String ZSTD_URL =
            "https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-5/zstd-jni-1.5.5-5.jar";

    // -------------------------------------------------------------------------
    // Container setup: create Iceberg dirs + download driver JARs
    // -------------------------------------------------------------------------
    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                for (String dir :
                        new String[] {
                            MYSQL_CATALOG_DIR + "seatunnel_namespace/mysql_ts_sink/data",
                            MYSQL_CATALOG_DIR + "seatunnel_namespace/mysql_ts_sink/metadata",
                            PG_CATALOG_DIR + "seatunnel_namespace/pg_ts_sink/data",
                            PG_CATALOG_DIR + "seatunnel_namespace/pg_ts_sink/metadata",
                        }) {
                    container.execInContainer("sh", "-c", "mkdir -p " + dir);
                }
                container.execInContainer("sh", "-c", "chmod -R 777 /tmp/seatunnel_mnt/iceberg/");

                // Download Iceberg compression codec
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p /tmp/seatunnel/plugins/Iceberg/lib"
                                + " && cd /tmp/seatunnel/plugins/Iceberg/lib"
                                + " && wget -q "
                                + ZSTD_URL);

                // Download JDBC drivers into the Jdbc plugin directory
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib"
                                + " && cd /tmp/seatunnel/plugins/Jdbc/lib"
                                + " && wget -q "
                                + MYSQL_DRIVER_URL
                                + " && wget -q "
                                + PG_DRIVER_URL);
            };

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------
    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("Starting MySQL and PostgreSQL containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER, PG_CONTAINER)).join();
        log.info("DB containers started. Initializing test data...");
        initMysqlData();
        initPostgresData();
        log.info("Test data initialised.");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
        if (PG_CONTAINER != null) {
            PG_CONTAINER.close();
        }
    }

    // -------------------------------------------------------------------------
    // Test: MySQL DATETIME (NTZ) → Iceberg withoutZone()
    // -------------------------------------------------------------------------
    @TestTemplate
    public void testMysqlDatetimeToIcebergNtz(TestContainer container)
            throws IOException, InterruptedException {
        // Step 1: Run job to write data from MySQL to Iceberg
        org.testcontainers.containers.Container.ExecResult result =
                container.executeJob("/iceberg/mysql_jdbc_to_iceberg_timestamp.conf");
        Assertions.assertEquals(
                0, result.getExitCode(), "Write job failed:\n" + result.getStderr());

        // Step 2: Run verification job (Iceberg -> Assert)
        // This job verifies that the data in Iceberg matches expected types and values
        org.testcontainers.containers.Container.ExecResult verifyResult =
                container.executeJob("/iceberg/mysql_iceberg_to_assert.conf");
        Assertions.assertEquals(
                0,
                verifyResult.getExitCode(),
                "Verification job failed:\n" + verifyResult.getStderr());
    }

    // -------------------------------------------------------------------------
    // Test: PostgreSQL timestamp (NTZ) → Iceberg withoutZone()
    //       PostgreSQL timestamptz (LTZ) → Iceberg withZone()
    // -------------------------------------------------------------------------
    @TestTemplate
    public void testPgTimestampToIceberg(TestContainer container)
            throws IOException, InterruptedException {
        // Step 1: Run job to write data from PostgreSQL to Iceberg
        org.testcontainers.containers.Container.ExecResult result =
                container.executeJob("/iceberg/pg_jdbc_to_iceberg_timestamp.conf");
        Assertions.assertEquals(
                0, result.getExitCode(), "Write job failed:\n" + result.getStderr());

        // Step 2: Run verification job (Iceberg -> Assert)
        org.testcontainers.containers.Container.ExecResult verifyResult =
                container.executeJob("/iceberg/pg_iceberg_to_assert.conf");
        Assertions.assertEquals(
                0,
                verifyResult.getExitCode(),
                "Verification job failed:\n" + verifyResult.getStderr());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    private void initMysqlData() throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(), MYSQL_USER, MYSQL_PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS ts_table ("
                            + "  id       INT PRIMARY KEY,"
                            + "  dt_col   DATETIME,"
                            + "  ts_col   TIMESTAMP"
                            + ")");
            stmt.execute(
                    "INSERT INTO ts_table (id, dt_col, ts_col) VALUES"
                            + " (1, '2026-01-01 00:00:00', '2026-01-01 00:00:00')");
        }
    }

    private void initPostgresData() throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                PG_CONTAINER.getJdbcUrl(), PG_USER, PG_PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS ts_table ("
                            + "  id       INT PRIMARY KEY,"
                            + "  ts_col   TIMESTAMP WITHOUT TIME ZONE,"
                            + "  tstz_col TIMESTAMP WITH TIME ZONE"
                            + ")");
            stmt.execute(
                    "INSERT INTO ts_table (id, ts_col, tstz_col) VALUES"
                            + " (1, '2026-01-01 00:00:00', '2026-01-01 00:00:00+00')");
        }
    }
}
