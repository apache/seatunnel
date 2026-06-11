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
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/**
 * E2E test verifying that MySQL NTZ/LTZ timestamp types are correctly distinguished by the JDBC
 * connector after the fix for https://github.com/apache/seatunnel/issues/10685.
 *
 * <ul>
 *   <li>MySQL {@code DATETIME} (NTZ) → SeaTunnel internal {@code TIMESTAMP} type
 *   <li>MySQL {@code TIMESTAMP} (LTZ) → SeaTunnel internal {@code TIMESTAMP_TZ} type
 * </ul>
 *
 * <p>The Assert sink's {@code field_type} check is used to validate the internal type mapping.
 */
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason =
                "Spark engine does not support TIMESTAMP_TZ (OffsetDateTime) natively; "
                        + "TIMESTAMP_TZ is serialized as a custom Decimal struct in Spark translation layer, "
                        + "which is incompatible with standard Sink connectors. "
                        + "Tested on Zeta and Flink engines only.")
@Slf4j
public class JdbcMysqlTimestampIT extends TestSuiteBase implements TestResource {

    private static final String MYSQL_IMAGE = "mysql:8.0";
    private static final String MYSQL_HOST = "mysql_ts_e2e";
    private static final String MYSQL_DATABASE = "ts_test";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";

    private static final String MYSQL_DRIVER_URL =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";

    private MySQLContainer<?> mysqlContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult result =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib"
                                        + " && cd /tmp/seatunnel/plugins/Jdbc/lib"
                                        + " && wget -q "
                                        + MYSQL_DRIVER_URL);
                Assertions.assertEquals(
                        0,
                        result.getExitCode(),
                        "Failed to download MySQL driver: " + result.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        mysqlContainer =
                new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
                        .withDatabaseName(MYSQL_DATABASE)
                        .withUsername(MYSQL_USER)
                        .withPassword(MYSQL_PASSWORD)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));

        Startables.deepStart(Stream.of(mysqlContainer)).join();

        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> initMysqlData());
        log.info("MySQL container started and test data initialised.");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (mysqlContainer != null) {
            mysqlContainer.close();
        }
    }

    /**
     * Verifies that MySQL {@code DATETIME} (NTZ) columns are read as SeaTunnel {@code TIMESTAMP}
     * (i.e. {@code LOCAL_DATE_TIME_TYPE}), not {@code TIMESTAMP_TZ}.
     *
     * <p>The Assert sink's {@code field_type = timestamp} assertion will fail if the connector
     * incorrectly maps {@code DATETIME} to {@code TIMESTAMP_TZ}.
     */
    @TestTemplate
    public void testMysqlDatetimeIsNtz(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result = container.executeJob("/jdbc_mysql_datetime_to_assert.conf");
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "MySQL DATETIME (NTZ) assertion failed:\n" + result.getStderr());
    }

    /**
     * Verifies that MySQL {@code TIMESTAMP} (LTZ) columns are read as SeaTunnel {@code
     * TIMESTAMP_TZ} (i.e. {@code OFFSET_DATE_TIME_TYPE}).
     *
     * <p>The Assert sink's {@code field_type = timestamp_tz} assertion will fail if the connector
     * incorrectly maps {@code TIMESTAMP} to plain {@code TIMESTAMP}.
     */
    @TestTemplate
    public void testMysqlTimestampIsLtz(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result = container.executeJob("/jdbc_mysql_timestamp_to_assert.conf");
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "MySQL TIMESTAMP (LTZ) assertion failed:\n" + result.getStderr());
    }

    /**
     * Core fix scenario: verifies that MySQL {@code TIMESTAMP} (LTZ) preserves the correct UTC
     * instant when the JDBC connection uses a non-UTC {@code serverTimezone} (Asia/Seoul, +09:00).
     *
     * <p>Before the fix, {@code JdbcFieldTypeUtils.getOffsetDateTime()} applied the JVM default
     * timezone during {@code ResultSet} traversal. In a Seoul-timezone session a value stored as
     * UTC midnight would be shifted by +09:00 and read back as {@code 2026-01-01T09:00:00Z} instead
     * of {@code 2026-01-01T00:00:00Z} — a 9-hour epoch error.
     */
    @TestTemplate
    public void testMysqlTimestampIsLtzInNonUtcSession(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                container.executeJob("/jdbc_mysql_timestamp_non_utc_to_assert.conf");
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "MySQL TIMESTAMP (LTZ) assertion failed with non-UTC serverTimezone (Asia/Seoul):\n"
                        + result.getStderr());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void initMysqlData() throws Exception {
        String jdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                        mysqlContainer.getHost(),
                        mysqlContainer.getFirstMappedPort(),
                        MYSQL_DATABASE);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, MYSQL_USER, MYSQL_PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS ts_source ("
                            + "  id       INT PRIMARY KEY,"
                            + "  dt_col   DATETIME,"
                            + "  ts_col   TIMESTAMP NULL"
                            + ")");
            // Insert a fixed wall-clock value: 2026-01-01 00:00:00
            // DATETIME stores it as-is (NTZ); TIMESTAMP stores UTC and displays in session TZ.
            stmt.execute(
                    "INSERT INTO ts_source (id, dt_col, ts_col) VALUES"
                            + " (1, '2026-01-01 00:00:00', '2026-01-01 00:00:00')");
        }
    }
}
