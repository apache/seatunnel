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
import org.testcontainers.containers.PostgreSQLContainer;
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
 * E2E test verifying that PostgreSQL NTZ/LTZ timestamp types are correctly distinguished by the
 * JDBC connector after the fix for https://github.com/apache/seatunnel/issues/10685.
 *
 * <ul>
 *   <li>PostgreSQL {@code TIMESTAMP WITHOUT TIME ZONE} (NTZ) → SeaTunnel internal {@code TIMESTAMP}
 *   <li>PostgreSQL {@code TIMESTAMP WITH TIME ZONE} (LTZ) → SeaTunnel internal {@code TIMESTAMP_TZ}
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
public class JdbcPostgresTimestampIT extends TestSuiteBase implements TestResource {

    private static final String PG_IMAGE = "postgres:14-alpine";
    private static final String PG_HOST = "pg_ts_e2e";
    private static final String PG_DATABASE = "ts_test";
    private static final String PG_USER = "postgres";
    private static final String PG_PASSWORD = "postgres";

    private static final String PG_DRIVER_URL =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";

    private PostgreSQLContainer<?> pgContainer;

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
                                        + PG_DRIVER_URL);
                Assertions.assertEquals(
                        0,
                        result.getExitCode(),
                        "Failed to download PostgreSQL driver: " + result.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        pgContainer =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withDatabaseName(PG_DATABASE)
                        .withUsername(PG_USER)
                        .withPassword(PG_PASSWORD)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(PG_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));

        Startables.deepStart(Stream.of(pgContainer)).join();

        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> initPgData());
        log.info("PostgreSQL container started and test data initialised.");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (pgContainer != null) {
            pgContainer.close();
        }
    }

    /**
     * Verifies that PostgreSQL {@code TIMESTAMP WITHOUT TIME ZONE} (NTZ) columns are read as
     * SeaTunnel {@code TIMESTAMP} (i.e. {@code LOCAL_DATE_TIME_TYPE}).
     *
     * <p>The Assert sink's {@code field_type = timestamp} assertion will fail if the connector
     * incorrectly maps plain {@code TIMESTAMP} to {@code TIMESTAMP_TZ}.
     */
    @TestTemplate
    public void testPgTimestampIsNtz(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result = container.executeJob("/jdbc_pg_timestamp_to_assert.conf");
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "PostgreSQL TIMESTAMP (NTZ) assertion failed:\n" + result.getStderr());
    }

    /**
     * Verifies that PostgreSQL {@code TIMESTAMP WITH TIME ZONE} (LTZ) columns are read as SeaTunnel
     * {@code TIMESTAMP_TZ} (i.e. {@code OFFSET_DATE_TIME_TYPE}).
     *
     * <p>The Assert sink's {@code field_type = timestamp_tz} assertion will fail if the connector
     * incorrectly maps {@code TIMESTAMPTZ} to plain {@code TIMESTAMP}.
     */
    @TestTemplate
    public void testPgTimestamptzIsLtz(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result = container.executeJob("/jdbc_pg_timestamptz_to_assert.conf");
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "PostgreSQL TIMESTAMPTZ (LTZ) assertion failed:\n" + result.getStderr());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void initPgData() throws Exception {
        String jdbcUrl =
                String.format(
                        "jdbc:postgresql://%s:%d/%s",
                        pgContainer.getHost(), pgContainer.getFirstMappedPort(), PG_DATABASE);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, PG_USER, PG_PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS ts_source ("
                            + "  id       INT PRIMARY KEY,"
                            + "  ts_col   TIMESTAMP WITHOUT TIME ZONE,"
                            + "  tstz_col TIMESTAMP WITH TIME ZONE"
                            + ")");
            // ts_col: wall-clock value stored as-is (NTZ, no timezone conversion)
            // tstz_col: value with explicit UTC offset stored in UTC internally (LTZ)
            stmt.execute(
                    "INSERT INTO ts_source (id, ts_col, tstz_col) VALUES"
                            + " (1, '2026-01-01 00:00:00', '2026-01-01 00:00:00+00')");
        }
    }
}
