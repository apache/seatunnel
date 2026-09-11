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
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
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
 * E2E test verifying that MariaDB NTZ/LTZ timestamp types are correctly distinguished by the JDBC
 * connector.
 *
 * <ul>
 *   <li>MariaDB {@code DATETIME} (NTZ) → SeaTunnel internal {@code TIMESTAMP} type
 *   <li>MariaDB {@code TIMESTAMP} (LTZ) → SeaTunnel internal {@code TIMESTAMP_TZ} type
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
public class JdbcMariaDBTimestampIT extends TestSuiteBase implements TestResource {

    private static final String MARIADB_IMAGE = "mariadb:10.11";
    private static final String MARIADB_HOST = "mariadb_ts_e2e";
    private static final String MARIADB_DATABASE = "ts_test";
    private static final String MARIADB_USER = "root";
    private static final String MARIADB_PASSWORD = "Abc!@#135_seatunnel";

    private GenericContainer<?> mariadbContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("org.mariadb.jdbc.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        mariadbContainer =
                new GenericContainer<>(DockerImageName.parse(MARIADB_IMAGE))
                        .withEnv("MARIADB_ROOT_PASSWORD", MARIADB_PASSWORD)
                        .withEnv("MARIADB_ROOT_HOST", "%")
                        .withEnv("MARIADB_DATABASE", MARIADB_DATABASE)
                        .withExposedPorts(3306)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MARIADB_HOST)
                        .waitingFor(Wait.forLogMessage(".*ready for connections.*\\n", 2))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MARIADB_IMAGE)));

        Startables.deepStart(Stream.of(mariadbContainer)).join();

        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> initMariaDbData());
        log.info("MariaDB container started and test data initialised.");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (mariadbContainer != null) {
            mariadbContainer.close();
        }
    }

    /**
     * Verifies that MariaDB {@code DATETIME} (NTZ) columns are read as SeaTunnel {@code TIMESTAMP}
     * (i.e. {@code LOCAL_DATE_TIME_TYPE}), not {@code TIMESTAMP_TZ}.
     */
    @TestTemplate
    public void testMariaDbDatetimeIsNtz(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result = container.executeJob("/jdbc_mariadb_datetime_to_assert.conf");
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "MariaDB DATETIME (NTZ) assertion failed:\n" + result.getStderr());
    }

    /**
     * Verifies that MariaDB {@code TIMESTAMP} (LTZ) columns are read as SeaTunnel {@code
     * TIMESTAMP_TZ} (i.e. {@code OFFSET_DATE_TIME_TYPE}).
     */
    @TestTemplate
    public void testMariaDbTimestampIsLtz(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                container.executeJob("/jdbc_mariadb_timestamp_to_assert.conf");
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "MariaDB TIMESTAMP (LTZ) assertion failed:\n" + result.getStderr());
    }

    /**
     * Verifies that MariaDB {@code TIMESTAMP} (LTZ) preserves the correct UTC instant when the JDBC
     * connection uses a non-UTC {@code serverTimezone} (Asia/Seoul, +09:00).
     */
    @TestTemplate
    public void testMariaDbTimestampIsLtzInNonUtcSession(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                container.executeJob("/jdbc_mariadb_timestamp_non_utc_to_assert.conf");
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "MariaDB TIMESTAMP (LTZ) assertion failed with non-UTC serverTimezone (Asia/Seoul):\n"
                        + result.getStderr());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void initMariaDbData() throws Exception {
        String jdbcUrl =
                String.format(
                        "jdbc:mariadb://%s:%d/%s?useSSL=false&serverTimezone=UTC",
                        mariadbContainer.getHost(),
                        mariadbContainer.getFirstMappedPort(),
                        MARIADB_DATABASE);
        try (Connection conn =
                        DriverManager.getConnection(jdbcUrl, MARIADB_USER, MARIADB_PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS ts_source ("
                            + "  id       INT PRIMARY KEY,"
                            + "  dt_col   DATETIME,"
                            + "  ts_col   TIMESTAMP NULL"
                            + ")");
            stmt.execute(
                    "INSERT INTO ts_source (id, dt_col, ts_col) VALUES"
                            + " (1, '2026-01-01 00:00:00', '2026-01-01 00:00:00')");
        }
    }
}
