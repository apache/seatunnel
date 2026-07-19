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
import org.apache.seatunnel.e2e.common.container.TestContainer;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/**
 * E2E test verifying that MySQL properties supplied from json param are correctly parsed by the
 * ParameterSplitter
 */
@Slf4j
public class JdbcMysqlJsonParamIT extends TestSuiteBase implements TestResource {

    private static final String MYSQL_IMAGE = "mysql:8.0";
    private static final String MYSQL_HOST = "mysql_json_param_e2e";
    private static final String MYSQL_DATABASE = "json_param_e2e_test";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final String TEST_TABLE = "json_param_test_through_tz";

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
    public void testMysqlJsonParam1(TestContainer container)
            throws IOException, InterruptedException {
        List<String> variables = new ArrayList<>();
        variables.add("mysql_db=" + MYSQL_DATABASE);
        variables.add(
                "mysql_pros='\\{"
                        + "\"useSSL\":\"false\","
                        + "\"connectionTimeZone\":\"UTC\","
                        + "\"serverTimezone\":\"UTC\","
                        + "\"allowPublicKeyRetrieval\":\"true\"\\}'");
        variables.add("mysql_user=" + MYSQL_USER);
        variables.add("mysql_password=" + MYSQL_PASSWORD);
        variables.add("mysql_table=" + TEST_TABLE);
        // filter UTC+8 timestamp
        variables.add("ts_col='2026-07-16 20:00:00'");
        Container.ExecResult result =
                container.executeJob("/jdbc_mysql_json_param1.conf", variables);
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "MySQL timezone convert assertion failed:\n" + result.getStderr());
    }

    @TestTemplate
    public void testMysqlJsonParam2(TestContainer container)
            throws IOException, InterruptedException {
        List<String> variables = new ArrayList<>();
        variables.add("mysql_db=" + MYSQL_DATABASE);
        variables.add(
                "mysql_props='\\{"
                        + "\"useSSL\":\"false\","
                        + "\"connectionTimeZone\":\"Asia/Shanghai\","
                        + "\"serverTimezone\":\"UTC\","
                        + "\"allowPublicKeyRetrieval\":\"true\"\\}'");
        variables.add("mysql_user=" + MYSQL_USER);
        variables.add("mysql_password=" + MYSQL_PASSWORD);
        variables.add("mysql_table=" + TEST_TABLE);
        // filter UTC timestamp
        variables.add("ts='2026-07-17 04:00:00'");
        Container.ExecResult result =
                container.executeJob("/jdbc_mysql_json_param2.conf", variables);
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "MySQL timezone convert assertion failed:\n" + result.getStderr());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void initMysqlData() throws Exception {
        String jdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true",
                        mysqlContainer.getHost(),
                        mysqlContainer.getFirstMappedPort(),
                        MYSQL_DATABASE);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, MYSQL_USER, MYSQL_PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS "
                            + TEST_TABLE
                            + " ("
                            + "  id       INT PRIMARY KEY,"
                            + "  raw_ts    DATETIME,"
                            + "  ts_col   TIMESTAMP NOT NULL"
                            + ")");
            // Insert two fixed wall-clock value: 2026-07-16 20:00:00 UTC+8 and UTC
            // DATETIME stores it as-is (NTZ); TIMESTAMP stores UTC and displays in session TZ.
            stmt.execute(
                    "insert into "
                            + TEST_TABLE
                            + " values(1,'2026-07-16 20:00:00',convert_tz('2026-07-16 20:00:00','+08:00','+00:00'));");
            stmt.execute(
                    "insert into "
                            + TEST_TABLE
                            + " values(1,'2026-07-16 20:00:00',convert_tz('2026-07-16 20:00:00','+00:00','+00:00'));");
        }
    }
}
