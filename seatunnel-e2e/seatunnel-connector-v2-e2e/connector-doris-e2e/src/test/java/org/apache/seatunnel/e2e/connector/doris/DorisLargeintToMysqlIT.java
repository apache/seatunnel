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

import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/** Doris LARGEINT keeps its full range when a non-Doris sink creates the table from the schema. */
@Slf4j
public class DorisLargeintToMysqlIT extends AbstractDorisIT {

    private static final String MYSQL_IMAGE = "mysql:8.0.43";
    private static final String MYSQL_HOST = "mysql_largeint_e2e";
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";

    private static final String DORIS_DATABASE = "e2e_largeint";
    private static final String TABLE = "largeint_table";

    private static final String MAX_LARGEINT = "170141183460469231731687303715884105727";
    private static final String MIN_LARGEINT = "-170141183460469231731687303715884105728";
    private static final String DIGITS_21 = "123456789012345678901";

    private MySQLContainer<?> mysqlContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.of(com.mysql.cj.jdbc.Driver.class)
                            .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");

    @BeforeAll
    @Override
    public void startUp() {
        super.startUp();
        mysqlContainer =
                new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
                        .withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(7)))
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));
        Startables.deepStart(Stream.of(mysqlContainer)).join();
        initializeDorisTable();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (mysqlContainer != null) {
            mysqlContainer.close();
        }
        super.tearDown();
    }

    @TestTemplate
    public void testLargeintToMysqlAutoCreate(TestContainer container) throws Exception {
        try (Connection connection = mysqlConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + MYSQL_DATABASE + "." + TABLE);
        }

        Container.ExecResult execResult = container.executeJob("/doris_largeint_to_mysql.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        try (Connection connection = mysqlConnection();
                Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =
                    statement.executeQuery(
                            "SELECT DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE"
                                    + " FROM information_schema.COLUMNS"
                                    + " WHERE TABLE_SCHEMA = '"
                                    + MYSQL_DATABASE
                                    + "' AND TABLE_NAME = '"
                                    + TABLE
                                    + "' AND COLUMN_NAME = 'f_largeint'")) {
                Assertions.assertTrue(resultSet.next(), "f_largeint was not created in MySQL");
                Assertions.assertEquals("decimal", resultSet.getString(1));
                Assertions.assertEquals(39, resultSet.getInt(2));
                Assertions.assertEquals(0, resultSet.getInt(3));
            }

            List<BigDecimal> actual = new ArrayList<>();
            try (ResultSet resultSet =
                    statement.executeQuery(
                            "SELECT f_largeint FROM "
                                    + MYSQL_DATABASE
                                    + "."
                                    + TABLE
                                    + " ORDER BY id")) {
                while (resultSet.next()) {
                    actual.add(resultSet.getBigDecimal(1));
                }
            }
            Assertions.assertEquals(
                    Arrays.asList(
                            new BigDecimal(DIGITS_21),
                            new BigDecimal(MAX_LARGEINT),
                            new BigDecimal(MIN_LARGEINT),
                            null),
                    actual);
        }
    }

    private void initializeDorisTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + DORIS_DATABASE);
            statement.execute("DROP TABLE IF EXISTS " + DORIS_DATABASE + "." + TABLE);
            statement.execute(
                    "CREATE TABLE "
                            + DORIS_DATABASE
                            + "."
                            + TABLE
                            + " (id INT NOT NULL, f_largeint LARGEINT NULL)"
                            + " DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
                            + " PROPERTIES (\"replication_allocation\" = \"tag.location.default: 1\")");
            statement.execute(
                    "INSERT INTO "
                            + DORIS_DATABASE
                            + "."
                            + TABLE
                            + " VALUES (1, "
                            + DIGITS_21
                            + "), (2, "
                            + MAX_LARGEINT
                            + "), (3, "
                            + MIN_LARGEINT
                            + "), (4, NULL)");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the Doris LARGEINT table", e);
        }
    }

    private Connection mysqlConnection() throws Exception {
        return mysqlContainer.createConnection("");
    }
}
