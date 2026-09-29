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
import org.apache.seatunnel.e2e.common.util.DependencyJar;

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
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.seatunnel.e2e.common.util.JdbcUtil.querySql;
import static org.awaitility.Awaitility.given;

@Slf4j
public class JdbcAutoGenerateSQLIT extends TestSuiteBase implements TestResource {
    private static final String PG_IMAGE = "postgres:14-alpine";
    private PostgreSQLContainer<?> postgreSQLContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("org.postgresql.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        postgreSQLContainer =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgresql")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(postgreSQLContainer)).join();
        log.info("PostgreSQL container started");
        Class.forName(postgreSQLContainer.getDriverClassName());
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
    }

    @TestTemplate
    public void testAutoGenerateSQL(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/jdbc_sink_auto_generate_sql.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<Object> result =
                querySql(
                                "select * from sink limit 1",
                                () -> {
                                    try {
                                        return DriverManager.getConnection(
                                                postgreSQLContainer.getJdbcUrl(),
                                                postgreSQLContainer.getUsername(),
                                                postgreSQLContainer.getPassword());
                                    } catch (SQLException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .get(0);
        Assertions.assertInstanceOf(Long.class, result.get(0));
        Assertions.assertInstanceOf(String.class, result.get(1));
        Assertions.assertInstanceOf(Integer.class, result.get(2));
        Assertions.assertInstanceOf(java.sql.Timestamp.class, result.get(3));
    }

    @TestTemplate
    public void testAutoGenerateUpsertSQL(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/jdbc_sink_auto_generate_upsql_sql.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    /** Verifies that the PostgreSQL quick start writes the exact rows documented for users. */
    @TestTemplate
    public void testDocumentedPostgresQuickStart(TestContainer container)
            throws IOException, InterruptedException {
        truncateOrdersTable();
        Container.ExecResult execResult =
                container.executeJob("/jdbc_sink_postgres_quick_start.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        List<List<Object>> result =
                querySql(
                        "select id, customer_name, amount from orders order by id",
                        () -> {
                            try {
                                return DriverManager.getConnection(
                                        postgreSQLContainer.getJdbcUrl(),
                                        postgreSQLContainer.getUsername(),
                                        postgreSQLContainer.getPassword());
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(
                Arrays.asList(1L, "Alice", new BigDecimal("120.50")), result.get(0));
        Assertions.assertEquals(Arrays.asList(2L, "Bob", new BigDecimal("80.00")), result.get(1));
        Assertions.assertEquals(Arrays.asList(3L, "Carol", new BigDecimal("42.00")), result.get(2));
    }

    /**
     * Clears rows from earlier TestTemplate invocations so every engine must prove its own write.
     */
    private void truncateOrdersTable() {
        try (Connection connection =
                        DriverManager.getConnection(
                                postgreSQLContainer.getJdbcUrl(),
                                postgreSQLContainer.getUsername(),
                                postgreSQLContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("truncate table orders");
        } catch (SQLException e) {
            throw new RuntimeException("Truncating PostgreSql orders table failed!", e);
        }
    }

    private void initializeJdbcTable() {
        try (Connection connection =
                DriverManager.getConnection(
                        postgreSQLContainer.getJdbcUrl(),
                        postgreSQLContainer.getUsername(),
                        postgreSQLContainer.getPassword())) {
            Statement statement = connection.createStatement();
            String sink =
                    "create table sink(\n"
                            + "user_id BIGINT NOT NULL PRIMARY KEY,\n"
                            + "name varchar(255),\n"
                            + "age INT,\n"
                            + "timestamp_tz TIMESTAMPTZ \n"
                            + ")";
            statement.execute(sink);
            statement.execute(
                    "create table if not exists orders("
                            + "id BIGINT PRIMARY KEY,"
                            + "customer_name VARCHAR(100) NOT NULL,"
                            + "amount DECIMAL(10, 2) NOT NULL)");
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (postgreSQLContainer != null) {
            postgreSQLContainer.stop();
        }
    }
}
