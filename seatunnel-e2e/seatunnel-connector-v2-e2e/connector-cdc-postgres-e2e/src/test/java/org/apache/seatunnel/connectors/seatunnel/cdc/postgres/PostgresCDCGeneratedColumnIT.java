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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres;

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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/** Generated columns are not streamed by pgoutput, so they must not be part of the CDC schema. */
@Slf4j
public class PostgresCDCGeneratedColumnIT extends TestSuiteBase implements TestResource {

    private static final String POSTGRES_HOST = "postgres_generated_column_e2e";
    private static final String DATABASE = "generated_cdc";
    private static final String SOURCE_TABLE = "public.orders";
    private static final String SINK_TABLE = "public.sink_orders";
    private static final String POSTGRES_CDC_PLUGIN_LIB = "/tmp/seatunnel/plugins/Postgres-CDC/lib";

    // generated columns need PostgreSQL 12+
    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(POSTGRES_HOST)
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .withDatabaseName(DATABASE)
                    .withLogConsumer(new Slf4jLogConsumer(log))
                    .withCommand("postgres", "-c", "wal_level=logical", "-c", "fsync=off");

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.of(org.postgresql.Driver.class)
                            .copyTo(container, POSTGRES_CDC_PLUGIN_LIB);

    @BeforeAll
    @Override
    public void startUp() {
        Startables.deepStart(Stream.of(POSTGRES_CONTAINER)).join();
        executeSql(
                "CREATE TABLE "
                        + SOURCE_TABLE
                        + " (id INT PRIMARY KEY, price NUMERIC(10, 2),"
                        + " total NUMERIC(10, 2) GENERATED ALWAYS AS (price * 2) STORED)");
        executeSql("ALTER TABLE " + SOURCE_TABLE + " REPLICA IDENTITY FULL");
        executeSql("INSERT INTO " + SOURCE_TABLE + " (id, price) VALUES (1, 1.00), (2, 2.00)");
    }

    @AfterAll
    @Override
    public void tearDown() {
        POSTGRES_CONTAINER.close();
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently only Zeta supports Postgres-CDC schema evolution")
    public void testGeneratedColumnIsExcludedFromCdcSchema(TestContainer container) {
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/postgrescdc_to_postgres_with_generated_column.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        given().ignoreExceptions()
                .await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query("SELECT id, price FROM " + SOURCE_TABLE),
                                        query("SELECT id, price FROM " + SINK_TABLE)));

        // the first streamed change used to fail the job or write NULL into the generated column
        executeSql("UPDATE " + SOURCE_TABLE + " SET price = 7.00 WHERE id = 1");
        executeSql("INSERT INTO " + SOURCE_TABLE + " (id, price) VALUES (3, 3.00)");

        given().ignoreExceptions()
                .await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query("SELECT id, price FROM " + SOURCE_TABLE),
                                        query("SELECT id, price FROM " + SINK_TABLE)));
        Assertions.assertTrue(
                query(
                                "SELECT column_name FROM information_schema.columns"
                                        + " WHERE table_name = 'sink_orders'"
                                        + " AND column_name = 'total'")
                        .isEmpty());
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES_CONTAINER.getJdbcUrl(),
                POSTGRES_CONTAINER.getUsername(),
                POSTGRES_CONTAINER.getPassword());
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<List<Object>> query(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql + " ORDER BY 1")) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(resultSet.getObject(i));
                }
                result.add(row);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
