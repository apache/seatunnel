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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSourceConfig;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.github.luben.zstd.Zstd;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;
import static org.awaitility.Awaitility.await;

/**
 * Verifies the documented PostgreSQL CDC to Iceberg recipe against snapshot, update, insert, and
 * delete events.
 */
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "The recipe uses Zeta job status and cancellation APIs")
@DisabledOnOs(OS.WINDOWS)
public class PostgresCdcToIcebergRecipeIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresCdcToIcebergRecipeIT.class);

    private static final String POSTGRES_HOST = "postgres_iceberg_recipe";
    private static final String POSTGRES_DATABASE = "sales";
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PASSWORD = "postgres";
    private static final String CATALOG_ROOT = "/tmp/seatunnel_mnt/iceberg/postgres-cdc-recipe/";
    private static final String NAMESPACE = "sales_analytics";
    private static final String TABLE = "customer_orders";
    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(POSTGRES_HOST)
                    .withDatabaseName(POSTGRES_DATABASE)
                    .withUsername(POSTGRES_USER)
                    .withPassword(POSTGRES_PASSWORD)
                    .withCommand(
                            "postgres",
                            "-c",
                            "wal_level=logical",
                            "-c",
                            "max_replication_slots=10",
                            "-c",
                            "max_wal_senders=10")
                    .withLogConsumer(
                            new Slf4jLogConsumer(
                                    DockerLoggerFactory.getLogger("postgres-iceberg-recipe")));

    /** Prepares connector dependencies and a writable catalog root in the Zeta container. */
    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                assertCommandSucceeded(
                        container.execInContainer(
                                "sh",
                                "-c",
                                "mkdir -p "
                                        + CATALOG_ROOT
                                        + " /tmp/seatunnel/plugins/Postgres-CDC/lib"
                                        + " /tmp/seatunnel/plugins/Iceberg/lib"
                                        + " && chmod -R 777 "
                                        + CATALOG_ROOT));
                DependencyJar.staged("postgresql-cdc.jar")
                        .copyTo(container, "/tmp/seatunnel/plugins/Postgres-CDC/lib");
                DependencyJar.of(Zstd.class)
                        .copyTo(container, "/tmp/seatunnel/plugins/Iceberg/lib");
            };

    /** Starts PostgreSQL with logical replication enabled and creates deterministic source data. */
    @BeforeAll
    @Override
    public void startUp() throws Exception {
        Startables.deepStart(Stream.of(POSTGRES_CONTAINER)).join();
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA inventory");
            statement.execute(
                    "CREATE TABLE inventory.customer_orders ("
                            + "id BIGINT PRIMARY KEY, customer_name VARCHAR(64) NOT NULL, "
                            + "amount NUMERIC(10, 2) NOT NULL, status VARCHAR(16) NOT NULL, "
                            + "updated_at TIMESTAMP NOT NULL)");
            statement.execute("ALTER TABLE inventory.customer_orders REPLICA IDENTITY FULL");
            statement.execute(
                    "INSERT INTO inventory.customer_orders VALUES "
                            + "(1001, ' Alice Zhang ', 120.50, 'pending', '2026-07-18 09:00:00'),"
                            + "(1002, 'Bob Li', 80.00, 'paid', '2026-07-18 09:05:00')");
        }
    }

    /** Stops the PostgreSQL container created by this test suite. */
    @AfterAll
    @Override
    public void tearDown() {
        POSTGRES_CONTAINER.close();
    }

    /**
     * Runs the exact recipe config and checks both the initial snapshot and the final CDC state in
     * Iceberg.
     */
    @TestTemplate
    public void testPostgresCdcToIcebergRecipe(TestContainer container) throws Exception {
        String jobId = String.valueOf(System.nanoTime());
        String slotName = "seatunnel_iceberg_" + jobId;
        String warehouse = CATALOG_ROOT;

        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/iceberg/postgres_cdc_to_iceberg_recipe.conf",
                                        jobId,
                                        "slot_name=" + slotName,
                                        "warehouse=file://" + warehouse);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            await().atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(jobFuture.isCompletedExceptionally());
                                Assertions.assertEquals("RUNNING", container.getJobStatus(jobId));
                                assertRows(
                                        warehouse,
                                        expectedRows(
                                                1001L,
                                                "Alice Zhang",
                                                "120.50",
                                                "PENDING",
                                                1002L,
                                                "Bob Li",
                                                "80.00",
                                                "PAID"));
                            });

            applyIncrementalChanges();

            await().atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () ->
                                    assertRows(
                                            warehouse,
                                            expectedRows(
                                                    1001L,
                                                    "Alice Zhang",
                                                    "150.75",
                                                    "PAID",
                                                    1003L,
                                                    "Carol Wang",
                                                    "42.00",
                                                    "PENDING")));
        } finally {
            Container.ExecResult cancelResult = container.cancelJob(jobId);
            Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
        }
    }

    /** Applies one update, one insert, and one delete after the initial snapshot is committed. */
    private void applyIncrementalChanges() throws Exception {
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE inventory.customer_orders SET amount = 150.75, status = 'paid', "
                            + "updated_at = '2026-07-18 10:00:00' WHERE id = 1001");
            statement.execute(
                    "INSERT INTO inventory.customer_orders VALUES "
                            + "(1003, ' Carol Wang ', 42.00, 'pending', '2026-07-18 10:05:00')");
            statement.execute("DELETE FROM inventory.customer_orders WHERE id = 1002");
        }
    }

    /** Reads the Iceberg table and compares every final business field by primary key. */
    private void assertRows(String warehouse, Map<Long, ExpectedOrder> expectedRows)
            throws IOException {
        Map<Long, Record> actualRows =
                loadRows(warehouse).stream()
                        .collect(Collectors.toMap(row -> (Long) row.getField("id"), row -> row));
        Assertions.assertEquals(expectedRows.keySet(), actualRows.keySet());
        expectedRows.forEach(
                (id, expected) -> {
                    Record actual = actualRows.get(id);
                    Assertions.assertEquals(
                            expected.customerName, actual.getField("customer_name"));
                    Assertions.assertEquals(expected.amount, actual.getField("amount"));
                    Assertions.assertEquals(expected.statusName, actual.getField("status_name"));
                    Assertions.assertEquals("postgresql_cdc", actual.getField("sync_source"));
                });
    }

    /** Builds the two-row expectation used before and after incremental changes. */
    private Map<Long, ExpectedOrder> expectedRows(
            long firstId,
            String firstName,
            String firstAmount,
            String firstStatus,
            long secondId,
            String secondName,
            String secondAmount,
            String secondStatus) {
        Map<Long, ExpectedOrder> expectedRows = new HashMap<>();
        expectedRows.put(
                firstId, new ExpectedOrder(firstName, new BigDecimal(firstAmount), firstStatus));
        expectedRows.put(
                secondId,
                new ExpectedOrder(secondName, new BigDecimal(secondAmount), secondStatus));
        return expectedRows;
    }

    /** Loads the current snapshot from the Hadoop catalog used by the recipe job. */
    private List<Record> loadRows(String warehouse) throws IOException {
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", HADOOP.getType());
        catalogProps.put("warehouse", "file://" + warehouse);

        Map<String, Object> config = new HashMap<>();
        config.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "recipe_catalog");
        config.put(IcebergCommonOptions.KEY_NAMESPACE.key(), NAMESPACE);
        config.put(IcebergCommonOptions.KEY_TABLE.key(), TABLE);
        config.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProps);

        try (IcebergTableLoader tableLoader =
                IcebergTableLoader.create(
                        new IcebergSourceConfig(ReadonlyConfig.fromMap(config)))) {
            tableLoader.open();
            Table table = tableLoader.loadTable();
            List<Record> rows = new ArrayList<>();
            try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                records.forEach(rows::add);
            }
            return rows;
        }
    }

    /** Opens a JDBC connection to the PostgreSQL Testcontainer. */
    private Connection getConnection() throws Exception {
        return DriverManager.getConnection(
                POSTGRES_CONTAINER.getJdbcUrl(), POSTGRES_USER, POSTGRES_PASSWORD);
    }

    /** Fails immediately when a container setup command is unsuccessful. */
    private static void assertCommandSucceeded(Container.ExecResult result) {
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    /** Expected transformed values for one Iceberg row. */
    private static class ExpectedOrder {
        private final String customerName;
        private final BigDecimal amount;
        private final String statusName;

        private ExpectedOrder(String customerName, BigDecimal amount, String statusName) {
            this.customerName = customerName;
            this.amount = amount;
            this.statusName = statusName;
        }
    }
}
