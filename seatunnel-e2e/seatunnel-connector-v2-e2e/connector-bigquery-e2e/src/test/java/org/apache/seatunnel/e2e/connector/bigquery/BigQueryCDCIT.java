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

package org.apache.seatunnel.e2e.connector.bigquery;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.awaitility.Awaitility.await;

@Slf4j
@Disabled("bigquery-emulator does not support bigquery storage write api.")
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class BigQueryCDCIT extends AbstractBigqueryIT {

    private static final String CDC_TABLE_NAME = "cdc_test_table";
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

    private TableId cdcTableId;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @BeforeAll
    @Override
    public void startUp() {
        super.startUp();

        log.info("Starting MySQL containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("MySQL containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("MySQL ddl execution is complete");

        try {
            initializeBigQueryCDCTable();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeBigQueryCDCTable() throws InterruptedException {
        this.cdcTableId = TableId.of(PROJECT_NAME, DATASET_NAME, CDC_TABLE_NAME);

        String sql =
                String.format(
                        "CREATE TABLE IF NOT EXISTS `%s.%s.%s` ("
                                + "  uuid INT64 NOT NULL,"
                                + "  name STRING,"
                                + "  score INT64,"
                                + "  PRIMARY KEY (uuid) NOT ENFORCED"
                                + ") "
                                + "OPTIONS (max_staleness = INTERVAL 0 MINUTE);",
                        PROJECT_NAME, DATASET_NAME, CDC_TABLE_NAME);

        bigquery.query(QueryJobConfiguration.of(sql));
        log.info("BigQuery CDC table created: {}", cdcTableId);
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
        super.tearDown();
    }

    @TestTemplate
    public void testBigQueryCDCSink(TestContainer container) {
        cleanCDCTable();

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysql_cdc_to_bigquery_sink.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception: " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // Verify initial snapshot data
        Set<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(1L, "Alice", 95L), Arrays.asList(2L, "Bob", 88L))
                        .collect(Collectors.toSet());

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Set<List<Object>> actual = queryBigQueryCDCTable();
                            log.info("Actual data in BigQuery CDC table: {}", actual);
                            Assertions.assertEquals(expected, actual);
                        });

        // Execute DELETE on MySQL source
        executeMysqlSql("DELETE FROM " + MYSQL_DATABASE + "." + SOURCE_TABLE + " WHERE uuid = 1");

        // Verify after delete
        Set<List<Object>> expectedAfterDelete =
                Stream.<List<Object>>of(Arrays.asList(2L, "Bob", 88L)).collect(Collectors.toSet());

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Set<List<Object>> actual = queryBigQueryCDCTable();
                            Assertions.assertEquals(expectedAfterDelete, actual);
                        });

        // Re-insert deleted row
        executeMysqlSql(
                "INSERT INTO " + MYSQL_DATABASE + "." + SOURCE_TABLE + " VALUES (1, 'Alice', 95)");

        // Verify after re-insert
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Set<List<Object>> actual = queryBigQueryCDCTable();
                            Assertions.assertEquals(expected, actual);
                        });
    }

    private Set<List<Object>> queryBigQueryCDCTable() {
        String query =
                String.format(
                        "SELECT uuid, name, score FROM `%s.%s.%s`",
                        cdcTableId.getProject(), cdcTableId.getDataset(), cdcTableId.getTable());

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

        try {
            TableResult result = bigquery.query(queryConfig);

            return StreamSupport.stream(result.iterateAll().spliterator(), false)
                    .map(
                            row ->
                                    Arrays.<Object>asList(
                                            row.get(0).isNull() ? null : row.get(0).getLongValue(),
                                            row.get(1).isNull()
                                                    ? null
                                                    : row.get(1).getStringValue(),
                                            row.get(2).isNull() ? null : row.get(2).getLongValue()))
                    .collect(Collectors.toSet());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BigQuery query interrupted", e);
        }
    }

    private void cleanCDCTable() {
        if (bigquery.getTable(cdcTableId) != null) {
            bigquery.delete(cdcTableId);
            log.info("BigQuery CDC table deleted: {}", cdcTableId);
        }
        try {
            initializeBigQueryCDCTable();
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getMysqlJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    private void executeMysqlSql(String sql) {
        try (Connection connection = getMysqlJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
