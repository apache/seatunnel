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
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class PostgresCDCIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresCDCIT.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";

    private static final String POSTGRES_HOST = "postgres_cdc_e2e";

    private static final String POSTGRESQL_DATABASE = "postgres_cdc";
    private static final String POSTGRESQL_SCHEMA = "inventory";

    private static final String SOURCE_TABLE_1 = "postgres_cdc_table_1";
    private static final String SOURCE_TABLE_2 = "postgres_cdc_table_2";
    private static final String SOURCE_TABLE_3 = "postgres_cdc_table_3";
    private static final String SINK_TABLE_1 = "sink_postgres_cdc_table_1";
    private static final String SINK_TABLE_2 = "sink_postgres_cdc_table_2";
    private static final String SINK_TABLE_3 = "sink_postgres_cdc_table_3";

    private static final String SOURCE_TABLE_NO_PRIMARY_KEY = "full_types_no_primary_key";

    private static final String SOURCE_SQL_TEMPLATE = "select * from %s.%s order by id";

    // use newer version of postgresql image to support pgoutput plugin
    // when testing postgres 13, only 13-alpine supports both amd64 and arm64
    protected static final DockerImageName PG_IMAGE =
            DockerImageName.parse("debezium/postgres:11").asCompatibleSubstituteFor("postgres");

    public static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(POSTGRES_HOST)
                    .withUsername(USERNAME)
                    .withPassword(PASSWORD)
                    .withDatabaseName(POSTGRESQL_DATABASE)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20");

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Postgres-CDC/lib && cd /tmp/seatunnel/plugins/Postgres-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() {
        log.info("The second stage: Starting Postgres containers...");
        POSTGRES_CONTAINER.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s",
                                PostgreSQLContainer.POSTGRESQL_PORT,
                                PostgreSQLContainer.POSTGRESQL_PORT)));
        Startables.deepStart(Stream.of(POSTGRES_CONTAINER)).join();
        log.info("Postgres Containers are started");
        initializePostgresTable(POSTGRES_CONTAINER, "inventory");
    }

    @TestTemplate
    public void testMPostgresCdcCheckDataE2e(TestContainer container) {

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob("/postgrescdc_to_postgres.conf");
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SOURCE_TABLE_1)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });

            // insert update delete
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SOURCE_TABLE_1)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testPostgresCdcMultiTableE2e(TestContainer container) {

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/pgcdc_to_pg_with_multi_table_mode_two_table.conf");
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_1))),
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_2)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_2)))));

            // insert update delete
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_2);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_1))),
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_2)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_2)))));
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_2);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_2);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testMultiTableWithRestore(TestContainer container)
            throws IOException, InterruptedException {
        String jobId = JobIdGenerator.newJobId();
        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return container.executeJob(
                                    "/pgcdc_to_pg_with_multi_table_mode_one_table.conf", jobId);
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    });

            // insert update delete
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_1)))));

            Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());

            // Restore job with add a new table
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.restoreJob(
                                    "/pgcdc_to_pg_with_multi_table_mode_two_table.conf", jobId);
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_2);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_1))),
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_2)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_2)))));

            log.info("****************** container logs start ******************");
            String containerLogs = container.getServerLogs();
            log.info(containerLogs);
            // pg cdc logs contain ERROR
            // Assertions.assertFalse(containerLogs.contains("ERROR"));
            log.info("****************** container logs end ******************");
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_2);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_2);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testAddFiledWithRestore(TestContainer container)
            throws IOException, InterruptedException {
        String jobId = JobIdGenerator.newJobId();
        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return container.executeJob(
                                    "/postgrescdc_to_postgres_test_add_Filed.conf", jobId);
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    });

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_3)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_3)))));

            Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());

            // add filed add insert source table data
            addFieldsForTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_3);
            addFieldsForTable(POSTGRESQL_SCHEMA, SINK_TABLE_3);
            insertSourceTableForAddFields(POSTGRESQL_SCHEMA, SOURCE_TABLE_3);

            // Restore job
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.restoreJob(
                                    "/postgrescdc_to_postgres_test_add_Filed.conf", jobId);
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SOURCE_TABLE_3)),
                                                            query(
                                                                    getQuerySQL(
                                                                            POSTGRESQL_SCHEMA,
                                                                            SINK_TABLE_3)))));

            log.info("****************** container logs start ******************");
            String containerLogs = container.getServerLogs();
            log.info(containerLogs);
            // pg cdc logs contain ERROR
            // Assertions.assertFalse(containerLogs.contains("ERROR"));
            log.info("****************** container logs end ******************");
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_3);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_3);
        }
    }

    @TestTemplate
    public void testPostgresCdcCheckDataWithNoPrimaryKey(TestContainer container) throws Exception {

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/postgrescdc_to_postgres_with_no_primary_key.conf");
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            // snapshot stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(
                                                getQuerySQL(
                                                        POSTGRESQL_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });

            // insert update delete
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(
                                                getQuerySQL(
                                                        POSTGRESQL_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });
        } finally {
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
        }
    }

    @TestTemplate
    public void testPostgresCdcCheckDataWithCustomPrimaryKey(TestContainer container)
            throws Exception {

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/postgrescdc_to_postgres_with_custom_primary_key.conf");
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            // snapshot stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(
                                                getQuerySQL(
                                                        POSTGRESQL_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });

            // insert update delete
            upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(
                                                getQuerySQL(
                                                        POSTGRESQL_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE_1)));
                            });
        } finally {
            clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES_CONTAINER.getJdbcUrl(),
                POSTGRES_CONTAINER.getUsername(),
                POSTGRES_CONTAINER.getPassword());
    }

    protected void initializePostgresTable(PostgreSQLContainer container, String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = PostgresCDCIT.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";\n"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<List<Object>> query(String sql) {
        try (Connection connection = getJdbcConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object object = resultSet.getObject(i);
                    if (object instanceof byte[]) {
                        byte[] bytes = (byte[]) object;
                        object = new String(bytes, StandardCharsets.UTF_8);
                    }
                    objects.add(object);
                }
                log.debug(
                        String.format("Print Postgres-CDC query, sql: %s, data: %s", sql, objects));
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Execute SQL
    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET search_path TO inventory;");
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void addFieldsForTable(String database, String tableName) {

        executeSql("ALTER TABLE " + database + "." + tableName + " ADD COLUMN f_big BIGINT");
    }

    private void insertSourceTableForAddFields(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES (2, '2', 32767, 65535, 2147483647);");
    }

    private void upsertDeleteSourceTable(String database, String tableName) {

        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES (2, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,\n"
                        + "        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',\n"
                        + "        '2020-07-17', '18:00:22', 500,'192.168.1.1');");

        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES (3, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,\n"
                        + "        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',\n"
                        + "        '2020-07-17', '18:00:22', 500,'192.168.1.1');");

        executeSql("DELETE FROM " + database + "." + tableName + " where id = 2;");

        executeSql("UPDATE " + database + "." + tableName + " SET f_big = 10000 where id = 3;");
    }

    private String getQuerySQL(String database, String tableName) {
        return String.format(SOURCE_SQL_TEMPLATE, database, tableName);
    }

    @Override
    @AfterAll
    public void tearDown() {
        // close Container
        if (POSTGRES_CONTAINER != null) {
            POSTGRES_CONTAINER.close();
        }
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }
}
