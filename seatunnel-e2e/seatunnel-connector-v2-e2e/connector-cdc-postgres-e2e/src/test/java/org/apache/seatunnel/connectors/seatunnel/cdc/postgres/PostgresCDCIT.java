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
    private static final String DEFAULT_DB = "postgres";

    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";

    private static final String POSTGRES_HOST = "postgres_cdc_e2e";

    private static final String POSTGRESQL_DATABASE = "postgres_cdc";
    private static final String POSTGRESQL_SCHEMA = "inventory";

    private static final String SOURCE_TABLE = "postgres_cdc_e2e_source_table";
    private static final String SINK_TABLE = "postgres_cdc_e2e_sink_table";

    private static final String SOURCE_SQL_TEMPLATE = "select * from %s.%s";

    // use newer version of postgresql image to support pgoutput plugin
    // when testing postgres 13, only 13-alpine supports both amd64 and arm64
    protected static final DockerImageName PG_IMAGE =
            DockerImageName.parse("debezium/postgres:9.6").asCompatibleSubstituteFor("postgres");

    public static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(POSTGRES_HOST)
                    .withDatabaseName(DEFAULT_DB)
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
        initializePostgresTable(POSTGRES_CONTAINER, "column_type_test");
    }

    @TestTemplate
    public void testMPostgresCdcCheckDataE2e(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(POSTGRESQL_SCHEMA, SOURCE_TABLE);
        clearTable(POSTGRESQL_SCHEMA, SINK_TABLE);

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
                            log.info(query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE)).toString());
                            Assertions.assertIterableEquals(
                                    query(getQuerySQL(POSTGRESQL_SCHEMA, SOURCE_TABLE)),
                                    query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE)));
                        });

        // insert update delete
        upsertDeleteSourceTable(POSTGRESQL_SCHEMA, SOURCE_TABLE);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(getQuerySQL(POSTGRESQL_SCHEMA, SOURCE_TABLE)),
                                    query(getQuerySQL(POSTGRESQL_SCHEMA, SINK_TABLE)));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support multi table")
    public void testPostgresCdcMultiTableE2e(TestContainer container) {}

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support multi table")
    public void testMultiTableWithRestore(TestContainer container)
            throws IOException, InterruptedException {}

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
                    objects.add(resultSet.getObject(i));
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
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void upsertDeleteSourceTable(String database, String tableName) {}

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
