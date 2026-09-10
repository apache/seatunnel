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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

/** End-to-end tests for GaussDB CDC with a PostgreSQL-compatible logical replication testbed. */
@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class GaussDBCDCIT extends TestSuiteBase implements TestResource {

    private static final int GAUSSDB_PORT = 5432;
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";
    private static final String GAUSSDB_DATABASE = "gaussdb_cdc";
    private static final String GAUSSDB_SCHEMA = "inventory";
    private static final String SOURCE_TABLE = "gaussdb_cdc_table";
    private static final String SINK_TABLE = "sink_gaussdb_cdc_table";
    private static final String GAUSSDB_HOST = "gaussdb_cdc_e2e";
    private static final String GAUSSDB_CDC_PLUGIN_LIB = "/tmp/seatunnel/plugins/GaussDB-CDC/lib";
    private static final String JDBC_PLUGIN_LIB = "/tmp/seatunnel/plugins/Jdbc/lib";
    private static final String SOURCE_SQL_TEMPLATE = "select * from %s.%s order by id";
    private static final String GENERATED_SLOT_PREFIX = "seatunnel_gaussdb_";

    protected static final DockerImageName PG_IMAGE =
            DockerImageName.parse("debezium/postgres:11").asCompatibleSubstituteFor("postgres");

    public static final PostgreSQLContainer<?> GAUSSDB_CONTAINER =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(GAUSSDB_HOST)
                    .withUsername(USERNAME)
                    .withPassword(PASSWORD)
                    .withDatabaseName(GAUSSDB_DATABASE)
                    .withLogConsumer(new Slf4jLogConsumer(log))
                    .withCommand("postgres", "-c", "fsync=off", "-c", "max_replication_slots=20");

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar.of(org.postgresql.Driver.class)
                        .copyTo(container, GAUSSDB_CDC_PLUGIN_LIB);
                DependencyJar.of(org.postgresql.Driver.class).copyTo(container, JDBC_PLUGIN_LIB);
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("Starting GaussDB CDC compatible container...");
        Startables.deepStart(Stream.of(GAUSSDB_CONTAINER)).join();
        initializeGaussDB();
    }

    @TestTemplate
    public void testGaussDBCdcCheckDataE2e(TestContainer container) {
        String slotVariable = toSlotVariable(createSlotName());
        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/gaussdbcdc_to_gaussdb.conf",
                                    Collections.singletonList(slotVariable));
                        } catch (Exception e) {
                            log.error("Commit task exception: {}", e.getMessage(), e);
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertIterableEquals(
                                            query(getQuerySQL(GAUSSDB_SCHEMA, SOURCE_TABLE)),
                                            query(getQuerySQL(GAUSSDB_SCHEMA, SINK_TABLE))));

            upsertDeleteSourceTable(GAUSSDB_SCHEMA, SOURCE_TABLE);

            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertIterableEquals(
                                            query(getQuerySQL(GAUSSDB_SCHEMA, SOURCE_TABLE)),
                                            query(getQuerySQL(GAUSSDB_SCHEMA, SINK_TABLE))));
        } finally {
            clearTable(GAUSSDB_SCHEMA, SOURCE_TABLE);
            clearTable(GAUSSDB_SCHEMA, SINK_TABLE);
        }
    }

    private String createSlotName() {
        return GENERATED_SLOT_PREFIX + Long.toHexString(JobIdGenerator.newJobId());
    }

    private String toSlotVariable(String slotName) {
        return "slot_name=" + slotName;
    }

    protected void initializeGaussDB() throws Exception {
        final String ddlFile = "ddl/inventory.sql";
        final URL ddlTestFile = GaussDBCDCIT.class.getClassLoader().getResource(ddlFile);
        Assertions.assertNotNull(ddlTestFile, "Cannot locate " + ddlFile);
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
        }
    }

    private void upsertDeleteSourceTable(String schema, String tableName) {
        executeSql("INSERT INTO " + schema + "." + tableName + " VALUES (2, 'inserted', 200);");
        executeSql(
                "INSERT INTO " + schema + "." + tableName + " VALUES (3, 'updated-before', 300);");
        executeSql("DELETE FROM " + schema + "." + tableName + " where id = 2;");
        executeSql(
                "UPDATE "
                        + schema
                        + "."
                        + tableName
                        + " SET name = 'updated-after', f_big = 301 where id = 3;");
    }

    private void clearTable(String schema, String tableName) {
        executeSql("truncate table " + schema + "." + tableName);
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET search_path TO " + GAUSSDB_SCHEMA);
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getQuerySQL(String schema, String tableName) {
        return String.format(SOURCE_SQL_TEMPLATE, schema, tableName);
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
                        object = new String((byte[]) object, StandardCharsets.UTF_8);
                    }
                    objects.add(object);
                }
                log.debug("Print gaussdb-CDC query, sql: {}, data: {}", sql, objects);
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://"
                        + GAUSSDB_CONTAINER.getHost()
                        + ":"
                        + GAUSSDB_CONTAINER.getMappedPort(GAUSSDB_PORT)
                        + "/"
                        + GAUSSDB_DATABASE,
                USERNAME,
                PASSWORD);
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (GAUSSDB_CONTAINER != null) {
            GAUSSDB_CONTAINER.close();
        }
    }
}
