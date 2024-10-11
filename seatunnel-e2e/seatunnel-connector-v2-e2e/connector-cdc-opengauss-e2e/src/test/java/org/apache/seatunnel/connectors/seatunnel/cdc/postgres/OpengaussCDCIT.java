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
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
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
import static org.awaitility.Awaitility.given;
import static org.junit.Assert.assertNotNull;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class OpengaussCDCIT extends TestSuiteBase implements TestResource {
    private static final int OPENGAUSS_PORT = 5432;
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final String USERNAME = "gaussdb";
    private static final String PASSWORD = "openGauss@123";
    private static final String OPENGAUSSQL_DATABASE = "opengauss_cdc";
    private static final String OPENGAUSSQL_DEFAULT_DATABASE = "postgres";
    private static final String OPENGAUSS_SCHEMA = "inventory";

    private static final String SOURCE_TABLE_1 = "opengauss_cdc_table_1";
    private static final String SOURCE_TABLE_2 = "opengauss_cdc_table_2";
    private static final String SOURCE_TABLE_3 = "opengauss_cdc_table_3";
    private static final String SINK_TABLE_1 = "sink_opengauss_cdc_table_1";
    private static final String SINK_TABLE_2 = "sink_opengauss_cdc_table_2";
    private static final String SINK_TABLE_3 = "sink_opengauss_cdc_table_3";

    private static final String SOURCE_TABLE_NO_PRIMARY_KEY = "full_types_no_primary_key";

    private static final String OPENGAUSS_HOST = "opengauss_cdc_e2e";

    protected static final DockerImageName OPENGAUSS_IMAGE =
            DockerImageName.parse("opengauss/opengauss:5.0.0")
                    .asCompatibleSubstituteFor("postgres");

    private static final String SOURCE_SQL_TEMPLATE = "select * from %s.%s order by id";

    public static final GenericContainer<?> OPENGAUSS_CONTAINER =
            new GenericContainer<>(OPENGAUSS_IMAGE)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(OPENGAUSS_HOST)
                    .withEnv("GS_PASSWORD", PASSWORD)
                    .withLogConsumer(new Slf4jLogConsumer(log));

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
                                "mkdir -p /tmp/seatunnel/plugins/JDBC/lib && cd /tmp/seatunnel/plugins/JDBC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("The second stage: Starting opengauss containers...");
        OPENGAUSS_CONTAINER.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", OPENGAUSS_PORT, OPENGAUSS_PORT)));
        Startables.deepStart(Stream.of(OPENGAUSS_CONTAINER)).join();
        log.info("Opengauss Containers are started");
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeOpengaussSql);

        String[] command1 = {
            "/bin/sh",
            "-c",
            "sed -i 's/^#password_encryption_type = 2/password_encryption_type = 1/' /var/lib/opengauss/data/postgresql.conf"
        };
        Container.ExecResult result1 = OPENGAUSS_CONTAINER.execInContainer(command1);
        Assertions.assertEquals(0, result1.getExitCode());

        String[] command2 = {
            "/bin/sh",
            "-c",
            "sed -i 's/host replication gaussdb 0.0.0.0\\/0 md5/host replication gaussdb 0.0.0.0\\/0 sha256/' /var/lib/opengauss/data/pg_hba.conf"
        };
        Container.ExecResult result2 = OPENGAUSS_CONTAINER.execInContainer(command2);
        Assertions.assertEquals(0, result2.getExitCode());
        String[] command3 = {
            "/bin/sh",
            "-c",
            "echo \"host all dailai 0.0.0.0/0 md5\" >> /var/lib/opengauss/data/pg_hba.conf"
        };
        Container.ExecResult result3 = OPENGAUSS_CONTAINER.execInContainer(command3);
        Assertions.assertEquals(0, result3.getExitCode());

        reloadConf();

        createNewUserForJdbcSink();
    }

    @TestTemplate
    public void testOpengaussCdcCheckDataE2e(TestContainer container) {
        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob("/opengausscdc_to_opengauss.conf");
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
                                        query(getQuerySQL(OPENGAUSS_SCHEMA, SOURCE_TABLE_1)),
                                        query(getQuerySQL(OPENGAUSS_SCHEMA, SINK_TABLE_1)));
                            });

            // insert update delete
            upsertDeleteSourceTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_1);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(getQuerySQL(OPENGAUSS_SCHEMA, SOURCE_TABLE_1)),
                                        query(getQuerySQL(OPENGAUSS_SCHEMA, SINK_TABLE_1)));
                            });
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_1);
            clearTable(OPENGAUSS_SCHEMA, SINK_TABLE_1);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testOpengaussCdcMultiTableE2e(TestContainer container) {
        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/opengausscdc_to_opengauss_with_multi_table_mode_two_table.conf");
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
                                                                            OPENGAUSS_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SINK_TABLE_1))),
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SOURCE_TABLE_2)),
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SINK_TABLE_2)))));

            // insert update delete
            upsertDeleteSourceTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_1);
            upsertDeleteSourceTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_2);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SINK_TABLE_1))),
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SOURCE_TABLE_2)),
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SINK_TABLE_2)))));
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_1);
            clearTable(OPENGAUSS_SCHEMA, SINK_TABLE_1);
            clearTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_2);
            clearTable(OPENGAUSS_SCHEMA, SINK_TABLE_2);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testMultiTableWithRestore(TestContainer container)
            throws IOException, InterruptedException {
        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return container.executeJob(
                                    "/opengausscdc_to_opengauss_with_multi_table_mode_one_table.conf");
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    });

            // insert update delete
            upsertDeleteSourceTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_1);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SINK_TABLE_1)))));

            Pattern jobIdPattern =
                    Pattern.compile(
                            ".*Init JobMaster for Job opengausscdc_to_opengauss_with_multi_table_mode_one_table.conf \\(([0-9]*)\\).*",
                            Pattern.DOTALL);
            Matcher matcher = jobIdPattern.matcher(container.getServerLogs());
            String jobId;
            if (matcher.matches()) {
                jobId = matcher.group(1);
            } else {
                throw new RuntimeException("Can not find jobId");
            }

            Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());

            // Restore job with add a new table
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.restoreJob(
                                    "/opengausscdc_to_opengauss_with_multi_table_mode_two_table.conf",
                                    jobId);
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            upsertDeleteSourceTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_2);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertAll(
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SOURCE_TABLE_1)),
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SINK_TABLE_1))),
                                            () ->
                                                    Assertions.assertIterableEquals(
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SOURCE_TABLE_2)),
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SINK_TABLE_2)))));

            log.info("****************** container logs start ******************");
            String containerLogs = container.getServerLogs();
            log.info(containerLogs);
            // pg cdc logs contain ERROR
            // Assertions.assertFalse(containerLogs.contains("ERROR"));
            log.info("****************** container logs end ******************");
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_1);
            clearTable(OPENGAUSS_SCHEMA, SINK_TABLE_1);
            clearTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_2);
            clearTable(OPENGAUSS_SCHEMA, SINK_TABLE_2);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testAddFiledWithRestore(TestContainer container)
            throws IOException, InterruptedException {
        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return container.executeJob(
                                    "/opengausscdc_to_opengauss_test_add_Filed.conf");
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
                                                                            OPENGAUSS_SCHEMA,
                                                                            SOURCE_TABLE_3)),
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SINK_TABLE_3)))));

            Pattern jobIdPattern =
                    Pattern.compile(
                            ".*Init JobMaster for Job opengausscdc_to_opengauss_test_add_Filed.conf \\(([0-9]*)\\).*",
                            Pattern.DOTALL);
            Matcher matcher = jobIdPattern.matcher(container.getServerLogs());
            String jobId;
            if (matcher.matches()) {
                jobId = matcher.group(1);
            } else {
                throw new RuntimeException("Can not find jobId");
            }

            Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());

            // add filed add insert source table data
            addFieldsForTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_3);
            addFieldsForTable(OPENGAUSS_SCHEMA, SINK_TABLE_3);
            insertSourceTableForAddFields(OPENGAUSS_SCHEMA, SOURCE_TABLE_3);

            // Restore job
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.restoreJob(
                                    "/opengausscdc_to_opengauss_test_add_Filed.conf", jobId);
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
                                                                            OPENGAUSS_SCHEMA,
                                                                            SOURCE_TABLE_3)),
                                                            query(
                                                                    getQuerySQL(
                                                                            OPENGAUSS_SCHEMA,
                                                                            SINK_TABLE_3)))));
        } finally {
            // Clear related content to ensure that multiple operations are not affected
            clearTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_3);
            clearTable(OPENGAUSS_SCHEMA, SINK_TABLE_3);
        }
    }

    @TestTemplate
    public void testOpengaussCdcCheckDataWithNoPrimaryKey(TestContainer container)
            throws Exception {

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/opengausscdc_to_opengauss_with_no_primary_key.conf");
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
                                                        OPENGAUSS_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(OPENGAUSS_SCHEMA, SINK_TABLE_1)));
                            });

            // insert update delete
            upsertDeleteSourceTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(
                                                getQuerySQL(
                                                        OPENGAUSS_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(OPENGAUSS_SCHEMA, SINK_TABLE_1)));
                            });
        } finally {
            clearTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);
            clearTable(OPENGAUSS_SCHEMA, SINK_TABLE_1);
        }
    }

    @TestTemplate
    public void testOpengaussCdcCheckDataWithCustomPrimaryKey(TestContainer container)
            throws Exception {

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.executeJob(
                                    "/opengausscdc_to_opengauss_with_custom_primary_key.conf");
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
                                                        OPENGAUSS_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(OPENGAUSS_SCHEMA, SINK_TABLE_1)));
                            });

            // insert update delete
            upsertDeleteSourceTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);

            // stream stage
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertIterableEquals(
                                        query(
                                                getQuerySQL(
                                                        OPENGAUSS_SCHEMA,
                                                        SOURCE_TABLE_NO_PRIMARY_KEY)),
                                        query(getQuerySQL(OPENGAUSS_SCHEMA, SINK_TABLE_1)));
                            });
        } finally {
            clearTable(OPENGAUSS_SCHEMA, SOURCE_TABLE_NO_PRIMARY_KEY);
            clearTable(OPENGAUSS_SCHEMA, SINK_TABLE_1);
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

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }

    private void upsertDeleteSourceTable(String database, String tableName) {

        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES (2, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,\n"
                        + "        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',\n"
                        + "        '2020-07-17', '18:00:22', 500);");

        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES (3, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,\n"
                        + "        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',\n"
                        + "        '2020-07-17', '18:00:22', 500);");

        executeSql("DELETE FROM " + database + "." + tableName + " where id = 2;");

        executeSql("UPDATE " + database + "." + tableName + " SET f_big = 10000 where id = 3;");
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection(OPENGAUSSQL_DATABASE);
                Statement statement = connection.createStatement()) {
            statement.execute("SET search_path TO inventory;");
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getQuerySQL(String database, String tableName) {
        return String.format(SOURCE_SQL_TEMPLATE, database, tableName);
    }

    private List<List<Object>> query(String sql) {
        try (Connection connection = getJdbcConnection(OPENGAUSSQL_DATABASE)) {
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
                        String.format(
                                "Print opengauss-CDC query, sql: %s, data: %s", sql, objects));
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void createNewUserForJdbcSink() throws Exception {
        try (Connection connection = getJdbcConnection(OPENGAUSSQL_DATABASE);
                Statement stmt = connection.createStatement()) {
            // create a user for jdbc sink
            stmt.execute("CREATE USER dailai WITH PASSWORD 'openGauss@123';");
            stmt.execute("GRANT ALL PRIVILEGES  TO dailai;");
        }
    }

    protected void reloadConf() throws Exception {
        try (Connection connection = getJdbcConnection(OPENGAUSSQL_DATABASE);
                Statement stmt = connection.createStatement()) {
            stmt.execute("select pg_reload_conf();");
        }
    }

    protected void initializeOpengaussSql() throws Exception {
        try (Connection connection = getJdbcConnection(OPENGAUSSQL_DEFAULT_DATABASE);
                Statement stmt = connection.createStatement()) {
            stmt.execute("create database " + OPENGAUSSQL_DATABASE);
        }
        final String ddlFile = String.format("ddl/%s.sql", "inventory");
        final URL ddlTestFile = OpengaussCDCIT.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection(OPENGAUSSQL_DATABASE);
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

    private Connection getJdbcConnection(String dbName) throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://"
                        + OPENGAUSS_CONTAINER.getHost()
                        + ":"
                        + OPENGAUSS_CONTAINER.getMappedPort(OPENGAUSS_PORT)
                        + "/"
                        + dbName,
                USERNAME,
                PASSWORD);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (OPENGAUSS_CONTAINER != null) {
            OPENGAUSS_CONTAINER.close();
        }
    }
}
