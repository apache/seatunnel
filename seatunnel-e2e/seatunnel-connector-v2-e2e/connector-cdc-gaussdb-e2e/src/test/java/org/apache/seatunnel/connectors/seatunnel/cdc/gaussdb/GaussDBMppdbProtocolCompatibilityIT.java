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
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
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
import static org.awaitility.Awaitility.given;

/** Protocol-compatibility E2E for the GaussDB mppdb reader against openGauss. */
@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class GaussDBMppdbProtocolCompatibilityIT extends TestSuiteBase implements TestResource {

    /** PostgreSQL-compatible database port exposed by the openGauss image. */
    private static final int OPENGAUSS_PORT = 5432;

    /** Pattern used to remove trailing SQL comments from the DDL fixture. */
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    /** Administrative account created by the openGauss image. */
    private static final String USERNAME = "gaussdb";

    /** Test-only password required by the openGauss image policy. */
    private static final String PASSWORD = "openGauss@123";

    /** Database containing the source and sink tables. */
    private static final String GAUSSDB_DATABASE = "gaussdb_cdc";

    /** Bootstrap database created by the openGauss image. */
    private static final String DEFAULT_DATABASE = "postgres";

    /** Schema containing the E2E tables. */
    private static final String GAUSSDB_SCHEMA = "inventory";

    /** Table captured by GaussDB CDC. */
    private static final String SOURCE_TABLE = "gaussdb_cdc_table";

    /** JDBC sink table used to verify emitted changes. */
    private static final String SINK_TABLE = "sink_gaussdb_cdc_table";

    /** Network alias visible from the SeaTunnel test container. */
    private static final String OPENGAUSS_HOST = "opengauss_mppdb_compatibility_e2e";

    /** Connector plugin directory receiving the PostgreSQL-compatible JDBC driver. */
    private static final String GAUSSDB_CDC_PLUGIN_LIB = "/tmp/seatunnel/plugins/GaussDB-CDC/lib";

    /** JDBC sink plugin directory receiving the test driver. */
    private static final String JDBC_PLUGIN_LIB = "/tmp/seatunnel/plugins/Jdbc/lib";

    /** Stable ordering query used for source and sink comparisons. */
    private static final String SOURCE_SQL_TEMPLATE = "select * from %s.%s order by id";

    /** Prefix for per-test logical replication slots. */
    private static final String GENERATED_SLOT_PREFIX = "seatunnel_gaussdb_";

    /** openGauss image supplying the mppdb_decoding plugin in CI. */
    protected static final DockerImageName OPENGAUSS_IMAGE =
            DockerImageName.parse("opengauss/opengauss:5.0.0")
                    .asCompatibleSubstituteFor("postgres");

    /** Shared database container used by all engine variants in this test class. */
    public static final GenericContainer<?> OPENGAUSS_CONTAINER =
            new GenericContainer<>(OPENGAUSS_IMAGE)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(OPENGAUSS_HOST)
                    .withExposedPorts(OPENGAUSS_PORT)
                    .withEnv("GS_PASSWORD", PASSWORD)
                    .withLogConsumer(new Slf4jLogConsumer(log));

    /** Copies JDBC drivers into both source and sink plugin directories. */
    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar.of(org.postgresql.Driver.class)
                        .copyTo(container, GAUSSDB_CDC_PLUGIN_LIB);
                DependencyJar.of(org.postgresql.Driver.class).copyTo(container, JDBC_PLUGIN_LIB);
            };

    /** Starts the database, creates fixtures, and enables replication authentication. */
    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("Starting openGauss mppdb_decoding compatibility container...");
        Startables.deepStart(Stream.of(OPENGAUSS_CONTAINER)).join();
        given().ignoreExceptions()
                .await()
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::assertDatabaseReady);
        initializeGaussDB();
        configureReplicationAuthentication();
    }

    /** Verifies snapshot rows and subsequent INSERT, UPDATE, and DELETE events end to end. */
    @TestTemplate
    public void testMppdbProtocolCompatibilityE2e(TestContainer container) {
        String slotVariable = toSlotVariable(createSlotName());
        try {
            CompletableFuture<Container.ExecResult> jobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return container.executeJob(
                                            "/gaussdbcdc_to_opengauss_mppdb.conf",
                                            Collections.singletonList(slotVariable));
                                } catch (Exception e) {
                                    throw new IllegalStateException(
                                            "GaussDB CDC job execution failed", e);
                                }
                            });

            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                checkJobFailure(jobFuture);
                                Assertions.assertIterableEquals(
                                        query(getQuerySQL(GAUSSDB_SCHEMA, SOURCE_TABLE)),
                                        query(getQuerySQL(GAUSSDB_SCHEMA, SINK_TABLE)));
                            });

            upsertDeleteSourceTable(GAUSSDB_SCHEMA, SOURCE_TABLE);

            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                checkJobFailure(jobFuture);
                                Assertions.assertIterableEquals(
                                        query(getQuerySQL(GAUSSDB_SCHEMA, SOURCE_TABLE)),
                                        query(getQuerySQL(GAUSSDB_SCHEMA, SINK_TABLE)));
                            });
            checkJobFailure(jobFuture);
        } finally {
            clearTable(GAUSSDB_SCHEMA, SOURCE_TABLE);
            clearTable(GAUSSDB_SCHEMA, SINK_TABLE);
        }
    }

    /**
     * Propagates completed job failures on the test thread without joining an active streaming job.
     * Runtime exceptions escape Awaitility immediately instead of being retried as row assertions.
     */
    private static void checkJobFailure(CompletableFuture<Container.ExecResult> jobFuture) {
        if (!jobFuture.isDone()) {
            return;
        }
        Container.ExecResult result = jobFuture.join();
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                    "GaussDB CDC job exited with code "
                            + result.getExitCode()
                            + "; stderr: "
                            + result.getStderr()
                            + "; stdout: "
                            + result.getStdout());
        }
    }

    /** Generates a unique slot name so parallel engine runs cannot contend for one slot. */
    private String createSlotName() {
        return GENERATED_SLOT_PREFIX + Long.toHexString(JobIdGenerator.newJobId());
    }

    /** Converts a slot name into the SeaTunnel test variable syntax. */
    private String toSlotVariable(String slotName) {
        return "slot_name=" + slotName;
    }

    /** Creates the test database and executes the DDL fixture. */
    protected void initializeGaussDB() throws Exception {
        try (Connection connection = getJdbcConnection(DEFAULT_DATABASE);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + GAUSSDB_DATABASE);
        }
        final String ddlFile = "ddl/inventory.sql";
        final URL ddlTestFile =
                GaussDBMppdbProtocolCompatibilityIT.class.getClassLoader().getResource(ddlFile);
        Assertions.assertNotNull(ddlTestFile, "Cannot locate " + ddlFile);
        try (Connection connection = getJdbcConnection(GAUSSDB_DATABASE);
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

    /** Verifies the container accepts PostgreSQL-compatible JDBC connections. */
    private void assertDatabaseReady() throws SQLException {
        try (Connection connection = getJdbcConnection(DEFAULT_DATABASE);
                Statement statement = connection.createStatement()) {
            Assertions.assertTrue(statement.execute("SELECT 1"));
        }
    }

    /** Applies INSERT, UPDATE, and DELETE changes to the captured table. */
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

    /** Truncates one E2E table between engine variants. */
    private void clearTable(String schema, String tableName) {
        executeSql("truncate table " + schema + "." + tableName);
    }

    /** Executes one committed DML statement in the test database. */
    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection(GAUSSDB_DATABASE);
                Statement statement = connection.createStatement()) {
            statement.execute("SET search_path TO " + GAUSSDB_SCHEMA);
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** Builds a deterministically ordered table query. */
    private String getQuerySQL(String schema, String tableName) {
        return String.format(SOURCE_SQL_TEMPLATE, schema, tableName);
    }

    /** Reads all rows for strict source-to-sink comparisons. */
    private List<List<Object>> query(String sql) {
        try (Connection connection = getJdbcConnection(GAUSSDB_DATABASE)) {
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

    /** Enables replication authentication compatible with the PostgreSQL JDBC driver. */
    private void configureReplicationAuthentication() throws Exception {
        Container.ExecResult passwordEncryption =
                OPENGAUSS_CONTAINER.execInContainer(
                        "/bin/sh",
                        "-c",
                        "sed -i 's/^#password_encryption_type = 2/password_encryption_type = 1/' /var/lib/opengauss/data/postgresql.conf");
        Assertions.assertEquals(0, passwordEncryption.getExitCode());
        Container.ExecResult replicationAuthentication =
                OPENGAUSS_CONTAINER.execInContainer(
                        "/bin/sh",
                        "-c",
                        "sed -i 's/host replication gaussdb 0.0.0.0\\/0 md5/host replication gaussdb 0.0.0.0\\/0 sha256/' /var/lib/opengauss/data/pg_hba.conf");
        Assertions.assertEquals(0, replicationAuthentication.getExitCode());
        try (Connection connection = getJdbcConnection(GAUSSDB_DATABASE);
                Statement statement = connection.createStatement()) {
            statement.execute("SELECT pg_reload_conf()");
        }
    }

    /** Opens a PostgreSQL-compatible JDBC connection to the requested database. */
    private Connection getJdbcConnection(String database) throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://"
                        + OPENGAUSS_CONTAINER.getHost()
                        + ":"
                        + OPENGAUSS_CONTAINER.getMappedPort(OPENGAUSS_PORT)
                        + "/"
                        + database,
                USERNAME,
                PASSWORD);
    }

    /** Stops the shared database container after all engine variants finish. */
    @AfterAll
    @Override
    public void tearDown() {
        if (OPENGAUSS_CONTAINER != null) {
            OPENGAUSS_CONTAINER.close();
        }
    }
}
