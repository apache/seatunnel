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

package org.apache.seatunnel.e2e.connector.cdc.sqlserver;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.config.SqlServerSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.SqlServerDialect;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JdbcUtil;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class SqlServerCDCIT extends TestSuiteBase implements TestResource {

    private static final String HOST = "sqlserver-host";

    private static final int PORT = 1433;

    private static final String STATEMENTS_PLACEHOLDER = "#";

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    public static final String DATABASE_NAME = "column_type_test";
    public static final String SCHEMA_NAME = "dbo";
    public static final String SCHEMA_EVOLUTION_DATABASE_NAME = "schema_change_test";
    private static final String SCHEMA_EVOLUTION_SOURCE_TABLE = "products";
    private static final String SCHEMA_EVOLUTION_SINK_TABLE = "products_sink";
    private static final int INCREMENTAL_MARKER_ID = 1000;

    private static final String DISABLE_DB_CDC =
            "IF EXISTS(select 1 from sys.databases where name='#' AND is_cdc_enabled=1)\n"
                    + "EXEC sys.sp_cdc_disable_db";
    private static final String SOURCE_TABLE =
            DATABASE_NAME + "." + SCHEMA_NAME + "." + "full_types";
    // Additional source table used to verify multi-table CDC capture in one job.
    private static final String SOURCE_TABLE_2 =
            DATABASE_NAME + "." + SCHEMA_NAME + "." + "full_types_2";
    private static final String SOURCE_TABLE_NO_PRIMARY_KEY =
            DATABASE_NAME + "." + SCHEMA_NAME + "." + "full_types_no_primary_key";
    private static final String SOURCE_TABLE_CUSTOM_PRIMARY_KEY =
            DATABASE_NAME + "." + SCHEMA_NAME + "." + "full_types_custom_primary_key";
    private static final String SINK_TABLE =
            DATABASE_NAME + "." + SCHEMA_NAME + "." + "full_types_sink";
    // Sink tables are derived from the source names with the configured sink_ prefix.
    private static final String MULTI_TABLE_SINK_1 =
            DATABASE_NAME + "." + SCHEMA_NAME + "." + "sink_full_types";
    private static final String MULTI_TABLE_SINK_2 =
            DATABASE_NAME + "." + SCHEMA_NAME + "." + "sink_full_types_2";

    private static final String SELECT_SOURCE_SQL =
            "select\n"
                    + "  id,\n"
                    + "  val_char,\n"
                    + "  val_varchar,\n"
                    + "  val_text,\n"
                    + "  val_nchar,\n"
                    + "  val_nvarchar,\n"
                    + "  val_ntext,\n"
                    + "  val_decimal,\n"
                    + "  val_numeric,\n"
                    + "  val_float,\n"
                    + "  val_real,\n"
                    + "  val_smallmoney,\n"
                    + "  val_money,\n"
                    + "  val_bit,\n"
                    + "  val_tinyint,\n"
                    + "  val_smallint,\n"
                    + "  val_int,\n"
                    + "  val_bigint,\n"
                    + "  val_date,\n"
                    + "  val_time,\n"
                    + "  val_datetime2,\n"
                    + "  val_datetime,\n"
                    + "  val_smalldatetime,\n"
                    + "  val_xml,\n"
                    + "  val_datetimeoffset,\n"
                    + "  CONVERT(varchar(100), val_varbinary) as val_varbinary,\n"
                    + "  val_udtdecimal\n"
                    + "from %s order by id asc";
    private static final String SELECT_SINK_SQL =
            "select\n"
                    + "  id,\n"
                    + "  val_char,\n"
                    + "  val_varchar,\n"
                    + "  val_text,\n"
                    + "  val_nchar,\n"
                    + "  val_nvarchar,\n"
                    + "  val_ntext,\n"
                    + "  val_decimal,\n"
                    + "  val_numeric,\n"
                    + "  val_float,\n"
                    + "  val_real,\n"
                    + "  val_smallmoney,\n"
                    + "  val_money,\n"
                    + "  val_bit,\n"
                    + "  val_tinyint,\n"
                    + "  val_smallint,\n"
                    + "  val_int,\n"
                    + "  val_bigint,\n"
                    + "  val_date,\n"
                    + "  val_time,\n"
                    + "  val_datetime2,\n"
                    + "  val_datetime,\n"
                    + "  val_smalldatetime,\n"
                    + "  val_xml,\n"
                    + "  val_datetimeoffset,\n"
                    + "  CONVERT(varchar(100), val_varbinary) as val_varbinary,\n"
                    + "  val_udtdecimal\n"
                    + "from %s order by id asc";
    private static final String SELECT_SCHEMA_EVOLUTION_DATA_SQL =
            "SELECT * FROM %s ORDER BY id ASC";
    private static final String SELECT_SCHEMA_EVOLUTION_COLUMNS_SQL =
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
                    + "COALESCE(CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR(20)), 'null'), "
                    + "COALESCE(CAST(NUMERIC_PRECISION AS VARCHAR(20)), 'null'), "
                    + "COALESCE(CAST(NUMERIC_SCALE AS VARCHAR(20)), 'null') "
                    + "FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_CATALOG = '%s' AND TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' "
                    + "ORDER BY ORDINAL_POSITION";

    public static final MSSQLServerContainer MSSQL_SERVER_CONTAINER =
            new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2019-latest")
                    .withPassword("Password!")
                    .withEnv("MSSQL_AGENT_ENABLED", "true")
                    .withEnv("MSSQL_PID", "Standard")
                    .withNetwork(NETWORK)
                    .withNetworkAliases(HOST)
                    .withLogConsumer(
                            new Slf4jLogConsumer(
                                    DockerLoggerFactory.getLogger("sqlserver-docker-image")));

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.4.1.jre8/mssql-jdbc-9.4.1.jre8.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/SqlServer-CDC/lib && cd /tmp/seatunnel/plugins/SqlServer-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        MSSQL_SERVER_CONTAINER.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", PORT, PORT)));
        log.info("Starting containers...");
        Startables.deepStart(Stream.of(MSSQL_SERVER_CONTAINER)).join();
        log.info("Containers are started.");
    }

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        log.info("Stopping containers...");
        if (MSSQL_SERVER_CONTAINER != null) {
            MSSQL_SERVER_CONTAINER.stop();
        }
        log.info("Containers are stopped.");
    }

    @TestTemplate
    public void test(TestContainer container) throws IOException, InterruptedException {
        initializeSqlServerTable(DATABASE_NAME);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/sqlservercdc_to_console.conf");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SELECT_SOURCE_SQL, SOURCE_TABLE),
                                    querySql(SELECT_SINK_SQL, SINK_TABLE));
                        });

        // insert update delete
        updateSourceTable(SOURCE_TABLE);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SELECT_SOURCE_SQL, SOURCE_TABLE),
                                    querySql(SELECT_SINK_SQL, SINK_TABLE));
                        });
    }

    /**
     * Verifies that a single SqlServer CDC source can capture multiple tables and route them to
     * different sink tables in the same database.
     *
     * <p>The sink tables are pre-created so this regression stays focused on multi-table routing
     * instead of SQL Server auto-create type derivation while still exercising Jdbc table-mode
     * writes.
     */
    @TestTemplate
    public void testSqlServerCdcMultiTableE2e(TestContainer container) {
        initializeSqlServerTable(DATABASE_NAME);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/sqlservercdc_to_sqlserver_with_multi_table_mode_two_table.conf");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(SELECT_SOURCE_SQL, SOURCE_TABLE),
                                                        querySql(
                                                                SELECT_SINK_SQL,
                                                                MULTI_TABLE_SINK_1)),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(SELECT_SOURCE_SQL, SOURCE_TABLE_2),
                                                        querySql(
                                                                SELECT_SINK_SQL,
                                                                MULTI_TABLE_SINK_2))));

        updateSourceTable(SOURCE_TABLE);
        updateSourceTable(SOURCE_TABLE_2);

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(SELECT_SOURCE_SQL, SOURCE_TABLE),
                                                        querySql(
                                                                SELECT_SINK_SQL,
                                                                MULTI_TABLE_SINK_1)),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(SELECT_SOURCE_SQL, SOURCE_TABLE_2),
                                                        querySql(
                                                                SELECT_SINK_SQL,
                                                                MULTI_TABLE_SINK_2))));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "Heartbeat action query is currently only supported by the zeta engine.")
    public void testWithHeartbeat(TestContainer container) {
        initializeSqlServerTable(DATABASE_NAME);

        String createHeartbeatTable =
                "IF OBJECT_ID('"
                        + DATABASE_NAME
                        + "."
                        + SCHEMA_NAME
                        + ".heartbeat', 'U') IS NULL\n"
                        + "BEGIN\n"
                        + "    CREATE TABLE "
                        + DATABASE_NAME
                        + "."
                        + SCHEMA_NAME
                        + ".heartbeat (\n"
                        + "        ts DATETIME DEFAULT GETDATE()\n"
                        + "    );\n"
                        + "END";

        executeSql(createHeartbeatTable);
        executeSql("TRUNCATE TABLE " + DATABASE_NAME + "." + SCHEMA_NAME + ".heartbeat;");

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/sqlservercdc_to_console_with_heartbeat.conf");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SELECT_SOURCE_SQL, SOURCE_TABLE),
                                    querySql(SELECT_SINK_SQL, SINK_TABLE));
                        });

        // insert update delete
        updateSourceTable(SOURCE_TABLE);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SELECT_SOURCE_SQL, SOURCE_TABLE),
                                    querySql(SELECT_SINK_SQL, SINK_TABLE));
                        });

        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<List<Object>> query =
                                    querySql(
                                            "SELECT * FROM "
                                                    + DATABASE_NAME
                                                    + "."
                                                    + SCHEMA_NAME
                                                    + ".heartbeat");
                            Assertions.assertFalse(query.isEmpty());
                        });
    }

    @TestTemplate
    public void testCDCWithNoPrimaryKey(TestContainer container) {
        initializeSqlServerTable(DATABASE_NAME);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/sqlservercdc_to_sqlserver_with_no_primary_key.conf");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SELECT_SOURCE_SQL, SOURCE_TABLE_NO_PRIMARY_KEY),
                                    querySql(SELECT_SINK_SQL, SINK_TABLE));
                        });

        // insert update delete
        updateSourceTable(SOURCE_TABLE_NO_PRIMARY_KEY);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SELECT_SOURCE_SQL, SOURCE_TABLE_NO_PRIMARY_KEY),
                                    querySql(SELECT_SINK_SQL, SINK_TABLE));
                        });
    }

    @TestTemplate
    public void testCDCWithCustomPrimaryKey(TestContainer container) {
        initializeSqlServerTable(DATABASE_NAME);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/sqlservercdc_to_sqlserver_with_custom_primary_key.conf");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SELECT_SOURCE_SQL, SOURCE_TABLE_CUSTOM_PRIMARY_KEY),
                                    querySql(SELECT_SINK_SQL, SINK_TABLE));
                        });

        // insert update delete
        updateSourceTable(SOURCE_TABLE_CUSTOM_PRIMARY_KEY);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SELECT_SOURCE_SQL, SOURCE_TABLE_CUSTOM_PRIMARY_KEY),
                                    querySql(SELECT_SINK_SQL, SINK_TABLE));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "This case checks SqlServer CDC earliest startup mode only on Zeta engine.")
    public void testEarliestStartupMode(TestContainer container) throws InterruptedException {
        initializeSqlServerTable(DATABASE_NAME);

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/sqlservercdc_earliest_to_sqlserver.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Execute earliest job exception: {}", e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // give the job some time to start
        TimeUnit.SECONDS.sleep(10);

        // verify job stays running (i.e. no fatal exception like ArrayIndexOutOfBounds from
        // Debezium)
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            String jobStatus = container.getJobStatus(String.valueOf(jobId));
                            Assertions.assertEquals("RUNNING", jobStatus);
                        });

        try {
            Container.ExecResult cancelJobResult = container.cancelJob(String.valueOf(jobId));
            Assertions.assertEquals(0, cancelJobResult.getExitCode(), cancelJobResult.getStderr());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "This case requires obtaining the task health status and manually canceling the canceled task, which is currently only supported by the zeta engine.")
    public void testSqlServerCDCMetadataTrans(TestContainer container) throws InterruptedException {
        initializeSqlServerTable(DATABASE_NAME);

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/sqlservercdc_to_metadata_trans.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
        TimeUnit.SECONDS.sleep(10);
        // insert update delete
        updateSourceTable(SOURCE_TABLE_CUSTOM_PRIMARY_KEY);
        TimeUnit.SECONDS.sleep(20);
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            String jobStatus = container.getJobStatus(String.valueOf(jobId));
                            Assertions.assertEquals("RUNNING", jobStatus);
                        });
        try {
            Container.ExecResult cancelJobResult = container.cancelJob(String.valueOf(jobId));
            Assertions.assertEquals(0, cancelJobResult.getExitCode(), cancelJobResult.getStderr());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDialectCheckDisabledCDCTable() throws SQLException {
        initializeSqlServerTable(DATABASE_NAME);
        JdbcSourceConfigFactory factory =
                new SqlServerSourceConfigFactory()
                        .hostname(MSSQL_SERVER_CONTAINER.getHost())
                        .port(PORT)
                        .username("sa")
                        .password("Password!")
                        .databaseList(DATABASE_NAME);
        SqlServerDialect dialect =
                new SqlServerDialect(
                        (SqlServerSourceConfigFactory) factory, Collections.emptyList());
        try (JdbcConnection connection = dialect.openJdbcConnection(factory.create(0))) {
            SeaTunnelException exception =
                    Assertions.assertThrows(
                            SeaTunnelException.class,
                            () ->
                                    dialect.checkAllTablesEnabledCapture(
                                            connection,
                                            Collections.singletonList(TableId.parse(SINK_TABLE))));
            Assertions.assertEquals(
                    "Table "
                            + DATABASE_NAME
                            + "."
                            + SCHEMA_NAME
                            + ".full_types_sink is not enabled for capture",
                    exception.getMessage());
        }
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    private void assertSchemaEvolutionTableStructureAndData(
            String databaseName, String sourceTable, String sinkTable) {
        String sourceTablePath = databaseName + "." + SCHEMA_NAME + "." + sourceTable;
        String sinkTablePath = databaseName + "." + SCHEMA_NAME + "." + sinkTable;
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        querySql(SELECT_SCHEMA_EVOLUTION_DATA_SQL, sourceTablePath),
                                        querySql(SELECT_SCHEMA_EVOLUTION_DATA_SQL, sinkTablePath)));
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        querySql(
                                                String.format(
                                                        SELECT_SCHEMA_EVOLUTION_COLUMNS_SQL,
                                                        databaseName,
                                                        SCHEMA_NAME,
                                                        sourceTable)),
                                        querySql(
                                                String.format(
                                                        SELECT_SCHEMA_EVOLUTION_COLUMNS_SQL,
                                                        databaseName,
                                                        SCHEMA_NAME,
                                                        sinkTable))));
    }

    private void executeSqlFile(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = TestSuiteBase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertNotNull(ddlTestFile, "Cannot locate " + ddlFile);
        try {
            List<String> statements =
                    parseStatements(Files.readAllLines(Paths.get(ddlTestFile.toURI())));
            String currentDatabase = null;
            for (String stmt : statements) {
                String trimmed = stmt.trim();
                if (trimmed.toUpperCase().startsWith("USE ")) {
                    currentDatabase = trimmed.substring(4).replaceAll(";\\s*$", "").trim();
                    continue;
                }
                executeWithDeadlockRetry(stmt, currentDatabase);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void executeWithDeadlockRetry(String sql, String database) {
        Awaitility.await(
                        "Executing: "
                                + sql.substring(0, Math.min(80, sql.length()))
                                        .replaceAll("\\s+", " "))
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .until(
                        () -> {
                            try (Connection connection = getJdbcConnection();
                                    Statement statement = connection.createStatement()) {
                                if (database != null) {
                                    statement.execute("USE " + database);
                                }
                                statement.execute(sql);
                                return true;
                            } catch (SQLException e) {
                                if (e.getMessage() != null
                                        && (e.getMessage().contains("deadlock")
                                                || e.getMessage().contains("Deadlock"))) {
                                    log.warn("Deadlock detected, will retry: {}", sql);
                                    return false;
                                }
                                throw new RuntimeException(e);
                            }
                        });
    }

    private void initializeSqlServerTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = TestSuiteBase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertNotNull(ddlTestFile, "Cannot locate " + ddlFile);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            List<String> ddlLines = Files.readAllLines(Paths.get(ddlTestFile.toURI()));
            String ddlContent = String.join("\n", ddlLines);
            String actualDatabaseName = extractDatabaseName(ddlContent);
            dropTestDatabase(connection, actualDatabaseName);
            final List<String> statements = parseStatements(ddlLines);
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> parseStatements(List<String> ddlLines) {
        return Arrays.stream(
                        ddlLines.stream()
                                .map(String::trim)
                                .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                .map(
                                        x -> {
                                            final Matcher m = COMMENT_PATTERN.matcher(x);
                                            return m.matches() ? m.group(1) : x;
                                        })
                                .collect(Collectors.joining("\n"))
                                .split(";"))
                .map(String::trim)
                .filter(x -> !x.isEmpty())
                .collect(Collectors.toList());
    }

    private String extractDatabaseName(String ddlContent) {
        Pattern createDbPattern =
                Pattern.compile(
                        "CREATE\\s+DATABASE\\s+\\[?([^\\s\\];]+)\\]?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = createDbPattern.matcher(ddlContent);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private void updateSourceTable(String table) {
        executeSql(
                "INSERT INTO "
                        + table
                        + " VALUES (3,\n"
                        + "                               'cč3', 'vcč', 'tč', N'cč', N'vcč', N'tč',\n"
                        + "                               1.123, 2, 3.323, 4.323, 5.323, 6.323,\n"
                        + "                               1, 22, 333, 4444, 55555,\n"
                        + "                               '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',\n"
                        + "                               '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32);");
        executeSql(
                "INSERT INTO "
                        + table
                        + " VALUES (4,\n"
                        + "                               'cč4', 'vcč', 'tč', N'cč', N'vcč', N'tč',\n"
                        + "                               1.123, 2, 3.323, 4.323, 5.323, 6.323,\n"
                        + "                               1, 22, 333, 4444, 55555,\n"
                        + "                               '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',\n"
                        + "                               '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32);");

        executeSql("DELETE FROM " + table + " where id = 2");

        executeSql("UPDATE " + table + " SET val_varchar = 'newvcč' where id = 1");
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MSSQL_SERVER_CONTAINER.getJdbcUrl(),
                MSSQL_SERVER_CONTAINER.getUsername(),
                MSSQL_SERVER_CONTAINER.getPassword());
    }

    private List<List<Object>> querySql(String sql, String table) {
        return querySql(String.format(sql, table));
    }

    private List<List<Object>> querySql(String sql) {
        return JdbcUtil.querySql(
                sql,
                () -> {
                    try {
                        return this.getJdbcConnection();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void dropTestDatabase(Connection connection, String databaseName)
            throws SQLException {
        try {
            Awaitility.await("Disabling CDC")
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    connection
                                            .createStatement()
                                            .execute(String.format("USE [%s]", databaseName));
                                } catch (SQLException e) {
                                    // if the database doesn't yet exist, there is no need to
                                    // disable CDC
                                    return true;
                                }
                                try {
                                    disableDbCdc(connection, databaseName);
                                    return true;
                                } catch (SQLException e) {
                                    return false;
                                }
                            });
        } catch (ConditionTimeoutException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to disable CDC on %s", databaseName), e);
        }

        connection.createStatement().execute("USE master");

        try {
            Awaitility.await(String.format("Dropping database %s", databaseName))
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    String sql =
                                            String.format(
                                                    "IF EXISTS(select 1 from sys.databases where name = '%s') DROP DATABASE [%s]",
                                                    databaseName, databaseName);
                                    connection.createStatement().execute(sql);
                                    return true;
                                } catch (SQLException e) {
                                    log.warn(
                                            String.format(
                                                    "DROP DATABASE %s failed (will be retried): {}",
                                                    databaseName),
                                            e.getMessage());
                                    try {
                                        connection
                                                .createStatement()
                                                .execute(
                                                        String.format(
                                                                "ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;",
                                                                databaseName));
                                    } catch (SQLException e2) {
                                        log.error("Failed to rollbackimmediately", e2);
                                    }
                                    return false;
                                }
                            });
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Failed to drop test database", e);
        }
    }

    /**
     * Disables CDC for a given database, if not already disabled.
     *
     * @param name the name of the DB, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    protected static void disableDbCdc(Connection connection, String name) throws SQLException {
        Objects.requireNonNull(name);
        connection.createStatement().execute(DISABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, name));
    }

    @TestTemplate
    public void testDatabaseNameWithSpecialCharacters(TestContainer container) {
        initializeSqlServerTable("test_db_name");

        CompletableFuture<Void> executeJobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                container.executeJob("/sqlservercdc_special_db_name.conf");
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        });

        String sourceTable = "[test-db-name].dbo.simple_table";
        String sinkTable = "[test-db-name].dbo.simple_table_sink";
        String selectSql = "select id, name, value from %s order by id asc";

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(selectSql, sourceTable),
                                    querySql(selectSql, sinkTable));
                        });

        executeSql("INSERT INTO [test-db-name].dbo.simple_table VALUES (4, 'test4', 400)");

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(selectSql, sourceTable),
                                    querySql(selectSql, sinkTable));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason =
                    "This case validates SqlServer CDC schema evolution on the Flink engine & zeta engine.")
    public void testWithSchemaEvolution(TestContainer container) {
        initializeSqlServerTable("schema_change_test");

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/sqlservercdc_to_sqlserver_with_schema_change.conf");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        assertSchemaEvolutionTableStructureAndData(
                SCHEMA_EVOLUTION_DATABASE_NAME,
                SCHEMA_EVOLUTION_SOURCE_TABLE,
                SCHEMA_EVOLUTION_SINK_TABLE);

        waitForSchemaEvolutionIncrementalStarted();

        executeSqlFile("sqlserver_schema_change_add_columns");
        assertSchemaEvolutionTableStructureAndData(
                SCHEMA_EVOLUTION_DATABASE_NAME,
                SCHEMA_EVOLUTION_SOURCE_TABLE,
                SCHEMA_EVOLUTION_SINK_TABLE);

        executeSqlFile("sqlserver_schema_change_drop_columns");
        assertSchemaEvolutionTableStructureAndData(
                SCHEMA_EVOLUTION_DATABASE_NAME,
                SCHEMA_EVOLUTION_SOURCE_TABLE,
                SCHEMA_EVOLUTION_SINK_TABLE);

        executeSqlFile("sqlserver_schema_change_rename_columns");
        assertSchemaEvolutionTableStructureAndData(
                SCHEMA_EVOLUTION_DATABASE_NAME,
                SCHEMA_EVOLUTION_SOURCE_TABLE,
                SCHEMA_EVOLUTION_SINK_TABLE);

        executeSqlFile("sqlserver_schema_change_modify_columns");
        assertSchemaEvolutionTableStructureAndData(
                SCHEMA_EVOLUTION_DATABASE_NAME,
                SCHEMA_EVOLUTION_SOURCE_TABLE,
                SCHEMA_EVOLUTION_SINK_TABLE);
    }

    private void waitForSchemaEvolutionIncrementalStarted() {
        String sourceTablePath =
                SCHEMA_EVOLUTION_DATABASE_NAME
                        + "."
                        + SCHEMA_NAME
                        + "."
                        + SCHEMA_EVOLUTION_SOURCE_TABLE;
        String sinkTablePath =
                SCHEMA_EVOLUTION_DATABASE_NAME
                        + "."
                        + SCHEMA_NAME
                        + "."
                        + SCHEMA_EVOLUTION_SINK_TABLE;
        executeSql(
                String.format(
                        "INSERT INTO %s VALUES (%d, 'incremental-marker', 'ensure-stream-phase', 9.9)",
                        sourceTablePath, INCREMENTAL_MARKER_ID));
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        querySql(
                                                        String.format(
                                                                "SELECT COUNT(1) FROM %s WHERE id = %d",
                                                                sourceTablePath,
                                                                INCREMENTAL_MARKER_ID))
                                                .get(0)
                                                .get(0),
                                        querySql(
                                                        String.format(
                                                                "SELECT COUNT(1) FROM %s WHERE id = %d",
                                                                sinkTablePath,
                                                                INCREMENTAL_MARKER_ID))
                                                .get(0)
                                                .get(0)));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testTimestampStartupMode(TestContainer container) throws InterruptedException {
        initializeSqlServerTable(DATABASE_NAME);
        executeSql("TRUNCATE TABLE " + DATABASE_NAME + "." + SCHEMA_NAME + ".full_types_sink;");

        // Use full fields insert to avoid implicit conversion error for varbinary columns with null
        // value
        executeSql(
                "INSERT INTO "
                        + SOURCE_TABLE_CUSTOM_PRIMARY_KEY
                        + " VALUES (1, 'cč1', 'vcč', 'tč', N'cč', N'vcč', N'tč', 1.123, 2, 3.323, 4.323, 5.323, 6.323, 1, 22, 333, 4444, 55555, '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45', '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32)");

        // sleep for a while to make sure the timestamp is different
        TimeUnit.SECONDS.sleep(5);
        long startTimestamp = System.currentTimeMillis();
        TimeUnit.SECONDS.sleep(5);

        executeSql(
                "INSERT INTO "
                        + SOURCE_TABLE_CUSTOM_PRIMARY_KEY
                        + " VALUES (2, 'cč2', 'vcč', 'tč', N'cč', N'vcč', N'tč', 1.123, 2, 3.323, 4.323, 5.323, 6.323, 1, 22, 333, 4444, 55555, '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45', '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)), 5.32)");

        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/sqlservercdc_to_sqlserver_timestamp.conf",
                                Arrays.asList("timestamp=" + startTimestamp));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        await().atMost(300000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<List<Object>> sinkRows =
                                    querySql(
                                            "SELECT id FROM "
                                                    + DATABASE_NAME
                                                    + "."
                                                    + SCHEMA_NAME
                                                    + ".full_types_sink ORDER BY id ASC");
                            Assertions.assertTrue(
                                    sinkRows.stream()
                                            .anyMatch(row -> row.get(0).toString().equals("2")));
                            Assertions.assertFalse(
                                    sinkRows.stream()
                                            .anyMatch(row -> row.get(0).toString().equals("1")));
                        });
    }
}
