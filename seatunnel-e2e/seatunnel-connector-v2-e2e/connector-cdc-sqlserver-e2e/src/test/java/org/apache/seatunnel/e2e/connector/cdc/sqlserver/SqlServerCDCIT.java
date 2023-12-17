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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

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
        disabledReason = "Currently SPARK and FLINK do not support cdc")
public class SqlServerCDCIT extends TestSuiteBase implements TestResource {

    private static final String HOST = "sqlserver-host";

    private static final int PORT = 1433;

    private static final String STATEMENTS_PLACEHOLDER = "#";

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private static final String DISABLE_DB_CDC =
            "IF EXISTS(select 1 from sys.databases where name='#' AND is_cdc_enabled=1)\n"
                    + "EXEC sys.sp_cdc_disable_db";

    private static final String SOURCE_SQL =
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
                    + "  CONVERT(varchar(100), val_varbinary) as val_varbinary\n"
                    + "from column_type_test.dbo.full_types";
    private static final String SINK_SQL =
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
                    + "  CONVERT(varchar(100), val_varbinary) as val_varbinary\n"
                    + "from column_type_test.dbo.full_types_sink";

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
        initializeSqlServerTable("column_type_test");

        CompletableFuture<Void> executeJobFuture =
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
                                    querySql(SOURCE_SQL), querySql(SINK_SQL));
                        });

        // insert update delete
        updateSourceTable();

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(SOURCE_SQL), querySql(SINK_SQL));
                        });
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    private void initializeSqlServerTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = TestSuiteBase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertNotNull(ddlTestFile, "Cannot locate " + ddlFile);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            dropTestDatabase(connection, sqlFile);
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
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void updateSourceTable() {
        executeSql(
                "INSERT INTO column_type_test.dbo.full_types VALUES (3,\n"
                        + "                               'cč3', 'vcč', 'tč', N'cč', N'vcč', N'tč',\n"
                        + "                               1.123, 2, 3.323, 4.323, 5.323, 6.323,\n"
                        + "                               1, 22, 333, 4444, 55555,\n"
                        + "                               '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',\n"
                        + "                               '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)));");
        executeSql(
                "INSERT INTO column_type_test.dbo.full_types VALUES (4,\n"
                        + "                               'cč4', 'vcč', 'tč', N'cč', N'vcč', N'tč',\n"
                        + "                               1.123, 2, 3.323, 4.323, 5.323, 6.323,\n"
                        + "                               1, 22, 333, 4444, 55555,\n"
                        + "                               '2018-07-13', '10:23:45', '2018-07-13 11:23:45.34', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45',\n"
                        + "                               '<a>b</a>',SYSDATETIMEOFFSET(),CAST('test_varbinary' AS varbinary(100)));");

        executeSql("DELETE FROM column_type_test.dbo.full_types where id = 2");

        executeSql(
                "UPDATE column_type_test.dbo.full_types SET val_varchar = 'newvcč' where id = 1");
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MSSQL_SERVER_CONTAINER.getJdbcUrl(),
                MSSQL_SERVER_CONTAINER.getUsername(),
                MSSQL_SERVER_CONTAINER.getPassword());
    }

    private List<List<Object>> querySql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
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
}
