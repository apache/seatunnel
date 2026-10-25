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

package org.apache.seatunnel.connectors.jdbc;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;

import static org.awaitility.Awaitility.await;

@Slf4j
public class SqlServerSchemaChangeIT extends AbstractSchemaChangeBaseIT {

    private static final String DATABASE_TYPE = "SqlServer";
    private static final String SQLSERVER_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest";
    private static final String SQLSERVER_CONTAINER_HOST = "sqlserver";
    private static final String SQLSERVER_DATABASE = "master";
    private static final String SQLSERVER_SCHEMA = "dbo";
    private static final String SQLSERVER_USER = "sa";
    private static final String ACCEPT_EULA = "ACCEPT_EULA";
    private static final String Y = "Y";
    /**
     * Uses the supported SQL Server container password variable instead of deprecated SA_PASSWORD.
     */
    private static final String MSSQL_SA_PASSWORD = "MSSQL_SA_PASSWORD";

    private static final String SQLSERVER_PASSWORD = "paanssy1234$";
    private static final int SQLSERVER_PORT = 1433;
    /** Exposes the RPC endpoint mapper required by SQL Server MSDTC inside containers. */
    private static final int SQLSERVER_RPC_PORT = 135;
    /** Exposes the MSDTC listener required by XA transactions in containerized SQL Server. */
    private static final int SQLSERVER_DTC_PORT = 51000;
    /** Executes real SQL Server login probes instead of relying on container logs alone. */
    private static final String SQLSERVER_COMMAND = "/opt/mssql-tools18/bin/sqlcmd";
    /** Gives SQL Server enough time to finish startup tasks before login and XA checks begin. */
    private static final Duration SQLSERVER_READY_TIMEOUT = Duration.ofMinutes(2);
    /** Exercises an authenticated query to prove the server is actually accepting connections. */
    private static final String SQLSERVER_READY_QUERY = "SET NOCOUNT ON; SELECT 1";
    /**
     * SQL Server 2022 can emit the client-ready log before recovery and `msdb` upgrades finish in
     * CI, so XA installation must wait for the stronger recovery-complete signal.
     */
    private static final String SQLSERVER_RECOVERY_COMPLETE_LOG = ".*Recovery is complete\\..*";
    /**
     * Verifies that the XA initialization procedure is visible before the exactly-once test runs.
     */
    private static final String SQLSERVER_XA_PROCEDURE_QUERY =
            "SET NOCOUNT ON; SELECT CASE WHEN OBJECT_ID('master..xp_sqljdbc_xa_init_ex') IS NOT NULL THEN 1 ELSE 0 END";
    /**
     * Newer SQL Server Linux containers do not always expose the external-user toggle. Guard the
     * setup statement so the XA bootstrap path stays compatible across image variants.
     */
    private static final String SQLSERVER_ENABLE_EXTERNAL_USER_IF_SUPPORTED =
            "IF EXISTS (SELECT 1 FROM sys.configurations WHERE name = 'external user enabled') "
                    + "BEGIN EXEC sp_configure 'external user enabled', 1; RECONFIGURE; END";

    private final String SQLSERVER_JDBC_URL =
            "jdbc:sqlserver://%s:%s;databaseName=%s;"
                    + "useBulkCopyForBatchInsert=true;delayLoadingLobs=true;useFmtOnly=false;"
                    + "integratedSecurity=false;xaTransactionCompatible=true;"
                    + "encrypt=false;trustServerCertificate=true;";
    private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private final String schemaEvolutionCaseConfig =
            "/mysqlcdc_to_sqlserver_with_schema_change.conf";
    private final String schemaEvolutionCaseExactlyOnceConfig =
            "/mysqlcdc_to_sqlserver_with_schema_change_exactly_once.conf";
    private final String QUERY_COLUMNS =
            "SELECT REPLACE(REPLACE(COLUMN_NAME, '[', ''), ']', '') COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER BY COLUMN_NAME";

    @Override
    protected SchemaChangeCase getSchemaChangeCase() {
        return SchemaChangeCase.builder()
                .jdbcUrl(SQLSERVER_JDBC_URL)
                .username(SQLSERVER_USER)
                .password(SQLSERVER_PASSWORD)
                .port(SQLSERVER_PORT)
                .driverClassName(DRIVER_CLASS)
                .databaseName(SQLSERVER_DATABASE)
                .schemaName(SQLSERVER_SCHEMA)
                .schemaEvolutionCase(schemaEvolutionCaseConfig)
                .schemaEvolutionCaseExactlyOnce(schemaEvolutionCaseExactlyOnceConfig)
                .sinkTable1(SINK_TABLE1)
                .sinkTable2(SINK_TABLE2)
                .sinkQueryColumns(QUERY_COLUMNS)
                .openExactlyOnce(true)
                .build();
    }

    @Override
    protected GenericContainer initSinkContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(SQLSERVER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(SQLSERVER_CONTAINER_HOST)
                        .withEnv(ACCEPT_EULA, Y)
                        .withEnv(MSSQL_SA_PASSWORD, SQLSERVER_PASSWORD)
                        .withEnv("MSSQL_AGENT_ENABLED", "true")
                        // MSDTC ports are required for distributed XA transactions in SQL Server
                        // Linux containers.
                        .withEnv("MSSQL_RPC_PORT", String.valueOf(SQLSERVER_RPC_PORT))
                        .withEnv("MSSQL_DTC_TCP_PORT", String.valueOf(SQLSERVER_DTC_PORT))
                        .withExposedPorts(SQLSERVER_PORT)
                        .waitingFor(Wait.forLogMessage(SQLSERVER_RECOVERY_COMPLETE_LOG, 1))
                        .withStartupTimeout(Duration.ofMinutes(10))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(SQLSERVER_IMAGE)));

        container.start();
        try {
            waitForSqlServerLogin(container);
            // Prepare the SQL Server instance before installing the XA procedures that the
            // exactly-once connector path depends on.
            assertSqlCommandSucceeded(
                    container,
                    "EXEC sp_configure 'show advanced options', 1; RECONFIGURE;",
                    "enable SQL Server advanced options");
            // Some SQL Server variants still expose this switch, while the current Linux image no
            // longer does. Keep the bootstrap compatible by enabling it only when the option
            // exists.
            assertSqlCommandSucceeded(
                    container,
                    SQLSERVER_ENABLE_EXTERNAL_USER_IF_SUPPORTED,
                    "enable external user access");

            log.info("Installing stored procedures sp_sqljdbc_xa_install.");
            assertSqlCommandSucceeded(
                    container,
                    "IF NOT EXISTS (SELECT * FROM sys.objects WHERE name = 'xp_sqljdbc_xa_init_ex') "
                            + "EXEC sp_sqljdbc_xa_install",
                    "install SQL Server XA stored procedures");
            waitForXaStoredProcedure(container);
        } catch (IOException | InterruptedException e) {
            log.error("XA procedure installation failed: ", e);
            throw new RuntimeException(e);
        }
        return container;
    }

    @Override
    protected String sinkDatabaseType() {
        return DATABASE_TYPE;
    }

    /**
     * Recovery-complete is the earliest safe container log marker, and a real authenticated probe
     * confirms that `sqlcmd` can connect before XA setup starts.
     */
    private void waitForSqlServerLogin(GenericContainer<?> container) {
        await().pollDelay(Duration.ofSeconds(1))
                .pollInterval(Duration.ofSeconds(2))
                .atMost(SQLSERVER_READY_TIMEOUT)
                .until(() -> isSqlCommandSuccessful(container, SQLSERVER_READY_QUERY));
    }

    /**
     * XA setup must complete before the exactly-once test starts, otherwise the job fails later
     * with a missing xp_sqljdbc_xa_init_ex procedure.
     */
    private void waitForXaStoredProcedure(GenericContainer<?> container) {
        await().pollDelay(Duration.ofSeconds(1))
                .pollInterval(Duration.ofSeconds(2))
                .atMost(SQLSERVER_READY_TIMEOUT)
                .until(
                        () ->
                                sqlCommandOutputContains(
                                        container, SQLSERVER_XA_PROCEDURE_QUERY, "1"));
    }

    /**
     * Fails fast when a setup statement is rejected so the test does not continue on false success.
     */
    private void assertSqlCommandSucceeded(
            GenericContainer<?> container, String sql, String description)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSqlCommand(container, sql);
        if (execResult.getExitCode() != 0) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to %s, exitCode=%s, stdout=%s, stderr=%s",
                            description,
                            execResult.getExitCode(),
                            execResult.getStdout(),
                            execResult.getStderr()));
        }
    }

    /** Returns true only when sqlcmd completes successfully with the supplied statement. */
    private boolean isSqlCommandSuccessful(GenericContainer<?> container, String sql) {
        try {
            return executeSqlCommand(container, sql).getExitCode() == 0;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }

    /** Checks the normalized sqlcmd output for the expected probe result. */
    private boolean sqlCommandOutputContains(
            GenericContainer<?> container, String sql, String expected) {
        try {
            Container.ExecResult execResult = executeSqlCommand(container, sql);
            return execResult.getExitCode() == 0
                    && execResult.getStdout().replaceAll("\\s+", "").equals(expected);
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }

    /** Executes sqlcmd with consistent flags so login, setup, and probe checks share one path. */
    private Container.ExecResult executeSqlCommand(GenericContainer<?> container, String sql)
            throws IOException, InterruptedException {
        return container.execInContainer(
                SQLSERVER_COMMAND,
                "-S",
                "localhost",
                "-U",
                SQLSERVER_USER,
                "-P",
                SQLSERVER_PASSWORD,
                "-Q",
                sql,
                "-b",
                "-h",
                "-1",
                "-W",
                "-C");
    }
}
