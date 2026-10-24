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

package org.apache.seatunnel.e2e.connector.cdc.db2;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JdbcUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

/**
 * Verifies DB2 CDC snapshot and stream replay against a JDBC sink using a minimal synthetic ASNCDC
 * capture catalog.
 */
@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "DB2 CDC E2E runs on the SeaTunnel engine to avoid heavy DB2 and Flink containers competing for memory")
public class Db2CDCIT extends TestSuiteBase implements TestResource {

    private static final String HOST = "db2-cdc-e2e";
    private static final int PORT = 50000;
    private static final String DATABASE = "E2E";
    private static final String USERNAME = "db2inst1";
    private static final String PASSWORD = "123456";
    private static final String SCHEMA = "DB2INST1";
    private static final String CDC_SCHEMA = "ASNCDC";
    private static final String SOURCE_TABLE = SCHEMA + ".CUSTOMERS";
    private static final String SINK_TABLE = SCHEMA + ".CUSTOMERS_SINK";
    private static final String CAPTURE_TABLE = "DB2INST1_CUSTOMERS";
    private static final String CHANGE_TABLE = CDC_SCHEMA + "." + CAPTURE_TABLE;
    private static final String PRODUCT_TABLE = SCHEMA + ".PRODUCTS";
    private static final String PRODUCT_CAPTURE = "DB2INST1_PRODUCTS";
    private static final String PRODUCT_CHANGES = CDC_SCHEMA + "." + PRODUCT_CAPTURE;
    private static final String RESTORE_SINK = SCHEMA + ".SAVEPOINT_SINK";
    private static final String MULTI_TABLE_CONF = "/db2cdc_to_db2_with_savepoint.conf";
    private static final String ADD_TABLE_CONF = "/db2cdc_to_db2_add_table_after_savepoint.conf";
    private static final String SELECT_CUSTOMERS =
            "SELECT ID, NAME, DESCRIPTION FROM %s ORDER BY ID";
    private static final String SEATUNNEL_SERVER_JVM_OPTION =
            "-Xms256m -Xmx768m -XX:MaxMetaspaceSize=512m";
    private static final String SEATUNNEL_CLIENT_JVM_OPTION = "-Xms128m -Xmx256m";

    /**
     * Use the fixed Testcontainers-supported DB2 tag instead of unversioned `latest` so the E2E
     * does not drift as IBM updates the image.
     */
    private static final String DB2_IMAGE = "ibmcom/db2:11.5.0.0a";

    public static final Db2Container DB2_CONTAINER =
            new Db2Container(DB2_IMAGE)
                    .withDatabaseName(DATABASE)
                    .withUsername(USERNAME)
                    .withPassword(PASSWORD)
                    // IBM's DB2 image is currently published only for amd64, so Apple Silicon
                    // developers need an explicit platform override to run this E2E locally.
                    .withCreateContainerCmdModifier(cmd -> cmd.withPlatform("linux/amd64"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(HOST)
                    .withExposedPorts(PORT)
                    // Apple Silicon runs the amd64 DB2 image under emulation, which can take
                    // significantly longer than the default DB2Container wait budget.
                    .withStartupTimeout(Duration.ofMinutes(30))
                    .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DB2_IMAGE)))
                    .acceptLicense();

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar driver = DependencyJar.ofClassName("com.ibm.db2.jcc.DB2Driver");
                driver.copyTo(container, "/tmp/seatunnel/plugins/DB2-CDC/lib", "db2jcc.jar");
                driver.copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib", "db2jcc.jar");
            };

    @Override
    @BeforeAll
    public void startUp() {
        configureSeaTunnelJvmOptions();
        log.info("Starting DB2 container...");
        Startables.deepStart(Stream.of(DB2_CONTAINER)).join();
        log.info("DB2 container is started.");
    }

    @Override
    @AfterAll
    public void tearDown() {
        log.info("Stopping DB2 container...");
        if (DB2_CONTAINER != null) {
            DB2_CONTAINER.stop();
        }
        clearSeaTunnelJvmOptions();
        log.info("DB2 container is stopped.");
    }

    @TestTemplate
    public void testDb2CdcToDb2(TestContainer container) {
        runSnapshotAndStream(container, "/db2cdc_to_db2.conf");
    }

    /** Exercises the factory normalization through real catalog discovery and capture filtering. */
    @TestTemplate
    public void testLowerCaseTableNames(TestContainer container) {
        runSnapshotAndStream(container, "/db2cdc_to_db2_with_lower_case_table_names.conf");
    }

    /**
     * Restores both tracked tables, catches up stopped-job writes and resumes both change streams.
     * The synthetic capture catalog does not exercise IBM ASN DDL-history replay.
     */
    @TestTemplate
    public void testMultiTableSavepointRestore(TestContainer container) throws Exception {
        initializeDb2Tables(true);
        CompletableFuture<Void> initial = executeAsync(container, MULTI_TABLE_CONF, null);
        awaitRestoreTables(initial, true);
        applyPhase(0, true);
        awaitRestoreTables(initial, true);
        String jobId = stopAtSavepoint(container, MULTI_TABLE_CONF, initial);

        applyPhase(1, true);
        CompletableFuture<Void> restored = executeAsync(container, MULTI_TABLE_CONF, jobId);
        awaitRestoreTables(restored, true);
        applyPhase(2, true);
        awaitRestoreTables(restored, true);
    }

    /**
     * Adds and registers the second source table only after the original single-table job stops.
     */
    @TestTemplate
    public void testAddTableDuringSavepointRestore(TestContainer container) throws Exception {
        initializeDb2Tables(false);
        CompletableFuture<Void> initial = executeAsync(container, ADD_TABLE_CONF, null);
        awaitRestoreTables(initial, false);
        applyPhase(0, false);
        awaitRestoreTables(initial, false);
        String jobId = stopAtSavepoint(container, ADD_TABLE_CONF, initial);

        applyPhase(1, false);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            createProductTables(connection, statement);
        }
        CompletableFuture<Void> restored = executeAsync(container, MULTI_TABLE_CONF, jobId);
        awaitRestoreTables(restored, true);
        applyPhase(2, false);
        try (Connection connection = getJdbcConnection()) {
            connection.setAutoCommit(false);
            applyTableChanges(connection, PRODUCT_TABLE, PRODUCT_CHANGES, 11, 13, 11);
            updateCaptureSynchpoint(connection, "PRODUCTS", 12);
            connection.commit();
        }
        awaitRestoreTables(restored, true);
    }

    private void runSnapshotAndStream(TestContainer container, String configuration) {
        initializeDb2Tables();

        CompletableFuture<Void> jobFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                Container.ExecResult result = container.executeJob(configuration);
                                Assertions.assertEquals(
                                        0, result.getExitCode(), result.getStderr());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            assertJobNotFailed(jobFuture);
                            Assertions.assertIterableEquals(
                                    querySql(SELECT_CUSTOMERS, SOURCE_TABLE),
                                    querySql(SELECT_CUSTOMERS, SINK_TABLE));
                        });

        applyChangeEvents();

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            assertJobNotFailed(jobFuture);
                            Assertions.assertIterableEquals(
                                    querySql(SELECT_CUSTOMERS, SOURCE_TABLE),
                                    querySql(SELECT_CUSTOMERS, SINK_TABLE));
                        });
    }

    private void initializeDb2Tables() {
        initializeDb2Tables(false);
    }

    private void initializeDb2Tables(boolean includeProducts) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            createSchemaIfNotExists(statement, SCHEMA);
            createSchemaIfNotExists(statement, CDC_SCHEMA);
            dropTableIfExists(statement, CHANGE_TABLE);
            dropTableIfExists(statement, PRODUCT_CHANGES);
            dropTableIfExists(statement, PRODUCT_TABLE);
            dropTableIfExists(statement, RESTORE_SINK);
            dropTableIfExists(statement, CDC_SCHEMA + ".IBMSNAP_REGISTER");
            dropTableIfExists(statement, SINK_TABLE);
            dropTableIfExists(statement, SOURCE_TABLE);

            statement.execute(
                    "CREATE TABLE "
                            + SOURCE_TABLE
                            + " (ID INTEGER NOT NULL, NAME VARCHAR(64) NOT NULL, "
                            + "DESCRIPTION VARCHAR(128), PRIMARY KEY (ID))");
            statement.execute(
                    "CREATE TABLE "
                            + SINK_TABLE
                            + " (ID INTEGER NOT NULL, NAME VARCHAR(64) NOT NULL, "
                            + "DESCRIPTION VARCHAR(128), PRIMARY KEY (ID))");
            statement.execute(
                    "CREATE TABLE "
                            + CDC_SCHEMA
                            + ".IBMSNAP_REGISTER (SOURCE_OWNER VARCHAR(128) NOT NULL, "
                            + "SOURCE_TABLE VARCHAR(128) NOT NULL, CD_OWNER VARCHAR(128) NOT NULL, "
                            + "CD_TABLE VARCHAR(128) NOT NULL, "
                            + "CD_NEW_SYNCHPOINT CHAR(16) FOR BIT DATA, "
                            + "CD_OLD_SYNCHPOINT CHAR(16) FOR BIT DATA, "
                            + "SYNCHPOINT CHAR(16) FOR BIT DATA)");
            // Debezium's ASNCDC.ADDTABLE procedure creates three CDC metadata columns before
            // the source columns. Adding extra metadata here would shift Debezium's result-set
            // offset and make row data decode against the wrong schema.
            statement.execute(
                    "CREATE TABLE "
                            + CHANGE_TABLE
                            + " (IBMSNAP_COMMITSEQ CHAR(16) FOR BIT DATA NOT NULL, "
                            + "IBMSNAP_INTENTSEQ CHAR(16) FOR BIT DATA NOT NULL, "
                            + "IBMSNAP_OPERATION CHAR(1) NOT NULL, "
                            + "ID INTEGER, NAME VARCHAR(64), DESCRIPTION VARCHAR(128))");

            // The DB2 E2E image does not run IBM ASN capture. This minimal ASNCDC catalog
            // exposes the source table to Debezium while the test appends deterministic change
            // rows to the capture table below.
            insertCustomer(connection, SOURCE_TABLE, 1, "Alice", "snapshot row 1");
            insertCustomer(connection, SOURCE_TABLE, 2, "Bob", "snapshot row 2");
            insertCaptureRegistration(connection);
            statement.execute(
                    "CREATE TABLE "
                            + RESTORE_SINK
                            + " (ID INTEGER NOT NULL, NAME VARCHAR(64) NOT NULL, DESCRIPTION VARCHAR(128), PRIMARY KEY (ID))");
            if (includeProducts) {
                createProductTables(connection, statement);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize DB2 CDC E2E tables", e);
        }
    }

    private void applyChangeEvents() {
        try (Connection connection = getJdbcConnection()) {
            connection.setAutoCommit(false);
            insertCustomer(connection, SOURCE_TABLE, 3, "Carol", "stream insert row");
            insertChangeEvent(connection, 1, "I", 3, "Carol", "stream insert row");

            deleteCustomer(connection, SOURCE_TABLE, 1);
            insertChangeEvent(connection, 2, "D", 1, "Alice", "snapshot row 1");
            updateCaptureSynchpoint(connection, 2);
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to apply DB2 CDC E2E change events", e);
        }
    }

    private void createSchemaIfNotExists(Statement statement, String schema) throws SQLException {
        try {
            statement.execute("CREATE SCHEMA " + schema);
        } catch (SQLException e) {
            if (!"42710".equals(e.getSQLState())) {
                throw e;
            }
        }
    }

    private void dropTableIfExists(Statement statement, String table) throws SQLException {
        try {
            statement.execute("DROP TABLE " + table);
        } catch (SQLException e) {
            if (!"42704".equals(e.getSQLState())) {
                throw e;
            }
        }
    }

    private void insertCaptureRegistration(Connection connection) throws SQLException {
        insertCaptureRegistration(connection, "CUSTOMERS", CAPTURE_TABLE);
    }

    private void insertCaptureRegistration(Connection connection, String table, String capture)
            throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "INSERT INTO "
                                + CDC_SCHEMA
                                + ".IBMSNAP_REGISTER "
                                + "(SOURCE_OWNER, SOURCE_TABLE, CD_OWNER, CD_TABLE, "
                                + "CD_NEW_SYNCHPOINT, CD_OLD_SYNCHPOINT, SYNCHPOINT) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            statement.setString(1, SCHEMA);
            statement.setString(2, table);
            statement.setString(3, CDC_SCHEMA);
            statement.setString(4, capture);
            statement.setBytes(5, lsn(0));
            // A real active registration can have a non-null OLD point. Treating it as a
            // stop LSN would filter every subsequent change and fail the source/sink checks.
            statement.setBytes(6, lsn(0));
            statement.setBytes(7, lsn(0));
            statement.executeUpdate();
        }
    }

    private void updateCaptureSynchpoint(Connection connection, int commitLsn) throws SQLException {
        updateCaptureSynchpoint(connection, "CUSTOMERS", commitLsn);
    }

    private void updateCaptureSynchpoint(Connection connection, String table, int commitLsn)
            throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "UPDATE "
                                + CDC_SCHEMA
                                + ".IBMSNAP_REGISTER SET SYNCHPOINT = ?, CD_NEW_SYNCHPOINT = ? "
                                + "WHERE SOURCE_OWNER = ? AND SOURCE_TABLE = ?")) {
            // Debezium polls the max LSN from IBMSNAP_REGISTER, so advancing SYNCHPOINT makes the
            // synthetic capture rows visible to the transaction-log reader.
            statement.setBytes(1, lsn(commitLsn));
            statement.setBytes(2, lsn(commitLsn));
            statement.setString(3, SCHEMA);
            statement.setString(4, table);
            statement.executeUpdate();
        }
    }

    private void insertCustomer(
            Connection connection, String table, int id, String name, String description)
            throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "INSERT INTO " + table + " (ID, NAME, DESCRIPTION) VALUES (?, ?, ?)")) {
            statement.setInt(1, id);
            statement.setString(2, name);
            statement.setString(3, description);
            statement.executeUpdate();
        }
    }

    private void deleteCustomer(Connection connection, String table, int id) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement("DELETE FROM " + table + " WHERE ID = ?")) {
            statement.setInt(1, id);
            statement.executeUpdate();
        }
    }

    /**
     * Inserts rows into the synthetic DB2 change table in the same shape that Debezium's DB2
     * connector reads from ASNCDC capture tables.
     */
    private void insertChangeEvent(
            Connection connection,
            int commitLsn,
            String operation,
            int id,
            String name,
            String description)
            throws SQLException {
        insertChangeEvent(connection, CHANGE_TABLE, commitLsn, operation, id, name, description);
    }

    private void insertChangeEvent(
            Connection connection,
            String changeTable,
            int commitLsn,
            String operation,
            int id,
            String name,
            String description)
            throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "INSERT INTO "
                                + changeTable
                                + " (IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ, IBMSNAP_OPERATION, "
                                + "ID, NAME, DESCRIPTION) "
                                + "VALUES (?, ?, ?, ?, ?, ?)")) {
            statement.setBytes(1, lsn(commitLsn));
            statement.setBytes(2, lsn(commitLsn));
            statement.setString(3, operation);
            statement.setInt(4, id);
            statement.setString(5, name);
            statement.setString(6, description);
            statement.executeUpdate();
        }
    }

    private void createProductTables(Connection connection, Statement statement)
            throws SQLException {
        statement.execute(
                "CREATE TABLE "
                        + PRODUCT_TABLE
                        + " (ID INTEGER NOT NULL, NAME VARCHAR(64) NOT NULL, DESCRIPTION VARCHAR(128), PRIMARY KEY (ID))");
        statement.execute(
                "CREATE TABLE "
                        + PRODUCT_CHANGES
                        + " (IBMSNAP_COMMITSEQ CHAR(16) FOR BIT DATA NOT NULL, IBMSNAP_INTENTSEQ CHAR(16) FOR BIT DATA NOT NULL, "
                        + "IBMSNAP_OPERATION CHAR(1) NOT NULL, ID INTEGER, NAME VARCHAR(64), DESCRIPTION VARCHAR(128))");
        insertCustomer(connection, PRODUCT_TABLE, 11, "Keyboard", "snapshot product 1");
        insertCustomer(connection, PRODUCT_TABLE, 12, "Mouse", "snapshot product 2");
        insertCaptureRegistration(connection, "PRODUCTS", PRODUCT_CAPTURE);
    }

    /**
     * Commits deterministic insert/delete pairs at distinct LSNs for each table and recovery phase.
     */
    private void applyPhase(int phase, boolean includeProducts) {
        try (Connection connection = getJdbcConnection()) {
            connection.setAutoCommit(false);
            int firstLsn = phase * 4 + 1;
            applyTableChanges(
                    connection, SOURCE_TABLE, CHANGE_TABLE, firstLsn, phase + 3, phase + 1);
            updateCaptureSynchpoint(connection, "CUSTOMERS", firstLsn + 1);
            if (includeProducts) {
                applyTableChanges(
                        connection,
                        PRODUCT_TABLE,
                        PRODUCT_CHANGES,
                        firstLsn + 2,
                        phase + 13,
                        phase + 11);
                updateCaptureSynchpoint(connection, "PRODUCTS", firstLsn + 3);
            }
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to write DB2 recovery phase " + phase, e);
        }
    }

    private void applyTableChanges(
            Connection connection,
            String table,
            String changeTable,
            int firstLsn,
            int insertedId,
            int deletedId)
            throws SQLException {
        String name = "row-" + insertedId;
        String description = "stream-" + firstLsn;
        insertCustomer(connection, table, insertedId, name, description);
        insertChangeEvent(connection, changeTable, firstLsn, "I", insertedId, name, description);
        try (PreparedStatement query =
                connection.prepareStatement(
                        "SELECT NAME, DESCRIPTION FROM " + table + " WHERE ID = ?")) {
            query.setInt(1, deletedId);
            try (ResultSet row = query.executeQuery()) {
                Assertions.assertTrue(
                        row.next(), "Delete fixture must exist: " + table + "/" + deletedId);
                insertChangeEvent(
                        connection,
                        changeTable,
                        firstLsn + 1,
                        "D",
                        deletedId,
                        row.getString(1),
                        row.getString(2));
            }
        }
        deleteCustomer(connection, table, deletedId);
    }

    private CompletableFuture<Void> executeAsync(
            TestContainer container, String configuration, String restoreJobId) {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        Container.ExecResult result =
                                restoreJobId == null
                                        ? container.executeJob(configuration)
                                        : container.restoreJob(configuration, restoreJobId);
                        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to run DB2 CDC recovery job", e);
                    }
                });
    }

    private void awaitRestoreTables(CompletableFuture<Void> job, boolean includeProducts) {
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            assertJobNotFailed(job);
                            String source =
                                    includeProducts
                                            ? "(SELECT ID, NAME, DESCRIPTION FROM "
                                                    + SOURCE_TABLE
                                                    + " UNION ALL SELECT ID, NAME, DESCRIPTION FROM "
                                                    + PRODUCT_TABLE
                                                    + ") AS SOURCES"
                                            : SOURCE_TABLE;
                            Assertions.assertIterableEquals(
                                    querySql(SELECT_CUSTOMERS, source),
                                    querySql(SELECT_CUSTOMERS, RESTORE_SINK));
                        });
    }

    /**
     * The Apache test runner supplies --name using the configuration basename, overriding
     * env.job.name.
     */
    private String stopAtSavepoint(
            TestContainer container, String configuration, CompletableFuture<Void> initial)
            throws Exception {
        String jobName = configuration.substring(configuration.lastIndexOf('/') + 1);
        Matcher matcher =
                Pattern.compile(
                                "Init JobMaster for Job "
                                        + Pattern.quote(jobName)
                                        + " \\(([0-9]+)\\)")
                        .matcher(container.getServerLogs());
        String jobId = null;
        int matches = 0;
        while (matcher.find()) {
            jobId = matcher.group(1);
            matches++;
        }
        Assertions.assertEquals(1, matches, "Expected one initial DB2 CDC job for " + jobName);
        Container.ExecResult result = container.savepointJob(jobId);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        await().atMost(30, TimeUnit.SECONDS).until(initial::isDone);
        assertJobNotFailed(initial);
        return jobId;
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                DB2_CONTAINER.getJdbcUrl(),
                DB2_CONTAINER.getUsername(),
                DB2_CONTAINER.getPassword());
    }

    private List<List<Object>> querySql(String sql, String table) {
        return JdbcUtil.querySql(
                String.format(sql, table),
                () -> {
                    try {
                        return this.getJdbcConnection();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void assertJobNotFailed(CompletableFuture<Void> jobFuture) {
        if (jobFuture.isCompletedExceptionally()) {
            jobFuture.join();
        }
    }

    private void configureSeaTunnelJvmOptions() {
        // DB2 is a memory-heavy container, so this E2E keeps Zeta's server and client JVMs
        // smaller than the shared defaults to avoid resource competition on constrained runners.
        System.setProperty(
                SeaTunnelContainer.SERVER_JVM_OPTION_PROPERTY, SEATUNNEL_SERVER_JVM_OPTION);
        System.setProperty(
                SeaTunnelContainer.CLIENT_JVM_OPTION_PROPERTY, SEATUNNEL_CLIENT_JVM_OPTION);
    }

    private void clearSeaTunnelJvmOptions() {
        System.clearProperty(SeaTunnelContainer.SERVER_JVM_OPTION_PROPERTY);
        System.clearProperty(SeaTunnelContainer.CLIENT_JVM_OPTION_PROPERTY);
    }

    private byte[] lsn(int value) {
        byte[] lsn = new byte[16];
        lsn[15] = (byte) value;
        return lsn;
    }
}
