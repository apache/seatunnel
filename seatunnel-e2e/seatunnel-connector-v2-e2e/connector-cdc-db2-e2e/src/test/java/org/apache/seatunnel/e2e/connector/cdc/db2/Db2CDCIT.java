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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
        initializeDb2Tables();

        CompletableFuture<Void> jobFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                Container.ExecResult result =
                                        container.executeJob("/db2cdc_to_db2.conf");
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
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            createSchemaIfNotExists(statement, SCHEMA);
            createSchemaIfNotExists(statement, CDC_SCHEMA);
            dropTableIfExists(statement, CHANGE_TABLE);
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
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "INSERT INTO "
                                + CDC_SCHEMA
                                + ".IBMSNAP_REGISTER "
                                + "(SOURCE_OWNER, SOURCE_TABLE, CD_OWNER, CD_TABLE, "
                                + "CD_NEW_SYNCHPOINT, CD_OLD_SYNCHPOINT, SYNCHPOINT) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            statement.setString(1, SCHEMA);
            statement.setString(2, "CUSTOMERS");
            statement.setString(3, CDC_SCHEMA);
            statement.setString(4, CAPTURE_TABLE);
            statement.setBytes(5, lsn(0));
            // A single active capture instance must not have a stop LSN. Debezium treats a
            // non-null CD_OLD_SYNCHPOINT as the stop point of a retired capture table.
            statement.setNull(6, Types.BINARY);
            statement.setBytes(7, lsn(0));
            statement.executeUpdate();
        }
    }

    private void updateCaptureSynchpoint(Connection connection, int commitLsn) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "UPDATE "
                                + CDC_SCHEMA
                                + ".IBMSNAP_REGISTER SET SYNCHPOINT = ? "
                                + "WHERE SOURCE_OWNER = ? AND SOURCE_TABLE = ?")) {
            // Debezium polls the max LSN from IBMSNAP_REGISTER, so advancing SYNCHPOINT makes the
            // synthetic capture rows visible to the transaction-log reader.
            statement.setBytes(1, lsn(commitLsn));
            statement.setString(2, SCHEMA);
            statement.setString(3, "CUSTOMERS");
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
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "INSERT INTO "
                                + CHANGE_TABLE
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
