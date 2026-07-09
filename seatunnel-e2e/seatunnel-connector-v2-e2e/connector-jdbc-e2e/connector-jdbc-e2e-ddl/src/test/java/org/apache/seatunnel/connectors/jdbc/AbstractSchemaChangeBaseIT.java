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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Currently SPARK do not support cdc. In addition, currently only the zeta engine supports schema evolution for pr https://github.com/apache/seatunnel/pull/5125.")
public abstract class AbstractSchemaChangeBaseIT extends TestSuiteBase implements TestResource {
    private static final long SCHEMA_ASSERT_TIMEOUT_MILLIS = 300000L;

    private static final String SOURCE_DATABASE = "shop";
    private static final String SOURCE_TABLE = "products";
    /**
     * Deterministic source row used to prove the MySQL CDC reader is consuming binlog events before
     * schema-change DDL is executed.
     */
    private static final int STREAM_READY_MARKER_ID = 1000;
    /**
     * Marker value written to the source table so sink-side readiness polling can identify the
     * probe row without depending on connector internals.
     */
    private static final String STREAM_READY_MARKER_NAME = "__cdc_stream_ready__";
    /**
     * Stable payload for the readiness probe row; keeping it constant makes repeated test attempts
     * idempotent through the upsert statement.
     */
    private static final String STREAM_READY_MARKER_DESCRIPTION =
            "wait for binlog stream readiness";

    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";

    private static final String ORDER_BY = " order by id";
    private static final String QUERY = "select * from %s.%s";
    private static final String PROJECTION_QUERY =
            "select id,name,description,weight,add_column1,add_column2,add_column3 from %s.%s";
    private static final String SOURCE_DESC_QUERY = "desc %s.%s";

    private static final String SOURCE_QUERY_COLUMNS =
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER by COLUMN_NAME";

    protected final String SINK_DATABASE = "shop";
    protected final String SINK_TABLE1 = "sink_table_with_schema_change";
    protected final String SINK_TABLE2 = "sink_table_with_schema_change_exactly_once";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase sourceDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, SOURCE_DATABASE, "mysqluser", "mysqlpw", SOURCE_DATABASE);

    protected GenericContainer<?> sinkDbServer;
    protected SchemaChangeCase schemaChangeCase;

    protected abstract SchemaChangeCase getSchemaChangeCase();

    protected abstract GenericContainer initSinkContainer();

    protected abstract String sinkDatabaseType();

    protected void intializeSinkDatabase() {}

    @BeforeAll
    @Override
    public void startUp() {
        schemaChangeCase = getSchemaChangeCase();
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        sourceDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
        // sink database initialization
        log.info("The third stage: Starting {} containers...", sinkDatabaseType());
        sinkDbServer = initSinkContainer().withImagePullPolicy(PullPolicy.defaultPolicy());
        Startables.deepStart(Stream.of(sinkDbServer)).join();
        log.info("{} Containers are started", sinkDatabaseType());
        intializeSinkDatabase();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
        if (sinkDbServer != null) {
            sinkDbServer.close();
        }
    }

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(SOURCE_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands1 =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(
                        0, extraCommands1.getExitCode(), extraCommands1.getStderr());
                Container.ExecResult extraCommands2 =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + schemaChangeCase.getDriverUrl());
                Assertions.assertEquals(
                        0, extraCommands2.getExitCode(), extraCommands2.getStderr());
            };

    @Order(1)
    @TestTemplate
    public void testMysqlCdcWithSchemaEvolutionCase(TestContainer container)
            throws IOException, InterruptedException {
        String jobConfigFile = schemaChangeCase.getSchemaEvolutionCase();
        if (StringUtils.isEmpty(jobConfigFile)) {
            Assertions.fail(
                    "testMysqlCdcWithSchemaEvolutionCase E2E case configuration file cannot be empty");
        }
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        given().pollDelay(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(1000))
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals("RUNNING", container.getJobStatus(jobId));
                        });

        // waiting for case1 completed
        assertSchemaEvolutionForAddColumns(SOURCE_TABLE, schemaChangeCase.getSinkTable1());

        // savepoint 1
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());

        // case2 drop columns with cdc data at same time
        sourceDatabase.setTemplateName("drop_columns").createAndInitialize();

        // restore 1
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // waiting for case2 completed
        assertTableStructureAndData(SOURCE_TABLE, schemaChangeCase.getSinkTable1());

        // savepoint 2
        given().pollDelay(Duration.ofSeconds(5))
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        0, container.savepointJob(jobId).getExitCode()));

        // case3 change column name with cdc data at same time
        sourceDatabase.setTemplateName("change_columns").createAndInitialize();

        // case4 modify column data type with cdc data at same time
        sourceDatabase.setTemplateName("modify_columns").createAndInitialize();

        // restore 2
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception : {}", e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // waiting for case3/case4 completed
        assertTableStructureAndData(SOURCE_TABLE, schemaChangeCase.getSinkTable1());
    }

    @Order(2)
    @TestTemplate
    public void testMysqlCdcWithSchemaEvolutionCaseExactlyOnce(TestContainer container) {
        if (!schemaChangeCase.isOpenExactlyOnce()) {
            log.info(
                    "{} not support Xa transactions, Skip testMysqlCdcWithSchemaEvolutionCaseExactlyOnce",
                    sinkDatabaseType());
            return;
        }
        String jobConfigFile = schemaChangeCase.getSchemaEvolutionCaseExactlyOnce();
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        sourceDatabase.setTemplateName("shop").createAndInitialize();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        given().pollDelay(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(1000))
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals("RUNNING", container.getJobStatus(jobId));
                        });

        assertSchemaEvolution(SOURCE_TABLE, schemaChangeCase.getSinkTable2());
    }

    private void assertSchemaEvolution(String sourceTable, String sinkTable) {
        // The exactly-once path can report RUNNING before the sink finishes the first XA batch in
        // slower CI environments, so reuse the longer schema assertion timeout for the initial
        // data catch-up instead of failing on a transient empty sink table.
        await().atMost(SCHEMA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        sourceTable, sinkTable, null));

        waitForStreamingReady(sourceTable, sinkTable);

        // case1 add columns with cdc data at same time
        sourceDatabase.setTemplateName("add_columns").createAndInitialize();
        waitForSinkColumnsCatchUp(sourceTable, sinkTable);
        assertAddColumnsDataSynced(sourceTable, sinkTable);

        // case2 drop columns with cdc data at same time
        assertCaseByDdlName("drop_columns");

        // case3 change column name with cdc data at same time
        assertCaseByDdlName("change_columns");

        // case4 modify column data type with cdc data at same time
        assertCaseByDdlName("modify_columns");
    }

    private void assertCaseByDdlName(String ddlTemplateName) {
        sourceDatabase.setTemplateName(ddlTemplateName).createAndInitialize();
        assertTableStructureAndData(SOURCE_TABLE, schemaChangeCase.getSinkTable2());
    }

    private void assertSchemaEvolutionForAddColumns(String sourceTable, String sinkTable) {
        await().atMost(SCHEMA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        sourceTable, sinkTable, null));

        waitForStreamingReady(sourceTable, sinkTable);

        // case1 add columns with cdc data at same time
        sourceDatabase.setTemplateName("add_columns").createAndInitialize();
        waitForSinkColumnsCatchUp(sourceTable, sinkTable);
        assertAddColumnsDataSynced(sourceTable, sinkTable);
    }

    /**
     * Snapshot convergence does not prove the MySQL CDC reader has entered steady-state binlog
     * consumption. Emit one deterministic DML event and wait until the sink receives it before
     * running schema-change DDL bursts.
     */
    private void waitForStreamingReady(String sourceTable, String sinkTable) {
        executeSourceSql(
                String.format(
                        "INSERT INTO %s.%s (id, name, description, weight) "
                                + "VALUES (%d, '%s', '%s', 0.0) "
                                + "ON DUPLICATE KEY UPDATE "
                                + "name = VALUES(name), description = VALUES(description), weight = VALUES(weight)",
                        SOURCE_DATABASE,
                        sourceTable,
                        STREAM_READY_MARKER_ID,
                        STREAM_READY_MARKER_NAME,
                        STREAM_READY_MARKER_DESCRIPTION));

        String readyQuery =
                String.format(
                        "select id,name,description,weight from %%s.%%s where id = %d order by id",
                        STREAM_READY_MARKER_ID);
        await().atMost(SCHEMA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        querySource(
                                                String.format(
                                                        readyQuery, SOURCE_DATABASE, sourceTable)),
                                        querySink(
                                                String.format(
                                                        readyQuery,
                                                        schemaChangeCase.getSchemaName(),
                                                        sinkTable))));
    }

    /**
     * Schema-change sinks can publish the new rows before the sink table metadata is fully updated.
     * Waiting for the column list first avoids racing the add-columns data assertions.
     */
    private void waitForSinkColumnsCatchUp(String sourceTable, String sinkTable) {
        await().atMost(SCHEMA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> assertColumnNamesEqualsIgnoringPhysicalOrder(sourceTable, sinkTable));
    }

    /**
     * Validates both the new add-columns rows and the projected full-table view once schema
     * evolution has settled on the sink side.
     */
    private void assertAddColumnsDataSynced(String sourceTable, String sinkTable) {
        await().atMost(SCHEMA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            assertTableDataEqualsBySourceColumnOrder(
                                    sourceTable, sinkTable, "id >= 128");

                            Assertions.assertIterableEquals(
                                    querySource(
                                            String.format(
                                                    PROJECTION_QUERY,
                                                    SOURCE_DATABASE,
                                                    sourceTable)),
                                    querySink(
                                            String.format(
                                                            PROJECTION_QUERY,
                                                            schemaChangeCase.getSchemaName(),
                                                            sinkTable)
                                                    + ORDER_BY));
                        });
    }

    private void assertTableStructureAndData(String sourceTable, String sinkTable) {
        given().pollDelay(Duration.ofSeconds(5))
                .await()
                .atMost(SCHEMA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> assertColumnNamesEqualsIgnoringPhysicalOrder(sourceTable, sinkTable));
        await().atMost(SCHEMA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        sourceTable, sinkTable, null));
    }

    /**
     * JDBC schema evolution can keep the effective column set while materializing a different
     * physical order in the sink, so schema assertions should compare normalized column names.
     */
    private void assertColumnNamesEqualsIgnoringPhysicalOrder(
            String sourceTable, String sinkTable) {
        Assertions.assertIterableEquals(
                normalizeColumnNames(
                        querySource(
                                String.format(SOURCE_QUERY_COLUMNS, SOURCE_DATABASE, sourceTable))),
                normalizeColumnNames(
                        querySink(
                                String.format(
                                        schemaChangeCase.getSinkQueryColumns(),
                                        schemaChangeCase.getSchemaName(),
                                        sinkTable))));
    }

    /**
     * Projects sink data with the current source column order so row assertions stay stable when a
     * JDBC sink reorders equivalent columns after applying schema changes.
     */
    private void assertTableDataEqualsBySourceColumnOrder(
            String sourceTable, String sinkTable, String whereClause) {
        List<String> sourceColumns = getSourceColumnNames(sourceTable);
        Assertions.assertIterableEquals(
                querySource(
                        buildProjectionQuery(
                                SOURCE_DATABASE, sourceTable, sourceColumns, whereClause)),
                querySink(
                        buildProjectionQuery(
                                schemaChangeCase.getSchemaName(),
                                sinkTable,
                                sourceColumns,
                                whereClause)));
    }

    /** Reads the current MySQL source schema order that downstream row assertions should follow. */
    private List<String> getSourceColumnNames(String sourceTable) {
        List<String> sourceColumns = new ArrayList<>();
        for (List<Object> row :
                querySource(String.format(SOURCE_DESC_QUERY, SOURCE_DATABASE, sourceTable))) {
            sourceColumns.add(String.valueOf(row.get(0)));
        }
        return sourceColumns;
    }

    /** Builds a deterministic projection query without relying on sink-specific physical order. */
    private String buildProjectionQuery(
            String database, String table, List<String> columns, String whereClause) {
        StringBuilder queryBuilder =
                new StringBuilder("select ")
                        .append(String.join(",", columns))
                        .append(" from ")
                        .append(database)
                        .append(".")
                        .append(table);
        if (StringUtils.isNotBlank(whereClause)) {
            queryBuilder.append(" where ").append(whereClause);
        }
        return queryBuilder.append(ORDER_BY).toString();
    }

    /** Sorts schema query output by column name so assertions ignore placement-only differences. */
    private List<String> normalizeColumnNames(List<List<Object>> rows) {
        List<String> normalizedColumnNames = new ArrayList<>();
        for (List<Object> row : rows) {
            normalizedColumnNames.add(String.valueOf(row.get(0)));
        }
        normalizedColumnNames.sort(String::compareTo);
        return normalizedColumnNames;
    }

    private Connection getJdbcConnection(String connectionType) throws SQLException {
        if (connectionType.equals("source")) {
            return DriverManager.getConnection(
                    MYSQL_CONTAINER.getJdbcUrl(),
                    MYSQL_CONTAINER.getUsername(),
                    MYSQL_CONTAINER.getPassword());
        }
        return DriverManager.getConnection(
                String.format(
                        schemaChangeCase.getJdbcUrl(),
                        sinkDbServer.getHost(),
                        schemaChangeCase.getPort(),
                        schemaChangeCase.getDatabaseName()),
                schemaChangeCase.getUsername(),
                schemaChangeCase.getPassword());
    }

    private List<List<Object>> querySource(String sql) {
        try (Connection connection = getJdbcConnection("source")) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                log.debug(String.format("Print MySQL-CDC query, sql: %s, data: %s", sql, objects));
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes a source-side DML statement directly against MySQL to produce a CDC event that the
     * running SeaTunnel job must consume.
     */
    private void executeSourceSql(String sql) {
        try (Connection connection = getJdbcConnection("source");
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<List<Object>> querySink(String sql) {
        try (Connection connection = getJdbcConnection("sink")) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object object = resultSet.getObject(i);
                    if (object instanceof NClob) {
                        objects.add(readNClobAsString((NClob) object));
                    } else if (object instanceof java.time.OffsetDateTime) {
                        // TIMESTAMP_TZ (OffsetDateTime) → normalize to Timestamp for comparison
                        // with MySQL source which returns java.sql.Timestamp
                        objects.add(
                                java.sql.Timestamp.valueOf(
                                        ((java.time.OffsetDateTime) object).toLocalDateTime()));
                    } else if (object != null
                            && object.getClass().getName().equals("microsoft.sql.DateTimeOffset")) {
                        // SQL Server DATETIMEOFFSET → normalize to Timestamp for comparison
                        // microsoft.sql.DateTimeOffset.getTimestamp() returns java.sql.Timestamp
                        try {
                            java.lang.reflect.Method getTimestamp =
                                    object.getClass().getMethod("getTimestamp");
                            objects.add(getTimestamp.invoke(object));
                        } catch (Exception e) {
                            objects.add(object);
                        }
                    } else {
                        objects.add(object);
                    }
                }
                log.debug(
                        String.format(
                                "Print %s query, sql: %s, data: %s",
                                sinkDatabaseType(), sql, objects));
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Object readNClobAsString(NClob nclob) {
        try (Reader reader = nclob.getCharacterStream();
                BufferedReader bufferedReader = new BufferedReader(reader)) {
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }
            return stringBuilder.toString();
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
