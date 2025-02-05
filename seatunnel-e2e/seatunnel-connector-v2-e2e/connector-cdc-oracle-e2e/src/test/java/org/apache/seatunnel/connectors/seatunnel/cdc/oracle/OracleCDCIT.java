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
package org.apache.seatunnel.connectors.seatunnel.cdc.oracle;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.lifecycle.Startables;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class OracleCDCIT extends AbstractOracleCDCIT implements TestResource {

    private static final String SOURCE_TABLE_NO_PRIMARY_KEY = "FULL_TYPES_NO_PRIMARY_KEY";

    private static final String SINK_TABLE1 = "SINK_FULL_TYPES";
    private static final String SINK_TABLE2 = "SINK_FULL_TYPES2";
    private static final String SOURCE_SQL_TEMPLATE = "select * from %s.%s ORDER BY ID";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Oracle-CDC/lib && cd /tmp/seatunnel/plugins/Oracle-CDC/lib && wget "
                                        + oracleDriverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        ORACLE_CONTAINER.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", ORACLE_PORT, ORACLE_PORT)));
        log.info("Starting Oracle containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        log.info("Oracle containers are started.");
        createAndInitialize("column_type_test", ADMIN_USER, ADMIN_PWD);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        ORACLE_CONTAINER.stop();
    }

    @TestTemplate
    public void testOracleCdcPartition(TestContainer container) throws Exception {
        String sourceTable = "PARTITION_SOURCE_TABLE";
        String sinkTable = "PARTITION_SINK_TABLE";
        clearTable(SCEHMA_NAME, sinkTable);
        clearTable(SCEHMA_NAME, sourceTable);

        insertSourceTable(SCEHMA_NAME, sourceTable);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/oraclecdc_to_oracle_with_partition.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, sourceTable)),
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, sinkTable)));
                        });

        // insert update delete
        updateSourceTable(SCEHMA_NAME, sourceTable);

        // stream stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, sourceTable)),
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, sinkTable)));
                        });
    }

    @TestTemplate
    public void testOracleCdcCheckDataE2e(TestContainer container) throws Exception {
        checkDataForTheJob(container, "/oraclecdc_to_oracle.conf", false);
    }

    @TestTemplate
    public void testOracleCdcCheckDataE2eForUseSelectCount(TestContainer container)
            throws Exception {
        checkDataForTheJob(container, "/oraclecdc_to_oracle_use_select_count.conf", false);
    }

    @TestTemplate
    public void testOracleCdcCheckDataE2eForSkipAnalysis(TestContainer container) throws Exception {
        checkDataForTheJob(container, "/oraclecdc_to_oracle_skip_analysis.conf", true);
    }

    private void checkDataForTheJob(
            TestContainer container, String jobConfPath, Boolean skipAnalysis) throws Exception {
        clearTable(SCEHMA_NAME, SOURCE_TABLE1);
        clearTable(SCEHMA_NAME, SOURCE_TABLE2);
        clearTable(SCEHMA_NAME, SINK_TABLE1);
        clearTable(SCEHMA_NAME, SINK_TABLE2);

        insertSourceTable(SCEHMA_NAME, SOURCE_TABLE1);

        if (skipAnalysis) {
            // analyzeTable before execute job
            String analyzeTable =
                    String.format(
                            "analyze table "
                                    + "\"DEBEZIUM\".\"FULL_TYPES\" "
                                    + "compute statistics for table");
            log.info("analyze table {}", analyzeTable);
            try (Connection connection = getJdbcConnection(ORACLE_CONTAINER);
                    Statement statement = connection.createStatement()) {
                statement.execute(analyzeTable);
            }
        }

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(jobConfPath);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, SOURCE_TABLE1)),
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, SINK_TABLE1)));
                        });

        // insert update delete
        updateSourceTable(SCEHMA_NAME, SOURCE_TABLE1);

        // stream stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, SOURCE_TABLE1)),
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, SINK_TABLE1)));
                        });
    }

    @TestTemplate
    public void testOracleCdcCheckDataWithNoPrimaryKey(TestContainer container) throws Exception {

        clearTable(SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY);
        clearTable(SCEHMA_NAME, SINK_TABLE1);

        insertSourceTable(SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/oraclecdc_to_oracle_with_no_primary_key.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(
                                            getSourceQuerySQL(
                                                    SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY)),
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, SINK_TABLE1)));
                        });

        // insert update delete
        updateSourceTable(SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY);

        // stream stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(
                                            getSourceQuerySQL(
                                                    SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY)),
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, SINK_TABLE1)));
                        });
    }

    @TestTemplate
    public void testOracleCdcCheckDataWithCustomPrimaryKey(TestContainer container)
            throws Exception {

        clearTable(SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY);
        clearTable(SCEHMA_NAME, SINK_TABLE1);

        insertSourceTable(SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/oraclecdc_to_oracle_with_custom_primary_key.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(
                                            getSourceQuerySQL(
                                                    SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY)),
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, SINK_TABLE1)));
                        });

        // insert update delete
        updateSourceTable(SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY);

        // stream stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    querySql(
                                            getSourceQuerySQL(
                                                    SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY)),
                                    querySql(getSourceQuerySQL(SCEHMA_NAME, SINK_TABLE1)));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "This case requires obtaining the task health status and manually canceling the canceled task, which is currently only supported by the zeta engine.")
    public void testOracleCdcMetadataTrans(TestContainer container) throws Exception {

        clearTable(SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY);
        clearTable(SCEHMA_NAME, SINK_TABLE1);

        insertSourceTable(SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY);
        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/oraclecdc_to_metadata_trans.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        TimeUnit.SECONDS.sleep(10);
        // insert update delete
        updateSourceTable(SCEHMA_NAME, SOURCE_TABLE_NO_PRIMARY_KEY);
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

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testOracleCdcMultiTableE2e(TestContainer container)
            throws IOException, InterruptedException {

        clearTable(SCEHMA_NAME, SOURCE_TABLE1);
        clearTable(SCEHMA_NAME, SOURCE_TABLE2);
        clearTable(SCEHMA_NAME, SINK_TABLE1);
        clearTable(SCEHMA_NAME, SINK_TABLE2);

        insertSourceTable(SCEHMA_NAME, SOURCE_TABLE1);
        insertSourceTable(SCEHMA_NAME, SOURCE_TABLE2);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/oraclecdc_to_oracle_with_multi_table_mode_two_table.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // stream stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SOURCE_TABLE1)),
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME, SINK_TABLE1))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SOURCE_TABLE2)),
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SINK_TABLE2)))));

        updateSourceTable(SCEHMA_NAME, SOURCE_TABLE1);
        updateSourceTable(SCEHMA_NAME, SOURCE_TABLE2);

        // stream stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SOURCE_TABLE1)),
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME, SINK_TABLE1))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SOURCE_TABLE2)),
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SINK_TABLE2)))));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testMultiTableWithRestore(TestContainer container)
            throws IOException, InterruptedException {

        clearTable(SCEHMA_NAME, SOURCE_TABLE1);
        clearTable(SCEHMA_NAME, SOURCE_TABLE2);
        clearTable(SCEHMA_NAME, SINK_TABLE1);
        clearTable(SCEHMA_NAME, SINK_TABLE2);

        insertSourceTable(SCEHMA_NAME, SOURCE_TABLE1);
        insertSourceTable(SCEHMA_NAME, SOURCE_TABLE2);

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(
                                "/oraclecdc_to_oracle_with_multi_table_mode_one_table.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // stream stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SOURCE_TABLE1)),
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SINK_TABLE1)))));

        // insert update delete
        updateSourceTable(SCEHMA_NAME, SOURCE_TABLE1);

        // stream stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SOURCE_TABLE1)),
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SINK_TABLE1)))));

        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());

        // Restore job with add a new table
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/oraclecdc_to_oracle_with_multi_table_mode_two_table.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // stream stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SOURCE_TABLE1)),
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME, SINK_TABLE1))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SOURCE_TABLE2)),
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SINK_TABLE2)))));

        updateSourceTable(SCEHMA_NAME, SOURCE_TABLE2);

        // stream stage
        await().atMost(600000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SOURCE_TABLE1)),
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME, SINK_TABLE1))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SOURCE_TABLE2)),
                                                        querySql(
                                                                getSourceQuerySQL(
                                                                        SCEHMA_NAME,
                                                                        SINK_TABLE2)))));

        log.info("****************** container logs start ******************");
        String containerLogs = container.getServerLogs();
        log.info(containerLogs);
        Assertions.assertFalse(containerLogs.contains("ERROR"));
        log.info("****************** container logs end ******************");

        clearTable(SCEHMA_NAME, SOURCE_TABLE1);
        clearTable(SCEHMA_NAME, SOURCE_TABLE2);
        clearTable(SCEHMA_NAME, SINK_TABLE1);
        clearTable(SCEHMA_NAME, SINK_TABLE2);
    }

    private List<List<Object>> querySql(String sql) {
        try (Connection connection = getJdbcConnection(ORACLE_CONTAINER);
                Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
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
        try (Connection connection = getJdbcConnection(ORACLE_CONTAINER);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getJdbcConnection(OracleContainer oracleContainer)
            throws SQLException {
        return DriverManager.getConnection(oracleContainer.getJdbcUrl(), SCHEMA_USER, SCHEMA_PWD);
    }

    public static Connection getJdbcConnection(
            OracleContainer oracleContainer, String username, String password) throws SQLException {
        return DriverManager.getConnection(oracleContainer.getJdbcUrl(), username, password);
    }

    private void executeSql(String sql, String username, String password) {
        try (Connection connection = getJdbcConnection(ORACLE_CONTAINER, username, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getSourceQuerySQL(String database, String tableName) {
        return String.format(SOURCE_SQL_TEMPLATE, database, tableName);
    }

    private void insertSourceTable(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES (1, 'vc2', 'vc2', 'nvc2', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 1001, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,TO_DATE('2022-10-30', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-10-30 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-30 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-10-30 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'))");
    }

    private void updateSourceTable(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES (2, 'vc2', 'vc2', 'nvc2', 'c', 'nc',1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,1, 22, 333, 4444, 5555, 1, 99, 2001, 999999999, 999999999999999999,94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,TO_DATE('2022-10-30', 'yyyy-mm-dd'),TO_TIMESTAMP('2022-10-30 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),TO_TIMESTAMP('2022-10-30 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),TO_TIMESTAMP_TZ('2022-10-30 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'))");

        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " VALUES (\n"
                        + "    3, 'vc2', 'vc2', 'nvc2', 'c', 'nc',\n"
                        + "    1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,\n"
                        + "    1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999,\n"
                        + "    94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949,\n"
                        + "    TO_DATE('2022-10-30', 'yyyy-mm-dd'),\n"
                        + "    TO_TIMESTAMP('2022-10-30 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),\n"
                        + "    TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),\n"
                        + "    TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),\n"
                        + "    TO_TIMESTAMP('2022-10-30 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),\n"
                        + "    TO_TIMESTAMP_TZ('2022-10-30 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5')\n"
                        + ")");

        executeSql("DELETE FROM " + database + "." + tableName + " where id = 2");

        executeSql(
                "UPDATE " + database + "." + tableName + " SET VAL_VARCHAR = 'vc3' where id = 3");
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName, SCHEMA_USER, SCHEMA_PWD);
    }
}
