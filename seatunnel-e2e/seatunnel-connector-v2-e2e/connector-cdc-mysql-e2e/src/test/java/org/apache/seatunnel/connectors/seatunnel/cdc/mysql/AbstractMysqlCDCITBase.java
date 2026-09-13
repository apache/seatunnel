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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql;

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
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

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
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

@Slf4j
public abstract class AbstractMysqlCDCITBase extends TestSuiteBase implements TestResource {

    // mysql
    protected static final String MYSQL_HOST = "mysql_cdc_e2e";
    protected static final String MYSQL_USER_NAME = "mysqluser";
    protected static final String MYSQL_USER_PASSWORD = "mysqlpw";
    protected static final String MYSQL_DATABASE = "mysql_cdc";
    private static final String MYSQL_DATABASE2 = "mysql_cdc2";

    private final String QUERY_SQL = "select * from %s.%s";

    // mysql source table query sql
    private static final String SOURCE_SQL_TEMPLATE =
            "select id, cast(f_binary as char) as f_binary, cast(f_blob as char) as f_blob, cast(f_long_varbinary as char) as f_long_varbinary,"
                    + " cast(f_longblob as char) as f_longblob, cast(f_tinyblob as char) as f_tinyblob, cast(f_varbinary as char) as f_varbinary,"
                    + " f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, f_integer_unsigned,"
                    + " f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, f_double_precision, f_longtext, f_mediumtext,"
                    + " f_text, f_tinytext, f_varchar, f_date, f_datetime, f_timestamp, f_bit1, cast(f_bit64 as char) as f_bit64, f_char,"
                    + " f_enum, cast(f_mediumblob as char) as f_mediumblob, f_long_varchar, f_real, f_time, f_tinyint, f_tinyint_unsigned,"
                    + " f_json, f_year from %s.%s";
    // mysql sink table query sql
    private static final String SINK_SQL_TEMPLATE =
            "select id, cast(f_binary as char) as f_binary, cast(f_blob as char) as f_blob, cast(f_long_varbinary as char) as f_long_varbinary,"
                    + " cast(f_longblob as char) as f_longblob, cast(f_tinyblob as char) as f_tinyblob, cast(f_varbinary as char) as f_varbinary,"
                    + " f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, f_integer_unsigned,"
                    + " f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, f_double_precision, f_longtext, f_mediumtext,"
                    + " f_text, f_tinytext, f_varchar, f_date, f_datetime, f_timestamp, f_bit1, cast(f_bit64 as char) as f_bit64, f_char,"
                    + " f_enum, cast(f_mediumblob as char) as f_mediumblob, f_long_varchar, f_real, f_time, f_tinyint, f_tinyint_unsigned,"
                    + " f_json, cast(f_year as year) from %s.%s";

    private static final String SOURCE_TABLE_1 = "mysql_cdc_e2e_source_table";
    private static final String SOURCE_TABLE_2 = "mysql_cdc_e2e_source_table2";
    private static final String SOURCE_TABLE_NO_PRIMARY_KEY =
            "mysql_cdc_e2e_source_table_no_primary_key";

    private static final String SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY =
            "mysql_cdc_e2e_source_table_1_custom_primary_key";
    private static final String SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY =
            "mysql_cdc_e2e_source_table_2_custom_primary_key";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table";
    private static final String SINK_TABLE_COLUMN_INCLUDE =
            "mysql_cdc_e2e_sink_table_column_include";

    private static final String MULTI_DATABASE_A = "mysql_multi_cdc_db_a";
    private static final String MULTI_DATABASE_B = "mysql_multi_cdc_db_b";
    private static final String MULTI_DATABASE_SINK = "mysql_multi_cdc_db_sink";
    private static final String MULTI_DATABASE_TABLE_A = "multi_src_a";
    private static final String MULTI_DATABASE_TABLE_B = "multi_src_b";
    private static final String TIMER_FLUSH_SRC_TABLE = "timer_flush_src";
    private static final String TIMER_FLUSH_SRC_TABLE_2 = "timer_flush_src_2";
    private static final String TIMER_FLUSH_SINK_TABLE = "timer_flush_sink";

    /** Parallel restore scenario that keeps snapshot splitting active during repeated recovery. */
    private static final String PARALLEL_SAVEPOINT_RESTORE_CONF =
            "/mysqlcdc_to_mysql_with_parallel_savepoint_restore.conf";

    protected MySqlContainer MYSQL_CONTAINER;
    protected UniqueDatabase inventoryDatabase;

    protected MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("com.mysql.cj.jdbc.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/MySQL-CDC/lib");

    @BeforeAll
    @Override
    public void startUp() {
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    @TestTemplate
    public void testMysqlCdcCheckDataE2e(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)).toString());
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });

        // insert update delete
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "Heartbeat action query is currently only supported by the zeta engine.")
    public void testMysqlCdcCheckDataE2eWithHeartbeat(TestContainer container)
            throws InterruptedException {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        executeSql(
                "CREATE TABLE IF NOT EXISTS "
                        + MYSQL_DATABASE
                        + ".heartbeat ("
                        + "  ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                        + ");");
        clearTable(MYSQL_DATABASE, "heartbeat");

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_heartbeat.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)).toString());
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });

        // insert update delete
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });

        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<List<Object>> query =
                                    query("SELECT * FROM " + MYSQL_DATABASE + ".heartbeat");
                            Assertions.assertFalse(query.isEmpty());
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "This case requires obtaining the task health status and manually canceling the canceled task, which is currently only supported by the zeta engine.")
    public void testMysqlCdcMetadataTrans(TestContainer container) throws InterruptedException {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE, SINK_TABLE);
        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/mysqlcdc_to_metadata_trans.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
        TimeUnit.SECONDS.sleep(10);
        // insert update delete
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        TimeUnit.SECONDS.sleep(10);
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
    public void testMysqlCdcCheckDataWithDisableExactlyonce(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_disable_exactly_once.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)).toString());
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });

        // insert update delete
        executeSql("DELETE FROM " + MYSQL_DATABASE + "." + SOURCE_TABLE_1);
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });
    }

    @TestTemplate
    public void testMysqlCdcCheckDataWithNoPrimaryKey(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_no_primary_key.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)).toString());
                            Assertions.assertIterableEquals(
                                    query(
                                            getSourceQuerySQL(
                                                    MYSQL_DATABASE, SOURCE_TABLE_NO_PRIMARY_KEY)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });

        // insert update delete
        executeSql("DELETE FROM " + MYSQL_DATABASE + "." + SOURCE_TABLE_NO_PRIMARY_KEY);
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_NO_PRIMARY_KEY);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(
                                            getSourceQuerySQL(
                                                    MYSQL_DATABASE, SOURCE_TABLE_NO_PRIMARY_KEY)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testMysqlCdcMultiTableE2e(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_2);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_2);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/mysqlcdc_to_mysql_with_multi_table_mode_two_table.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // insert update delete
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_2);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_1)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_1))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_2)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_2)))));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testMysqlCdcMultiDatabaseMultiTableE2e(TestContainer container) {
        inventoryDatabase.setTemplateName("mysql_cdc_multi_db").createAndInitialize();

        clearTable(MULTI_DATABASE_SINK, MULTI_DATABASE_TABLE_A);
        clearTable(MULTI_DATABASE_SINK, MULTI_DATABASE_TABLE_B);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_multi_db_multi_table.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot phase
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MULTI_DATABASE_A,
                                                                        MULTI_DATABASE_TABLE_A)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MULTI_DATABASE_SINK,
                                                                        MULTI_DATABASE_TABLE_A))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MULTI_DATABASE_B,
                                                                        MULTI_DATABASE_TABLE_B)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MULTI_DATABASE_SINK,
                                                                        MULTI_DATABASE_TABLE_B)))));

        // incremental phase
        upsertDeleteSourceTable(MULTI_DATABASE_A, MULTI_DATABASE_TABLE_A);
        upsertDeleteSourceTable(MULTI_DATABASE_B, MULTI_DATABASE_TABLE_B);

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MULTI_DATABASE_A,
                                                                        MULTI_DATABASE_TABLE_A)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MULTI_DATABASE_SINK,
                                                                        MULTI_DATABASE_TABLE_A))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MULTI_DATABASE_B,
                                                                        MULTI_DATABASE_TABLE_B)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MULTI_DATABASE_SINK,
                                                                        MULTI_DATABASE_TABLE_B)))));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testMultiDatabaseWithRestore(TestContainer container)
            throws IOException, InterruptedException {

        inventoryDatabase.setTemplateName("mysql_cdc_multi_db").createAndInitialize();

        clearTable(MULTI_DATABASE_SINK, MULTI_DATABASE_TABLE_B);
        clearTable(MULTI_DATABASE_SINK, MULTI_DATABASE_TABLE_A);

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(
                                "/mysqlcdc_to_mysql_with_multi_db_multi_table.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // wait for snapshot data
        await().atMost(100000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertTrue(
                                                        query(
                                                                                getSourceQuerySQL(
                                                                                        MULTI_DATABASE_SINK,
                                                                                        MULTI_DATABASE_TABLE_A))
                                                                        .size()
                                                                > 1),
                                        () ->
                                                Assertions.assertTrue(
                                                        query(
                                                                                getSourceQuerySQL(
                                                                                        MULTI_DATABASE_SINK,
                                                                                        MULTI_DATABASE_TABLE_B))
                                                                        .size()
                                                                > 1)));

        // savepoint + restore
        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/mysqlcdc_to_mysql_with_multi_db_multi_table.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // incremental changes after restore
        upsertDeleteSourceTable(MULTI_DATABASE_A, MULTI_DATABASE_TABLE_A);
        upsertDeleteSourceTable(MULTI_DATABASE_B, MULTI_DATABASE_TABLE_B);

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .until(() -> getConnectionStatus("st_user_source").size() == 1);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .until(() -> getConnectionStatus("st_user_sink").size() == 1);

        await().atMost(300000, TimeUnit.MILLISECONDS)
                .pollInterval(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MULTI_DATABASE_A,
                                                                        MULTI_DATABASE_TABLE_A)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MULTI_DATABASE_SINK,
                                                                        MULTI_DATABASE_TABLE_A))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MULTI_DATABASE_B,
                                                                        MULTI_DATABASE_TABLE_B)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MULTI_DATABASE_SINK,
                                                                        MULTI_DATABASE_TABLE_B)))));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testMultiTableWithRestore(TestContainer container)
            throws IOException, InterruptedException {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_2);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_2);

        // init
        initSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(
                                "/mysqlcdc_to_mysql_with_multi_table_mode_one_table.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // wait for data written to sink
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        query(getSourceQuerySQL(MYSQL_DATABASE2, SOURCE_TABLE_1))
                                                        .size()
                                                > 1));

        // Restore job with snapshot read phase
        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/mysqlcdc_to_mysql_with_multi_table_mode_one_table.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // insert update delete
        changeSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1);

        // stream stage
        await().atMost(300000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE_1)),
                                        query(getSourceQuerySQL(MYSQL_DATABASE2, SOURCE_TABLE_1))));
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .until(() -> getConnectionStatus("st_user_source").size() == 1);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .until(() -> getConnectionStatus("st_user_sink").size() == 1);

        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());

        // Restore job with add a new table
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/mysqlcdc_to_mysql_with_multi_table_mode_two_table.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_2);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_1)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_1))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_2)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_2)))));

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .until(() -> getConnectionStatus("st_user_source").size() == 1);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .until(() -> getConnectionStatus("st_user_sink").size() == 1);

        log.info("****************** container logs start ******************");
        String containerLogs = container.getServerLogs();
        log.info(containerLogs);
        Assertions.assertFalse(containerLogs.contains("ERROR"));
        log.info("****************** container logs end ******************");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support savepoint restore")
    public void testMysqlCdcParallelSnapshotRestoreAcrossMultipleRounds(TestContainer container)
            throws Exception {
        Long sourceJobId = JobIdGenerator.newJobId();
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_1);
        insertParallelRestoreProbeRows(MYSQL_DATABASE, SOURCE_TABLE_1, 1, 24);

        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return container.executeJob(
                                    PARALLEL_SAVEPOINT_RESTORE_CONF, String.valueOf(sourceJobId));
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    });

            awaitSourceAndMirrorSinkConsistent(MYSQL_DATABASE, MYSQL_DATABASE2, SOURCE_TABLE_1);
            awaitJobRunning(container, sourceJobId);
            Assertions.assertEquals(
                    0, container.savepointJob(String.valueOf(sourceJobId)).getExitCode());

            applyParallelRestoreProbeMutations(MYSQL_DATABASE, SOURCE_TABLE_1, 101, 1, 2);

            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.restoreJob(
                                    PARALLEL_SAVEPOINT_RESTORE_CONF, String.valueOf(sourceJobId));
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            awaitSourceAndMirrorSinkConsistent(MYSQL_DATABASE, MYSQL_DATABASE2, SOURCE_TABLE_1);
            awaitJobRunning(container, sourceJobId);
            Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE2, SOURCE_TABLE_1, 101));
            Assertions.assertEquals(0L, getRowCountById(MYSQL_DATABASE2, SOURCE_TABLE_1, 2));
            Assertions.assertEquals(
                    0, container.savepointJob(String.valueOf(sourceJobId)).getExitCode());

            applyParallelRestoreProbeMutations(MYSQL_DATABASE, SOURCE_TABLE_1, 201, 3, 4);

            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            container.restoreJob(
                                    PARALLEL_SAVEPOINT_RESTORE_CONF, String.valueOf(sourceJobId));
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });

            awaitSourceAndMirrorSinkConsistent(MYSQL_DATABASE, MYSQL_DATABASE2, SOURCE_TABLE_1);
            awaitJobRunning(container, sourceJobId);
            Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE2, SOURCE_TABLE_1, 201));
            Assertions.assertEquals(0L, getRowCountById(MYSQL_DATABASE2, SOURCE_TABLE_1, 4));
        } finally {
            cancelJobIfRunning(container, sourceJobId);
            clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
            clearTable(MYSQL_DATABASE2, SOURCE_TABLE_1);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testMysqlCdcMultiTableWithCustomPrimaryKey(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY);
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY);
        clearTable(MYSQL_DATABASE2, SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_custom_primary_key.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // insert update delete
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY);
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY);

        // stream stage
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .pollInterval(2000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_1_CUSTOM_PRIMARY_KEY))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY)),
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        SOURCE_TABLE_2_CUSTOM_PRIMARY_KEY)))));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Currently SPARK do not support cdc")
    public void testMysqlCdcByWildcardsConfig(TestContainer container)
            throws IOException, InterruptedException {
        inventoryDatabase.setTemplateName("wildcards").createAndInitialize();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_wildcards_to_mysql.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
        TimeUnit.SECONDS.sleep(5);
        inventoryDatabase.setTemplateName("wildcards_dml").createAndInitialize();
        given().pollDelay(20, TimeUnit.SECONDS)
                .pollInterval(2000, TimeUnit.MILLISECONDS)
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertAll(
                                    () -> {
                                        log.info(
                                                query(getQuerySQL("sink", "source_products"))
                                                        .toString());
                                        Assertions.assertIterableEquals(
                                                query(getQuerySQL("source", "products")),
                                                query(getQuerySQL("sink", "source_products")));
                                    },
                                    () -> {
                                        log.info(
                                                query(getQuerySQL("sink", "source_customers"))
                                                        .toString());
                                        Assertions.assertIterableEquals(
                                                query(getQuerySQL("source", "customers")),
                                                query(getQuerySQL("sink", "source_customers")));
                                    },
                                    () -> {
                                        log.info(
                                                query(getQuerySQL("sink", "source1_orders"))
                                                        .toString());
                                        Assertions.assertIterableEquals(
                                                query(getQuerySQL("source1", "orders")),
                                                query(getQuerySQL("sink", "source1_orders")));
                                    });
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "engine-level timer flush (sink.flush.interval) is only supported on Zeta engine")
    public void testJdbcSinkTimerFlushEnabled(TestContainer container) throws Exception {
        inventoryDatabase.setTemplateName("timer_flush").createAndInitialize();
        clearTable(MYSQL_DATABASE, TIMER_FLUSH_SINK_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_timer_flush.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot phase: wait for initial rows to arrive in sink
        await().atMost(60, TimeUnit.SECONDS)
                .pollInterval(3, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        query(
                                                                getSinkQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        TIMER_FLUSH_SINK_TABLE))
                                                        .size()
                                                > 0));

        // Insert 100 rows, verify every 10 rows are flushed to sink by engine timer
        int startId = 10;
        int totalRows = 100;
        int batchSize = 10;
        for (int i = 0; i < totalRows; i++) {
            insertTimerFlushRow(MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE, startId + i);
            if ((i + 1) % batchSize == 0) {
                final int checkUpToId = startId + i;
                await().atMost(30, TimeUnit.SECONDS)
                        .pollInterval(1, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        Assertions.assertEquals(
                                                checkUpToId - startId + 1,
                                                query(
                                                                String.format(
                                                                        "select id from %s.%s where id >= %d and id <= %d",
                                                                        MYSQL_DATABASE,
                                                                        TIMER_FLUSH_SINK_TABLE,
                                                                        startId,
                                                                        checkUpToId))
                                                        .size(),
                                                "expected "
                                                        + (checkUpToId - startId + 1)
                                                        + " rows flushed up to id "
                                                        + checkUpToId));
            }
        }

        // final consistency check
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                getSourceQuerySQL(
                                                        MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE)),
                                        query(
                                                getSinkQuerySQL(
                                                        MYSQL_DATABASE, TIMER_FLUSH_SINK_TABLE))));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "engine-level timer flush and savepoint/restore are only supported on Zeta engine")
    public void testJdbcSinkTimerFlushRestore(TestContainer container) throws Exception {
        inventoryDatabase.setTemplateName("timer_flush").createAndInitialize();
        clearTable(MYSQL_DATABASE, TIMER_FLUSH_SINK_TABLE);

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(
                                "/mysqlcdc_to_mysql_with_timer_flush_restore.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // snapshot phase
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .pollInterval(1000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        query(getQuerySQL(MYSQL_DATABASE, TIMER_FLUSH_SINK_TABLE))
                                                        .size()
                                                > 0));

        // phase 1: insert 100 rows before savepoint, verify every 10 rows
        int phase1Start = 10;
        int phase1Total = 100;
        int batchSize = 10;
        for (int i = 0; i < phase1Total; i++) {
            insertTimerFlushRow(MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE, phase1Start + i);
            if ((i + 1) % batchSize == 0) {
                final int checkUpToId = phase1Start + i;
                await().atMost(30, TimeUnit.SECONDS)
                        .pollInterval(1, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        Assertions.assertEquals(
                                                checkUpToId - phase1Start + 1,
                                                query(
                                                                String.format(
                                                                        "select id from %s.%s where id >= %d and id <= %d",
                                                                        MYSQL_DATABASE,
                                                                        TIMER_FLUSH_SINK_TABLE,
                                                                        phase1Start,
                                                                        checkUpToId))
                                                        .size(),
                                                "expected "
                                                        + (checkUpToId - phase1Start + 1)
                                                        + " rows flushed up to id "
                                                        + checkUpToId));
            }
        }

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                getSourceQuerySQL(
                                                        MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE)),
                                        query(
                                                getSinkQuerySQL(
                                                        MYSQL_DATABASE, TIMER_FLUSH_SINK_TABLE))));

        // savepoint + restore
        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/mysqlcdc_to_mysql_with_timer_flush_restore.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // phase 2: insert 100 rows after restore, verify every 10 rows
        int phase2Start = 200;
        int phase2Total = 100;
        for (int i = 0; i < phase2Total; i++) {
            insertTimerFlushRow(MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE, phase2Start + i);
            if ((i + 1) % batchSize == 0) {
                final int checkUpToId = phase2Start + i;
                await().atMost(30, TimeUnit.SECONDS)
                        .pollInterval(1, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        Assertions.assertEquals(
                                                checkUpToId - phase2Start + 1,
                                                query(
                                                                String.format(
                                                                        "select id from %s.%s where id >= %d and id <= %d",
                                                                        MYSQL_DATABASE,
                                                                        TIMER_FLUSH_SINK_TABLE,
                                                                        phase2Start,
                                                                        checkUpToId))
                                                        .size(),
                                                "expected "
                                                        + (checkUpToId - phase2Start + 1)
                                                        + " rows flushed up to id "
                                                        + checkUpToId
                                                        + " after restore"));
            }
        }

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                getSourceQuerySQL(
                                                        MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE)),
                                        query(
                                                getSinkQuerySQL(
                                                        MYSQL_DATABASE, TIMER_FLUSH_SINK_TABLE))));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "engine-level timer flush (sink.flush.interval) is only supported on Zeta engine")
    public void testJdbcSinkTimerFlushMultiTable(TestContainer container) throws Exception {
        inventoryDatabase.setTemplateName("multi_timer_flush").createAndInitialize();
        clearTable(MYSQL_DATABASE2, TIMER_FLUSH_SRC_TABLE);
        clearTable(MYSQL_DATABASE2, TIMER_FLUSH_SRC_TABLE_2);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/mysqlcdc_to_mysql_with_timer_flush_multi_table.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot phase: both source tables should be flushed to sink database (mysql_cdc2)
        await().atMost(60, TimeUnit.SECONDS)
                .pollInterval(3, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertTrue(
                                                        query(
                                                                                getSourceQuerySQL(
                                                                                        MYSQL_DATABASE2,
                                                                                        TIMER_FLUSH_SRC_TABLE))
                                                                        .size()
                                                                > 0,
                                                        "timer_flush_src should be flushed to mysql_cdc2"),
                                        () ->
                                                Assertions.assertTrue(
                                                        query(
                                                                                getSourceQuerySQL(
                                                                                        MYSQL_DATABASE2,
                                                                                        TIMER_FLUSH_SRC_TABLE_2))
                                                                        .size()
                                                                > 0,
                                                        "timer_flush_src_2 should be flushed to mysql_cdc2")));

        // incremental phase: insert 100 rows into each source table, verify every 10 rows
        int startId = 10;
        int totalRows = 100;
        int batchSize = 10;
        for (int i = 0; i < totalRows; i++) {
            int id = startId + i;
            insertTimerFlushRow(MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE, id);
            insertTimerFlushRow(MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE_2, id);

            if ((i + 1) % batchSize == 0) {
                final int checkUpToId = id;
                await().atMost(30, TimeUnit.SECONDS)
                        .pollInterval(1, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        Assertions.assertAll(
                                                () ->
                                                        Assertions.assertEquals(
                                                                checkUpToId - startId + 1,
                                                                query(
                                                                                String.format(
                                                                                        "select id from %s.%s where id >= %d and id <= %d",
                                                                                        MYSQL_DATABASE2,
                                                                                        TIMER_FLUSH_SRC_TABLE,
                                                                                        startId,
                                                                                        checkUpToId))
                                                                        .size(),
                                                                "timer_flush_src: expected "
                                                                        + (checkUpToId
                                                                                - startId
                                                                                + 1)
                                                                        + " rows flushed up to id "
                                                                        + checkUpToId),
                                                () ->
                                                        Assertions.assertEquals(
                                                                checkUpToId - startId + 1,
                                                                query(
                                                                                String.format(
                                                                                        "select id from %s.%s where id >= %d and id <= %d",
                                                                                        MYSQL_DATABASE2,
                                                                                        TIMER_FLUSH_SRC_TABLE_2,
                                                                                        startId,
                                                                                        checkUpToId))
                                                                        .size(),
                                                                "timer_flush_src_2: expected "
                                                                        + (checkUpToId
                                                                                - startId
                                                                                + 1)
                                                                        + " rows flushed up to id "
                                                                        + checkUpToId)));
            }
        }

        // final consistency: source and sink should have identical data for both tables
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        TIMER_FLUSH_SRC_TABLE)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        TIMER_FLUSH_SRC_TABLE))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        TIMER_FLUSH_SRC_TABLE_2)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        TIMER_FLUSH_SRC_TABLE_2)))));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "engine-level timer flush and savepoint/restore are only supported on Zeta engine")
    public void testJdbcSinkTimerFlushMultiTableRestore(TestContainer container) throws Exception {
        inventoryDatabase.setTemplateName("multi_timer_flush").createAndInitialize();
        clearTable(MYSQL_DATABASE2, TIMER_FLUSH_SRC_TABLE);
        clearTable(MYSQL_DATABASE2, TIMER_FLUSH_SRC_TABLE_2);

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(
                                "/mysqlcdc_to_mysql_with_timer_flush_multi_table_restore.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // snapshot phase: wait for initial rows to arrive in both sink tables
        await().atMost(60, TimeUnit.SECONDS)
                .pollInterval(3, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertTrue(
                                                        query(
                                                                                getSourceQuerySQL(
                                                                                        MYSQL_DATABASE2,
                                                                                        TIMER_FLUSH_SRC_TABLE))
                                                                        .size()
                                                                > 0,
                                                        "timer_flush_src should be flushed to mysql_cdc2"),
                                        () ->
                                                Assertions.assertTrue(
                                                        query(
                                                                                getSourceQuerySQL(
                                                                                        MYSQL_DATABASE2,
                                                                                        TIMER_FLUSH_SRC_TABLE_2))
                                                                        .size()
                                                                > 0,
                                                        "timer_flush_src_2 should be flushed to mysql_cdc2")));

        // phase 1: insert 100 rows before savepoint, verify every 10 rows
        int phase1StartId = 10;
        int totalRows = 100;
        int batchSize = 10;
        for (int i = 0; i < totalRows; i++) {
            int id = phase1StartId + i;
            insertTimerFlushRow(MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE, id);
            insertTimerFlushRow(MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE_2, id);

            if ((i + 1) % batchSize == 0) {
                final int checkUpToId = id;
                await().atMost(30, TimeUnit.SECONDS)
                        .pollInterval(1, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        Assertions.assertAll(
                                                () ->
                                                        Assertions.assertEquals(
                                                                checkUpToId - phase1StartId + 1,
                                                                query(
                                                                                String.format(
                                                                                        "select id from %s.%s where id >= %d and id <= %d",
                                                                                        MYSQL_DATABASE2,
                                                                                        TIMER_FLUSH_SRC_TABLE,
                                                                                        phase1StartId,
                                                                                        checkUpToId))
                                                                        .size(),
                                                                "timer_flush_src: expected "
                                                                        + (checkUpToId
                                                                                - phase1StartId
                                                                                + 1)
                                                                        + " rows flushed up to id "
                                                                        + checkUpToId),
                                                () ->
                                                        Assertions.assertEquals(
                                                                checkUpToId - phase1StartId + 1,
                                                                query(
                                                                                String.format(
                                                                                        "select id from %s.%s where id >= %d and id <= %d",
                                                                                        MYSQL_DATABASE2,
                                                                                        TIMER_FLUSH_SRC_TABLE_2,
                                                                                        phase1StartId,
                                                                                        checkUpToId))
                                                                        .size(),
                                                                "timer_flush_src_2: expected "
                                                                        + (checkUpToId
                                                                                - phase1StartId
                                                                                + 1)
                                                                        + " rows flushed up to id "
                                                                        + checkUpToId)));
            }
        }

        // consistency check before savepoint
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        TIMER_FLUSH_SRC_TABLE)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        TIMER_FLUSH_SRC_TABLE))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        TIMER_FLUSH_SRC_TABLE_2)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        TIMER_FLUSH_SRC_TABLE_2)))));

        // savepoint + restore
        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/mysqlcdc_to_mysql_with_timer_flush_multi_table_restore.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        await().atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "RUNNING", container.getJobStatus(String.valueOf(jobId))));

        // phase 2: insert 100 rows after restore, verify every 10 rows
        int phase2StartId = phase1StartId + totalRows + 100;
        for (int i = 0; i < totalRows; i++) {
            int id = phase2StartId + i;
            insertTimerFlushRow(MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE, id);
            insertTimerFlushRow(MYSQL_DATABASE, TIMER_FLUSH_SRC_TABLE_2, id);

            if ((i + 1) % batchSize == 0) {
                final int checkUpToId = id;
                await().atMost(30, TimeUnit.SECONDS)
                        .pollInterval(1, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        Assertions.assertAll(
                                                () ->
                                                        Assertions.assertEquals(
                                                                checkUpToId - phase2StartId + 1,
                                                                query(
                                                                                String.format(
                                                                                        "select id from %s.%s where id >= %d and id <= %d",
                                                                                        MYSQL_DATABASE2,
                                                                                        TIMER_FLUSH_SRC_TABLE,
                                                                                        phase2StartId,
                                                                                        checkUpToId))
                                                                        .size(),
                                                                "timer_flush_src after restore: expected "
                                                                        + (checkUpToId
                                                                                - phase2StartId
                                                                                + 1)
                                                                        + " rows flushed up to id "
                                                                        + checkUpToId),
                                                () ->
                                                        Assertions.assertEquals(
                                                                checkUpToId - phase2StartId + 1,
                                                                query(
                                                                                String.format(
                                                                                        "select id from %s.%s where id >= %d and id <= %d",
                                                                                        MYSQL_DATABASE2,
                                                                                        TIMER_FLUSH_SRC_TABLE_2,
                                                                                        phase2StartId,
                                                                                        checkUpToId))
                                                                        .size(),
                                                                "timer_flush_src_2 after restore: expected "
                                                                        + (checkUpToId
                                                                                - phase2StartId
                                                                                + 1)
                                                                        + " rows flushed up to id "
                                                                        + checkUpToId)));
            }
        }

        // final consistency check after restore
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertAll(
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        TIMER_FLUSH_SRC_TABLE)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        TIMER_FLUSH_SRC_TABLE))),
                                        () ->
                                                Assertions.assertIterableEquals(
                                                        query(
                                                                getSourceQuerySQL(
                                                                        MYSQL_DATABASE,
                                                                        TIMER_FLUSH_SRC_TABLE_2)),
                                                        query(
                                                                getSinkQuerySQL(
                                                                        MYSQL_DATABASE2,
                                                                        TIMER_FLUSH_SRC_TABLE_2)))));
    }

    @TestTemplate
    public void testMysqlCdcWithColumnIncludeList(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE_1);
        clearTable(MYSQL_DATABASE, SINK_TABLE_COLUMN_INCLUDE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_column_include_list.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(
                                    query(
                                                    getColumnIncludeQuerySQL(
                                                            MYSQL_DATABASE,
                                                            SINK_TABLE_COLUMN_INCLUDE))
                                            .toString());
                            Assertions.assertIterableEquals(
                                    query(
                                            getColumnIncludeSourceQuerySQL(
                                                    MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(
                                            getColumnIncludeQuerySQL(
                                                    MYSQL_DATABASE, SINK_TABLE_COLUMN_INCLUDE)));
                        });

        // insert update delete
        upsertDeleteSourceTableColumnInclude(MYSQL_DATABASE, SOURCE_TABLE_1);

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(
                                            getColumnIncludeSourceQuerySQL(
                                                    MYSQL_DATABASE, SOURCE_TABLE_1)),
                                    query(
                                            getColumnIncludeQuerySQL(
                                                    MYSQL_DATABASE, SINK_TABLE_COLUMN_INCLUDE)));
                        });
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    private List<List<Object>> getConnectionStatus(String user) {
        return query(
                "select USER,HOST,DB,COMMAND,TIME,STATE from information_schema.processlist where USER = '"
                        + user
                        + "'");
    }

    private List<List<Object>> query(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
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

    // Execute SQL
    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void initSourceTable(String database, String tableName) {
        for (int i = 1; i < 100; i++) {
            executeSql(
                    "INSERT INTO "
                            + database
                            + "."
                            + tableName
                            + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                            + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                            + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                            + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                            + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                            + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                            + "VALUES ( "
                            + i
                            + ", 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                            + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                            + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                            + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                            + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                            + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                            + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                            + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 )");
        }
    }

    private void changeSourceTable(String database, String tableName) {
        for (int i = 100; i < 110; i++) {
            executeSql(
                    "INSERT INTO "
                            + database
                            + "."
                            + tableName
                            + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                            + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                            + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                            + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                            + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                            + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                            + "VALUES ( "
                            + i
                            + ", 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                            + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                            + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                            + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                            + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                            + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                            + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                            + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 )");
        }

        executeSql("DELETE FROM " + database + "." + tableName + " where id > 100");

        executeSql("UPDATE " + database + "." + tableName + " SET f_bigint = 10000 where id < 10");
    }

    private void upsertDeleteSourceTable(String database, String tableName) {

        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( 5, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 )");
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( 6, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1999 )");
        executeSql("DELETE FROM " + database + "." + tableName + " where id = 2");

        executeSql("UPDATE " + database + "." + tableName + " SET f_bigint = 10000 where id = 3");
    }

    /** Seeds enough rows to force multiple snapshot splits before repeated restore rounds. */
    private void insertParallelRestoreProbeRows(
            String database, String tableName, int startInclusive, int endInclusive) {
        for (int id = startInclusive; id <= endInclusive; id++) {
            insertParallelRestoreProbeRow(database, tableName, id);
        }
    }

    /** Applies a mixed insert, update, and delete batch that must survive the next restore. */
    private void applyParallelRestoreProbeMutations(
            String database, String tableName, int batchStart, int updatedId, int deletedId) {
        insertParallelRestoreProbeRows(database, tableName, batchStart, batchStart + 4);
        executeSql(
                "UPDATE "
                        + database
                        + "."
                        + tableName
                        + " SET f_bigint = "
                        + (900000000L + batchStart)
                        + ", f_varchar = 'restore-round-"
                        + batchStart
                        + "' WHERE id = "
                        + updatedId);
        executeSql("DELETE FROM " + database + "." + tableName + " WHERE id = " + deletedId);
    }

    /** Inserts a compact probe row that is stable across repeated savepoint restores. */
    private void insertParallelRestoreProbeRow(String database, String tableName, int id) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " (id, f_smallint, f_int, f_bigint, f_varchar, f_date, f_datetime, f_timestamp, f_tinyint, f_json, f_year)"
                        + " VALUES ("
                        + id
                        + ", "
                        + id
                        + ", "
                        + id
                        + ", "
                        + (100000L + id)
                        + ", 'parallel-savepoint-"
                        + id
                        + "', '2024-01-01', '2024-01-01 00:00:00', '2024-01-01 00:00:00', 1, '{\"probe\":"
                        + id
                        + "}', 2024)");
    }

    /** Waits until the source table and the generated mirror sink contain the same ordered rows. */
    private void awaitSourceAndMirrorSinkConsistent(
            String sourceDatabase, String sinkDatabase, String tableName) {
        await().atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(getOrderedSourceQuerySQL(sourceDatabase, tableName)),
                                        query(getOrderedSourceQuerySQL(sinkDatabase, tableName))));
    }

    /** Waits until the restored Zeta job is reported as running again. */
    private void awaitJobRunning(TestContainer container, long jobId) {
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "RUNNING", container.getJobStatus(String.valueOf(jobId))));
    }

    /** Cancels the current restore round when the job is still running during cleanup. */
    private void cancelJobIfRunning(TestContainer container, long jobId) {
        try {
            if ("RUNNING".equals(container.getJobStatus(String.valueOf(jobId)))) {
                Assertions.assertEquals(
                        0, container.cancelJob(String.valueOf(jobId)).getExitCode());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            log.warn("Ignore cleanup failure for job {}", jobId, e);
        }
    }

    /** Counts a specific id in the mirror sink to detect dropped or duplicated change replay. */
    private long getRowCountById(String database, String tableName, int id) {
        List<List<Object>> result =
                query("select count(*) from " + database + "." + tableName + " where id = " + id);
        Object value = result.get(0).get(0);
        return value instanceof Number
                ? ((Number) value).longValue()
                : Long.parseLong(String.valueOf(value));
    }

    @Override
    @AfterAll
    public void tearDown() {
        // close Container
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    private void insertTimerFlushRow(String database, String tableName, int id) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " (id, f_smallint, f_int, f_bigint, f_varchar, f_date, f_datetime, f_timestamp, f_tinyint, f_json, f_year)"
                        + " VALUES ("
                        + id
                        + ", 1, 1, 1, 'timer-probe', '2024-01-01', '2024-01-01 00:00:00', '2024-01-01 00:00:00', 1, '{\"probe\":"
                        + id
                        + "}', 2024)");
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }

    private String getSourceQuerySQL(String database, String tableName) {
        return String.format(SOURCE_SQL_TEMPLATE, database, tableName);
    }

    /** Applies deterministic ordering so repeated restore assertions compare a stable snapshot. */
    private String getOrderedSourceQuerySQL(String database, String tableName) {
        return getSourceQuerySQL(database, tableName) + " order by id";
    }

    private String getSinkQuerySQL(String database, String tableName) {
        return String.format(SINK_SQL_TEMPLATE, database, tableName);
    }

    private String getQuerySQL(String database, String tableName) {
        return String.format(QUERY_SQL, database, tableName);
    }

    // Query SQL for column include list test (selecting only 10 specific columns)
    private static final String COLUMN_INCLUDE_SOURCE_SQL_TEMPLATE =
            "select id, cast(f_binary as char) as f_binary, cast(f_blob as char) as f_blob, cast(f_long_varbinary as char) as f_long_varbinary,"
                    + " cast(f_varbinary as char) as f_varbinary, f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int from %s.%s";

    private static final String COLUMN_INCLUDE_SINK_SQL_TEMPLATE =
            "select id, cast(f_binary as char) as f_binary, cast(f_blob as char) as f_blob, cast(f_long_varbinary as char) as f_long_varbinary,"
                    + " cast(f_varbinary as char) as f_varbinary, f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int from %s.%s";

    private String getColumnIncludeSourceQuerySQL(String database, String tableName) {
        return String.format(COLUMN_INCLUDE_SOURCE_SQL_TEMPLATE, database, tableName);
    }

    private String getColumnIncludeQuerySQL(String database, String tableName) {
        return String.format(COLUMN_INCLUDE_SINK_SQL_TEMPLATE, database, tableName);
    }

    private void upsertDeleteSourceTableColumnInclude(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( 5, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 )");
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( 6, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1999 )");
        executeSql("DELETE FROM " + database + "." + tableName + " where id = 2");
        executeSql("UPDATE " + database + "." + tableName + " SET f_bigint = 10000 where id = 3");
    }
}
