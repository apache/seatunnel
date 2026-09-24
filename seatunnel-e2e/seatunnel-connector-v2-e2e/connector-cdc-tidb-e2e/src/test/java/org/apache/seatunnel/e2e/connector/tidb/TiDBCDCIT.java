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

package org.apache.seatunnel.e2e.connector.tidb;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestResource;
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
import org.tikv.common.TiSession;

import com.mysql.cj.jdbc.Driver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class TiDBCDCIT extends TiDBTestBase implements TestResource {

    private static final String TIDB_DATABASE = "tidb_cdc";
    private static final String SOURCE_TABLE = "tidb_cdc_e2e_source_table";
    private static final String SINK_TABLE = "tidb_cdc_e2e_sink_table";
    private static final String SOURCE_TABLE_NO_PRIMARY_KEY =
            "tidb_cdc_e2e_source_table_no_primary_key";
    private static final String SOURCE_TABLE_2 = "tidb_cdc_e2e_source_table_2";
    private static final String SOURCE_TABLE_3 = "tidb_cdc_e2e_source_table_3";
    private static final String TIDB_SINK_DATABASE = "tidb_cdc_sink";

    private static final String SIMPLE_SQL_TEMPLATE = "select id, name, val from %s.%s";

    // tidb source table query sql
    private static final String SOURCE_SQL_TEMPLATE =
            "select id, f_binary, f_blob,  f_long_varbinary,"
                    + "  f_longblob, f_tinyblob, f_varbinary,"
                    + " f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, f_integer_unsigned,"
                    + " f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, f_double_precision, f_longtext, f_mediumtext,"
                    + " f_text, f_tinytext, f_varchar, f_date, f_datetime, f_timestamp, f_bit1,  f_bit64, f_char,"
                    + " f_enum,  f_mediumblob, f_long_varchar, f_real, f_time, f_tinyint, f_tinyint_unsigned,"
                    + " f_json, f_year from %s.%s";
    // tidb sink table query sql
    private static final String SINK_SQL_TEMPLATE =
            "select id, f_binary,  f_blob, f_long_varbinary,"
                    + " f_longblob, f_tinyblob, f_varbinary,"
                    + " f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, f_integer_unsigned,"
                    + " f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, f_double_precision, f_longtext, f_mediumtext,"
                    + " f_text, f_tinytext, f_varchar, f_date, f_datetime, f_timestamp, f_bit1, f_bit64, f_char,"
                    + " f_enum, f_mediumblob, f_long_varchar, f_real, f_time, f_tinyint, f_tinyint_unsigned,"
                    + " f_json, cast(f_year as year) from %s.%s";
    private static final String TIDB_CDC_PLUGIN_LIB = "/tmp/seatunnel/plugins/TiDB-CDC/lib";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar.of(Driver.class).copyTo(container, TIDB_CDC_PLUGIN_LIB);
                DependencyJar.of(TiSession.class).copyTo(container, TIDB_CDC_PLUGIN_LIB);
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        startContainers();
        initializeTidbTable("tidb_cdc");
    }

    @TestTemplate
    public void testTiDBCdcCheckDataE2e(TestContainer container) throws Exception {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(TIDB_DATABASE, SOURCE_TABLE);
        clearTable(TIDB_DATABASE, SINK_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/tidb/tidbcdc_to_tidb.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(query(getSinkQuerySQL(TIDB_DATABASE, SINK_TABLE)).toString());
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(getSourceQuerySQL(TIDB_DATABASE, SOURCE_TABLE))),
                                    JsonUtils.toJsonString(
                                            query(getSinkQuerySQL(TIDB_DATABASE, SINK_TABLE))));
                        });

        // insert update delete
        upsertDeleteSourceTable(TIDB_DATABASE, SOURCE_TABLE);

        // stream stage
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(getSourceQuerySQL(TIDB_DATABASE, SOURCE_TABLE))),
                                    JsonUtils.toJsonString(
                                            query(getSinkQuerySQL(TIDB_DATABASE, SINK_TABLE))));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testMultiTableWithRestore(TestContainer container)
            throws IOException, InterruptedException {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(TIDB_DATABASE, SOURCE_TABLE);
        clearTable(TIDB_DATABASE, SINK_TABLE);
        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/tidb/tidbcdc_to_tidb.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        // insert update delete
        upsertDeleteSourceTable(TIDB_DATABASE, SOURCE_TABLE);

        // stream stage
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(getSourceQuerySQL(TIDB_DATABASE, SOURCE_TABLE))),
                                    JsonUtils.toJsonString(
                                            query(getSinkQuerySQL(TIDB_DATABASE, SINK_TABLE))));
                        });

        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());

        // Restore job
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob("/tidb/tidbcdc_to_tidb.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        upsertDeleteSourceTableForRestore(TIDB_DATABASE, SOURCE_TABLE);
        // stream stage
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(getSourceQuerySQL(TIDB_DATABASE, SOURCE_TABLE))),
                                    JsonUtils.toJsonString(
                                            query(getSinkQuerySQL(TIDB_DATABASE, SINK_TABLE))));
                        });

        log.info("****************** container logs start ******************");
        String containerLogs = container.getServerLogs();
        log.info(containerLogs);
        Assertions.assertFalse(containerLogs.contains("ERROR"));
        log.info("****************** container logs end ******************");
    }

    @TestTemplate
    public void testTiDBCdcCheckDataWithDisableExactlyonce(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(TIDB_DATABASE, SINK_TABLE);
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/tidb/tidbcdc_to_tidb_with_disable_exactly_once.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(query(getSinkQuerySQL(TIDB_DATABASE, SINK_TABLE)).toString());
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(getSourceQuerySQL(TIDB_DATABASE, SOURCE_TABLE))),
                                    JsonUtils.toJsonString(
                                            query(getSinkQuerySQL(TIDB_DATABASE, SINK_TABLE))));
                        });

        // insert update delete
        executeSql("DELETE FROM " + TIDB_DATABASE + "." + SOURCE_TABLE);
        upsertDeleteSourceTable(TIDB_DATABASE, SOURCE_TABLE);

        // stream stage
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(getSourceQuerySQL(TIDB_DATABASE, SOURCE_TABLE))),
                                    JsonUtils.toJsonString(
                                            query(getSinkQuerySQL(TIDB_DATABASE, SINK_TABLE))));
                        });
    }

    @TestTemplate
    public void testMysqlCdcCheckDataWithNoPrimaryKey(TestContainer container) {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(TIDB_DATABASE, SINK_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/tidb/tidbcdc_to_tidb_with_no_primary_key.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            log.info(query(getSinkQuerySQL(TIDB_DATABASE, SINK_TABLE)).toString());
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    getSourceQuerySQL(
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_NO_PRIMARY_KEY))),
                                    JsonUtils.toJsonString(
                                            query(getSinkQuerySQL(TIDB_DATABASE, SINK_TABLE))));
                        });

        // insert update delete
        executeSql("DELETE FROM " + TIDB_DATABASE + "." + SOURCE_TABLE_NO_PRIMARY_KEY);
        upsertDeleteSourceTable(TIDB_DATABASE, SOURCE_TABLE_NO_PRIMARY_KEY);

        // stream stage
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    getSourceQuerySQL(
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_NO_PRIMARY_KEY))),
                                    JsonUtils.toJsonString(
                                            query(getSinkQuerySQL(TIDB_DATABASE, SINK_TABLE))));
                        });
    }

    @TestTemplate
    public void testTiDBCdcMultiTableCheckDataE2E(TestContainer container) throws Exception {
        clearSimpleTables();

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/tidb/tidbcdc_multi_table_to_tidb.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // Flink deployments can take longer than the default window before the first snapshot lands
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_2))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_2))));
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_3))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_3))));
                        });

        // stream stage: insert update delete on both tables
        upsertDeleteSimpleTable(TIDB_DATABASE, SOURCE_TABLE_2, 1, 2);
        upsertDeleteSimpleTable(TIDB_DATABASE, SOURCE_TABLE_3, 5, 6);

        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_2))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_2))));
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_3))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_3))));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testTiDBCdcSavepointRestoreWithAddedTable(TestContainer container)
            throws IOException, InterruptedException {
        clearSimpleTables();

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/tidb/tidbcdc_multi_table_to_tidb_single.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // only source_table_2 is captured before the savepoint
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_2))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_2))));
                        });

        upsertDeleteSimpleTable(TIDB_DATABASE, SOURCE_TABLE_2, 11, 12);

        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_2))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_2))));
                        });

        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());

        // restore with the multi-table config which additionally captures source_table_3
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/tidb/tidbcdc_multi_table_to_tidb.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // the newly added table must run its snapshot after restore
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_3))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_3))));
                        });

        // stream stage on both the original and the added table
        upsertDeleteSimpleTable(TIDB_DATABASE, SOURCE_TABLE_2, 21, 22);
        upsertDeleteSimpleTable(TIDB_DATABASE, SOURCE_TABLE_3, 25, 26);

        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_2))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_2))));
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_3))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_3))));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testTiDBCdcSavepointRestoreWithRemovedTable(TestContainer container)
            throws IOException, InterruptedException {
        clearSimpleTables();

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/tidb/tidbcdc_multi_table_to_tidb.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // both tables are captured before the savepoint
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_2))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_2))));
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_3))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_3))));
                        });

        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());

        // this row lands in source table_3 while the job is down
        executeSql(
                "INSERT INTO "
                        + TIDB_DATABASE
                        + "."
                        + SOURCE_TABLE_3
                        + " (id, name, val) VALUES (31, 'row_while_removed', 9.99)");

        // restore with the single-table config: table_3 is removed from the job
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/tidb/tidbcdc_multi_table_to_tidb_single.conf",
                                String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // the remaining table_2 must keep streaming after the restore
        upsertDeleteSimpleTable(TIDB_DATABASE, SOURCE_TABLE_2, 41, 42);
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JsonUtils.toJsonString(
                                                query(
                                                        String.format(
                                                                SIMPLE_SQL_TEMPLATE,
                                                                TIDB_DATABASE,
                                                                SOURCE_TABLE_2))),
                                        JsonUtils.toJsonString(
                                                query(
                                                        String.format(
                                                                SIMPLE_SQL_TEMPLATE,
                                                                TIDB_SINK_DATABASE,
                                                                SOURCE_TABLE_2)))));

        // the removed table's row written while the job was down must not be synced
        Assertions.assertEquals(
                0,
                query(
                                "select id, name, val from "
                                        + TIDB_SINK_DATABASE
                                        + "."
                                        + SOURCE_TABLE_3
                                        + " where id = 31")
                        .size());

        // adding the table back runs a fresh snapshot which picks up the missed row
        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/tidb/tidbcdc_multi_table_to_tidb.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JsonUtils.toJsonString(
                                                query(
                                                        String.format(
                                                                SIMPLE_SQL_TEMPLATE,
                                                                TIDB_DATABASE,
                                                                SOURCE_TABLE_3))),
                                        JsonUtils.toJsonString(
                                                query(
                                                        String.format(
                                                                SIMPLE_SQL_TEMPLATE,
                                                                TIDB_SINK_DATABASE,
                                                                SOURCE_TABLE_3)))));
    }

    private List<List<Object>> query(String sql) {
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

    private String getSourceQuerySQL(String database, String tableName) {
        return String.format(SOURCE_SQL_TEMPLATE, database, tableName);
    }

    private String getSinkQuerySQL(String database, String tableName) {
        return String.format(SINK_SQL_TEMPLATE, database, tableName);
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }

    // Execute SQL
    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void upsertDeleteSourceTable(String database, String tableName) {

        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + "( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( 1, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', '中文测试', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 2022 ),\n"
                        + "       ( 2, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,\n"
                        + "         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321,\n"
                        + "         123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',\n"
                        + "         'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',\n"
                        + "         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         112.345, '14:30:00', -128, 22, '{ \"key\": \"value\" }', 2013 ),\n"
                        + "       ( 3, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,\n"
                        + "         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321, 123,\n"
                        + "         789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',\n"
                        + "         'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',\n"
                        + "         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', 112.345,\n"
                        + "         '14:30:00', -128, 22, '{ \"key\": \"value\" }', 2021 );");

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

    private void upsertDeleteSourceTableForRestore(String database, String tableName) {
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
                        + "VALUES ( 20, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
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
                        + "VALUES ( 30, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1999 )");
        executeSql("DELETE FROM " + database + "." + tableName + " where id = 20");

        executeSql("UPDATE " + database + "." + tableName + " SET f_bigint = 10000 where id = 30");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testTiDBCdcLegacyConfigRestoredWithTableNamesExpanded(TestContainer container)
            throws IOException, InterruptedException {
        clearSimpleTables();

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/tidb/tidbcdc_legacy_single_table.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // snapshot stage with the original database-name/table-name config
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_2))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_2))));
                        });

        // streaming stage before the savepoint
        upsertDeleteSimpleTable(TIDB_DATABASE, SOURCE_TABLE_2, 11, 12);
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_2))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_2))));
                        });

        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());

        // restore with the table-names config which additionally captures SOURCE_TABLE_3:
        // the config style migrates from the original keys to table-names in one restore
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/tidb/tidbcdc_multi_table_to_tidb.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // the table added by the config migration must run its initial snapshot
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_3))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_3))));
                        });

        // stream stage on both the original and the added table
        upsertDeleteSimpleTable(TIDB_DATABASE, SOURCE_TABLE_2, 21, 22);
        upsertDeleteSimpleTable(TIDB_DATABASE, SOURCE_TABLE_3, 25, 26);

        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_2))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_2))));
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_3))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_3))));
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support restore")
    public void testTiDBCdcTableNamesRestoredWithLegacyConfigReduced(TestContainer container)
            throws IOException, InterruptedException {
        clearSimpleTables();

        Long jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/tidb/tidbcdc_multi_table_to_tidb.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // both tables sync with the table-names config before the savepoint
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_2))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_2))));
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_3))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_3))));
                        });

        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());

        // restore with the original single-table config keys: SOURCE_TABLE_3 is no longer
        // configured and must stop receiving changes, SOURCE_TABLE_2 keeps streaming
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(
                                "/tidb/tidbcdc_legacy_single_table.conf", String.valueOf(jobId));
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        upsertDeleteSimpleTable(TIDB_DATABASE, SOURCE_TABLE_2, 31, 32);
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_DATABASE,
                                                            SOURCE_TABLE_2))),
                                    JsonUtils.toJsonString(
                                            query(
                                                    String.format(
                                                            SIMPLE_SQL_TEMPLATE,
                                                            TIDB_SINK_DATABASE,
                                                            SOURCE_TABLE_2))));
                        });

        // changes on the removed table must not reach the sink any more; the sink keeps
        // exactly the pre-savepoint rows while the pipeline keeps streaming SOURCE_TABLE_2
        upsertDeleteSimpleTable(TIDB_DATABASE, SOURCE_TABLE_3, 35, 36);
        await().atMost(20, TimeUnit.SECONDS)
                .during(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        2,
                                        query(
                                                        String.format(
                                                                SIMPLE_SQL_TEMPLATE,
                                                                TIDB_SINK_DATABASE,
                                                                SOURCE_TABLE_3))
                                                .size()));
    }

    private void clearSimpleTables() {
        clearTable(TIDB_DATABASE, SOURCE_TABLE_2);
        clearTable(TIDB_DATABASE, SOURCE_TABLE_3);
        clearTable(TIDB_SINK_DATABASE, SOURCE_TABLE_2);
        clearTable(TIDB_SINK_DATABASE, SOURCE_TABLE_3);
        executeSql(
                "INSERT INTO "
                        + TIDB_DATABASE
                        + "."
                        + SOURCE_TABLE_2
                        + " (id, name, val) VALUES (101, 'seed_a', 1.50), (102, 'seed_b', 2.50)");
        executeSql(
                "INSERT INTO "
                        + TIDB_DATABASE
                        + "."
                        + SOURCE_TABLE_3
                        + " (id, name, val) VALUES (201, 'seed_c', 3.50), (202, 'seed_d', 4.50)");
    }

    private void upsertDeleteSimpleTable(
            String database, String tableName, int firstId, int secondId) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " (id, name, val) VALUES ("
                        + firstId
                        + ", 'Alice', 10.50), ("
                        + secondId
                        + ", 'Bob', 20.25)");
        executeSql(
                "UPDATE "
                        + database
                        + "."
                        + tableName
                        + " SET val = 99.99, name = 'Alice2' WHERE id = "
                        + firstId);
        executeSql("DELETE FROM " + database + "." + tableName + " WHERE id = " + secondId);
    }

    private List<List<Object>> getConnectionStatus(String user) {
        return query(
                "select USER,HOST,DB,COMMAND,TIME,STATE from information_schema.processlist where USER = '"
                        + user
                        + "'");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        stopContainers();
    }
}
