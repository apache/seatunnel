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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

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

/**
 * End-to-end test for MySQL CDC {@code startup.mode = "snapshot-only"}.
 *
 * <p>Verifies that a snapshot-only job reads the current table contents, writes them to the sink,
 * and finishes without entering the incremental/binlog phase.
 */
@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Snapshot-only mode requires Zeta engine for BATCH job mode")
public class MysqlCDCSnapshotOnlyIT extends TestSuiteBase implements TestResource {

    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table_snapshot_only";
    private static final String CONF_FILE = "/mysqlcdc_snapshot_only_to_mysql.conf";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
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
    private final ContainerExtendedFactory extendedFactory =
            MysqlCDCDriverResolver::copyMySQLDriverToContainer;

    @BeforeAll
    @Override
    public void startUp() {
        log.info("Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    @TestTemplate
    public void testMysqlCdcSnapshotOnly(TestContainer container) {
        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(CONF_FILE);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // Snapshot-only job should complete and sync all data
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(getSourceQuerySQL(MYSQL_DATABASE, SOURCE_TABLE)),
                                    query(getSinkQuerySQL(MYSQL_DATABASE, SINK_TABLE)));
                        });
    }

    private List<List<Object>> query(String sql) {
        try (Connection connection =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    List<List<Object>> result = new ArrayList<>();
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    while (resultSet.next()) {
                        List<Object> row = new ArrayList<>();
                        for (int i = 1; i <= columnCount; i++) {
                            row.add(resultSet.getObject(i));
                        }
                        result.add(row);
                    }
                    return result;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute query: " + sql, e);
        }
    }

    private void clearTable(String database, String tableName) {
        try (Connection connection =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("TRUNCATE TABLE " + database + "." + tableName);
            }
        } catch (SQLException e) {
            log.warn("Failed to truncate table {}.{}: {}", database, tableName, e.getMessage());
        }
    }

    private String getSourceQuerySQL(String database, String tableName) {
        return "select id, cast(f_binary as char) as f_binary, cast(f_blob as char) as f_blob,"
                + " cast(f_long_varbinary as char) as f_long_varbinary,"
                + " cast(f_longblob as char) as f_longblob,"
                + " cast(f_tinyblob as char) as f_tinyblob,"
                + " cast(f_varbinary as char) as f_varbinary,"
                + " f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned,"
                + " f_int, f_int_unsigned, f_integer, f_integer_unsigned,"
                + " f_bigint, f_bigint_unsigned, f_numeric, f_decimal,"
                + " f_float, f_double, f_double_precision, f_longtext, f_mediumtext,"
                + " f_text, f_tinytext, f_varchar, f_date, f_datetime, f_timestamp,"
                + " f_bit1, cast(f_bit64 as char) as f_bit64, f_char,"
                + " f_enum, cast(f_mediumblob as char) as f_mediumblob,"
                + " f_long_varchar, f_real, f_time, f_tinyint, f_tinyint_unsigned,"
                + " f_json, f_year"
                + " from "
                + database
                + "."
                + tableName;
    }

    private String getSinkQuerySQL(String database, String tableName) {
        return "select id, cast(f_binary as char) as f_binary, cast(f_blob as char) as f_blob,"
                + " cast(f_long_varbinary as char) as f_long_varbinary,"
                + " cast(f_longblob as char) as f_longblob,"
                + " cast(f_tinyblob as char) as f_tinyblob,"
                + " cast(f_varbinary as char) as f_varbinary,"
                + " f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned,"
                + " f_int, f_int_unsigned, f_integer, f_integer_unsigned,"
                + " f_bigint, f_bigint_unsigned, f_numeric, f_decimal,"
                + " f_float, f_double, f_double_precision, f_longtext, f_mediumtext,"
                + " f_text, f_tinytext, f_varchar, f_date, f_datetime, f_timestamp,"
                + " f_bit1, cast(f_bit64 as char) as f_bit64, f_char,"
                + " f_enum, cast(f_mediumblob as char) as f_mediumblob,"
                + " f_long_varchar, f_real, f_time, f_tinyint, f_tinyint_unsigned,"
                + " f_json, cast(f_year as year)"
                + " from "
                + database
                + "."
                + tableName;
    }
}
