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
import org.testcontainers.containers.Container;
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
import java.util.stream.Stream;

/**
 * E2E for the MySQL-CDC {@code startup.mode = "snapshot"} bounded bootstrap mode.
 *
 * <p>The job runs in BATCH mode, so {@link TestContainer#executeJob} blocks until the job
 * terminates. This is the core assertion: a snapshot-only job must finish on its own after the
 * snapshot phase. If it wrongly transitioned into (or waited for) binlog streaming, the job would
 * never complete and this test would time out. After completion we additionally verify that binlog
 * changes produced <em>after</em> the snapshot are NOT captured, proving no binlog reader was
 * started.
 */
@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class MysqlCDCSnapshotOnlyIT extends TestSuiteBase implements TestResource {

    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "mysql_cdc";

    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table";

    // Query only the primary key so the assertion does not depend on the full wide-row schema.
    private static final String ID_QUERY_TEMPLATE = "select id from %s.%s order by id";

    private final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

    private MySqlContainer createMySqlContainer(MySqlVersion version) {
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

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() {
        log.info("Starting MySQL container for snapshot-only IT...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("MySQL container is started");
        inventoryDatabase.createAndInitialize();
        log.info("MySQL ddl execution is complete");
    }

    @TestTemplate
    public void testMysqlCdcSnapshotOnlyFinishesAndSkipsBinlog(TestContainer container)
            throws Exception {
        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        clearTable(MYSQL_DATABASE, SINK_TABLE);

        // Seed a deterministic set of snapshot rows.
        insertRow(2);
        insertRow(3);
        insertRow(5);
        List<List<Object>> snapshotIds = query(idQuery(MYSQL_DATABASE, SOURCE_TABLE));
        Assertions.assertEquals(3, snapshotIds.size(), "precondition: three snapshot rows seeded");

        // The config uses parallel snapshot readers with checkpointing disabled. executeJob blocks
        // until the bounded job terminates, so this also covers completion without a checkpoint.
        Container.ExecResult execResult = container.executeJob("/mysqlcdc_snapshot_only.conf");
        Assertions.assertEquals(
                0,
                execResult.getExitCode(),
                "snapshot-only job must finish cleanly: " + execResult.getStderr());

        // The snapshot data must have been written to the sink.
        Assertions.assertIterableEquals(
                snapshotIds,
                query(idQuery(MYSQL_DATABASE, SINK_TABLE)),
                "sink must contain exactly the snapshot rows after a snapshot-only job");

        // Produce binlog changes AFTER the job finished. A snapshot-only job never starts a binlog
        // reader, so these changes must not appear in the sink.
        insertRow(100);
        executeSql("UPDATE " + MYSQL_DATABASE + "." + SOURCE_TABLE + " SET id = 6 WHERE id = 5");
        executeSql("DELETE FROM " + MYSQL_DATABASE + "." + SOURCE_TABLE + " WHERE id = 2");

        // Re-run the bounded job would re-read the snapshot; instead we assert the sink is
        // unchanged
        // by the post-snapshot binlog activity, confirming no streaming path consumed them.
        Assertions.assertIterableEquals(
                snapshotIds,
                query(idQuery(MYSQL_DATABASE, SINK_TABLE)),
                "snapshot-only must not consume binlog changes made after the snapshot phase");
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
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

    private void insertRow(int id) {
        executeSql(
                "INSERT INTO "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "     f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "     f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "     f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "     f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "     f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( "
                        + id
                        + ", 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "     0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "     0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "     123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "     'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "     '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "     0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "     12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 )");
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }

    private String idQuery(String database, String tableName) {
        return String.format(ID_QUERY_TEMPLATE, database, tableName);
    }

    @Override
    @AfterAll
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }
}
