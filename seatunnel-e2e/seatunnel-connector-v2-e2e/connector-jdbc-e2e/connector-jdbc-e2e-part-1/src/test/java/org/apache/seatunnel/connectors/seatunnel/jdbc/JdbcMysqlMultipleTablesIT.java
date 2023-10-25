/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class JdbcMysqlMultipleTablesIT extends TestSuiteBase implements TestResource {
    private static final String MYSQL_IMAGE = "mysql:8.0";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3306;
    private static final Pair<String[], List<SeaTunnelRow>> TEST_DATASET = generateTestDataset();
    private static final String SOURCE_DATABASE = "source";
    private static final String SINK_DATABASE = "sink";
    private static final List<String> TABLES = Arrays.asList("table1", "table2");
    private static final List<String> SOURCE_TABLES =
            TABLES.stream()
                    .map(table -> SOURCE_DATABASE + "." + table)
                    .collect(Collectors.toList());

    private static final List<String> SINK_TABLES =
            TABLES.stream().map(table -> SINK_DATABASE + "." + table).collect(Collectors.toList());
    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS %s\n"
                    + "(\n"
                    + "    `c_bit_1`                bit(1)                DEFAULT NULL,\n"
                    + "    `c_bit_8`                bit(8)                DEFAULT NULL,\n"
                    + "    `c_bit_16`               bit(16)               DEFAULT NULL,\n"
                    + "    `c_bit_32`               bit(32)               DEFAULT NULL,\n"
                    + "    `c_bit_64`               bit(64)               DEFAULT NULL,\n"
                    + "    `c_boolean`              tinyint(1)            DEFAULT NULL,\n"
                    + "    `c_tinyint`              tinyint(4)            DEFAULT NULL,\n"
                    + "    `c_tinyint_unsigned`     tinyint(3) unsigned   DEFAULT NULL,\n"
                    + "    `c_smallint`             smallint(6)           DEFAULT NULL,\n"
                    + "    `c_smallint_unsigned`    smallint(5) unsigned  DEFAULT NULL,\n"
                    + "    `c_mediumint`            mediumint(9)          DEFAULT NULL,\n"
                    + "    `c_mediumint_unsigned`   mediumint(8) unsigned DEFAULT NULL,\n"
                    + "    `c_int`                  int(11)               DEFAULT NULL,\n"
                    + "    `c_integer`              int(11)               DEFAULT NULL,\n"
                    + "    `c_bigint`               bigint(20)            DEFAULT NULL,\n"
                    + "    `c_bigint_unsigned`      bigint(20) unsigned   DEFAULT NULL,\n"
                    + "    `c_decimal`              decimal(20, 0)        DEFAULT NULL,\n"
                    + "    `c_decimal_unsigned`     decimal(38, 18)       DEFAULT NULL,\n"
                    + "    `c_float`                float                 DEFAULT NULL,\n"
                    + "    `c_float_unsigned`       float unsigned        DEFAULT NULL,\n"
                    + "    `c_double`               double                DEFAULT NULL,\n"
                    + "    `c_double_unsigned`      double unsigned       DEFAULT NULL,\n"
                    + "    `c_char`                 char(1)               DEFAULT NULL,\n"
                    + "    `c_tinytext`             tinytext,\n"
                    + "    `c_mediumtext`           mediumtext,\n"
                    + "    `c_text`                 text,\n"
                    + "    `c_varchar`              varchar(255)          DEFAULT NULL,\n"
                    + "    `c_json`                 json                  DEFAULT NULL,\n"
                    + "    `c_longtext`             longtext,\n"
                    + "    `c_date`                 date                  DEFAULT NULL,\n"
                    + "    `c_datetime`             datetime              DEFAULT NULL,\n"
                    + "    `c_timestamp`            timestamp NULL        DEFAULT NULL,\n"
                    + "    `c_tinyblob`             tinyblob,\n"
                    + "    `c_mediumblob`           mediumblob,\n"
                    + "    `c_blob`                 blob,\n"
                    + "    `c_longblob`             longblob,\n"
                    + "    `c_varbinary`            varbinary(255)        DEFAULT NULL,\n"
                    + "    `c_binary`               binary(1)             DEFAULT NULL,\n"
                    + "    `c_year`                 year(4)               DEFAULT NULL,\n"
                    + "    `c_int_unsigned`         int(10) unsigned      DEFAULT NULL,\n"
                    + "    `c_integer_unsigned`     int(10) unsigned      DEFAULT NULL,\n"
                    + "    `c_bigint_30`            BIGINT(40)  unsigned  DEFAULT NULL,\n"
                    + "    `c_decimal_unsigned_30`  DECIMAL(30) unsigned  DEFAULT NULL,\n"
                    + "    `c_decimal_30`           DECIMAL(30)           DEFAULT NULL\n"
                    + ");";

    private MySQLContainer mysqlContainer;
    private Connection connection;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        mysqlContainer = startMySqlContainer();
        connection = mysqlContainer.createConnection("");
        createTables(SOURCE_DATABASE, TABLES);
        createTables(SINK_DATABASE, TABLES);
        initSourceTablesData();
    }

    @TestTemplate
    public void testMysqlJdbcSingleTableE2e(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        clearSinkTables();

        Container.ExecResult execResult =
                container.executeJob("/jdbc_mysql_source_using_table_path.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        Assertions.assertIterableEquals(
                query(String.format("SELECT * FROM %s.%s", SOURCE_DATABASE, "table1")),
                query(String.format("SELECT * FROM %s.%s", SINK_DATABASE, "table1")));
    }

    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "Currently SPARK and FLINK do not support multiple tables")
    @TestTemplate
    public void testMysqlJdbcMultipleTableE2e(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        clearSinkTables();

        Container.ExecResult execResult =
                container.executeJob("/jdbc_mysql_source_and_sink_with_multiple_tables.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        List<Executable> asserts =
                TABLES.stream()
                        .map(
                                (Function<String, Executable>)
                                        table ->
                                                () ->
                                                        Assertions.assertIterableEquals(
                                                                query(
                                                                        String.format(
                                                                                "SELECT * FROM %s.%s",
                                                                                SOURCE_DATABASE,
                                                                                table)),
                                                                query(
                                                                        String.format(
                                                                                "SELECT * FROM %s.%s",
                                                                                SINK_DATABASE,
                                                                                table))))
                        .collect(Collectors.toList());
        Assertions.assertAll(asserts);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (mysqlContainer != null) {
            mysqlContainer.close();
        }
    }

    private MySQLContainer startMySqlContainer() {
        MySQLContainer container =
                new MySQLContainer<>(MYSQL_IMAGE)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));

        Startables.deepStart(Stream.of(container)).join();
        return container;
    }

    private void createTables(String database, List<String> tables) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("create database if not exists " + database);
            tables.forEach(
                    tableName -> {
                        try {
                            statement.execute(
                                    String.format(CREATE_TABLE_SQL, database + "." + tableName));
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    private void initSourceTablesData() throws SQLException {
        String columns = Arrays.stream(TEST_DATASET.getLeft()).collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(TEST_DATASET.getLeft())
                        .map(f -> "?")
                        .collect(Collectors.joining(", "));
        for (String table : SOURCE_TABLES) {
            String sql =
                    "INSERT INTO " + table + " (" + columns + " ) VALUES (" + placeholders + ")";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (SeaTunnelRow row : TEST_DATASET.getRight()) {
                    for (int i = 0; i < row.getArity(); i++) {
                        statement.setObject(i + 1, row.getField(i));
                    }
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        }
    }

    private List<List<Object>> query(String sql) {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getString(i));
                }
                result.add(objects);
                log.debug(String.format("Print query, sql: %s, data: %s", sql, objects));
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void clearSinkTables() throws SQLException {
        for (String table : SINK_TABLES) {
            String sql = "truncate table " + table;
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    private static Pair<String[], List<SeaTunnelRow>> generateTestDataset() {
        String[] fieldNames =
                new String[] {
                    "c_bit_1",
                    "c_bit_8",
                    "c_bit_16",
                    "c_bit_32",
                    "c_bit_64",
                    "c_boolean",
                    "c_tinyint",
                    "c_tinyint_unsigned",
                    "c_smallint",
                    "c_smallint_unsigned",
                    "c_mediumint",
                    "c_mediumint_unsigned",
                    "c_int",
                    "c_integer",
                    "c_year",
                    "c_int_unsigned",
                    "c_integer_unsigned",
                    "c_bigint",
                    "c_bigint_unsigned",
                    "c_decimal",
                    "c_decimal_unsigned",
                    "c_float",
                    "c_float_unsigned",
                    "c_double",
                    "c_double_unsigned",
                    "c_char",
                    "c_tinytext",
                    "c_mediumtext",
                    "c_text",
                    "c_varchar",
                    "c_json",
                    "c_longtext",
                    "c_date",
                    "c_datetime",
                    "c_timestamp",
                    "c_tinyblob",
                    "c_mediumblob",
                    "c_blob",
                    "c_longblob",
                    "c_varbinary",
                    "c_binary",
                    "c_bigint_30",
                    "c_decimal_unsigned_30",
                    "c_decimal_30",
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        BigDecimal bigintValue = new BigDecimal("2844674407371055000");
        BigDecimal decimalValue = new BigDecimal("999999999999999999999999999899");
        for (int i = 0; i < 100; i++) {
            byte byteArr = Integer.valueOf(i).byteValue();
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i % 2 == 0 ? (byte) 1 : (byte) 0,
                                new byte[] {byteArr},
                                new byte[] {byteArr, byteArr},
                                new byte[] {byteArr, byteArr, byteArr, byteArr},
                                new byte[] {
                                    byteArr, byteArr, byteArr, byteArr, byteArr, byteArr, byteArr,
                                    byteArr
                                },
                                i % 2 == 0 ? Boolean.TRUE : Boolean.FALSE,
                                i,
                                i,
                                i,
                                i,
                                i,
                                i,
                                i,
                                i,
                                i,
                                Long.parseLong("1"),
                                Long.parseLong("1"),
                                Long.parseLong("1"),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 18),
                                BigDecimal.valueOf(i, 18),
                                Float.parseFloat("1.1"),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                Double.parseDouble("1.1"),
                                "f",
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("{\"aa\":\"bb_%s\"}", i),
                                String.format("f1_%s", i),
                                Date.valueOf(LocalDate.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                new Timestamp(System.currentTimeMillis()),
                                "test".getBytes(),
                                "test".getBytes(),
                                "test".getBytes(),
                                "test".getBytes(),
                                "test".getBytes(),
                                "f".getBytes(),
                                bigintValue.add(BigDecimal.valueOf(i)),
                                decimalValue.add(BigDecimal.valueOf(i)),
                                decimalValue.add(BigDecimal.valueOf(i)),
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }
}
