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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mariadb.MariaDbCatalog;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** E2E tests for MariaDB JDBC connector. */
public class JdbcMariaDBIT extends AbstractJdbcIT {

    private static final String MARIADB_IMAGE = "mariadb:11.6.2-ubi9";
    private static final String MARIADB_CONTAINER_HOST = "mariadb-e2e";
    private static final String MARIADB_DATABASE = "seatunnel";
    private static final String MARIADB_SOURCE = "source";
    private static final String MARIADB_SINK = "sink";
    private static final String CATALOG_DATABASE = "catalog_database";

    private static final String MARIADB_USERNAME = "root";
    private static final String MARIADB_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MARIADB_PORT = 3306;
    private static final String MARIADB_URL =
            "jdbc:mariadb://" + HOST + ":%s/%s?useSSL=false&useBulkStmts=false";
    private static final String URL =
            "jdbc:mariadb://%s:%s/seatunnel?useSSL=false&useBulkStmts=false";

    private static final String DRIVER_CLASS = "org.mariadb.jdbc.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList(
                    "/jdbc_mariadb_source_and_sink.conf",
                    "/jdbc_mariadb_source_and_sink_parallel.conf",
                    "/jdbc_mariadb_source_and_sink_parallel_upper_lower.conf",
                    "/jdbc_mariadb_source_and_sink.sql",
                    "/jdbc_mariadb_source_and_sink_parallel.sql");
    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS %s\n"
                    + "(\n"
                    + "    `c-bit_1`                bit(1)                DEFAULT NULL,\n"
                    + "    `c_bit_8`                bit(8)                DEFAULT NULL,\n"
                    + "    `c_bit_16`               bit(16)               DEFAULT NULL,\n"
                    + "    `c_bit_32`               bit(32)               DEFAULT NULL,\n"
                    + "    `c_bit_64`               bit(64)               DEFAULT NULL,\n"
                    + "    `c_tinyint_1`              tinyint(1)            DEFAULT NULL,\n"
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
                    + "    `c_time`                 time                  DEFAULT NULL,\n"
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
                    + "    `c_decimal_30`           DECIMAL(30)           DEFAULT NULL,\n"
                    + "    UNIQUE (c_bigint_30)\n"
                    + ");";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(MARIADB_URL, MARIADB_PORT, MARIADB_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(MARIADB_DATABASE, MARIADB_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(MARIADB_IMAGE)
                .networkAliases(MARIADB_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(MARIADB_PORT)
                .localPort(MARIADB_PORT)
                .jdbcTemplate(MARIADB_URL)
                .jdbcUrl(jdbcUrl)
                .userName(MARIADB_USERNAME)
                .password(MARIADB_PASSWORD)
                .database(MARIADB_DATABASE)
                .sourceTable(MARIADB_SOURCE)
                .sinkTable(MARIADB_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .catalogDatabase(CATALOG_DATABASE)
                .catalogTable(MARIADB_SINK)
                .tablePathFullName(MARIADB_DATABASE + "." + MARIADB_SOURCE)
                .build();
    }

    @Override
    protected void checkResult(
            String executeKey, TestContainer container, Container.ExecResult execResult) {
        String[] fieldNames =
                new String[] {
                    "c-bit_1",
                    "c_bit_8",
                    "c_bit_16",
                    "c_bit_32",
                    "c_bit_64",
                    "c_tinyint_1",
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
                    "c_time",
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
        defaultCompare(executeKey, fieldNames, "c_bigint_30");
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "c-bit_1",
                    "c_bit_8",
                    "c_bit_16",
                    "c_bit_32",
                    "c_bit_64",
                    "c_tinyint_1",
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
                    "c_time",
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
            SeaTunnelRow row;
            if (i == 99) {
                row =
                        new SeaTunnelRow(
                                new Object[] {
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    bigintValue.add(BigDecimal.valueOf(i)),
                                    decimalValue.add(BigDecimal.valueOf(i)),
                                    null,
                                });
            } else {
                row =
                        new SeaTunnelRow(
                                new Object[] {
                                    i % 2 == 0 ? (byte) 1 : (byte) 0,
                                    new byte[] {byteArr},
                                    new byte[] {byteArr, byteArr},
                                    new byte[] {byteArr, byteArr, byteArr, byteArr},
                                    new byte[] {
                                        byteArr, byteArr, byteArr, byteArr, byteArr, byteArr,
                                        byteArr, byteArr
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
                                    2000 + (i % 20),
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
                                    Time.valueOf(LocalTime.now()),
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
            }
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    protected GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(MARIADB_IMAGE);

        GenericContainer<?> container =
                new GenericContainer<>(imageName)
                        .withEnv("MARIADB_ROOT_PASSWORD", MARIADB_PASSWORD)
                        .withEnv("MARIADB_ROOT_HOST", "%")
                        .withEnv("MARIADB_DATABASE", MARIADB_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MARIADB_CONTAINER_HOST)
                        .withExposedPorts(MARIADB_PORT)
                        .waitingFor(Wait.forLogMessage(".*ready for connections.*\\n", 2))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MARIADB_IMAGE)));

        return container;
    }

    @Override
    protected void initCatalog() {
        catalog =
                new MariaDbCatalog(
                        "mariadb",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        JdbcUrlUtil.getUrlInfo(
                                jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost())),
                        null);
        catalog.open();
    }

    @Test
    public void testTinyInt1AsBooleanOrTINYINT() throws SQLException {
        testTinyInt1AsBooleanOrTINYINT(true, BasicType.BOOLEAN_TYPE);
        testTinyInt1AsBooleanOrTINYINT(false, BasicType.BYTE_TYPE);
    }

    private void testTinyInt1AsBooleanOrTINYINT(boolean intTypeNarrowing, BasicType<?> exceptType)
            throws SQLException {
        try (MariaDbCatalog catalogWithIntTypeNarrowing =
                new MariaDbCatalog(
                        "mariadb",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        JdbcUrlUtil.getUrlInfo(
                                jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost())),
                        null,
                        intTypeNarrowing)) {
            catalogWithIntTypeNarrowing.open();
            CatalogTable tableFromPath =
                    catalogWithIntTypeNarrowing.getTable(
                            TablePath.of(MARIADB_DATABASE, MARIADB_SOURCE));
            Assertions.assertEquals(
                    exceptType,
                    tableFromPath.getTableSchema().getColumn("c_tinyint_1").getDataType());
            CatalogTable tableFromSQL =
                    catalogWithIntTypeNarrowing.getTable(
                            "select c_tinyint_1 from " + MARIADB_DATABASE + "." + MARIADB_SOURCE);
            Assertions.assertEquals(
                    exceptType,
                    tableFromSQL.getTableSchema().getColumn("c_tinyint_1").getDataType());
        }
    }
}
