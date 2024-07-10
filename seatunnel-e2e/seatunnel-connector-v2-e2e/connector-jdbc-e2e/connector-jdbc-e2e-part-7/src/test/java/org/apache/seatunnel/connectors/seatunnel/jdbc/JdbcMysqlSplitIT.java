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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.DynamicChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.apache.commons.lang3.tuple.Pair;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

public class JdbcMysqlSplitIT extends TestSuiteBase implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcMysqlSplitIT.class);

    private static final String MYSQL_IMAGE = "mysql:8.0";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "auto";
    private static final String MYSQL_TABLE = "split_test";

    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3312;

    private MySQLContainer<?> mysql_container;

    LocalDate currentDateOld = LocalDate.of(2024, 1, 18);

    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS "
                    + MYSQL_TABLE
                    + "\n"
                    + "(\n"
                    + "    `id`                     int                   NOT NULL,\n"
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
                    + "    `c_decimal_unsigned`     decimal(38, 10)       DEFAULT NULL,\n"
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
                    + "    `c_decimal_30`           DECIMAL(30)           DEFAULT NULL,\n"
                    + "    PRIMARY KEY (`id`)\n"
                    + ");";

    void initContainer() throws ClassNotFoundException {
        // ============= mysql
        DockerImageName imageName = DockerImageName.parse(MYSQL_IMAGE);
        mysql_container =
                new MySQLContainer<>(imageName)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));
        mysql_container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", MYSQL_PORT, 3306)));

        Startables.deepStart(Stream.of(mysql_container)).join();
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        initContainer();
        given().await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
    }

    private void initializeJdbcTable() {
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();
        String insertSql = insertTable(MYSQL_DATABASE, MYSQL_TABLE, fieldNames);
        insertTestData(insertSql, testDataSet.getRight());
    }

    public String insertTable(String schema, String table, String... fields) {
        String columns =
                Arrays.stream(fields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(fields).map(f -> "?").collect(Collectors.joining(", "));

        return "INSERT INTO "
                + schema
                + "."
                + table
                + " ("
                + columns
                + " )"
                + " VALUES ("
                + placeholders
                + ")";
    }

    protected void insertTestData(String insertSql, List<SeaTunnelRow> rows) {
        try (Connection connection = getJdbcConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {

            preparedStatement.execute(CREATE_SQL);
            for (SeaTunnelRow row : rows) {
                for (int index = 0; index < row.getArity(); index++) {
                    preparedStatement.setObject(index + 1, row.getField(index));
                }
                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();

            // ANALYZE TABLE
            preparedStatement.execute("ANALYZE TABLE " + MYSQL_DATABASE + "." + MYSQL_TABLE);

        } catch (Exception exception) {
            LOG.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.INSERT_DATA_FAILED, exception);
        }
    }

    public String quoteIdentifier(String field) {
        return "`" + field + "`";
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                mysql_container.getJdbcUrl(),
                mysql_container.getUsername(),
                mysql_container.getPassword());
    }

    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "id",
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
        LocalDate currentDate = LocalDate.of(2024, 1, 17);

        LocalDateTime currLocalDateTime =
                LocalDateTime.of(currentDate, LocalDateTime.now().toLocalTime());

        for (int i = 0; i < 100; i++) {
            currentDate = currentDate.plusDays(1);
            currLocalDateTime = currLocalDateTime.plusDays(1);
            byte byteArr = Integer.valueOf(i).byteValue();
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i,
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
                                Long.parseLong(i + ""),
                                Long.parseLong(i + ""),
                                Long.parseLong(i + ""),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i * 10000000000L, 10),
                                Float.parseFloat(i + ".1"),
                                Float.parseFloat(i + ".1"),
                                Double.parseDouble(i + ".1"),
                                Double.parseDouble(i + ".1"),
                                "f",
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                String.format("{\"aa\":\"bb_%s\"}", i),
                                String.format("f1_%s", i),
                                Date.valueOf(currentDate),
                                Timestamp.valueOf(currLocalDateTime),
                                Timestamp.valueOf(currLocalDateTime),
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

    static JdbcUrlUtil.UrlInfo mysqlUrlInfo =
            JdbcUrlUtil.getUrlInfo(
                    String.format("jdbc:mysql://localhost:%s/auto?useSSL=false", MYSQL_PORT));

    @Test
    public void testSplit() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", mysqlUrlInfo.getUrlWithDatabase().get());
        configMap.put("driver", "com.mysql.cj.jdbc.Driver");
        configMap.put("user", MYSQL_USERNAME);
        configMap.put("password", MYSQL_PASSWORD);
        configMap.put("table_path", MYSQL_DATABASE + "." + MYSQL_TABLE);
        configMap.put("split.size", "10");
        DynamicChunkSplitter splitter = getDynamicChunkSplitter(configMap);

        TablePath tablePathMySql = TablePath.of(MYSQL_DATABASE, MYSQL_TABLE);
        MySqlCatalog mySqlCatalog =
                new MySqlCatalog("mysql", MYSQL_USERNAME, MYSQL_PASSWORD, mysqlUrlInfo);
        mySqlCatalog.open();
        Assertions.assertTrue(mySqlCatalog.tableExists(tablePathMySql));
        CatalogTable table = mySqlCatalog.getTable(tablePathMySql);

        JdbcSourceTable jdbcSourceTable =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of(MYSQL_DATABASE, MYSQL_TABLE))
                        .catalogTable(table)
                        .build();
        Collection<JdbcSourceSplit> jdbcSourceSplits = splitter.generateSplits(jdbcSourceTable);
        Assertions.assertEquals(10, jdbcSourceSplits.size());
        JdbcSourceSplit[] splitArray = jdbcSourceSplits.toArray(new JdbcSourceSplit[0]);
        Assertions.assertEquals("id", splitArray[0].getSplitKeyName());
        assertNumSplit(splitArray, "");

        // use tinyint column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_tinyint", 10);
        assertNumSplit(splitArray, "");

        // use tinyint_unsigned column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_tinyint_unsigned", 10);
        configMap.put("partition_column", "c_tinyint_unsigned");
        assertNumSplit(splitArray, "");

        // use smallint column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_smallint", 10);
        configMap.put("partition_column", "c_smallint");
        assertNumSplit(splitArray, "");

        // use smallint_unsigned column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_smallint_unsigned", 10);
        configMap.put("partition_column", "c_smallint_unsigned");
        assertNumSplit(splitArray, "");

        // use int column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_int", 10);
        configMap.put("partition_column", "c_int");
        assertNumSplit(splitArray, "");

        // use int column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_integer", 10);
        configMap.put("partition_column", "c_integer");
        assertNumSplit(splitArray, "");

        // use int_unsigned column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_int_unsigned", 10);
        configMap.put("partition_column", "c_int_unsigned");
        assertNumSplit(splitArray, "");

        // use integer_unsigned column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_integer_unsigned", 10);
        configMap.put("partition_column", "c_integer_unsigned");
        assertNumSplit(splitArray, "");

        // use int column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_mediumint", 10);
        configMap.put("partition_column", "c_mediumint");
        assertNumSplit(splitArray, "");

        // use int column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_mediumint_unsigned", 10);
        configMap.put("partition_column", "c_mediumint_unsigned");
        assertNumSplit(splitArray, "");

        // use bigint column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_bigint", 10);
        configMap.put("partition_column", "c_bigint");
        assertNumSplit(splitArray, "");

        // use bigint_unsigned column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_bigint_unsigned", 10);
        configMap.put("partition_column", "c_bigint_unsigned");
        assertNumSplit(splitArray, "");

        // use decimal column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_decimal", 10);
        configMap.put("partition_column", "c_decimal");
        assertNumSplit(splitArray, "");

        // use decimal_unsigned column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_decimal_unsigned", 10);
        configMap.put("partition_column", "c_decimal_unsigned");
        assertNumSplit(splitArray, ".0000000000");

        // use double column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_double", 10);
        configMap.put("partition_column", "c_double");
        assertNumSplit(splitArray, ".1");

        // use unsigned double column to split
        splitArray = getCheckedSplitArray(configMap, table, "c_double_unsigned", 10);
        configMap.put("partition_column", "c_double_unsigned");
        assertNumSplit(splitArray, ".1");

        // use date column to split
        configMap.put("partition_column", "c_date");
        splitArray = getCheckedSplitArray(configMap, table, "c_date", 13);
        configMap.put("partition_column", "c_date");
        assertDateSplit(splitArray);

        // use datetime column to split
        configMap.put("partition_column", "c_datetime");
        splitArray = getCheckedSplitArray(configMap, table, "c_datetime", 13);
        configMap.put("partition_column", "c_datetime");
        assertDateSplit(splitArray);

        // use c_timestamp column to split
        configMap.put("partition_column", "c_timestamp");
        splitArray = getCheckedSplitArray(configMap, table, "c_timestamp", 13);
        configMap.put("partition_column", "c_timestamp");
        assertDateSplit(splitArray);

        mySqlCatalog.close();
    }

    private JdbcSourceSplit[] getCheckedSplitArray(
            Map<String, Object> configMap, CatalogTable table, String splitKey, int splitNum)
            throws Exception {
        configMap.put("partition_column", splitKey);
        DynamicChunkSplitter splitter = getDynamicChunkSplitter(configMap);

        JdbcSourceTable jdbcSourceTable =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of(MYSQL_DATABASE, MYSQL_TABLE))
                        .catalogTable(table)
                        .partitionColumn(splitKey)
                        .build();
        Collection<JdbcSourceSplit> jdbcSourceSplits = splitter.generateSplits(jdbcSourceTable);
        Assertions.assertEquals(splitNum, jdbcSourceSplits.size());
        JdbcSourceSplit[] splitArray = jdbcSourceSplits.toArray(new JdbcSourceSplit[0]);
        Assertions.assertEquals(splitKey, splitArray[0].getSplitKeyName());
        return splitArray;
    }

    private void assertNumSplit(JdbcSourceSplit[] splitArray, String info) {
        for (int i = 0; i < splitArray.length; i++) {
            if (i == 0) {
                Assertions.assertEquals(null, splitArray[i].getSplitStart());
                Assertions.assertEquals("10" + info, splitArray[i].getSplitEnd().toString());
                continue;
            }

            if (i == splitArray.length - 1 && i != 0) {
                Assertions.assertEquals(10 * i + info, splitArray[i].getSplitStart().toString());
                Assertions.assertEquals(null, splitArray[i].getSplitEnd());
                continue;
            }

            Assertions.assertEquals(10 * i + info, splitArray[i].getSplitStart().toString());
            Assertions.assertEquals(10 * (i + 1) + info, splitArray[i].getSplitEnd().toString());
        }
    }

    private void assertDateSplit(JdbcSourceSplit[] splitArray) {
        for (int i = 0; i < splitArray.length; i++) {
            if (i == 0) {
                Assertions.assertEquals(null, splitArray[i].getSplitStart());
                Assertions.assertEquals(
                        currentDateOld.plusDays(i * 9).toString(),
                        splitArray[i].getSplitEnd().toString());
                continue;
            }

            if (i == splitArray.length - 1 && i != 0) {
                Assertions.assertEquals(
                        currentDateOld.plusDays((i - 1) * 9).toString(),
                        splitArray[i].getSplitStart().toString());
                Assertions.assertEquals(null, splitArray[i].getSplitEnd());
                continue;
            }

            Assertions.assertEquals(
                    currentDateOld.plusDays((i - 1) * 9).toString(),
                    splitArray[i].getSplitStart().toString());
            Assertions.assertEquals(
                    currentDateOld.plusDays(i * 9).toString(),
                    splitArray[i].getSplitEnd().toString());
        }
    }

    @NotNull private DynamicChunkSplitter getDynamicChunkSplitter(Map<String, Object> configMap) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        JdbcSourceConfig sourceConfig = JdbcSourceConfig.of(readonlyConfig);
        DynamicChunkSplitter splitter = new DynamicChunkSplitter(sourceConfig);
        return splitter;
    }

    @Override
    public void tearDown() throws Exception {
        if (mysql_container != null) {
            mysql_container.close();
            dockerClient.removeContainerCmd(mysql_container.getContainerId()).exec();
        }
    }
}
