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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSink;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.ChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSource;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.commons.lang3.tuple.Pair;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mysql.cj.jdbc.ConnectionImpl;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JdbcMysqlIT extends AbstractJdbcIT {

    private static final String MYSQL_IMAGE = "mysql:8.0";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final String MYSQL_SOURCE = "source";
    private static final String MYSQL_SINK = "sink";
    private static final String CATALOG_DATABASE = "catalog_database";

    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_URL = "jdbc:mysql://" + HOST + ":%s/%s?useSSL=false";
    private static final String URL = "jdbc:mysql://" + HOST + ":3306/seatunnel";

    private static final String SQL = "select * from seatunnel.source";

    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList(
                    "/jdbc_mysql_source_and_sink.conf",
                    "/jdbc_mysql_source_and_sink_parallel.conf",
                    "/jdbc_mysql_source_and_sink_parallel_upper_lower.conf",
                    "/jdbc_mysql_source_and_sink.sql",
                    "/jdbc_mysql_source_and_sink_parallel.sql");
    private static final String CREATE_SQL =
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
        String jdbcUrl = String.format(MYSQL_URL, MYSQL_PORT, MYSQL_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(MYSQL_DATABASE, MYSQL_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(MYSQL_IMAGE)
                .networkAliases(MYSQL_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(MYSQL_PORT)
                .localPort(MYSQL_PORT)
                .jdbcTemplate(MYSQL_URL)
                .jdbcUrl(jdbcUrl)
                .userName(MYSQL_USERNAME)
                .password(MYSQL_PASSWORD)
                .database(MYSQL_DATABASE)
                .sourceTable(MYSQL_SOURCE)
                .sinkTable(MYSQL_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .catalogDatabase(CATALOG_DATABASE)
                .catalogTable(MYSQL_SINK)
                .tablePathFullName(MYSQL_DATABASE + "." + MYSQL_SOURCE)
                .build();
    }

    @Override
    protected void checkResult(
            String executeKey, TestContainer container, Container.ExecResult execResult) {
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
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
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
                                    // https://github.com/apache/seatunnel/issues/5559 this value
                                    // cannot set null, this null
                                    // value column's row will be lost in
                                    // jdbc_mysql_source_and_sink_parallel.conf,jdbc_mysql_source_and_sink_parallel_upper_lower.conf.
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
        DockerImageName imageName = DockerImageName.parse(MYSQL_IMAGE);

        GenericContainer<?> container =
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

        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));

        return container;
    }

    @Override
    protected void initCatalog() {
        catalog =
                new MySqlCatalog(
                        "mysql",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        JdbcUrlUtil.getUrlInfo(
                                jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost())));
        catalog.open();
    }

    private String getUrl() {
        return URL.replace("HOST", dbServer.getHost());
    }

    @Test
    public void parametersTest() throws Exception {
        defaultSinkParametersTest();
        defaultSourceParametersTest();
    }

    void defaultSinkParametersTest() throws IOException, SQLException, ClassNotFoundException {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "c_bigint",
                                        BasicType.LONG_TYPE,
                                        22,
                                        false,
                                        null,
                                        "c_bigint"))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "seatunnel", "source"),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "User table");

        // case1 url not contains parameters and properties not contains parameters
        Map<String, Object> map1 = getDefaultConfigMap();
        map1.put("url", getUrl());
        ReadonlyConfig config1 = ReadonlyConfig.fromMap(map1);
        TableSinkFactoryContext context1 =
                TableSinkFactoryContext.replacePlaceholderAndCreate(
                        catalogTable,
                        config1,
                        Thread.currentThread().getContextClassLoader(),
                        Collections.emptyList());
        JdbcSink jdbcSink1 = (JdbcSink) new JdbcSinkFactory().createSink(context1).createSink();
        Properties connectionProperties1 = getSinkProperties(jdbcSink1);
        Assertions.assertEquals(connectionProperties1.get("rewriteBatchedStatements"), "true");

        // case2 url contains parameters and properties not contains parameters
        Map<String, Object> map2 = getDefaultConfigMap();
        map2.put("url", getUrl() + "?rewriteBatchedStatements=false");
        ReadonlyConfig config2 = ReadonlyConfig.fromMap(map2);
        TableSinkFactoryContext context2 =
                TableSinkFactoryContext.replacePlaceholderAndCreate(
                        catalogTable,
                        config2,
                        Thread.currentThread().getContextClassLoader(),
                        Collections.emptyList());
        JdbcSink jdbcSink2 = (JdbcSink) new JdbcSinkFactory().createSink(context2).createSink();
        Properties connectionProperties2 = getSinkProperties(jdbcSink2);
        Assertions.assertEquals(connectionProperties2.get("rewriteBatchedStatements"), "false");

        // case3 url not contains parameters and properties not contains parameters
        Map<String, Object> map3 = getDefaultConfigMap();
        Map<String, String> properties3 = new HashMap<>();
        properties3.put("rewriteBatchedStatements", "false");
        map3.put("properties", properties3);
        map3.put("url", getUrl());
        ReadonlyConfig config3 = ReadonlyConfig.fromMap(map3);
        TableSinkFactoryContext context3 =
                TableSinkFactoryContext.replacePlaceholderAndCreate(
                        catalogTable,
                        config3,
                        Thread.currentThread().getContextClassLoader(),
                        Collections.emptyList());
        JdbcSink jdbcSink3 = (JdbcSink) new JdbcSinkFactory().createSink(context3).createSink();
        Properties connectionProperties3 = getSinkProperties(jdbcSink3);
        Assertions.assertEquals(connectionProperties3.get("rewriteBatchedStatements"), "false");

        // case4 url contains parameters and properties contains parameters
        Map<String, Object> map4 = getDefaultConfigMap();
        Map<String, String> properties4 = new HashMap<>();
        properties4.put("useSSL", "true");
        properties4.put("rewriteBatchedStatements", "false");
        map4.put("properties", properties4);
        map4.put("url", getUrl() + "?useSSL=false&rewriteBatchedStatements=true");
        ReadonlyConfig config4 = ReadonlyConfig.fromMap(map4);
        TableSinkFactoryContext context4 =
                TableSinkFactoryContext.replacePlaceholderAndCreate(
                        catalogTable,
                        config4,
                        Thread.currentThread().getContextClassLoader(),
                        Collections.emptyList());
        JdbcSink jdbcSink4 = (JdbcSink) new JdbcSinkFactory().createSink(context4).createSink();
        Properties connectionProperties4 = getSinkProperties(jdbcSink4);
        Assertions.assertEquals(connectionProperties4.get("useSSL"), "true");
        Assertions.assertEquals(connectionProperties4.get("rewriteBatchedStatements"), "false");
    }

    void defaultSourceParametersTest() throws Exception {
        // case1 url not contains parameters and properties not contains parameters
        Map<String, Object> map1 = getDefaultConfigMap();
        map1.put("url", getUrl());
        map1.put("query", SQL);
        ReadonlyConfig config1 = ReadonlyConfig.fromMap(map1);
        TableSourceFactoryContext context1 =
                new TableSourceFactoryContext(
                        config1, Thread.currentThread().getContextClassLoader());
        JdbcSource jdbcSource1 =
                (JdbcSource)
                        new JdbcSourceFactory()
                                .<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>createSource(
                                        context1)
                                .createSource();
        Properties connectionProperties1 = getSourceProperties(jdbcSource1);
        Assertions.assertEquals(connectionProperties1.get("rewriteBatchedStatements"), "true");

        // case2 url contains parameters and properties not contains parameters
        Map<String, Object> map2 = getDefaultConfigMap();
        map2.put("url", getUrl() + "?rewriteBatchedStatements=false");
        map2.put("query", SQL);
        ReadonlyConfig config2 = ReadonlyConfig.fromMap(map2);
        TableSourceFactoryContext context2 =
                new TableSourceFactoryContext(
                        config2, Thread.currentThread().getContextClassLoader());
        JdbcSource jdbcSource2 =
                (JdbcSource)
                        new JdbcSourceFactory()
                                .<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>createSource(
                                        context2)
                                .createSource();
        Properties connectionProperties2 = getSourceProperties(jdbcSource2);
        Assertions.assertEquals(connectionProperties2.get("rewriteBatchedStatements"), "false");

        // case3 url not contains parameters and properties not contains parameters
        Map<String, Object> map3 = getDefaultConfigMap();
        Map<String, String> properties3 = new HashMap<>();
        properties3.put("rewriteBatchedStatements", "false");
        map3.put("properties", properties3);
        map3.put("url", getUrl());
        map3.put("query", SQL);
        ReadonlyConfig config3 = ReadonlyConfig.fromMap(map3);
        TableSourceFactoryContext context3 =
                new TableSourceFactoryContext(
                        config3, Thread.currentThread().getContextClassLoader());
        JdbcSource jdbcSource3 =
                (JdbcSource)
                        new JdbcSourceFactory()
                                .<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>createSource(
                                        context3)
                                .createSource();
        Properties connectionProperties3 = getSourceProperties(jdbcSource3);
        Assertions.assertEquals(connectionProperties3.get("rewriteBatchedStatements"), "false");

        // case4 url contains parameters and properties contains parameters
        Map<String, Object> map4 = getDefaultConfigMap();
        Map<String, String> properties4 = new HashMap<>();
        properties4.put("useSSL", "true");
        properties4.put("rewriteBatchedStatements", "false");
        map4.put("properties", properties4);
        map4.put("url", getUrl() + "?useSSL=false&rewriteBatchedStatements=true");
        map4.put("query", SQL);
        ReadonlyConfig config4 = ReadonlyConfig.fromMap(map4);
        TableSourceFactoryContext context4 =
                new TableSourceFactoryContext(
                        config4, Thread.currentThread().getContextClassLoader());
        JdbcSource jdbcSource4 =
                (JdbcSource)
                        new JdbcSourceFactory()
                                .<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>createSource(
                                        context4)
                                .createSource();
        Properties connectionProperties4 = getSourceProperties(jdbcSource4);
        Assertions.assertEquals(connectionProperties4.get("useSSL"), "true");
        Assertions.assertEquals(connectionProperties4.get("rewriteBatchedStatements"), "false");
    }

    @NotNull private Map<String, Object> getDefaultConfigMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("driver", "com.mysql.cj.jdbc.Driver");
        map.put("user", MYSQL_USERNAME);
        map.put("password", MYSQL_PASSWORD);
        return map;
    }

    private Properties getSinkProperties(JdbcSink jdbcSink)
            throws IOException, SQLException, ClassNotFoundException {
        JdbcSinkWriter jdbcSinkWriter =
                (JdbcSinkWriter)
                        jdbcSink.createWriter(new DefaultSinkWriterContext(Integer.MAX_VALUE, 1));
        JdbcConnectionProvider connectionProvider =
                (JdbcConnectionProvider)
                        ReflectionUtils.getField(jdbcSinkWriter, "connectionProvider").get();
        ConnectionImpl connection = (ConnectionImpl) connectionProvider.getOrEstablishConnection();
        Properties connectionProperties = connection.getProperties();
        return connectionProperties;
    }

    private Properties getSourceProperties(JdbcSource jdbcSource) throws Exception {
        JdbcSourceSplitEnumerator enumerator =
                ((JdbcSourceSplitEnumerator) jdbcSource.createEnumerator(null));
        ChunkSplitter splitter =
                ((ChunkSplitter) ReflectionUtils.getField(enumerator, "splitter").get());
        JdbcConnectionProvider connectionProvider =
                (JdbcConnectionProvider)
                        ReflectionUtils.getField(splitter, "connectionProvider").get();
        ConnectionImpl connection = (ConnectionImpl) connectionProvider.getOrEstablishConnection();
        Properties connectionProperties = connection.getProperties();
        return connectionProperties;
    }
}
