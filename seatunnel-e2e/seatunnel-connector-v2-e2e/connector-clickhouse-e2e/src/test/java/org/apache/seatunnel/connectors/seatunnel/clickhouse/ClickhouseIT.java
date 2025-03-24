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

package org.apache.seatunnel.connectors.seatunnel.clickhouse;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;
import org.testcontainers.utility.DockerLoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClickhouseIT extends TestSuiteBase implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseIT.class);
    private static final String CLICKHOUSE_DOCKER_IMAGE = "clickhouse/clickhouse-server:23.3.13.6";
    private static final String HOST = "clickhouse";
    private static final String DRIVER_CLASS = "com.clickhouse.jdbc.ClickHouseDriver";
    private static final String INIT_CLICKHOUSE_PATH = "/init/clickhouse_init.conf";
    private static final String CLICKHOUSE_JOB_CONFIG = "/clickhouse_to_clickhouse.conf";
    private static final String DATABASE = "default";
    private static final String SOURCE_TABLE = "source_table";
    private static final String SINK_TABLE = "sink_table";
    private static final String INSERT_SQL = "insert_sql";
    private static final String COMPARE_SQL = "compare_sql";
    private static final Pair<SeaTunnelRowType, List<SeaTunnelRow>> TEST_DATASET =
            generateTestDataSet();
    private static final Config CONFIG = getInitClickhouseConfig();
    private ClickHouseContainer container;
    private Connection connection;

    @TestTemplate
    public void testClickhouse(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob(CLICKHOUSE_JOB_CONFIG);
        Assertions.assertEquals(0, execResult.getExitCode());
        assertHasData(SINK_TABLE);
        compareResult();
        clearSinkTable();
    }

    @TestTemplate
    public void testSourceParallelism(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/clickhouse_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testClickhouseWithCreateSchemaWhenComment(TestContainer container)
            throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/clickhouse_with_create_schema_when_comment.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void clickhouseWithCreateSchemaWhenNotExist(TestContainer container) throws Exception {
        String tableName = "default.sink_table_for_schema";
        Container.ExecResult execResult =
                container.executeJob("/clickhouse_with_create_schema_when_not_exist.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(100, countData(tableName));
        execResult = container.executeJob("/clickhouse_with_create_schema_when_not_exist.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(200, countData(tableName));
        dropTable(tableName);
    }

    @TestTemplate
    public void clickhouseWithRecreateSchemaAndAppendData(TestContainer container)
            throws Exception {
        String tableName = "default.sink_table_for_schema";
        Container.ExecResult execResult =
                container.executeJob("/clickhouse_with_recreate_schema_and_append_data.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(100, countData(tableName));
        execResult = container.executeJob("/clickhouse_with_recreate_schema_and_append_data.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(100, countData(tableName));
        dropTable(tableName);
    }

    @TestTemplate
    public void clickhouseWithErrorWhenSchemaNotExist(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/clickhouse_with_error_when_schema_not_exist.conf");
        Assertions.assertEquals(1, execResult.getExitCode());
        Assertions.assertTrue(
                execResult
                        .getStderr()
                        .contains(
                                "ErrorCode:[API-11], ErrorDescription:[The sink table not exist]"));
    }

    @TestTemplate
    public void clickhouseWithCreateSchemaWhenNotExistAndDropData(TestContainer container)
            throws Exception {
        String tableName = "default.sink_table_for_schema";
        Container.ExecResult execResult =
                container.executeJob(
                        "/clickhouse_with_create_schema_when_not_exist_and_drop_data.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(100, countData(tableName));
        execResult =
                container.executeJob(
                        "/clickhouse_with_create_schema_when_not_exist_and_drop_data.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(100, countData(tableName));
        dropTable(tableName);
    }

    @TestTemplate
    public void clickhouseWithErrorWhenDataExists(TestContainer container) throws Exception {
        String tableName = "default.sink_table_for_schema";
        Container.ExecResult execResult =
                container.executeJob("/clickhouse_with_error_when_data_exists.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(100, countData(tableName));
        execResult = container.executeJob("/clickhouse_with_error_when_data_exists.conf");
        Assertions.assertEquals(1, execResult.getExitCode());
        Assertions.assertTrue(
                execResult.getStderr().contains("The target data source already has data"));
        dropTable(tableName);
    }

    @TestTemplate
    public void clickhouseRecreateSchemaAndCustom(TestContainer container) throws Exception {
        String tableName = "default.sink_table_for_schema";
        Container.ExecResult execResult =
                container.executeJob("/clickhouse_with_recreate_schema_and_custom.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(101, countData(tableName));
        dropTable(tableName);
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.container =
                new ClickHouseContainer(CLICKHOUSE_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CLICKHOUSE_DOCKER_IMAGE)));
        Startables.deepStart(Stream.of(this.container)).join();
        LOG.info("Clickhouse container started");
        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(360L, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
        this.initializeClickhouseTable();
        this.batchInsertData();
    }

    private void initializeClickhouseTable() {
        try {
            Statement statement = this.connection.createStatement();
            statement.execute(CONFIG.getString(SOURCE_TABLE));
            statement.execute(CONFIG.getString(SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Clickhouse table failed!", e);
        }
    }

    private void initConnection()
            throws SQLException, ClassNotFoundException, InstantiationException,
                    IllegalAccessException {
        final Properties info = new Properties();
        info.put("user", this.container.getUsername());
        info.put("password", this.container.getPassword());
        this.connection =
                ((Driver) Class.forName(DRIVER_CLASS).newInstance())
                        .connect(this.container.getJdbcUrl(), info);
    }

    private static Config getInitClickhouseConfig() {
        File file = ContainerUtil.getResourcesFile(INIT_CLICKHOUSE_PATH);
        Config config = ConfigFactory.parseFile(file);
        assert config.hasPath(SOURCE_TABLE)
                && config.hasPath(SINK_TABLE)
                && config.hasPath(INSERT_SQL)
                && config.hasPath(COMPARE_SQL);
        return config;
    }

    private Array toSqlArray(Object value) throws SQLException {
        Object[] elements = null;
        String sqlType = null;
        if (String[].class.equals(value.getClass())) {
            sqlType = "TEXT";
            elements = (String[]) value;
        } else if (Boolean[].class.equals(value.getClass())) {
            sqlType = "BOOLEAN";
            elements = (Boolean[]) value;
        } else if (Byte[].class.equals(value.getClass())) {
            sqlType = "TINYINT";
            elements = (Byte[]) value;
        } else if (Short[].class.equals(value.getClass())) {
            sqlType = "SMALLINT";
            elements = (Short[]) value;
        } else if (Integer[].class.equals(value.getClass())) {
            sqlType = "INTEGER";
            elements = (Integer[]) value;
        } else if (Long[].class.equals(value.getClass())) {
            sqlType = "BIGINT";
            elements = (Long[]) value;
        } else if (Float[].class.equals(value.getClass())) {
            sqlType = "REAL";
            elements = (Float[]) value;
        } else if (Double[].class.equals(value.getClass())) {
            sqlType = "DOUBLE";
            elements = (Double[]) value;
        }
        if (sqlType == null) {
            throw new IllegalArgumentException(
                    "array inject error, not supported data type: " + value.getClass());
        }
        return connection.createArrayOf(sqlType, elements);
    }

    private int countData(String tableName) {
        try {
            String sql = "select count(1) from " + tableName;
            ResultSet resultSet = this.connection.createStatement().executeQuery(sql);
            if (resultSet.next()) {
                return resultSet.getInt(1);
            } else {
                return -1;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void dropTable(String tableName) {
        try {
            Statement statement = this.connection.createStatement();
            statement.execute("drop table if exists " + tableName);
        } catch (SQLException e) {
            throw new RuntimeException("Drop table failed!", e);
        }
    }

    private void batchInsertData() {
        String sql = CONFIG.getString(INSERT_SQL);
        PreparedStatement preparedStatement = null;
        try {
            this.connection.setAutoCommit(true);
            preparedStatement = this.connection.prepareStatement(sql);
            for (SeaTunnelRow row : TEST_DATASET.getValue()) {
                preparedStatement.setLong(1, (Long) row.getField(0));
                preparedStatement.setObject(2, row.getField(1));
                preparedStatement.setArray(3, toSqlArray(row.getField(2)));
                preparedStatement.setArray(4, toSqlArray(row.getField(3)));
                preparedStatement.setArray(5, toSqlArray(row.getField(4)));
                preparedStatement.setArray(6, toSqlArray(row.getField(5)));
                preparedStatement.setArray(7, toSqlArray(row.getField(6)));
                preparedStatement.setArray(8, toSqlArray(row.getField(7)));
                preparedStatement.setString(9, (String) row.getField(8));
                preparedStatement.setBoolean(10, (Boolean) row.getField(9));
                preparedStatement.setByte(11, (Byte) row.getField(10));
                preparedStatement.setShort(12, (Short) row.getField(11));
                preparedStatement.setInt(13, (Integer) row.getField(12));
                preparedStatement.setLong(14, (Long) row.getField(13));
                preparedStatement.setFloat(15, (Float) row.getField(14));
                preparedStatement.setDouble(16, (Double) row.getField(15));
                preparedStatement.setBigDecimal(17, (BigDecimal) row.getField(16));
                preparedStatement.setDate(18, Date.valueOf((LocalDate) row.getField(17)));
                preparedStatement.setTimestamp(
                        19, Timestamp.valueOf((LocalDateTime) row.getField(18)));
                preparedStatement.setInt(20, (Integer) row.getField(19));
                preparedStatement.setString(21, (String) row.getField(20));
                preparedStatement.setArray(22, toSqlArray(row.getField(21)));
                preparedStatement.setArray(23, toSqlArray(row.getField(22)));
                preparedStatement.setArray(24, toSqlArray(row.getField(23)));
                preparedStatement.setObject(25, row.getField(24));
                preparedStatement.setObject(26, row.getField(25));
                preparedStatement.setObject(27, row.getField(26));
                preparedStatement.setObject(28, row.getField(27));
                preparedStatement.setObject(29, row.getField(28));
                preparedStatement.setObject(30, row.getField(29));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Batch insert data failed!", e);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException("PreparedStatement close failed!", e);
                }
            }
        }
    }

    private static Pair<SeaTunnelRowType, List<SeaTunnelRow>> generateTestDataSet() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {
                            "id",
                            "c_map",
                            "c_array_string",
                            "c_array_short",
                            "c_array_int",
                            "c_array_long",
                            "c_array_float",
                            "c_array_double",
                            "c_string",
                            "c_boolean",
                            "c_int8",
                            "c_int16",
                            "c_int32",
                            "c_int64",
                            "c_float32",
                            "c_float64",
                            "c_decimal",
                            "c_date",
                            "c_datetime",
                            "c_nullable",
                            "c_lowcardinality",
                            "c_nested.int",
                            "c_nested.double",
                            "c_nested.string",
                            "c_int128",
                            "c_uint128",
                            "c_int256",
                            "c_uint256",
                            "c_point",
                            "c_ring"
                        },
                        new SeaTunnelDataType[] {
                            BasicType.LONG_TYPE,
                            new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE),
                            ArrayType.STRING_ARRAY_TYPE,
                            ArrayType.SHORT_ARRAY_TYPE,
                            ArrayType.INT_ARRAY_TYPE,
                            ArrayType.LONG_ARRAY_TYPE,
                            ArrayType.FLOAT_ARRAY_TYPE,
                            ArrayType.DOUBLE_ARRAY_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.BYTE_TYPE,
                            BasicType.SHORT_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            new DecimalType(9, 4),
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            ArrayType.INT_ARRAY_TYPE,
                            ArrayType.DOUBLE_ARRAY_TYPE,
                            ArrayType.STRING_ARRAY_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE
                        });
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                (long) i,
                                Collections.singletonMap("key", Integer.parseInt("1")),
                                new String[] {"string"},
                                new Short[] {Short.parseShort("1")},
                                new Integer[] {Integer.parseInt("1")},
                                new Long[] {Long.parseLong("1")},
                                new Float[] {Float.parseFloat("1.1")},
                                new Double[] {Double.parseDouble("1.1")},
                                "string",
                                Boolean.FALSE,
                                Byte.parseByte("1"),
                                Short.parseShort("1"),
                                Integer.parseInt("1"),
                                Long.parseLong("1"),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                BigDecimal.valueOf(11L, 1),
                                LocalDate.now(),
                                LocalDateTime.now(),
                                i,
                                "string",
                                new Integer[] {Integer.parseInt("1")},
                                new Double[] {Double.parseDouble("1.1")},
                                new String[] {"1"},
                                "170141183460469231731687303715884105727",
                                "340282366920938463463374607431768211455",
                                "57896044618658097711785492504343953926634992332820282019728792003956564819967",
                                "115792089237316195423570985008687907853269984665640564039457584007913129639935",
                                new double[] {1, 2},
                                new double[][] {{2, 3}, {4, 5}}
                            });
            rows.add(row);
        }
        return Pair.of(rowType, rows);
    }

    private void compareResult() throws SQLException, IOException {
        String sourceSql = "select * from " + SOURCE_TABLE + " order by id";
        String sinkSql = "select * from " + SINK_TABLE + " order by id";
        List<String> columnList =
                Arrays.stream(generateTestDataSet().getKey().getFieldNames())
                        .collect(Collectors.toList());
        try (Statement sourceStatement = connection.createStatement();
                Statement sinkStatement = connection.createStatement();
                ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
                ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql)) {
            Assertions.assertEquals(
                    sourceResultSet.getMetaData().getColumnCount(),
                    sinkResultSet.getMetaData().getColumnCount());
            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                    for (String column : columnList) {
                        Object source = sourceResultSet.getObject(column);
                        Object sink = sinkResultSet.getObject(column);
                        if (!Objects.deepEquals(source, sink)) {
                            InputStream sourceAsciiStream = sourceResultSet.getBinaryStream(column);
                            InputStream sinkAsciiStream = sinkResultSet.getBinaryStream(column);
                            String sourceValue =
                                    IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
                            String sinkValue =
                                    IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
                            Assertions.assertEquals(sourceValue, sinkValue);
                        }
                        Assertions.assertTrue(true);
                    }
                }
            }
            String columns = String.join(",", generateTestDataSet().getKey().getFieldNames());
            Assertions.assertTrue(
                    compare(String.format(CONFIG.getString(COMPARE_SQL), columns, columns)));
        }
    }

    private Boolean compare(String sql) {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            return !resultSet.next();
        } catch (SQLException e) {
            throw new RuntimeException("result compare error", e);
        }
    }

    private void assertHasData(String table) {
        String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
        try (Statement statement = connection.createStatement();
                ResultSet source = statement.executeQuery(sql); ) {
            Assertions.assertTrue(source.next());
        } catch (SQLException e) {
            throw new RuntimeException("test clickhouse server image error", e);
        }
    }

    private void clearSinkTable() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("truncate table %s.%s", DATABASE, SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("Test clickhouse server image error", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (this.connection != null) {
            this.connection.close();
        }
        if (this.container != null) {
            this.container.stop();
        }
    }
}
