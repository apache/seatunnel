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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.duckdb.DuckDBDriver;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class FixedChunkSplitterTest {

    @Test
    public void testCreateFirstStringRangeSplitStatement() throws SQLException {
        CapturingFixedChunkSplitter splitter = new CapturingFixedChunkSplitter(mysqlConfig());
        JdbcSourceSplit split =
                new JdbcSourceSplit(
                        TablePath.of("db", "tbl"),
                        "split-0",
                        null,
                        "id",
                        BasicType.STRING_TYPE,
                        null,
                        "mm");

        splitter.generateSplitStatement(split, TableSchema.builder().build());

        assertEquals("SELECT * FROM `db`.`tbl` WHERE `id` <= ? AND NOT (`id` = ?)", splitter.sql);
        assertEquals("mm", splitter.stringParameters.get(1));
        assertEquals("mm", splitter.stringParameters.get(2));
    }

    @Test
    public void testCreateLastStringRangeSplitStatement() throws SQLException {
        CapturingFixedChunkSplitter splitter = new CapturingFixedChunkSplitter(mysqlConfig());
        JdbcSourceSplit split =
                new JdbcSourceSplit(
                        TablePath.of("db", "tbl"),
                        "split-1",
                        null,
                        "id",
                        BasicType.STRING_TYPE,
                        "mm",
                        null);

        splitter.generateSplitStatement(split, TableSchema.builder().build());

        assertEquals("SELECT * FROM `db`.`tbl` WHERE `id` >= ?", splitter.sql);
        assertEquals("mm", splitter.stringParameters.get(1));
    }

    @Test
    public void testRejectAutoStringRangeSplitForNonMysqlDialect() {
        JdbcSourceConfig config =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:postgresql://localhost:5432/test")
                                        .driverName("org.postgresql.Driver")
                                        .build())
                        .stringSplitStrategy(StringSplitStrategy.AUTO)
                        .build();
        CapturingFixedChunkSplitter splitter = new CapturingFixedChunkSplitter(config);
        JdbcSourceTable table =
                JdbcSourceTable.builder().tablePath(TablePath.of("public", "tbl")).build();

        JdbcConnectorException exception =
                assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                splitter.createSplits(
                                        table,
                                        new SeaTunnelRowType(
                                                new String[] {"id"},
                                                new SeaTunnelDataType[] {BasicType.STRING_TYPE})));

        assertTrue(exception.getMessage().contains("does not support range/auto"));
    }

    @Test
    public void testConvertFloat() throws Exception {
        JdbcSourceConfig config =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:postgresql://localhost:5432/test")
                                        .driverName("org.postgresql.Driver")
                                        .build())
                        .build();

        FixedChunkSplitter splitter = new FixedChunkSplitter(config);

        // Use reflection to access private method
        Method convertToBigDecimalMethod =
                FixedChunkSplitter.class.getDeclaredMethod("convertToBigDecimal", Object.class);
        convertToBigDecimalMethod.setAccessible(true);

        // Test precision-sensitive Float values
        Float testFloat = 123.456f;
        BigDecimal result = (BigDecimal) convertToBigDecimalMethod.invoke(splitter, testFloat);

        // Verify that using toString() method prevents precision loss
        BigDecimal expected = new BigDecimal(testFloat.toString());
        assertEquals(expected, result);

        // Verify the difference from the old method (this test should demonstrate the fix
        // necessity)
        BigDecimal oldWay = BigDecimal.valueOf(testFloat);
        assertNotEquals(oldWay, result);

        // Test boundary values
        Float maxFloat = Float.MAX_VALUE;
        BigDecimal maxResult = (BigDecimal) convertToBigDecimalMethod.invoke(splitter, maxFloat);
        assertEquals(new BigDecimal(maxFloat.toString()), maxResult);

        Float minFloat = Float.MIN_VALUE;
        BigDecimal minResult = (BigDecimal) convertToBigDecimalMethod.invoke(splitter, minFloat);
        assertEquals(new BigDecimal(minFloat.toString()), minResult);

        // Test values that better demonstrate precision issues
        Float precisionTestFloat = 0.1f;
        BigDecimal precisionResult =
                (BigDecimal) convertToBigDecimalMethod.invoke(splitter, precisionTestFloat);
        assertEquals(new BigDecimal("0.1"), precisionResult);

        // Verify that the old method indeed has precision issues
        BigDecimal oldPrecisionWay = BigDecimal.valueOf(precisionTestFloat);
        assertNotEquals(new BigDecimal("0.1"), oldPrecisionWay);
    }

    private static JdbcSourceConfig mysqlConfig() {
        return JdbcSourceConfig.builder()
                .jdbcConnectionConfig(
                        JdbcConnectionConfig.builder()
                                .url("jdbc:mysql://localhost:3306/test")
                                .driverName("com.mysql.cj.jdbc.Driver")
                                .build())
                .stringSplitStrategy(StringSplitStrategy.RANGE)
                .build();
    }

    private static class CapturingFixedChunkSplitter extends FixedChunkSplitter {
        private String sql;
        private final Map<Integer, String> stringParameters = new HashMap<>();

        private CapturingFixedChunkSplitter(JdbcSourceConfig config) {
            super(config);
        }

        @Override
        protected PreparedStatement createPreparedStatement(String sql) {
            this.sql = sql;
            InvocationHandler handler =
                    (proxy, method, args) -> {
                        if ("setString".equals(method.getName())) {
                            stringParameters.put((Integer) args[0], (String) args[1]);
                        }
                        return null;
                    };
            return (PreparedStatement)
                    Proxy.newProxyInstance(
                            PreparedStatement.class.getClassLoader(),
                            new Class<?>[] {PreparedStatement.class},
                            handler);
        }
    }

    private static final String DUCKDB_URL = "jdbc:duckdb:";

    @Test
    public void testNumericSplitsReadEveryRowExactlyOnce() throws Exception {
        try (Connection connection = new DuckDBDriver().connect(DUCKDB_URL, new Properties());
                DuckDbFixedChunkSplitter splitter =
                        new DuckDbFixedChunkSplitter(duckDbConfig(), connection)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE t (id BIGINT)");
                statement.execute("INSERT INTO t SELECT * FROM range(1, 2002)");
            }

            JdbcSourceTable table =
                    JdbcSourceTable.builder()
                            .tablePath(TablePath.of("main", "t"))
                            .query("SELECT id FROM t")
                            .partitionColumn("id")
                            .partitionStart("1")
                            .partitionEnd("2001")
                            .partitionNumber(1000)
                            .build();

            Collection<JdbcSourceSplit> splits = splitter.createSplits(table, splitKeyType());
            List<Long> ids = readAllIds(splitter, splits);

            assertEquals(2001, ids.size(), "every row must be read exactly once");
            List<Long> distinct = new ArrayList<>(new LinkedHashSet<>(ids));
            assertEquals(2001, distinct.size(), "no row may be read twice");
            Collections.sort(distinct);
            assertEquals(
                    LongStream.rangeClosed(1, 2001).boxed().collect(Collectors.toList()), distinct);
        }
    }

    @Test
    public void testSignedBigIntBoundariesAreSplitWithoutNarrowing() throws Exception {
        try (Connection connection = new DuckDBDriver().connect(DUCKDB_URL, new Properties());
                DuckDbFixedChunkSplitter splitter =
                        new DuckDbFixedChunkSplitter(duckDbConfig(), connection)) {
            long[] boundaries = {Long.MIN_VALUE, -1L, 0L, Long.MAX_VALUE};
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE t (id BIGINT)");
            }
            try (PreparedStatement insert =
                    connection.prepareStatement("INSERT INTO t (id) VALUES (?)")) {
                for (long value : boundaries) {
                    insert.setLong(1, value);
                    insert.addBatch();
                }
                insert.executeBatch();
            }

            JdbcSourceTable table =
                    JdbcSourceTable.builder()
                            .tablePath(TablePath.of("main", "t"))
                            .query("SELECT id FROM t")
                            .partitionColumn("id")
                            .partitionStart(String.valueOf(Long.MIN_VALUE))
                            .partitionEnd(String.valueOf(Long.MAX_VALUE))
                            .partitionNumber(2)
                            .build();

            Collection<JdbcSourceSplit> splits = splitter.createSplits(table, splitKeyType());
            List<Long> ids = readAllIds(splitter, splits);

            assertEquals(4, ids.size(), "each signed BIGINT boundary must be read exactly once");
            List<Long> sorted = new ArrayList<>(ids);
            Collections.sort(sorted);
            assertEquals(Arrays.asList(Long.MIN_VALUE, -1L, 0L, Long.MAX_VALUE), sorted);
        }
    }

    @Test
    public void testConfiguredBoundsCoverRangeAndExcludeOutsiders() throws Exception {
        try (Connection connection = new DuckDBDriver().connect(DUCKDB_URL, new Properties());
                DuckDbFixedChunkSplitter splitter =
                        new DuckDbFixedChunkSplitter(duckDbConfig(), connection)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE t (id BIGINT)");
                statement.execute("INSERT INTO t SELECT * FROM range(0, 12)");
            }

            JdbcSourceTable table =
                    JdbcSourceTable.builder()
                            .tablePath(TablePath.of("main", "t"))
                            .query("SELECT id FROM t")
                            .partitionColumn("id")
                            .partitionStart("1")
                            .partitionEnd("10")
                            .partitionNumber(3)
                            .build();

            Collection<JdbcSourceSplit> splits = splitter.createSplits(table, splitKeyType());
            List<Long> ids = readAllIds(splitter, splits);
            assertEquals(10, ids.size());
            Set<Long> distinct = new HashSet<>(ids);

            assertEquals(
                    LongStream.rangeClosed(1, 10).boxed().collect(Collectors.toSet()),
                    distinct,
                    "the configured bounds must cover 1..10");
            assertFalse(
                    distinct.contains(0L), "values below the configured start must be excluded");
            assertFalse(distinct.contains(11L), "values above the configured end must be excluded");
        }
    }

    private static List<Long> readAllIds(ChunkSplitter splitter, Collection<JdbcSourceSplit> splits)
            throws SQLException {
        List<Long> ids = new ArrayList<>();
        for (JdbcSourceSplit split : splits) {
            try (PreparedStatement statement =
                            splitter.generateSplitStatement(split, TableSchema.builder().build());
                    ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    ids.add(resultSet.getLong(1));
                }
            }
        }
        return ids;
    }

    private static JdbcSourceConfig duckDbConfig() {
        return JdbcSourceConfig.builder()
                .jdbcConnectionConfig(
                        JdbcConnectionConfig.builder()
                                .url(DUCKDB_URL)
                                .driverName("org.duckdb.DuckDBDriver")
                                .build())
                .build();
    }

    private static SeaTunnelRowType splitKeyType() {
        return new SeaTunnelRowType(
                new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE});
    }

    private static final class DuckDbFixedChunkSplitter extends FixedChunkSplitter {

        private final Connection connection;

        private DuckDbFixedChunkSplitter(JdbcSourceConfig config, Connection connection) {
            super(config);
            this.connection = connection;
        }

        @Override
        protected PreparedStatement createPreparedStatement(String sql) throws SQLException {
            return connection.prepareStatement(sql);
        }
    }
}
