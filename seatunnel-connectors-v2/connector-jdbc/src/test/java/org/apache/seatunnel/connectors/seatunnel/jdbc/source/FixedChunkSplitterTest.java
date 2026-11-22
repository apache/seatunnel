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

import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.duckdb.DuckDBDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
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
import java.util.TimeZone;
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
    public void testDateSplitsWithIsoAndLegacyBoundsBindJdbcDates() throws Exception {
        for (String[] bounds :
                new String[][] {
                    {"2024-01-01", "2024-01-03"},
                    {"19723", "19725"}
                }) {
            CapturingFixedChunkSplitter splitter = new CapturingFixedChunkSplitter(mysqlConfig());
            assertDateSplits(splitter, dateTable(bounds[0], bounds[1]));
        }
    }

    @Test
    public void testDateSplitsDiscoverDateAndTimestampMinMax() throws Exception {
        for (Object[] bounds :
                new Object[][] {
                    {Date.valueOf("2024-01-01"), Date.valueOf("2024-01-03")},
                    {
                        Timestamp.valueOf("2024-01-01 12:34:56"),
                        Timestamp.valueOf("2024-01-03 12:34:56")
                    }
                }) {
            CapturingFixedChunkSplitter splitter =
                    new CapturingFixedChunkSplitter(mysqlConfig()) {
                        @Override
                        protected Pair<Object, Object> queryMinMax(
                                JdbcSourceTable table, String columnName) {
                            return Pair.of(bounds[0], bounds[1]);
                        }
                    };
            assertDateSplits(splitter, dateTable(null, null));
        }
    }

    @Test
    @ResourceLock("java.util.TimeZone")
    public void testDateSplitsPreserveDaysAcrossTimeZoneSerialization() throws Exception {
        TimeZone original = TimeZone.getDefault();
        try {
            for (String[] zones :
                    new String[][] {
                        {"Asia/Shanghai", "America/Los_Angeles"},
                        {"Etc/GMT+12", "Pacific/Kiritimati"}
                    }) {
                TimeZone.setDefault(TimeZone.getTimeZone(zones[0]));
                CapturingFixedChunkSplitter splitter =
                        new CapturingFixedChunkSplitter(mysqlConfig());
                DefaultSerializer<JdbcSourceSplit> serializer = new DefaultSerializer<>();
                List<byte[]> serialized = new ArrayList<>();
                for (JdbcSourceSplit split :
                        splitter.createSplits(dateTable("2024-01-01", "2024-01-03"), dateKey())) {
                    serialized.add(serializer.serialize(split));
                }
                TimeZone.setDefault(TimeZone.getTimeZone(zones[1]));
                for (int i = 0; i < serialized.size(); i++) {
                    JdbcSourceSplit restored = serializer.deserialize(serialized.get(i));
                    splitter.generateSplitStatement(restored, TableSchema.builder().build());
                    LocalDate expected = LocalDate.of(2024, 1, 1).plusDays(i);
                    assertEquals(expected, splitter.dateParameters.get(1).toLocalDate());
                    assertEquals(expected, splitter.dateParameters.get(2).toLocalDate());
                    assertTrue(splitter.bigDecimalParameters.isEmpty());
                }
                assertEquals(3, serialized.size());
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    public void testRejectInvalidAndFractionalDateBounds() {
        FixedChunkSplitter splitter = new FixedChunkSplitter(mysqlConfig());
        for (String bound : new String[] {"2024-01-03 00:00:00", "20000.5"}) {
            JdbcConnectorException exception =
                    assertThrows(
                            JdbcConnectorException.class,
                            () -> splitter.createSplits(dateTable(bound, "2024-01-03"), dateKey()));
            assertTrue(exception.getMessage().contains("DATE"));
        }
    }

    @Test
    public void testRejectOutOfRangeDateBounds() {
        FixedChunkSplitter splitter = new FixedChunkSplitter(mysqlConfig());
        for (String bound :
                new String[] {String.valueOf(Long.MAX_VALUE), "1000000000000000000000000000000"}) {
            JdbcConnectorException error =
                    assertThrows(
                            JdbcConnectorException.class,
                            () -> splitter.createSplits(dateTable(bound, bound), dateKey()));
            assertTrue(error.getMessage().contains("DATE partition bound"));
        }
    }

    @Test
    public void testExistingJdbcDateSplitParametersRemainSupported() throws Exception {
        CapturingFixedChunkSplitter splitter = new CapturingFixedChunkSplitter(mysqlConfig());
        Date date = Date.valueOf("2024-01-01");
        JdbcSourceSplit split =
                new JdbcSourceSplit(
                        TablePath.of("db", "tbl"),
                        "legacy-date",
                        null,
                        "create_date",
                        LocalTimeType.LOCAL_DATE_TYPE,
                        date,
                        date);
        splitter.generateSplitStatement(split, TableSchema.builder().build());
        assertEquals(date, splitter.dateParameters.get(1));
        assertEquals(date, splitter.dateParameters.get(2));
    }

    @Test
    public void testNonDateTemporalPartitionColumnsRemainUnsupported() {
        FixedChunkSplitter splitter = new FixedChunkSplitter(mysqlConfig());
        for (SeaTunnelDataType<?> type :
                new SeaTunnelDataType<?>[] {
                    LocalTimeType.LOCAL_TIME_TYPE,
                    LocalTimeType.LOCAL_DATE_TIME_TYPE,
                    LocalTimeType.OFFSET_DATE_TIME_TYPE
                }) {
            TableSchema schema =
                    TableSchema.builder()
                            .column(PhysicalColumn.of("created_at", type, 0L, true, null, null))
                            .build();
            JdbcSourceTable table =
                    JdbcSourceTable.builder()
                            .tablePath(TablePath.of("db", "tbl"))
                            .partitionColumn("created_at")
                            .catalogTable(
                                    CatalogTable.of(
                                            TableIdentifier.of("catalog", "db", "tbl"),
                                            schema,
                                            Collections.emptyMap(),
                                            Collections.emptyList(),
                                            ""))
                            .build();
            assertThrows(JdbcConnectorException.class, () -> splitter.generateSplits(table));
        }
    }

    private static JdbcSourceTable dateTable(String lower, String upper) {
        return JdbcSourceTable.builder()
                .tablePath(TablePath.of("db", "tbl"))
                .partitionNumber(3)
                .partitionStart(lower)
                .partitionEnd(upper)
                .build();
    }

    private static SeaTunnelRowType dateKey() {
        return new SeaTunnelRowType(
                new String[] {"create_date"},
                new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});
    }

    private static void assertDateSplits(
            CapturingFixedChunkSplitter splitter, JdbcSourceTable table) throws Exception {
        List<JdbcSourceSplit> splits = new ArrayList<>(splitter.createSplits(table, dateKey()));
        assertEquals(3, splits.size());
        for (int i = 0; i < splits.size(); i++) {
            Date expected = Date.valueOf(LocalDate.of(2024, 1, 1).plusDays(i));
            JdbcSourceSplit split = splits.get(i);
            assertEquals(expected.toLocalDate(), split.getSplitStart());
            assertEquals(expected.toLocalDate(), split.getSplitEnd());
            splitter.generateSplitStatement(split, TableSchema.builder().build());
            assertEquals(expected, splitter.dateParameters.get(1));
            assertEquals(expected, splitter.dateParameters.get(2));
            assertTrue(splitter.bigDecimalParameters.isEmpty());
        }
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
                .enableConcurrentRead(true)
                .build();
    }

    private static class CapturingFixedChunkSplitter extends FixedChunkSplitter {
        private String sql;
        private final Map<Integer, String> stringParameters = new HashMap<>();
        private final Map<Integer, Date> dateParameters = new HashMap<>();
        private final Map<Integer, BigDecimal> bigDecimalParameters = new HashMap<>();

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
                        } else if ("setDate".equals(method.getName())) {
                            dateParameters.put((Integer) args[0], (Date) args[1]);
                        } else if ("setBigDecimal".equals(method.getName())) {
                            bigDecimalParameters.put((Integer) args[0], (BigDecimal) args[1]);
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

    @Test
    public void testDateSplitsUseNumericRangeFixForUnevenAndExcessPartitions() throws Exception {
        try (Connection connection = new DuckDBDriver().connect(DUCKDB_URL, new Properties());
                DuckDbFixedChunkSplitter splitter =
                        new DuckDbFixedChunkSplitter(duckDbConfig(), connection)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE t (id BIGINT, create_date DATE)");
            }
            try (PreparedStatement insert =
                    connection.prepareStatement("INSERT INTO t VALUES (?, ?)")) {
                for (int id = 0; id <= 11; id++) {
                    insert.setInt(1, id);
                    insert.setDate(2, Date.valueOf(LocalDate.of(2024, 1, 1).plusDays(id - 1)));
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            for (int partitions : new int[] {3, 20}) {
                JdbcSourceTable table =
                        JdbcSourceTable.builder()
                                .tablePath(TablePath.of("main", "t"))
                                .query("SELECT id, create_date FROM t")
                                .partitionColumn("create_date")
                                .partitionStart("2024-01-01")
                                .partitionEnd("2024-01-10")
                                .partitionNumber(partitions)
                                .build();
                Collection<JdbcSourceSplit> splits = splitter.createSplits(table, dateKey());
                assertEquals(Math.min(partitions, 10), splits.size());
                List<Long> ids = readAllIds(splitter, splits);
                Collections.sort(ids);
                assertEquals(
                        LongStream.rangeClosed(1, 10).boxed().collect(Collectors.toList()),
                        ids,
                        "DATE ranges must retain every day exactly once and exclude outsiders");
            }
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
