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
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public void testCreateDateColumnSplitsUseSqlDateBoundaries() throws Exception {
        FixedChunkSplitter splitter = new FixedChunkSplitter(mysqlConfig());
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of("db", "tbl"))
                        .partitionNumber(3)
                        .build();
        Method createNumberColumnSplitsMethod =
                FixedChunkSplitter.class.getDeclaredMethod(
                        "createNumberColumnSplits",
                        JdbcSourceTable.class,
                        String.class,
                        SeaTunnelDataType.class,
                        BigDecimal.class,
                        BigDecimal.class);
        createNumberColumnSplitsMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        Collection<JdbcSourceSplit> splits =
                (Collection<JdbcSourceSplit>)
                        createNumberColumnSplitsMethod.invoke(
                                splitter,
                                table,
                                "create_date",
                                LocalTimeType.LOCAL_DATE_TYPE,
                                BigDecimal.valueOf(
                                        Date.valueOf("2024-01-01").toLocalDate().toEpochDay()),
                                BigDecimal.valueOf(
                                        Date.valueOf("2024-01-03").toLocalDate().toEpochDay()));

        List<JdbcSourceSplit> splitList = new ArrayList<>(splits);
        assertEquals(3, splitList.size());
        assertEquals(Date.valueOf("2024-01-01"), splitList.get(0).getSplitStart());
        assertEquals(Date.valueOf("2024-01-01"), splitList.get(0).getSplitEnd());
        assertEquals(Date.valueOf("2024-01-02"), splitList.get(1).getSplitStart());
        assertEquals(Date.valueOf("2024-01-02"), splitList.get(1).getSplitEnd());
        assertEquals(Date.valueOf("2024-01-03"), splitList.get(2).getSplitStart());
        assertEquals(Date.valueOf("2024-01-03"), splitList.get(2).getSplitEnd());
    }

    @Test
    public void testCreateDateSplitStatementBindsSqlDateParameters() throws SQLException {
        CapturingFixedChunkSplitter splitter = new CapturingFixedChunkSplitter(mysqlConfig());
        JdbcSourceSplit split =
                new JdbcSourceSplit(
                        TablePath.of("db", "tbl"),
                        "split-0",
                        null,
                        "create_date",
                        LocalTimeType.LOCAL_DATE_TYPE,
                        Date.valueOf("2024-01-01"),
                        Date.valueOf("2024-01-02"));

        splitter.generateSplitStatement(split, TableSchema.builder().build());

        assertEquals(
                "SELECT * FROM `db`.`tbl` WHERE `create_date` >= ? AND `create_date` <= ?",
                splitter.sql);
        assertEquals(Date.valueOf("2024-01-01"), splitter.dateParameters.get(1));
        assertEquals(Date.valueOf("2024-01-02"), splitter.dateParameters.get(2));
        assertFalse(splitter.bigDecimalParameters.containsKey(1));
        assertFalse(splitter.bigDecimalParameters.containsKey(2));
    }

    @Test
    public void testConvertTemporalSplitBoundaries() throws Exception {
        FixedChunkSplitter splitter = new FixedChunkSplitter(mysqlConfig());
        Method convertSplitBoundaryMethod =
                FixedChunkSplitter.class.getDeclaredMethod(
                        "convertSplitBoundary", SeaTunnelDataType.class, Serializable.class);
        convertSplitBoundaryMethod.setAccessible(true);

        assertEquals(
                new Time(1000),
                convertSplitBoundaryMethod.invoke(
                        splitter, LocalTimeType.LOCAL_TIME_TYPE, BigDecimal.valueOf(1000)));
        assertEquals(
                new Timestamp(2000),
                convertSplitBoundaryMethod.invoke(
                        splitter, LocalTimeType.LOCAL_DATE_TIME_TYPE, BigDecimal.valueOf(2000)));
        assertEquals(
                new Timestamp(3000),
                convertSplitBoundaryMethod.invoke(
                        splitter, LocalTimeType.OFFSET_DATE_TIME_TYPE, BigDecimal.valueOf(3000)));
    }

    @Test
    public void testConvertDateMinMaxResultsToEpochDays() throws Exception {
        FixedChunkSplitter splitter = new FixedChunkSplitter(mysqlConfig());
        Method convertToBigDecimalMethod =
                FixedChunkSplitter.class.getDeclaredMethod(
                        "convertToBigDecimal", Object.class, SeaTunnelDataType.class);
        convertToBigDecimalMethod.setAccessible(true);
        BigDecimal expected =
                BigDecimal.valueOf(Date.valueOf("2024-01-03").toLocalDate().toEpochDay());

        assertEquals(
                expected,
                convertToBigDecimalMethod.invoke(
                        splitter, Date.valueOf("2024-01-03"), LocalTimeType.LOCAL_DATE_TYPE));
        assertEquals(
                expected,
                convertToBigDecimalMethod.invoke(
                        splitter,
                        Timestamp.valueOf("2024-01-03 12:34:56"),
                        LocalTimeType.LOCAL_DATE_TYPE));
    }

    @Test
    public void testParseDatePartitionBoundsSupportsIsoAndLegacyEpochDays() throws Exception {
        FixedChunkSplitter splitter = new FixedChunkSplitter(mysqlConfig());
        Method parsePartitionBoundMethod =
                FixedChunkSplitter.class.getDeclaredMethod(
                        "parsePartitionBound", String.class, SeaTunnelDataType.class);
        parsePartitionBoundMethod.setAccessible(true);
        BigDecimal expected =
                BigDecimal.valueOf(Date.valueOf("2024-01-03").toLocalDate().toEpochDay());

        assertEquals(
                expected,
                parsePartitionBoundMethod.invoke(
                        splitter, "2024-01-03", LocalTimeType.LOCAL_DATE_TYPE));
        assertEquals(
                expected,
                parsePartitionBoundMethod.invoke(
                        splitter, expected.toPlainString(), LocalTimeType.LOCAL_DATE_TYPE));
    }

    @Test
    public void testRejectInvalidDatePartitionBound() throws Exception {
        FixedChunkSplitter splitter = new FixedChunkSplitter(mysqlConfig());
        Method parsePartitionBoundMethod =
                FixedChunkSplitter.class.getDeclaredMethod(
                        "parsePartitionBound", String.class, SeaTunnelDataType.class);
        parsePartitionBoundMethod.setAccessible(true);

        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () ->
                                parsePartitionBoundMethod.invoke(
                                        splitter,
                                        "2024-01-03 00:00:00",
                                        LocalTimeType.LOCAL_DATE_TYPE));

        assertTrue(exception.getCause() instanceof JdbcConnectorException);
        assertTrue(exception.getCause().getMessage().contains("yyyy-MM-dd"));
    }

    @Test
    public void testRejectInvalidNonDateTemporalPartitionBound() throws Exception {
        FixedChunkSplitter splitter = new FixedChunkSplitter(mysqlConfig());
        Method parsePartitionBoundMethod =
                FixedChunkSplitter.class.getDeclaredMethod(
                        "parsePartitionBound", String.class, SeaTunnelDataType.class);
        parsePartitionBoundMethod.setAccessible(true);

        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () ->
                                parsePartitionBoundMethod.invoke(
                                        splitter,
                                        "2024-01-03 00:00:00",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE));

        assertTrue(exception.getCause() instanceof JdbcConnectorException);
        assertTrue(exception.getCause().getMessage().contains("TIMESTAMP"));
        assertTrue(exception.getCause().getMessage().contains("epoch-millisecond"));
    }

    @Test
    public void testRejectFractionalTemporalPartitionBound() throws Exception {
        FixedChunkSplitter splitter = new FixedChunkSplitter(mysqlConfig());
        Method parsePartitionBoundMethod =
                FixedChunkSplitter.class.getDeclaredMethod(
                        "parsePartitionBound", String.class, SeaTunnelDataType.class);
        parsePartitionBoundMethod.setAccessible(true);

        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () ->
                                parsePartitionBoundMethod.invoke(
                                        splitter, "20000.5", LocalTimeType.LOCAL_DATE_TYPE));

        assertTrue(exception.getCause() instanceof JdbcConnectorException);
        assertTrue(exception.getCause().getMessage().contains("whole numbers"));

        InvocationTargetException timestampException =
                assertThrows(
                        InvocationTargetException.class,
                        () ->
                                parsePartitionBoundMethod.invoke(
                                        splitter, "20000.5", LocalTimeType.LOCAL_DATE_TIME_TYPE));

        assertTrue(timestampException.getCause() instanceof JdbcConnectorException);
        assertTrue(timestampException.getCause().getMessage().contains("whole numbers"));
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
}
