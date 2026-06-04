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

import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
