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

package org.apache.seatunnel.benchmark.jdbc;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;

/** Provides a positioned result set and a parameter recorder for the common JDBC converter. */
public final class JdbcRowConverterFixture {

    private static final LocalDate SAMPLE_DATE = LocalDate.of(2026, 4, 2);
    private static final LocalTime SAMPLE_TIME = LocalTime.of(13, 45, 7);
    private static final LocalDateTime SAMPLE_TIMESTAMP =
            SAMPLE_DATE.atTime(SAMPLE_TIME).withNano(123_456_000);

    public enum TypeProfile {
        INTEGRAL(
                new FieldSample(BasicType.BOOLEAN_TYPE, JDBCType.BOOLEAN, true),
                new FieldSample(BasicType.BYTE_TYPE, JDBCType.TINYINT, (byte) 42),
                new FieldSample(BasicType.SHORT_TYPE, JDBCType.SMALLINT, (short) 1009),
                new FieldSample(BasicType.INT_TYPE, JDBCType.INTEGER, 1009),
                new FieldSample(BasicType.LONG_TYPE, JDBCType.BIGINT, 10_000_007_123L)),
        DECIMAL_AND_BINARY(
                new FieldSample(BasicType.FLOAT_TYPE, JDBCType.REAL, 1.25F),
                new FieldSample(BasicType.DOUBLE_TYPE, JDBCType.DOUBLE, 2.5D),
                new FieldSample(
                        new DecimalType(18, 4),
                        JDBCType.DECIMAL,
                        new BigDecimal("1234567890123.4567")),
                new FieldSample(BasicType.STRING_TYPE, JDBCType.VARCHAR, "customer_0291"),
                new FieldSample(
                        PrimitiveByteArrayType.INSTANCE,
                        JDBCType.VARBINARY,
                        new byte[] {1, 0, -1})),
        TEMPORAL(
                new FieldSample(
                        LocalTimeType.LOCAL_DATE_TYPE,
                        JDBCType.DATE,
                        SAMPLE_DATE,
                        Date.valueOf(SAMPLE_DATE)),
                new FieldSample(
                        LocalTimeType.LOCAL_TIME_TYPE,
                        JDBCType.TIME,
                        SAMPLE_TIME,
                        Time.valueOf(SAMPLE_TIME)),
                new FieldSample(
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        JDBCType.TIMESTAMP,
                        SAMPLE_TIMESTAMP,
                        Timestamp.valueOf(SAMPLE_TIMESTAMP)),
                new FieldSample(
                        LocalTimeType.OFFSET_DATE_TIME_TYPE,
                        JDBCType.TIMESTAMP_WITH_TIMEZONE,
                        OffsetDateTime.parse("2026-04-02T13:45:07.123456+08:00")),
                new FieldSample(BasicType.STRING_TYPE, JDBCType.VARCHAR, null));

        private final FieldSample[] fields;

        TypeProfile(FieldSample... fields) {
            this.fields = fields;
        }
    }

    private final JdbcRowConverter converter =
            new AbstractJdbcRowConverter() {
                @Override
                public String converterName() {
                    return "benchmark";
                }
            };
    private final TableSchema tableSchema;
    private final TableSchema targetTableSchema;
    private final SeaTunnelRow inputRow;
    private final PreparedStatement statement;
    private final ResultSet resultSet;

    public JdbcRowConverterFixture(int columns, TypeProfile typeProfile) throws SQLException {
        if (columns < 1) {
            throw new IllegalArgumentException("columns must be positive");
        }

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        Object[] fields = new Object[columns];
        Object[] jdbcValues = new Object[columns];
        for (int column = 0; column < columns; column++) {
            FieldSample field = typeProfile.fields[column % typeProfile.fields.length];
            String name = "c" + column;
            schemaBuilder.column(
                    PhysicalColumn.of(
                            name,
                            field.type,
                            64L,
                            true,
                            null,
                            "",
                            field.jdbcType.getName(),
                            Collections.emptyMap()));
            fields[column] = field.rowValue;
            jdbcValues[column] = field.jdbcValue;
        }
        tableSchema = schemaBuilder.build();
        targetTableSchema = tableSchema.copy();
        inputRow = new SeaTunnelRow(fields);
        resultSet = positionedResultSet(fields, jdbcValues);

        Object[] parameters = new Object[columns];
        Arrays.fill(parameters, new Object());
        statement = recordingStatement(parameters);
        bindRow();
        if (!Arrays.deepEquals(jdbcValues, parameters)
                || !Arrays.deepEquals(fields, readRow().getFields())) {
            throw new IllegalStateException("JDBC conversion did not preserve the input fields");
        }
    }

    public SeaTunnelRow readRow() throws SQLException {
        return converter.toInternal(resultSet, tableSchema);
    }

    public PreparedStatement bindRow() throws SQLException {
        return converter.toExternal(tableSchema, targetTableSchema, inputRow, statement);
    }

    private static ResultSet positionedResultSet(Object[] fields, Object[] jdbcValues) {
        return (ResultSet)
                Proxy.newProxyInstance(
                        JdbcRowConverterFixture.class.getClassLoader(),
                        new Class<?>[] {ResultSet.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "getObject":
                                    int index = (Integer) args[0] - 1;
                                    if (args.length == 2 && args[1] instanceof Class) {
                                        // Model the JDBC 4.2 getter for a local timestamp.
                                        return ((Class<?>) args[1]).cast(fields[index]);
                                    }
                                    return jdbcValues[index];
                                case "getBoolean":
                                case "getByte":
                                case "getShort":
                                case "getInt":
                                case "getLong":
                                case "getFloat":
                                case "getDouble":
                                case "getBigDecimal":
                                case "getString":
                                case "getBytes":
                                case "getDate":
                                case "getTime":
                                case "getTimestamp":
                                    return jdbcValues[(Integer) args[0] - 1];
                                default:
                                    throw new UnsupportedOperationException(method.getName());
                            }
                        });
    }

    private static PreparedStatement recordingStatement(Object[] parameters) {
        return (PreparedStatement)
                Proxy.newProxyInstance(
                        JdbcRowConverterFixture.class.getClassLoader(),
                        new Class<?>[] {PreparedStatement.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "setBoolean":
                                case "setByte":
                                case "setShort":
                                case "setInt":
                                case "setLong":
                                case "setFloat":
                                case "setString":
                                case "setDouble":
                                case "setBigDecimal":
                                case "setBytes":
                                case "setDate":
                                case "setTime":
                                case "setTimestamp":
                                case "setObject":
                                    parameters[(Integer) args[0] - 1] = args[1];
                                    return null;
                                case "setNull":
                                    parameters[(Integer) args[0] - 1] = null;
                                    return null;
                                default:
                                    throw new UnsupportedOperationException(method.getName());
                            }
                        });
    }

    private static final class FieldSample {
        private final SeaTunnelDataType<?> type;
        private final JDBCType jdbcType;
        private final Object rowValue;
        private final Object jdbcValue;

        private FieldSample(SeaTunnelDataType<?> type, JDBCType jdbcType, Object value) {
            this(type, jdbcType, value, value);
        }

        private FieldSample(
                SeaTunnelDataType<?> type, JDBCType jdbcType, Object rowValue, Object jdbcValue) {
            this.type = type;
            this.jdbcType = jdbcType;
            this.rowValue = rowValue;
            this.jdbcValue = jdbcValue;
        }
    }
}
