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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.GenericDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.starrocks.StarRocksDialect;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Calendar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MysqlJdbcRowConverterTest {

    @Test
    public void testTimestampReadAndWritePreserveLocalFields() throws Exception {
        for (LocalDateTime expected :
                new LocalDateTime[] {
                    LocalDateTime.parse("2026-01-01T12:34:56.123456"),
                    LocalDateTime.parse("2026-03-08T02:30:00.123456"),
                    LocalDateTime.parse("2026-11-01T01:30:00.654321")
                }) {
            TableSchema schema = timestampSchema(LocalTimeType.LOCAL_DATE_TIME_TYPE);
            MysqlJdbcRowConverter converter = new MysqlJdbcRowConverter();
            ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.getObject(1, LocalDateTime.class)).thenReturn(expected);
            SeaTunnelRow row = converter.toInternal(resultSet, schema);
            assertEquals(expected, row.getField(0));

            PreparedStatement statement = mock(PreparedStatement.class);
            converter.toExternal(schema, schema, row, statement);
            verify(statement).setObject(1, expected);
            verify(statement, never()).setTimestamp(eq(1), any(Timestamp.class));
        }
    }

    @Test
    public void testTimestampNullBindingIsUnchanged() throws Exception {
        TableSchema schema = timestampSchema(LocalTimeType.LOCAL_DATE_TIME_TYPE);
        MysqlJdbcRowConverter converter = new MysqlJdbcRowConverter();
        ResultSet resultSet = mock(ResultSet.class);
        SeaTunnelRow row = converter.toInternal(resultSet, schema);
        assertEquals(null, row.getField(0));
        PreparedStatement statement = mock(PreparedStatement.class);
        converter.toExternal(schema, row, statement);
        verify(statement).setObject(1, null);
        verify(statement, never()).setTimestamp(eq(1), any(Timestamp.class));
    }

    @Test
    public void testTimestampBindingFailureIsNotRetriedWithTimestamp() throws Exception {
        TableSchema schema = timestampSchema(LocalTimeType.LOCAL_DATE_TIME_TYPE);
        LocalDateTime value = LocalDateTime.parse("2026-01-01T12:34:56.123456");
        PreparedStatement statement = mock(PreparedStatement.class);
        SQLException failure = new SQLException("binding failed", "22007");
        doThrow(failure).when(statement).setObject(1, value);
        JdbcConnectorException actual =
                assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                new MysqlJdbcRowConverter()
                                        .toExternal(
                                                schema,
                                                new SeaTunnelRow(new Object[] {value}),
                                                statement));
        assertSame(failure, actual.getCause());
        verify(statement, never()).setTimestamp(eq(1), any(Timestamp.class));
    }

    @Test
    public void testDefaultAndStarRocksTimestampBindingsAreUnchanged() throws Exception {
        LocalDateTime value = LocalDateTime.parse("2026-01-01T12:34:56.123456");
        TableSchema schema = timestampSchema(LocalTimeType.LOCAL_DATE_TIME_TYPE);
        for (JdbcRowConverter converter :
                new JdbcRowConverter[] {
                    new GenericDialect().getRowConverter(), new StarRocksDialect().getRowConverter()
                }) {
            PreparedStatement statement = mock(PreparedStatement.class);
            converter.toExternal(schema, new SeaTunnelRow(new Object[] {value}), statement);
            ArgumentCaptor<Timestamp> timestamp = ArgumentCaptor.forClass(Timestamp.class);
            verify(statement).setTimestamp(eq(1), timestamp.capture());
            assertEquals(value, timestamp.getValue().toLocalDateTime());
            verify(statement, never()).setObject(1, value);
        }
    }

    @Test
    public void testTimestampTzBindingAndUtcFallbackAreUnchanged() throws Exception {
        TableSchema schema = timestampSchema(LocalTimeType.OFFSET_DATE_TIME_TYPE);
        OffsetDateTime value = OffsetDateTime.parse("2026-01-01T12:34:56.123456+08:00");
        MysqlJdbcRowConverter converter = new MysqlJdbcRowConverter();
        PreparedStatement statement = mock(PreparedStatement.class);
        converter.toExternal(schema, new SeaTunnelRow(new Object[] {value}), statement);
        verify(statement).setObject(1, value);

        PreparedStatement legacy = mock(PreparedStatement.class);
        doThrow(new SQLException("unsupported")).when(legacy).setObject(1, value);
        converter.toExternal(schema, new SeaTunnelRow(new Object[] {value}), legacy);
        ArgumentCaptor<Timestamp> timestamp = ArgumentCaptor.forClass(Timestamp.class);
        ArgumentCaptor<Calendar> calendar = ArgumentCaptor.forClass(Calendar.class);
        verify(legacy).setTimestamp(eq(1), timestamp.capture(), calendar.capture());
        assertEquals(value.toInstant(), timestamp.getValue().toInstant());
        assertEquals("UTC", calendar.getValue().getTimeZone().getID());
    }

    private TableSchema timestampSchema(LocalTimeType<?> type) {
        return TableSchema.builder()
                .column(PhysicalColumn.builder().name("timestamp_col").dataType(type).build())
                .build();
    }

    @Test
    public void testTimeReadAndWritePreserveMicroseconds() throws Exception {
        LocalTime expected = LocalTime.parse("12:34:56.123456");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("time_col")
                                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                                        .build())
                        .build();
        MysqlJdbcRowConverter converter = new MysqlJdbcRowConverter();
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getObject(1, LocalTime.class)).thenReturn(expected);

        SeaTunnelRow result = converter.toInternal(resultSet, tableSchema);
        assertEquals(expected, result.getField(0));

        PreparedStatement statement = mock(PreparedStatement.class);
        converter.toExternal(tableSchema, new SeaTunnelRow(new Object[] {expected}), statement);
        ArgumentCaptor<Timestamp> timestamp = ArgumentCaptor.forClass(Timestamp.class);
        verify(statement).setTimestamp(org.mockito.ArgumentMatchers.eq(1), timestamp.capture());
        assertEquals(expected.getNano(), timestamp.getValue().getNanos());
    }

    @Test
    public void testWideDecimalValueIsReadAndWrittenWithoutNarrowing() throws Exception {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("amount")
                                        .dataType(new DecimalType(65, 0))
                                        .build())
                        .build();
        BigDecimal expected =
                new BigDecimal("12345678901234567890123456789012345678901234567890123456789012345");
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getBigDecimal(1)).thenReturn(expected);

        SeaTunnelRow result = new MysqlJdbcRowConverter().toInternal(resultSet, tableSchema);
        assertEquals(expected, result.getField(0));

        PreparedStatement statement = mock(PreparedStatement.class);

        new MysqlJdbcRowConverter()
                .toExternal(tableSchema, new SeaTunnelRow(new Object[] {expected}), statement);

        verify(statement).setBigDecimal(1, expected);
    }
}
