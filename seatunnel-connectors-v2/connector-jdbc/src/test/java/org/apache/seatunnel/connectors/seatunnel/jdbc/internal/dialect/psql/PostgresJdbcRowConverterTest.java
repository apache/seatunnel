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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostgresJdbcRowConverterTest {

    private static final String INTERVAL_SOURCE_TYPE = "interval";

    private PostgresJdbcRowConverter converter;

    @BeforeEach
    public void setUp() {
        converter = new PostgresJdbcRowConverter();
    }

    // Helper methods for test setup
    private TableSchema createTableSchema(
            String col2Name, Object col2DataType, String col2SourceType) {
        List<Column> columns = new ArrayList<>();
        columns.add(PhysicalColumn.builder().name("id").dataType(BasicType.INT_TYPE).build());
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(col2Name)
                        .dataType((SeaTunnelDataType<?>) col2DataType);
        if (col2SourceType != null) {
            builder.sourceType(col2SourceType);
        }
        columns.add(builder.build());
        return TableSchema.builder().columns(columns).build();
    }

    private void setupMockResultSet(
            ResultSet rs, String col1Type, String col2Type, Object col1Value, Object col2Value)
            throws SQLException {
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(2);
        when(metaData.getColumnTypeName(1)).thenReturn(col1Type);
        when(metaData.getColumnTypeName(2)).thenReturn(col2Type);
        // Handle multiple calls to getObject() - return same value each time
        when(rs.getObject(1)).thenReturn(col1Value, col1Value);
        when(rs.getObject(2)).thenReturn(col2Value, col2Value);
        // Configure getInt() for INT type columns
        if (col1Value instanceof Integer) {
            when(rs.getInt(1)).thenReturn((Integer) col1Value);
        }
    }

    private void assertOffsetDateTime(
            OffsetDateTime offsetDateTime,
            int year,
            int month,
            int day,
            int hour,
            int minute,
            ZoneOffset offset) {
        Assertions.assertEquals(year, offsetDateTime.getYear());
        Assertions.assertEquals(month, offsetDateTime.getMonthValue());
        Assertions.assertEquals(day, offsetDateTime.getDayOfMonth());
        Assertions.assertEquals(hour, offsetDateTime.getHour());
        Assertions.assertEquals(minute, offsetDateTime.getMinute());
        Assertions.assertEquals(offset, offsetDateTime.getOffset());
    }

    @Test
    public void testToInternalWithTimestampTzFromPGobject() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        TableSchema tableSchema =
                createTableSchema("timestamp_tz_col", LocalTimeType.OFFSET_DATE_TIME_TYPE, null);

        PGobject pgObject = new PGobject();
        pgObject.setType("timestamptz");
        pgObject.setValue("2023-05-07 14:30:00+08:00");

        setupMockResultSet(rs, "INT4", "TIMESTAMPTZ", 1, pgObject);

        SeaTunnelRow row = converter.toInternal(rs, tableSchema);

        Assertions.assertNotNull(row);
        Assertions.assertEquals(1, row.getField(0));

        OffsetDateTime offsetDateTime = (OffsetDateTime) row.getField(1);
        Assertions.assertNotNull(
                offsetDateTime, "timestamp_tz_col should not be null when reading from PGobject");
        assertOffsetDateTime(offsetDateTime, 2023, 5, 7, 14, 30, ZoneOffset.ofHours(8));
    }

    @Test
    public void testToInternalWithTimestampTzFromString() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        TableSchema tableSchema =
                createTableSchema("timestamp_tz_col", LocalTimeType.OFFSET_DATE_TIME_TYPE, null);

        setupMockResultSet(rs, "INT4", "TIMESTAMPTZ", 1, "2023-05-07 14:30:00+08:00");

        SeaTunnelRow row = converter.toInternal(rs, tableSchema);

        Assertions.assertNotNull(row);
        Assertions.assertEquals(1, row.getField(0));

        OffsetDateTime offsetDateTime = (OffsetDateTime) row.getField(1);
        Assertions.assertNotNull(
                offsetDateTime, "timestamp_tz_col should not be null when reading from string");
        assertOffsetDateTime(offsetDateTime, 2023, 5, 7, 14, 30, ZoneOffset.ofHours(8));
    }

    @Test
    public void testToInternalWithNullTimestampTz() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        TableSchema tableSchema =
                createTableSchema("timestamp_tz_col", LocalTimeType.OFFSET_DATE_TIME_TYPE, null);

        setupMockResultSet(rs, "INT4", "TIMESTAMPTZ", 1, null);

        SeaTunnelRow row = converter.toInternal(rs, tableSchema);

        Assertions.assertNotNull(row);
        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertNull(row.getField(1), "timestamp_tz_col should be null");
    }

    @Test
    public void testTimeReadAndWritePreserveMicroseconds() throws SQLException {
        LocalTime expected = LocalTime.parse("12:34:56.123456");
        ResultSet rs = mock(ResultSet.class);
        TableSchema tableSchema =
                createTableSchema("time_col", LocalTimeType.LOCAL_TIME_TYPE, "time(6)");
        setupMockResultSet(rs, "INT4", "TIME", 1, expected);
        when(rs.getObject(2, LocalTime.class)).thenReturn(expected);

        SeaTunnelRow result = converter.toInternal(rs, tableSchema);
        Assertions.assertEquals(expected, result.getField(1));

        PreparedStatement statement = mock(PreparedStatement.class);
        converter.toExternal(
                tableSchema, null, new SeaTunnelRow(new Object[] {1, expected}), statement);
        verify(statement).setObject(2, expected);
    }

    @Test
    public void testToInternalWithGeometryType() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        TableSchema tableSchema =
                createTableSchema("geometry_col", BasicType.STRING_TYPE, "GEOMETRY");

        setupMockResultSet(rs, "INT4", "GEOMETRY", 1, "POINT(1 2)");

        SeaTunnelRow row = converter.toInternal(rs, tableSchema);

        Assertions.assertNotNull(row);
        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertEquals("POINT(1 2)", row.getField(1));
    }

    @Test
    public void testToInternalWithNullGeometryType() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        TableSchema tableSchema =
                createTableSchema("geometry_col", BasicType.STRING_TYPE, "GEOMETRY");

        setupMockResultSet(rs, "INT4", "GEOMETRY", 1, null);

        SeaTunnelRow row = converter.toInternal(rs, tableSchema);

        Assertions.assertNotNull(row);
        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertNull(row.getField(1), "geometry_col should be null");
    }

    @Test
    public void testToExternalWithGeometryType() throws SQLException {
        TableSchema tableSchema =
                createTableSchema("geometry_col", BasicType.STRING_TYPE, "geometry");

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "0102FF"});
        PreparedStatement statement = mock(PreparedStatement.class);

        converter.toExternal(tableSchema, null, row, statement);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(statement).setObject(eq(2), captor.capture());

        Object arg = captor.getValue();
        Assertions.assertTrue(arg instanceof PGobject);
        PGobject pg = (PGobject) arg;
        Assertions.assertEquals("geometry", pg.getType());
        Assertions.assertEquals("0102FF", pg.getValue());
    }

    @Test
    public void testToExternalWithGeometryTypeFromDatabaseSchema() throws SQLException {
        TableSchema writeSchema = createTableSchema("geometry_col", BasicType.STRING_TYPE, null);
        TableSchema databaseSchema =
                createTableSchema("geometry_col", BasicType.STRING_TYPE, "geometry");

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "0102FF"});
        PreparedStatement statement = mock(PreparedStatement.class);

        converter.toExternal(writeSchema, databaseSchema, row, statement);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(statement).setObject(eq(2), captor.capture());

        Object arg = captor.getValue();
        Assertions.assertTrue(arg instanceof PGobject);
        PGobject pg = (PGobject) arg;
        Assertions.assertEquals("geometry", pg.getType());
        Assertions.assertEquals("0102FF", pg.getValue());
    }

    @Test
    public void testToInternalWithGeographyType() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        TableSchema tableSchema =
                createTableSchema("geography_col", BasicType.STRING_TYPE, "GEOGRAPHY");

        setupMockResultSet(rs, "INT4", "GEOGRAPHY", 1, "POINT(1 2)");

        SeaTunnelRow row = converter.toInternal(rs, tableSchema);

        Assertions.assertNotNull(row);
        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertEquals("POINT(1 2)", row.getField(1));
    }

    @Test
    public void testToInternalWithNormalDecimalValue() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        TableSchema tableSchema = createTableSchema("decimal_col", new DecimalType(10, 2), null);

        setupMockResultSet(rs, "INT4", "NUMERIC", 1, null);
        when(rs.getBigDecimal(2)).thenReturn(new BigDecimal("123.45"));

        SeaTunnelRow row = new PostgresJdbcRowConverter(true).toInternal(rs, tableSchema);

        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertEquals(new BigDecimal("123.45"), row.getField(1));
    }

    @Test
    public void testToInternalWithNumericSpecialValueAsNull() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        TableSchema tableSchema = createTableSchema("decimal_col", new DecimalType(10, 2), null);

        setupMockResultSet(rs, "INT4", "NUMERIC", 1, null);
        when(rs.getBigDecimal(2))
                .thenThrow(
                        new PSQLException(
                                "Bad value for type BigDecimal : NaN",
                                PSQLState.NUMERIC_VALUE_OUT_OF_RANGE));
        when(rs.getString(2)).thenReturn("NaN");

        SeaTunnelRow row = new PostgresJdbcRowConverter(true).toInternal(rs, tableSchema);

        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertNull(row.getField(1));
    }

    @Test
    public void testToInternalWithNumericSpecialValueWithoutNarrowing() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        TableSchema tableSchema = createTableSchema("decimal_col", new DecimalType(10, 2), null);

        setupMockResultSet(rs, "INT4", "NUMERIC", 1, null);
        when(rs.getBigDecimal(2))
                .thenThrow(
                        new PSQLException(
                                "Bad value for type BigDecimal : NaN",
                                PSQLState.NUMERIC_VALUE_OUT_OF_RANGE));

        Assertions.assertThrows(
                SQLException.class,
                () -> new PostgresJdbcRowConverter().toInternal(rs, tableSchema));
    }

    @Test
    public void testNumericSpecialValueIsLoggedOncePerColumn() {
        PostgresJdbcRowConverter converter = new PostgresJdbcRowConverter(true);

        // The first occurrence per column is logged, the following ones are not
        Assertions.assertTrue(converter.shouldLogNumericSpecialValue(2));
        Assertions.assertFalse(converter.shouldLogNumericSpecialValue(2));
        Assertions.assertFalse(converter.shouldLogNumericSpecialValue(2));
        // Another column is logged again on its first occurrence
        Assertions.assertTrue(converter.shouldLogNumericSpecialValue(3));
    }

    @Test
    public void testToInternalWithNullGeographyType() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        TableSchema tableSchema =
                createTableSchema("geography_col", BasicType.STRING_TYPE, "GEOGRAPHY");

        setupMockResultSet(rs, "INT4", "GEOGRAPHY", 1, null);

        SeaTunnelRow row = converter.toInternal(rs, tableSchema);

        Assertions.assertNotNull(row);
        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertNull(row.getField(1), "geography_col should be null");
    }

    @Test
    public void testToExternalWithGeographyType() throws SQLException {
        TableSchema tableSchema =
                createTableSchema("geography_col", BasicType.STRING_TYPE, "geography");

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "0102FF"});
        PreparedStatement statement = mock(PreparedStatement.class);

        converter.toExternal(tableSchema, null, row, statement);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(statement).setObject(eq(2), captor.capture());

        Object arg = captor.getValue();
        Assertions.assertTrue(arg instanceof PGobject);
        PGobject pg = (PGobject) arg;
        Assertions.assertEquals("geography", pg.getType());
        Assertions.assertEquals("0102FF", pg.getValue());
    }

    // ---------------------------------------------------------------------------------------------
    // PostgreSQL interval binding.
    //
    // A PostgreSQL interval is represented as a number of microseconds by Debezium, so a numeric
    // interval value is translated into a PostgreSQL interval literal before it is bound. The
    // assertions below read the interval back with the pgjdbc interval parser and compare the
    // number of microseconds it represents, so that they do not depend on a particular textual
    // layout of the generated literal.

    private TableSchema intervalTableSchema() {
        return createTableSchema("interval_col", BasicType.STRING_TYPE, INTERVAL_SOURCE_TYPE);
    }

    /** Binds an interval column value through {@code toExternal} and returns the bound PGobject. */
    private PGobject captureBoundInterval(Object intervalFieldValue) throws SQLException {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, intervalFieldValue});
        PreparedStatement statement = mock(PreparedStatement.class);

        converter.toExternal(intervalTableSchema(), null, row, statement);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(statement).setObject(eq(2), captor.capture());
        Object boundValue = captor.getValue();
        Assertions.assertTrue(
                boundValue instanceof PGobject,
                "the interval column must be bound as PGobject but was " + boundValue);
        PGobject intervalObject = (PGobject) boundValue;
        Assertions.assertEquals(
                INTERVAL_SOURCE_TYPE,
                intervalObject.getType(),
                "the bound PGobject must keep the PostgreSQL interval type");
        return intervalObject;
    }

    /**
     * Converts an interval literal into its total number of microseconds with the pgjdbc interval
     * parser, i.e. the parser PostgreSQL users rely on, instead of comparing textual layouts.
     */
    private long toMicroseconds(String intervalLiteral) {
        PGInterval interval;
        try {
            interval = new PGInterval(intervalLiteral);
        } catch (SQLException e) {
            throw new AssertionError(
                    "pgjdbc cannot parse the bound interval value '" + intervalLiteral + "'", e);
        }
        Assertions.assertEquals(0, interval.getYears(), "unexpected years in " + intervalLiteral);
        Assertions.assertEquals(0, interval.getMonths(), "unexpected months in " + intervalLiteral);
        long totalMinutes =
                (interval.getDays() * 24L + interval.getHours()) * 60L + interval.getMinutes();
        return (totalMinutes * 60L + interval.getWholeSeconds()) * 1_000_000L
                + interval.getMicroSeconds();
    }

    /**
     * Binds a microsecond interval value and asserts that the bound interval still represents
     * exactly the same number of microseconds.
     */
    private void assertMicrosecondsPreserved(String microsecondValue, long expectedMicroseconds)
            throws SQLException {
        String boundLiteral = captureBoundInterval(microsecondValue).getValue();
        Assertions.assertEquals(
                expectedMicroseconds,
                toMicroseconds(boundLiteral),
                "binding "
                        + microsecondValue
                        + " microseconds produced the interval '"
                        + boundLiteral
                        + "', which does not represent "
                        + expectedMicroseconds
                        + " microseconds");
    }

    @Test
    public void testPgjdbcIntervalParserKeepsFractionalAndNegativeMicroseconds() {
        // Reference semantics of the pgjdbc interval parser used by the assertions below:
        // fractional seconds are kept as microseconds and the sign of the interval is kept.
        Assertions.assertEquals(1_000_000L, toMicroseconds("1 seconds"));
        Assertions.assertEquals(1_500_000L, toMicroseconds("1.5 seconds"));
        Assertions.assertEquals(-1_500_000L, toMicroseconds("-1.5 seconds"));
        Assertions.assertEquals(0L, toMicroseconds("0 seconds"));
    }

    @Test
    public void testToExternalIntervalWithWholeSecondValue() throws SQLException {
        // Behaviour that must not change: whole positive days/hours/minutes/seconds.
        assertMicrosecondsPreserved("1000000", 1_000_000L);
        assertMicrosecondsPreserved("90061000000", 90_061_000_000L);
    }

    @Test
    public void testToExternalIntervalWithSingleMicrosecond() throws SQLException {
        assertMicrosecondsPreserved("1", 1L);
    }

    @Test
    public void testToExternalIntervalWithFractionalSecond() throws SQLException {
        assertMicrosecondsPreserved("1500000", 1_500_000L);
    }

    @Test
    public void testToExternalIntervalWithNegativeMicrosecond() throws SQLException {
        assertMicrosecondsPreserved("-1", -1L);
    }

    @Test
    public void testToExternalIntervalWithNegativeFractionalSecond() throws SQLException {
        assertMicrosecondsPreserved("-1500000", -1_500_000L);
    }

    @Test
    public void testToExternalIntervalWithZeroValue() throws SQLException {
        String boundLiteral = captureBoundInterval("0").getValue();
        // pgjdbc parses an empty interval literal as zero, but the PostgreSQL server rejects it, so
        // zero microseconds must be bound as a non empty literal.
        Assertions.assertNotNull(boundLiteral);
        Assertions.assertFalse(
                boundLiteral.trim().isEmpty(),
                "zero microseconds must be bound as a non empty interval literal");
        Assertions.assertEquals(0L, toMicroseconds(boundLiteral));
    }

    @Test
    public void testToExternalIntervalBeyondLongNanosecondRange() throws SQLException {
        // 9223372036854776 is the first microsecond count whose nanosecond representation, i.e.
        // microsecond * 1000, overflows a long. It is still far inside the PostgreSQL interval
        // range of -178000000 .. 178000000 years.
        assertMicrosecondsPreserved("9223372036854776", 9_223_372_036_854_776L);
    }

    @Test
    public void testToExternalIntervalWithLargeFractionalSeconds() throws SQLException {
        // 9223372036854777 microseconds is 106751 days 85636.854777 seconds, i.e. the seconds part
        // of the generated literal is almost a whole day instead of a small remainder.
        assertMicrosecondsPreserved("9223372036854777", 9_223_372_036_854_777L);
    }

    @Test
    public void testToExternalIntervalWithLongBoundaryValues() throws SQLException {
        // The largest and the smallest representable microsecond counts must be preserved exactly.
        assertMicrosecondsPreserved(String.valueOf(Long.MAX_VALUE), Long.MAX_VALUE);
        assertMicrosecondsPreserved(String.valueOf(Long.MIN_VALUE), Long.MIN_VALUE);
    }

    @Test
    public void testToExternalIntervalWithHumanReadableValue() throws SQLException {
        // A non numeric interval value is already a PostgreSQL interval literal and must be
        // passed through untouched.
        PGobject boundInterval = captureBoundInterval("1 day 02:03:04.5");

        Assertions.assertEquals("1 day 02:03:04.5", boundInterval.getValue());
        Assertions.assertEquals(93_784_500_000L, toMicroseconds(boundInterval.getValue()));
    }

    @Test
    public void testToExternalIntervalWithNullValue() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);

        converter.toExternal(
                intervalTableSchema(), null, new SeaTunnelRow(new Object[] {1, null}), statement);

        verify(statement).setObject(2, (Object) null);
    }
}
