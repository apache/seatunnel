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
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostgresJdbcRowConverterTest {

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
}
