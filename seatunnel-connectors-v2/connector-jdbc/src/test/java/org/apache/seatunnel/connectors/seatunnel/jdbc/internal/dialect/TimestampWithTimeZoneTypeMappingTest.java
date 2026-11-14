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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.JdbcColumnConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.presto.PrestoDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.presto.PrestoTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.teradata.TeradataTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.vertica.VerticaTypeMapper;

import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/* Verifies timezone-aware timestamp discovery for JDBC dialects that use metadata mappers. */
public class TimestampWithTimeZoneTypeMappingTest {

    /* Verifies the generic JDBC 4.2 type code maps to SeaTunnel TIMESTAMP_TZ. */
    @Test
    public void testGenericTimestampWithTimeZone() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("event_time")
                        .columnType("TIMESTAMP WITH TIME ZONE")
                        .dataType("TIMESTAMP WITH TIME ZONE")
                        .sqlType(Types.TIMESTAMP_WITH_TIMEZONE)
                        .scale(6)
                        .build();

        Column column = new GenericTypeConverter().convert(typeDefine);

        assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, column.getDataType());
        assertEquals(6, column.getScale());
    }

    @Test
    public void testTableMetadataFallbackTimestampWithTimeZone() throws SQLException {
        Column column = JdbcColumnConverter.convert(metadata("TIMESTAMP WITH TIME ZONE", 6), 1);

        assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, column.getDataType());
    }

    @Test
    public void testTableMetadataFallbackRecognizesProprietaryTypeCodes() throws SQLException {
        Column verticaColumn = JdbcColumnConverter.convert(metadata("TIMESTAMPTZ", 6, 14), 1);
        Column teradataColumn =
                JdbcColumnConverter.convert(
                        metadata("TIMESTAMP(6) WITH TIME ZONE", 6, Types.OTHER), 1);

        assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, verticaColumn.getDataType());
        assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, teradataColumn.getDataType());
    }

    /* Verifies Presto precision-qualified timezone timestamps remain distinguishable from NTZ. */
    @Test
    public void testPrestoTimestampWithTimeZone() throws SQLException {
        ResultSetMetaData metadata = metadata("timestamp(3) with time zone", 3);

        assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, new PrestoTypeMapper().mapping(metadata, 1));
    }

    @Test
    public void testPrestoSkipsUnsupportedPrimaryKeyMetadata() {
        assertFalse(new PrestoDialect().supportsPrimaryKeyMetadata());
    }

    /* Verifies Teradata timezone timestamps map to OffsetDateTime-backed SeaTunnel rows. */
    @Test
    public void testTeradataTimestampWithTimeZone() throws SQLException {
        ResultSetMetaData metadata = metadata("TIMESTAMP WITH TIME ZONE", 6);

        assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, new TeradataTypeMapper().mapping(metadata, 1));
    }

    /* Verifies both Vertica names for TIMESTAMPTZ use the timezone-aware SeaTunnel type. */
    @Test
    public void testVerticaTimestampWithTimeZoneAliases() throws SQLException {
        VerticaTypeMapper mapper = new VerticaTypeMapper();

        assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, mapper.mapping(metadata("TIMESTAMPTZ", 6), 1));
        assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE,
                mapper.mapping(metadata("TIMESTAMP WITH TIME ZONE", 6), 1));
    }

    /* Creates deterministic JDBC metadata for source type-mapping tests. */
    private ResultSetMetaData metadata(String typeName, int scale) throws SQLException {
        return metadata(typeName, scale, Types.TIMESTAMP_WITH_TIMEZONE);
    }

    private ResultSetMetaData metadata(String typeName, int scale, int jdbcType)
            throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnTypeName(1)).thenReturn(typeName);
        when(metadata.getColumnType(1)).thenReturn(jdbcType);
        when(metadata.getColumnLabel(1)).thenReturn("event_time");
        when(metadata.isNullable(1)).thenReturn(ResultSetMetaData.columnNullable);
        when(metadata.getPrecision(1)).thenReturn(29);
        when(metadata.getScale(1)).thenReturn(scale);
        when(metadata.getColumnName(1)).thenReturn("event_time");
        return metadata;
    }
}
