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

package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergTypeMapperTest {

    @Test
    void returnsReconvertedTypeWhenSinkTypeNotNull() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getSinkType()).thenReturn("int");

        Type result = IcebergTypeMapper.toIcebergType(column, new AtomicInteger(1));

        assertEquals(Types.IntegerType.get(), result);
    }

    @Test
    void returnsReconvertedTypeWhenSinkTypeIsNull() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.LONG_TYPE);

        Type result = IcebergTypeMapper.toIcebergType(column, new AtomicInteger(1));

        assertEquals(Types.LongType.get(), result);
    }

    @Test
    void returnsReconvertedTypeWhenTypesNotNull() {
        Column column = mock(Column.class);
        when(column.getName()).thenReturn("col1");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.LONG_TYPE);
        when(column.getSinkType()).thenReturn("int");

        Type result = IcebergTypeMapper.toIcebergType(column, new AtomicInteger(1));

        assertEquals(Types.IntegerType.get(), result);
    }

    @Test
    void throwsExceptionWhenSinkTypeIsInvalid() {
        Column column = mock(Column.class);
        when(column.getSinkType()).thenReturn("invalid_type");

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    IcebergTypeMapper.toIcebergType(column, new AtomicInteger(1));
                });
    }

    @Test
    void timestampNtzMapsToWithoutZone() {
        // TIMESTAMP (NTZ) → Iceberg withoutZone()
        Column column = mock(Column.class);
        when(column.getSinkType()).thenReturn(null);
        when(column.getDataType())
                .thenReturn((SeaTunnelDataType) LocalTimeType.LOCAL_DATE_TIME_TYPE);

        Type result = IcebergTypeMapper.toIcebergType(column, new AtomicInteger(1));
        assertEquals(Types.TimestampType.withoutZone(), result);
    }

    @Test
    void timestampTzMapsToWithZone() {
        // TIMESTAMP_TZ (LTZ) → Iceberg withZone()
        Column column = mock(Column.class);
        when(column.getSinkType()).thenReturn(null);
        when(column.getDataType())
                .thenReturn((SeaTunnelDataType) LocalTimeType.OFFSET_DATE_TIME_TYPE);

        Type result = IcebergTypeMapper.toIcebergType(column, new AtomicInteger(1));
        assertEquals(Types.TimestampType.withZone(), result);
    }

    @Test
    void icebergWithoutZoneMapsToLocalDateTimeType() {
        // Iceberg withoutZone() → SeaTunnel LOCAL_DATE_TIME_TYPE (TIMESTAMP / NTZ)
        SeaTunnelDataType<?> result =
                IcebergTypeMapper.mapping("ts_ntz", Types.TimestampType.withoutZone());
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, result);
    }

    @Test
    void icebergWithZoneMapsToOffsetDateTimeType() {
        // Iceberg withZone() → SeaTunnel OFFSET_DATE_TIME_TYPE (TIMESTAMP_TZ / LTZ)
        SeaTunnelDataType<?> result =
                IcebergTypeMapper.mapping("ts_ltz", Types.TimestampType.withZone());
        assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, result);
    }

    /**
     * Upgrade-path regression: old SeaTunnel (before SEATUNNEL-10685) incorrectly wrote SeaTunnel
     * TIMESTAMP columns to Iceberg as {@code withZone()}. After the fix, reading such a legacy
     * column back must return {@code TIMESTAMP_TZ} (OFFSET_DATE_TIME_TYPE), not TIMESTAMP
     * (LOCAL_DATE_TIME_TYPE).
     *
     * <p>This is a documented breaking change — see incompatible-changes.md.
     */
    @Test
    void upgradePath_legacyWithZoneColumnIsReadAsTimestampTz() {
        SeaTunnelDataType<?> result =
                IcebergTypeMapper.mapping("legacy_ts_col", Types.TimestampType.withZone());
        assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE,
                result,
                "Existing Iceberg withZone() column (written by old SeaTunnel as TIMESTAMP) "
                        + "must be read as TIMESTAMP_TZ after upgrade");
    }

    /**
     * Upgrade-path regression: new SeaTunnel writes SeaTunnel TIMESTAMP columns to Iceberg as
     * {@code withoutZone()} (NTZ). Reading them back must return TIMESTAMP (LOCAL_DATE_TIME_TYPE).
     */
    @Test
    void upgradePath_newTimestampWrittenAsWithoutZoneIsReadAsTimestamp() {
        SeaTunnelDataType<?> result =
                IcebergTypeMapper.mapping("new_ts_col", Types.TimestampType.withoutZone());
        assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                result,
                "New Iceberg withoutZone() column (written by new SeaTunnel as TIMESTAMP) "
                        + "must be read as TIMESTAMP (NTZ)");
    }
}
