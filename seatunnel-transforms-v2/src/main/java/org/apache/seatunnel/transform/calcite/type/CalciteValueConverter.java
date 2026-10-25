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

package org.apache.seatunnel.transform.calcite.type;

import org.apache.seatunnel.shade.org.apache.calcite.avatica.util.ByteString;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** Bidirectional value converter between SeaTunnel runtime values and Calcite runtime values. */
@UtilityClass
public final class CalciteValueConverter {

    /**
     * Converts a SeaTunnel runtime value into the representation Calcite expects when scanning a
     * row. {@code null} is returned as-is.
     */
    public static Object toCalcite(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalDate) {
            return Date.valueOf((LocalDate) value);
        }
        if (value instanceof LocalTime) {
            return (int) (((LocalTime) value).toNanoOfDay() / 1_000_000L);
        }
        if (value instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return Timestamp.from(((OffsetDateTime) value).toInstant());
        }
        if (value instanceof ByteString) {
            return value;
        }
        if (value instanceof byte[]) {
            return new ByteString((byte[]) value);
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) value;
            byte[] bytes = new byte[buf.remaining()];
            buf.duplicate().get(bytes);
            return new ByteString(bytes);
        }
        return value;
    }

    /**
     * Converts a Calcite runtime value back to the SeaTunnel representation expected for {@code
     * targetType}. {@code null} is returned as-is.
     */
    public static Object fromCalcite(Object value, SeaTunnelDataType<?> targetType) {
        if (value == null) {
            return null;
        }
        if (value instanceof ByteString) {
            return convertBinary(((ByteString) value).getBytes(), targetType);
        }
        if (value instanceof byte[]) {
            return convertBinary((byte[]) value, targetType);
        }
        switch (targetType.getSqlType()) {
            case DATE:
                if (value instanceof Date) {
                    return ((Date) value).toLocalDate();
                }
                if (value instanceof Number) {
                    return LocalDate.ofEpochDay(((Number) value).longValue());
                }
                return value;
            case TIME:
                if (value instanceof Number) {
                    long millis = ((Number) value).longValue();
                    return LocalTime.ofNanoOfDay(millis * 1_000_000L);
                }
                if (value instanceof Time) {
                    return ((Time) value).toLocalTime();
                }
                return value;
            case TIMESTAMP:
                if (value instanceof Timestamp) {
                    return ((Timestamp) value).toLocalDateTime();
                }
                if (value instanceof Number) {
                    return new Timestamp(((Number) value).longValue()).toLocalDateTime();
                }
                return value;
            case TIMESTAMP_TZ:
                if (value instanceof Timestamp) {
                    return ((Timestamp) value).toInstant().atOffset(ZoneOffset.UTC);
                }
                if (value instanceof Number) {
                    return Instant.ofEpochMilli(((Number) value).longValue())
                            .atOffset(ZoneOffset.UTC);
                }
                return value;
            case TINYINT:
                if (value instanceof Number) {
                    return ((Number) value).byteValue();
                }
                return value;
            case SMALLINT:
                if (value instanceof Number) {
                    return ((Number) value).shortValue();
                }
                return value;
            case INT:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return value;
            case BIGINT:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return value;
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                return value;
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return value;
            default:
                return value;
        }
    }

    private static Object convertBinary(byte[] value, SeaTunnelDataType<?> targetType) {
        switch (targetType.getSqlType()) {
            case BYTES:
                return value;
            case BINARY_VECTOR:
            case FLOAT_VECTOR:
            case FLOAT16_VECTOR:
            case BFLOAT16_VECTOR:
            case SPARSE_FLOAT_VECTOR:
                return ByteBuffer.wrap(value);
            default:
                return value;
        }
    }
}
