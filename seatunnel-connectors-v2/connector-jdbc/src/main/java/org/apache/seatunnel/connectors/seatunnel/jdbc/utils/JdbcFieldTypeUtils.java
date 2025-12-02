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
package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;

public final class JdbcFieldTypeUtils {

    private JdbcFieldTypeUtils() {}

    public static Boolean getBoolean(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getBoolean);
    }

    public static Byte getByte(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getByte);
    }

    public static Short getShort(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getShort);
    }

    public static Integer getInt(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getInt);
    }

    public static Long getLong(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getLong);
    }

    public static Float getFloat(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getFloat);
    }

    public static Double getDouble(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getDouble);
    }

    public static String getString(ResultSet resultSet, int columnIndex) throws SQLException {
        Object obj = resultSet.getObject(columnIndex);
        if (obj == null) {
            return null;
        }

        // Add special handling for the BLOB data type.
        if (obj instanceof java.sql.Blob) {
            java.sql.Blob blob = (java.sql.Blob) obj;
            try {
                byte[] bytes = blob.getBytes(1, (int) blob.length());
                return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            } finally {
                blob.free();
            }
        }
        return resultSet.getString(columnIndex);
    }

    public static BigDecimal getBigDecimal(ResultSet resultSet, int columnIndex)
            throws SQLException {
        return resultSet.getBigDecimal(columnIndex);
    }

    public static Date getDate(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getDate(columnIndex);
    }

    public static Time getTime(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getTime(columnIndex);
    }

    public static Timestamp getTimestamp(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getTimestamp(columnIndex);
    }

    public static byte[] getBytes(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getBytes(columnIndex);
    }

    public static OffsetDateTime getOffsetDateTime(ResultSet resultSet, int columnIndex)
            throws SQLException {
        Object obj = resultSet.getObject(columnIndex);
        if (obj == null) {
            return null;
        }

        // Handle OffsetDateTime directly
        if (obj instanceof OffsetDateTime) {
            return (OffsetDateTime) obj;
        }

        // Handle ZonedDateTime
        if (obj instanceof ZonedDateTime) {
            return ((ZonedDateTime) obj).toOffsetDateTime();
        }

        // Handle Instant
        if (obj instanceof Instant) {
            return ((Instant) obj).atOffset(ZoneOffset.UTC);
        }

        // Handle java.sql.Timestamp
        if (obj instanceof Timestamp) {
            return ((Timestamp) obj).toLocalDateTime().atOffset(ZoneOffset.UTC);
        }

        // Handle java.util.Date
        if (obj instanceof java.util.Date) {
            return ((java.util.Date) obj).toInstant().atOffset(ZoneOffset.UTC);
        }

        // Handle Long (epoch milliseconds)
        if (obj instanceof Long) {
            return Instant.ofEpochMilli((Long) obj).atOffset(ZoneOffset.UTC);
        }

        // Try to parse as string
        String str = obj.toString();
        try {
            return parseOffsetDateTimeFromString(str);
        } catch (Exception e) {
            throw new SQLException(
                    "Failed to parse OffsetDateTime value: "
                            + str
                            + " (class: "
                            + obj.getClass().getName()
                            + ")",
                    e);
        }
    }

    public static OffsetDateTime parseOffsetDateTimeFromString(String str)
            throws DateTimeParseException {
        if (str == null || str.trim().isEmpty()) {
            return null;
        }

        String trimmed = str.trim();

        // Try standard ISO-8601 format first
        try {
            return OffsetDateTime.parse(trimmed);
        } catch (DateTimeParseException ignore) {
            // fall through
        }

        // Try with space separator instead of 'T'
        try {
            String normalized = trimmed.replace('T', ' ');
            return OffsetDateTime.parse(normalized, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (DateTimeParseException ignore) {
            // fall through
        }

        // Try ZonedDateTime parsing
        try {
            return ZonedDateTime.parse(trimmed).toOffsetDateTime();
        } catch (DateTimeParseException ignore) {
            // fall through
        }

        // Try SQL Server DateTimeOffset format with variable precision
        // Examples: "2025-11-04 21:10:06.891977 +00:00" (6 decimals)
        //           "2025-11-05 05:54:15.069 +00:00" (3 decimals)
        try {
            return parseSqlServerDateTimeOffset(trimmed);
        } catch (DateTimeParseException ignore) {
            // fall through
        }

        // If all parsing attempts fail, throw exception
        throw new DateTimeParseException(
                "Unable to parse OffsetDateTime from string: " + str, trimmed, 0);
    }

    /**
     * Parse SQL Server DateTimeOffset format with variable precision. Supports formats like: -
     * "2025-11-04 21:10:06.891977 +00:00" (6 decimals) - "2025-11-05 05:54:15.069 +00:00" (3
     * decimals) - "2025-11-05 05:54:15.1 +00:00" (1 decimal)
     */
    private static OffsetDateTime parseSqlServerDateTimeOffset(String str)
            throws DateTimeParseException {
        // Pattern: YYYY-MM-DD HH:MM:SS.fff... +HH:MM
        // We need to handle variable precision in the fractional seconds

        // Find the position of the space before the offset
        int lastSpaceIndex = str.lastIndexOf(' ');
        if (lastSpaceIndex <= 0) {
            throw new DateTimeParseException(
                    "Invalid SQL Server DateTimeOffset format: " + str, str, 0);
        }

        String dateTimePart = str.substring(0, lastSpaceIndex);
        String offsetPart = str.substring(lastSpaceIndex + 1);

        try {
            // Parse the offset part (e.g., "+00:00" or "-05:00")
            ZoneOffset offset = ZoneOffset.of(offsetPart);

            // Parse the date-time part with variable precision
            // Use a formatter that allows optional fractional seconds
            DateTimeFormatter formatter =
                    new DateTimeFormatterBuilder()
                            .append(DateTimeFormatter.ISO_LOCAL_DATE)
                            .appendLiteral(' ')
                            .append(DateTimeFormatter.ISO_LOCAL_TIME)
                            .toFormatter();

            LocalDateTime localDateTime = LocalDateTime.parse(dateTimePart, formatter);
            return localDateTime.atOffset(offset);
        } catch (Exception e) {
            throw new DateTimeParseException(
                    "Failed to parse SQL Server DateTimeOffset: " + str, str, 0);
        }
    }

    private static <T> T getNullableValue(
            ResultSet resultSet,
            int columnIndex,
            ThrowingFunction<ResultSet, T, SQLException> getter)
            throws SQLException {
        final Object obj = resultSet.getObject(columnIndex);
        if (obj == null) {
            return null;
        }
        return getter.apply(resultSet, columnIndex);
    }

    @FunctionalInterface
    private interface ThrowingFunction<T, R, E extends Exception> {
        R apply(T t, int columnIndex) throws E;
    }
}
