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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
        final Object obj = resultSet.getObject(columnIndex);
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
            return ((Timestamp) obj).toInstant().atOffset(ZoneOffset.UTC);
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
        String trimmed = str.trim();
        // Treat empty string as "no value"
        if (trimmed.isEmpty()) {
            return null;
        }
        // Try parsing as standard ISO-8601 OffsetDateTime
        OffsetDateTime directParsed = tryParseOffsetDateTime(trimmed);
        if (directParsed != null) {
            return directParsed;
        }
        // Normalize common relaxed forms and try again
        String normalized = normalizeOffsetDateTimeString(trimmed);
        OffsetDateTime normalizedParsed = tryParseOffsetDateTime(normalized);
        if (normalizedParsed != null) {
            return normalizedParsed;
        }
        // Finally, try parsing as ZonedDateTime and convert to OffsetDateTime
        OffsetDateTime zonedParsed = tryParseZonedDateTime(trimmed);
        if (zonedParsed != null) {
            return zonedParsed;
        }

        throw new DateTimeParseException(
                "Unable to parse OffsetDateTime from string: " + str, trimmed, 0);
    }

    private static OffsetDateTime tryParseOffsetDateTime(String value) {
        try {
            return OffsetDateTime.parse(value);
        } catch (DateTimeParseException ignore) {
            return null;
        }
    }

    private static OffsetDateTime tryParseZonedDateTime(String value) {
        try {
            return ZonedDateTime.parse(value).toOffsetDateTime();
        } catch (DateTimeParseException ignore) {
            return null;
        }
    }

    private static String normalizeOffsetDateTimeString(String value) {
        String normalized = value;
        if (normalized.endsWith(" UTC")) {
            normalized = normalized.substring(0, normalized.length() - 4) + "Z";
        }
        normalized = normalized.replace(' ', 'T');
        if (normalized.matches(".*[+-]\\d{2}$")) {
            normalized = normalized + ":00";
        } else if (normalized.matches(".*[+-]\\d{4}$")) {
            normalized =
                    normalized.substring(0, normalized.length() - 2)
                            + ":"
                            + normalized.substring(normalized.length() - 2);
        }
        return normalized;
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
