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

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class JdbcFieldTypeUtils {

    private static final Map<Class<?>, OracleOffsetDateTimeAccessor>
            ORACLE_OFFSET_DATETIME_ACCESSOR_CACHE = new ConcurrentHashMap<>();

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

        // Handle Oracle proprietary TIMESTAMP WITH TIME ZONE types
        // oracle.sql.TIMESTAMPTZ - TIMESTAMP WITH TIME ZONE
        // oracle.sql.TIMESTAMPLTZ - TIMESTAMP WITH LOCAL TIME ZONE
        String className = obj.getClass().getName();
        if ("oracle.sql.TIMESTAMPTZ".equals(className)
                || "oracle.sql.TIMESTAMPLTZ".equals(className)) {
            try {
                OracleOffsetDateTimeAccessor accessor =
                        ORACLE_OFFSET_DATETIME_ACCESSOR_CACHE.computeIfAbsent(
                                obj.getClass(),
                                clazz -> {
                                    try {
                                        return resolveOracleOffsetDateTimeAccessor(clazz);
                                    } catch (NoSuchMethodException e) {
                                        throw new IllegalStateException(e);
                                    }
                                });
                return accessor.invoke(obj, resultSet);
            } catch (Exception e) {
                throw new SQLException(
                        "Failed to convert Oracle TIMESTAMP WITH TIME ZONE value: " + className, e);
            }
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
                    "Failed to parse OffsetDateTime value: " + str + " (class: " + className + ")",
                    e);
        }
    }

    public static OffsetDateTime parseOffsetDateTimeFromString(String str)
            throws DateTimeParseException {
        if (str.trim().isEmpty()) {
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

        try {
            DateTimeFormatter oracleFormatter =
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSSxxx");
            return OffsetDateTime.parse(trimmed, oracleFormatter);
        } catch (DateTimeParseException ignore) {
            // fall through
        }

        // Try pattern with trailing zone token like 'UTC', 'Etc/UTC', 'GMT', 'Z' or '+08:00' /
        // '8:00'
        int lastSpace = trimmed.lastIndexOf(' ');
        if (lastSpace > 0 && lastSpace + 1 < trimmed.length()) {
            String dtPart = trimmed.substring(0, lastSpace);
            String zonePart = trimmed.substring(lastSpace + 1).trim();
            try {
                DateTimeFormatter ldtFormatter =
                        new java.time.format.DateTimeFormatterBuilder()
                                .appendPattern("yyyy-MM-dd HH:mm:ss")
                                .optionalStart()
                                .appendLiteral('.')
                                .appendFraction(
                                        java.time.temporal.ChronoField.NANO_OF_SECOND, 1, 9, false)
                                .optionalEnd()
                                .toFormatter();
                java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(dtPart, ldtFormatter);

                java.time.ZoneOffset zoneOffset = null;
                String z = zonePart;
                if ("UTC".equalsIgnoreCase(z)
                        || "Etc/UTC".equalsIgnoreCase(z)
                        || "GMT".equalsIgnoreCase(z)
                        || "Z".equals(z)) {
                    zoneOffset = java.time.ZoneOffset.UTC;
                } else {
                    // support '+08:00', '-05:00', '8:00', '0:00' (no sign -> '+')
                    if (!z.startsWith("+")
                            && !z.startsWith("-")
                            && z.matches("^\\d{1,2}:\\d{2}$")) {
                        z = "+" + z;
                    }
                    if (z.matches("^[+-]\\d{1,2}:\\d{2}$")) {
                        // normalize to two-digit hour
                        String sign = z.substring(0, 1);
                        String[] hm = z.substring(1).split(":", 2);
                        int h = Integer.parseInt(hm[0]);
                        int m = Integer.parseInt(hm[1]);
                        if (h >= 0 && h <= 18 && m >= 0 && m < 60) {
                            String hh = (h < 10 ? "0" : "") + h;
                            String mm = (m < 10 ? "0" : "") + m;
                            zoneOffset = java.time.ZoneOffset.of(sign + hh + ":" + mm);
                        }
                    }
                }

                if (zoneOffset != null) {
                    return OffsetDateTime.of(ldt, zoneOffset);
                }
            } catch (Exception ignore) {
                // fall through to final error
            }
        }

        // If all parsing attempts fail, throw exception
        throw new DateTimeParseException(
                "Unable to parse OffsetDateTime from string: " + str, trimmed, 0);
    }

    private static OracleOffsetDateTimeAccessor resolveOracleOffsetDateTimeAccessor(Class<?> clazz)
            throws NoSuchMethodException {
        try {
            Method method = clazz.getMethod("toOffsetDateTime");
            return new OracleOffsetDateTimeAccessor(method, false);
        } catch (NoSuchMethodException e) {
            Method method = clazz.getMethod("offsetDateTimeValue", Connection.class);
            return new OracleOffsetDateTimeAccessor(method, true);
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

    private static final class OracleOffsetDateTimeAccessor {
        private final Method method;
        private final boolean requiresConnection;

        private OracleOffsetDateTimeAccessor(Method method, boolean requiresConnection) {
            this.method = method;
            this.requiresConnection = requiresConnection;
        }

        private OffsetDateTime invoke(Object obj, ResultSet resultSet) throws Exception {
            if (requiresConnection) {
                Connection connection = resultSet.getStatement().getConnection();
                return (OffsetDateTime) method.invoke(obj, connection);
            } else {
                return (OffsetDateTime) method.invoke(obj);
            }
        }
    }

    @FunctionalInterface
    private interface ThrowingFunction<T, R, E extends Exception> {
        R apply(T t, int columnIndex) throws E;
    }
}
