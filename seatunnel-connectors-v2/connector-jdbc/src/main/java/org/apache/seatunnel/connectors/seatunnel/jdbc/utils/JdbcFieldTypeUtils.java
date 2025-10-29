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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

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

    private static <T> T getNullableValue(
            ResultSet resultSet,
            int columnIndex,
            ThrowingFunction<ResultSet, T, SQLException> getter)
            throws SQLException {
        if (resultSet.getObject(columnIndex) == null) {
            return null;
        }
        return getter.apply(resultSet, columnIndex);
    }

    @FunctionalInterface
    private interface ThrowingFunction<T, R, E extends Exception> {
        R apply(T t, int columnIndex) throws E;
    }

    public static OffsetDateTime getOffsetDateTime(ResultSet resultSet, int columnIndex)
            throws SQLException {
        // Try JDBC 4.2 API first – most modern drivers (Oracle, PostgreSQL, etc.) support this.
        try {
            OffsetDateTime direct = resultSet.getObject(columnIndex, OffsetDateTime.class);
            if (direct != null) {
                return direct;
            }
        } catch (AbstractMethodError | SQLFeatureNotSupportedException | SQLException ignored) {
            // fall through to best-effort handling below
        }

        // Avoid driver-specific issues by not calling getObject with a target class.
        Object obj;
        try {
            obj = resultSet.getObject(columnIndex);
        } catch (Throwable e) {
            // Defensive: buggy drivers might throw unchecked exceptions; try alternative approaches
            obj = null;
        }

        if (obj == null) {
            return null;
        }

        // Handle OffsetDateTime directly
        if (obj instanceof OffsetDateTime) {
            return (OffsetDateTime) obj;
        }

        // Handle ZonedDateTime by converting to OffsetDateTime
        if (obj instanceof java.time.ZonedDateTime) {
            return ((java.time.ZonedDateTime) obj).toOffsetDateTime();
        }

        // Handle Instant by converting to UTC OffsetDateTime
        if (obj instanceof java.time.Instant) {
            return ((java.time.Instant) obj).atOffset(ZoneOffset.UTC);
        }

        // Handle Timestamp by converting to UTC OffsetDateTime
        if (obj instanceof Timestamp) {
            Timestamp ts = (Timestamp) obj;
            return ts.toInstant().atOffset(ZoneOffset.UTC);
        }

        // Handle java.util.Date by converting to UTC OffsetDateTime
        if (obj instanceof java.util.Date) {
            return ((java.util.Date) obj).toInstant().atOffset(ZoneOffset.UTC);
        }

        // Handle Long (epoch milliseconds) by converting to UTC OffsetDateTime
        if (obj instanceof Long) {
            return java.time.Instant.ofEpochMilli((Long) obj).atOffset(ZoneOffset.UTC);
        }

        // Handle Oracle-specific TIMESTAMPTZ objects
        if (obj.getClass().getName().equals("oracle.sql.TIMESTAMPTZ")) {
            try {
                // Prefer offsetDateTimeValue(Connection) when available to preserve original zone.
                java.lang.reflect.Method offsetMethod =
                        obj.getClass().getMethod("offsetDateTimeValue", java.sql.Connection.class);
                java.sql.Connection connection = extractConnection(resultSet);
                if (connection != null) {
                    Object value = offsetMethod.invoke(obj, connection);
                    if (value instanceof OffsetDateTime) {
                        return (OffsetDateTime) value;
                    }
                    if (value instanceof Timestamp) {
                        Timestamp ts = (Timestamp) value;
                        return ts.toInstant().atOffset(ZoneOffset.UTC);
                    }
                }
            } catch (NoSuchMethodException ignore) {
                // Fall back to timestampValue when offset method is unavailable.
                try {
                    java.lang.reflect.Method timestampValueMethod =
                            obj.getClass().getMethod("timestampValue", java.sql.Connection.class);
                    java.sql.Connection connection = extractConnection(resultSet);
                    if (connection != null) {
                        Timestamp ts = (Timestamp) timestampValueMethod.invoke(obj, connection);
                        if (ts != null) {
                            return ts.toInstant().atOffset(ZoneOffset.UTC);
                        }
                    }
                } catch (Exception inner) {
                    // Fall through to string parsing
                }
            } catch (Exception e) {
                // Fall through to string parsing
            }
        }

        // Handle PostgreSQL-specific objects
        if (obj.getClass().getName().startsWith("org.postgresql.")) {
            try {
                // Try to get string representation and parse it
                String str = obj.toString();
                if (str != null && !str.isEmpty()) {
                    return parseOffsetDateTimeFromString(str);
                }
            } catch (Exception e) {
                // Fall through to string parsing
            }
        }

        // Try parsing from string value if it contains offset information
        String str = null;
        try {
            str = resultSet.getString(columnIndex);
        } catch (Throwable ignored) {
            // Try toString() on the object as last resort
            try {
                str = obj.toString();
            } catch (Throwable ignored2) {
                // ignore and keep str as null
            }
        }

        if (str != null) {
            return parseOffsetDateTimeFromString(str);
        }

        // Unknown representation; return null to let caller decide further handling
        return null;
    }

    private static OffsetDateTime parseOffsetDateTimeFromString(String str) {
        if (str == null || str.trim().isEmpty()) {
            return null;
        }

        String iso = normalizeOffsetDateTimeString(str);

        // If ends with 'Z', try parse directly
        if (iso.endsWith("Z")) {
            try {
                return OffsetDateTime.parse(iso);
            } catch (Exception ignored) {
            }
        }

        // Add colon to offsets like +HH or +HHMM -> +HH:MM
        if (iso.matches(".*[+-]\\d{2}$")) {
            iso = iso + ":00";
        } else if (iso.matches(".*[+-]\\d{4}$")) {
            iso = iso.substring(0, iso.length() - 2) + ":" + iso.substring(iso.length() - 2);
        }

        try {
            return OffsetDateTime.parse(iso);
        } catch (Exception ignored) {
        }

        try {
            // Handle formats without 'T' separator
            if (iso.contains(" ") && !iso.contains("T")) {
                iso = iso.replace(" ", "T");
                return OffsetDateTime.parse(iso);
            }
        } catch (Exception ignored) {
        }

        try {
            // Last resort: drop offset if present and treat as UTC
            String withoutOffset = iso.replaceFirst("([+-]\\d{2}:?\\d{2}|Z)$", "");
            Timestamp ts = Timestamp.valueOf(withoutOffset.replace('T', ' '));
            return ts.toInstant().atOffset(ZoneOffset.UTC);
        } catch (Exception ignored) {
        }

        return null;
    }

    private static String normalizeOffsetDateTimeString(String raw) {
        String trimmed = raw.trim();

        // Replace textual UTC suffix with 'Z'
        if (trimmed.endsWith(" UTC")) {
            trimmed = trimmed.substring(0, trimmed.length() - 4) + "Z";
        }

        // Replace the first space (between date and time) with 'T'
        int firstSpace = trimmed.indexOf(' ');
        if (firstSpace >= 0) {
            trimmed = trimmed.substring(0, firstSpace) + "T" + trimmed.substring(firstSpace + 1);
        }

        // Remove remaining spaces (e.g. before offset)
        trimmed = trimmed.replace(" ", "");
        return trimmed;
    }

    private static java.sql.Connection extractConnection(ResultSet resultSet) {
        try {
            java.sql.Statement statement = resultSet.getStatement();
            if (statement != null) {
                return statement.getConnection();
            }
        } catch (SQLException ignored) {
            // ignore and fall back to other strategies
        }
        return null;
    }
}
