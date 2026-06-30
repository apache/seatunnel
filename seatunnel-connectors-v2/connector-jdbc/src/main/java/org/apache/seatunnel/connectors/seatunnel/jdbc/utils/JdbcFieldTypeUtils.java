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
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.TimeZone;

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

    /**
     * Reads a NTZ (No Time Zone) timestamp column as {@link LocalDateTime}, free from JVM default
     * timezone influence.
     *
     * <p>Strategy:
     *
     * <ol>
     *   <li>Try {@code getObject(index, LocalDateTime.class)} first — supported by modern JDBC
     *       drivers (PostgreSQL ≥ 42.2, MySQL Connector/J ≥ 8.0, MariaDB Connector/J ≥ 3.x). This
     *       returns the wall-clock value exactly as stored, with no timezone conversion.
     *   <li>Try {@code getTimestamp(index, utcCalendar)}: passing a UTC {@link Calendar} forces the
     *       driver to treat the raw bytes as UTC epoch millis, then {@link
     *       Timestamp#toLocalDateTime()} reconstructs the wall-clock via UTC — again
     *       timezone-neutral. A new {@link Calendar} is created per call to avoid thread-safety
     *       issues with a shared mutable instance. Not supported by all drivers (e.g. Hive JDBC).
     *   <li>Last resort: plain {@code getTimestamp(index)} — may be affected by JVM timezone for
     *       drivers that apply session/JVM timezone conversion internally (e.g. Hive JDBC).
     * </ol>
     *
     * @param resultSet the JDBC result set
     * @param columnIndex 1-based column index
     * @return the wall-clock {@link LocalDateTime} exactly as stored in the DB, or {@code null}
     */
    public static LocalDateTime getLocalDateTime(ResultSet resultSet, int columnIndex)
            throws SQLException {
        // Prefer the modern JDBC 4.2 API — returns wall-clock value directly, no TZ involved
        try {
            return resultSet.getObject(columnIndex, LocalDateTime.class);
        } catch (SQLException | UnsupportedOperationException ignored) {
            // Driver does not support getObject(index, LocalDateTime.class) — fall back
        }
        // Try UTC Calendar to avoid JVM-default-timezone influence.
        // A new Calendar is created per call to avoid thread-safety issues with a shared instance.
        try {
            Timestamp ts =
                    resultSet.getTimestamp(
                            columnIndex, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
            return ts == null ? null : ts.toLocalDateTime();
        } catch (SQLException | UnsupportedOperationException ignored) {
            // Driver does not support getTimestamp(index, Calendar) — fall back (e.g. Hive JDBC)
        }
        // Last resort: plain getTimestamp() — may be affected by JVM timezone for some drivers
        Timestamp ts = resultSet.getTimestamp(columnIndex);
        return ts == null ? null : ts.toLocalDateTime();
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
        // Avoid using Timestamp.toInstant() directly because the Timestamp was constructed
        // with JVM-default-timezone semantics, which would shift the value by the JVM offset.
        // Instead, try to re-read the column as a string and parse it with timezone info preserved.
        if (obj instanceof Timestamp) {
            String strVal = resultSet.getString(columnIndex);
            if (strVal == null) {
                return null;
            }
            try {
                return parseOffsetDateTimeFromString(strVal);
            } catch (Exception e) {
                // Last resort: use the instant-based conversion (may shift by JVM offset)
                return ((Timestamp) obj).toInstant().atOffset(ZoneOffset.UTC);
            }
        }

        // Handle java.util.Date
        if (obj instanceof java.util.Date) {
            return ((java.util.Date) obj).toInstant().atOffset(ZoneOffset.UTC);
        }

        // Handle Long (epoch milliseconds)
        if (obj instanceof Long) {
            return Instant.ofEpochMilli((Long) obj).atOffset(ZoneOffset.UTC);
        }

        // Handle Oracle-specific TIMESTAMPLTZ / TIMESTAMPTZ types.
        // oracle.sql.TIMESTAMPLTZ and oracle.sql.TIMESTAMPTZ do not implement standard interfaces
        // and their toString() returns the Java object reference (e.g.
        // "oracle.sql.TIMESTAMPLTZ@xxx").
        // Fall back to ResultSet.getTimestamp() which the Oracle JDBC driver converts correctly.
        String objClassName = obj.getClass().getName();
        if (objClassName.equals("oracle.sql.TIMESTAMPLTZ")
                || objClassName.equals("oracle.sql.TIMESTAMPTZ")) {
            Timestamp oracleTs =
                    resultSet.getTimestamp(
                            columnIndex, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
            if (oracleTs == null) {
                return null;
            }
            return oracleTs.toInstant().atOffset(ZoneOffset.UTC);
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
        // Only replace the first space (between date and time) with 'T'.
        // Then remove any space before the timezone offset (+/-).
        // e.g. "2026-04-15 04:53:44.407 +08:00" → "2026-04-15T04:53:44.407+08:00"
        normalized = normalized.replaceFirst(" ", "T");
        normalized = normalized.replace(" +", "+").replace(" -", "-");
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
