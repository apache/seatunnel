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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;

@Slf4j
public class DuckDBJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

    /**
     * Renders a local timestamp as text that the tested DuckDB JDBC driver parses exactly: always
     * with seconds and an optional fraction, and never with the {@code +} sign the formatter would
     * otherwise add to a year of more than four digits, because DuckDB rejects that form.
     */
    private static final DateTimeFormatter ISO_LOCAL_TIMESTAMP =
            new DateTimeFormatterBuilder()
                    .appendValue(ChronoField.YEAR, 4, 10, SignStyle.NORMAL)
                    .appendPattern("-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .optionalEnd()
                    .toFormatter(Locale.ROOT);

    /** Set once per reader so the remaining historical limitation is not logged per row. */
    private transient boolean aliasTimestampFallbackWarned;

    /**
     * The result set the cached alias decisions below belong to. Both fields are transient driver
     * state: the converter is serialized with the input format and must not carry a result set into
     * another JVM. Their life cycle follows {@code JdbcInputFormat}, which owns one converter per
     * split and one result set per {@code open()}, read by a single thread.
     */
    private transient ResultSet aliasMetadataResultSet;

    /** Per-column alias decisions of {@link #aliasMetadataResultSet}, indexed by column number. */
    private transient boolean[] aliasColumns;

    /**
     * Reads DuckDB timestamp columns without reinterpreting the stored wall clock. Standard {@code
     * TIMESTAMP} columns keep the typed getter; the aliases are reconciled between the typed values
     * of two JDBC routes because neither one is correct on its own.
     */
    @Override
    protected LocalDateTime readTimestamp(ResultSet rs, int resultSetIndex) throws SQLException {
        if (!isAliasTimestamp(rs, resultSetIndex)) {
            // Standard TIMESTAMP supports the typed getter. Report its errors unchanged rather
            // than silently switching to another Java getter or remembering a missing capability.
            return rs.getObject(resultSetIndex, LocalDateTime.class);
        }
        // An alias has no typed getter. The driver renders unzoned timestamps in the JVM default
        // zone, so it moves a value inside a DST gap forward by the transition; the explicit UTC
        // calendar recovers the stored wall clock for such values but adds the fall-back offset
        // for values after a DST fall-back. Read both routes and reconcile below; a NULL value is
        // returned before any fallback warning.
        // Calendar is mutable, so keep it local to this call as JdbcFieldTypeUtils does.
        Timestamp utcTimestamp =
                rs.getTimestamp(resultSetIndex, Calendar.getInstance(UTC_TIME_ZONE));
        if (utcTimestamp == null) {
            return null;
        }
        Instant instant = utcTimestamp.toInstant();
        if (instant.isBefore(Instant.EPOCH)) {
            // Before the epoch the driver mixes java.time's historical offsets into the UTC
            // calendar route. Keep the legacy plain getter, whose historical limitation and the
            // negative fractional-second read defect are unchanged. Historical offsets and
            // pre-epoch DST gaps remain limitations of this legacy read path.
            warnOnceBeforeLegacyAliasFallback();
            Timestamp legacy = rs.getTimestamp(resultSetIndex);
            return legacy == null ? null : legacy.toLocalDateTime();
        }
        LocalDateTime utcWallClock = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        Timestamp plainTimestamp = rs.getTimestamp(resultSetIndex);
        LocalDateTime plainWallClock =
                plainTimestamp == null ? null : plainTimestamp.toLocalDateTime();
        if (Objects.equals(plainWallClock, utcWallClock)) {
            return utcWallClock;
        }
        // The routes disagree only across a DST transition in the default zone: the plain getter
        // moved a nonexistent local time forward out of a spring-forward gap, which the UTC route
        // preserves, whereas after a fall-back the UTC route added the transition offset and the
        // plain getter already kept the stored wall clock. Prefer the UTC value only for a gap.
        if (ZoneId.systemDefault().getRules().getValidOffsets(utcWallClock).isEmpty()) {
            return utcWallClock;
        }
        return plainWallClock;
    }

    /** Logs the remaining historical limitation once, and only when it is actually reached. */
    private void warnOnceBeforeLegacyAliasFallback() {
        if (aliasTimestampFallbackWarned) {
            return;
        }
        aliasTimestampFallbackWarned = true;
        log.warn(
                "DuckDB JDBC timestamp aliases fall back to the legacy getter before the Unix "
                        + "epoch. Historical dates may still be normalized by the driver and "
                        + "negative fractional timestamps can be read one second late; validate "
                        + "these conversions on your driver version before relying on them");
    }

    /**
     * Whether the column is one of the timestamp aliases that has no typed getter: {@code
     * getObject(index, LocalDateTime.class)} throws a plain {@link SQLException} for them, which
     * cannot be told apart from a real data error. The decision is based on the column type names
     * reported by the result set metadata, so it does not depend on the JDBC driver version. A
     * future driver release that adds a typed getter for these aliases will therefore not be picked
     * up automatically; the reconciled routes stay in use until this method is updated.
     *
     * <p>The per-column decisions are taken once per result set and reused for its remaining
     * values, so a long result set does not repeat the metadata lookup for every cell. Handing a
     * different result set to the converter rebuilds them, which also covers switching back to a
     * result set that was read before.
     */
    private boolean isAliasTimestamp(ResultSet rs, int resultSetIndex) throws SQLException {
        if (rs != aliasMetadataResultSet) {
            aliasColumns = readAliasColumns(rs);
            // Metadata that cannot be read keeps the typed getter and is looked up again next time
            // instead of being remembered as "no column is an alias".
            aliasMetadataResultSet = aliasColumns == null ? null : rs;
        }
        boolean[] columns = aliasColumns;
        return columns != null
                && resultSetIndex > 0
                && resultSetIndex < columns.length
                && columns[resultSetIndex];
    }

    /**
     * Classifies every column of the result set in one metadata pass.
     *
     * @return the per-column decisions indexed by column number, or {@code null} when the result
     *     set reports no metadata
     */
    private static boolean[] readAliasColumns(ResultSet rs) throws SQLException {
        ResultSetMetaData metadata = rs.getMetaData();
        if (metadata == null) {
            return null;
        }
        int columnCount = metadata.getColumnCount();
        boolean[] columns = new boolean[columnCount + 1];
        for (int column = 1; column <= columnCount; column++) {
            columns[column] = isAliasTypeName(metadata.getColumnTypeName(column));
        }
        return columns;
    }

    /** Type-name comparison for one column, independent of the driver version and the locale. */
    private static boolean isAliasTypeName(@Nullable String columnTypeName) {
        return DuckDBTypeConverter.DUCKDB_TIMESTAMP_S.equalsIgnoreCase(columnTypeName)
                || DuckDBTypeConverter.DUCKDB_TIMESTAMP_MS.equalsIgnoreCase(columnTypeName)
                || DuckDBTypeConverter.DUCKDB_TIMESTAMP_NS.equalsIgnoreCase(columnTypeName);
    }

    @Override
    protected LocalTime readTime(ResultSet resultSet, int index) throws SQLException {
        // java.sql.Time discards DuckDB TIME's fractional seconds.
        return JdbcFieldTypeUtils.getLocalTime(resultSet, index);
    }

    @Override
    protected void writeTime(PreparedStatement statement, int index, LocalTime time)
            throws SQLException {
        statement.setObject(index, time);
    }

    /**
     * Binds a local timestamp as timestamp text, which the DuckDB JDBC driver this connector
     * bundles ({@code duckdb.version} in connector-jdbc's pom) stores unchanged.
     *
     * <p>The driver converts both {@code setTimestamp} and the typed {@code setObject} binding
     * through the JVM default zone. A local time inside a DST gap is therefore moved forward by the
     * transition, the 1582 Gregorian cutover is normalized even when the JVM runs in UTC, and the
     * conversion is lossy below the microsecond. Text reaches DuckDB unchanged and is cast to the
     * target column type, which leaves the stored wall clock intact and keeps every digit up to the
     * target column's precision. A SQL {@code NULL} is bound by the base class before this method
     * is called, and every other data type keeps the base behaviour.
     */
    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        if (value instanceof LocalDateTime && seaTunnelDataType.getSqlType() == SqlType.TIMESTAMP) {
            statement.setString(statementIndex, ISO_LOCAL_TIMESTAMP.format((LocalDateTime) value));
            return;
        }
        super.setValueToStatementByDataType(
                value, statement, seaTunnelDataType, statementIndex, sourceType);
    }

    @Override
    public String converterName() {
        return DatabaseIdentifier.DUCKDB;
    }
}
