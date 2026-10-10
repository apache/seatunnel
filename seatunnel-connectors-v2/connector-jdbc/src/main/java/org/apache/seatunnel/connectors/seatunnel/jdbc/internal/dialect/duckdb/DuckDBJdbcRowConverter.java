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

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import lombok.extern.slf4j.Slf4j;

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
import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;

@Slf4j
public class DuckDBJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

    /** Set once per reader so the remaining historical limitation is not logged per row. */
    private transient boolean aliasTimestampFallbackWarned;

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
        Timestamp utcTimestamp =
                rs.getTimestamp(resultSetIndex, Calendar.getInstance(UTC_TIME_ZONE));
        if (utcTimestamp == null) {
            return null;
        }
        Instant instant = utcTimestamp.toInstant();
        if (instant.isBefore(Instant.EPOCH)) {
            // Before the epoch the driver mixes java.time's historical offsets into the UTC
            // calendar route. Keep the legacy plain getter, whose historical limitation and the
            // negative fractional-second read defect are unchanged.
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
     * Whether the column is one of the timestamp aliases for which the tested DuckDB JDBC version
     * (1.3.1.0) has no typed getter: {@code getObject(index, LocalDateTime.class)} throws a plain
     * {@link SQLException} that cannot be told apart from a real data error. Such columns are
     * recognized from the result set metadata instead and read through the reconciled routes.
     */
    private boolean isAliasTimestamp(ResultSet rs, int resultSetIndex) throws SQLException {
        ResultSetMetaData metadata = rs.getMetaData();
        if (metadata == null) {
            return false;
        }
        String columnTypeName = metadata.getColumnTypeName(resultSetIndex);
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

    @Override
    public String converterName() {
        return DatabaseIdentifier.DUCKDB;
    }
}
