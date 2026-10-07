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

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;

@Slf4j
public class DuckDBJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

    /** Set once per reader so the remaining historical limitation is not logged per row. */
    private transient boolean aliasTimestampFallbackWarned;

    /** Reads standard timestamps with the typed getter and aliases with the guarded UTC route. */
    @Override
    protected LocalDateTime readTimestamp(ResultSet rs, int resultSetIndex) throws SQLException {
        if (isAliasTimestamp(rs, resultSetIndex)) {
            if (!aliasTimestampFallbackWarned) {
                aliasTimestampFallbackWarned = true;
                log.warn(
                        "DuckDB JDBC 1.3.1 timestamp aliases retain the legacy getter for "
                                + "pre-epoch values. Historical dates and negative fractional "
                                + "timestamps may still be normalized by the driver. The JDBC "
                                + "getString accessor is also Timestamp-based; validate SQL "
                                + "text conversions on your driver version before using them");
            }
            // The driver renders unzoned timestamps in the JVM default zone, which shifts values
            // inside a DST gap/overlap. Reading against an explicit UTC calendar and interpreting
            // the result as a UTC instant returns the stored wall clock for every time zone from
            // the Unix epoch onwards; earlier values are handled by the guard below.
            Timestamp value = rs.getTimestamp(resultSetIndex, Calendar.getInstance(UTC_TIME_ZONE));
            if (value == null) {
                return null;
            }
            Instant instant = value.toInstant();
            if (instant.isBefore(Instant.EPOCH)) {
                // For pre-epoch values, the driver's UTC calendar route can mix java.time's
                // historical offsets with legacy Timestamp rules. Retain the plain getter to
                // avoid introducing an offset shift; existing driver limitations still apply.
                Timestamp legacy = rs.getTimestamp(resultSetIndex);
                return legacy == null ? null : legacy.toLocalDateTime();
            }
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        }
        // Standard TIMESTAMP supports the typed getter. Report its errors unchanged rather
        // than silently switching to another Java getter or remembering a missing capability.
        return rs.getObject(resultSetIndex, LocalDateTime.class);
    }

    /**
     * Whether the column is one of the timestamp aliases for which the tested DuckDB JDBC version
     * (1.3.1.0) has no typed getter: {@code getObject(index, LocalDateTime.class)} throws a plain
     * {@link SQLException} that cannot be told apart from a real data error. Such columns are
     * recognized from the result set metadata instead and read through the UTC calendar route.
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
    public String converterName() {
        return DatabaseIdentifier.DUCKDB;
    }
}
