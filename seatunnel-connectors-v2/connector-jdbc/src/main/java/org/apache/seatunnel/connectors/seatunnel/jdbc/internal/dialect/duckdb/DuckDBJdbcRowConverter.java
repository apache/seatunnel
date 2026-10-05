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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class DuckDBJdbcRowConverter extends AbstractJdbcRowConverter {

    private transient ResultSet timestampResultSet;
    private transient Set<Integer> unsupportedTimestampColumns;

    @Override
    protected LocalDateTime readTimestamp(ResultSet rs, int resultSetIndex) throws SQLException {
        if (timestampResultSet != rs) {
            timestampResultSet = rs;
            unsupportedTimestampColumns = new HashSet<>();
        }
        // Avoid Timestamp's JVM-zone and Gregorian-cutover normalization when supported.
        if (!unsupportedTimestampColumns.contains(resultSetIndex)) {
            try {
                return rs.getObject(resultSetIndex, LocalDateTime.class);
            } catch (SQLException | UnsupportedOperationException ignored) {
                // DuckDB JDBC 1.3.1 does not support typed reads for TIMESTAMP_S/MS/NS.
                Timestamp value = rs.getTimestamp(resultSetIndex);
                unsupportedTimestampColumns.add(resultSetIndex);
                return value == null ? null : value.toLocalDateTime();
            }
        }
        // Keep the plain fallback: the Calendar overload shifts unzoned timestamps.
        Timestamp value = rs.getTimestamp(resultSetIndex);
        return value == null ? null : value.toLocalDateTime();
    }

    @Override
    public String converterName() {
        return DatabaseIdentifier.DUCKDB;
    }
}
