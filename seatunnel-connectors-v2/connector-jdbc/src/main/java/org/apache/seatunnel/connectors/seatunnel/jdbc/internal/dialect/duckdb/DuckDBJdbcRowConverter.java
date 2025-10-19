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
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.checkerframework.checker.nullness.qual.Nullable;

import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * DuckDB row converter for converting between DuckDB data types and SeaTunnel data types.
 *
 * <p>This converter handles the mapping of SeaTunnel data types to DuckDB-specific JDBC types. Uses
 * type-specific setters instead of generic setObject to avoid batch execution issues with DuckDB
 * JDBC driver.
 */
@Slf4j
public class DuckDBJdbcRowConverter extends AbstractJdbcRowConverter {

    /**
     * Get the converter name identifier.
     *
     * @return the database identifier for DuckDB
     */
    @Override
    public String converterName() {
        return DatabaseIdentifier.DUCKDB;
    }

    /**
     * Set value to prepared statement based on SeaTunnel data type.
     *
     * <p>This method uses type-specific setters (setByte, setInt, setString, etc.) instead of
     * generic setObject to ensure compatibility with DuckDB's batch execution. Each data type is
     * handled explicitly to avoid type conversion issues.
     *
     * @param value the value to set (can be null)
     * @param statement the prepared statement
     * @param seaTunnelDataType the SeaTunnel data type
     * @param statementIndex the parameter index in the statement (1-based)
     * @param sourceType the source type name (optional, can be null)
     * @throws SQLException if setting the value fails
     */
    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {

        log.debug(
                "Setting DuckDB parameter: index={}, type={}, valueClass={}, value={}",
                statementIndex,
                seaTunnelDataType.getSqlType(),
                value != null ? value.getClass().getSimpleName() : "null",
                value);

        if (value == null) {
            statement.setObject(statementIndex, null);
            return;
        }

        try {
            /*
             * Use type-specific setters instead of setObject to avoid DuckDB batch execution issues.
             * DuckDB JDBC driver has better compatibility with explicit type setters.
             */
            switch (seaTunnelDataType.getSqlType()) {
                case TINYINT:
                    statement.setByte(statementIndex, ((Number) value).byteValue());
                    break;
                case SMALLINT:
                    statement.setShort(statementIndex, ((Number) value).shortValue());
                    break;
                case INT:
                    statement.setInt(statementIndex, ((Number) value).intValue());
                    break;
                case BIGINT:
                    statement.setLong(statementIndex, ((Number) value).longValue());
                    break;
                case FLOAT:
                    statement.setFloat(statementIndex, ((Number) value).floatValue());
                    break;
                case DOUBLE:
                    statement.setDouble(statementIndex, ((Number) value).doubleValue());
                    break;
                case DECIMAL:
                    if (value instanceof java.math.BigDecimal) {
                        statement.setBigDecimal(statementIndex, (java.math.BigDecimal) value);
                    } else {
                        statement.setBigDecimal(
                                statementIndex, new java.math.BigDecimal(value.toString()));
                    }
                    break;
                case BOOLEAN:
                    statement.setBoolean(statementIndex, (Boolean) value);
                    break;
                case STRING:
                    statement.setString(statementIndex, value.toString());
                    break;
                case DATE:
                    if (value instanceof java.time.LocalDate) {
                        statement.setDate(
                                statementIndex, java.sql.Date.valueOf((java.time.LocalDate) value));
                    } else if (value instanceof java.sql.Date) {
                        statement.setDate(statementIndex, (java.sql.Date) value);
                    } else {
                        statement.setDate(statementIndex, java.sql.Date.valueOf(value.toString()));
                    }
                    break;
                case TIME:
                    if (value instanceof java.time.LocalTime) {
                        statement.setTime(
                                statementIndex, java.sql.Time.valueOf((java.time.LocalTime) value));
                    } else if (value instanceof java.sql.Time) {
                        statement.setTime(statementIndex, (java.sql.Time) value);
                    } else {
                        statement.setTime(statementIndex, java.sql.Time.valueOf(value.toString()));
                    }
                    break;
                case TIMESTAMP:
                    if (value instanceof java.time.LocalDateTime) {
                        statement.setTimestamp(
                                statementIndex,
                                java.sql.Timestamp.valueOf((java.time.LocalDateTime) value));
                    } else if (value instanceof java.sql.Timestamp) {
                        statement.setTimestamp(statementIndex, (java.sql.Timestamp) value);
                    } else {
                        statement.setTimestamp(
                                statementIndex, java.sql.Timestamp.valueOf(value.toString()));
                    }
                    break;
                default:
                    // For any other types, fall back to setObject
                    statement.setObject(statementIndex, value);
                    break;
            }
        } catch (SQLException e) {
            /*
             * Enhanced error logging for DuckDB-specific issues.
             * Provides detailed context for debugging type conversion problems.
             */
            log.error(
                    "Failed to set DuckDB parameter: index={}, type={}, valueClass={}, value={}, error={}",
                    statementIndex,
                    seaTunnelDataType.getSqlType(),
                    value.getClass().getSimpleName(),
                    value.toString(),
                    e.getMessage());
            throw e;
        } catch (Exception e) {
            // Catch any other exceptions and convert to SQLException
            log.error(
                    "Unexpected DuckDB error: index={}, type={}, valueClass={}, value={}, error={}",
                    statementIndex,
                    seaTunnelDataType.getSqlType(),
                    value.getClass().getSimpleName(),
                    value.toString(),
                    e.getMessage());
            throw new SQLException("Failed to set DuckDB parameter", e);
        }
    }
}
