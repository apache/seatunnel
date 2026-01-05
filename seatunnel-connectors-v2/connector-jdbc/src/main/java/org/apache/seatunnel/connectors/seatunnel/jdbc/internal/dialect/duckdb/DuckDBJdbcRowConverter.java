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

@Slf4j
public class DuckDBJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.DUCKDB;
    }

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
                    statement.setObject(statementIndex, value);
                    break;
            }
        } catch (SQLException e) {
            log.error(
                    "Failed to set DuckDB parameter: index={}, type={}, valueClass={}, value={}, error={}",
                    statementIndex,
                    seaTunnelDataType.getSqlType(),
                    value.getClass().getSimpleName(),
                    value,
                    e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error(
                    "Unexpected DuckDB error: index={}, type={}, valueClass={}, value={}, error={}",
                    statementIndex,
                    seaTunnelDataType.getSqlType(),
                    value.getClass().getSimpleName(),
                    value,
                    e.getMessage());
            throw new SQLException("Failed to set DuckDB parameter", e);
        }
    }
}
