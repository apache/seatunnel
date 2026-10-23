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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Optional;

@Slf4j
public class SqlserverJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    protected LocalTime readTime(ResultSet rs, int resultSetIndex) throws SQLException {
        Timestamp sqlTime = JdbcFieldTypeUtils.getTimestamp(rs, resultSetIndex);
        return Optional.ofNullable(sqlTime)
                .map(e -> e.toLocalDateTime().toLocalTime())
                .orElse(null);
    }

    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        if (seaTunnelDataType.getSqlType().equals(SqlType.TIMESTAMP_TZ)) {
            // DATETIMEOFFSET supports OffsetDateTime directly via setObject
            statement.setObject(statementIndex, (java.time.OffsetDateTime) value);
        } else {
            super.setValueToStatementByDataType(
                    value, statement, seaTunnelDataType, statementIndex, sourceType);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>SQL Server requires explicit type information for null values in binary columns (IMAGE,
     * VARBINARY, BINARY). Using {@code setObject(index, null)} causes the driver to infer the type
     * as NVARCHAR, which leads to conversion errors:
     *
     * <pre>
     * Implicit conversion from data type nvarchar to image is not allowed
     * </pre>
     *
     * <p>This method uses {@code setNull(index, sqlType)} with the appropriate JDBC type based on
     * the source column type.
     */
    @Override
    protected void setNullToStatementByDataType(
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {

        if (seaTunnelDataType.getSqlType() == SqlType.BYTES) {
            statement.setNull(statementIndex, resolveSqlServerBytesNullType(sourceType));
            return;
        }

        statement.setObject(statementIndex, null);
    }

    /**
     * Resolves the appropriate JDBC type for SQL Server binary columns when the value is null.
     *
     * <p>SQL Server has multiple binary types, each mapping to a different JDBC type:
     *
     * <ul>
     *   <li>IMAGE → LONGVARBINARY (legacy large binary type)
     *   <li>VARBINARY(*) → VARBINARY
     *   <li>VARBINARY(MAX) → VARBINARY
     *   <li>BINARY(*) → BINARY (fixed-length)
     *   <li>other → VARBINARY
     * </ul>
     *
     * @param sourceType the SQL Server column type (e.g., "IMAGE", "VARBINARY", "BINARY")
     * @return the JDBC type constant from {@link java.sql.Types}
     */
    private int resolveSqlServerBytesNullType(@Nullable String sourceType) {
        if (sourceType == null) {
            log.warn(
                    "Cannot determine source type for BYTES field, defaulting to VARBINARY. "
                            + "If this causes errors, please provide databaseTableSchema.");
            return java.sql.Types.VARBINARY;
        }

        if (SqlServerTypeConverter.SQLSERVER_IMAGE.equals(sourceType)) {
            return java.sql.Types.LONGVARBINARY;
        }
        if (sourceType.startsWith(SqlServerTypeConverter.SQLSERVER_VARBINARY)) {
            return java.sql.Types.VARBINARY;
        }
        if (sourceType.startsWith(SqlServerTypeConverter.SQLSERVER_BINARY)) {
            return java.sql.Types.BINARY;
        }

        // Unknown type, log warning and use conservative LONGVARBINARY
        log.warn(
                "Unknown SQLServer binary type: {}, defaulting to LONGVARBINARY. "
                        + "This may cause issues if the target column is not a large binary type.",
                sourceType);
        return java.sql.Types.VARBINARY;
    }
}
