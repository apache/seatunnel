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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_BLOB;

@Slf4j
public class OracleJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.ORACLE;
    }

    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        if (seaTunnelDataType.getSqlType().equals(SqlType.BYTES)) {
            if (ORACLE_BLOB.equals(sourceType)) {
                statement.setBinaryStream(statementIndex, new ByteArrayInputStream((byte[]) value));
            } else {
                statement.setBytes(statementIndex, (byte[]) value);
            }
            return;
        }
        if (seaTunnelDataType.getSqlType().equals(SqlType.TIMESTAMP_TZ)) {
            OffsetDateTime odt = (OffsetDateTime) value;
            try {
                statement.setObject(statementIndex, odt);
                return;
            } catch (AbstractMethodError | SQLException e) {
                log.debug(
                        "JDBC 4.2 setObject(OffsetDateTime) failed, trying Oracle-specific approach",
                        e);
            }

            try {
                java.sql.Connection conn = statement.getConnection();
                oracle.jdbc.OracleConnection oracleConn =
                        conn.unwrap(oracle.jdbc.OracleConnection.class);
                String iso = odt.toString();
                String oracleLiteral = iso.replace('T', ' ');
                oracle.sql.TIMESTAMPTZ tsTz = new oracle.sql.TIMESTAMPTZ(oracleConn, oracleLiteral);
                statement.setObject(statementIndex, tsTz);
                return;
            } catch (Throwable t) {
                log.debug(
                        "Oracle-specific TIMESTAMPTZ handling failed, using instant conversion", t);
                try {
                    statement.setTimestamp(
                            statementIndex, java.sql.Timestamp.from(odt.toInstant()));
                    return;
                } catch (SQLException se) {
                    log.error("Failed to set TIMESTAMP_TZ value using all fallback methods", se);
                    throw se;
                }
            }
        }
        super.setValueToStatementByDataType(
                value, statement, seaTunnelDataType, statementIndex, sourceType);
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;

            switch (seaTunnelDataType.getSqlType()) {
                case TIMESTAMP_TZ:
                    // Handle Oracle-specific TIMESTAMP_TZ processing
                    fields[fieldIndex] = getOracleOffsetDateTime(rs, resultSetIndex);
                    break;
                default:
                    // Use parent class implementation for other types
                    SeaTunnelRow parentRow = super.toInternal(rs, tableSchema);
                    fields[fieldIndex] = parentRow.getField(fieldIndex);
                    break;
            }
        }
        return new SeaTunnelRow(fields);
    }

    private OffsetDateTime getOracleOffsetDateTime(ResultSet rs, int columnIndex)
            throws SQLException {
        Object obj = null;
        try {
            obj = rs.getObject(columnIndex);
        } catch (SQLException e) {
            log.debug("Failed to get object from ResultSet at column {}", columnIndex, e);
            return null;
        }

        if (obj == null) {
            return null;
        }

        // Handle Oracle-specific TIMESTAMPTZ objects
        if (obj.getClass().getName().equals("oracle.sql.TIMESTAMPTZ")) {
            try {
                // Use reflection to call timestampValue() method to get Timestamp
                java.lang.reflect.Method timestampValueMethod =
                        obj.getClass().getMethod("timestampValue");
                Timestamp ts = (Timestamp) timestampValueMethod.invoke(obj);
                if (ts != null) {
                    return ts.toInstant().atOffset(ZoneOffset.UTC);
                }
            } catch (Exception e) {
                log.debug(
                        "Failed to extract timestamp from Oracle TIMESTAMPTZ using reflection", e);
            }

            try {
                // Try to get string representation and parse it
                String str = obj.toString();
                if (str != null && !str.isEmpty()) {
                    return JdbcFieldTypeUtils.getOffsetDateTime(rs, columnIndex);
                }
            } catch (Exception e) {
                log.debug("Failed to parse Oracle TIMESTAMPTZ from string representation", e);
            }
        }

        // Fall back to the enhanced JdbcFieldTypeUtils method
        return JdbcFieldTypeUtils.getOffsetDateTime(rs, columnIndex);
    }
}
