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

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_BLOB;

public class OracleJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.ORACLE;
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException {
        SeaTunnelRow row = super.toInternal(rs, tableSchema);
        // Handle TIMESTAMP_TZ types for Oracle
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = rowType.getFieldType(fieldIndex);
            if (seaTunnelDataType.getSqlType().equals(SqlType.TIMESTAMP_TZ)) {
                int resultSetIndex = fieldIndex + 1;
                OffsetDateTime offsetDateTime = getOracleOffsetDateTime(rs, resultSetIndex);
                row.setField(fieldIndex, offsetDateTime);
            }
        }
        return row;
    }

    /**
     * Get OffsetDateTime from Oracle TIMESTAMP WITH TIME ZONE column. Oracle stores TIMESTAMP WITH
     * TIME ZONE as a proprietary TIMESTAMPTZ object.
     */
    private OffsetDateTime getOracleOffsetDateTime(ResultSet rs, int columnIndex)
            throws SQLException {
        Object obj = rs.getObject(columnIndex);
        if (obj == null) {
            return null;
        }

        // Handle OffsetDateTime directly
        if (obj instanceof OffsetDateTime) {
            return (OffsetDateTime) obj;
        }

        // Handle other time types
        if (obj instanceof java.time.ZonedDateTime) {
            return ((java.time.ZonedDateTime) obj).toOffsetDateTime();
        }

        if (obj instanceof java.time.Instant) {
            return ((java.time.Instant) obj).atOffset(java.time.ZoneOffset.UTC);
        }

        if (obj instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) obj).toLocalDateTime().atOffset(java.time.ZoneOffset.UTC);
        }

        if (obj instanceof java.util.Date) {
            return ((java.util.Date) obj).toInstant().atOffset(java.time.ZoneOffset.UTC);
        }

        if (obj instanceof Long) {
            return java.time.Instant.ofEpochMilli((Long) obj).atOffset(java.time.ZoneOffset.UTC);
        }

        // Try to parse as string (Oracle TIMESTAMPTZ string representation)
        String str = obj.toString();
        try {
            return JdbcFieldTypeUtils.parseOffsetDateTimeFromString(str);
        } catch (Exception e) {
            // Last resort: try standard OffsetDateTime parsing
            try {
                return OffsetDateTime.parse(str);
            } catch (Exception ex) {
                throw new SQLException(
                        "Failed to parse Oracle TIMESTAMP WITH TIME ZONE value: " + str, ex);
            }
        }
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
        } else {
            super.setValueToStatementByDataType(
                    value, statement, seaTunnelDataType, statementIndex, sourceType);
        }
    }
}
