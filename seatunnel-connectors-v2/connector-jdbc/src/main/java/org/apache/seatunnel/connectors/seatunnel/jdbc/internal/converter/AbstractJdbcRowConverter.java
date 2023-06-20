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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

/** Base class for all converters that convert between JDBC object and SeaTunnel internal object. */
public abstract class AbstractJdbcRowConverter implements JdbcRowConverter {

    public abstract String converterName();

    public AbstractJdbcRowConverter() {}

    @Override
    @SuppressWarnings("checkstyle:Indentation")
    public SeaTunnelRow toInternal(ResultSet rs, SeaTunnelRowType typeInfo) throws SQLException {
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    fields[fieldIndex] = rs.getString(resultSetIndex);
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = rs.getBoolean(resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = rs.getByte(resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = rs.getShort(resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = rs.getInt(resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = rs.getLong(resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = rs.getFloat(resultSetIndex);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = rs.getDouble(resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = rs.getBigDecimal(resultSetIndex);
                    break;
                case DATE:
                    Date sqlDate = rs.getDate(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    Time sqlTime = rs.getTime(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = rs.getTimestamp(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = rs.getBytes(resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return new SeaTunnelRow(fields);
    }

    @Override
    public PreparedStatement toExternal(
            SeaTunnelRowType rowType, SeaTunnelRow row, PreparedStatement statement)
            throws SQLException {
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = rowType.getFieldType(fieldIndex);
            int statementIndex = fieldIndex + 1;
            Object fieldValue = row.getField(fieldIndex);
            if (fieldValue == null) {
                statement.setObject(statementIndex, null);
                continue;
            }

            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    statement.setString(statementIndex, (String) row.getField(fieldIndex));
                    break;
                case BOOLEAN:
                    statement.setBoolean(statementIndex, (Boolean) row.getField(fieldIndex));
                    break;
                case TINYINT:
                    statement.setByte(statementIndex, (Byte) row.getField(fieldIndex));
                    break;
                case SMALLINT:
                    statement.setShort(statementIndex, (Short) row.getField(fieldIndex));
                    break;
                case INT:
                    statement.setInt(statementIndex, (Integer) row.getField(fieldIndex));
                    break;
                case BIGINT:
                    statement.setLong(statementIndex, (Long) row.getField(fieldIndex));
                    break;
                case FLOAT:
                    statement.setFloat(statementIndex, (Float) row.getField(fieldIndex));
                    break;
                case DOUBLE:
                    statement.setDouble(statementIndex, (Double) row.getField(fieldIndex));
                    break;
                case DECIMAL:
                    statement.setBigDecimal(statementIndex, (BigDecimal) row.getField(fieldIndex));
                    break;
                case DATE:
                    LocalDate localDate = (LocalDate) row.getField(fieldIndex);
                    statement.setDate(statementIndex, java.sql.Date.valueOf(localDate));
                    break;
                case TIME:
                    LocalTime localTime = (LocalTime) row.getField(fieldIndex);
                    statement.setTime(statementIndex, java.sql.Time.valueOf(localTime));
                    break;
                case TIMESTAMP:
                    LocalDateTime localDateTime = (LocalDateTime) row.getField(fieldIndex);
                    statement.setTimestamp(
                            statementIndex, java.sql.Timestamp.valueOf(localDateTime));
                    break;
                case BYTES:
                    statement.setBytes(statementIndex, (byte[]) row.getField(fieldIndex));
                    break;
                case NULL:
                    statement.setNull(statementIndex, java.sql.Types.NULL);
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return statement;
    }
}
