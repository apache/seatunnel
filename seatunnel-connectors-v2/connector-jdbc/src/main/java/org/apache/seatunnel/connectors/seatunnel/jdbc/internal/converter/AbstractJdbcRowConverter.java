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
import java.util.Objects;
import java.util.Optional;

/**
 * Base class for all converters that convert between JDBC object and Seatunnel internal object.
 */
public abstract class AbstractJdbcRowConverter implements JdbcRowConverter {

    public abstract String converterName();

    public AbstractJdbcRowConverter() {
    }

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
                    fields[fieldIndex] = Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    Time sqlTime = rs.getTime(resultSetIndex);
                    fields[fieldIndex] = Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = rs.getTimestamp(resultSetIndex);
                    fields[fieldIndex] = Optional.ofNullable(sqlTimestamp).map(e -> e.toLocalDateTime()).orElse(null);
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
                    throw new UnsupportedOperationException("Unsupported type: " + seaTunnelDataType);
            }
        }
        return new SeaTunnelRow(fields);
    }

    @Override
    public PreparedStatement toExternal(SeaTunnelRowType rowType, SeaTunnelRow row, PreparedStatement statement) throws SQLException {
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = rowType.getFieldType(fieldIndex);
            int statementIndex = fieldIndex + 1;
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
//                    statement.setString(statementIndex, (String) row.getField(fieldIndex));
                    String strValue = (String) row.getField(fieldIndex);
                    if(Objects.nonNull(strValue)) {
                        statement.setString(statementIndex, strValue);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case BOOLEAN:
                    Boolean boolValue = (Boolean)row.getField(fieldIndex);
                    if(Objects.nonNull(boolValue)){
                        statement.setBoolean(statementIndex, boolValue);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
//                    statement.setBoolean(statementIndex, (Boolean) row.getField(fieldIndex));
                    break;
                case TINYINT:
//                    statement.setByte(statementIndex, (Byte) row.getField(fieldIndex));
                    Byte byteValue = (Byte)row.getField(fieldIndex);
                    if(Objects.nonNull(byteValue)){
                        statement.setByte(statementIndex, byteValue);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case SMALLINT:
//                    statement.setShort(statementIndex, (Short) row.getField(fieldIndex));
                    Short shortValue = (Short)row.getField(fieldIndex);
                    if(Objects.nonNull(shortValue)){
                        statement.setShort(statementIndex, shortValue);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case INT:
//                    statement.setInt(statementIndex, (Integer) row.getField(fieldIndex));
                    Integer intValue = (Integer)row.getField(fieldIndex);
                    if(Objects.nonNull(intValue)){
                        statement.setInt(statementIndex, intValue);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case BIGINT:
                    Long longValue = (Long) row.getField(fieldIndex);
                    if(Objects.nonNull(longValue)) {
                        statement.setLong(statementIndex, longValue);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case FLOAT:
//                    statement.setFloat(statementIndex, (Float) row.getField(fieldIndex));
                    Float floatValue = (Float)row.getField(fieldIndex);
                    if(Objects.nonNull(floatValue)){
                        statement.setFloat(statementIndex, floatValue);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case DOUBLE:
//                    statement.setDouble(statementIndex, (Double) row.getField(fieldIndex));
                    Double doubleValue = (Double)row.getField(fieldIndex);
                    if(Objects.nonNull(doubleValue)){
                        statement.setDouble(statementIndex, doubleValue);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case DECIMAL:
//                    statement.setBigDecimal(statementIndex, (BigDecimal) row.getField(fieldIndex));
                    BigDecimal decimalValue = (BigDecimal)row.getField(fieldIndex);
                    if(Objects.nonNull(decimalValue)){
                        statement.setBigDecimal(statementIndex, decimalValue);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case DATE:
                    LocalDate localDate = (LocalDate) row.getField(fieldIndex);
                    java.sql.Date date = Optional.ofNullable(localDate)
                            .map(e -> java.sql.Date.valueOf(e))
                            .orElse(null);
//                    statement.setDate(statementIndex, date);
                    if(Objects.nonNull(date)){
                        statement.setDate(statementIndex, date);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case TIME:
                    LocalTime localTime = (LocalTime) row.getField(fieldIndex);
                    java.sql.Time time = Optional.ofNullable(localTime)
                            .map(e -> java.sql.Time.valueOf(e))
                            .orElse(null);
//                    statement.setTime(statementIndex, time);
                    if(Objects.nonNull(time)){
                        statement.setTime(statementIndex, time);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case TIMESTAMP:
                    LocalDateTime localDateTime = (LocalDateTime) row.getField(fieldIndex);
                    java.sql.Timestamp timestamp = Optional.ofNullable(localDateTime)
                            .map(e -> java.sql.Timestamp.valueOf(e))
                            .orElse(null);
//                    statement.setTimestamp(statementIndex, timestamp);
                    if(Objects.nonNull(timestamp)){
                        statement.setTimestamp(statementIndex, timestamp);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case BYTES:
//                    statement.setBytes(statementIndex, (byte[]) row.getField(fieldIndex));
                    byte[] bytes = (byte[])row.getField(fieldIndex);
                    if(Objects.nonNull(bytes)){
                        statement.setBytes(statementIndex, bytes);
                    }else{
                        statement.setNull(statementIndex, java.sql.Types.NULL);
                    }
                    break;
                case NULL:
                    statement.setNull(statementIndex, java.sql.Types.NULL);
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + seaTunnelDataType);
            }
        }
        return statement;
    }
}
