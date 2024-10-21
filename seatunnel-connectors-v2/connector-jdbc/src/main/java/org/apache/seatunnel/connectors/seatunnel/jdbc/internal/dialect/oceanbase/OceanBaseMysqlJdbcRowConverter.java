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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

public class OceanBaseMysqlJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.OCENABASE;
    }

    @Override
    protected void writeTime(PreparedStatement statement, int index, LocalTime time)
            throws SQLException {
        // Write to time column using timestamp retains milliseconds
        statement.setTimestamp(
                index, java.sql.Timestamp.valueOf(LocalDateTime.of(LocalDate.now(), time)));
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            String fieldName = typeInfo.getFieldName(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getString(rs, resultSetIndex);
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBoolean(rs, resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getByte(rs, resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getShort(rs, resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getInt(rs, resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getLong(rs, resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getFloat(rs, resultSetIndex);
                    break;
                case FLOAT_VECTOR:
                    String result = JdbcFieldTypeUtils.getString(rs, resultSetIndex);
                    if (StringUtils.isNotBlank(result)) {
                        result = result.replace("[", "").replace("]", "");
                        String[] stringArray = result.split(",");
                        Float[] arrays = new Float[stringArray.length];
                        for (int i = 0; i < stringArray.length; i++) {
                            arrays[i] = Float.parseFloat(stringArray[i]);
                        }
                        fields[fieldIndex] = BufferUtils.toByteBuffer(arrays);
                    }
                    break;
                case DOUBLE:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getDouble(rs, resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBigDecimal(rs, resultSetIndex);
                    break;
                case DATE:
                    Date sqlDate = JdbcFieldTypeUtils.getDate(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    fields[fieldIndex] = readTime(rs, resultSetIndex);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = JdbcFieldTypeUtils.getTimestamp(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBytes(rs, resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case ARRAY:
                    fields[fieldIndex] =
                            convertToArray(rs, resultSetIndex, seaTunnelDataType, fieldName);
                    break;
                case MAP:
                case ROW:
                default:
                    throw CommonError.unsupportedDataType(
                            converterName(), seaTunnelDataType.getSqlType().toString(), fieldName);
            }
        }
        return new SeaTunnelRow(fields);
    }

    @Override
    public PreparedStatement toExternal(
            TableSchema tableSchema, SeaTunnelRow row, PreparedStatement statement)
            throws SQLException {
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            try {
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
                    case FLOAT_VECTOR:
                        if (row.getField(fieldIndex) instanceof ByteBuffer) {
                            ByteBuffer byteBuffer = (ByteBuffer) row.getField(fieldIndex);
                            // Convert ByteBuffer to Float[]
                            Float[] floatArray = BufferUtils.toFloatArray(byteBuffer);
                            StringBuilder vector = new StringBuilder();
                            vector.append("[");
                            for (Float aFloat : floatArray) {
                                vector.append(aFloat).append(", ");
                            }
                            if (vector.length() > 0) {
                                vector.setLength(vector.length() - 2);
                            }
                            vector.append("]");
                            statement.setString(statementIndex, vector.toString());
                        }
                        break;
                    case DOUBLE:
                        statement.setDouble(statementIndex, (Double) row.getField(fieldIndex));
                        break;
                    case DECIMAL:
                        statement.setBigDecimal(
                                statementIndex, (BigDecimal) row.getField(fieldIndex));
                        break;
                    case DATE:
                        LocalDate localDate = (LocalDate) row.getField(fieldIndex);
                        statement.setDate(statementIndex, java.sql.Date.valueOf(localDate));
                        break;
                    case TIME:
                        writeTime(statement, statementIndex, (LocalTime) row.getField(fieldIndex));
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
                    case ARRAY:
                        SeaTunnelDataType elementType =
                                ((ArrayType) seaTunnelDataType).getElementType();
                        Object[] array = (Object[]) row.getField(fieldIndex);
                        if (array == null) {
                            statement.setNull(statementIndex, java.sql.Types.ARRAY);
                            break;
                        }
                        if (SqlType.TINYINT.equals(elementType.getSqlType())) {
                            Short[] shortArray = new Short[array.length];
                            for (int i = 0; i < array.length; i++) {
                                shortArray[i] = Short.valueOf(array[i].toString());
                            }
                            statement.setObject(statementIndex, shortArray);
                        } else {
                            statement.setObject(statementIndex, array);
                        }
                        break;
                    case MAP:
                    case ROW:
                    default:
                        throw new JdbcConnectorException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                                "Unexpected value: " + seaTunnelDataType);
                }
            } catch (Exception e) {
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.DATA_TYPE_CAST_FAILED,
                        "error field:" + rowType.getFieldNames()[fieldIndex],
                        e);
            }
        }
        return statement;
    }
}
