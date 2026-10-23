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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import com.yashandb.vector.Vector;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Optional;

public class YashanDbJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.YASHANDB;
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
                case TIMESTAMP_TZ:
                    OffsetDateTime offsetDateTime =
                            JdbcFieldTypeUtils.getOffsetDateTime(rs, resultSetIndex);
                    fields[fieldIndex] = offsetDateTime;
                    break;
                case FLOAT16_VECTOR:
                case BFLOAT16_VECTOR:
                case FLOAT_VECTOR:
                    fields[fieldIndex] = convertToVectorBuffer(rs.getObject(resultSetIndex));
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
    protected void setNullToStatementByDataType(
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        SqlType sqlType = seaTunnelDataType.getSqlType();
        if (SqlType.ARRAY.equals(sqlType)) {
            statement.setNull(statementIndex, java.sql.Types.ARRAY);
            return;
        }
        if (isFloatVectorType(sqlType)) {
            statement.setNull(statementIndex, java.sql.Types.VARCHAR);
            return;
        }
        super.setNullToStatementByDataType(
                statement, seaTunnelDataType, statementIndex, sourceType);
    }

    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        SqlType sqlType = seaTunnelDataType.getSqlType();
        if (sqlType.equals(SqlType.ARRAY)) {
            if (value == null) {
                statement.setNull(statementIndex, java.sql.Types.ARRAY);
                return;
            }
            Object[] array = (Object[]) value;
            setArray(statement, (ArrayType<?, ?>) seaTunnelDataType, statementIndex, array);
            return;
        }
        if (isFloatVectorType(sqlType)) {
            if (value == null) {
                statement.setNull(statementIndex, java.sql.Types.VARCHAR);
            } else if (value instanceof ByteBuffer) {
                statement.setObject(
                        statementIndex,
                        Vector.ofFloat32Values(toFloat32VectorValues(value, sqlType)));
            } else if (value instanceof Object[]) {
                statement.setObject(
                        statementIndex,
                        Vector.ofFloat32Values(toFloat32VectorValues(value, sqlType)));
            } else {
                throw new SQLException(
                        "Unsupported YashanDB vector value type: " + value.getClass().getName());
            }
            return;
        }
        super.setValueToStatementByDataType(
                value, statement, seaTunnelDataType, statementIndex, sourceType);
    }

    private static boolean isFloatVectorType(SqlType sqlType) {
        return SqlType.FLOAT_VECTOR.equals(sqlType)
                || SqlType.FLOAT16_VECTOR.equals(sqlType)
                || SqlType.BFLOAT16_VECTOR.equals(sqlType);
    }

    private static ByteBuffer convertToVectorBuffer(Object vectorValue) {
        if (vectorValue == null) {
            return null;
        }
        if (vectorValue instanceof Vector) {
            return toByteBuffer(((Vector) vectorValue).toFloatArray());
        }
        String vectorString = vectorValue.toString();
        if (StringUtils.isBlank(vectorString)) {
            return null;
        }
        vectorString = vectorString.replace("[", "").replace("]", "");
        String[] values = vectorString.split(",");
        return toByteBuffer(parseFloatArray(values));
    }

    private static float[] parseFloatArray(String[] values) {
        float[] floatArray = new float[values.length];
        for (int i = 0; i < values.length; i++) {
            floatArray[i] = Float.parseFloat(values[i].trim());
        }
        return floatArray;
    }

    private static ByteBuffer toByteBuffer(float[] floatArray) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(floatArray.length * Float.BYTES);
        for (float value : floatArray) {
            byteBuffer.putFloat(value);
        }
        byteBuffer.flip();
        return byteBuffer;
    }

    private static float[] toPrimitiveFloatArray(Object value) {
        if (value instanceof ByteBuffer) {
            ByteBuffer byteBuffer = ((ByteBuffer) value).duplicate();
            byteBuffer.rewind();
            float[] floatArray = new float[byteBuffer.remaining() / Float.BYTES];
            for (int i = 0; i < floatArray.length; i++) {
                floatArray[i] = byteBuffer.getFloat();
            }
            return floatArray;
        }

        Object[] objectArray = (Object[]) value;
        float[] floatArray = new float[objectArray.length];
        for (int i = 0; i < objectArray.length; i++) {
            floatArray[i] = ((Number) objectArray[i]).floatValue();
        }
        return floatArray;
    }

    private static float[] toFloat32VectorValues(Object value, SqlType sqlType)
            throws SQLException {
        // YashanDB Vector writes use float32 values for SeaTunnel vector inputs. FLOAT16 and
        // BFLOAT16 buffers keep their original 16-bit precision; this path widens them to
        // float32 only at the JDBC boundary so the element count is preserved.
        if (SqlType.FLOAT16_VECTOR.equals(sqlType)) {
            return toFloat16Array(value);
        }
        if (SqlType.BFLOAT16_VECTOR.equals(sqlType)) {
            return toBFloat16Array(value);
        }
        return toPrimitiveFloatArray(value);
    }

    private static float[] toFloat16Array(Object value) throws SQLException {
        short[] shortArray = toPrimitiveShortArray(value);
        float[] floatArray = new float[shortArray.length];
        for (int i = 0; i < shortArray.length; i++) {
            floatArray[i] = float16ToFloat(shortArray[i]);
        }
        return floatArray;
    }

    private static float[] toBFloat16Array(Object value) throws SQLException {
        short[] shortArray = toPrimitiveShortArray(value);
        float[] floatArray = new float[shortArray.length];
        for (int i = 0; i < shortArray.length; i++) {
            floatArray[i] = bfloat16ToFloat(shortArray[i]);
        }
        return floatArray;
    }

    private static short[] toPrimitiveShortArray(Object value) throws SQLException {
        if (value instanceof ByteBuffer) {
            ByteBuffer byteBuffer = ((ByteBuffer) value).duplicate();
            byteBuffer.rewind();
            // SeaTunnel FLOAT16_VECTOR and BFLOAT16_VECTOR ByteBuffers are encoded as one
            // big-endian 2-byte short per vector element via VectorUtils.toByteBuffer(Short[]).
            if (byteBuffer.remaining() % Short.BYTES != 0) {
                throw new SQLException(
                        "Invalid YashanDB 2-byte vector buffer length: " + byteBuffer.remaining());
            }
            short[] shortArray = new short[byteBuffer.remaining() / Short.BYTES];
            for (int i = 0; i < shortArray.length; i++) {
                shortArray[i] = byteBuffer.getShort();
            }
            return shortArray;
        }

        Object[] objectArray = (Object[]) value;
        short[] shortArray = new short[objectArray.length];
        for (int i = 0; i < objectArray.length; i++) {
            shortArray[i] = ((Number) objectArray[i]).shortValue();
        }
        return shortArray;
    }

    private static float float16ToFloat(short value) {
        int half = value & 0xffff;
        int sign = (half & 0x8000) << 16;
        int exponent = (half >>> 10) & 0x1f;
        int mantissa = half & 0x03ff;
        int bits;
        if (exponent == 0) {
            if (mantissa == 0) {
                bits = sign;
            } else {
                while ((mantissa & 0x0400) == 0) {
                    mantissa <<= 1;
                    exponent--;
                }
                exponent++;
                mantissa &= ~0x0400;
                bits = sign | ((exponent + 112) << 23) | (mantissa << 13);
            }
        } else if (exponent == 0x1f) {
            bits = sign | 0x7f800000 | (mantissa << 13);
        } else {
            bits = sign | ((exponent + 112) << 23) | (mantissa << 13);
        }
        return Float.intBitsToFloat(bits);
    }

    private static float bfloat16ToFloat(short value) {
        return Float.intBitsToFloat((value & 0xffff) << 16);
    }

    private static void setArray(
            PreparedStatement statement,
            ArrayType<?, ?> arrayType,
            int statementIndex,
            Object[] array)
            throws SQLException {
        SqlType elementSqlType = arrayType.getElementType().getSqlType();
        if (YashanDbTypeConverter.SUPPORT_FLOAT32_VECTOR.contains(elementSqlType)) {
            statement.setObject(
                    statementIndex, Vector.ofFloat32Values(toPrimitiveFloatArray(array)));
        } else if (YashanDbTypeConverter.SUPPORT_FLOAT64_VECTOR.contains(elementSqlType)) {
            double[] doubleArray = new double[array.length];
            for (int i = 0; i < array.length; i++) {
                doubleArray[i] = ((Number) array[i]).doubleValue();
            }
            statement.setObject(statementIndex, Vector.ofFloat64Values(doubleArray));
        } else {
            String arrayStr = Arrays.toString(array);
            statement.setString(statementIndex, arrayStr);
        }
    }
}
