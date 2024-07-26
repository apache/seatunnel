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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.converter;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.cdc.debezium.utils.TemporalConversions;

import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.DataType;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Slf4j
public class DefaultDataConverter implements DataConverter<Object[]> {

    @Override
    public SeaTunnelRow convert(Object[] values, TiTableInfo tableInfo, SeaTunnelRowType rowType)
            throws Exception {
        Object[] fields = new Object[rowType.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = rowType.getFieldType(fieldIndex);
            String fieldName = rowType.getFieldName(fieldIndex);
            TiColumnInfo columnInfo = tableInfo.getColumn(fieldName);
            if (columnInfo == null) {
                fields[fieldIndex] = null;
            }
            DataType dataType = columnInfo.getType();
            Object value = values[columnInfo.getOffset()];
            if (value == null) {
                fields[fieldIndex] = null;
                continue;
            }
            if (dataType.isUnsigned()) {
                value = rewriteUnsignedColumnValue(dataType, value);
            }
            switch (seaTunnelDataType.getSqlType()) {
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case STRING:
                    fields[fieldIndex] = convertToString(value);
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = convertToBoolean(value);
                    break;
                case TINYINT:
                    fields[fieldIndex] = Byte.parseByte(value.toString());
                    break;
                case SMALLINT:
                    fields[fieldIndex] = Short.parseShort(value.toString());
                    break;
                case INT:
                    fields[fieldIndex] = convertToInt(value, dataType);
                    break;
                case BIGINT:
                    fields[fieldIndex] = convertToLong(value);
                    break;
                case FLOAT:
                    fields[fieldIndex] = convertToFloat(value);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = convertToDouble(value);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = createDecimalConverter(value);
                    break;
                case DATE:
                    fields[fieldIndex] = convertToDate(value);
                    break;
                case TIME:
                    fields[fieldIndex] = convertToTime(value);
                    break;
                case TIMESTAMP:
                    fields[fieldIndex] = convertToTimestamp(value, dataType);
                    break;
                case BYTES:
                    fields[fieldIndex] = convertToBinary(value);
                    break;
                case ARRAY:
                    fields[fieldIndex] = convertToArray(value);
                    break;
                case MAP:
                case ROW:
                default:
                    throw CommonError.unsupportedDataType(
                            "SeaTunnel", seaTunnelDataType.getSqlType().toString(), fieldName);
            }
        }
        return new SeaTunnelRow(fields);
    }

    public static Object rewriteUnsignedColumnValue(
            org.tikv.common.types.DataType dataType, Object object) {
        // https://docs.pingcap.com/tidb/stable/data-type-numeric.
        switch (dataType.getType()) {
            case TypeTiny:
                return (short) Byte.toUnsignedInt(((Long) object).byteValue());
            case TypeShort:
                return Short.toUnsignedInt(((Long) object).shortValue());
            case TypeInt24:
                return (((Long) object).intValue()) & 0xffffff;
            case TypeLong:
                return Integer.toUnsignedLong(((Long) object).intValue());
            case TypeLonglong:
                return new BigDecimal(Long.toUnsignedString(((Long) object)));
            default:
                return object;
        }
    }

    private static Object convertToBoolean(Object value) {
        if (value instanceof Boolean) {
            return value;
        } else if (value instanceof Long) {
            return (long) value != 0;
        } else if (value instanceof Byte) {
            return (byte) value != 0;
        } else if (value instanceof byte[]) {
            Long result = bitToLong((byte[]) value, 0, ((byte[]) value).length);
            return result == -1L || result > 0L;
        } else if (value instanceof Short) {
            return (short) value != 0;
        } else if (value instanceof BigDecimal) {
            return ((BigDecimal) value).shortValue() != 0;
        } else {
            return Boolean.parseBoolean(value.toString());
        }
    }

    private static Object convertToInt(Object value, org.tikv.common.types.DataType dataType) {
        if (value instanceof Integer) {
            return value;
        } else if (value instanceof Long) {
            return dataType.isUnsigned()
                    ? Integer.valueOf(Short.toUnsignedInt(((Long) value).shortValue()))
                    : ((Long) value).intValue();
        } else {
            return Integer.parseInt(value.toString());
        }
    }

    private static Object convertToLong(Object value) {
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof Long) {
            return value;
        } else {
            return Long.parseLong(value.toString());
        }
    }

    private static Object convertToDouble(Object value) {
        if (value instanceof Float) {
            return ((Float) value).doubleValue();
        } else if (value instanceof Double) {
            return value;
        } else {
            return Double.parseDouble(value.toString());
        }
    }

    private static Object convertToFloat(Object value) {
        if (value instanceof Float) {
            return value;
        } else if (value instanceof Double) {
            return ((Double) value).floatValue();
        } else {
            return Float.parseFloat(value.toString());
        }
    }

    private static Object createDecimalConverter(Object value) {
        BigDecimal result;
        if (value instanceof String) {
            result = new BigDecimal((String) value);
        } else if (value instanceof Long) {
            result = new BigDecimal(Long.parseLong(value.toString()));
        } else if (value instanceof Double) {
            result = BigDecimal.valueOf(Double.parseDouble(value.toString()));
        } else if (value instanceof BigDecimal) {
            result = (BigDecimal) value;
        } else {
            throw new IllegalArgumentException(
                    "Unable to convert to decimal from unexpected value '"
                            + value
                            + "' of type "
                            + value.getClass());
        }
        return result;
    }

    public Object[] convertToArray(Object value) throws SQLException {
        String[] array = ((String) value).split(",");
        if (array == null) {
            return null;
        }
        return array;
    }

    private static Object convertToBinary(Object value) {
        if (value instanceof byte[]) {
            return value;
        } else if (value instanceof String) {
            return ((String) value).getBytes();
        } else if (value instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) value;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported BYTES value type: " + value.getClass().getSimpleName());
        }
    }

    private static Object convertToString(Object value) {
        if (value instanceof byte[]) {
            return new String((byte[]) value);
        }
        return value;
    }

    private static Object convertToDate(Object value) {
        return TemporalConversions.toLocalDate(value);
    }

    private static Object convertToTime(Object value) {
        if (value instanceof Long) {
            return LocalTime.ofNanoOfDay((Long) value);
        }
        return TemporalConversions.toLocalTime(value);
    }

    private static Object convertToTimestamp(
            Object value, org.tikv.common.types.DataType dataType) {
        switch (dataType.getType()) {
            case TypeTimestamp:
                if (value instanceof Timestamp) {
                    Instant instant = ((Timestamp) value).toInstant();
                    long epochSecond = instant.getEpochSecond();
                    int nanoSecond = instant.getNano();
                    long millisecond = epochSecond * 1000L + (long) (nanoSecond / 1000000);
                    int nanoOfMillisecond = nanoSecond % 1000000;
                    return toLocalDateTime(millisecond, nanoOfMillisecond);
                }
                break;
            case TypeDatetime:
                if (value instanceof Timestamp) {
                    LocalDateTime dateTime = ((Timestamp) value).toLocalDateTime();
                    long epochDay = dateTime.toLocalDate().toEpochDay();
                    long nanoOfDay = dateTime.toLocalTime().toNanoOfDay();
                    long millisecond = epochDay * 86400000L + nanoOfDay / 1000000L;
                    int nanoOfMillisecond = (int) (nanoOfDay % 1000000L);

                    return toLocalDateTime(millisecond, nanoOfMillisecond);
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "Unable to convert to LocalDateTime from unexpected value '"
                                + value
                                + "' of type "
                                + value.getClass().getName());
        }
        return value;
    }

    public static LocalDateTime toLocalDateTime(long millisecond, int nanoOfMillisecond) {
        // 86400000 = 24 * 60 * 60 * 1000
        int date = (int) (millisecond / 86400000);
        int time = (int) (millisecond % 86400000);
        if (time < 0) {
            --date;
            time += 86400000;
        }
        long nanoOfDay = time * 1_000_000L + nanoOfMillisecond;
        LocalDate localDate = LocalDate.ofEpochDay(date);
        LocalTime localTime = LocalTime.ofNanoOfDay(nanoOfDay);
        return LocalDateTime.of(localDate, localTime);
    }

    public static long bitToLong(byte[] bytes, int offset, int length) {
        long valueAsLong = 0;
        for (int i = 0; i < length; i++) {
            valueAsLong = valueAsLong << 8 | bytes[offset + i] & 0xff;
        }
        return valueAsLong;
    }
}
