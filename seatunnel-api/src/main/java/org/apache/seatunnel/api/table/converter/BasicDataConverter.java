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

package org.apache.seatunnel.api.table.converter;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface BasicDataConverter<T> extends DataConverter<T> {

    @Override
    default Object convert(SeaTunnelDataType typeDefine, Object value) {
        if (value == null) {
            return null;
        }
        switch (typeDefine.getSqlType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return convertBoolean(value);
            case TINYINT:
                return convertByte(value);
            case SMALLINT:
                return convertShort(value);
            case INT:
                return convertInt(value);
            case BIGINT:
                return convertLong(value);
            case FLOAT:
                return convertFloat(value);
            case DOUBLE:
                return convertDouble(value);
            case DECIMAL:
                return convertDecimal(value);
            case DATE:
                return convertLocalDate(value);
            case TIME:
                return convertTime(value);
            case TIMESTAMP:
                return convertLocalDateTime(value);
            case TIMESTAMP_TZ:
                return convertOffsetDateTime(value);
            case BYTES:
                return convertBytes(value);
            case STRING:
                return convertString(value);
            case ROW:
                return convertRow((SeaTunnelRowType) typeDefine, value);
            case ARRAY:
                return convertArray((ArrayType) typeDefine, value);
            case MAP:
                return convertMap((MapType) typeDefine, value);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported convert "
                                + value.getClass()
                                + " to "
                                + typeDefine.getSqlType());
        }
    }

    @Override
    default Object convert(T typeDefine, Column columnDefine, Object value) {
        if (value == null) {
            return null;
        }
        switch (columnDefine.getDataType().getSqlType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return convertBoolean(typeDefine, value);
            case TINYINT:
                return convertByte(typeDefine, value);
            case SMALLINT:
                return convertShort(typeDefine, value);
            case INT:
                return convertInt(typeDefine, value);
            case BIGINT:
                return convertLong(typeDefine, value);
            case FLOAT:
                return convertFloat(typeDefine, value);
            case DOUBLE:
                return convertDouble(typeDefine, value);
            case DECIMAL:
                return convertDecimal(typeDefine, value);
            case DATE:
                return convertLocalDate(typeDefine, value);
            case TIME:
                return convertTime(typeDefine, value);
            case TIMESTAMP:
                return convertLocalDateTime(typeDefine, value);
            case TIMESTAMP_TZ:
                return convertOffsetDateTime(typeDefine, value);
            case BYTES:
                return convertBytes(typeDefine, value);
            case STRING:
                return convertString(typeDefine, value);
            case ROW:
                return convertRow(typeDefine, columnDefine, value);
            case ARRAY:
                return convertArray(typeDefine, columnDefine, value);
            case MAP:
                return convertMap(typeDefine, columnDefine, value);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported convert "
                                + value.getClass()
                                + " to "
                                + columnDefine.getDataType().getSqlType());
        }
    }

    default Map convertMap(T typeDefine, Column columnDefine, Object value)
            throws UnsupportedOperationException {
        return convertMap((MapType) columnDefine.getDataType(), value);
    }

    default Map convertMap(MapType typeDefine, Object value) throws UnsupportedOperationException {
        if (value instanceof Map) {
            return (Map) value;
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Map, typeDefine: " + typeDefine);
    }

    default Object[] convertArray(T typeDefine, Column columnDefine, Object value)
            throws UnsupportedOperationException {
        return convertArray((ArrayType) columnDefine.getDataType(), value);
    }

    default Object[] convertArray(ArrayType typeDefine, Object value)
            throws UnsupportedOperationException {
        if (value.getClass().isArray()) {
            SeaTunnelDataType elementType = typeDefine.getElementType();

            Object[] array = (Object[]) value;
            for (int i = 0; i < array.length; i++) {
                array[i] = convert(elementType, array[i]);
            }
            return array;
        }
        if (value instanceof List) {
            SeaTunnelDataType elementType = typeDefine.getElementType();

            List<Object> list = (List<Object>) value;
            int elements = list.size();
            for (int i = 0; i < elements; i++) {
                list.set(i, convert(elementType, list.get(i)));
            }
            return list.toArray();
        }
        if (value instanceof Set) {
            SeaTunnelDataType elementType = typeDefine.getElementType();

            return ((Set) value).stream().map(e -> convert(elementType, e)).toArray();
        }

        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Array, typeDefine: " + typeDefine);
    }

    default SeaTunnelRow convertRow(T typeDefine, Column columnDefine, Object value)
            throws UnsupportedOperationException {
        return convertRow((SeaTunnelRowType) columnDefine.getDataType(), value);
    }

    default SeaTunnelRow convertRow(SeaTunnelRowType typeDefine, Object value)
            throws UnsupportedOperationException {
        if (value instanceof SeaTunnelRow) {
            return (SeaTunnelRow) value;
        }
        if (value instanceof Collection) {
            Collection collection = (Collection) value;
            if (collection.size() != typeDefine.getTotalFields()) {
                throw new IllegalArgumentException(
                        "The size of collection is not equal to the size of row type");
            }

            Object[] array = new Object[collection.size()];
            int i = 0;
            for (Iterator iterator = collection.iterator(); iterator.hasNext(); i++) {
                Object object = iterator.next();
                SeaTunnelDataType<?> type = typeDefine.getFieldType(i);
                array[i] = convert(type, object);
            }
            return new SeaTunnelRow(array);
        }
        if (value instanceof Map) {
            Map map = (Map) value;

            Object[] array = new Object[typeDefine.getTotalFields()];
            for (int i = 0; i < typeDefine.getTotalFields(); i++) {
                String key = typeDefine.getFieldName(i);
                SeaTunnelDataType<?> type = typeDefine.getFieldType(i);
                Object object = map.get(key);
                array[i] = convert(type, object);
            }
            return new SeaTunnelRow(array);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Row, typeDefine: " + typeDefine);
    }

    default String convertString(T typeDefine, Object value) throws UnsupportedOperationException {
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof Number) {
            return convertString(typeDefine, (Number) value);
        }
        if (value instanceof byte[]) {
            return convertString(typeDefine, (byte[]) value);
        }
        if (value instanceof Boolean) {
            return convertString(typeDefine, (boolean) value);
        }
        if (value instanceof Date) {
            return convertString(typeDefine, (Date) value);
        }
        if (value instanceof LocalDate) {
            return convertString(typeDefine, (LocalDate) value);
        }
        if (value instanceof LocalTime) {
            return convertString(typeDefine, (LocalTime) value);
        }
        if (value instanceof LocalDateTime) {
            return convertString(typeDefine, (LocalDateTime) value);
        }
        return value.toString();
    }

    default String convertString(T typeDefine, Number value) {
        return convertString(value);
    }

    default String convertString(T typeDefine, byte[] value) {
        return convertString(value);
    }

    default String convertString(T typeDefine, boolean value) {
        return convertString(value);
    }

    default String convertString(T typeDefine, Date value) {
        return convertString(value);
    }

    default String convertString(T typeDefine, LocalDate value) {
        return convertString(value);
    }

    default String convertString(T typeDefine, Time value) {
        return convertString(value);
    }

    default String convertString(T typeDefine, LocalTime value) {
        return convertString(value);
    }

    default String convertString(T typeDefine, LocalDateTime value) {
        return convertString(value);
    }

    default String convertString(Object value) throws UnsupportedOperationException {
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof Number) {
            return convertString((Number) value);
        }
        if (value instanceof byte[]) {
            return convertString((byte[]) value);
        }
        if (value instanceof Boolean) {
            return convertString((boolean) value);
        }
        if (value instanceof Date) {
            return convertString((Date) value);
        }
        if (value instanceof LocalDate) {
            return convertString((LocalDate) value);
        }
        if (value instanceof LocalTime) {
            return convertString((LocalTime) value);
        }
        if (value instanceof LocalDateTime) {
            return convertString((LocalDateTime) value);
        }
        return value.toString();
    }

    default String convertString(Number value) {
        return String.valueOf(value);
    }

    default String convertString(byte[] value) {
        return new String(value);
    }

    default String convertString(boolean value) {
        return value ? "true" : "false";
    }

    default String convertString(Date value) {
        return value.toString();
    }

    default String convertString(LocalDate value) {
        return value.toString();
    }

    default String convertString(Time value) {
        return value.toString();
    }

    default String convertString(LocalTime value) {
        return value.toString();
    }

    default String convertString(LocalDateTime value) {
        return value.toString();
    }

    default byte[] convertBytes(T typeDefine, Object value) throws UnsupportedOperationException {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof ByteBuffer) {
            return convertBytes((ByteBuffer) value);
        }
        if (value instanceof String) {
            return convertBytes(typeDefine, (String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert "
                        + value.getClass()
                        + " to byte[], typeDefine: "
                        + typeDefine);
    }

    default byte[] convertBytes(T typeDefine, String value) {
        return convertBytes(value);
    }

    default byte[] convertBytes(Object value) throws UnsupportedOperationException {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof ByteBuffer) {
            return convertBytes((ByteBuffer) value);
        }
        if (value instanceof String) {
            return convertBytes((String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to byte[]");
    }

    default byte[] convertBytes(ByteBuffer value) {
        byte[] bytes = new byte[value.remaining()];
        value.get(bytes);
        return bytes;
    }

    default byte[] convertBytes(String value) {
        return value.getBytes();
    }

    default LocalDateTime convertLocalDateTime(T typeDefine, Object value)
            throws UnsupportedOperationException {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toLocalDateTime();
        }
        if (value instanceof Instant) {
            return convertLocalDateTime(typeDefine, (Instant) value);
        }
        if (value instanceof Date) {
            return convertLocalDateTime(typeDefine, (Date) value);
        }
        if (value instanceof LocalDate) {
            return convertLocalDateTime((LocalDate) value);
        }
        if (value instanceof java.sql.Date) {
            return convertLocalDateTime((java.sql.Date) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return convertLocalDateTime((java.sql.Timestamp) value);
        }
        if (value instanceof String) {
            return convertLocalDateTime(typeDefine, (String) value);
        }
        if (value instanceof Number) {
            return convertLocalDateTime(typeDefine, (Number) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert "
                        + value.getClass()
                        + " to LocalDateTime, typeDefine: "
                        + typeDefine);
    }

    default OffsetDateTime convertOffsetDateTime(T typeDefine, Object value)
            throws UnsupportedOperationException {
        if (value instanceof OffsetDateTime) {
            return (OffsetDateTime) value;
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atZone(ZoneId.systemDefault()).toOffsetDateTime();
        }
        if (value instanceof Instant) {
            return ((Instant) value).atZone(ZoneId.systemDefault()).toOffsetDateTime();
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value)
                    .toLocalDate()
                    .atTime(LocalTime.MIDNIGHT)
                    .atZone(ZoneId.systemDefault())
                    .toOffsetDateTime();
        }
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value)
                    .toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toOffsetDateTime();
        }
        if (value instanceof Date) {
            return ((Date) value).toInstant().atZone(ZoneId.systemDefault()).toOffsetDateTime();
        }
        if (value instanceof LocalDate) {
            return ((LocalDate) value)
                    .atTime(LocalTime.MIDNIGHT)
                    .atZone(ZoneId.systemDefault())
                    .toOffsetDateTime();
        }
        if (value instanceof String) {
            return OffsetDateTime.parse((String) value);
        }

        throw new UnsupportedOperationException(
                "Unsupported convert "
                        + value.getClass()
                        + " to OffsetDateTime, typeDefine: "
                        + typeDefine);
    }

    default LocalDateTime convertLocalDateTime(T typeDefine, Instant value) {
        return convertLocalDateTime(value);
    }

    default LocalDateTime convertLocalDateTime(T typeDefine, Date value) {
        return convertLocalDateTime(value);
    }

    default LocalDateTime convertLocalDateTime(T typeDefine, String value) {
        return convertLocalDateTime(value);
    }

    default LocalDateTime convertLocalDateTime(T typeDefine, Number value) {
        return convertLocalDateTime(value);
    }

    default LocalDateTime convertLocalDateTime(Object value) throws UnsupportedOperationException {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toLocalDateTime();
        }
        if (value instanceof Instant) {
            return convertLocalDateTime((Instant) value);
        }
        if (value instanceof Date) {
            return convertLocalDateTime((Date) value);
        }
        if (value instanceof LocalDate) {
            return convertLocalDateTime((LocalDate) value);
        }
        if (value instanceof java.sql.Date) {
            return convertLocalDateTime((java.sql.Date) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return convertLocalDateTime((java.sql.Timestamp) value);
        }
        if (value instanceof String) {
            return convertLocalDateTime((String) value);
        }
        if (value instanceof Number) {
            return convertLocalDateTime((Number) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to LocalDateTime");
    }

    default OffsetDateTime convertOffsetDateTime(Object value)
            throws UnsupportedOperationException {
        if (value instanceof OffsetDateTime) {
            return (OffsetDateTime) value;
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atZone(ZoneId.systemDefault()).toOffsetDateTime();
        }
        if (value instanceof Instant) {
            return ((Instant) value).atZone(ZoneId.systemDefault()).toOffsetDateTime();
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value)
                    .toLocalDate()
                    .atTime(LocalTime.MIDNIGHT)
                    .atZone(ZoneId.systemDefault())
                    .toOffsetDateTime();
        }
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value)
                    .toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toOffsetDateTime();
        }
        if (value instanceof Date) {
            return ((Date) value).toInstant().atZone(ZoneId.systemDefault()).toOffsetDateTime();
        }
        if (value instanceof LocalDate) {
            return ((LocalDate) value)
                    .atTime(LocalTime.MIDNIGHT)
                    .atZone(ZoneId.systemDefault())
                    .toOffsetDateTime();
        }
        if (value instanceof String) {
            return OffsetDateTime.parse((String) value);
        }

        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to LocalDateTime");
    }

    default LocalDateTime convertLocalDateTime(Instant value) {
        return value.atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    default LocalDateTime convertLocalDateTime(Date value) {
        return value.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    default LocalDateTime convertLocalDateTime(LocalDate value) {
        return LocalDateTime.of(value, LocalTime.MIDNIGHT);
    }

    default LocalDateTime convertLocalDateTime(java.sql.Date value) {
        LocalDate date = value.toLocalDate();
        return LocalDateTime.of(date, LocalTime.MIDNIGHT);
    }

    default LocalDateTime convertLocalDateTime(java.sql.Timestamp value) {
        return LocalDateTime.of(
                value.getYear() + 1900,
                value.getMonth() + 1,
                value.getDate(),
                value.getHours(),
                value.getMinutes(),
                value.getSeconds(),
                value.getNanos());
    }

    default LocalDateTime convertLocalDateTime(String value) {
        return LocalDateTime.parse(value);
    }

    default LocalDateTime convertLocalDateTime(Number value) {
        if (value.longValue() < 999999999) {
            return LocalDateTime.ofEpochSecond(
                    value.longValue(),
                    0,
                    ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now()));
        }
        return new Date(value.longValue())
                .toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
    }

    default LocalTime convertTime(T typeDefine, Object value) throws UnsupportedOperationException {
        if (value instanceof LocalTime) {
            return (LocalTime) value;
        }
        if (value instanceof Date) {
            return convertLocalTime((Date) value);
        }
        if (value instanceof Time) {
            return convertLocalTime(typeDefine, (Time) value);
        }
        if (value instanceof LocalDateTime) {
            return convertLocalTime((LocalDateTime) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return convertLocalTime((java.sql.Timestamp) value);
        }
        if (value instanceof String) {
            return convertLocalTime(typeDefine, (String) value);
        }
        if (value instanceof Number) {
            return convertLocalTime(typeDefine, (Number) value);
        }
        if (value instanceof Duration) {
            return convertLocalTime((Duration) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert "
                        + value.getClass()
                        + " to LocalTime, typeDefine: "
                        + typeDefine);
    }

    default LocalTime convertLocalTime(T typeDefine, Time value) {
        return convertLocalTime(value);
    }

    default LocalTime convertLocalTime(T typeDefine, String value) {
        return convertLocalTime(value);
    }

    default LocalTime convertLocalTime(T typeDefine, Number value) {
        return convertLocalTime(value);
    }

    default LocalTime convertTime(Object value) throws UnsupportedOperationException {
        if (value instanceof LocalTime) {
            return (LocalTime) value;
        }
        if (value instanceof Date) {
            return convertLocalTime((Date) value);
        }
        if (value instanceof Time) {
            return convertLocalTime((Time) value);
        }
        if (value instanceof LocalDateTime) {
            return convertLocalTime((LocalDateTime) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return convertLocalTime((java.sql.Timestamp) value);
        }
        if (value instanceof String) {
            return convertLocalTime((String) value);
        }
        if (value instanceof Number) {
            return convertLocalTime((Number) value);
        }
        if (value instanceof Duration) {
            return convertLocalTime((Duration) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to LocalTime");
    }

    default LocalTime convertLocalTime(LocalDateTime value) {
        return value.toLocalTime();
    }

    default LocalTime convertLocalTime(Time value) {
        return value.toLocalTime();
    }

    default LocalTime convertLocalTime(java.sql.Timestamp value) {
        return LocalTime.of(
                value.getHours(), value.getMinutes(), value.getSeconds(), value.getNanos());
    }

    default LocalTime convertLocalTime(Date value) {
        long millis = (int) (value.getTime() % TimeUnit.SECONDS.toMillis(1));
        int nanosOfSecond = (int) (millis * TimeUnit.MILLISECONDS.toNanos(1));
        return LocalTime.of(
                value.getHours(), value.getMinutes(), value.getSeconds(), nanosOfSecond);
    }

    default LocalTime convertLocalTime(Duration value) {
        Long nanos = value.toNanos();
        if (nanos >= 0 && nanos <= TimeUnit.DAYS.toNanos(1)) {
            return LocalTime.ofNanoOfDay(nanos);
        } else {
            throw new IllegalArgumentException(
                    "Time values must use number of milliseconds greater than 0 and less than 86400000000000");
        }
    }

    default LocalTime convertLocalTime(String value) {
        return LocalTime.parse(value);
    }

    default LocalTime convertLocalTime(Number value) {
        return LocalTime.ofSecondOfDay(value.longValue());
    }

    default LocalDate convertLocalDate(T typeDefine, Object value)
            throws UnsupportedOperationException {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        if (value instanceof Date) {
            return convertLocalDate(typeDefine, (Date) value);
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate();
        }
        if (value instanceof String) {
            return convertLocalDate(typeDefine, (String) value);
        }
        if (value instanceof Number) {
            return convertLocalDate(typeDefine, (Number) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert "
                        + value.getClass()
                        + " to LocalDate, typeDefine: "
                        + typeDefine);
    }

    default LocalDate convertLocalDate(T typeDefine, Date value) {
        return convertLocalDate(value);
    }

    default LocalDate convertLocalDate(T typeDefine, String value) {
        return convertLocalDate(value);
    }

    default LocalDate convertLocalDate(T typeDefine, Number value) {
        return convertLocalDate(value);
    }

    default LocalDate convertLocalDate(Object value) throws UnsupportedOperationException {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        if (value instanceof Date) {
            return convertLocalDate((Date) value);
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate();
        }
        if (value instanceof String) {
            return convertLocalDate((String) value);
        }
        if (value instanceof Number) {
            return convertLocalDate((Number) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to LocalDate");
    }

    default LocalDate convertLocalDate(Date value) {
        return value.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    default LocalDate convertLocalDate(String value) {
        return LocalDate.parse(value);
    }

    default LocalDate convertLocalDate(Number value) {
        if (value.longValue() < 999999999) {
            return LocalDateTime.ofEpochSecond(
                            value.longValue(),
                            0,
                            ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now()))
                    .toLocalDate();
        }
        return new Date(value.longValue()).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    default BigDecimal convertDecimal(T typeDefine, Object value)
            throws UnsupportedOperationException {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return convertDecimal(typeDefine, (Number) value);
        }
        if (value instanceof String) {
            return convertDecimal(typeDefine, (String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert "
                        + value.getClass()
                        + " to BigDecimal, typeDefine: "
                        + typeDefine);
    }

    default BigDecimal convertDecimal(T typeDefine, Number value) {
        return convertDecimal(value);
    }

    default BigDecimal convertDecimal(T typeDefine, String value) {
        return convertDecimal(value);
    }

    default BigDecimal convertDecimal(Object value) throws UnsupportedOperationException {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return convertDecimal((Number) value);
        }
        if (value instanceof String) {
            return convertDecimal((String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to BigDecimal");
    }

    default BigDecimal convertDecimal(Number value) {
        return new BigDecimal(value.doubleValue());
    }

    default BigDecimal convertDecimal(String value) {
        return new BigDecimal(value);
    }

    default double convertDouble(T typeDefine, Object value) throws UnsupportedOperationException {
        if (value instanceof Double) {
            return (double) value;
        }
        if (value instanceof Number) {
            return convertDouble(typeDefine, (Number) value);
        }
        if (value instanceof String) {
            return convertDouble(typeDefine, (String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert "
                        + value.getClass()
                        + " to Double, typeDefine: "
                        + typeDefine);
    }

    default double convertDouble(T typeDefine, Number value) {
        return convertDouble(value);
    }

    default double convertDouble(T typeDefine, String value) {
        return convertDouble(value);
    }

    default double convertDouble(Object value) throws UnsupportedOperationException {
        if (value instanceof Double) {
            return (double) value;
        }
        if (value instanceof Number) {
            return convertDouble((Number) value);
        }
        if (value instanceof String) {
            return convertDouble((String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Double");
    }

    default double convertDouble(Number value) {
        return value.doubleValue();
    }

    default double convertDouble(String value) {
        return Double.parseDouble(value);
    }

    default float convertFloat(T typeDefine, Object value) throws UnsupportedOperationException {
        if (value instanceof Float) {
            return (float) value;
        }
        if (value instanceof Number) {
            return convertFloat(typeDefine, (Number) value);
        }
        if (value instanceof String) {
            return convertFloat(typeDefine, (String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Float, typeDefine: " + typeDefine);
    }

    default float convertFloat(T typeDefine, Number value) {
        return convertFloat(value);
    }

    default float convertFloat(T typeDefine, String value) {
        return convertFloat(value);
    }

    default float convertFloat(Object value) throws UnsupportedOperationException {
        if (value instanceof Float) {
            return (float) value;
        }
        if (value instanceof Number) {
            return convertFloat((Number) value);
        }
        if (value instanceof String) {
            return convertFloat((String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Float");
    }

    default float convertFloat(Number value) {
        return value.floatValue();
    }

    default float convertFloat(String value) {
        return Float.parseFloat(value);
    }

    default long convertLong(T typeDefine, Object value) throws UnsupportedOperationException {
        if (value instanceof Long) {
            return (long) value;
        }
        if (value instanceof Number) {
            return convertLong(typeDefine, (Number) value);
        }
        if (value instanceof String) {
            return convertLong(typeDefine, (String) value);
        }
        if (value instanceof Time) {
            return convertLong(typeDefine, (Time) value);
        }
        if (value instanceof LocalTime) {
            return convertLong(typeDefine, (LocalTime) value);
        }
        if (value instanceof Date) {
            return convertLong(typeDefine, (Date) value);
        }
        if (value instanceof LocalDate) {
            return convertLong(typeDefine, (LocalDate) value);
        }
        if (value instanceof LocalDateTime) {
            return convertLong(typeDefine, (LocalDateTime) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Long, typeDefine: " + typeDefine);
    }

    default long convertLong(T typeDefine, Number value) {
        return convertLong(value);
    }

    default long convertLong(T typeDefine, String value) {
        return convertLong(value);
    }

    default long convertLong(T typeDefine, Time value) {
        return convertLong(value);
    }

    default long convertLong(T typeDefine, LocalTime value) {
        return convertLong(value);
    }

    default long convertLong(T typeDefine, Date value) {
        return convertLong(value);
    }

    default long convertLong(T typeDefine, LocalDate value) {
        return convertLong(value);
    }

    default long convertLong(T typeDefine, LocalDateTime value) {
        return convertLong(value);
    }

    default long convertLong(Object value) throws UnsupportedOperationException {
        if (value instanceof Long) {
            return (long) value;
        }
        if (value instanceof Number) {
            return convertLong((Number) value);
        }
        if (value instanceof String) {
            return convertLong((String) value);
        }
        if (value instanceof Time) {
            return convertLong((Time) value);
        }
        if (value instanceof LocalTime) {
            return convertLong((LocalTime) value);
        }
        if (value instanceof Date) {
            return convertLong((Date) value);
        }
        if (value instanceof LocalDate) {
            return convertLong((LocalDate) value);
        }
        if (value instanceof LocalDateTime) {
            return convertLong((LocalDateTime) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Long");
    }

    default long convertLong(Number value) {
        return value.longValue();
    }

    default long convertLong(String value) {
        return Long.parseLong(value);
    }

    default long convertLong(Time value) {
        return value.toLocalTime().toSecondOfDay();
    }

    default long convertLong(LocalTime value) {
        return value.toSecondOfDay();
    }

    default long convertLong(Date value) {
        return value.getTime();
    }

    default long convertLong(LocalDate value) {
        return value.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    default long convertLong(LocalDateTime value) {
        return value.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    default int convertInt(T typeDefine, Object value) throws UnsupportedOperationException {
        if (value instanceof Integer) {
            return (int) value;
        }
        if (value instanceof Number) {
            return convertInt(typeDefine, (Number) value);
        }
        if (value instanceof String) {
            return convertInt(typeDefine, (String) value);
        }
        if (value instanceof Time) {
            return convertInt(typeDefine, (Time) value);
        }
        if (value instanceof LocalTime) {
            return convertInt(typeDefine, (LocalTime) value);
        }
        if (value instanceof Date) {
            return convertInt(typeDefine, (Date) value);
        }
        if (value instanceof LocalDate) {
            return convertInt(typeDefine, (LocalDate) value);
        }
        if (value instanceof LocalDateTime) {
            return convertInt(typeDefine, (LocalDateTime) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert "
                        + value.getClass()
                        + " to Integer, typeDefine: "
                        + typeDefine);
    }

    default int convertInt(T typeDefine, Number value) {
        return convertInt(value);
    }

    default int convertInt(T typeDefine, String value) {
        return convertInt(value);
    }

    default int convertInt(T typeDefine, Time value) {
        return convertInt(value);
    }

    default int convertInt(T typeDefine, LocalTime value) {
        return convertInt(value);
    }

    default int convertInt(T typeDefine, Date value) {
        return convertInt(value);
    }

    default int convertInt(T typeDefine, LocalDate value) {
        return convertInt(value);
    }

    default int convertInt(T typeDefine, LocalDateTime value) {
        return convertInt(value);
    }

    default int convertInt(Object value) throws UnsupportedOperationException {
        if (value instanceof Integer) {
            return (int) value;
        }
        if (value instanceof Number) {
            return convertInt((Number) value);
        }
        if (value instanceof String) {
            return convertInt((String) value);
        }
        if (value instanceof Time) {
            return convertInt((Time) value);
        }
        if (value instanceof LocalTime) {
            return convertInt((LocalTime) value);
        }
        if (value instanceof Date) {
            return convertInt((Date) value);
        }
        if (value instanceof LocalDate) {
            return convertInt((LocalDate) value);
        }
        if (value instanceof LocalDateTime) {
            return convertInt((LocalDateTime) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Integer");
    }

    default int convertInt(Number value) {
        return value.intValue();
    }

    default int convertInt(String value) {
        return Integer.parseInt(value);
    }

    default int convertInt(Time value) {
        return value.toLocalTime().toSecondOfDay();
    }

    default int convertInt(LocalTime value) {
        return value.toSecondOfDay();
    }

    default int convertInt(Date value) {
        return (int) (value.getTime() / 1000);
    }

    default int convertInt(LocalDateTime value) {
        return (int) (value.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() / 1000);
    }

    default int convertInt(LocalDate value) {
        return (int) (value.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli() / 1000);
    }

    default short convertShort(T typeDefine, Object value) throws UnsupportedOperationException {
        if (value instanceof Short) {
            return (short) value;
        }
        if (value instanceof Number) {
            return convertShort(typeDefine, (Number) value);
        }
        if (value instanceof String) {
            return convertShort(typeDefine, (String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Short, typeDefine: " + typeDefine);
    }

    default short convertShort(T typeDefine, Number value) {
        return convertShort(value);
    }

    default short convertShort(T typeDefine, String value) {
        return convertShort(value);
    }

    default short convertShort(Object value) throws UnsupportedOperationException {
        if (value instanceof Short) {
            return (short) value;
        }
        if (value instanceof Number) {
            return convertShort((Number) value);
        }
        if (value instanceof String) {
            return convertShort((String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Short");
    }

    default short convertShort(Number value) {
        return value.shortValue();
    }

    default short convertShort(String value) {
        return Short.parseShort(value);
    }

    default byte convertByte(T typeDefine, Object value) throws UnsupportedOperationException {
        if (value instanceof Byte) {
            return (byte) value;
        }
        if (value instanceof Number) {
            return convertByte(typeDefine, (Number) value);
        }
        if (value instanceof String) {
            return convertByte(typeDefine, (String) value);
        }
        if (value instanceof Boolean) {
            return convertByte(typeDefine, ((boolean) value));
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Byte, typeDefine: " + typeDefine);
    }

    default byte convertByte(T typeDefine, Number value) {
        return convertByte(value);
    }

    default byte convertByte(T typeDefine, String value) {
        return convertByte(value);
    }

    default byte convertByte(T typeDefine, boolean value) {
        return convertByte(value);
    }

    default byte convertByte(Object value) throws UnsupportedOperationException {
        if (value instanceof Byte) {
            return (byte) value;
        }
        if (value instanceof Number) {
            return convertByte((Number) value);
        }
        if (value instanceof String) {
            return convertByte((String) value);
        }
        if (value instanceof Boolean) {
            return convertByte(((boolean) value));
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Byte");
    }

    default byte convertByte(Number value) {
        return value.byteValue();
    }

    default byte convertByte(String value) {
        return Byte.parseByte(value);
    }

    default byte convertByte(boolean value) {
        return value ? (byte) 1 : (byte) 0;
    }

    default boolean convertBoolean(T typeDefine, Object value)
            throws UnsupportedOperationException {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return convertBoolean(typeDefine, (Number) value);
        }
        if (value instanceof String) {
            return convertBoolean(typeDefine, (String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert "
                        + value.getClass()
                        + " to Boolean, typeDefine: "
                        + typeDefine);
    }

    default boolean convertBoolean(T typeDefine, Number value) {
        return convertBoolean(value);
    }

    default boolean convertBoolean(T typeDefine, String value) {
        return convertBoolean(value);
    }

    default boolean convertBoolean(Object value) throws UnsupportedOperationException {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return convertBoolean((Number) value);
        }
        if (value instanceof String) {
            return convertBoolean((String) value);
        }
        throw new UnsupportedOperationException(
                "Unsupported convert " + value.getClass() + " to Boolean");
    }

    default boolean convertBoolean(Number value) {
        return value.intValue() != 0;
    }

    default boolean convertBoolean(String value) {
        return Boolean.parseBoolean(value);
    }
}
