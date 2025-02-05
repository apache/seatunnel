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

package org.apache.seatunnel.translation.spark.serialization;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.serialization.RowConverter;
import org.apache.seatunnel.translation.spark.utils.InstantConverterUtils;
import org.apache.seatunnel.translation.spark.utils.OffsetDateTimeUtils;
import org.apache.seatunnel.translation.spark.utils.TypeConverterUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.MutableAny;
import org.apache.spark.sql.catalyst.expressions.MutableBoolean;
import org.apache.spark.sql.catalyst.expressions.MutableByte;
import org.apache.spark.sql.catalyst.expressions.MutableDouble;
import org.apache.spark.sql.catalyst.expressions.MutableFloat;
import org.apache.spark.sql.catalyst.expressions.MutableInt;
import org.apache.spark.sql.catalyst.expressions.MutableLong;
import org.apache.spark.sql.catalyst.expressions.MutableShort;
import org.apache.spark.sql.catalyst.expressions.MutableValue;
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import scala.Some;
import scala.Tuple2;
import scala.collection.immutable.HashMap.HashTrieMap;
import scala.collection.immutable.List;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.IntStream;

public final class InternalRowConverter extends RowConverter<InternalRow> {
    private final int[] indexes;

    public InternalRowConverter(SeaTunnelDataType<?> dataType) {
        super(dataType);
        indexes = IntStream.range(0, ((SeaTunnelRowType) dataType).getTotalFields()).toArray();
    }

    public InternalRowConverter(SeaTunnelDataType<?> dataType, int[] indexes) {
        super(dataType);
        this.indexes = indexes;
    }

    @Override
    public InternalRow convert(SeaTunnelRow seaTunnelRow) throws IOException {
        return parcel(seaTunnelRow, (SeaTunnelRowType) dataType);
    }

    private static Object convert(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case ROW:
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) field;
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                return convert(seaTunnelRow, rowType);
            case DATE:
                return (int) ((LocalDate) field).toEpochDay();
            case TIME:
                return ((LocalTime) field).toNanoOfDay();
            case TIMESTAMP:
                return InstantConverterUtils.toEpochMicro(
                        Timestamp.valueOf((LocalDateTime) field).toInstant());
            case TIMESTAMP_TZ:
                return Decimal.apply(OffsetDateTimeUtils.toBigDecimal((OffsetDateTime) field));
            case MAP:
                return convertMap((Map<?, ?>) field, (MapType<?, ?>) dataType);
            case STRING:
                return UTF8String.fromString((String) field);
            case DECIMAL:
                return Decimal.apply((BigDecimal) field);
            case ARRAY:
                Class<?> elementTypeClass =
                        ((ArrayType<?, ?>) dataType).getElementType().getTypeClass();

                if (((ArrayType<?, ?>) dataType).getElementType() instanceof MapType) {
                    Object arrayMap =
                            Array.newInstance(ArrayBasedMapData.class, ((Map[]) field).length);
                    for (int i = 0; i < ((Map[]) field).length; i++) {
                        Map<?, ?> value = (Map<?, ?>) ((Map[]) field)[i];
                        MapType<?, ?> type =
                                (MapType<?, ?>) ((ArrayType<?, ?>) dataType).getElementType();
                        Array.set(arrayMap, i, convertMap(value, type));
                    }
                    return ArrayData.toArrayData(arrayMap);
                }
                // if string array, we need to covert every item in array from String to UTF8String
                if (((ArrayType<?, ?>) dataType).getElementType().equals(BasicType.STRING_TYPE)) {
                    Object[] fields = (Object[]) field;
                    UTF8String[] objects =
                            Arrays.stream(fields)
                                    .map(v -> UTF8String.fromString((String) v))
                                    .toArray(UTF8String[]::new);
                    return ArrayData.toArrayData(objects);
                }
                // except string, now only support convert boolean int tinyint smallint bigint float
                // double, because SeaTunnel Array only support these types
                Object array = Array.newInstance(elementTypeClass, ((Object[]) field).length);
                for (int i = 0; i < ((Object[]) field).length; i++) {
                    Array.set(array, i, ((Object[]) field)[i]);
                }
                return ArrayData.toArrayData(field);
            default:
                if (field instanceof Some) {
                    return ((Some<?>) field).get();
                }
                return field;
        }
    }

    private static InternalRow convert(SeaTunnelRow seaTunnelRow, SeaTunnelRowType rowType) {
        int arity = rowType.getTotalFields();
        MutableValue[] values = new MutableValue[arity];
        for (int i = 0; i < arity; i++) {
            values[i] = createMutableValue(rowType.getFieldType(i));
            if (TypeConverterUtils.ROW_KIND_FIELD.equals(rowType.getFieldName(i))) {
                values[i].update(seaTunnelRow.getRowKind().toByteValue());
            } else {
                Object fieldValue = convert(seaTunnelRow.getField(i), rowType.getFieldType(i));
                if (fieldValue != null) {
                    values[i].update(fieldValue);
                }
            }
        }
        return new SpecificInternalRow(values);
    }

    private InternalRow parcel(SeaTunnelRow seaTunnelRow, SeaTunnelRowType rowType) {
        // 0 -> row kind, 1 -> table id
        int arity = rowType.getTotalFields();
        MutableValue[] values = new MutableValue[arity + 2];
        for (int i = 0; i < indexes.length; i++) {
            values[indexes[i] + 2] = createMutableValue(rowType.getFieldType(indexes[i]));
            Object fieldValue = convert(seaTunnelRow.getField(i), rowType.getFieldType(indexes[i]));
            if (fieldValue != null) {
                values[indexes[i] + 2].update(fieldValue);
            }
        }
        values[0] = new MutableByte();
        values[0].update(seaTunnelRow.getRowKind().toByteValue());
        values[1] = new MutableAny();
        values[1].update(UTF8String.fromString(seaTunnelRow.getTableId()));
        // Fill any remaining null values with MutableAny
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                values[i] = new MutableAny();
            }
        }
        return new SpecificInternalRow(values);
    }

    private static ArrayBasedMapData convertMap(Map<?, ?> mapData, MapType<?, ?> mapType) {
        if (mapData == null || mapData.size() == 0) {
            return ArrayBasedMapData.apply(new Object[] {}, new Object[] {});
        }
        SeaTunnelDataType<?> keyType = mapType.getKeyType();
        SeaTunnelDataType<?> valueType = mapType.getValueType();
        Map<Object, Object> newMap = new HashMap<>(mapData.size());
        mapData.forEach(
                (key, value) -> newMap.put(convert(key, keyType), convert(value, valueType)));
        Object[] keys = newMap.keySet().toArray();
        Object[] values = newMap.values().toArray();
        return ArrayBasedMapData.apply(keys, values);
    }

    private static Map<Object, Object> reconvertMap(MapData mapData, MapType<?, ?> mapType) {
        if (mapData == null || mapData.numElements() == 0) {
            return Collections.emptyMap();
        }
        Map<Object, Object> newMap = new HashMap<>(mapData.numElements());
        int num = mapData.numElements();
        SeaTunnelDataType<?> keyType = mapType.getKeyType();
        SeaTunnelDataType<?> valueType = mapType.getValueType();
        Object[] keys = mapData.keyArray().toObjectArray(TypeConverterUtils.convert(keyType));
        Object[] values = mapData.valueArray().toObjectArray(TypeConverterUtils.convert(valueType));
        for (int i = 0; i < num; i++) {
            keys[i] = reconvert(keys[i], keyType);
            values[i] = reconvert(values[i], valueType);
            newMap.put(keys[i], values[i]);
        }
        return newMap;
    }

    private static Map<Object, Object> reconvertMap(
            HashTrieMap<?, ?> hashTrieMap, MapType<?, ?> mapType) {
        if (hashTrieMap == null || hashTrieMap.size() == 0) {
            return Collections.emptyMap();
        }
        int num = hashTrieMap.size();
        Map<Object, Object> newMap = new LinkedHashMap<>(num);
        SeaTunnelDataType<?> keyType = mapType.getKeyType();
        SeaTunnelDataType<?> valueType = mapType.getValueType();
        List<?> keyList = hashTrieMap.keySet().toList();
        List<?> valueList = hashTrieMap.values().toList();
        for (int i = 0; i < num; i++) {
            Object key = keyList.apply(i);
            Object value = valueList.apply(i);
            key = reconvert(key, keyType);
            value = reconvert(value, valueType);
            newMap.put(key, value);
        }
        return newMap;
    }

    private static MutableValue createMutableValue(SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return new MutableBoolean();
            case TINYINT:
                return new MutableByte();
            case SMALLINT:
                return new MutableShort();
            case INT:
            case DATE:
                return new MutableInt();
            case BIGINT:
            case TIME:
            case TIMESTAMP:
                return new MutableLong();
            case FLOAT:
                return new MutableFloat();
            case DOUBLE:
                return new MutableDouble();
            default:
                return new MutableAny();
        }
    }

    public SeaTunnelRow unpack(InternalRow engineRow, SeaTunnelRowType rowType) throws IOException {
        RowKind rowKind = RowKind.fromByteValue(engineRow.getByte(0));
        String tableId = engineRow.getString(1);
        Object[] fields = new Object[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            fields[i] =
                    reconvert(
                            engineRow.get(
                                    indexes[i] + 2,
                                    TypeConverterUtils.convert(rowType.getFieldType(indexes[i]))),
                            rowType.getFieldType(indexes[i]));
        }
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        seaTunnelRow.setRowKind(rowKind);
        seaTunnelRow.setTableId(tableId);
        return seaTunnelRow;
    }

    @Override
    public SeaTunnelRow reconvert(InternalRow engineRow) throws IOException {
        return unpack(engineRow, (SeaTunnelRowType) dataType);
    }

    private static Object reconvert(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case ROW:
                return reconvert((InternalRow) field, (SeaTunnelRowType) dataType);
            case DATE:
                if (field instanceof Date) {
                    return ((Date) field).toLocalDate();
                }
                return LocalDate.ofEpochDay((int) field);
            case TIME:
                if (field instanceof Timestamp) {
                    return LocalTime.ofNanoOfDay(((Timestamp) field).getNanos());
                }
                return LocalTime.ofNanoOfDay((long) field);
            case TIMESTAMP:
                if (field instanceof Timestamp) {
                    return ((Timestamp) field).toLocalDateTime();
                }
                return Timestamp.from(InstantConverterUtils.ofEpochMicro((long) field))
                        .toLocalDateTime();
            case TIMESTAMP_TZ:
                BigDecimal timeWithDecimal = null;
                if (field instanceof Decimal) {
                    timeWithDecimal = ((Decimal) field).toJavaBigDecimal();
                } else if (field instanceof BigDecimal) {
                    timeWithDecimal = (BigDecimal) field;
                }
                return OffsetDateTimeUtils.toOffsetDateTime(timeWithDecimal);
            case MAP:
                if (field instanceof MapData) {
                    return reconvertMap((MapData) field, (MapType<?, ?>) dataType);
                } else if (field instanceof HashTrieMap) {
                    return reconvertMap((HashTrieMap<?, ?>) field, (MapType<?, ?>) dataType);
                } else {
                    throw new RuntimeException(
                            String.format(
                                    "SeaTunnel unsupported Spark internal Map type: %s ",
                                    field.getClass()));
                }
            case STRING:
                return field.toString();
            case DECIMAL:
                if (field instanceof Decimal) {
                    return ((Decimal) field).toJavaBigDecimal();
                } else if (field instanceof BigDecimal) {
                    return field;
                }
            case ARRAY:
                if (field instanceof ArrayData) {
                    return reconvertArray((ArrayData) field, (ArrayType<?, ?>) dataType);
                } else if (field instanceof WrappedArray.ofRef) {
                    return reconvertArray(
                            (WrappedArray.ofRef<?>) field, (ArrayType<?, ?>) dataType);
                } else {
                    throw new RuntimeException(
                            String.format(
                                    "SeaTunnel unsupported Spark internal Array type: %s ",
                                    field.getClass()));
                }
            default:
                return field;
        }
    }

    private static SeaTunnelRow reconvert(InternalRow engineRow, SeaTunnelRowType rowType) {
        Object[] fields = new Object[engineRow.numFields()];
        for (int i = 0; i < engineRow.numFields(); i++) {
            fields[i] =
                    reconvert(
                            engineRow.get(i, TypeConverterUtils.convert(rowType.getFieldType(i))),
                            rowType.getFieldType(i));
        }
        return new SeaTunnelRow(fields);
    }

    private static Object reconvertArray(ArrayData arrayData, ArrayType<?, ?> arrayType) {
        Class<?> elementTypeClass = arrayType.getElementType().getTypeClass();
        if (arrayData == null || arrayData.numElements() == 0) {
            return Collections.emptyList().toArray();
        }
        Object[] newArray = (Object[]) Array.newInstance(elementTypeClass, arrayData.numElements());
        Object[] values =
                arrayData.toObjectArray(TypeConverterUtils.convert(arrayType.getElementType()));
        for (int i = 0; i < arrayData.numElements(); i++) {
            Object reconvert =
                    elementTypeClass.cast(reconvert(values[i], arrayType.getElementType()));
            newArray[i] = reconvert;
        }
        return newArray;
    }

    private static Object reconvertArray(
            WrappedArray.ofRef<?> arrayData, ArrayType<?, ?> arrayType) {
        if (arrayData == null || arrayData.size() == 0) {
            return Collections.emptyList().toArray();
        }
        Object[] newArray = new Object[arrayData.size()];
        for (int i = 0; i < arrayData.size(); i++) {
            newArray[i] = reconvert(arrayData.apply(i), arrayType.getElementType());
        }
        return newArray;
    }

    public Object[] convertToFields(InternalRow internalRow, StructType structType) {
        Object[] fields =
                Arrays.stream(((SpecificInternalRow) internalRow).values())
                        .map(MutableValue::boxed)
                        .toArray();
        int len = structType.fields().length;
        for (int i = 0; i < len; i++) {
            DataType dataType = structType.fields()[i].dataType();
            fields[i] = convertToField(fields[i], dataType);
        }
        return fields;
    }

    private Object convertToField(Object internalRowField, DataType dataType) {
        if (dataType == DataTypes.TimestampType && internalRowField instanceof Long) {
            return Timestamp.from(InstantConverterUtils.ofEpochMicro((long) internalRowField));
        } else if (dataType == DataTypes.DateType && internalRowField instanceof Integer) {
            return Date.valueOf(LocalDate.ofEpochDay((int) internalRowField));
        } else if (dataType == DataTypes.StringType && internalRowField instanceof UTF8String) {
            return internalRowField.toString();
        } else if (dataType instanceof org.apache.spark.sql.types.MapType
                && internalRowField instanceof MapData) {
            MapData mapData = (MapData) internalRowField;

            scala.collection.immutable.HashMap<Object, Object> newMap =
                    new scala.collection.immutable.HashMap<>();

            if (mapData.numElements() == 0) {
                return newMap;
            }
            org.apache.spark.sql.types.MapType mapType =
                    (org.apache.spark.sql.types.MapType) dataType;

            int num = mapData.numElements();
            Object[] keys = mapData.keyArray().toObjectArray(mapType.keyType());
            Object[] values = mapData.valueArray().toObjectArray(mapType.valueType());
            for (int i = 0; i < num; i++) {
                keys[i] = convertToField(keys[i], mapType.keyType());
                values[i] = convertToField(values[i], mapType.valueType());
                Tuple2<Object, Object> tuple2 = new Tuple2<>(keys[i], values[i]);
                newMap = newMap.$plus(tuple2);
            }
            return newMap;
        } else if (dataType instanceof org.apache.spark.sql.types.ArrayType
                && internalRowField instanceof ArrayData) {
            ArrayData arrayData = (ArrayData) internalRowField;
            if (arrayData.numElements() == 0) {
                return new WrappedArray.ofRef<>(new Object[0]);
            }
            org.apache.spark.sql.types.ArrayType arrayType =
                    (org.apache.spark.sql.types.ArrayType) dataType;
            Object[] values = arrayData.array();
            int num = arrayData.numElements();
            for (int i = 0; i < num; i++) {
                values[i] = convertToField(values[i], arrayType.elementType());
            }
            return new WrappedArray.ofRef<>(values);
        }
        return internalRowField;
    }
}
