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

package org.apache.seatunnel.translation.spark.common.serialization;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.serialization.RowConverter;
import org.apache.seatunnel.translation.spark.common.utils.InstantConverterUtils;
import org.apache.seatunnel.translation.spark.common.utils.TypeConverterUtils;

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
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class InternalRowConverter extends RowConverter<InternalRow> {

    public InternalRowConverter(SeaTunnelDataType<?> dataType) {
        super(dataType);
    }

    @Override
    public InternalRow convert(SeaTunnelRow seaTunnelRow) throws IOException {
        validate(seaTunnelRow);
        return (InternalRow) convert(seaTunnelRow, dataType);
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
                // TODO: Support TIME Type
                throw new RuntimeException("time type is not supported now, but will be supported in the future.");
            case TIMESTAMP:
                return InstantConverterUtils.toEpochMicro(Timestamp.valueOf((LocalDateTime) field).toInstant());
            case MAP:
                return convertMap((Map<?, ?>) field, (MapType<?, ?>) dataType);
            case STRING:
                return UTF8String.fromString((String) field);
            case DECIMAL:
                return Decimal.apply((BigDecimal) field);
            case ARRAY:
                // if string array, we need to covert every item in array from String to UTF8String
                if (((ArrayType<?, ?>) dataType).getElementType().equals(BasicType.STRING_TYPE)) {
                    String[] fields = (String[]) field;
                    Object[] objects = Arrays.stream(fields).map(UTF8String::fromString).toArray();
                    return ArrayData.toArrayData(objects);
                }
                // except string, now only support convert boolean int tinyint smallint bigint float double, because SeaTunnel Array only support these types
                return ArrayData.toArrayData(field);
            default:
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

    private static ArrayBasedMapData convertMap(Map<?, ?> mapData, MapType<?, ?> mapType) {
        if (mapData == null || mapData.size() == 0) {
            return ArrayBasedMapData.apply(new Object[]{}, new Object[]{});
        }
        SeaTunnelDataType<?> keyType = mapType.getKeyType();
        SeaTunnelDataType<?> valueType = mapType.getValueType();
        Map<Object, Object> newMap = new HashMap<>(mapData.size());
        mapData.forEach((key, value) -> newMap.put(convert(key, keyType), convert(value, valueType)));
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

    @Override
    public SeaTunnelRow reconvert(InternalRow engineRow) throws IOException {
        return (SeaTunnelRow) reconvert(engineRow, dataType);
    }

    private static Object reconvert(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case ROW:
                return reconvert((InternalRow) field, (SeaTunnelRowType) dataType);
            case DATE:
                return LocalDate.ofEpochDay((int) field);
            case TIME:
                // TODO: Support TIME Type
                throw new RuntimeException("SeaTunnel not support time type, it will be supported in the future.");
            case TIMESTAMP:
                return Timestamp.from(InstantConverterUtils.ofEpochMicro((long) field)).toLocalDateTime();
            case MAP:
                return reconvertMap((MapData) field, (MapType<?, ?>) dataType);
            case STRING:
                return field.toString();
            case DECIMAL:
                return ((Decimal) field).toJavaBigDecimal();
            case ARRAY:
                return reconvertArray((ArrayData) field, (ArrayType<?, ?>) dataType);
            default:
                return field;
        }
    }

    private static SeaTunnelRow reconvert(InternalRow engineRow, SeaTunnelRowType rowType) {
        Object[] fields = new Object[engineRow.numFields()];
        for (int i = 0; i < engineRow.numFields(); i++) {
            fields[i] = reconvert(engineRow.get(i, TypeConverterUtils.convert(rowType.getFieldType(i))),
                rowType.getFieldType(i));
        }
        return new SeaTunnelRow(fields);
    }

    private static Object reconvertArray(ArrayData arrayData, ArrayType<?, ?> arrayType) {
        if (arrayData == null || arrayData.numElements() == 0) {
            return Collections.emptyList().toArray();
        }
        Object[] newArray = new Object[arrayData.numElements()];
        Object[] values = arrayData.toObjectArray(TypeConverterUtils.convert(arrayType.getElementType()));
        for (int i = 0; i < arrayData.numElements(); i++) {
            newArray[i] = reconvert(values[i], arrayType.getElementType());
        }
        return newArray;
    }
}
