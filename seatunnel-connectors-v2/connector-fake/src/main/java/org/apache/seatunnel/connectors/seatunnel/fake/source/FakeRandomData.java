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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import static org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.BYTE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.SHORT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.VOID_TYPE;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeatunnelSchema;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FakeRandomData {
    public static final String SCHEMA = "schema";
    private final SeatunnelSchema schema;

    public FakeRandomData(SeatunnelSchema schema) {
        this.schema = schema;
    }

    public SeaTunnelRow randomRow() {
        SeaTunnelRowType seaTunnelRowType = schema.getSeaTunnelRowType();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        List<Object> randomRow = new ArrayList<>(fieldNames.length);
        for (SeaTunnelDataType<?> fieldType : fieldTypes) {
            randomRow.add(randomColumnValue(fieldType));
        }
        return new SeaTunnelRow(randomRow.toArray());
    }

    @SuppressWarnings("magicnumber")
    private Object randomColumnValue(SeaTunnelDataType<?> fieldType) {
        if (BOOLEAN_TYPE.equals(fieldType)) {
            return RandomUtils.nextInt(0, 2) == 1;
        } else if (BYTE_TYPE.equals(fieldType)) {
            return (byte) RandomUtils.nextInt(0, 255);
        } else if (SHORT_TYPE.equals(fieldType)) {
            return (short) RandomUtils.nextInt(Byte.MAX_VALUE, Short.MAX_VALUE);
        } else if (INT_TYPE.equals(fieldType)) {
            return RandomUtils.nextInt(Short.MAX_VALUE, Integer.MAX_VALUE);
        } else if (LONG_TYPE.equals(fieldType)) {
            return RandomUtils.nextLong(Integer.MAX_VALUE, Long.MAX_VALUE);
        } else if (FLOAT_TYPE.equals(fieldType)) {
            return RandomUtils.nextFloat(Float.MIN_VALUE, Float.MAX_VALUE);
        } else if (DOUBLE_TYPE.equals(fieldType)) {
            return RandomUtils.nextDouble(Float.MAX_VALUE, Double.MAX_VALUE);
        } else if (STRING_TYPE.equals(fieldType)) {
            return RandomStringUtils.randomAlphabetic(10);
        } else if (LocalTimeType.LOCAL_DATE_TYPE.equals(fieldType)) {
            return randomLocalDateTime().toLocalDate();
        } else if (LocalTimeType.LOCAL_TIME_TYPE.equals(fieldType)) {
            return randomLocalDateTime().toLocalTime();
        } else if (LocalTimeType.LOCAL_DATE_TIME_TYPE.equals(fieldType)) {
            return randomLocalDateTime();
        } else if (fieldType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) fieldType;
            return new BigDecimal(RandomStringUtils.randomNumeric(decimalType.getPrecision() - decimalType.getScale()) + "." +
                RandomStringUtils.randomNumeric(decimalType.getScale()));
        } else if (fieldType instanceof ArrayType) {
            ArrayType<?, ?> arrayType = (ArrayType<?, ?>) fieldType;
            BasicType<?> elementType = arrayType.getElementType();
            Object value = randomColumnValue(elementType);
            Object arr = Array.newInstance(elementType.getTypeClass(), 1);
            Array.set(arr, 0, value);
            return arr;
        } else if (fieldType instanceof MapType) {
            MapType<?, ?> mapType = (MapType<?, ?>) fieldType;
            SeaTunnelDataType<?> keyType = mapType.getKeyType();
            Object key = randomColumnValue(keyType);
            SeaTunnelDataType<?> valueType = mapType.getValueType();
            Object value = randomColumnValue(valueType);
            HashMap<Object, Object> objectObjectHashMap = new HashMap<>();
            objectObjectHashMap.put(key, value);
            return objectObjectHashMap;
        } else if (fieldType instanceof PrimitiveByteArrayType) {
            return RandomUtils.nextBytes(100);
        } else if (VOID_TYPE.equals(fieldType) || fieldType == null) {
            return Void.TYPE;
        } else {
            throw new UnsupportedOperationException("Unexpected value: " + fieldType);
        }
    }

    @SuppressWarnings("magicnumber")
    private LocalDateTime randomLocalDateTime() {
        return LocalDateTime.of(
            LocalDateTime.now().getYear(),
            RandomUtils.nextInt(1, 12),
            RandomUtils.nextInt(1, LocalDateTime.now().getDayOfMonth()),
            RandomUtils.nextInt(0, 24),
            RandomUtils.nextInt(0, 59)
        );
    }
}
