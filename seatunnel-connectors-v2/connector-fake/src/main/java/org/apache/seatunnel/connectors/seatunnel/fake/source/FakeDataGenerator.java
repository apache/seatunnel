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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;
import org.apache.seatunnel.connectors.seatunnel.fake.exception.FakeConnectorException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FakeDataGenerator {
    public static final String SCHEMA = "schema";
    private final SeaTunnelSchema schema;
    private final FakeConfig fakeConfig;

    public FakeDataGenerator(SeaTunnelSchema schema, FakeConfig fakeConfig) {
        this.schema = schema;
        this.fakeConfig = fakeConfig;
    }

    private SeaTunnelRow randomRow() {
        SeaTunnelRowType seaTunnelRowType = schema.getSeaTunnelRowType();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        List<Object> randomRow = new ArrayList<>(fieldNames.length);
        for (SeaTunnelDataType<?> fieldType : fieldTypes) {
            randomRow.add(randomColumnValue(fieldType));
        }
        return new SeaTunnelRow(randomRow.toArray());
    }

    public List<SeaTunnelRow> generateFakedRows(int rowNum) {
        ArrayList<SeaTunnelRow> seaTunnelRows = new ArrayList<>();
        for (int i = 0; i < rowNum; i++) {
            seaTunnelRows.add(randomRow());
        }
        return seaTunnelRows;
    }

    @SuppressWarnings("magicnumber")
    private Object randomColumnValue(SeaTunnelDataType<?> fieldType) {
        switch (fieldType.getSqlType()) {
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) fieldType;
                BasicType<?> elementType = arrayType.getElementType();
                int length = fakeConfig.getArraySize();
                Object array = Array.newInstance(elementType.getTypeClass(), length);
                for (int i = 0; i < length; i++) {
                    Object value = randomColumnValue(elementType);
                    Array.set(array, i, value);
                }
                return array;
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) fieldType;
                SeaTunnelDataType<?> keyType = mapType.getKeyType();
                SeaTunnelDataType<?> valueType = mapType.getValueType();
                HashMap<Object, Object> objectMap = new HashMap<>();
                int mapSize = fakeConfig.getMapSize();
                for (int i = 0; i < mapSize; i++) {
                    Object key = randomColumnValue(keyType);
                    Object value = randomColumnValue(valueType);
                    objectMap.put(key, value);
                }
                return objectMap;
            case STRING:
                return RandomStringUtils.randomAlphabetic(fakeConfig.getStringLength());
            case BOOLEAN:
                return RandomUtils.nextInt(0, 2) == 1;
            case TINYINT:
                return (byte) RandomUtils.nextInt(0, 255);
            case SMALLINT:
                return (short) RandomUtils.nextInt(Byte.MAX_VALUE, Short.MAX_VALUE);
            case INT:
                return RandomUtils.nextInt(Short.MAX_VALUE, Integer.MAX_VALUE);
            case BIGINT:
                return RandomUtils.nextLong(Integer.MAX_VALUE, Long.MAX_VALUE);
            case FLOAT:
                return RandomUtils.nextFloat(Float.MIN_VALUE, Float.MAX_VALUE);
            case DOUBLE:
                return RandomUtils.nextDouble(Float.MAX_VALUE, Double.MAX_VALUE);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return new BigDecimal(RandomStringUtils.randomNumeric(decimalType.getPrecision() - decimalType.getScale()) + "." +
                        RandomStringUtils.randomNumeric(decimalType.getScale()));
            case NULL:
                return null;
            case BYTES:
                return RandomStringUtils.randomAlphabetic(fakeConfig.getBytesLength()).getBytes();
            case DATE:
                return randomLocalDateTime().toLocalDate();
            case TIME:
                return randomLocalDateTime().toLocalTime();
            case TIMESTAMP:
                return randomLocalDateTime();
            case ROW:
                SeaTunnelDataType<?>[] fieldTypes = ((SeaTunnelRowType) fieldType).getFieldTypes();
                Object[] objects = new Object[fieldTypes.length];
                for (int i = 0; i < fieldTypes.length; i++) {
                    Object object = randomColumnValue(fieldTypes[i]);
                    objects[i] = object;
                }
                return new SeaTunnelRow(objects);
            default:
                // never got in there
                throw new FakeConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "SeaTunnel Fake source connector not support this data type");
        }
    }

    @SuppressWarnings("magicnumber")
    private LocalDateTime randomLocalDateTime() {
        return LocalDateTime.of(
            LocalDateTime.now().getYear(),
            RandomUtils.nextInt(1, 12),
            RandomUtils.nextInt(1, 28),
            RandomUtils.nextInt(0, 24),
            RandomUtils.nextInt(0, 59)
        );
    }
}
