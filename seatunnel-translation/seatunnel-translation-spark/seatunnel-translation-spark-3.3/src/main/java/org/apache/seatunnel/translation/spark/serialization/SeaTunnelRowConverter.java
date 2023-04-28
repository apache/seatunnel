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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.translation.serialization.BaseRowConverter;

import org.apache.spark.unsafe.types.UTF8String;

import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.mutable.WrappedArray;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class SeaTunnelRowConverter extends BaseRowConverter {
    public SeaTunnelRowConverter(SeaTunnelDataType<?> dataType) {
        super(dataType);
    }

    @Override
    protected Object convert(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case STRING:
                return field.toString();
            case MAP:
                return convertMap((Map<?, ?>) field, (MapType<?, ?>) dataType);
            case ARRAY:
                // if string array, we need to covert every item in array from String to UTF8String
                if (((ArrayType<?, ?>) dataType).getElementType().equals(BasicType.STRING_TYPE)) {
                    Object[] fields = (Object[]) field;
                    Object[] objects =
                            Arrays.stream(fields)
                                    .map(v -> UTF8String.fromString((String) v))
                                    .toArray();
                    return convertArray(objects, (ArrayType<?, ?>) dataType);
                }
                // except string, now only support convert boolean int tinyint smallint bigint float
                // double, because SeaTunnel Array only support these types
                return convertArray((Object[]) field, (ArrayType<?, ?>) dataType);
        }
        field = super.convert(field, dataType);
        if (field instanceof scala.Some) {
            return ((scala.Some<?>) field).get();
        }
        return field;
    }

    private scala.collection.immutable.HashMap<Object, Object> convertMap(
            Map<?, ?> mapData, MapType<?, ?> mapType) {
        scala.collection.immutable.HashMap<Object, Object> newMap =
                new scala.collection.immutable.HashMap<>();
        if (mapData.size() == 0) {
            return newMap;
        }
        int num = mapData.size();
        Object[] keys = mapData.keySet().toArray();
        Object[] values = mapData.values().toArray();
        for (int i = 0; i < num; i++) {
            keys[i] = convert(keys[i], mapType.getKeyType());
            values[i] = convert(values[i], mapType.getValueType());
            Tuple2<Object, Object> tuple2 = new Tuple2<>(keys[i], values[i]);
            newMap = newMap.$plus(tuple2);
        }

        return newMap;
    }

    private WrappedArray.ofRef convertArray(Object[] arrayData, ArrayType<?, ?> arrayType) {
        if (arrayData.length == 0) {
            return new WrappedArray.ofRef(new Object[0]);
        }
        int num = arrayData.length;
        for (int i = 0; i < num; i++) {
            arrayData[i] = convert(arrayData[i], arrayType.getElementType());
        }
        return new WrappedArray.ofRef(arrayData);
    }

    @Override
    protected Object reconvert(Object field, SeaTunnelDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case STRING:
                return field.toString();
            case MAP:
                return reconvertMap((HashMap.HashTrieMap) field, (MapType<?, ?>) dataType);
            case ARRAY:
                return reconvertArray((WrappedArray.ofRef) field, (ArrayType<?, ?>) dataType);
        }
        return super.reconvert(field, dataType);
    }

    private Map<Object, Object> reconvertMap(
            HashMap.HashTrieMap hashTrieMap, MapType<?, ?> mapType) {
        if (hashTrieMap == null || hashTrieMap.size() == 0) {
            return Collections.emptyMap();
        }
        Map<Object, Object> newMap = new LinkedHashMap<>(hashTrieMap.size());
        int num = hashTrieMap.size();
        SeaTunnelDataType<?> keyType = mapType.getKeyType();
        SeaTunnelDataType<?> valueType = mapType.getValueType();
        scala.collection.immutable.List<?> list = hashTrieMap.keySet().toList();
        for (int i = 0; i < num; i++) {
            Object key = list.apply(i);
            Object value = hashTrieMap.get(key);
            key = reconvert(key, keyType);
            value = reconvert(value, valueType);
            newMap.put(key, value);
        }
        return newMap;
    }

    private Object reconvertArray(WrappedArray.ofRef<?> arrayData, ArrayType<?, ?> arrayType) {
        if (arrayData == null || arrayData.size() == 0) {
            return Collections.emptyList().toArray();
        }
        Object[] newArray = new Object[arrayData.size()];
        for (int i = 0; i < arrayData.size(); i++) {
            newArray[i] = reconvert(arrayData.apply(i), arrayType.getElementType());
        }
        return newArray;
    }
}
