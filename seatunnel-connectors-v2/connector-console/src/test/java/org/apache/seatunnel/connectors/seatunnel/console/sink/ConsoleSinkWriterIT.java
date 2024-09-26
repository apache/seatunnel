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

package org.apache.seatunnel.connectors.seatunnel.console.sink;

import org.apache.seatunnel.api.common.metrics.CycleMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.ReflectionUtils;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

public class ConsoleSinkWriterIT {

    private ConsoleSinkWriter consoleSinkWriter;

    @BeforeEach
    void setUp() {
        String[] fieldNames = {};
        SeaTunnelDataType<?>[] fieldTypes = {};
        SeaTunnelRowType seaTunnelRowType = new SeaTunnelRowType(fieldNames, fieldTypes);
        consoleSinkWriter = new ConsoleSinkWriter(seaTunnelRowType, new DefaultSinkWriterContext(Integer.MAX_VALUE, 1), true, 0);
    }

    private Object fieldToStringTest(SeaTunnelDataType<?> dataType, Object value) {
        Optional<Method> fieldToString =
                ReflectionUtils.getDeclaredMethod(
                        ConsoleSinkWriter.class,
                        "fieldToString",
                        SeaTunnelDataType.class,
                        Object.class);
        Method method =
                fieldToString.orElseThrow(
                        () -> new RuntimeException("method fieldToString not found"));
        try {
            return method.invoke(consoleSinkWriter, dataType, value);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void arrayIntTest() {
        Assertions.assertDoesNotThrow(
                () -> {
                    Integer[] integerArr = {1};
                    Object integerArrString =
                            fieldToStringTest(ArrayType.INT_ARRAY_TYPE, integerArr);
                    Assertions.assertEquals(integerArrString, "[1]");
                    int[] intArr = {1, 2};
                    Object intArrString = fieldToStringTest(ArrayType.INT_ARRAY_TYPE, intArr);
                    Assertions.assertEquals(intArrString, "[1, 2]");
                });
    }

    @Test
    void stringTest() {
        Assertions.assertDoesNotThrow(
                () -> {
                    String str = RandomStringUtils.randomAlphanumeric(10);
                    Object obj = fieldToStringTest(BasicType.STRING_TYPE, str);
                    Assertions.assertTrue(obj instanceof String);
                    Assertions.assertEquals(10, ((String) obj).length());
                });
    }

    @Test
    void hashMapTest() {
        Assertions.assertDoesNotThrow(
                () -> {
                    HashMap<Object, Object> map = new HashMap<>();
                    map.put("key", "value");
                    MapType<String, String> mapType =
                            new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
                    Object mapString = fieldToStringTest(mapType, map);
                    Assertions.assertNotNull(mapString);
                    Assertions.assertEquals("{\"key\":\"value\"}", mapString);
                });
    }

    @Test
    void rowTypeTest() {
        Assertions.assertDoesNotThrow(
                () -> {
                    String[] fieldNames = {"c_byte", "c_array", "bytes"};
                    SeaTunnelDataType<?>[] fieldTypes = {
                            BasicType.BYTE_TYPE,
                            ArrayType.BYTE_ARRAY_TYPE,
                            PrimitiveByteArrayType.INSTANCE
                    };
                    SeaTunnelRowType seaTunnelRowType =
                            new SeaTunnelRowType(fieldNames, fieldTypes);
                    byte[] bytes = RandomUtils.nextBytes(10);
                    Object[] rowData = {(byte) 1, bytes, bytes};
                    SeaTunnelRow seaTunnelRow = new SeaTunnelRow(rowData);
                    Object rowString = fieldToStringTest(seaTunnelRowType, seaTunnelRow);
                    Assertions.assertNotNull(rowString);
                    Assertions.assertEquals(
                            String.format(
                                    "[1, %s, %s]", Arrays.toString(bytes), Arrays.toString(bytes)),
                            rowString.toString());
                });
    }
}
