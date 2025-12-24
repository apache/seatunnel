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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HiveTypeConvertorTest {

    @Test
    void covertHiveTypeToSeaTunnelType() {
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> HiveTypeConvertor.covertHiveTypeToSeaTunnelType("test", "char"));
        assertEquals(
                "ErrorCode:[COMMON-16], ErrorDescription:['Hive' source unsupported convert type 'char' of 'test' to SeaTunnel data type.]",
                exception.getMessage());
    }

    @Test
    void convertHiveStructType() {
        SeaTunnelDataType<?> structType =
                HiveTypeConvertor.covertHiveTypeToSeaTunnelType(
                        "structType", "struct<country:String,city:String>");
        assertEquals(SqlType.ROW, structType.getSqlType());
        SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) structType;
        assertEquals(BasicType.STRING_TYPE, seaTunnelRowType.getFieldType(0));
        assertEquals(BasicType.STRING_TYPE, seaTunnelRowType.getFieldType(0));
    }

    @Test
    void testSeatunnelToHiveTypeConversion() {
        // Test basic types
        assertEquals("string", HiveTypeConvertor.seatunnelToHiveType(BasicType.STRING_TYPE));
        assertEquals("boolean", HiveTypeConvertor.seatunnelToHiveType(BasicType.BOOLEAN_TYPE));
        assertEquals("tinyint", HiveTypeConvertor.seatunnelToHiveType(BasicType.BYTE_TYPE));
        assertEquals("smallint", HiveTypeConvertor.seatunnelToHiveType(BasicType.SHORT_TYPE));
        assertEquals("int", HiveTypeConvertor.seatunnelToHiveType(BasicType.INT_TYPE));
        assertEquals("bigint", HiveTypeConvertor.seatunnelToHiveType(BasicType.LONG_TYPE));
        assertEquals("float", HiveTypeConvertor.seatunnelToHiveType(BasicType.FLOAT_TYPE));
        assertEquals("double", HiveTypeConvertor.seatunnelToHiveType(BasicType.DOUBLE_TYPE));

        // Test decimal type
        DecimalType decimalType = new DecimalType(10, 2);
        assertEquals("decimal(10,2)", HiveTypeConvertor.seatunnelToHiveType(decimalType));

        // Test time types
        assertEquals("date", HiveTypeConvertor.seatunnelToHiveType(LocalTimeType.LOCAL_DATE_TYPE));
        assertEquals(
                "string", HiveTypeConvertor.seatunnelToHiveType(LocalTimeType.LOCAL_TIME_TYPE));
        assertEquals(
                "timestamp",
                HiveTypeConvertor.seatunnelToHiveType(LocalTimeType.LOCAL_DATE_TIME_TYPE));
    }

    @Test
    void testSeatunnelToHiveTypeComplexTypes() {
        // ARRAY
        org.apache.seatunnel.api.table.type.ArrayType<Integer[], Integer> intArrayType =
                new org.apache.seatunnel.api.table.type.ArrayType<>(
                        Integer[].class, BasicType.INT_TYPE);
        assertEquals("array<int>", HiveTypeConvertor.seatunnelToHiveType(intArrayType));

        // MAP
        org.apache.seatunnel.api.table.type.MapType<String, Integer> mapType =
                new org.apache.seatunnel.api.table.type.MapType<>(
                        BasicType.STRING_TYPE, BasicType.INT_TYPE);
        assertEquals("map<string,int>", HiveTypeConvertor.seatunnelToHiveType(mapType));

        // ROW (struct)
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });
        assertEquals("struct<a:int,b:string>", HiveTypeConvertor.seatunnelToHiveType(rowType));

        // Nested: array<map<string,array<int>>>
        org.apache.seatunnel.api.table.type.ArrayType<Integer[], Integer> nestedArray =
                new org.apache.seatunnel.api.table.type.ArrayType<>(
                        Integer[].class, BasicType.INT_TYPE);
        org.apache.seatunnel.api.table.type.MapType<String, Integer[]> nestedMap =
                new org.apache.seatunnel.api.table.type.MapType<>(
                        BasicType.STRING_TYPE, nestedArray);
        org.apache.seatunnel.api.table.type.ArrayType<
                        java.util.Map<String, Integer[]>[], java.util.Map<String, Integer[]>>
                complexArray =
                        new org.apache.seatunnel.api.table.type.ArrayType<>(
                                (Class) java.util.Map[].class, nestedMap);
        assertEquals(
                "array<map<string,array<int>>>",
                HiveTypeConvertor.seatunnelToHiveType(complexArray));

        // Nested: struct<f1:array<int>,f2:map<string,string>>
        SeaTunnelRowType nestedRow =
                new SeaTunnelRowType(
                        new String[] {"f1", "f2"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType<?>[] {
                            intArrayType,
                            new org.apache.seatunnel.api.table.type.MapType<>(
                                    BasicType.STRING_TYPE, BasicType.STRING_TYPE)
                        });
        assertEquals(
                "struct<f1:array<int>,f2:map<string,string>>",
                HiveTypeConvertor.seatunnelToHiveType(nestedRow));
    }

    @Test
    void testArrayWithoutElementTypeThrows() {
        org.apache.seatunnel.api.table.type.ArrayType<int[], Integer> badArray =
                new org.apache.seatunnel.api.table.type.ArrayType<>((Class) int[].class, null);
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> HiveTypeConvertor.seatunnelToHiveType(badArray));
    }

    @Test
    void testMapWithoutKeyOrValueTypeThrows() {
        // null key -> MapType constructor throws NPE before conversion
        Assertions.assertThrows(
                NullPointerException.class,
                () -> new org.apache.seatunnel.api.table.type.MapType<>(null, BasicType.INT_TYPE));
        // null value -> MapType constructor throws NPE before conversion
        Assertions.assertThrows(
                NullPointerException.class,
                () ->
                        new org.apache.seatunnel.api.table.type.MapType<>(
                                BasicType.STRING_TYPE, null));
    }

    @Test
    void testRowWithEmptyFieldsThrows() {
        SeaTunnelRowType emptyRow =
                new SeaTunnelRowType(new String[] {}, new SeaTunnelDataType<?>[] {});
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> HiveTypeConvertor.seatunnelToHiveType(emptyRow));
    }

    @Test
    void testRowWithMismatchedFieldsThrows() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> {
                    SeaTunnelRowType badRow =
                            new SeaTunnelRowType(
                                    new String[] {"a", "b"},
                                    new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
                    HiveTypeConvertor.seatunnelToHiveType(badRow);
                });
    }
}
