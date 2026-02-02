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

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SeaTunnelDataTypeConvertorUtilTest {

    @Test
    void testParseWithUnsupportedType() {
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test", "MULTIPLE_ROW"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:['SeaTunnel' unsupported data type 'MULTIPLE_ROW' of 'test']",
                exception.getMessage());

        SeaTunnelRuntimeException exception2 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test", "map<string, MULTIPLE_ROW>"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:['SeaTunnel' unsupported data type 'MULTIPLE_ROW' of 'test']",
                exception2.getMessage());

        SeaTunnelRuntimeException exception3 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test", "array<MULTIPLE_ROW>"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:['SeaTunnel' unsupported data type 'MULTIPLE_ROW' of 'test']",
                exception3.getMessage());

        SeaTunnelRuntimeException exception4 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test", "uuid"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:['SeaTunnel' unsupported data type 'uuid' of 'test']",
                exception4.getMessage());

        IllegalArgumentException exception5 =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test", "{uuid}"));
        String expectedMsg5 =
                String.format("HOCON Config parse from %s failed.", "{conf = {uuid}}");
        Assertions.assertEquals(expectedMsg5, exception5.getMessage());

        String invalidTypeDeclaration = "[e]";
        IllegalArgumentException exception6 =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                        "test",
                                        String.format("{c_0 = %s}", invalidTypeDeclaration)));
        String expectedMsg6 =
                String.format(
                        "Unsupported parse SeaTunnel Type from '%s'.", invalidTypeDeclaration);
        Assertions.assertEquals(expectedMsg6, exception6.getMessage());
    }

    @Test
    public void testCompatibleTypeDeclare() {
        SeaTunnelDataType<?> longType =
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_long", "long");
        Assertions.assertEquals(BasicType.LONG_TYPE, longType);

        SeaTunnelDataType<?> shortType =
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_short", "short");
        Assertions.assertEquals(BasicType.SHORT_TYPE, shortType);

        SeaTunnelDataType<?> byteType =
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_byte", "byte");
        Assertions.assertEquals(BasicType.BYTE_TYPE, byteType);

        ArrayType<?, ?> longArrayType =
                (ArrayType<?, ?>)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_long_array", "array<long>");
        Assertions.assertEquals(ArrayType.LONG_ARRAY_TYPE, longArrayType);

        ArrayType<?, ?> shortArrayType =
                (ArrayType<?, ?>)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_short_array", "array<short>");
        Assertions.assertEquals(ArrayType.SHORT_ARRAY_TYPE, shortArrayType);

        ArrayType<?, ?> byteArrayType =
                (ArrayType<?, ?>)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_byte_array", "array<byte>");
        Assertions.assertEquals(ArrayType.BYTE_ARRAY_TYPE, byteArrayType);

        MapType<?, ?> longMapType =
                (MapType<?, ?>)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_long_map", "map<long, long>");
        Assertions.assertEquals(BasicType.LONG_TYPE, longMapType.getKeyType());
        Assertions.assertEquals(BasicType.LONG_TYPE, longMapType.getValueType());

        MapType<?, ?> shortMapType =
                (MapType<?, ?>)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_short_map", "map<short, short>");
        Assertions.assertEquals(BasicType.SHORT_TYPE, shortMapType.getKeyType());
        Assertions.assertEquals(BasicType.SHORT_TYPE, shortMapType.getValueType());

        MapType<?, ?> byteMapType =
                (MapType<?, ?>)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_byte_map", "map<byte, byte>");
        Assertions.assertEquals(BasicType.BYTE_TYPE, byteMapType.getKeyType());
        Assertions.assertEquals(BasicType.BYTE_TYPE, byteMapType.getValueType());

        SeaTunnelRowType longRow =
                (SeaTunnelRowType)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_long_row", "{c = long}");
        Assertions.assertEquals(BasicType.LONG_TYPE, longRow.getFieldType(0));

        SeaTunnelRowType shortRow =
                (SeaTunnelRowType)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_short_row", "{c = short}");
        Assertions.assertEquals(BasicType.SHORT_TYPE, shortRow.getFieldType(0));

        SeaTunnelRowType byteRow =
                (SeaTunnelRowType)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_byte_row", "{c = byte}");
        Assertions.assertEquals(BasicType.BYTE_TYPE, byteRow.getFieldType(0));
    }

    @Test
    public void testAllSupportedTypes() {
        // Test basic types
        Assertions.assertEquals(
                BasicType.STRING_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_string", "string"));
        Assertions.assertEquals(
                BasicType.BOOLEAN_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_boolean", "boolean"));
        Assertions.assertEquals(
                BasicType.BYTE_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_tinyint", "tinyint"));
        Assertions.assertEquals(
                PrimitiveByteArrayType.INSTANCE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_bytes", "bytes"));
        Assertions.assertEquals(
                BasicType.SHORT_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_smallint", "smallint"));
        Assertions.assertEquals(
                BasicType.INT_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_int", "int"));
        Assertions.assertEquals(
                BasicType.LONG_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_bigint", "bigint"));
        Assertions.assertEquals(
                BasicType.FLOAT_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_float", "float"));
        Assertions.assertEquals(
                BasicType.DOUBLE_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_double", "double"));
        Assertions.assertEquals(
                BasicType.VOID_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_null", "null"));

        // Test datetime types
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_date", "date"));
        Assertions.assertEquals(
                LocalTimeType.LOCAL_TIME_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType("c_time", "time"));
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_timestamp", "timestamp"));
        Assertions.assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_timestamp_tz", "timestamp_tz"));

        // Test vector types
        Assertions.assertEquals(
                VectorType.VECTOR_BINARY_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_binary_vector", "binary_vector"));
        Assertions.assertEquals(
                VectorType.VECTOR_FLOAT_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_float_vector", "float_vector"));
        Assertions.assertEquals(
                VectorType.VECTOR_FLOAT16_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_float16_vector", "float16_vector"));
        Assertions.assertEquals(
                VectorType.VECTOR_BFLOAT16_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_bfloat16_vector", "bfloat16_vector"));
        Assertions.assertEquals(
                VectorType.VECTOR_SPARSE_FLOAT_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_sparse_float_vector", "sparse_float_vector"));

        // Test complex types - Array
        Assertions.assertEquals(
                ArrayType.STRING_ARRAY_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_string_array", "array<string>"));
        Assertions.assertEquals(
                ArrayType.BOOLEAN_ARRAY_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_boolean_array", "array<boolean>"));
        Assertions.assertEquals(
                ArrayType.INT_ARRAY_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_int_array", "array<int>"));
        Assertions.assertEquals(
                ArrayType.LONG_ARRAY_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_bigint_array", "array<bigint>"));
        Assertions.assertEquals(
                ArrayType.FLOAT_ARRAY_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_float_array", "array<float>"));
        Assertions.assertEquals(
                ArrayType.DOUBLE_ARRAY_TYPE,
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "c_double_array", "array<double>"));

        // Test complex types - Map
        MapType<?, ?> stringMapType =
                (MapType<?, ?>)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_string_map", "map<string, string>");
        Assertions.assertEquals(BasicType.STRING_TYPE, stringMapType.getKeyType());
        Assertions.assertEquals(BasicType.STRING_TYPE, stringMapType.getValueType());

        MapType<?, ?> intStringMapType =
                (MapType<?, ?>)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_int_string_map", "map<int, string>");
        Assertions.assertEquals(BasicType.INT_TYPE, intStringMapType.getKeyType());
        Assertions.assertEquals(BasicType.STRING_TYPE, intStringMapType.getValueType());

        // Test complex types - Decimal
        DecimalType decimalType =
                (DecimalType)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_decimal", "decimal(10,2)");
        Assertions.assertEquals(10, decimalType.getPrecision());
        Assertions.assertEquals(2, decimalType.getScale());

        // Test complex types - Row
        SeaTunnelRowType rowType =
                (SeaTunnelRowType)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_row", "{c1 = string, c2 = int, c3 = boolean}");
        Assertions.assertEquals(3, rowType.getFieldNames().length);
        Assertions.assertEquals(3, rowType.getFieldTypes().length);
        Assertions.assertEquals("c1", rowType.getFieldName(0));
        Assertions.assertEquals(BasicType.STRING_TYPE, rowType.getFieldType(0));
        Assertions.assertEquals("c2", rowType.getFieldName(1));
        Assertions.assertEquals(BasicType.INT_TYPE, rowType.getFieldType(1));
        Assertions.assertEquals("c3", rowType.getFieldName(2));
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, rowType.getFieldType(2));

        // Test nested complex types
        ArrayType<?, ?> arrayOfArrayType =
                (ArrayType<?, ?>)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_array_array", "array<array<int>>");
        Assertions.assertEquals(ArrayType.INT_ARRAY_TYPE, arrayOfArrayType.getElementType());

        MapType<?, ?> mapOfArrayType =
                (MapType<?, ?>)
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                                "c_map_array", "map<string, array<int>>");
        Assertions.assertEquals(BasicType.STRING_TYPE, mapOfArrayType.getKeyType());
        Assertions.assertEquals(ArrayType.INT_ARRAY_TYPE, mapOfArrayType.getValueType());
    }
}
