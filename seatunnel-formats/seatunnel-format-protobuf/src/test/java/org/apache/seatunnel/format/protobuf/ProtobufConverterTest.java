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
package org.apache.seatunnel.format.protobuf;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class ProtobufConverterTest {

    private SeaTunnelRow buildSeaTunnelRow() {
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(10);

        Map<String, Float> attributesMap = new HashMap<>();
        attributesMap.put("k1", 0.1F);
        attributesMap.put("k2", 2.3F);

        String[] phoneNumbers = {"1", "2"};
        byte[] byteVal = {1, 2, 3};

        SeaTunnelRow address = new SeaTunnelRow(4);
        address.setField(0, "street_value");
        address.setField(1, "city_value");
        address.setField(2, "state_value");
        address.setField(3, "zip_value");

        seaTunnelRow.setField(0, 123);
        seaTunnelRow.setField(1, 123123123123L);
        seaTunnelRow.setField(2, 0.123f);
        seaTunnelRow.setField(3, 0.123d);
        seaTunnelRow.setField(4, false);
        seaTunnelRow.setField(5, "test data");
        seaTunnelRow.setField(6, byteVal);
        seaTunnelRow.setField(7, address);
        seaTunnelRow.setField(8, attributesMap);
        seaTunnelRow.setField(9, phoneNumbers);

        return seaTunnelRow;
    }

    private SeaTunnelRowType buildSeaTunnelRowType() {
        SeaTunnelRowType addressType =
                new SeaTunnelRowType(
                        new String[] {"street", "city", "state", "zip"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE
                        });

        return new SeaTunnelRowType(
                new String[] {
                    "c_int32",
                    "c_int64",
                    "c_float",
                    "c_double",
                    "c_bool",
                    "c_string",
                    "c_bytes",
                    "Address",
                    "attributes",
                    "phone_numbers"
                },
                new SeaTunnelDataType<?>[] {
                    BasicType.INT_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.FLOAT_TYPE,
                    BasicType.DOUBLE_TYPE,
                    BasicType.BOOLEAN_TYPE,
                    BasicType.STRING_TYPE,
                    PrimitiveByteArrayType.INSTANCE,
                    addressType,
                    new MapType<>(BasicType.STRING_TYPE, BasicType.FLOAT_TYPE),
                    ArrayType.STRING_ARRAY_TYPE
                });
    }

    @Test
    public void testConverter()
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        SeaTunnelRow originalRow = buildSeaTunnelRow();

        String protoContent =
                "syntax = \"proto3\";\n"
                        + "\n"
                        + "package org.apache.seatunnel.format.protobuf;\n"
                        + "\n"
                        + "option java_outer_classname = \"ProtobufE2E\";\n"
                        + "\n"
                        + "message Person {\n"
                        + "  int32 c_int32 = 1;\n"
                        + "  int64 c_int64 = 2;\n"
                        + "  float c_float = 3;\n"
                        + "  double c_double = 4;\n"
                        + "  bool c_bool = 5;\n"
                        + "  string c_string = 6;\n"
                        + "  bytes c_bytes = 7;\n"
                        + "\n"
                        + "  message Address {\n"
                        + "    string street = 1;\n"
                        + "    string city = 2;\n"
                        + "    string state = 3;\n"
                        + "    string zip = 4;\n"
                        + "  }\n"
                        + "\n"
                        + "  Address address = 8;\n"
                        + "\n"
                        + "  map<string, float> attributes = 9;\n"
                        + "\n"
                        + "  repeated string phone_numbers = 10;\n"
                        + "}";

        String messageName = "Person";
        Descriptors.Descriptor descriptor =
                CompileDescriptor.compileDescriptorTempFile(protoContent, messageName);

        RowToProtobufConverter rowToProtobufConverter =
                new RowToProtobufConverter(rowType, descriptor);
        byte[] protobufMessage = rowToProtobufConverter.convertRowToGenericRecord(originalRow);

        ProtobufToRowConverter protobufToRowConverter =
                new ProtobufToRowConverter(protoContent, messageName);
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, protobufMessage);
        SeaTunnelRow convertedRow =
                protobufToRowConverter.converter(descriptor, dynamicMessage, rowType);

        Assertions.assertEquals(originalRow, convertedRow);
    }

    @Test
    public void testFieldNameCaseSensitive()
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        // Test that field names are case-sensitive and not converted to lowercase
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"MyIntField", "CamelCaseString", "snake_case_field"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });

        SeaTunnelRow row = new SeaTunnelRow(3);
        row.setField(0, 100);
        row.setField(1, "test");
        row.setField(2, "value");

        String protoContent =
                "syntax = \"proto3\";\n"
                        + "package org.apache.seatunnel.format.protobuf;\n"
                        + "message TestMessage {\n"
                        + "  int32 MyIntField = 1;\n"
                        + "  string CamelCaseString = 2;\n"
                        + "  string snake_case_field = 3;\n"
                        + "}";

        String messageName = "TestMessage";
        Descriptors.Descriptor descriptor =
                CompileDescriptor.compileDescriptorTempFile(protoContent, messageName);

        RowToProtobufConverter converter = new RowToProtobufConverter(rowType, descriptor);
        byte[] protobufMessage = converter.convertRowToGenericRecord(row);

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, protobufMessage);
        Assertions.assertEquals(
                100, dynamicMessage.getField(descriptor.findFieldByName("MyIntField")));
        Assertions.assertEquals(
                "test", dynamicMessage.getField(descriptor.findFieldByName("CamelCaseString")));
        Assertions.assertEquals(
                "value", dynamicMessage.getField(descriptor.findFieldByName("snake_case_field")));
    }

    @Test
    public void testNullFieldHandling()
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        // Test that null fields are handled correctly
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"field1", "field2", "field3"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.BOOLEAN_TYPE
                        });

        SeaTunnelRow row = new SeaTunnelRow(3);
        row.setField(0, 42);
        row.setField(1, null); // null value
        row.setField(2, true);

        String protoContent =
                "syntax = \"proto3\";\n"
                        + "package org.apache.seatunnel.format.protobuf;\n"
                        + "message TestMessage {\n"
                        + "  int32 field1 = 1;\n"
                        + "  string field2 = 2;\n"
                        + "  bool field3 = 3;\n"
                        + "}";

        String messageName = "TestMessage";
        Descriptors.Descriptor descriptor =
                CompileDescriptor.compileDescriptorTempFile(protoContent, messageName);

        RowToProtobufConverter converter = new RowToProtobufConverter(rowType, descriptor);
        byte[] protobufMessage = converter.convertRowToGenericRecord(row);

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, protobufMessage);
        Assertions.assertEquals(42, dynamicMessage.getField(descriptor.findFieldByName("field1")));
        // In proto3, null string fields default to empty string
        Assertions.assertEquals("", dynamicMessage.getField(descriptor.findFieldByName("field2")));
        Assertions.assertEquals(
                true, dynamicMessage.getField(descriptor.findFieldByName("field3")));
    }

    @Test
    public void testNestedRowTypeWithCaseSensitiveFields()
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        // Test nested row type with case-sensitive field names
        SeaTunnelRowType nestedType =
                new SeaTunnelRowType(
                        new String[] {"NestedField", "AnotherField"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"TopLevelField", "myNestedObject"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, nestedType});

        SeaTunnelRow nestedRow = new SeaTunnelRow(2);
        nestedRow.setField(0, "nested_value");
        nestedRow.setField(1, 999);

        SeaTunnelRow row = new SeaTunnelRow(2);
        row.setField(0, "top_value");
        row.setField(1, nestedRow);

        String protoContent =
                "syntax = \"proto3\";\n"
                        + "package org.apache.seatunnel.format.protobuf;\n"
                        + "message TestMessage {\n"
                        + "  string TopLevelField = 1;\n"
                        + "  message MyNestedObject {\n"
                        + "    string NestedField = 1;\n"
                        + "    int32 AnotherField = 2;\n"
                        + "  }\n"
                        + "  MyNestedObject myNestedObject = 2;\n"
                        + "}";

        String messageName = "TestMessage";
        Descriptors.Descriptor descriptor =
                CompileDescriptor.compileDescriptorTempFile(protoContent, messageName);

        RowToProtobufConverter converter = new RowToProtobufConverter(rowType, descriptor);
        byte[] protobufMessage = converter.convertRowToGenericRecord(row);

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, protobufMessage);
        Assertions.assertEquals(
                "top_value", dynamicMessage.getField(descriptor.findFieldByName("TopLevelField")));

        DynamicMessage nestedMessage =
                (DynamicMessage)
                        dynamicMessage.getField(descriptor.findFieldByName("myNestedObject"));
        Descriptors.Descriptor nestedDescriptor = descriptor.findNestedTypeByName("MyNestedObject");
        Assertions.assertEquals(
                "nested_value",
                nestedMessage.getField(nestedDescriptor.findFieldByName("NestedField")));
        Assertions.assertEquals(
                999, nestedMessage.getField(nestedDescriptor.findFieldByName("AnotherField")));
    }

    @Test
    public void testMapTypeWithCaseSensitiveFieldName()
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        // Test map type with case-sensitive field name
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"MyMapField"},
                        new SeaTunnelDataType<?>[] {
                            new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE)
                        });

        Map<String, Integer> mapData = new HashMap<>();
        mapData.put("key1", 100);
        mapData.put("key2", 200);

        SeaTunnelRow row = new SeaTunnelRow(1);
        row.setField(0, mapData);

        String protoContent =
                "syntax = \"proto3\";\n"
                        + "package org.apache.seatunnel.format.protobuf;\n"
                        + "message TestMessage {\n"
                        + "  map<string, int32> MyMapField = 1;\n"
                        + "}";

        String messageName = "TestMessage";
        Descriptors.Descriptor descriptor =
                CompileDescriptor.compileDescriptorTempFile(protoContent, messageName);

        RowToProtobufConverter converter = new RowToProtobufConverter(rowType, descriptor);
        byte[] protobufMessage = converter.convertRowToGenericRecord(row);

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, protobufMessage);
        @SuppressWarnings("unchecked")
        java.util.List<DynamicMessage> mapEntries =
                (java.util.List<DynamicMessage>)
                        dynamicMessage.getField(descriptor.findFieldByName("MyMapField"));
        Assertions.assertEquals(2, mapEntries.size());
    }

    @Test
    public void testTinyIntUnsignedConversion()
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        // Test TINYINT to unsigned int conversion
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"tinyint_field"},
                        new SeaTunnelDataType<?>[] {BasicType.BYTE_TYPE});

        SeaTunnelRow row = new SeaTunnelRow(1);
        row.setField(0, (byte) -1); // -1 as signed byte should become 255 as unsigned

        String protoContent =
                "syntax = \"proto3\";\n"
                        + "package org.apache.seatunnel.format.protobuf;\n"
                        + "message TestMessage {\n"
                        + "  int32 tinyint_field = 1;\n"
                        + "}";

        String messageName = "TestMessage";
        Descriptors.Descriptor descriptor =
                CompileDescriptor.compileDescriptorTempFile(protoContent, messageName);

        RowToProtobufConverter converter = new RowToProtobufConverter(rowType, descriptor);
        byte[] protobufMessage = converter.convertRowToGenericRecord(row);

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, protobufMessage);
        Assertions.assertEquals(
                255, dynamicMessage.getField(descriptor.findFieldByName("tinyint_field")));
    }

    @Test
    public void testAllNullFields()
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        // Test when all fields are null
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"field1", "field2", "field3"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE, BasicType.INT_TYPE, BasicType.DOUBLE_TYPE
                        });

        SeaTunnelRow row = new SeaTunnelRow(3);
        row.setField(0, null);
        row.setField(1, null);
        row.setField(2, null);

        String protoContent =
                "syntax = \"proto3\";\n"
                        + "package org.apache.seatunnel.format.protobuf;\n"
                        + "message TestMessage {\n"
                        + "  string field1 = 1;\n"
                        + "  int32 field2 = 2;\n"
                        + "  double field3 = 3;\n"
                        + "}";

        String messageName = "TestMessage";
        Descriptors.Descriptor descriptor =
                CompileDescriptor.compileDescriptorTempFile(protoContent, messageName);

        RowToProtobufConverter converter = new RowToProtobufConverter(rowType, descriptor);
        byte[] protobufMessage = converter.convertRowToGenericRecord(row);

        // Should not throw exception, and produce valid protobuf message with default values
        Assertions.assertNotNull(protobufMessage);
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, protobufMessage);
        Assertions.assertNotNull(dynamicMessage);
    }
}
