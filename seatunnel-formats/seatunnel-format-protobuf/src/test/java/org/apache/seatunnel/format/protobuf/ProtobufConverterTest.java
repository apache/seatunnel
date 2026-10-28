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
import org.apache.seatunnel.format.protobuf.exception.ProtobufFormatErrorCode;
import org.apache.seatunnel.format.protobuf.exception.SeaTunnelProtobufFormatException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ProtobufConverterTest {

    /** Holds the schema and descriptor paths the argument matrix asserts on. */
    @TempDir Path tempDir;

    private SeaTunnelRow buildSeaTunnelRow() {
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(10);

        Map<String, Float> attributesMap = new HashMap<>();
        attributesMap.put("k1", 0.1F);
        attributesMap.put("k2", 2.3F);

        String[] phoneNumbers = {"1", "2"};
        byte[] byteVal = {1, 2, 3};

        SeaTunnelRow address = new SeaTunnelRow(3);
        address.setField(0, "city_value");
        address.setField(1, "state_value");
        address.setField(2, "street_value");

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
                        new String[] {"city", "state", "street"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
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

    /** Guards that the compiled descriptor keeps the typed presence declared in a proto2 schema. */
    @Test
    public void testProto2RequiredAndOptionalFieldPresence()
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        String protoContent =
                "syntax = \"proto2\";\n"
                        + "\n"
                        + "message Order {\n"
                        + "  required int32 id = 1;\n"
                        + "  optional string note = 2;\n"
                        + "  repeated string tags = 3;\n"
                        + "}";

        Descriptors.Descriptor descriptor =
                CompileDescriptor.compileDescriptorTempFile(protoContent, "Order");

        Descriptors.FieldDescriptor idField = descriptor.findFieldByName("id");
        Assertions.assertTrue(idField.isRequired(), "proto2 required field stays required");
        Assertions.assertTrue(idField.hasPresence(), "proto2 field tracks presence");
        Assertions.assertFalse(idField.isRepeated());

        Descriptors.FieldDescriptor noteField = descriptor.findFieldByName("note");
        Assertions.assertTrue(noteField.isOptional());
        Assertions.assertFalse(noteField.isRequired());
        Assertions.assertTrue(noteField.hasPresence(), "proto2 optional scalar has presence");

        Descriptors.FieldDescriptor tagsField = descriptor.findFieldByName("tags");
        Assertions.assertTrue(tagsField.isRepeated());
        Assertions.assertFalse(tagsField.hasPresence(), "repeated field has no presence");
    }

    /** Guards the exact proto3 wire bytes and the row -> bytes -> row round trip. */
    @Test
    public void testProto3SimpleMessageWireBytesAndRoundTrip()
            throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        String protoContent =
                "syntax = \"proto3\";\n" + "\n" + "message Simple {\n" + "  int32 id = 1;\n" + "}";

        String messageName = "Simple";
        Descriptors.Descriptor descriptor =
                CompileDescriptor.compileDescriptorTempFile(protoContent, messageName);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});

        SeaTunnelRow originalRow = new SeaTunnelRow(1);
        originalRow.setField(0, 7);

        RowToProtobufConverter rowToProtobufConverter =
                new RowToProtobufConverter(rowType, descriptor);
        byte[] protobufMessage = rowToProtobufConverter.convertRowToGenericRecord(originalRow);

        // field 1 as varint: tag 0x08 followed by the value 0x07
        Assertions.assertArrayEquals(new byte[] {8, 7}, protobufMessage);

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, protobufMessage);
        ProtobufToRowConverter protobufToRowConverter =
                new ProtobufToRowConverter(protoContent, messageName);
        SeaTunnelRow convertedRow =
                protobufToRowConverter.converter(descriptor, dynamicMessage, rowType);

        Assertions.assertEquals(originalRow, convertedRow);
    }

    /** Guards that a malformed schema keeps the existing DESCRIPTOR_CONVERT_FAILED contract. */
    @Test
    public void testInvalidProtoSchemaKeepsDescriptorConvertFailedContract() {
        String badProto =
                "syntax = \"proto3\";\n" + "\n" + "message Broken {\n" + "  int32 id = 1\n" + "}";
        String messageName = "Broken";

        SeaTunnelProtobufFormatException exception =
                Assertions.assertThrows(
                        SeaTunnelProtobufFormatException.class,
                        () -> CompileDescriptor.compileDescriptorTempFile(badProto, messageName));

        Assertions.assertEquals(
                ProtobufFormatErrorCode.DESCRIPTOR_CONVERT_FAILED,
                exception.getSeaTunnelErrorCode());
        Assertions.assertEquals("PROTOBUF-01", exception.getSeaTunnelErrorCode().getCode());
        Assertions.assertTrue(
                exception.getMessage().contains("Protoc compile error, exit code: "),
                exception.getMessage());
    }

    /**
     * Guards the compiler selection of the new macOS ARM64 branch on the hosts CI actually runs on.
     * The matrix asserts the complete argument list, so every other platform must stay on the
     * previous three arguments in the previous order. No protoc process is started.
     */
    @ParameterizedTest(name = "os.name={0}, os.arch={1}, macOS ARM64={2}")
    @CsvSource({
        "Mac OS X, aarch64, true",
        "Mac OS X, arm64, true",
        "macOSX, AARCH64, true",
        "Mac OS X, x86_64, false",
        "Linux, x86_64, false",
        "Linux, aarch64, false",
        "Windows 11, amd64, false",
        "Unknown OS, '', false",
        "'', '', false"
    })
    void testBuildProtocArgsSelectsMacOsArm64Compiler(
            String osName, String osArch, boolean macOsArm64) {
        File protoFile = tempDir.resolve("platform.proto").toFile();
        String targetDesc = tempDir.resolve("platform.desc").toString();

        List<String> expected = new ArrayList<>();
        if (macOsArm64) {
            expected.add("-v3.25.3");
        }
        expected.add("--proto_path=" + protoFile.getParent());
        expected.add("--descriptor_set_out=" + targetDesc);
        expected.add(protoFile.getPath());

        Assertions.assertArrayEquals(
                expected.toArray(new String[0]),
                CompileDescriptor.buildProtocArgs(protoFile, targetDesc, osName, osArch));
    }
}
