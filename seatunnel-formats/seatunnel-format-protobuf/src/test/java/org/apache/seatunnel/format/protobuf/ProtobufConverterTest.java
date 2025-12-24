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
}
