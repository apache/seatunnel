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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Descriptors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SchemaRegistryAwareProtobufDeserializationSchemaTest {

    private static final String PROTO_CONTENT =
            "syntax = \"proto3\";\n"
                    + "\n"
                    + "package org.apache.seatunnel.format.protobuf;\n"
                    + "\n"
                    + "option java_outer_classname = \"TestProto\";\n"
                    + "\n"
                    + "message TestMessage {\n"
                    + "  int32 id = 1;\n"
                    + "  string name = 2;\n"
                    + "}";

    private static final String MESSAGE_NAME = "TestMessage";

    private CatalogTable createCatalogTable() {
        Map<String, String> options = new HashMap<>();
        options.put("protobuf_schema", PROTO_CONTENT);
        options.put("protobuf_message_name", MESSAGE_NAME);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType<?>[] {
                            org.apache.seatunnel.api.table.type.BasicType.INT_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE
                        });

        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test_table", rowType);
        catalogTable.getOptions().putAll(options);
        return catalogTable;
    }

    private byte[] createPlainProtobufMessage() throws Exception {
        Descriptors.Descriptor descriptor =
                CompileDescriptor.compileDescriptorTempFile(PROTO_CONTENT, MESSAGE_NAME);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType<?>[] {
                            org.apache.seatunnel.api.table.type.BasicType.INT_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE
                        });

        RowToProtobufConverter converter = new RowToProtobufConverter(rowType, descriptor);

        SeaTunnelRow row = new SeaTunnelRow(2);
        row.setField(0, 123);
        row.setField(1, "test");

        return converter.convertRowToGenericRecord(row);
    }

    private byte[] createSchemaRegistryMessage(byte[] plainMessage) {
        byte[] srMessage = new byte[6 + plainMessage.length];
        srMessage[0] = 0;
        srMessage[1] = 0;
        srMessage[2] = 0;
        srMessage[3] = 0;
        srMessage[4] = 1;
        srMessage[5] = 0;
        System.arraycopy(plainMessage, 0, srMessage, 6, plainMessage.length);
        return srMessage;
    }

    @Test
    void testDeserializeNullMessage() {
        CatalogTable catalogTable = createCatalogTable();
        SchemaRegistryAwareProtobufDeserializationSchema schema =
                new SchemaRegistryAwareProtobufDeserializationSchema(catalogTable);

        Assertions.assertThrows(NullPointerException.class, () -> schema.deserialize(null));
    }

    @Test
    void testDeserializeEmptyMessage() throws IOException {
        CatalogTable catalogTable = createCatalogTable();
        SchemaRegistryAwareProtobufDeserializationSchema schema =
                new SchemaRegistryAwareProtobufDeserializationSchema(catalogTable);

        // Empty message may return a row with default values
        SeaTunnelRow result = schema.deserialize(new byte[0]);
        // After fallback tries, the inner schema returns a row with default values
        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.getField(0));
        Assertions.assertEquals("", result.getField(1));
    }

    @Test
    void testDeserializeInvalidMessage() throws IOException {
        CatalogTable catalogTable = createCatalogTable();
        SchemaRegistryAwareProtobufDeserializationSchema schema =
                new SchemaRegistryAwareProtobufDeserializationSchema(catalogTable);

        // Invalid protobuf message without magic byte - should throw exception
        byte[] invalidMessage = new byte[] {0, 1, 2, 3, 4};

        Assertions.assertThrows(IOException.class, () -> schema.deserialize(invalidMessage));
    }

    @Test
    void testDeserializePlainProtobufMessage() throws Exception {
        CatalogTable catalogTable = createCatalogTable();
        SchemaRegistryAwareProtobufDeserializationSchema schema =
                new SchemaRegistryAwareProtobufDeserializationSchema(catalogTable);

        byte[] plainMessage = createPlainProtobufMessage();
        SeaTunnelRow result = schema.deserialize(plainMessage);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(123, result.getField(0));
        Assertions.assertEquals("test", result.getField(1));
    }

    @Test
    void testDeserializeSchemaRegistryMessage() throws Exception {
        CatalogTable catalogTable = createCatalogTable();
        SchemaRegistryAwareProtobufDeserializationSchema schema =
                new SchemaRegistryAwareProtobufDeserializationSchema(catalogTable);

        byte[] plainMessage = createPlainProtobufMessage();
        byte[] srMessage = createSchemaRegistryMessage(plainMessage);

        SeaTunnelRow result = schema.deserialize(srMessage);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(123, result.getField(0));
        Assertions.assertEquals("test", result.getField(1));
    }

    @Test
    void testDeserializeMessageWithMagicByteOnly() throws IOException {
        CatalogTable catalogTable = createCatalogTable();
        SchemaRegistryAwareProtobufDeserializationSchema schema =
                new SchemaRegistryAwareProtobufDeserializationSchema(catalogTable);

        // Message with magic byte but invalid protobuf content
        byte[] message = new byte[] {0, 1, 2, 3, 4, 5};

        // Should try to strip header, fail on all offsets, then fallback to original
        // Original message is also invalid, so throws exception
        Assertions.assertThrows(IOException.class, () -> schema.deserialize(message));
    }

    @Test
    void testDeserializeMessageWithoutMagicByte() throws Exception {
        CatalogTable catalogTable = createCatalogTable();
        SchemaRegistryAwareProtobufDeserializationSchema schema =
                new SchemaRegistryAwareProtobufDeserializationSchema(catalogTable);

        byte[] plainMessage = createPlainProtobufMessage();
        SeaTunnelRow result = schema.deserialize(plainMessage);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(123, result.getField(0));
    }
}
