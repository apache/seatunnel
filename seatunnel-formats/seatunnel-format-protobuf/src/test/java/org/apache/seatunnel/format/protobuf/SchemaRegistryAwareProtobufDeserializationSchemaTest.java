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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.google.protobuf.Descriptors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
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

    private static final String EMPTY_PROTO_CONTENT =
            "syntax = \"proto3\";\n"
                    + "package org.apache.seatunnel.format.protobuf;\n"
                    + "message EmptyMessage {}";

    private static final String EMPTY_MESSAGE_NAME = "EmptyMessage";

    private static final byte[] OPTIMIZED_FIRST_MESSAGE_INDEX = {0};

    private SchemaRegistryAwareProtobufDeserializationSchema schema;
    private SchemaRegistryAwareProtobufDeserializationSchema emptyMessageSchema;

    @BeforeAll
    void setUpSchemas() {
        schema = new SchemaRegistryAwareProtobufDeserializationSchema(createCatalogTable());
        emptyMessageSchema =
                new SchemaRegistryAwareProtobufDeserializationSchema(
                        createEmptyMessageCatalogTable());
    }

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

    private CatalogTable createEmptyMessageCatalogTable() {
        Map<String, String> options = new HashMap<>();
        options.put("protobuf_schema", EMPTY_PROTO_CONTENT);
        options.put("protobuf_message_name", EMPTY_MESSAGE_NAME);

        SeaTunnelRowType rowType = new SeaTunnelRowType(new String[0], new SeaTunnelDataType<?>[0]);
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("empty_table", rowType);
        catalogTable.getOptions().putAll(options);
        return catalogTable;
    }

    private byte[] createPlainProtobufMessage() throws Exception {
        return createPlainProtobufMessage(123, "test");
    }

    private byte[] createPlainProtobufMessage(int id, String name) throws Exception {
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
        row.setField(0, id);
        row.setField(1, name);

        return converter.convertRowToGenericRecord(row);
    }

    private byte[] createSchemaRegistryMessage(byte[] plainMessage) {
        return createSchemaRegistryMessage(OPTIMIZED_FIRST_MESSAGE_INDEX, plainMessage);
    }

    private byte[] createSchemaRegistryMessage(byte[] messageIndexes, byte[] plainMessage) {
        byte[] srMessage = new byte[5 + messageIndexes.length + plainMessage.length];
        srMessage[0] = 0;
        srMessage[1] = 0;
        srMessage[2] = 0;
        srMessage[3] = 0;
        srMessage[4] = 1;
        System.arraycopy(messageIndexes, 0, srMessage, 5, messageIndexes.length);
        System.arraycopy(
                plainMessage, 0, srMessage, 5 + messageIndexes.length, plainMessage.length);
        return srMessage;
    }

    private void assertInvalidPayload(byte[] message) {
        IOException exception =
                Assertions.assertThrows(IOException.class, () -> schema.deserialize(message));

        Assertions.assertTrue(exception.getMessage().contains("invalid tag"));
    }

    @Test
    void throwsWhenMessageIsNull() {
        Assertions.assertThrows(NullPointerException.class, () -> schema.deserialize(null));
    }

    @Test
    void shouldDeserializePlainEmptyPayloadWithDefaultValues() throws IOException {
        SeaTunnelRow result = schema.deserialize(new byte[0]);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.getField(0));
        Assertions.assertEquals("", result.getField(1));
    }

    @Test
    void shouldRejectInvalidPlainPayload() {
        assertInvalidPayload(new byte[] {0, 1, 2, 3, 4});
    }

    @Test
    void shouldDeserializePlainProtobufMessage() throws Exception {
        byte[] plainMessage = createPlainProtobufMessage();
        SeaTunnelRow result = schema.deserialize(plainMessage);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(123, result.getField(0));
        Assertions.assertEquals("test", result.getField(1));
    }

    @Test
    void shouldDeserializeSchemaRegistryMessage() throws Exception {
        byte[] plainMessage = createPlainProtobufMessage();
        byte[] srMessage = createSchemaRegistryMessage(plainMessage);

        SeaTunnelRow result = schema.deserialize(srMessage);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(123, result.getField(0));
        Assertions.assertEquals("test", result.getField(1));
    }

    @Test
    void shouldDeserializeSchemaRegistryMessageWithDefaultValuedPayload() throws Exception {
        byte[] defaultValuedMessage = createPlainProtobufMessage(0, "");

        Assertions.assertEquals(0, defaultValuedMessage.length);
        SeaTunnelRow result = schema.deserialize(createSchemaRegistryMessage(defaultValuedMessage));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.getField(0));
        Assertions.assertEquals("", result.getField(1));
    }

    @Test
    void shouldDeserializeSchemaRegistryEmptyMessage() throws IOException {
        SeaTunnelRow result =
                emptyMessageSchema.deserialize(createSchemaRegistryMessage(new byte[0]));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.getArity());
    }

    @Test
    void shouldDeserializeEmptyPayloadWithExplicitSingleIndex() throws IOException {
        SeaTunnelRow result =
                schema.deserialize(createSchemaRegistryMessage(new byte[] {2, 0}, new byte[0]));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.getField(0));
        Assertions.assertEquals("", result.getField(1));
    }

    @Test
    void shouldDeserializeNestedMessageIndexPayload() throws Exception {
        byte[] nestedMessageIndexes = {6, 0, 4, 2};

        SeaTunnelRow result =
                schema.deserialize(
                        createSchemaRegistryMessage(
                                nestedMessageIndexes, createPlainProtobufMessage()));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(123, result.getField(0));
        Assertions.assertEquals("test", result.getField(1));
    }

    @Test
    void shouldPreserveLegacyNonEmptyHeaderCompatibility() throws Exception {
        SeaTunnelRow result =
                schema.deserialize(
                        createSchemaRegistryMessage(new byte[] {1}, createPlainProtobufMessage()));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(123, result.getField(0));
        Assertions.assertEquals("test", result.getField(1));
    }

    @Test
    void shouldRejectEmptyPayloadWhenMessageIndexLengthIsTruncated() {
        assertInvalidPayload(createSchemaRegistryMessage(new byte[] {(byte) 0x80}, new byte[0]));
    }

    @Test
    void shouldRejectEmptyPayloadWhenMessageIndexLengthIsNegative() {
        assertInvalidPayload(createSchemaRegistryMessage(new byte[] {1}, new byte[0]));
    }

    @Test
    void shouldRejectEmptyPayloadWhenMessageIndexVectorIsTruncated() {
        assertInvalidPayload(createSchemaRegistryMessage(new byte[] {4, 0}, new byte[0]));
    }

    @Test
    void shouldRejectEmptyPayloadWhenMessageIndexIsNegative() {
        assertInvalidPayload(createSchemaRegistryMessage(new byte[] {2, 1}, new byte[0]));
    }

    @Test
    void shouldRejectEmptyPayloadWhenMessageIndexLengthVarIntIsOversized() {
        assertInvalidPayload(
                createSchemaRegistryMessage(
                        new byte[] {
                            (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x10
                        },
                        new byte[0]));
    }

    @Test
    void shouldRejectEmptyPayloadWhenMessageIndexVarIntIsOversized() {
        assertInvalidPayload(
                createSchemaRegistryMessage(
                        new byte[] {
                            2, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x10
                        },
                        new byte[0]));
    }

    @Test
    void shouldRejectEmptyPayloadWhenMagicByteIsInvalid() {
        byte[] message = createSchemaRegistryMessage(new byte[0]);
        message[0] = 1;

        assertInvalidPayload(message);
    }

    @Test
    void shouldRejectMalformedSchemaRegistryHeader() {
        assertInvalidPayload(new byte[] {0, 1, 2, 3, 4, 5});
    }
}
