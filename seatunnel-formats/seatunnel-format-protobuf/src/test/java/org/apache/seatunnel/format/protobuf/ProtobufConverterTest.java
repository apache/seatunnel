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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
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

    private static final String MESSAGE_NAME = "Test";

    /**
     * {@link ProtobufToRowConverter} compiles its proto source only when {@link
     * ProtobufToRowConverter#getDescriptor()} is called, and {@link
     * ProtobufToRowConverter#converter} never calls it. The descriptors under test are assembled
     * programmatically, so this placeholder is never handed to a compiler.
     */
    private static final String UNCOMPILED_PROTO_SOURCE = "descriptor assembled programmatically";

    /**
     * Writes {@code values} through the sink-side converter and reads them back through the
     * source-side converter, i.e. the exact path used by a connector with {@code format =
     * protobuf}.
     */
    private SeaTunnelRow roundTrip(
            Descriptors.Descriptor descriptor, SeaTunnelRowType rowType, Object[] values)
            throws IOException {
        byte[] protobufMessage =
                new RowToProtobufConverter(rowType, descriptor)
                        .convertRowToGenericRecord(new SeaTunnelRow(values));
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, protobufMessage);
        return new ProtobufToRowConverter(UNCOMPILED_PROTO_SOURCE, MESSAGE_NAME)
                .converter(descriptor, dynamicMessage, rowType);
    }

    /** Fixture: a proto3 {@code string} field, as in {@code string c_string = 6;}. */
    private static Descriptors.Descriptor stringFieldMessage(String fieldName, int number)
            throws Descriptors.DescriptorValidationException {
        return buildTestMessage(testMessage().addField(stringField(fieldName, number)));
    }

    /** Fixture: a proto3 {@code int32} field, as in {@code int32 c_int32 = 1;}. */
    private static Descriptors.Descriptor int32FieldMessage(String fieldName, int number)
            throws Descriptors.DescriptorValidationException {
        return buildTestMessage(testMessage().addField(int32Field(fieldName, number)));
    }

    /** Fixture: a proto3 map field, as in {@code map<string, float> attributes = 9;}. */
    private static Descriptors.Descriptor mapFieldMessage(String fieldName, int number)
            throws Descriptors.DescriptorValidationException {
        return buildTestMessage(addMapField(testMessage(), fieldName, number));
    }

    /** Fixture: a nested {@code Address} message plus the message-typed field referring to it. */
    private static Descriptors.Descriptor nestedAddressMessage(String fieldName)
            throws Descriptors.DescriptorValidationException {
        return buildTestMessage(
                testMessage()
                        .addNestedType(addressMessage())
                        .addField(messageField(fieldName, "Address", 8)));
    }

    /** The nested {@code Address} message used by the ROW fixtures below. */
    private static DescriptorProtos.DescriptorProto.Builder addressMessage() {
        return DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Address")
                .addField(stringField("street", 1));
    }

    /** Fixture: a ROW field whose message type is the top level sibling {@code PersonDetails}. */
    private static Descriptors.Descriptor siblingRowFieldMessage(String fieldName)
            throws Descriptors.DescriptorValidationException {
        return buildTestMessage(
                testMessage().addField(topLevelMessageField(fieldName, "PersonDetails", 11)),
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("PersonDetails")
                        .addField(stringField("email", 1))
                        .addField(int32Field("age", 2))
                        .build());
    }

    /** Fixture: two ROW fields referring to the nested {@code Address} message. */
    private static Descriptors.Descriptor twoNestedRowFieldsMessage(String first, String second)
            throws Descriptors.DescriptorValidationException {
        return buildTestMessage(
                testMessage()
                        .addNestedType(addressMessage())
                        .addField(messageField(first, "Address", 8))
                        .addField(messageField(second, "Address", 12)));
    }

    /** Fixture: a nested ROW whose {@code Address} message declares a map field. */
    private static Descriptors.Descriptor mapInsideNestedRowMessage(String fieldName)
            throws Descriptors.DescriptorValidationException {
        return buildTestMessage(
                testMessage()
                        .addNestedType(
                                addMapField(
                                        addressMessage(),
                                        "attributes",
                                        9,
                                        "." + MESSAGE_NAME + ".Address"))
                        .addField(messageField(fieldName, "Address", 8)));
    }

    /** Fixture: mixed case proto3 fields {@code C_INT32}, {@code C_STRING}, {@code Attributes}. */
    private static Descriptors.Descriptor mixedCaseMessage()
            throws Descriptors.DescriptorValidationException {
        return buildTestMessage(
                addMapField(
                        testMessage()
                                .addField(int32Field("C_INT32", 1))
                                .addField(stringField("C_STRING", 6)),
                        "Attributes",
                        9));
    }

    /** Builds the {@link #MESSAGE_NAME} descriptor, plus the given sibling top level messages. */
    private static Descriptors.Descriptor buildTestMessage(
            DescriptorProtos.DescriptorProto.Builder message,
            DescriptorProtos.DescriptorProto... additionalMessages)
            throws Descriptors.DescriptorValidationException {
        DescriptorProtos.FileDescriptorProto.Builder fileBuilder =
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("protobuf_converter_test.proto")
                        .setSyntax("proto3")
                        .addMessageType(message);
        for (DescriptorProtos.DescriptorProto additionalMessage : additionalMessages) {
            fileBuilder.addMessageType(additionalMessage);
        }
        DescriptorProtos.FileDescriptorProto file = fileBuilder.build();
        return Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0])
                .findMessageTypeByName(MESSAGE_NAME);
    }

    private static DescriptorProtos.DescriptorProto.Builder testMessage() {
        return DescriptorProtos.DescriptorProto.newBuilder().setName(MESSAGE_NAME);
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder field(
            String name, DescriptorProtos.FieldDescriptorProto.Type type, int number) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(type);
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder stringField(
            String name, int number) {
        return field(name, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, number);
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder int32Field(
            String name, int number) {
        return field(name, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, number);
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder floatField(
            String name, int number) {
        return field(name, DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, number);
    }

    /** A message-typed field, as in {@code Address ADDRESS = 8;}. */
    private static DescriptorProtos.FieldDescriptorProto.Builder messageField(
            String name, String messageTypeName, int number) {
        return field(name, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, number)
                .setTypeName("." + MESSAGE_NAME + "." + messageTypeName);
    }

    /** A field referring to a top level message, as in {@code PersonDetails contact = 11;}. */
    private static DescriptorProtos.FieldDescriptorProto.Builder topLevelMessageField(
            String name, String messageTypeName, int number) {
        return field(name, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, number)
                .setTypeName("." + messageTypeName);
    }

    /** Adds a map field and the map entry type protoc would synthesize for it. */
    private static DescriptorProtos.DescriptorProto.Builder addMapField(
            DescriptorProtos.DescriptorProto.Builder message, String name, int number) {
        return addMapField(message, name, number, "." + MESSAGE_NAME);
    }

    /** Adds a map field and its entry type to a nested message instead of to the root message. */
    private static DescriptorProtos.DescriptorProto.Builder addMapField(
            DescriptorProtos.DescriptorProto.Builder message,
            String name,
            int number,
            String ownerTypeFullName) {
        String entryTypeName = mapEntryTypeName(name);
        message.addNestedType(
                DescriptorProtos.DescriptorProto.newBuilder()
                        .setName(entryTypeName)
                        .setOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true))
                        .addField(stringField("key", 1))
                        .addField(floatField("value", 2)));
        // protoc nests the synthesized entry type inside the message that declares the map field.
        String entryTypeFullName = ownerTypeFullName + "." + entryTypeName;
        message.addField(
                field(name, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, number)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                        .setTypeName(entryTypeFullName));
        return message;
    }

    /** Mirrors protoc's map entry type name, e.g. {@code attributes} to {@code AttributesEntry}. */
    private static String mapEntryTypeName(String fieldName) {
        return Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1) + "Entry";
    }

    /**
     * A scalar field whose schema name differs in case from the proto field ({@code C_STRING}
     * against {@code string c_string = 6;}) must still resolve on the read path.
     */
    @Test
    void testTopLevelScalarFieldNameIsResolvedCaseInsensitively() throws Exception {
        Descriptors.Descriptor descriptor = stringFieldMessage("c_string", 6);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"C_STRING"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});

        SeaTunnelRow convertedRow = roundTrip(descriptor, rowType, new Object[] {"test data"});

        Assertions.assertEquals("test data", convertedRow.getField(0));
    }

    /**
     * A MAP field whose schema name differs in case from the proto field ({@code Attributes} versus
     * {@code map<string, float> attributes = 9;}) must neither fail while writing nor read back as
     * {@code null}.
     */
    @Test
    void testMapFieldNameIsResolvedCaseInsensitively() throws Exception {
        Descriptors.Descriptor descriptor = mapFieldMessage("attributes", 9);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"Attributes"},
                        new SeaTunnelDataType<?>[] {
                            new MapType<>(BasicType.STRING_TYPE, BasicType.FLOAT_TYPE)
                        });

        Map<String, Float> attributes = new HashMap<>();
        attributes.put("k1", 0.1F);
        attributes.put("k2", 2.3F);

        SeaTunnelRow convertedRow = roundTrip(descriptor, rowType, new Object[] {attributes});

        Assertions.assertEquals(attributes, convertedRow.getField(0));
    }

    /**
     * A nested ROW field whose schema name differs in case from the proto field ({@code Address}
     * against {@code Address ADDRESS = 8;}) must resolve on both the write and the read path.
     */
    @Test
    void testNestedFieldNameIsResolvedCaseInsensitively() throws Exception {
        Descriptors.Descriptor descriptor = nestedAddressMessage("ADDRESS");
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"Address"},
                        new SeaTunnelDataType<?>[] {
                            new SeaTunnelRowType(
                                    new String[] {"street"},
                                    new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE})
                        });

        SeaTunnelRow address = new SeaTunnelRow(new Object[] {"street_value"});
        SeaTunnelRow convertedRow = roundTrip(descriptor, rowType, new Object[] {address});

        Assertions.assertEquals(address, convertedRow.getField(0));
    }

    /**
     * A proto is allowed to declare mixed case field names. Those fields can only be resolved if
     * the exact name is tried before any case-insensitive fallback, so a fix that merely lowercases
     * the schema name is not sufficient.
     */
    @Test
    void testExactCaseProtoFieldNamesAreResolvedExactly() throws Exception {
        Descriptors.Descriptor descriptor = mixedCaseMessage();
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"C_INT32", "C_STRING", "Attributes"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            new MapType<>(BasicType.STRING_TYPE, BasicType.FLOAT_TYPE)
                        });

        Map<String, Float> attributes = new HashMap<>();
        attributes.put("k1", 0.1F);

        SeaTunnelRow convertedRow =
                roundTrip(descriptor, rowType, new Object[] {123, "test data", attributes});

        Assertions.assertEquals(123, convertedRow.getField(0));
        Assertions.assertEquals("test data", convertedRow.getField(1));
        Assertions.assertEquals(attributes, convertedRow.getField(2));
    }

    /**
     * Name resolution must not depend on the JVM default locale: {@code "C_INT32".toLowerCase()} is
     * {@code "c_ınt32"} (dotless i) under the Turkish locale, which no proto field can match.
     */
    @Test
    void testFieldNameResolutionDoesNotDependOnDefaultLocale() throws Exception {
        Descriptors.Descriptor descriptor = int32FieldMessage("c_int32", 1);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"C_INT32"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});

        Locale previousLocale = Locale.getDefault();
        SeaTunnelRow convertedRow;
        try {
            Locale.setDefault(Locale.forLanguageTag("tr"));
            convertedRow = roundTrip(descriptor, rowType, new Object[] {123});
        } finally {
            Locale.setDefault(previousLocale);
        }

        Assertions.assertEquals(123, convertedRow.getField(0));
    }

    /** A ROW column may map to a message type declared at the top level of the proto file. */
    @Test
    void testNestedRowColumnWithTopLevelSiblingMessageType() throws Exception {
        Descriptors.Descriptor descriptor = siblingRowFieldMessage("contact");
        SeaTunnelRowType contactType =
                new SeaTunnelRowType(
                        new String[] {"email", "age"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"CONTACT"}, new SeaTunnelDataType<?>[] {contactType});

        SeaTunnelRow contact = new SeaTunnelRow(new Object[] {"email_value", 42});
        SeaTunnelRow convertedRow = roundTrip(descriptor, rowType, new Object[] {contact});

        Assertions.assertEquals(contact, convertedRow.getField(0));
    }

    /** A MAP column nested inside a nested ROW column belongs to the nested message. */
    @Test
    void testMapColumnInsideNestedRowColumn() throws Exception {
        Descriptors.Descriptor descriptor = mapInsideNestedRowMessage("address");
        SeaTunnelRowType addressType =
                new SeaTunnelRowType(
                        new String[] {"street", "attributes"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE,
                            new MapType<>(BasicType.STRING_TYPE, BasicType.FLOAT_TYPE)
                        });
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"address"}, new SeaTunnelDataType<?>[] {addressType});

        Map<String, Float> attributes = new HashMap<>();
        attributes.put("k1", 0.1F);
        attributes.put("k2", 2.3F);
        SeaTunnelRow address = new SeaTunnelRow(new Object[] {"street_value", attributes});

        SeaTunnelRow convertedRow = roundTrip(descriptor, rowType, new Object[] {address});

        Assertions.assertEquals(address, convertedRow.getField(0));
    }

    /** A null ROW column must not throw while writing nor disturb its populated sibling. */
    @Test
    void testNullNestedRowColumnNextToPopulatedOne() throws Exception {
        Descriptors.Descriptor descriptor = twoNestedRowFieldsMessage("address", "backup_address");
        SeaTunnelRowType addressType =
                new SeaTunnelRowType(
                        new String[] {"street"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"address", "backup_address"},
                        new SeaTunnelDataType<?>[] {addressType, addressType});

        SeaTunnelRow address = new SeaTunnelRow(new Object[] {"street_value"});
        SeaTunnelRow convertedRow = roundTrip(descriptor, rowType, new Object[] {address, null});

        Assertions.assertEquals(address, convertedRow.getField(0));
        // proto3 has no set-but-null message, so the column written as null reads back as the
        // default instance of the nested message instead of as the value of its sibling.
        Assertions.assertNotEquals(address, convertedRow.getField(1));
    }

    /** A schema column missing from the proto must fail the write with {@code PROTOBUF-03}. */
    @Test
    void testWriteOfValueForMissingColumnFailsWithProtobuf03() throws Exception {
        Descriptors.Descriptor descriptor = stringFieldMessage("c_string", 6);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"c_string", "c_missing"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});

        SeaTunnelProtobufFormatException exception =
                Assertions.assertThrows(
                        SeaTunnelProtobufFormatException.class,
                        () -> roundTrip(descriptor, rowType, new Object[] {"test data", 123}));

        Assertions.assertEquals(
                ProtobufFormatErrorCode.FIELD_NOT_FOUND, exception.getSeaTunnelErrorCode());
        Assertions.assertEquals("PROTOBUF-03", exception.getSeaTunnelErrorCode().getCode());
        Assertions.assertTrue(exception.getMessage().contains("c_missing"));
    }

    /** A schema column missing from the proto must be read back as {@code null}. */
    @Test
    void testReadOfMissingColumnReturnsNull() throws Exception {
        Descriptors.Descriptor descriptor = stringFieldMessage("c_string", 6);
        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"c_string"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        byte[] protobufMessage =
                new RowToProtobufConverter(writeRowType, descriptor)
                        .convertRowToGenericRecord(new SeaTunnelRow(new Object[] {"test data"}));
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, protobufMessage);

        SeaTunnelRowType readRowType =
                new SeaTunnelRowType(
                        new String[] {"c_string", "c_missing"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
        SeaTunnelRow convertedRow =
                new ProtobufToRowConverter(UNCOMPILED_PROTO_SOURCE, MESSAGE_NAME)
                        .converter(descriptor, dynamicMessage, readRowType);

        Assertions.assertEquals("test data", convertedRow.getField(0));
        Assertions.assertNull(convertedRow.getField(1));
    }
}
