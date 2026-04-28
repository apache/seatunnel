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

package org.apache.seatunnel.engine.server.serializable;

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

import java.io.IOException;

class RecordSerializerTest {
    private static final String TABLE_ID = "test_table";
    private static final int EXTENDED_ROW_ARITY_MAGIC = 0x524F5741;

    private final RecordSerializer serializer = new RecordSerializer();
    private final InternalSerializationService serializationService =
            (InternalSerializationService) new DefaultSerializationServiceBuilder().build();

    @AfterEach
    void tearDown() {
        serializationService.dispose();
    }

    @Test
    void testLegacyWriteCurrentReadWithArity0To127Succeeds() throws IOException {
        SeaTunnelRow row = createRow(Byte.MAX_VALUE);
        assertSeaTunnelRowEquals(row, deserializeWithCurrentSerializer(serializeLegacyRecord(row)));
    }

    @Test
    void testLegacyWriteCurrentReadWithArity128PlusFails() throws IOException {
        byte[] bytes = serializeLegacyRecord(createRow(Byte.MAX_VALUE + 1));

        Assertions.assertThrows(Exception.class, () -> deserializeWithCurrentSerializer(bytes));
    }

    @Test
    void testCurrentWriteLegacyReadWithArity0To127Succeeds() throws IOException {
        SeaTunnelRow row = createRow(Byte.MAX_VALUE);
        assertSeaTunnelRowEquals(
                row, deserializeWithLegacySerializer(serializeRecordWithCurrentSerializer(row)));
    }

    @Test
    void testCurrentWriteLegacyReadWithArity128PlusFails() throws IOException {
        Assertions.assertThrows(
                NegativeArraySizeException.class,
                () ->
                        deserializeWithLegacySerializer(
                                serializeRecordWithCurrentSerializer(
                                        createRow(Byte.MAX_VALUE + 1))));
    }

    @Test
    void testCurrentWriteCurrentReadWithArity0To127Succeeds() throws IOException {
        SeaTunnelRow row = createRow(Byte.MAX_VALUE);
        byte[] bytes = serializeRecordWithCurrentSerializer(row);

        assertLegacyEncodingHeader(bytes, Byte.MAX_VALUE);
        assertSeaTunnelRowEquals(row, deserializeWithCurrentSerializer(bytes));
    }

    @Test
    void testCurrentWriteCurrentReadWithArity128PlusSucceeds() throws IOException {
        SeaTunnelRow row = createRow(Byte.MAX_VALUE + 1);
        byte[] bytes = serializeRecordWithCurrentSerializer(row);

        assertExtendedEncodingHeader(bytes, Byte.MAX_VALUE + 1);
        assertSeaTunnelRowEquals(row, deserializeWithCurrentSerializer(bytes));
    }

    private SeaTunnelRow createRow(int arity) {
        SeaTunnelRow row = new SeaTunnelRow(arity);
        row.setTableId(TABLE_ID);
        row.setRowKind(RowKind.INSERT);
        for (int i = 0; i < arity; i++) {
            row.setField(i, "field-" + i);
        }
        return row;
    }

    private byte[] serializeRecordWithCurrentSerializer(SeaTunnelRow row) throws IOException {
        BufferObjectDataOutput out = serializationService.createObjectDataOutput();
        serializer.write(out, new Record<>(row));
        return out.toByteArray();
    }

    private byte[] serializeLegacyRecord(SeaTunnelRow row) throws IOException {
        BufferObjectDataOutput out = serializationService.createObjectDataOutput();
        out.writeByte(RecordSerializer.RecordDataType.SEATUNNEL_ROW.ordinal());
        out.writeString(row.getTableId());
        out.writeByte(row.getRowKind().toByteValue());
        out.writeByte(row.getArity());
        for (Object field : row.getFields()) {
            out.writeObject(field);
        }
        return out.toByteArray();
    }

    private Record<?> deserializeWithCurrentSerializer(byte[] bytes) throws IOException {
        return serializer.read(serializationService.createObjectDataInput(bytes));
    }

    private Record<?> deserializeWithLegacySerializer(byte[] bytes) throws IOException {
        BufferObjectDataInput in = serializationService.createObjectDataInput(bytes);
        Assertions.assertEquals(
                RecordSerializer.RecordDataType.SEATUNNEL_ROW.ordinal(), in.readByte());
        String tableId = in.readString();
        byte rowKind = in.readByte();
        int arity = in.readByte();
        SeaTunnelRow row = new SeaTunnelRow(arity);
        row.setTableId(tableId);
        row.setRowKind(RowKind.fromByteValue(rowKind));
        for (int i = 0; i < arity; i++) {
            row.setField(i, in.readObject());
        }
        return new Record<>(row);
    }

    private void assertLegacyEncodingHeader(byte[] bytes, int expectedArity) throws IOException {
        BufferObjectDataInput in = serializationService.createObjectDataInput(bytes);
        assertCommonRowPrefix(in);
        Assertions.assertEquals(expectedArity, in.readByte());
    }

    private void assertExtendedEncodingHeader(byte[] bytes, int expectedArity) throws IOException {
        BufferObjectDataInput in = serializationService.createObjectDataInput(bytes);
        assertCommonRowPrefix(in);
        Assertions.assertEquals(-1, in.readByte());
        Assertions.assertEquals(EXTENDED_ROW_ARITY_MAGIC, in.readInt());
        Assertions.assertEquals(expectedArity, in.readInt());
    }

    private void assertCommonRowPrefix(BufferObjectDataInput in) throws IOException {
        Assertions.assertEquals(
                RecordSerializer.RecordDataType.SEATUNNEL_ROW.ordinal(), in.readByte());
        Assertions.assertEquals(TABLE_ID, in.readString());
        Assertions.assertEquals(RowKind.INSERT.toByteValue(), in.readByte());
    }

    private void assertSeaTunnelRowEquals(SeaTunnelRow expected, Record<?> actualRecord) {
        Assertions.assertInstanceOf(SeaTunnelRow.class, actualRecord.getData());
        SeaTunnelRow actual = (SeaTunnelRow) actualRecord.getData();
        Assertions.assertEquals(expected.getTableId(), actual.getTableId());
        Assertions.assertEquals(expected.getRowKind(), actual.getRowKind());
        Assertions.assertEquals(expected.getArity(), actual.getArity());
        Assertions.assertArrayEquals(expected.getFields(), actual.getFields());
    }
}
