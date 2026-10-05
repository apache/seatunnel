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

import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.server.trace.StainTraceConstants;
import org.apache.seatunnel.engine.server.trace.StainTracePayload;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

/** Verifies record serialization keeps stain trace payloads compatible and bounded. */
class RecordSerializerTest {
    private static final String TABLE_ID = "test_table";
    private static final byte TYPE_CHECKPOINT_BARRIER = 0;
    private static final byte TYPE_SEATUNNEL_ROW_V1 = 1;
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

    @Test
    void testSerializeDeserializeRowWithTracePayload() throws IOException {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
        row.setTableId(TABLE_ID);
        row.setRowKind(RowKind.INSERT);
        byte[] payload = StainTracePayload.init(1L, 2L);
        payload =
                StainTracePayload.append(payload, StainTraceStage.SOURCE_EMIT, 3L, 4L, 32)
                        .getPayload();
        row.getOptions().put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, payload);

        byte[] bytes = serializeRecordWithCurrentSerializer(row);
        Assertions.assertEquals(TYPE_SEATUNNEL_ROW_V1, bytes[0], "Should use legacy row type");

        Record<?> deserialized = deserializeWithCurrentSerializer(bytes);
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertEquals(TABLE_ID, readRow.getTableId());
        Assertions.assertEquals(RowKind.INSERT, readRow.getRowKind());
        Assertions.assertNotNull(readRow.getOptionsOrNull());
        Assertions.assertArrayEquals(
                payload,
                (byte[])
                        readRow.getOptionsOrNull()
                                .get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY));
    }

    @Test
    void testSerializeDeserializeRowWithNonTraceOptions() throws IOException {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
        row.setTableId(TABLE_ID);
        row.setRowKind(RowKind.INSERT);
        row.getOptions().put(CommonOptions.EVENT_TIME.getName(), 12345L);

        byte[] bytes = serializeRecordWithCurrentSerializer(row);
        Assertions.assertEquals(TYPE_SEATUNNEL_ROW_V1, bytes[0], "Should use legacy row type");

        Record<?> deserialized = deserializeWithCurrentSerializer(bytes);
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertNotNull(readRow.getOptionsOrNull());
        Assertions.assertEquals(
                12345L, readRow.getOptionsOrNull().get(CommonOptions.EVENT_TIME.getName()));
    }

    @Test
    void testSerializeDeserializeRowWithMixedOptions() throws IOException {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
        row.setTableId(TABLE_ID);
        row.setRowKind(RowKind.INSERT);
        byte[] payload = StainTracePayload.init(1L, 2L);
        payload =
                StainTracePayload.append(payload, StainTraceStage.SOURCE_EMIT, 3L, 4L, 32)
                        .getPayload();
        row.getOptions().put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, payload);
        row.getOptions().put(CommonOptions.EVENT_TIME.getName(), 99L);
        row.getOptions().put(CommonOptions.DATABASE.getName(), "mydb");

        Record<?> deserialized =
                deserializeWithCurrentSerializer(serializeRecordWithCurrentSerializer(row));
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertNotNull(readRow.getOptionsOrNull());
        Assertions.assertArrayEquals(
                payload,
                (byte[])
                        readRow.getOptionsOrNull()
                                .get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY));
        Assertions.assertEquals(
                99L, readRow.getOptionsOrNull().get(CommonOptions.EVENT_TIME.getName()));
        Assertions.assertEquals(
                "mydb", readRow.getOptionsOrNull().get(CommonOptions.DATABASE.getName()));
    }

    @Test
    void testSerializeDeserializeRowWithoutOptionsUsesLegacyType() throws IOException {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
        row.setTableId(TABLE_ID);
        row.setRowKind(RowKind.INSERT);

        byte[] bytes = serializeRecordWithCurrentSerializer(row);
        Assertions.assertEquals(TYPE_SEATUNNEL_ROW_V1, bytes[0]);

        Record<?> deserialized = deserializeWithCurrentSerializer(bytes);
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertEquals(TABLE_ID, readRow.getTableId());
        Assertions.assertEquals(RowKind.INSERT, readRow.getRowKind());
        Assertions.assertNull(readRow.getOptionsOrNull());
    }

    @Test
    void testSerializeDeserializeRowWithEmptyTracePayloadUsesLegacyType() throws IOException {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
        row.setTableId(TABLE_ID);
        row.setRowKind(RowKind.INSERT);
        row.getOptions().put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, new byte[0]);

        byte[] bytes = serializeRecordWithCurrentSerializer(row);
        Assertions.assertEquals(TYPE_SEATUNNEL_ROW_V1, bytes[0]);

        Record<?> deserialized = deserializeWithCurrentSerializer(bytes);
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertNull(readRow.getOptionsOrNull());
    }

    @Test
    void testSerializeDeserializeRowWithOversizeTracePayloadUsesLegacyType() throws IOException {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
        row.setTableId(TABLE_ID);
        row.setRowKind(RowKind.INSERT);
        row.getOptions().put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, new byte[9 * 1024]);

        byte[] bytes = serializeRecordWithCurrentSerializer(row);
        Assertions.assertEquals(TYPE_SEATUNNEL_ROW_V1, bytes[0]);

        Record<?> deserialized = deserializeWithCurrentSerializer(bytes);
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertNull(readRow.getOptionsOrNull());
    }

    @Test
    void testSerializeDeserializeRowWithOversizeTracePayloadButOtherOptionsPreserved()
            throws IOException {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
        row.setTableId(TABLE_ID);
        row.setRowKind(RowKind.INSERT);
        // Oversized trace payload should be stripped, but EventTime should survive.
        row.getOptions().put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, new byte[9 * 1024]);
        row.getOptions().put(CommonOptions.EVENT_TIME.getName(), 777L);

        byte[] bytes = serializeRecordWithCurrentSerializer(row);
        Assertions.assertEquals(TYPE_SEATUNNEL_ROW_V1, bytes[0], "Should use legacy row type");

        Record<?> deserialized = deserializeWithCurrentSerializer(bytes);
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertNotNull(readRow.getOptionsOrNull());
        Assertions.assertNull(
                readRow.getOptionsOrNull().get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY),
                "Oversized trace payload should be stripped");
        Assertions.assertEquals(
                777L, readRow.getOptionsOrNull().get(CommonOptions.EVENT_TIME.getName()));
    }

    @Test
    void testBackwardCompatibilityReadLegacyBytes() throws IOException {
        BufferObjectDataOutput out = serializationService.createObjectDataOutput();
        out.writeByte(TYPE_SEATUNNEL_ROW_V1);
        out.writeString(TABLE_ID);
        out.writeByte(RowKind.INSERT.toByteValue());
        out.writeByte((byte) 2);
        out.writeObject(1);
        out.writeObject("a");

        Record<?> deserialized = deserializeWithCurrentSerializer(out.toByteArray());
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertEquals(TABLE_ID, readRow.getTableId());
        Assertions.assertEquals(RowKind.INSERT, readRow.getRowKind());
        Assertions.assertNull(readRow.getOptionsOrNull());
        Assertions.assertEquals(1, readRow.getField(0));
        Assertions.assertEquals("a", readRow.getField(1));
    }

    @Test
    void testExplicitTypeValuesMatchLegacyOrdinalOrder() throws IOException {
        BufferObjectDataOutput checkpointOut = serializationService.createObjectDataOutput();
        serializer.write(
                checkpointOut,
                new Record<>(
                        new org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier(
                                1L,
                                2L,
                                org.apache.seatunnel.engine.core.checkpoint.CheckpointType
                                        .CHECKPOINT_TYPE)));
        Assertions.assertEquals(TYPE_CHECKPOINT_BARRIER, checkpointOut.toByteArray()[0]);

        BufferObjectDataOutput rowOut = serializationService.createObjectDataOutput();
        serializer.write(rowOut, new Record<>(new SeaTunnelRow(new Object[] {1})));
        Assertions.assertEquals(TYPE_SEATUNNEL_ROW_V1, rowOut.toByteArray()[0]);
    }

    @Test
    void testLegacyHazelcastDataReaderCanReadCurrentRowWithOptions() {
        InternalSerializationService currentService =
                new DefaultSerializationServiceBuilder().build();
        InternalSerializationService legacyService =
                createSerializationService(new LegacyRecordSerializer());
        try {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
            row.setTableId(TABLE_ID);
            row.setRowKind(RowKind.INSERT);
            row.getOptions().put(CommonOptions.EVENT_TIME.getName(), 12345L);
            row.getOptions()
                    .put(
                            StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY,
                            StainTracePayload.init(1L, 2L));

            Record<?> deserialized =
                    legacyService.toObject(currentService.toData(new Record<>(row)));
            SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
            Assertions.assertEquals(TABLE_ID, readRow.getTableId());
            Assertions.assertEquals(RowKind.INSERT, readRow.getRowKind());
            Assertions.assertEquals(1, readRow.getField(0));
            Assertions.assertEquals("a", readRow.getField(1));
            Assertions.assertNull(readRow.getOptionsOrNull());
        } finally {
            legacyService.dispose();
            currentService.dispose();
        }
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
        out.writeByte(TYPE_SEATUNNEL_ROW_V1);
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
        Assertions.assertEquals(TYPE_SEATUNNEL_ROW_V1, in.readByte());
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
        Assertions.assertEquals(TYPE_SEATUNNEL_ROW_V1, in.readByte());
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

    /**
     * Uses the historical Record serializer implementation from upstream/dev to verify the current
     * service can still interoperate with old members at the standalone Hazelcast Data boundary.
     */
    private static InternalSerializationService createSerializationService(
            StreamSerializer<Record> recordSerializer) {
        SerializationConfig config = new SerializationConfig();
        config.addSerializerConfig(
                new SerializerConfig()
                        .setTypeClass(Record.class)
                        .setImplementation(recordSerializer));
        return new DefaultSerializationServiceBuilder().setConfig(config).build();
    }

    /**
     * Mirrors the pre-PR Record wire contract so compatibility tests can exercise mixed-version
     * read behavior without depending on another source checkout.
     */
    private static final class LegacyRecordSerializer implements StreamSerializer<Record> {

        @Override
        public void write(ObjectDataOutput out, Record record) throws IOException {
            Object data = record.getData();
            if (data instanceof org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier) {
                org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier checkpointBarrier =
                        (org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier) data;
                out.writeByte(TYPE_CHECKPOINT_BARRIER);
                out.writeLong(checkpointBarrier.getId());
                out.writeLong(checkpointBarrier.getTimestamp());
                out.writeString(checkpointBarrier.getCheckpointType().getName());
                out.writeObject(checkpointBarrier.getPrepareCloseTasks());
                out.writeObject(checkpointBarrier.getClosedTasks());
            } else if (data instanceof SeaTunnelRow) {
                SeaTunnelRow row = (SeaTunnelRow) data;
                out.writeByte(TYPE_SEATUNNEL_ROW_V1);
                out.writeString(row.getTableId());
                out.writeByte(row.getRowKind().toByteValue());
                out.writeByte(row.getArity());
                for (Object field : row.getFields()) {
                    out.writeObject(field);
                }
            } else {
                throw new IOException("Unsupported serialize class: " + data.getClass());
            }
        }

        @Override
        public Record read(ObjectDataInput in) throws IOException {
            Object data;
            byte dataType = in.readByte();
            if (dataType == TYPE_CHECKPOINT_BARRIER) {
                data =
                        new org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier(
                                in.readLong(),
                                in.readLong(),
                                org.apache.seatunnel.engine.core.checkpoint.CheckpointType.fromName(
                                        in.readString()),
                                in.readObject(),
                                in.readObject());
            } else if (dataType == TYPE_SEATUNNEL_ROW_V1) {
                String tableId = in.readString();
                byte rowKind = in.readByte();
                byte arity = in.readByte();
                SeaTunnelRow row = new SeaTunnelRow(arity);
                row.setTableId(tableId);
                row.setRowKind(RowKind.fromByteValue(rowKind));
                for (int i = 0; i < arity; i++) {
                    row.setField(i, in.readObject());
                }
                data = row;
            } else {
                throw new IOException("Unsupported deserialize data type: " + dataType);
            }
            return new Record(data);
        }

        @Override
        public int getTypeId() {
            return TypeId.RECORD;
        }
    }
}
