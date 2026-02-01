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
import org.apache.seatunnel.engine.server.trace.StainTraceConstants;
import org.apache.seatunnel.engine.server.trace.StainTracePayload;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

import java.io.IOException;

public class RecordSerializerTest {

    @Test
    void testSerializeDeserializeRowWithTracePayload() throws IOException {
        RecordSerializer serializer = new RecordSerializer();
        InternalSerializationService service = new DefaultSerializationServiceBuilder().build();
        BufferObjectDataOutput out = service.createObjectDataOutput();

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
        row.setTableId("t");
        row.setRowKind(RowKind.INSERT);
        byte[] payload = StainTracePayload.init(1L, 2L);
        payload =
                StainTracePayload.append(payload, StainTraceStage.SOURCE_EMIT, 3L, 4L, 32)
                        .getPayload();
        row.getOptions().put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, payload);

        serializer.write(out, new Record<>(row));

        BufferObjectDataInput in = service.createObjectDataInput(out.toByteArray());
        Record<?> deserialized = serializer.read(in);
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertEquals("t", readRow.getTableId());
        Assertions.assertEquals(RowKind.INSERT, readRow.getRowKind());
        Assertions.assertNotNull(readRow.getOptionsOrNull());
        Assertions.assertArrayEquals(
                payload,
                (byte[])
                        readRow.getOptionsOrNull()
                                .get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY));
    }

    @Test
    void testSerializeDeserializeRowWithoutTracePayloadUsesLegacyType() throws IOException {
        RecordSerializer serializer = new RecordSerializer();
        InternalSerializationService service = new DefaultSerializationServiceBuilder().build();
        BufferObjectDataOutput out = service.createObjectDataOutput();

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
        row.setTableId("t");
        row.setRowKind(RowKind.INSERT);
        serializer.write(out, new Record<>(row));

        byte[] bytes = out.toByteArray();
        Assertions.assertEquals(1, bytes[0]);

        BufferObjectDataInput in = service.createObjectDataInput(bytes);
        Record<?> deserialized = serializer.read(in);
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertEquals("t", readRow.getTableId());
        Assertions.assertEquals(RowKind.INSERT, readRow.getRowKind());
        Assertions.assertNull(readRow.getOptionsOrNull());
    }

    @Test
    void testSerializeDeserializeRowWithEmptyTracePayloadUsesLegacyType() throws IOException {
        RecordSerializer serializer = new RecordSerializer();
        InternalSerializationService service = new DefaultSerializationServiceBuilder().build();
        BufferObjectDataOutput out = service.createObjectDataOutput();

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
        row.setTableId("t");
        row.setRowKind(RowKind.INSERT);
        row.getOptions().put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, new byte[0]);

        serializer.write(out, new Record<>(row));

        byte[] bytes = out.toByteArray();
        Assertions.assertEquals(1, bytes[0]);

        BufferObjectDataInput in = service.createObjectDataInput(bytes);
        Record<?> deserialized = serializer.read(in);
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertNull(readRow.getOptionsOrNull());
    }

    @Test
    void testSerializeDeserializeRowWithOversizeTracePayloadUsesLegacyType() throws IOException {
        RecordSerializer serializer = new RecordSerializer();
        InternalSerializationService service = new DefaultSerializationServiceBuilder().build();
        BufferObjectDataOutput out = service.createObjectDataOutput();

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "a"});
        row.setTableId("t");
        row.setRowKind(RowKind.INSERT);
        row.getOptions().put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, new byte[9 * 1024]);

        serializer.write(out, new Record<>(row));

        byte[] bytes = out.toByteArray();
        Assertions.assertEquals(1, bytes[0]);

        BufferObjectDataInput in = service.createObjectDataInput(bytes);
        Record<?> deserialized = serializer.read(in);
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertNull(readRow.getOptionsOrNull());
    }

    @Test
    void testReadNegativePayloadLengthFailsFast() throws IOException {
        RecordSerializer serializer = new RecordSerializer();
        InternalSerializationService service = new DefaultSerializationServiceBuilder().build();
        BufferObjectDataOutput out = service.createObjectDataOutput();

        out.writeByte(2);
        out.writeString("t");
        out.writeByte(RowKind.INSERT.toByteValue());
        out.writeByte((byte) 2);
        out.writeObject(1);
        out.writeObject("a");
        out.writeInt(-1);

        BufferObjectDataInput in = service.createObjectDataInput(out.toByteArray());
        Assertions.assertThrows(IOException.class, () -> serializer.read(in));
    }

    @Test
    void testBackwardCompatibilityReadLegacyBytes() throws IOException {
        RecordSerializer serializer = new RecordSerializer();
        InternalSerializationService service = new DefaultSerializationServiceBuilder().build();
        BufferObjectDataOutput out = service.createObjectDataOutput();

        out.writeByte(1);
        out.writeString("t");
        out.writeByte(RowKind.INSERT.toByteValue());
        out.writeByte((byte) 2);
        out.writeObject(1);
        out.writeObject("a");

        BufferObjectDataInput in = service.createObjectDataInput(out.toByteArray());
        Record<?> deserialized = serializer.read(in);
        SeaTunnelRow readRow = (SeaTunnelRow) deserialized.getData();
        Assertions.assertEquals("t", readRow.getTableId());
        Assertions.assertEquals(RowKind.INSERT, readRow.getRowKind());
        Assertions.assertNull(readRow.getOptionsOrNull());
        Assertions.assertEquals(1, readRow.getField(0));
        Assertions.assertEquals("a", readRow.getField(1));
    }
}
