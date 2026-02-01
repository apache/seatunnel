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
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.trace.StainTraceConstants;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class RecordSerializer implements StreamSerializer<Record> {
    private static final byte TYPE_CHECKPOINT_BARRIER = 0;
    /**
     * Legacy SeaTunnelRow record type.
     *
     * <p>Keep this value for backward compatibility (new version must be able to read old data).
     */
    private static final byte TYPE_SEATUNNEL_ROW_V1 = 1;

    /** SeaTunnelRow with optional stain trace payload extension. */
    private static final byte TYPE_SEATUNNEL_ROW_V2 = 2;

    private static final int MAX_TRACE_PAYLOAD_LENGTH = 8 * 1024;

    @Override
    public void write(ObjectDataOutput out, Record record) throws IOException {
        Object data = record.getData();
        if (data instanceof CheckpointBarrier) {
            CheckpointBarrier checkpointBarrier = (CheckpointBarrier) data;
            out.writeByte(TYPE_CHECKPOINT_BARRIER);
            out.writeLong(checkpointBarrier.getId());
            out.writeLong(checkpointBarrier.getTimestamp());
            out.writeString(checkpointBarrier.getCheckpointType().getName());
            out.writeObject(checkpointBarrier.getPrepareCloseTasks());
            out.writeObject(checkpointBarrier.getClosedTasks());
        } else if (data instanceof SeaTunnelRow) {
            SeaTunnelRow row = (SeaTunnelRow) data;
            Object payloadObj =
                    row.getOptionsOrNull() == null
                            ? null
                            : row.getOptionsOrNull()
                                    .get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);
            byte[] payload = payloadObj instanceof byte[] ? (byte[]) payloadObj : null;
            if (payload != null
                    && (payload.length <= 0 || payload.length > MAX_TRACE_PAYLOAD_LENGTH)) {
                payload = null;
            }
            if (payload == null) {
                out.writeByte(TYPE_SEATUNNEL_ROW_V1);
            } else {
                out.writeByte(TYPE_SEATUNNEL_ROW_V2);
            }
            out.writeString(row.getTableId());
            out.writeByte(row.getRowKind().toByteValue());
            out.writeByte(row.getArity());
            for (Object field : row.getFields()) {
                out.writeObject(field);
            }
            if (payload != null) {
                out.writeInt(payload.length);
                out.write(payload);
            }
        } else {
            throw new UnsupportedEncodingException(
                    "Unsupported serialize class: " + data.getClass());
        }
    }

    @Override
    public Record read(ObjectDataInput in) throws IOException {
        Object data;
        byte dataType = in.readByte();
        if (dataType == TYPE_CHECKPOINT_BARRIER) {
            data =
                    new CheckpointBarrier(
                            in.readLong(),
                            in.readLong(),
                            CheckpointType.fromName(in.readString()),
                            in.readObject(),
                            in.readObject());
        } else if (dataType == TYPE_SEATUNNEL_ROW_V1 || dataType == TYPE_SEATUNNEL_ROW_V2) {
            String tableId = in.readString();
            byte rowKind = in.readByte();
            byte arity = in.readByte();
            SeaTunnelRow row = new SeaTunnelRow(arity);
            row.setTableId(tableId);
            row.setRowKind(RowKind.fromByteValue(rowKind));
            for (int i = 0; i < arity; i++) {
                row.setField(i, in.readObject());
            }
            if (dataType == TYPE_SEATUNNEL_ROW_V2) {
                int payloadLength = in.readInt();
                if (payloadLength < 0) {
                    throw new IOException("Negative stain trace payload length: " + payloadLength);
                } else if (payloadLength > MAX_TRACE_PAYLOAD_LENGTH) {
                    in.skipBytes(payloadLength);
                } else if (payloadLength > 0) {
                    byte[] payload = new byte[payloadLength];
                    in.readFully(payload);
                    row.getOptions().put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, payload);
                }
            }
            data = row;
        } else {
            throw new UnsupportedEncodingException(
                    "Unsupported deserialize data type: " + dataType);
        }
        return new Record(data);
    }

    @Override
    public int getTypeId() {
        return TypeId.RECORD;
    }
}
