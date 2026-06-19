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

import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/** Serializes records together with row metadata and stain trace payload options. */
public class RecordSerializer implements StreamSerializer<Record> {
    private static final byte TYPE_CHECKPOINT_BARRIER = 0;
    /**
     * Legacy SeaTunnelRow record type.
     *
     * <p>Keep this value for backward compatibility because new members must still be able to read
     * payloads produced by old serializers.
     */
    private static final byte TYPE_SEATUNNEL_ROW_V1 = 1;

    // Kept only for reading data written by old serializer implementations.
    private static final byte TYPE_SEATUNNEL_ROW_V3 = 3;

    private static final byte EXTENDED_ROW_ARITY_MARKER = -1;
    private static final int EXTENDED_ROW_ARITY_MAGIC = 0x524F5741;
    private static final int MAX_TRACE_PAYLOAD_LENGTH = 8 * 1024;

    /**
     * Writes checkpoints or rows while filtering oversized stain trace payloads from row options.
     */
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
            Map<String, Object> opts = buildSerializableOptions(row);
            out.writeByte(TYPE_SEATUNNEL_ROW_V1);
            out.writeString(row.getTableId());
            out.writeByte(row.getRowKind().toByteValue());
            writeRowArity(out, row.getArity());
            for (Object field : row.getFields()) {
                out.writeObject(field);
            }
            out.writeBoolean(opts != null);
            if (opts != null) {
                out.writeObject(opts);
            }
        } else {
            throw new UnsupportedEncodingException(
                    "Unsupported serialize class: " + data.getClass());
        }
    }

    /**
     * Reads both legacy and current row encodings so upgraded clusters can still deserialize data.
     */
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
        } else if (dataType == TYPE_SEATUNNEL_ROW_V1 || dataType == TYPE_SEATUNNEL_ROW_V3) {
            String tableId = in.readString();
            byte rowKind = in.readByte();
            int arity = readRowArity(in);
            SeaTunnelRow row = new SeaTunnelRow(arity);
            row.setTableId(tableId);
            row.setRowKind(RowKind.fromByteValue(rowKind));
            for (int i = 0; i < arity; i++) {
                row.setField(i, in.readObject());
            }
            Map<String, Object> opts = null;
            if (dataType == TYPE_SEATUNNEL_ROW_V3) {
                opts = in.readObject();
            } else {
                // Legacy V1 payload may not contain the options marker and map tail.
                try {
                    if (in.readBoolean()) {
                        opts = in.readObject();
                    }
                } catch (EOFException ignore) {
                    // Ignore; this is legacy V1 bytes without the marker.
                }
            }
            if (opts != null && !opts.isEmpty()) {
                row.setOptions(opts);
            }
            data = row;
        } else {
            throw new UnsupportedEncodingException(
                    "Unsupported deserialize data type: " + dataType);
        }
        return new Record(data);
    }

    /**
     * Builds the options map to be serialized with a row.
     *
     * <p>Returns {@code null} when the row has no options worth transmitting. Otherwise returns a
     * shallow copy of the row's options map with any invalid StainTrace payload removed.
     */
    private Map<String, Object> buildSerializableOptions(SeaTunnelRow row) {
        Map<String, Object> source = row.getOptionsOrNull();
        if (source == null || source.isEmpty()) {
            return null;
        }
        Map<String, Object> opts = new HashMap<>(source);
        Object payloadObj = opts.get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);
        if (payloadObj instanceof byte[]) {
            byte[] payload = (byte[]) payloadObj;
            if (payload.length <= 0 || payload.length > MAX_TRACE_PAYLOAD_LENGTH) {
                opts.remove(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);
            }
        }
        return opts.isEmpty() ? null : opts;
    }

    @Override
    public int getTypeId() {
        return TypeId.RECORD;
    }

    /**
     * Keep the legacy single-byte arity encoding for rows up to {@link Byte#MAX_VALUE} so mixed
     * old/new clusters can continue to deserialize valid legacy records during rolling upgrades.
     */
    private void writeRowArity(ObjectDataOutput out, int arity) throws IOException {
        if (arity <= Byte.MAX_VALUE) {
            out.writeByte(arity);
            return;
        }
        out.writeByte(EXTENDED_ROW_ARITY_MARKER);
        out.writeInt(EXTENDED_ROW_ARITY_MAGIC);
        out.writeInt(arity);
    }

    private int readRowArity(ObjectDataInput in) throws IOException {
        byte encodedArity = in.readByte();
        if (encodedArity >= 0) {
            return encodedArity;
        }
        in.readInt();
        return in.readInt();
    }
}
