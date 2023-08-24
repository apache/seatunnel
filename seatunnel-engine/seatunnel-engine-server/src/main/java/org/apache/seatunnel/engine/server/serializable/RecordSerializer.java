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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class RecordSerializer implements StreamSerializer<Record> {
    enum RecordDataType {
        CHECKPOINT_BARRIER,
        SEATUNNEL_ROW;
    }

    @Override
    public void write(ObjectDataOutput out, Record record) throws IOException {
        Object data = record.getData();
        if (data instanceof CheckpointBarrier) {
            CheckpointBarrier checkpointBarrier = (CheckpointBarrier) data;
            out.writeByte(RecordDataType.CHECKPOINT_BARRIER.ordinal());
            out.writeLong(checkpointBarrier.getId());
            out.writeLong(checkpointBarrier.getTimestamp());
            out.writeString(checkpointBarrier.getCheckpointType().getName());
        } else if (data instanceof SeaTunnelRow) {
            SeaTunnelRow row = (SeaTunnelRow) data;
            out.writeByte(RecordDataType.SEATUNNEL_ROW.ordinal());
            out.writeString(row.getTableId());
            out.writeByte(row.getRowKind().toByteValue());
            out.writeByte(row.getArity());
            for (Object field : row.getFields()) {
                out.writeObject(field);
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
        if (dataType == RecordDataType.CHECKPOINT_BARRIER.ordinal()) {
            data =
                    new CheckpointBarrier(
                            in.readLong(), in.readLong(), CheckpointType.fromName(in.readString()));
        } else if (dataType == RecordDataType.SEATUNNEL_ROW.ordinal()) {
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
