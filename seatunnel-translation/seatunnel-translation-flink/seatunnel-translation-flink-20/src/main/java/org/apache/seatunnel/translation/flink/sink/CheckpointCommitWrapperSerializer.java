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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.translation.flink.serialization.CommitWrapperSerializer;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Adds a checkpoint sequence while retaining reads of the original version-zero envelope. */
final class CheckpointCommitWrapperSerializer<T>
        implements SimpleVersionedSerializer<CommitWrapper<T>> {
    private final CommitWrapperSerializer<T> legacy;

    CheckpointCommitWrapperSerializer(Serializer<T> serializer) {
        legacy = new CommitWrapperSerializer<>(serializer);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(CommitWrapper<T> wrapper) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(
                    wrapper instanceof CheckpointCommitWrapper
                            ? ((CheckpointCommitWrapper<T>) wrapper).getCheckpointId()
                            : -1);
            out.write(legacy.serialize(wrapper));
            return bytes.toByteArray();
        }
    }

    @Override
    public CommitWrapper<T> deserialize(int version, byte[] bytes) throws IOException {
        if (version == 0) {
            return deserializePayload(bytes);
        }
        if (version != 1) {
            throw new IOException("Unsupported committable envelope version: " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            long checkpointId = in.readLong();
            byte[] payload = new byte[in.available()];
            in.readFully(payload);
            return new CheckpointCommitWrapper<>(
                    deserializePayload(payload).getCommit(), checkpointId);
        }
    }

    private CommitWrapper<T> deserializePayload(byte[] bytes) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            int length = in.readInt();
            if (length < 0 || length != in.available()) {
                throw new IOException("Invalid committable payload length: " + length);
            }
        }
        return legacy.deserialize(0, bytes);
    }
}
