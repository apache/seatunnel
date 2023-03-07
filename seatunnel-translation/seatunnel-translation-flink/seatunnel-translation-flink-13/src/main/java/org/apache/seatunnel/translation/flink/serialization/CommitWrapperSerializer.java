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

package org.apache.seatunnel.translation.flink.serialization;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.translation.flink.sink.CommitWrapper;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The serializer wrapper of the commit message serializer, which is created by {@link
 * Sink#getCommittableSerializer()}, used to unify the different implementations of {@link
 * Serializer}
 *
 * @param <T> The generic type of commit message
 */
public class CommitWrapperSerializer<T> implements SimpleVersionedSerializer<CommitWrapper<T>> {
    private final Serializer<T> serializer;

    public CommitWrapperSerializer(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(CommitWrapper<T> commitWrapper) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            byte[] serialize = serializer.serialize(commitWrapper.getCommit());
            out.writeInt(serialize.length);
            out.write(serialize);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public CommitWrapper<T> deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            final int size = in.readInt();
            final byte[] stateBytes = new byte[size];
            in.read(stateBytes);
            T commitT = serializer.deserialize(stateBytes);
            return new CommitWrapper<>(commitT);
        }
    }
}
