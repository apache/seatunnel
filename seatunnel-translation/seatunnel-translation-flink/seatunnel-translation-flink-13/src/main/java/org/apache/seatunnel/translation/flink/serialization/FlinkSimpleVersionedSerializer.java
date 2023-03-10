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

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * The serializer wrapper of aggregate commit message serializer, which is created by {@link
 * Sink#getGlobalCommittableSerializer()}, used to unify the different implementations of {@link
 * Serializer}
 *
 * @param <T> The generic type of aggregate commit message
 */
public class FlinkSimpleVersionedSerializer<T> implements SimpleVersionedSerializer<T> {

    private final Serializer<T> serializer;

    public FlinkSimpleVersionedSerializer(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(T obj) throws IOException {
        return serializer.serialize(obj);
    }

    @Override
    public T deserialize(int version, byte[] serialized) throws IOException {
        return serializer.deserialize(serialized);
    }
}
