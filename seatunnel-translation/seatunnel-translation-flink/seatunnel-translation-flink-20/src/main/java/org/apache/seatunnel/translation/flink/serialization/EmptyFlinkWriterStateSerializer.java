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

import org.apache.seatunnel.translation.flink.sink.FlinkWriterState;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * Empty serializer for FlinkWriterState when the SeaTunnel sink doesn't support state management.
 * This serializer is used to satisfy Flink 1.20's requirement that
 * SupportsWriterState.getWriterStateSerializer() must return a non-null value.
 *
 * @param <T> The generic type of writer state (unused in this implementation)
 */
public class EmptyFlinkWriterStateSerializer<T>
        implements SimpleVersionedSerializer<FlinkWriterState<T>> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(FlinkWriterState<T> state) throws IOException {
        return new byte[0];
    }

    @Override
    public FlinkWriterState<T> deserialize(int version, byte[] serialized) throws IOException {
        return new FlinkWriterState<>(0, null);
    }
}
