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

package org.apache.seatunnel.connectors.seatunnel.common.sink;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public abstract class AbstractSimpleSink<T, StateT> implements SeaTunnelSink<T, StateT, Void, Void> {

    @Override
    public abstract AbstractSinkWriter<T, StateT> createWriter(SinkWriter.Context context) throws IOException;

    @Override
    public SinkWriter<T, Void, StateT> restoreWriter(SinkWriter.Context context, List<StateT> states) throws IOException {
        return createWriter(context);
    }

    @Override
    public final Optional<SinkCommitter<Void>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public final Optional<Serializer<Void>> getCommitInfoSerializer() {
        return Optional.empty();
    }

    @Override
    public final Optional<SinkAggregatedCommitter<Void, Void>> createAggregatedCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public final Optional<Serializer<Void>> getAggregatedCommitInfoSerializer() {
        return Optional.empty();
    }
}
