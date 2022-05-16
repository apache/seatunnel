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
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class FlinkSink<InputT, WriterStateT, CommT, GlobalCommT> implements Sink<InputT, Serializable, Serializable, Serializable> {

    private final SeaTunnelSink<InputT, WriterStateT, CommT, GlobalCommT> sink;
    private final Map<String, String> configuration;

    FlinkSink(SeaTunnelSink<InputT, WriterStateT, CommT, GlobalCommT> sink,
              Map<String, String> configuration) {
        this.sink = sink;
        this.configuration = configuration;
    }

    @Override
    public SinkWriter<InputT, Serializable, Serializable> createWriter(org.apache.flink.api.connector.sink.Sink.InitContext context, List<Serializable> states) throws IOException {
        // TODO add subtask and parallelism.
        org.apache.seatunnel.api.sink.SinkWriter.Context stContext =
                new DefaultSinkWriterContext(configuration, 0, 0);

        FlinkSinkWriterConverter<InputT, Serializable, Serializable> converter = new FlinkSinkWriterConverter<>();

        if (states == null || states.isEmpty()) {
            return converter.convert(sink.createWriter(stContext));
        } else {
            return converter.convert(sink.restoreWriter(stContext, states.stream().map(s -> (WriterStateT) s).collect(Collectors.toList())));
        }
    }

    @Override
    public Optional<Committer<Serializable>> createCommitter() throws IOException {

        FlinkCommitterConverter<Serializable> converter = new FlinkCommitterConverter<>();
        Optional<SinkCommitter<CommT>> committer = sink.createCommitter();
        return committer.map(sinkCommitter -> converter.convert((SinkCommitter<Serializable>) sinkCommitter));

    }

    @Override
    public Optional<GlobalCommitter<Serializable, Serializable>> createGlobalCommitter() throws IOException {
        FlinkGlobalCommitterConverter<Serializable, Serializable> converter = new FlinkGlobalCommitterConverter<>();
        Optional<SinkAggregatedCommitter<CommT, GlobalCommT>> committer = sink.createAggregatedCommitter();
        return committer.map(commTGlobalCommTSinkAggregatedCommitter -> converter.convert((SinkAggregatedCommitter<Serializable,
                Serializable>) commTGlobalCommTSinkAggregatedCommitter));
    }

    @Override
    public Optional<SimpleVersionedSerializer<Serializable>> getCommittableSerializer() {
        final FlinkSimpleVersionedSerializerConverter<Serializable> converter = new FlinkSimpleVersionedSerializerConverter<>();
        final Optional<Serializer<CommT>> commTSerializer = sink.getCommitInfoSerializer();
        return commTSerializer.map(serializer -> converter.convert((Serializer<Serializable>) serializer));
    }

    @Override
    public Optional<SimpleVersionedSerializer<Serializable>> getGlobalCommittableSerializer() {
        final Optional<Serializer<GlobalCommT>> globalCommTSerializer = sink.getAggregatedCommitInfoSerializer();
        final FlinkSimpleVersionedSerializerConverter<Serializable> converter = new FlinkSimpleVersionedSerializerConverter<>();
        return globalCommTSerializer.map(serializer -> converter.convert((Serializer<Serializable>) serializer));
    }

    @Override
    public Optional<SimpleVersionedSerializer<Serializable>> getWriterStateSerializer() {
        final FlinkSimpleVersionedSerializerConverter<Serializable> converter = new FlinkSimpleVersionedSerializerConverter<>();
        final Optional<Serializer<WriterStateT>> writerStateTSerializer = sink.getWriterStateSerializer();
        return writerStateTSerializer.map(serializer -> converter.convert((Serializer<Serializable>) serializer));
    }
}
