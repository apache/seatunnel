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

import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FlinkSink<InputT, CommT, WriterStateT, GlobalCommT> implements Sink<InputT, CommT, WriterStateT, GlobalCommT> {

    private final org.apache.seatunnel.api.sink.Sink<InputT, WriterStateT, CommT, GlobalCommT> sink;
    private final Map<String, String> configuration;

    FlinkSink(org.apache.seatunnel.api.sink.Sink<InputT, WriterStateT, CommT, GlobalCommT> sink,
              Map<String, String> configuration) {
        this.sink = sink;
        this.configuration = configuration;
    }

    @Override
    public SinkWriter<InputT, CommT, WriterStateT> createWriter(InitContext context, List<WriterStateT> states) throws IOException {
        // TODO add subtask and parallelism.
        org.apache.seatunnel.api.sink.SinkWriter.Context stContext =
                new DefaultSinkWriterContext(configuration, 0, 0);

        FlinkSinkWriterConverter<InputT, CommT, WriterStateT> converter = new FlinkSinkWriterConverter<>();

        if (states == null || states.isEmpty()) {
            return converter.convert(sink.createWriter(stContext));
        } else {
            return converter.convert(sink.restoreWriter(stContext, states));
        }
    }

    @Override
    public Optional<Committer<CommT>> createCommitter() throws IOException {

        FlinkCommitterConverter<CommT> converter = new FlinkCommitterConverter<>();
        Optional<SinkCommitter<CommT>> committer = sink.createCommitter();
        return committer.map(converter::convert);

    }

    @Override
    public Optional<GlobalCommitter<CommT, GlobalCommT>> createGlobalCommitter() throws IOException {
        FlinkGlobalCommitterConverter<CommT, GlobalCommT> converter = new FlinkGlobalCommitterConverter<>();
        Optional<SinkAggregatedCommitter<CommT, GlobalCommT>> committer = sink.createAggregatedCommitter();
        return committer.map(converter::convert);
    }

    @Override
    public Optional<SimpleVersionedSerializer<CommT>> getCommittableSerializer() {
        if (sink.getCommitInfoSerializer().isPresent()) {
            return Optional.of(new FlinkSimpleVersionedSerializerConverter().convert(sink.getCommitInfoSerializer().get()));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<SimpleVersionedSerializer<GlobalCommT>> getGlobalCommittableSerializer() {
        if (sink.getAggregatedCommitInfoSerializer().isPresent()) {
            return Optional.of(new FlinkSimpleVersionedSerializerConverter().convert(sink.getAggregatedCommitInfoSerializer().get()));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<SimpleVersionedSerializer<WriterStateT>> getWriterStateSerializer() {
        if (sink.getWriterStateSerializer().isPresent()) {
            return Optional.of(new FlinkSimpleVersionedSerializerConverter().convert(sink.getWriterStateSerializer().get()));
        } else {
            return Optional.empty();
        }
    }
}
