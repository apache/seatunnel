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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.serialization.CommitWrapperSerializer;
import org.apache.seatunnel.translation.flink.serialization.FlinkSimpleVersionedSerializer;
import org.apache.seatunnel.translation.flink.serialization.FlinkWriterStateSerializer;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The sink implementation of {@link Sink}, the entrypoint of flink sink translation
 *
 * @param <InputT> The generic type of input data
 * @param <CommT> The generic type of commit message
 * @param <WriterStateT> The generic type of writer state
 * @param <GlobalCommT> The generic type of global commit message
 */
public class FlinkSink<InputT, CommT, WriterStateT, GlobalCommT>
        implements Sink<InputT, CommitWrapper<CommT>, FlinkWriterState<WriterStateT>, GlobalCommT> {

    private final SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT> sink;

    private final List<CatalogTable> catalogTables;

    public FlinkSink(
            SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT> sink,
            List<CatalogTable> catalogTables) {
        this.sink = sink;
        this.catalogTables = catalogTables;
    }

    @Override
    public SinkWriter<InputT, CommitWrapper<CommT>, FlinkWriterState<WriterStateT>> createWriter(
            Sink.InitContext context, List<FlinkWriterState<WriterStateT>> states)
            throws IOException {
        org.apache.seatunnel.api.sink.SinkWriter.Context stContext =
                new FlinkSinkWriterContext(context);

        if (states == null || states.isEmpty()) {
            return new FlinkSinkWriter<>(sink.createWriter(stContext), 1, stContext);
        } else {
            List<WriterStateT> restoredState =
                    states.stream().map(FlinkWriterState::getState).collect(Collectors.toList());
            return new FlinkSinkWriter<>(
                    sink.restoreWriter(stContext, restoredState),
                    states.get(0).getCheckpointId() + 1,
                    stContext);
        }
    }

    @Override
    public Optional<Committer<CommitWrapper<CommT>>> createCommitter() throws IOException {
        return sink.createCommitter().map(FlinkCommitter::new);
    }

    @Override
    public Optional<GlobalCommitter<CommitWrapper<CommT>, GlobalCommT>> createGlobalCommitter()
            throws IOException {
        return sink.createAggregatedCommitter().map(FlinkGlobalCommitter::new);
    }

    @Override
    public Optional<SimpleVersionedSerializer<CommitWrapper<CommT>>> getCommittableSerializer() {
        try {
            if (sink.createCommitter().isPresent()
                    || sink.createAggregatedCommitter().isPresent()) {
                return sink.getCommitInfoSerializer().map(CommitWrapperSerializer::new);
            } else {
                return Optional.empty();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create Committer or AggregatedCommitter", e);
        }
    }

    @Override
    public Optional<SimpleVersionedSerializer<GlobalCommT>> getGlobalCommittableSerializer() {
        try {
            if (sink.createAggregatedCommitter().isPresent()) {
                return sink.getAggregatedCommitInfoSerializer()
                        .map(FlinkSimpleVersionedSerializer::new);
            } else {
                return Optional.empty();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create AggregatedCommitter", e);
        }
    }

    @Override
    public Optional<SimpleVersionedSerializer<FlinkWriterState<WriterStateT>>>
            getWriterStateSerializer() {
        return sink.getWriterStateSerializer().map(FlinkWriterStateSerializer::new);
    }
}
