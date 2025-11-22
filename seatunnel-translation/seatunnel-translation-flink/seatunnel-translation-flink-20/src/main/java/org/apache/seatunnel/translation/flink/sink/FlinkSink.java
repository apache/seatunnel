/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.serialization.CommitWrapperSerializer;
import org.apache.seatunnel.translation.flink.serialization.EmptyFlinkWriterStateSerializer;
import org.apache.seatunnel.translation.flink.serialization.FlinkWriterStateSerializer;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class FlinkSink<CommT, WriterStateT, GlobalCommT>
        implements Sink<SeaTunnelRow>,
                SupportsCommitter<CommitWrapper<CommT>>,
                SupportsWriterState<SeaTunnelRow, FlinkWriterState<WriterStateT>> {

    private final SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT> seaTunnelSink;
    private final List<CatalogTable> catalogTables;
    private final int parallelism;

    @SuppressWarnings("unchecked")
    public FlinkSink(
            SeaTunnelSink<?, ?, ?, ?> seaTunnelSink,
            List<CatalogTable> catalogTables,
            int parallelism) {
        this.seaTunnelSink =
                (SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT>) seaTunnelSink;
        this.catalogTables = catalogTables;
        this.parallelism = parallelism;
    }

    @Override
    public SinkWriter<SeaTunnelRow> createWriter(Sink.InitContext initContext) throws IOException {
        // This is the deprecated method that we must implement
        // We'll delegate to the WriterInitContext version by wrapping the context
        if (initContext instanceof WriterInitContext) {
            return createWriter((WriterInitContext) initContext);
        } else {
            throw new UnsupportedOperationException(
                    "createWriter(InitContext) requires WriterInitContext in this implementation");
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow> createWriter(WriterInitContext context) throws IOException {
        FlinkSinkWriterContext writerContext = new FlinkSinkWriterContext(context, parallelism);

        org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
                seatunnelWriter = seaTunnelSink.createWriter(writerContext);

        return new FlinkSinkWriter<>(seatunnelWriter, context, writerContext);
    }

    @Override
    public Committer<CommitWrapper<CommT>> createCommitter(CommitterInitContext context)
            throws IOException {
        // Try to create SinkCommitter first
        if (seaTunnelSink.createCommitter().isPresent()) {
            return seaTunnelSink
                    .createCommitter()
                    .<Committer<CommitWrapper<CommT>>>map(FlinkCommitter::new)
                    .orElse(null);
        }

        if (seaTunnelSink.createAggregatedCommitter().isPresent()) {
            return new FlinkSimpleAggregatedCommitter<>(
                    seaTunnelSink.createAggregatedCommitter().get());
        }

        return null;
    }

    @Override
    public SimpleVersionedSerializer<CommitWrapper<CommT>> getCommittableSerializer() {
        try {
            if (seaTunnelSink.createCommitter().isPresent()
                    || seaTunnelSink.createAggregatedCommitter().isPresent()) {
                return seaTunnelSink
                        .getCommitInfoSerializer()
                        .map(CommitWrapperSerializer::new)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Committer is present but commit serializer is missing"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // No committer path: still need a non-null serializer to satisfy Flink sink2 contract.
        return new CommitWrapperSerializer<>(new NoOpCommitSerializer<>());
    }

    /**
     * Minimal no-op serializer to satisfy Flink's serializer requirement when no committer is used.
     */
    private static class NoOpCommitSerializer<T> implements Serializer<T> {
        @Override
        public byte[] serialize(T obj) {
            return new byte[0];
        }

        @Override
        public T deserialize(byte[] bytes) {
            return null;
        }
    }

    // SupportsWriterState interface methods
    @Override
    public StatefulSinkWriter<SeaTunnelRow, FlinkWriterState<WriterStateT>> restoreWriter(
            WriterInitContext context, Collection<FlinkWriterState<WriterStateT>> recoveredState)
            throws IOException {
        FlinkSinkWriterContext writerContext = new FlinkSinkWriterContext(context, parallelism);

        if (recoveredState == null || recoveredState.isEmpty()) {
            // No state to restore, create new writer
            org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
                    seatunnelWriter = seaTunnelSink.createWriter(writerContext);
            return new FlinkSinkWriter<>(seatunnelWriter, context, writerContext);
        } else {
            // Restore from state
            List<WriterStateT> states =
                    recoveredState.stream()
                            .map(FlinkWriterState::getState)
                            .collect(Collectors.toList());

            org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT>
                    seatunnelWriter = seaTunnelSink.restoreWriter(writerContext, states);

            // Find the maximum checkpoint ID from all recovered states to ensure consistency
            long maxCheckpointId =
                    recoveredState.stream()
                            .mapToLong(FlinkWriterState::getCheckpointId)
                            .max()
                            .orElse(0L);

            // Start from the next checkpoint ID after the maximum recovered checkpoint
            long nextCheckpointId = maxCheckpointId + 1;

            return new FlinkSinkWriter<>(seatunnelWriter, context, writerContext, nextCheckpointId);
        }
    }

    @Override
    public SimpleVersionedSerializer<FlinkWriterState<WriterStateT>> getWriterStateSerializer() {
        if (seaTunnelSink.getWriterStateSerializer().isPresent()) {
            return new FlinkWriterStateSerializer<>(seaTunnelSink.getWriterStateSerializer().get());
        } else {
            return new EmptyFlinkWriterStateSerializer<>();
        }
    }
}
