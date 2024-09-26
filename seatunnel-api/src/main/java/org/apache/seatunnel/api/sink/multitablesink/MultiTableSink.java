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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkCommonOptions;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.Getter;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class MultiTableSink
        implements SeaTunnelSink<
                SeaTunnelRow,
                MultiTableState,
                MultiTableCommitInfo,
                MultiTableAggregatedCommitInfo> {

    @Getter private final Map<String, SeaTunnelSink> sinks;
    private final int replicaNum;
    // private TaskMetricsCalcContext taskMetricsCalcContext;

    public MultiTableSink(MultiTableFactoryContext context) {
        this.sinks = context.getSinks();
        this.replicaNum = context.getOptions().get(SinkCommonOptions.MULTI_TABLE_SINK_REPLICA);
    }

    @Override
    public String getPluginName() {
        return "MultiTableSink";
    }

    @Override
    public SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState> createWriter(
            SinkWriter.Context context) throws IOException {

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        for (int i = 0; i < replicaNum; i++) {
            for (String tableIdentifier : sinks.keySet()) {
                SeaTunnelSink sink = sinks.get(tableIdentifier);
                int index = context.getIndexOfSubtask() * replicaNum + i;
                writers.put(
                        SinkIdentifier.of(tableIdentifier, index),
                        sink.createWriter(new SinkContextProxy(index, replicaNum, context)));
                sinkWritersContext.put(SinkIdentifier.of(tableIdentifier, index), context);
            }
        }
        return new MultiTableSinkWriter(writers, replicaNum, sinkWritersContext);
    }

    @Override
    public SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState> restoreWriter(
            SinkWriter.Context context, List<MultiTableState> states) throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();

        for (int i = 0; i < replicaNum; i++) {
            for (String tableIdentifier : sinks.keySet()) {
                SeaTunnelSink sink = sinks.get(tableIdentifier);
                int index = context.getIndexOfSubtask() * replicaNum + i;
                SinkIdentifier sinkIdentifier = SinkIdentifier.of(tableIdentifier, index);
                List<?> state =
                        states.stream()
                                .map(
                                        multiTableState ->
                                                multiTableState.getStates().get(sinkIdentifier))
                                .filter(Objects::nonNull)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList());
                if (state.isEmpty()) {
                    writers.put(
                            sinkIdentifier,
                            sink.createWriter(new SinkContextProxy(index, replicaNum, context)));
                } else {
                    writers.put(
                            sinkIdentifier,
                            sink.restoreWriter(
                                    new SinkContextProxy(index, replicaNum, context), state));
                }
                sinkWritersContext.put(SinkIdentifier.of(tableIdentifier, index), context);
            }
        }
        return new MultiTableSinkWriter(writers, replicaNum, sinkWritersContext);
    }

    @Override
    public Optional<Serializer<MultiTableState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkCommitter<MultiTableCommitInfo>> createCommitter() throws IOException {
        Map<String, SinkCommitter<?>> committers = new HashMap<>();
        for (String tableIdentifier : sinks.keySet()) {
            SeaTunnelSink sink = sinks.get(tableIdentifier);
            sink.createCommitter()
                    .ifPresent(
                            committer ->
                                    committers.put(tableIdentifier, (SinkCommitter<?>) committer));
        }
        if (committers.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new MultiTableSinkCommitter(committers));
    }

    @Override
    public Optional<Serializer<MultiTableCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkAggregatedCommitter<MultiTableCommitInfo, MultiTableAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        Map<String, SinkAggregatedCommitter<?, ?>> aggCommitters = new HashMap<>();
        for (String tableIdentifier : sinks.keySet()) {
            SeaTunnelSink sink = sinks.get(tableIdentifier);
            Optional<SinkAggregatedCommitter<?, ?>> sinkOptional = sink.createAggregatedCommitter();
            sinkOptional.ifPresent(
                    sinkAggregatedCommitter ->
                            aggCommitters.put(tableIdentifier, sinkAggregatedCommitter));
        }
        if (aggCommitters.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new MultiTableSinkAggregatedCommitter(aggCommitters));
    }

    public List<TablePath> getSinkTables() {
        return sinks.keySet().stream().map(TablePath::of).collect(Collectors.toList());
    }

    @Override
    public Optional<Serializer<MultiTableAggregatedCommitInfo>>
            getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        sinks.values().forEach(sink -> sink.setJobContext(jobContext));
    }
}
