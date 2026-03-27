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
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Central sink adapter that wraps multiple per-table {@link SeaTunnelSink} instances into a single
 * unified sink. Each table's sink is created from the {@link MultiTableFactoryContext} and managed
 * independently for writing, committing, and state snapshotting.
 *
 * <p>This class multiplies writers per subtask using {@code replicaNum} to fill blocking write
 * queues in {@link MultiTableSinkWriter}, improving throughput for multi-table workloads.
 */
public class MultiTableSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        MultiTableState,
                        MultiTableCommitInfo,
                        MultiTableAggregatedCommitInfo>,
                SupportSchemaEvolutionSink {

    @Getter private final Map<TablePath, SeaTunnelSink> sinks;
    private final int replicaNum;

    /**
     * Constructs a MultiTableSink from the given factory context.
     *
     * <p>The {@code sinks} map is populated directly from {@link
     * MultiTableFactoryContext#getSinks()}, keyed by {@link TablePath}. The {@code replicaNum}
     * controls how many writers are created per table per subtask. Each subtask creates {@code
     * replicaNum} writers to fill the blocking queues in {@link MultiTableSinkWriter}.
     *
     * @param context the factory context containing per-table sinks and configuration options
     */
    public MultiTableSink(MultiTableFactoryContext context) {
        this.sinks = context.getSinks();
        this.replicaNum =
                context.getOptions().get(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA);
    }

    @Override
    public String getPluginName() {
        return "MultiTableSink";
    }

    /**
     * Creates a new {@link MultiTableSinkWriter} with freshly initialized per-table writers.
     *
     * <p>For each table and each replica, a writer is created with a computed index using the
     * formula {@code index = subtaskIndex * replicaNum + i}. This scatters writers across the
     * blocking queues inside {@link MultiTableSinkWriter}, ensuring even distribution of write
     * load.
     *
     * @param context the sink writer context providing subtask index and parallelism info
     * @return a new {@link MultiTableSinkWriter} wrapping all per-table writers
     * @throws IOException if any per-table writer creation fails
     */
    @Override
    public SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState> createWriter(
            SinkWriter.Context context) throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        for (int i = 0; i < replicaNum; i++) {
            for (TablePath tablePath : sinks.keySet()) {
                SeaTunnelSink sink = sinks.get(tablePath);
                int index = context.getIndexOfSubtask() * replicaNum + i;
                String tableIdentifier = tablePath.toString();
                writers.put(
                        SinkIdentifier.of(tableIdentifier, index),
                        sink.createWriter(new SinkContextProxy(index, replicaNum, context)));
                sinkWritersContext.put(SinkIdentifier.of(tableIdentifier, index), context);
            }
        }
        return new MultiTableSinkWriter(writers, replicaNum, sinkWritersContext);
    }

    /**
     * Restores a {@link MultiTableSinkWriter} from previously checkpointed states.
     *
     * <p>Checkpoint states are matched back to per-table writers using {@link SinkIdentifier}
     * (composed of table identifier and computed index). If no matching state is found for a given
     * table and replica, a fresh writer is created instead via {@link
     * SeaTunnelSink#createWriter(SinkWriter.Context)}.
     *
     * @param context the sink writer context providing subtask index and parallelism info
     * @param states the list of checkpoint states from a previous snapshot
     * @return a restored {@link MultiTableSinkWriter} with per-table writers rebuilt from state
     * @throws IOException if any per-table writer restoration fails
     */
    @Override
    public SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState> restoreWriter(
            SinkWriter.Context context, List<MultiTableState> states) throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();

        for (int i = 0; i < replicaNum; i++) {
            for (TablePath tablePath : sinks.keySet()) {
                SeaTunnelSink sink = sinks.get(tablePath);
                int index = context.getIndexOfSubtask() * replicaNum + i;
                SinkIdentifier sinkIdentifier = SinkIdentifier.of(tablePath.toString(), index);
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
                sinkWritersContext.put(sinkIdentifier, context);
            }
        }
        return new MultiTableSinkWriter(writers, replicaNum, sinkWritersContext);
    }

    @Override
    public Optional<Serializer<MultiTableState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    /**
     * Creates a {@link MultiTableSinkCommitter} that aggregates per-table {@link SinkCommitter}
     * instances.
     *
     * <p>Iterates over all registered sinks and collects their committers. If none of the sub-sinks
     * provide a committer, returns {@link Optional#empty()}.
     *
     * @return an optional containing the aggregated committer, or empty if no sub-sink has one
     * @throws IOException if any per-table committer creation fails
     */
    @Override
    public Optional<SinkCommitter<MultiTableCommitInfo>> createCommitter() throws IOException {
        Map<String, SinkCommitter<?>> committers = new HashMap<>();
        for (TablePath tablePath : sinks.keySet()) {
            SeaTunnelSink sink = sinks.get(tablePath);
            sink.createCommitter()
                    .ifPresent(
                            committer ->
                                    committers.put(
                                            tablePath.toString(), (SinkCommitter<?>) committer));
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

    /**
     * Creates a {@link MultiTableSinkAggregatedCommitter} that aggregates per-table {@link
     * SinkAggregatedCommitter} instances across all sub-sinks.
     *
     * <p>If none of the sub-sinks provide an aggregated committer, returns {@link
     * Optional#empty()}.
     *
     * @return an optional containing the aggregated committer, or empty if no sub-sink has one
     * @throws IOException if any per-table aggregated committer creation fails
     */
    @Override
    public Optional<SinkAggregatedCommitter<MultiTableCommitInfo, MultiTableAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        Map<String, SinkAggregatedCommitter<?, ?>> aggCommitters = new HashMap<>();
        for (TablePath tablePath : sinks.keySet()) {
            SeaTunnelSink sink = sinks.get(tablePath);
            Optional<SinkAggregatedCommitter<?, ?>> sinkOptional = sink.createAggregatedCommitter();
            sinkOptional.ifPresent(
                    sinkAggregatedCommitter ->
                            aggCommitters.put(tablePath.toString(), sinkAggregatedCommitter));
        }
        if (aggCommitters.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new MultiTableSinkAggregatedCommitter(aggCommitters));
    }

    /**
     * Returns the list of {@link TablePath}s for all tables managed by this sink.
     *
     * <p>For each sub-sink, tries {@link SeaTunnelSink#getWriteCatalogTable()} first to extract the
     * table path from the catalog table. If that is not present, falls back to using the {@link
     * TablePath} key from the original sinks map.
     *
     * @return the list of table paths for all managed tables
     */
    public List<TablePath> getSinkTables() {

        List<TablePath> tablePaths = new ArrayList<>();
        List<SeaTunnelSink> values = new ArrayList<>(sinks.values());
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i).getWriteCatalogTable().isPresent()) {
                tablePaths.add(
                        ((CatalogTable) values.get(i).getWriteCatalogTable().get()).getTablePath());
            } else {
                tablePaths.add(sinks.keySet().toArray(new TablePath[0])[i]);
            }
        }
        return tablePaths;
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

    /**
     * Always returns empty in multi-table context.
     *
     * <p>In a multi-table sink, catalog tables are managed individually by each sub-sink rather
     * than at the top level. This method delegates to the parent interface default, which returns
     * {@link Optional#empty()}.
     *
     * @return {@link Optional#empty()}, always
     */
    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return SeaTunnelSink.super.getWriteCatalogTable();
    }

    /**
     * Delegates schema evolution support to the first sub-sink.
     *
     * <p>Precondition: the sinks map must contain at least one entry.
     *
     * <p>If the first sub-sink implements {@link SupportSchemaEvolutionSink}, returns its supported
     * {@link SchemaChangeType} list. Otherwise returns an empty list, indicating no schema
     * evolution support.
     *
     * @return the list of supported schema change types, or empty if not supported
     */
    @Override
    public List<SchemaChangeType> supports() {
        SeaTunnelSink firstSink = sinks.entrySet().iterator().next().getValue();
        if (firstSink instanceof SupportSchemaEvolutionSink) {
            return ((SupportSchemaEvolutionSink) firstSink).supports();
        }
        return Collections.emptyList();
    }
}
