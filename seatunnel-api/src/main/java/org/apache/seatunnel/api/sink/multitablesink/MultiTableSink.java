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
import org.apache.seatunnel.api.common.multitable.MultiTableFailedTable;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.MultiTableCommonOptions;
import org.apache.seatunnel.api.options.MultiTableFailurePolicy;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
    @Getter private final MultiTableFailurePolicy failurePolicy;
    private final List<MultiTableFailedTable> initialFailedTables;
    private final int tableRetryTimes;
    private final int tableRetryIntervalSeconds;
    private final ReadonlyConfig options;
    private JobContext jobContext;
    /**
     * Builds or restores a sink writer from runtime table metadata without persisting sink config.
     */
    private final MultiTableSinkWriter.RuntimeSinkWriterFactory runtimeSinkWriterFactory =
            new MultiTableSinkWriter.RuntimeSinkWriterFactory() {
                @Override
                public SinkWriter<SeaTunnelRow, ?, ?> create(
                        CatalogTable catalogTable, SinkWriter.Context context) throws IOException {
                    return createRuntimeSinkWriter(catalogTable, context);
                }

                @Override
                public SinkWriter<SeaTunnelRow, ?, ?> restore(
                        CatalogTable catalogTable, SinkWriter.Context context, List<?> states)
                        throws IOException {
                    return restoreRuntimeSinkWriter(catalogTable, context, states);
                }
            };

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
        this.options = context.getOptions();
        this.replicaNum =
                context.getOptions().get(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA);
        this.failurePolicy =
                context.getOptions().get(MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY);
        this.tableRetryTimes = context.getOptions().get(EnvCommonOptions.JOB_RETRY_TIMES);
        this.tableRetryIntervalSeconds =
                context.getOptions().get(EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS);
        this.initialFailedTables =
                new ArrayList<>(
                        MultiTableFailureHelper.getInitialFailedTables(context.getOptions()));
    }

    public List<MultiTableFailedTable> getInitialFailedTables() {
        return Collections.unmodifiableList(initialFailedTables);
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
        Map<SinkIdentifier, SinkContextProxy> proxyContexts = new HashMap<>();
        List<MultiTableSinkWriter.SinkWriterTemplate> sinkWriterTemplates = new ArrayList<>();
        for (int i = 0; i < replicaNum; i++) {
            int index = context.getIndexOfSubtask() * replicaNum + i;
            for (TablePath tablePath : sinks.keySet()) {
                if (shouldSkipFailedTable(initialFailedTables, tablePath)) {
                    continue;
                }
                SeaTunnelSink sink = sinks.get(tablePath);
                SinkIdentifier id = SinkIdentifier.of(tablePath.toString(), index);
                SinkContextProxy proxy = new SinkContextProxy(index, replicaNum, context);
                SinkWriter<SeaTunnelRow, ?, ?> sinkWriter = sink.createWriter(proxy);
                writers.put(id, sinkWriter);
                if (sinkWriterTemplates.size() == i) {
                    sinkWriterTemplates.add(
                            new MultiTableSinkWriter.SinkWriterTemplate(
                                    (SupportMultiTableSinkWriter<?>) sinkWriter, context, index));
                }
                proxyContexts.put(id, proxy);
                sinkWritersContext.put(id, context);
            }
            if (sinkWriterTemplates.size() == i) {
                sinkWriterTemplates.add(
                        new MultiTableSinkWriter.SinkWriterTemplate(null, context, index));
            }
        }
        MultiTableSinkWriter writer =
                new MultiTableSinkWriter(
                        writers,
                        replicaNum,
                        sinkWritersContext,
                        failurePolicy,
                        getJobMode(),
                        initialFailedTables,
                        tableRetryTimes,
                        tableRetryIntervalSeconds,
                        sinkWriterTemplates,
                        runtimeSinkWriterFactory,
                        proxyContexts);
        registerAggregatedFlushIfNeeded(context, writer, proxyContexts);
        return writer;
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
        Map<SinkIdentifier, SinkContextProxy> proxyContexts = new HashMap<>();
        List<MultiTableSinkWriter.SinkWriterTemplate> sinkWriterTemplates = new ArrayList<>();
        List<MultiTableFailedTable> restoredFailedTables =
                states.stream()
                        .map(MultiTableState::getFailedTables)
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
        List<MultiTableFailedTable> effectiveFailedTables = new ArrayList<>(initialFailedTables);
        effectiveFailedTables.addAll(restoredFailedTables);
        List<CatalogTable> restoredRuntimeTables =
                states.stream()
                        .map(MultiTableState::getRuntimeCreatedTables)
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .collect(
                                Collectors.toMap(
                                        CatalogTable::getTablePath,
                                        table -> table,
                                        (left, right) -> left,
                                        LinkedHashMap::new))
                        .values()
                        .stream()
                        .collect(Collectors.toList());

        for (int i = 0; i < replicaNum; i++) {
            int index = context.getIndexOfSubtask() * replicaNum + i;
            for (TablePath tablePath : sinks.keySet()) {
                if (shouldSkipFailedTable(effectiveFailedTables, tablePath)) {
                    continue;
                }
                SeaTunnelSink sink = sinks.get(tablePath);
                SinkIdentifier sinkIdentifier = SinkIdentifier.of(tablePath.toString(), index);
                SinkContextProxy proxy = new SinkContextProxy(index, replicaNum, context);
                List<?> state =
                        states.stream()
                                .map(
                                        multiTableState ->
                                                multiTableState.getStates().get(sinkIdentifier))
                                .filter(Objects::nonNull)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList());
                SinkWriter<SeaTunnelRow, ?, ?> sinkWriter =
                        state.isEmpty()
                                ? sink.createWriter(proxy)
                                : sink.restoreWriter(proxy, state);
                writers.put(sinkIdentifier, sinkWriter);
                if (sinkWriterTemplates.size() == i) {
                    sinkWriterTemplates.add(
                            new MultiTableSinkWriter.SinkWriterTemplate(
                                    (SupportMultiTableSinkWriter<?>) sinkWriter, context, index));
                }
                proxyContexts.put(sinkIdentifier, proxy);
                sinkWritersContext.put(sinkIdentifier, context);
            }
            for (CatalogTable catalogTable : restoredRuntimeTables) {
                TablePath tablePath = catalogTable.getTablePath();
                if (sinks.containsKey(tablePath)
                        || shouldSkipFailedTable(effectiveFailedTables, tablePath)) {
                    continue;
                }
                SinkIdentifier sinkIdentifier = SinkIdentifier.of(tablePath.getFullName(), index);
                SinkContextProxy proxy = new SinkContextProxy(index, replicaNum, context);
                List<?> state =
                        states.stream()
                                .map(
                                        multiTableState ->
                                                multiTableState.getStates().get(sinkIdentifier))
                                .filter(Objects::nonNull)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList());
                SinkWriter<SeaTunnelRow, ?, ?> sinkWriter =
                        state.isEmpty()
                                ? runtimeSinkWriterFactory.create(catalogTable, proxy)
                                : runtimeSinkWriterFactory.restore(catalogTable, proxy, state);
                writers.put(sinkIdentifier, sinkWriter);
                if (sinkWriterTemplates.size() == i) {
                    sinkWriterTemplates.add(
                            new MultiTableSinkWriter.SinkWriterTemplate(
                                    (SupportMultiTableSinkWriter<?>) sinkWriter, context, index));
                }
                proxyContexts.put(sinkIdentifier, proxy);
                sinkWritersContext.put(sinkIdentifier, context);
            }
            if (sinkWriterTemplates.size() == i) {
                sinkWriterTemplates.add(
                        new MultiTableSinkWriter.SinkWriterTemplate(null, context, index));
            }
        }
        MultiTableSinkWriter writer =
                new MultiTableSinkWriter(
                        writers,
                        replicaNum,
                        sinkWritersContext,
                        failurePolicy,
                        getJobMode(),
                        effectiveFailedTables,
                        tableRetryTimes,
                        tableRetryIntervalSeconds,
                        sinkWriterTemplates,
                        runtimeSinkWriterFactory,
                        proxyContexts);
        writer.registerRestoredRuntimeCreatedTables(restoredRuntimeTables);

        registerAggregatedFlushIfNeeded(context, writer, proxyContexts);
        return writer;
    }

    private boolean shouldSkipFailedTable(
            Collection<MultiTableFailedTable> failedTables, TablePath tablePath) {
        if (!failurePolicy.continueOtherTables()
                || failedTables == null
                || failedTables.isEmpty()
                || tablePath == null) {
            return false;
        }
        String tablePathText = tablePath.toString();
        String fullName = tablePath.getFullName();
        return failedTables.stream()
                .map(MultiTableFailedTable::getTablePath)
                .filter(Objects::nonNull)
                .anyMatch(
                        failedTable ->
                                failedTable.equals(tablePathText) || failedTable.equals(fullName));
    }

    /**
     * Builds a per-table writer from the original unresolved sink options. This factory remains
     * available even when no source table matched at startup.
     */
    private SinkWriter<SeaTunnelRow, ?, ?> createRuntimeSinkWriter(
            CatalogTable catalogTable, SinkWriter.Context context) throws IOException {
        return createRuntimeSinkWriter(catalogTable, context, Collections.emptyList());
    }

    /** Restores a runtime-created writer with the state captured under its sink identifier. */
    private SinkWriter<SeaTunnelRow, ?, ?> restoreRuntimeSinkWriter(
            CatalogTable catalogTable, SinkWriter.Context context, List<?> states)
            throws IOException {
        return createRuntimeSinkWriter(catalogTable, context, states);
    }

    /**
     * Resolves the runtime sink factory and delegates writer creation or restoration to that sink.
     *
     * <p>The original unresolved options stay on this runtime object and are never copied into
     * {@link MultiTableState}.
     */
    private SinkWriter<SeaTunnelRow, ?, ?> createRuntimeSinkWriter(
            CatalogTable catalogTable, SinkWriter.Context context, List<?> states)
            throws IOException {
        try {
            String factoryIdentifier = options.get(ConnectorCommonOptions.PLUGIN_NAME);
            ClassLoader runtimeClassLoader = Thread.currentThread().getContextClassLoader();
            TableSinkFactory<?, ?, ?, ?> tableSinkFactory =
                    FactoryUtil.discoverFactory(
                            runtimeClassLoader, TableSinkFactory.class, factoryIdentifier);
            TableSinkFactoryContext factoryContext =
                    TableSinkFactoryContext.replacePlaceholderAndCreate(
                            catalogTable,
                            options,
                            runtimeClassLoader,
                            tableSinkFactory.excludeTablePlaceholderReplaceKeys());
            ConfigValidator.of(factoryContext.getOptions()).validate(tableSinkFactory.optionRule());
            SeaTunnelSink sink = tableSinkFactory.createSink(factoryContext).createSink();
            if (jobContext != null) {
                sink.setJobContext(jobContext);
            }
            return createOrRestoreRuntimeSinkWriter(sink, context, states);
        } catch (Throwable error) {
            if (error instanceof IOException) {
                throw (IOException) error;
            }
            throw new IOException(
                    "Failed to create sink writer for runtime table " + catalogTable.getTablePath(),
                    error);
        }
    }

    /**
     * Delegates runtime writer setup to the sink's correct lifecycle method based on checkpoint
     * state presence.
     *
     * <p>This prevents runtime-created writers from dropping state during a failover restore.
     */
    static SinkWriter<SeaTunnelRow, ?, ?> createOrRestoreRuntimeSinkWriter(
            SeaTunnelSink sink, SinkWriter.Context context, List<?> states) throws IOException {
        return states == null || states.isEmpty()
                ? sink.createWriter(context)
                : sink.restoreWriter(context, states);
    }

    /**
     * Registers an aggregated flush action on the parent context if a sub-writer needs timer flush.
     *
     * <p>The registered action drains all blocking queues and then calls each sub-writer's flush
     * action under the corresponding lock, ensuring safe execution from the engine timer thread.
     */
    private void registerAggregatedFlushIfNeeded(
            SinkWriter.Context context,
            MultiTableSinkWriter writer,
            Map<SinkIdentifier, SinkContextProxy> proxyContexts) {
        boolean anyFlush =
                proxyContexts.values().stream().anyMatch(proxy -> proxy.getFlushAction() != null);
        if (anyFlush) {
            context.registerFlushAction(writer::aggregatedFlush);
        }
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
     * Returns the list of resolved sink {@link TablePath}s for all tables managed by this sink.
     *
     * <p>Delegates to {@link #getSinkTableMapping()} and returns its values as a list.
     *
     * @return the list of resolved sink table paths
     */
    public List<TablePath> getSinkTables() {
        return new ArrayList<>(getSinkTableMapping().values());
    }

    /**
     * Returns a mapping from upstream {@link TablePath} keys to their resolved sink table paths.
     *
     * <p>For each sub-sink, if {@link SeaTunnelSink#getWriteCatalogTable()} is present, the
     * resolved path comes from the catalog table. Otherwise, the upstream key is used as-is.
     *
     * @return a map of upstream table paths to resolved sink table paths
     */
    public Map<TablePath, TablePath> getSinkTableMapping() {
        Map<TablePath, TablePath> mapping = new HashMap<>();
        for (Map.Entry<TablePath, SeaTunnelSink> entry : sinks.entrySet()) {
            if (entry.getValue().getWriteCatalogTable().isPresent()) {
                mapping.put(
                        entry.getKey(),
                        ((CatalogTable) entry.getValue().getWriteCatalogTable().get())
                                .getTablePath());
            } else {
                mapping.put(entry.getKey(), entry.getKey());
            }
        }
        return mapping;
    }

    @Override
    public Optional<Serializer<MultiTableAggregatedCommitInfo>>
            getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
        sinks.values().forEach(sink -> sink.setJobContext(jobContext));
    }

    public void registerInitialFailedTables(Collection<MultiTableFailedTable> failedTables) {
        if (failedTables == null || failedTables.isEmpty()) {
            return;
        }
        Map<String, MultiTableFailedTable> mergedFailedTables = new LinkedHashMap<>();
        initialFailedTables.forEach(
                failedTable -> mergedFailedTables.put(failedTable.getTablePath(), failedTable));
        failedTables.forEach(
                failedTable -> mergedFailedTables.put(failedTable.getTablePath(), failedTable));
        initialFailedTables.clear();
        initialFailedTables.addAll(mergedFailedTables.values());
    }

    public void removeSink(TablePath tablePath) {
        if (tablePath != null) {
            sinks.remove(tablePath);
        }
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

    private JobMode getJobMode() {
        return jobContext == null || jobContext.getJobMode() == null
                ? JobMode.BATCH
                : jobContext.getJobMode();
    }
}
