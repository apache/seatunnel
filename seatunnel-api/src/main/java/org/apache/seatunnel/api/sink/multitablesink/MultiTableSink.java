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
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Central sink adapter that wraps multiple per-table {@link SeaTunnelSink} instances into a single
 * unified sink. Each table's sink is created from the {@link MultiTableFactoryContext} and managed
 * independently for writing, committing, and state snapshotting.
 *
 * <p>This class multiplies writers per subtask using {@code replicaNum} to fill blocking write
 * queues in {@link MultiTableSinkWriter}, improving throughput for multi-table workloads.
 *
 * <p>Optimization: When multiple source tables resolve to the same destination table, only one
 * SinkWriter is created per replica instead of one per source table, avoiding hundreds of redundant
 * connections.
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
    private JobContext jobContext;

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
     * <p>Optimized to detect cases where compatible static-schema source tables map to the same
     * destination (for example, file-sink aliases that share one path). In such cases, only one
     * SinkWriter is created per destination per replica, preventing redundant connections.
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

        Map<DestinationKey, SinkWriter<SeaTunnelRow, ?, ?>> destinationWriters = new HashMap<>();
        Map<DestinationKey, SinkContextProxy> destinationProxyContexts = new HashMap<>();
        Map<SinkIdentifier, SinkContextProxy> proxyContexts = new HashMap<>();
        try {
            for (int i = 0; i < replicaNum; i++) {
                for (TablePath tablePath : sinks.keySet()) {
                    if (shouldSkipFailedTable(initialFailedTables, tablePath)) {
                        continue;
                    }
                    SeaTunnelSink sink = sinks.get(tablePath);
                    int index = context.getIndexOfSubtask() * replicaNum + i;
                    SinkIdentifier id = SinkIdentifier.of(tablePath.toString(), index);
                    DestinationKey destinationKey = getDestinationKey(tablePath, i);
                    SinkContextProxy proxy =
                            destinationProxyContexts.computeIfAbsent(
                                    destinationKey,
                                    key -> new SinkContextProxy(index, replicaNum, context));
                    SinkWriter<SeaTunnelRow, ?, ?> writer = destinationWriters.get(destinationKey);
                    if (writer == null) {
                        writer = sink.createWriter(proxy);
                        destinationWriters.put(destinationKey, writer);
                    }
                    writers.put(id, writer);
                    // Every alias must retain the shared proxy so listener and metric lookups stay
                    // aligned with the writer and context maps.
                    proxyContexts.put(id, proxy);
                    sinkWritersContext.put(id, context);
                }
            }
        } catch (IOException error) {
            closeCreatedWriters(destinationWriters.values(), error);
            throw error;
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
                        tableRetryIntervalSeconds);
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
     * <p>Uses the same deduplication optimization as {@link #createWriter(SinkWriter.Context)}:
     * when multiple source tables share one destination table, the checkpointed state of every
     * aliased identifier is merged before being handed to {@link
     * SeaTunnelSink#restoreWriter(SinkWriter.Context, List)} so no state is lost.
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
        List<MultiTableFailedTable> restoredFailedTables =
                states.stream()
                        .map(MultiTableState::getFailedTables)
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
        List<MultiTableFailedTable> effectiveFailedTables = new ArrayList<>(initialFailedTables);
        effectiveFailedTables.addAll(restoredFailedTables);

        Map<DestinationKey, SinkWriter<SeaTunnelRow, ?, ?>> destinationWriters = new HashMap<>();
        Map<DestinationKey, SinkContextProxy> destinationProxyContexts = new HashMap<>();
        // Pre-compute all SinkIdentifiers per destination key so the restore path can merge state
        // from legacy per-alias checkpoints before handing it to the shared destination writer.
        Map<DestinationKey, List<SinkIdentifier>> identifiersByDestinationKey =
                new LinkedHashMap<>();
        for (int i = 0; i < replicaNum; i++) {
            for (TablePath tablePath : sinks.keySet()) {
                if (shouldSkipFailedTable(effectiveFailedTables, tablePath)) {
                    continue;
                }
                int index = context.getIndexOfSubtask() * replicaNum + i;
                identifiersByDestinationKey
                        .computeIfAbsent(getDestinationKey(tablePath, i), key -> new ArrayList<>())
                        .add(SinkIdentifier.of(tablePath.toString(), index));
            }
        }

        try {
            for (int i = 0; i < replicaNum; i++) {
                for (TablePath tablePath : sinks.keySet()) {
                    if (shouldSkipFailedTable(effectiveFailedTables, tablePath)) {
                        continue;
                    }
                    SeaTunnelSink sink = sinks.get(tablePath);
                    int index = context.getIndexOfSubtask() * replicaNum + i;
                    SinkIdentifier sinkIdentifier = SinkIdentifier.of(tablePath.toString(), index);
                    DestinationKey destinationKey = getDestinationKey(tablePath, i);
                    SinkContextProxy proxy =
                            destinationProxyContexts.computeIfAbsent(
                                    destinationKey,
                                    key -> new SinkContextProxy(index, replicaNum, context));
                    SinkWriter<SeaTunnelRow, ?, ?> writer = destinationWriters.get(destinationKey);
                    if (writer == null) {
                        List<?> state =
                                getRestoredState(
                                        states,
                                        identifiersByDestinationKey.getOrDefault(
                                                destinationKey, Collections.emptyList()));
                        writer =
                                state.isEmpty()
                                        ? sink.createWriter(proxy)
                                        : sink.restoreWriter(proxy, state);
                        destinationWriters.put(destinationKey, writer);
                    }
                    writers.put(sinkIdentifier, writer);
                    // The shared proxy is intentionally reachable through every alias, but its
                    // action is invoked once per writer by MultiTableSinkWriter.
                    proxyContexts.put(sinkIdentifier, proxy);
                    sinkWritersContext.put(sinkIdentifier, context);
                }
            }
        } catch (IOException error) {
            closeCreatedWriters(destinationWriters.values(), error);
            throw error;
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
                        tableRetryIntervalSeconds);

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
     * Registers an aggregated flush action on the parent context if any sub-writer registered a
     * flush action via its {@link SinkContextProxy}.
     *
     * <p>The registered action drains all blocking queues and then calls each sub-writer's flush
     * action under the corresponding lock, ensuring safe execution from the engine timer thread.
     */
    private void registerAggregatedFlushIfNeeded(
            SinkWriter.Context context,
            MultiTableSinkWriter writer,
            Map<SinkIdentifier, SinkContextProxy> proxyContexts) {
        boolean anyFlush =
                proxyContexts.values().stream().anyMatch(p -> p.getFlushAction() != null);
        if (anyFlush) {
            context.registerFlushAction(() -> writer.aggregatedFlush(proxyContexts));
        }
    }

    /**
     * Collects all pre-optimization per-alias states that belong to a physical destination.
     *
     * <p>New checkpoints store a shared writer state under one canonical identifier. Earlier
     * checkpoints can contain a distinct state list for each alias, which still must be restored
     * together.
     *
     * @param states all states restored for this writer subtask
     * @param identifiers aliases that resolve to one physical destination and replica
     * @return the combined state passed to one restored writer
     */
    private List<?> getRestoredState(
            List<MultiTableState> states, Collection<SinkIdentifier> identifiers) {
        return identifiers.stream()
                .flatMap(
                        identifier ->
                                states.stream()
                                        .map(
                                                multiTableState ->
                                                        multiTableState.getStates().get(identifier))
                                        .filter(Objects::nonNull)
                                        .flatMap(Collection::stream))
                .collect(Collectors.toList());
    }

    /**
     * Closes successfully created unique writers when later initialization fails.
     *
     * @param writers writers that completed creation before the failure
     * @param failure the initialization failure to preserve for the caller
     */
    private void closeCreatedWriters(
            Collection<SinkWriter<SeaTunnelRow, ?, ?>> writers, IOException failure) {
        Set<SinkWriter<SeaTunnelRow, ?, ?>> uniqueWriters =
                Collections.newSetFromMap(new IdentityHashMap<>());
        for (SinkWriter<SeaTunnelRow, ?, ?> writer : writers) {
            if (uniqueWriters.add(writer)) {
                try {
                    writer.close();
                } catch (Throwable closeError) {
                    failure.addSuppressed(closeError);
                }
            }
        }
    }

    /**
     * Builds the destination and replica key used for writer and committer reuse.
     *
     * <p>A connector that opts in supplies a stable physical destination identifier. The connector
     * class is part of that key so identifiers from different connector implementations can never
     * share a writer. A connector that does not opt in remains isolated by sink instance identity.
     *
     * @param tablePath source table associated with the sink instance
     * @param replicaIndex replica index for the destination writer
     * @return a collision-safe physical destination key
     */
    private DestinationKey getDestinationKey(TablePath tablePath, int replicaIndex) {
        SeaTunnelSink<?, ?, ?, ?> sink = sinks.get(tablePath);
        return new DestinationKey(sink, sink.getPhysicalDestinationIdentifier(), replicaIndex);
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
        Map<DestinationKey, Optional<SinkCommitter<?>>> destinationCommitters = new HashMap<>();
        for (TablePath tablePath : sinks.keySet()) {
            SeaTunnelSink sink = sinks.get(tablePath);
            DestinationKey destinationKey = getDestinationKey(tablePath, 0);
            Optional<SinkCommitter<?>> committer = destinationCommitters.get(destinationKey);
            if (committer == null) {
                committer = sink.createCommitter().map(value -> (SinkCommitter<?>) value);
                destinationCommitters.put(destinationKey, committer);
            }
            committer.ifPresent(value -> committers.put(tablePath.toString(), value));
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
        Map<DestinationKey, Optional<SinkAggregatedCommitter<?, ?>>> destinationCommitters =
                new HashMap<>();
        for (TablePath tablePath : sinks.keySet()) {
            SeaTunnelSink sink = sinks.get(tablePath);
            DestinationKey destinationKey = getDestinationKey(tablePath, 0);
            Optional<SinkAggregatedCommitter<?, ?>> committer =
                    destinationCommitters.get(destinationKey);
            if (committer == null) {
                committer = sink.createAggregatedCommitter();
                destinationCommitters.put(destinationKey, committer);
            }
            committer.ifPresent(value -> aggCommitters.put(tablePath.toString(), value));
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

    /**
     * Key for one physical destination writer or committer replica.
     *
     * <p>Opt-in identifiers use connector class plus connector-provided destination identity. The
     * fallback compares sink objects by reference, so identity hash collisions remain harmless.
     */
    private static final class DestinationKey {

        /** Sink retained for identity-isolated keys. */
        private final SeaTunnelSink<?, ?, ?, ?> sink;

        /** Connector-provided physical destination identifier, if the sink opts in. */
        private final String physicalDestinationIdentifier;

        /** Writer replica that must remain independent from other replicas. */
        private final int replicaIndex;

        private DestinationKey(
                SeaTunnelSink<?, ?, ?, ?> sink,
                Optional<String> physicalDestinationIdentifier,
                int replicaIndex) {
            this.sink = sink;
            this.physicalDestinationIdentifier = physicalDestinationIdentifier.orElse(null);
            this.replicaIndex = replicaIndex;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof DestinationKey)) {
                return false;
            }
            DestinationKey that = (DestinationKey) object;
            if (replicaIndex != that.replicaIndex) {
                return false;
            }
            if (physicalDestinationIdentifier == null
                    || that.physicalDestinationIdentifier == null) {
                return physicalDestinationIdentifier == null
                        && that.physicalDestinationIdentifier == null
                        && sink == that.sink;
            }
            return sink.getClass().equals(that.sink.getClass())
                    && physicalDestinationIdentifier.equals(that.physicalDestinationIdentifier);
        }

        @Override
        public int hashCode() {
            if (physicalDestinationIdentifier == null) {
                return 31 * System.identityHashCode(sink) + replicaIndex;
            }
            return Objects.hash(sink.getClass(), physicalDestinationIdentifier, replicaIndex);
        }
    }

    private JobMode getJobMode() {
        return jobContext == null || jobContext.getJobMode() == null
                ? JobMode.BATCH
                : jobContext.getJobMode();
    }
}
