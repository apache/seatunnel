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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotPhaseEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SnapshotSplitState;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SourceSplitStateBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;

import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkState;

/**
 * The multi-parallel source reader for table snapshot phase from {@link SnapshotSplit} and then
 * single-parallel source reader for table stream phase from {@link IncrementalSplit}.
 */
@Slf4j
public class IncrementalSourceReader<T, C extends SourceConfig>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecords, T, SourceSplitBase, SourceSplitStateBase> {

    private final Map<String, SnapshotSplit> finishedUnackedSplits;

    private volatile boolean running = false;
    private final int subtaskId;

    private final C sourceConfig;
    private final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;

    private final DataSourceDialect<C> dataSourceDialect;

    private transient volatile Offset snapshotChangeLogOffset;

    private final AtomicBoolean needSendSplitRequest = new AtomicBoolean(false);

    private final AtomicReference<List<CatalogTable>> restoredCheckpointTables =
            new AtomicReference<>();

    public IncrementalSourceReader(
            DataSourceDialect<C> dataSourceDialect,
            BlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue,
            Supplier<IncrementalSourceSplitReader<C>> splitReaderSupplier,
            RecordEmitter<SourceRecords, T, SourceSplitStateBase> recordEmitter,
            SourceReaderOptions options,
            SourceReader.Context context,
            C sourceConfig,
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema) {
        super(
                elementsQueue,
                new SingleThreadFetcherManager<>(elementsQueue, splitReaderSupplier::get),
                recordEmitter,
                options,
                context);
        this.dataSourceDialect = dataSourceDialect;
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.subtaskId = context.getIndexOfSubtask();
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
    }

    @Override
    public void pollNext(Collector<T> output) throws Exception {
        restoreCollectorSchema(output);
        if (!running) {
            if (getNumberOfCurrentlyAssignedSplits() == 0) {
                context.sendSplitRequest();
            }
            running = true;
        }
        if (needSendSplitRequest.get()) {
            context.sendSplitRequest();
            needSendSplitRequest.compareAndSet(true, false);
        }

        if (isNoMoreSplitsAssignment() && isNoMoreElement()) {
            log.info("Reader {} send NoMoreElement event", context.getIndexOfSubtask());
            context.signalNoMoreElement();
        } else {
            super.pollNext(output);
        }
    }

    private void restoreCollectorSchema(Collector<T> output) {
        List<CatalogTable> checkpointTables = restoredCheckpointTables.getAndSet(null);
        if (checkpointTables == null || checkpointTables.isEmpty()) {
            return;
        }
        output.restoreSchema(checkpointTables);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        dataSourceDialect.commitChangeLogOffset(snapshotChangeLogOffset);
    }

    @Override
    public void addSplits(List<SourceSplitBase> splits) {
        // restore for finishedUnackedSplits
        List<SourceSplitBase> unfinishedSplits = new ArrayList<>();
        log.info(
                "subtask {} add splits: {}",
                subtaskId,
                splits.stream().map(SourceSplitBase::splitId).collect(Collectors.joining(",")));
        for (SourceSplitBase split : splits) {
            if (split.isSnapshotSplit()) {
                SnapshotSplit snapshotSplit = split.asSnapshotSplit();
                if (snapshotSplit.isSnapshotReadFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                    log.info(
                            "subtask {} add finished split: {}",
                            subtaskId,
                            snapshotSplit.splitId());
                } else {
                    unfinishedSplits.add(split);
                }
            } else {
                unfinishedSplits.add(split.asIncrementalSplit());
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including incremental split) to SourceReaderBase
        if (!unfinishedSplits.isEmpty()) {
            super.addSplits(unfinishedSplits);
        } else {
            // If the split received is 'isSnapshotReadFinished', we will not run this split, hence
            // we need to send the split request.
            // We cannot directly execute context.sendSplitRequest() here, as it is a synchronous
            // call and can lead to a deadlock.
            needSendSplitRequest.set(true);
        }
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitStateBase> finishedSplitIds) {
        for (SourceSplitStateBase splitState : finishedSplitIds.values()) {
            SourceSplitBase sourceSplit = splitState.toSourceSplit();
            if (sourceSplit.isSnapshotSplit()) {
                checkState(
                        sourceSplit.asSnapshotSplit().isSnapshotReadFinished(),
                        String.format(
                                "Snapshot split should be finished, but the actual split is %s",
                                sourceSplit));
                finishedUnackedSplits.put(sourceSplit.splitId(), sourceSplit.asSnapshotSplit());
            } else {
                log.info(
                        "Incremental split {} has finished (bounded read completed).",
                        sourceSplit.splitId());
            }
        }
        reportFinishedSnapshotSplitsIfNeed();
        context.sendSplitRequest();
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            List<SnapshotSplitWatermark> completedSnapshotSplitWatermarks = new ArrayList<>();

            for (SnapshotSplit split : finishedUnackedSplits.values()) {
                completedSnapshotSplitWatermarks.add(
                        new SnapshotSplitWatermark(
                                split.splitId(),
                                split.getLowWatermark(),
                                split.getHighWatermark()));
            }
            CompletedSnapshotSplitsReportEvent reportEvent =
                    new CompletedSnapshotSplitsReportEvent();
            reportEvent.setCompletedSnapshotSplitWatermarks(completedSnapshotSplitWatermarks);
            context.sendSourceEventToEnumerator(reportEvent);
            // TODO need enumerator return ack
            finishedUnackedSplits.clear();
            log.debug(
                    "The subtask {} reports offsets of finished snapshot splits {}.",
                    subtaskId,
                    completedSnapshotSplitWatermarks);
        }
    }

    @Override
    protected SourceSplitStateBase initializedState(SourceSplitBase split) {
        if (split.isSnapshotSplit()) {
            return new SnapshotSplitState(split.asSnapshotSplit());
        } else {
            IncrementalSplit incrementalSplit = split.asIncrementalSplit();
            List<CatalogTable> checkpointTables =
                    restoreCheckpointState(incrementalSplit, debeziumDeserializationSchema);
            if (!checkpointTables.isEmpty()) {
                restoredCheckpointTables.set(checkpointTables);
            }
            IncrementalSplitState splitState = new IncrementalSplitState(incrementalSplit);
            if (splitState.autoEnterPureIncrementPhaseIfAllowed()) {
                log.info(
                        "The incremental split[{}] startup position {} is equal the maxSnapshotSplitsHighWatermark {}, auto enter pure increment phase.",
                        incrementalSplit.splitId(),
                        splitState.getStartupOffset(),
                        splitState.getMaxSnapshotSplitsHighWatermark());
                log.info("Clean the IncrementalSplit#completedSnapshotSplitInfos to empty.");
                CompletedSnapshotPhaseEvent event =
                        new CompletedSnapshotPhaseEvent(splitState.getTableIds());
                context.sendSourceEventToEnumerator(event);
            }
            return splitState;
        }
    }

    /**
     * Restores schema-evolution state from the current or legacy incremental split checkpoint.
     *
     * <p>Both the Debezium deserializer and the engine collector must restore the same table list.
     * Resolver-less CDC dialects deliberately keep their current table definition, so this method
     * returns no tables for them.
     */
    static <T> List<CatalogTable> restoreCheckpointState(
            IncrementalSplit incrementalSplit,
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema) {
        if (debeziumDeserializationSchema.getSchemaChangeResolver() == null) {
            return Collections.emptyList();
        }

        List<CatalogTable> checkpointTables = incrementalSplit.getCheckpointTables();
        if (checkpointTables != null && !checkpointTables.isEmpty()) {
            log.info(
                    "The incremental split[{}] has checkpoint tables {} for restore.",
                    incrementalSplit.splitId(),
                    checkpointTables);
        } else if (incrementalSplit.getCheckpointDataType() != null) {
            checkpointTables = restoreLegacyCheckpointTables(incrementalSplit);
            if (checkpointTables.isEmpty()) {
                log.warn(
                        "Skip restoring the legacy checkpoint data type for incremental split[{}] because the table identity cannot be recovered from split state.",
                        incrementalSplit.splitId());
            } else {
                log.info(
                        "The incremental split[{}] restores {} legacy checkpoint table(s): {}.",
                        incrementalSplit.splitId(),
                        checkpointTables.size(),
                        toCheckpointTablePaths(checkpointTables));
            }
        }

        if (checkpointTables == null || checkpointTables.isEmpty()) {
            return Collections.emptyList();
        }
        debeziumDeserializationSchema.restoreCheckpointProducedType(checkpointTables);
        return checkpointTables;
    }

    private static List<CatalogTable> restoreLegacyCheckpointTables(
            IncrementalSplit incrementalSplit) {
        if (incrementalSplit.getCheckpointDataType() instanceof MultipleRowType) {
            MultipleRowType checkpointTables =
                    (MultipleRowType) incrementalSplit.getCheckpointDataType();
            return Arrays.stream(checkpointTables.getTableIds())
                    .map(
                            tableId ->
                                    toLegacyCheckpointTable(
                                            tableId, checkpointTables.getRowType(tableId)))
                    .collect(Collectors.toList());
        }

        List<TableId> tableIds = incrementalSplit.getTableIds();
        if (tableIds == null || tableIds.size() != 1) {
            return Collections.emptyList();
        }

        return Collections.singletonList(
                CatalogTableUtil.getCatalogTable(
                        "schema",
                        tableIds.get(0).catalog(),
                        tableIds.get(0).schema(),
                        tableIds.get(0).table(),
                        (SeaTunnelRowType) incrementalSplit.getCheckpointDataType()));
    }

    private static CatalogTable toLegacyCheckpointTable(String tableId, SeaTunnelRowType rowType) {
        TablePath tablePath = TablePath.of(tableId);
        return CatalogTableUtil.getCatalogTable(
                "schema",
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName(),
                rowType);
    }

    private static List<String> toCheckpointTablePaths(List<CatalogTable> checkpointTables) {
        return checkpointTables.stream()
                .map(table -> table.getTablePath().getFullName())
                .collect(Collectors.toList());
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        List<SourceSplitBase> stateSplits = super.snapshotState(checkpointId);

        // unfinished splits
        List<SourceSplitBase> unfinishedSplits =
                stateSplits.stream()
                        .filter(split -> !finishedUnackedSplits.containsKey(split.splitId()))
                        .collect(Collectors.toList());

        // add finished snapshot splits that didn't receive ack yet
        unfinishedSplits.addAll(finishedUnackedSplits.values());

        if (isIncrementalSplitPhase(unfinishedSplits)) {
            IncrementalSplit incrementalSplit = unfinishedSplits.get(0).asIncrementalSplit();
            snapshotChangeLogOffset = incrementalSplit.getStartupOffset();
            return snapshotCheckpointDataType(incrementalSplit);
        }

        return unfinishedSplits;
    }

    @Override
    protected SourceSplitBase toSplitType(String splitId, SourceSplitStateBase splitState) {
        return splitState.toSourceSplit();
    }

    private boolean isIncrementalSplitPhase(List<SourceSplitBase> stateSplits) {
        return stateSplits.size() == 1 && stateSplits.get(0).isIncrementalSplit();
    }

    private List<SourceSplitBase> snapshotCheckpointDataType(IncrementalSplit incrementalSplit) {
        // Snapshot current table struct to checkpoint
        List<CatalogTable> checkpointTables = debeziumDeserializationSchema.getProducedType();

        // Snapshot current history table changes to checkpoint for debezium
        IncrementalSplit newIncrementalSplit =
                new IncrementalSplit(
                        incrementalSplit,
                        checkpointTables,
                        debeziumDeserializationSchema.getHistoryTableChanges());
        log.debug(
                "Snapshot checkpoint datatype {} into split[{}] state.",
                checkpointTables,
                incrementalSplit.splitId());
        return Arrays.asList(newIncrementalSplit);
    }
}
