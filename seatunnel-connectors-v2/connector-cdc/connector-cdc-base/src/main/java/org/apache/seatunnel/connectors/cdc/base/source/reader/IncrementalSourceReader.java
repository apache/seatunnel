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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
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

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

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

    public IncrementalSourceReader(
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
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.subtaskId = context.getIndexOfSubtask();
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
    }

    @Override
    public void pollNext(Collector<T> output) throws Exception {
        if (!running) {
            if (getNumberOfCurrentlyAssignedSplits() == 0) {
                context.sendSplitRequest();
            }
            running = true;
        }
        super.pollNext(output);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    @Override
    public void addSplits(List<SourceSplitBase> splits) {
        // restore for finishedUnackedSplits
        List<SourceSplitBase> unfinishedSplits = new ArrayList<>();
        for (SourceSplitBase split : splits) {
            if (split.isSnapshotSplit()) {
                SnapshotSplit snapshotSplit = split.asSnapshotSplit();
                if (snapshotSplit.isSnapshotReadFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
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
        }
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitStateBase> finishedSplitIds) {
        for (SourceSplitStateBase splitState : finishedSplitIds.values()) {
            SourceSplitBase sourceSplit = splitState.toSourceSplit();
            checkState(
                    sourceSplit.isSnapshotSplit(),
                    String.format(
                            "Only snapshot split could finish, but the actual split is incremental split %s",
                            sourceSplit));
            finishedUnackedSplits.put(sourceSplit.splitId(), sourceSplit.asSnapshotSplit());
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
            if (incrementalSplit.getCheckpointDataType() != null) {
                log.info(
                        "The incremental split[{}] has checkpoint datatype {} for restore.",
                        incrementalSplit.splitId(),
                        incrementalSplit.getCheckpointDataType());
                debeziumDeserializationSchema.restoreCheckpointProducedType(
                        incrementalSplit.getCheckpointDataType());
            }
            return new IncrementalSplitState(split.asIncrementalSplit());
        }
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
            return snapshotCheckpointDataType(unfinishedSplits);
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

    private List<SourceSplitBase> snapshotCheckpointDataType(List<SourceSplitBase> stateSplits) {
        if (!isIncrementalSplitPhase(stateSplits)) {
            throw new IllegalStateException(
                    "The splits should be incremental split when snapshot  checkpoint datatype");
        }
        IncrementalSplit incrementalSplit = stateSplits.get(0).asIncrementalSplit();
        // Snapshot current datatype to checkpoint
        SeaTunnelDataType<T> checkpointDataType = debeziumDeserializationSchema.getProducedType();
        IncrementalSplit newIncrementalSplit =
                new IncrementalSplit(incrementalSplit, checkpointDataType);
        log.debug(
                "Snapshot checkpoint datatype {} into split[{}] state.",
                checkpointDataType,
                incrementalSplit.splitId());
        return Arrays.asList(newIncrementalSplit);
    }
}
