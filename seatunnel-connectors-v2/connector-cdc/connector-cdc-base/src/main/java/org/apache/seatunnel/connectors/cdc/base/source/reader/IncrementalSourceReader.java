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

import static com.google.common.base.Preconditions.checkState;

import org.apache.seatunnel.api.source.SourceReader;
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
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

/**
 * The multi-parallel source reader for table snapshot phase from {@link SnapshotSplit} and then
 * single-parallel source reader for table stream phase from {@link IncrementalSplit}.
 */
@Slf4j
public class IncrementalSourceReader<T, C extends SourceConfig>
        extends SingleThreadMultiplexSourceReaderBase<
    SourceRecords, T, SourceSplitBase, SourceSplitStateBase> {

    private final Map<String, SnapshotSplit> finishedUnackedSplits;

    private final Map<String, IncrementalSplit> uncompletedIncrementalSplits;

    private final int subtaskId;

    private final C sourceConfig;

    public IncrementalSourceReader(
        BlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue,
        Supplier<IncrementalSourceSplitReader<C>> splitReaderSupplier,
        RecordEmitter<SourceRecords, T, SourceSplitStateBase> recordEmitter,
        SourceReaderOptions options,
        SourceReader.Context context,
        C sourceConfig) {
        super(
            elementsQueue,
            new SingleThreadFetcherManager<>(elementsQueue, splitReaderSupplier::get),
            recordEmitter,
            options,
            context);
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.uncompletedIncrementalSplits = new HashMap<>();
        this.subtaskId = context.getIndexOfSubtask();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

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
                // the incremental split is uncompleted
                uncompletedIncrementalSplits.put(split.splitId(), split.asIncrementalSplit());
                unfinishedSplits.add(split.asIncrementalSplit());
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including incremental split) to SourceReaderBase
        super.addSplits(unfinishedSplits);
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
                completedSnapshotSplitWatermarks.add(new SnapshotSplitWatermark(split.splitId(), split.getHighWatermark()));
            }
            CompletedSnapshotSplitsReportEvent reportEvent = new CompletedSnapshotSplitsReportEvent();
            reportEvent.setCompletedSnapshotSplitWatermarks(completedSnapshotSplitWatermarks);
            context.sendSourceEventToEnumerator(reportEvent);
            //TODO need enumerator return ack
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
            return new IncrementalSplitState(split.asIncrementalSplit());
        }
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        // unfinished splits
        List<SourceSplitBase> stateSplits = super.snapshotState(checkpointId);

        // add finished snapshot splits that didn't receive ack yet
        stateSplits.addAll(finishedUnackedSplits.values());

        // add incremental splits who are uncompleted
        stateSplits.addAll(uncompletedIncrementalSplits.values());

        return stateSplits;
    }

    @Override
    protected SourceSplitBase toSplitType(String splitId, SourceSplitStateBase splitState) {
        return splitState.toSourceSplit();
    }
}
