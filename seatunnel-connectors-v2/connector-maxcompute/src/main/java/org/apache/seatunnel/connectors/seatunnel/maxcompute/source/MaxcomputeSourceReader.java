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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.source.event.MaxcomputeCompletedSplitsReportEvent;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeUtil;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

@Slf4j
/**
 * Reads bounded MaxCompute split batches and reports durable completion to the enumerator.
 *
 * <p>Checkpoint-enabled jobs retain completed splits in reader state until checkpoint success.
 * Batch jobs without checkpoints report in bounded batches because they restart from the start
 * after a failure.
 */
public class MaxcomputeSourceReader implements SourceReader<SeaTunnelRow, MaxcomputeSourceSplit> {
    /** Maximum completion report size for jobs without checkpointing. */
    private static final int COMPLETED_SPLIT_REPORT_BATCH_SIZE = 100;

    /** Source runtime context used for split requests, state, and completion events. */
    private final SourceReader.Context context;

    /** Bounded queue of split batches assigned by the enumerator. */
    private final Queue<MaxcomputeSourceSplit> sourceSplits;

    /** Connector configuration used to open MaxCompute download sessions. */
    private final ReadonlyConfig readonlyConfig;

    private volatile boolean noMoreSplit;

    /** Source metadata required to map a split to its download session and row type. */
    private final Map<TablePath, SourceTableInfo> sourceTableInfos;

    /** Serializes current/completed split state with checkpoint snapshots. */
    private final Object splitStateLock = new Object();

    /** Completed split metadata retained until it can be safely released by the enumerator. */
    private final List<MaxcomputeSourceSplit> completedSplits;

    /** Completed split snapshots keyed by the checkpoint that first recorded them. */
    private final Map<Long, List<MaxcomputeSourceSplit>> completedSplitsByCheckpoint;

    /** Split currently being read and therefore required in a concurrent checkpoint snapshot. */
    private volatile MaxcomputeSourceSplit currentProcessingSplit;

    public MaxcomputeSourceReader(
            ReadonlyConfig readonlyConfig,
            SourceReader.Context context,
            Map<TablePath, SourceTableInfo> sourceTableInfos) {
        this.readonlyConfig = readonlyConfig;
        this.context = context;
        this.sourceSplits = new ConcurrentLinkedDeque<>();
        this.sourceTableInfos = sourceTableInfos;
        this.completedSplits = new ArrayList<>();
        this.completedSplitsByCheckpoint = new HashMap<>();
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        MaxcomputeSourceSplit split = sourceSplits.poll();
        boolean shouldReportCompletedSplits = false;
        if (split != null) {
            synchronized (splitStateLock) {
                currentProcessingSplit = split;
            }
            synchronized (output.getCheckpointLock()) {
                try {
                    TableTunnel.DownloadSession session =
                            MaxcomputeUtil.getDownloadSession(
                                    readonlyConfig,
                                    sourceTableInfos
                                            .get(split.getTablePath())
                                            .getCatalogTable()
                                            .getTablePath(),
                                    sourceTableInfos.get(split.getTablePath()).getPartitionSpec());
                    TunnelRecordReader recordReader =
                            session.openRecordReader(split.getRowStart(), split.getRowNum());
                    log.info("open record reader success");
                    Record record;
                    while ((record = recordReader.read()) != null) {
                        SeaTunnelRow seaTunnelRow =
                                MaxcomputeTypeMapper.getSeaTunnelRowData(
                                        record,
                                        sourceTableInfos
                                                .get(split.getTablePath())
                                                .getCatalogTable()
                                                .getSeaTunnelRowType());
                        seaTunnelRow.setTableId(
                                sourceTableInfos
                                        .get(split.getTablePath())
                                        .getCatalogTable()
                                        .getTablePath()
                                        .toString());
                        output.collect(seaTunnelRow);
                    }
                    recordReader.close();
                } catch (Exception e) {
                    throw new MaxcomputeConnectorException(
                            CommonErrorCodeDeprecated.READER_OPERATION_FAILED, e);
                }
                // Mark completion before releasing the barrier lock so its rows and split state
                // always belong to the same checkpoint.
                shouldReportCompletedSplits = completeSplit(split);
            }
        }
        if (shouldReportCompletedSplits) {
            reportCompletedSplits();
        }
        if (this.sourceSplits.isEmpty()
                && this.noMoreSplit
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            if (!context.isCheckpointEnabled()) {
                reportCompletedSplits();
            }
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded Maxcompute source");
            context.signalNoMoreElement();
        } else if (this.sourceSplits.isEmpty() && !this.noMoreSplit) {
            context.sendSplitRequest();
        }
    }

    @Override
    public List<MaxcomputeSourceSplit> snapshotState(long checkpointId) throws Exception {
        List<MaxcomputeSourceSplit> snapshot;
        synchronized (splitStateLock) {
            List<MaxcomputeSourceSplit> completedSplitsAtCheckpoint =
                    new ArrayList<>(completedSplits);
            if (context.isCheckpointEnabled()) {
                completedSplitsByCheckpoint.put(checkpointId, completedSplitsAtCheckpoint);
            }
            snapshot =
                    new ArrayList<>(
                            sourceSplits.size()
                                    + completedSplitsAtCheckpoint.size()
                                    + (currentProcessingSplit == null ? 0 : 1));
            if (currentProcessingSplit != null) {
                snapshot.add(currentProcessingSplit);
            }
            snapshot.addAll(completedSplitsAtCheckpoint);
        }
        snapshot.addAll(sourceSplits);
        return snapshot;
    }

    @Override
    public void addSplits(List<MaxcomputeSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        this.noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        reportCompletedSplits(checkpointId);
    }

    /** Discards completion markers for a checkpoint that did not become durable. */
    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        synchronized (splitStateLock) {
            completedSplitsByCheckpoint.remove(checkpointId);
        }
    }

    /**
     * Records a completed split until the completion is durable for the current job mode.
     *
     * <p>Checkpoint-enabled jobs retain a finished reader-state marker until checkpoint success;
     * jobs without checkpoints report bounded completion batches immediately.
     *
     * @param split fully processed split
     */
    private boolean completeSplit(MaxcomputeSourceSplit split) {
        synchronized (splitStateLock) {
            if (context.isCheckpointEnabled()) {
                split.setFinished(true);
            }
            completedSplits.add(split);
            currentProcessingSplit = null;
            return !context.isCheckpointEnabled()
                    && completedSplits.size() >= COMPLETED_SPLIT_REPORT_BATCH_SIZE;
        }
    }

    /** Reports all completion references for jobs that do not use checkpoints. */
    private void reportCompletedSplits() {
        List<MaxcomputeSourceSplit> splitsToReport;
        synchronized (splitStateLock) {
            splitsToReport = new ArrayList<>(completedSplits);
        }
        reportCompletedSplits(splitsToReport);
    }

    /**
     * Reports only completed splits included by a successful checkpoint snapshot.
     *
     * <p>Splits completed after that checkpoint's snapshot stay in reader state until a later
     * checkpoint records them, preventing source progress from being skipped during recovery.
     *
     * @param checkpointId successful checkpoint identifier
     */
    private void reportCompletedSplits(long checkpointId) {
        List<MaxcomputeSourceSplit> splitsToReport;
        synchronized (splitStateLock) {
            splitsToReport = completedSplitsByCheckpoint.remove(checkpointId);
        }
        reportCompletedSplits(splitsToReport);
    }

    /**
     * Synchronously reports completed split identifiers before releasing reader-side references.
     *
     * <p>The event operation waits until the enumerator has removed its matching assignment, which
     * keeps the two state snapshots consistent for the next checkpoint.
     *
     * @param splitsToReport completed splits safe to release
     */
    private void reportCompletedSplits(List<MaxcomputeSourceSplit> splitsToReport) {
        if (splitsToReport == null || splitsToReport.isEmpty()) {
            return;
        }

        context.sendSourceEventToEnumerator(
                new MaxcomputeCompletedSplitsReportEvent(
                        splitsToReport.stream()
                                .map(MaxcomputeSourceSplit::splitId)
                                .collect(Collectors.toList())));
        synchronized (splitStateLock) {
            completedSplits.removeAll(splitsToReport);
            completedSplitsByCheckpoint
                    .values()
                    .forEach(checkpointSplits -> checkpointSplits.removeAll(splitsToReport));
        }
    }
}
