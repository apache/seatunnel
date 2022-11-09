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

package org.apache.seatunnel.connectors.seatunnel.cdc.base.source.reader;

import static com.google.common.base.Preconditions.checkState;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.events.FinishedSnapshotSplitsAckEvent;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.events.FinishedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.events.FinishedSnapshotSplitsRequestEvent;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.events.StreamSplitMetaEvent;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.events.StreamSplitMetaRequestEvent;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.offset.Offset;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SnapshotSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SnapshotSplitState;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SourceRecords;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SourceSplitState;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.StreamSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.StreamSplitState;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * The multi-parallel source reader for table snapshot phase from {@link SnapshotSplit} and then
 * single-parallel source reader for table stream phase from {@link StreamSplit}.
 */
@Slf4j
public class IncrementalSourceReader<T, C extends SourceConfig>
        extends SingleThreadMultiplexSourceReaderBase<
            SourceRecords, T, SourceSplitBase, SourceSplitState> {
    private final Map<String, SnapshotSplit> finishedUnackedSplits;
    private final Map<String, StreamSplit> uncompletedStreamSplits;
    private final int subtaskId;
    private final DefaultSerializer<FinishedSnapshotSplitInfo> sourceSplitSerializer;
    private final C sourceConfig;
    private final DataSourceDialect<C> dialect;

    public IncrementalSourceReader(
            BlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue,
            Supplier<IncrementalSourceSplitReader<C>> splitReaderSupplier,
            RecordEmitter<SourceRecords, T, SourceSplitState> recordEmitter,
            SourceReaderOptions options,
            SourceReader.Context context,
            C sourceConfig,
            DataSourceDialect<C> dialect) {
        super(
                elementsQueue,
                new SingleThreadFetcherManager<>(elementsQueue, splitReaderSupplier::get),
                recordEmitter,
                options,
                context);
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.uncompletedStreamSplits = new HashMap<>();
        this.subtaskId = context.getIndexOfSubtask();
        this.sourceSplitSerializer = new DefaultSerializer<FinishedSnapshotSplitInfo>();
        this.dialect = dialect;
    }

    @Override
    public void open() {
        super.open();
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        if (split.isSnapshotSplit()) {
            return new SnapshotSplitState(split.asSnapshotSplit());
        } else {
            return new StreamSplitState(split.asStreamSplit());
        }
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        // unfinished splits
        List<SourceSplitBase> stateSplits = super.snapshotState(checkpointId);

        // add finished snapshot splits that didn't receive ack yet
        stateSplits.addAll(finishedUnackedSplits.values());

        // add stream splits who are uncompleted
        stateSplits.addAll(uncompletedStreamSplits.values());

        return stateSplits;
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitState> finishedSplitIds) {
        for (SourceSplitState splitState : finishedSplitIds.values()) {
            SourceSplitBase sourceSplit = splitState.toSourceSplit();
            checkState(
                    sourceSplit.isSnapshotSplit(),
                    String.format(
                            "Only snapshot split could finish, but the actual split is stream split %s",
                            sourceSplit));
            finishedUnackedSplits.put(sourceSplit.splitId(), sourceSplit.asSnapshotSplit());
        }
        reportFinishedSnapshotSplitsIfNeed();
        context.sendSplitRequest();
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
                // the stream split is uncompleted
                if (!split.asStreamSplit().isCompletedSplit()) {
                    uncompletedStreamSplits.put(split.splitId(), split.asStreamSplit());
                    requestStreamSplitMetaIfNeeded(split.asStreamSplit());
                } else {
                    uncompletedStreamSplits.remove(split.splitId());
                    StreamSplit streamSplit =
                            discoverTableSchemasForStreamSplit(split.asStreamSplit());
                    unfinishedSplits.add(streamSplit);
                }
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including stream split) to SourceReaderBase
        super.addSplits(unfinishedSplits);
    }

    private StreamSplit discoverTableSchemasForStreamSplit(StreamSplit split) {
        final String splitId = split.splitId();
        if (split.getTableSchemas().isEmpty()) {
            try {
                Map<TableId, TableChanges.TableChange> tableSchemas =
                        dialect.discoverDataCollectionSchemas(sourceConfig);
                log.info("The table schema discovery for stream split {} success", splitId);
                return StreamSplit.fillTableSchemas(split, tableSchemas);
            } catch (Exception e) {
                log.error("Failed to obtains table schemas due to {}", e.getMessage());
                throw new SeaTunnelException(e);
            }
        } else {
            log.warn(
                    "The stream split {} has table schemas yet, skip the table schema discovery",
                    split);
            return split;
        }
    }

    @Override
    public void handleSourceEvent(SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsAckEvent) {
            FinishedSnapshotSplitsAckEvent ackEvent = (FinishedSnapshotSplitsAckEvent) sourceEvent;
            log.debug(
                    "The subtask {} receives ack event for {} from enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedUnackedSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
            // report finished snapshot splits
            log.debug(
                    "The subtask {} receives request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplitsIfNeed();
        } else if (sourceEvent instanceof StreamSplitMetaEvent) {
            log.debug(
                    "The subtask {} receives stream meta with group id {}.",
                    subtaskId,
                    ((StreamSplitMetaEvent) sourceEvent).getMetaGroupId());
            fillMetaDataForStreamSplit((StreamSplitMetaEvent) sourceEvent);
        } else {
            super.handleSourceEvent(sourceEvent);
        }
    }

    private void fillMetaDataForStreamSplit(StreamSplitMetaEvent metadataEvent) {
        StreamSplit streamSplit = uncompletedStreamSplits.get(metadataEvent.getSplitId());
        if (streamSplit != null) {
            final int receivedMetaGroupId = metadataEvent.getMetaGroupId();
            final int expectedMetaGroupId =
                getNextMetaGroupId(
                    streamSplit.getFinishedSnapshotSplitInfos().size(),
                    sourceConfig.getSplitMetaGroupSize());
            if (receivedMetaGroupId == expectedMetaGroupId) {
                List<FinishedSnapshotSplitInfo> metaDataGroup =
                    metadataEvent.getMetaGroup().stream()
                        .map(bytes -> {
                            try {
                                return sourceSplitSerializer.deserialize(bytes);
                            } catch (IOException e) {
                                throw new SeaTunnelException(e);
                            }
                        })
                        .collect(Collectors.toList());
                uncompletedStreamSplits.put(
                    streamSplit.splitId(),
                    StreamSplit.appendFinishedSplitInfos(streamSplit, metaDataGroup));

                log.info("Fill meta data of group {} to stream split", metaDataGroup.size());
            } else {
                log.warn(
                    "Received out of oder metadata event for split {}, the received meta group id is {}, but expected is {}, ignore it",
                    metadataEvent.getSplitId(),
                    receivedMetaGroupId,
                    expectedMetaGroupId);
            }
            requestStreamSplitMetaIfNeeded(streamSplit);
        } else {
            log.warn(
                "Received metadata event for split {}, but the uncompleted split map does not contain it",
                metadataEvent.getSplitId());
        }
    }

    private void requestStreamSplitMetaIfNeeded(StreamSplit streamSplit) {
        final String splitId = streamSplit.splitId();
        if (!streamSplit.isCompletedSplit()) {
            final int nextMetaGroupId =
                    getNextMetaGroupId(
                            streamSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            StreamSplitMetaRequestEvent splitMetaRequestEvent =
                    new StreamSplitMetaRequestEvent(splitId, nextMetaGroupId);
            context.sendSourceEventToEnumerator(splitMetaRequestEvent);
        } else {
            log.info("The meta of stream split {} has been collected success", splitId);
            this.addSplits(Collections.singletonList(streamSplit));
        }
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<String, Offset> finishedOffsets = new HashMap<>();
            for (SnapshotSplit split : finishedUnackedSplits.values()) {
                finishedOffsets.put(split.splitId(), split.getHighWatermark());
            }
            FinishedSnapshotSplitsReportEvent reportEvent =
                    new FinishedSnapshotSplitsReportEvent(finishedOffsets);
            context.sendSourceEventToEnumerator(reportEvent);
            log.debug(
                    "The subtask {} reports offsets of finished snapshot splits {}.",
                    subtaskId,
                    finishedOffsets);
        }
    }

    /** Returns next meta group id according to received meta number and meta group size. */
    public static int getNextMetaGroupId(int receivedMetaNum, int metaGroupSize) {
        checkState(metaGroupSize > 0);
        return receivedMetaNum % metaGroupSize == 0 ? (receivedMetaNum / metaGroupSize)
                : (receivedMetaNum / metaGroupSize) + 1;
    }

    @Override
    protected SourceSplitBase toSplitType(String splitId, SourceSplitState splitState) {
        return splitState.toSourceSplit();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
