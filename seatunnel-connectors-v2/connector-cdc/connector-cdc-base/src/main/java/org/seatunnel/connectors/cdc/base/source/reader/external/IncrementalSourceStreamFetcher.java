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

package org.seatunnel.connectors.cdc.base.source.reader.external;

import org.apache.seatunnel.common.utils.SeaTunnelException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Fetcher to fetch data from table split, the split is the incremental split {@link IncrementalSplit}.
 */
@Slf4j
public class IncrementalSourceStreamFetcher implements Fetcher<SourceRecords, SourceSplitBase> {
    private final FetchTask.Context taskContext;
    private final ExecutorService executorService;
    private final Set<TableId> pureStreamPhaseTables;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile Throwable readException;

    private FetchTask<SourceSplitBase> streamFetchTask;

    private IncrementalSplit currentIncrementalSplit;

    private Map<TableId, Offset> maxSplitHighWatermarkMap;

    private static final long READER_CLOSE_TIMEOUT_SECONDS = 30L;

    public IncrementalSourceStreamFetcher(FetchTask.Context taskContext, int subTaskId) {
        this.taskContext = taskContext;
        ThreadFactory threadFactory =
            new ThreadFactoryBuilder().setNameFormat("debezium-reader-" + subTaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.pureStreamPhaseTables = new HashSet<>();
    }

    @Override
    public void submitTask(FetchTask<SourceSplitBase> fetchTask) {
        this.streamFetchTask = fetchTask;
        this.currentIncrementalSplit = fetchTask.getSplit().asIncrementalSplit();
        configureFilter();
        taskContext.configure(currentIncrementalSplit);
        this.queue = taskContext.getQueue();
        executorService.submit(
            () -> {
                try {
                    streamFetchTask.execute(taskContext);
                } catch (Exception e) {
                    log.error(
                        String.format(
                            "Execute stream read task for incremental split %s fail",
                            currentIncrementalSplit),
                        e);
                    readException = e;
                }
            });
    }

    @Override
    public boolean isFinished() {
        return currentIncrementalSplit == null || !streamFetchTask.isRunning();
    }

    @Override
    public Iterator<SourceRecords> pollSplitRecords() throws InterruptedException {
        checkReadException();
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        if (streamFetchTask.isRunning()) {
            List<DataChangeEvent> batch = queue.poll();
            for (DataChangeEvent event : batch) {
                if (shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                }
            }
        }
        List<SourceRecords> sourceRecordsSet = new ArrayList<>();
        sourceRecordsSet.add(new SourceRecords(sourceRecords));
        return sourceRecordsSet.iterator();
    }

    private void checkReadException() {
        if (readException != null) {
            throw new SeaTunnelException(
                String.format(
                    "Read split %s error due to %s.",
                    currentIncrementalSplit, readException.getMessage()),
                readException);
        }
    }

    @Override
    public void close() {
        try {
            if (executorService != null) {
                executorService.shutdown();
                if (executorService.awaitTermination(
                    READER_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    log.warn(
                        "Failed to close the stream fetcher in {} seconds.",
                        READER_CLOSE_TIMEOUT_SECONDS);
                }
            }
        } catch (Exception e) {
            log.error("Close stream fetcher error", e);
        }
    }

    /**
     * Returns the record should emit or not.
     *
     * <p>The watermark signal algorithm is the incremental split reader only sends the change event that
     * belongs to its finished snapshot splits. For each snapshot split, the change event is valid
     * since the offset is after its high watermark.
     *
     * <pre> E.g: the data input is :
     *    snapshot-split-0 info : [0,    1024) highWatermark0
     *    snapshot-split-1 info : [1024, 2048) highWatermark1
     *  the data output is:
     *  only the change event belong to [0,    1024) and offset is after highWatermark0 should send,
     *  only the change event belong to [1024, 2048) and offset is after highWatermark1 should send.
     * </pre>
     */
    private boolean shouldEmit(SourceRecord sourceRecord) {
        if (taskContext.isDataChangeRecord(sourceRecord)) {
            TableId tableId = taskContext.getTableId(sourceRecord);
            Offset position = taskContext.getStreamOffset(sourceRecord);
            return hasEnterPureStreamPhase(tableId, position);
            // TODO only the table who captured snapshot splits need to filter( Used to support Exactly-Once )
            // not in the monitored splits scope, do not emit
        }
        // always send the schema change event and signal event
        // we need record them to state of SeaTunnel
        return true;
    }

    private boolean hasEnterPureStreamPhase(TableId tableId, Offset position) {
        if (pureStreamPhaseTables.contains(tableId)) {
            return true;
        }
        // the existed tables those have finished snapshot reading
        if (maxSplitHighWatermarkMap.containsKey(tableId)
            && position.isAtOrAfter(maxSplitHighWatermarkMap.get(tableId))) {
            pureStreamPhaseTables.add(tableId);
            return true;
        }

        return !maxSplitHighWatermarkMap.containsKey(tableId)
            && taskContext.getTableFilter().isIncluded(tableId);
    }

    private void configureFilter() {
        Map<TableId, Offset> tableIdOffsetPositionMap = new HashMap<>();
        // latest-offset mode

        for (TableId tableId : currentIncrementalSplit.getTableIds()) {
            tableIdOffsetPositionMap.put(tableId, currentIncrementalSplit.getStartupOffset());
        }
        this.maxSplitHighWatermarkMap = tableIdOffsetPositionMap;
        this.pureStreamPhaseTables.clear();
    }
}
