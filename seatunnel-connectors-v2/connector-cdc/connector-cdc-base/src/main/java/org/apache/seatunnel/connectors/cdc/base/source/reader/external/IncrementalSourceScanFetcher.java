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

package org.apache.seatunnel.connectors.cdc.base.source.reader.external;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isEndWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isHighWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isLowWatermarkEvent;

/**
 * Fetcher to fetch data from table split, the split is the snapshot split {@link SnapshotSplit}.
 */
@Slf4j
public class IncrementalSourceScanFetcher implements Fetcher<SourceRecords, SourceSplitBase> {

    public AtomicBoolean hasNextElement;
    public AtomicBoolean reachEnd;

    private final FetchTask.Context taskContext;
    private final ExecutorService executorService;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile Throwable readException;

    // task to read snapshot for current split
    private FetchTask<SourceSplitBase> snapshotSplitReadTask;
    private SnapshotSplit currentSnapshotSplit;

    private static final long READER_CLOSE_TIMEOUT_SECONDS = 30L;

    public IncrementalSourceScanFetcher(FetchTask.Context taskContext, int subtaskId) {
        this.taskContext = taskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("debezium-snapshot-reader-" + subtaskId)
                        .build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.hasNextElement = new AtomicBoolean(false);
        this.reachEnd = new AtomicBoolean(false);
    }

    @Override
    public void submitTask(FetchTask<SourceSplitBase> fetchTask) {
        this.snapshotSplitReadTask = fetchTask;
        this.currentSnapshotSplit = fetchTask.getSplit().asSnapshotSplit();
        taskContext.configure(currentSnapshotSplit);
        this.queue = taskContext.getQueue();
        this.hasNextElement.set(true);
        this.reachEnd.set(false);
        executorService.submit(
                () -> {
                    try {
                        snapshotSplitReadTask.execute(taskContext);
                    } catch (Exception e) {
                        log.error(
                                String.format(
                                        "Execute snapshot read task for snapshot split %s fail",
                                        currentSnapshotSplit),
                                e);
                        readException = e;
                    }
                });
    }

    @Override
    public boolean isFinished() {
        return currentSnapshotSplit == null
                || !snapshotSplitReadTask.isRunning() && !hasNextElement.get() && reachEnd.get();
    }

    @Override
    public Iterator<SourceRecords> pollSplitRecords()
            throws InterruptedException, SeaTunnelException {
        checkReadException();

        if (hasNextElement.get()) {
            // eg:
            // data input: [low watermark event][snapshot events][high watermark event][change
            // events][end watermark event]
            // data output: [low watermark event][normalized events][high watermark event]
            boolean reachChangeLogStart = false;
            boolean reachChangeLogEnd = false;
            SourceRecord lowWatermark = null;
            SourceRecord highWatermark = null;
            Map<Struct, SourceRecord> outputBuffer = new LinkedHashMap<>();
            while (!reachChangeLogEnd) {
                checkReadException();
                List<DataChangeEvent> batch = queue.poll();
                for (DataChangeEvent event : batch) {
                    SourceRecord record = event.getRecord();
                    if (lowWatermark == null) {
                        lowWatermark = record;
                        assertLowWatermark(lowWatermark);
                        continue;
                    }

                    if (highWatermark == null && isHighWatermarkEvent(record)) {
                        highWatermark = record;
                        // snapshot events capture end
                        if (taskContext.isExactlyOnce()) {
                            // begin to capture binlog events
                            reachChangeLogStart = true;
                            continue;
                        } else {
                            // not support exactly-once, stop the loop
                            reachChangeLogEnd = true;
                            break;
                        }
                    }

                    if (reachChangeLogStart && isEndWatermarkEvent(record)) {
                        // capture to end watermark events, stop the loop
                        reachChangeLogEnd = true;
                        break;
                    }

                    if (!reachChangeLogStart) {
                        outputBuffer.put((Struct) record.key(), record);
                    } else {
                        if (isChangeRecordInChunkRange(record)) {
                            // rewrite overlapping snapshot records through the record key
                            taskContext.rewriteOutputBuffer(outputBuffer, record);
                        }
                    }
                }
            }
            // snapshot split return its data once
            hasNextElement.set(false);

            final List<SourceRecord> normalizedRecords = new ArrayList<>();
            normalizedRecords.add(lowWatermark);
            normalizedRecords.addAll(taskContext.formatMessageTimestamp(outputBuffer.values()));
            normalizedRecords.add(highWatermark);

            final List<SourceRecords> sourceRecordsSet = new ArrayList<>();
            sourceRecordsSet.add(new SourceRecords(normalizedRecords));
            return sourceRecordsSet.iterator();
        }
        // the data has been polled, no more data
        reachEnd.compareAndSet(false, true);
        return null;
    }

    private void assertLowWatermark(SourceRecord lowWatermark) {
        checkState(
                isLowWatermarkEvent(lowWatermark),
                String.format(
                        "The first record should be low watermark signal event, but actual is %s",
                        lowWatermark));
    }

    private void checkReadException() {
        if (readException != null) {
            throw new SeaTunnelException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentSnapshotSplit, readException.getMessage()),
                    readException);
        }
    }

    @Override
    public void close() {
        try {
            if (taskContext != null) {
                taskContext.close();
            }
            if (snapshotSplitReadTask != null) {
                snapshotSplitReadTask.shutdown();
            }
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(
                        READER_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    log.warn(
                            "Failed to close the scan fetcher in {} seconds. Service will execute force close(ExecutorService.shutdownNow)",
                            READER_CLOSE_TIMEOUT_SECONDS);
                    executorService.shutdownNow();
                }
            }
        } catch (Exception e) {
            log.error("Close scan fetcher error", e);
        }
    }

    private boolean isChangeRecordInChunkRange(SourceRecord record) {
        if (taskContext.isDataChangeRecord(record)) {
            // fix the between condition
            return taskContext.isRecordBetween(
                    record,
                    currentSnapshotSplit.getSplitStart(),
                    currentSnapshotSplit.getSplitEnd());
        }
        return false;
    }
}
