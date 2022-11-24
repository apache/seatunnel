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
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile Throwable readException;

    private FetchTask<SourceSplitBase> streamFetchTask;

    private IncrementalSplit currentIncrementalSplit;

    private Offset splitStartWatermark;

    private static final long READER_CLOSE_TIMEOUT_SECONDS = 30L;

    public IncrementalSourceStreamFetcher(FetchTask.Context taskContext, int subTaskId) {
        this.taskContext = taskContext;
        ThreadFactory threadFactory =
            new ThreadFactoryBuilder().setNameFormat("debezium-reader-" + subTaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
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
     */
    private boolean shouldEmit(SourceRecord sourceRecord) {
        if (taskContext.isDataChangeRecord(sourceRecord)) {
            Offset position = taskContext.getStreamOffset(sourceRecord);
            return position.isAtOrAfter(splitStartWatermark);
            // TODO only the table who captured snapshot splits need to filter( Used to support Exactly-Once )
        }
        return true;
    }

    private void configureFilter() {
        splitStartWatermark = currentIncrementalSplit.getStartupOffset();
    }
}
