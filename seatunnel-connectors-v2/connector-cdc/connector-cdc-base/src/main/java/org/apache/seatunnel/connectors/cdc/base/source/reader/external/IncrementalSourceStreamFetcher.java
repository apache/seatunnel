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
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;

import org.apache.kafka.connect.source.SourceRecord;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Fetcher to fetch data from table split, the split is the incremental split {@link
 * IncrementalSplit}.
 */
@Slf4j
public class IncrementalSourceStreamFetcher implements Fetcher<SourceRecords, SourceSplitBase> {
    private final FetchTask.Context taskContext;
    private final SchemaChangeResolver schemaChangeResolver;
    private final ExecutorService executorService;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile Throwable readException;

    private FetchTask<SourceSplitBase> streamFetchTask;

    private IncrementalSplit currentIncrementalSplit;

    private Offset splitStartWatermark;

    private static final long READER_CLOSE_TIMEOUT_SECONDS = 30L;

    public IncrementalSourceStreamFetcher(
            FetchTask.Context taskContext,
            int subTaskId,
            SchemaChangeResolver schemaChangeResolver) {
        this.taskContext = taskContext;
        this.schemaChangeResolver = schemaChangeResolver;
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
    public Iterator<SourceRecords> pollSplitRecords()
            throws InterruptedException, SeaTunnelException {
        checkReadException();

        Iterator<SourceRecords> sourceRecordsIterator = Collections.emptyIterator();
        if (streamFetchTask.isRunning()) {
            List<DataChangeEvent> batch = queue.poll();
            if (!batch.isEmpty()) {
                if (schemaChangeResolver != null) {
                    sourceRecordsIterator = splitSchemaChangeStream(batch);
                } else {
                    sourceRecordsIterator = splitNormalStream(batch);
                }
            }
        }
        return sourceRecordsIterator;
    }

    private Iterator<SourceRecords> splitNormalStream(List<DataChangeEvent> batchEvents) {
        List<SourceRecord> sourceRecords = new ArrayList<>();
        if (streamFetchTask.isRunning()) {
            for (DataChangeEvent event : batchEvents) {
                if (shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                }
            }
        }
        List<SourceRecords> sourceRecordsSet = new ArrayList<>();
        sourceRecordsSet.add(new SourceRecords(sourceRecords));
        return sourceRecordsSet.iterator();
    }

    /**
     * Split schema change stream.
     *
     * <p>For example 1:
     *
     * <p>Before event batch: [a, b, c, SchemaChangeEvent-1, SchemaChangeEvent-2, d, e]
     *
     * <p>After event batch: [a, b, c, checkpoint-before] [SchemaChangeEvent-1, SchemaChangeEvent-2,
     * checkpoint-after] [d, e]
     *
     * <p>For example 2:
     *
     * <p>Before event batch: [SchemaChangeEvent-1, SchemaChangeEvent-2, a, b, c, d, e]
     *
     * <p>After event batch: [checkpoint-before] [SchemaChangeEvent-1, SchemaChangeEvent-2,
     * checkpoint-after] [a, b, c, d, e]
     */
    private Iterator<SourceRecords> splitSchemaChangeStream(List<DataChangeEvent> batchEvents) {
        List<SourceRecords> sourceRecordsSet = new ArrayList<>();

        List<SourceRecord> sourceRecordList = new ArrayList<>();
        SourceRecord previousRecord = null;
        for (int i = 0; i < batchEvents.size(); i++) {
            DataChangeEvent event = batchEvents.get(i);
            SourceRecord currentRecord = event.getRecord();
            if (!shouldEmit(currentRecord)) {
                continue;
            }
            if (!SourceRecordUtils.isDataChangeRecord(currentRecord)
                    && !SourceRecordUtils.isSchemaChangeEvent(currentRecord)) {
                sourceRecordList.add(currentRecord);
                continue;
            }

            if (SourceRecordUtils.isSchemaChangeEvent(currentRecord)) {
                if (!schemaChangeResolver.support(currentRecord)) {
                    continue;
                }

                if (previousRecord == null) {
                    // add schema-change-before to first
                    sourceRecordList.add(
                            WatermarkEvent.createSchemaChangeBeforeWatermark(currentRecord));
                    sourceRecordsSet.add(new SourceRecords(sourceRecordList));
                    sourceRecordList = new ArrayList<>();
                    sourceRecordList.add(currentRecord);
                } else if (SourceRecordUtils.isSchemaChangeEvent(previousRecord)) {
                    sourceRecordList.add(currentRecord);
                } else {
                    sourceRecordList.add(
                            WatermarkEvent.createSchemaChangeBeforeWatermark(currentRecord));
                    sourceRecordsSet.add(new SourceRecords(sourceRecordList));
                    sourceRecordList = new ArrayList<>();
                    sourceRecordList.add(currentRecord);
                }
            } else if (SourceRecordUtils.isDataChangeRecord(currentRecord)) {
                if (previousRecord == null
                        || SourceRecordUtils.isDataChangeRecord(previousRecord)) {
                    sourceRecordList.add(currentRecord);
                } else {
                    sourceRecordList.add(
                            WatermarkEvent.createSchemaChangeAfterWatermark(currentRecord));
                    sourceRecordsSet.add(new SourceRecords(sourceRecordList));
                    sourceRecordList = new ArrayList<>();
                    sourceRecordList.add(currentRecord);
                }
            }
            previousRecord = currentRecord;
            if (i == batchEvents.size() - 1) {
                if (SourceRecordUtils.isSchemaChangeEvent(currentRecord)) {
                    sourceRecordList.add(
                            WatermarkEvent.createSchemaChangeAfterWatermark(currentRecord));
                }
                sourceRecordsSet.add(new SourceRecords(sourceRecordList));
            }
        }

        if (sourceRecordsSet.size() > 1) {
            log.debug(
                    "Split events stream into {} batches and mark schema checkpoint before/after",
                    sourceRecordsSet.size());
        }

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
            if (taskContext != null) {
                taskContext.close();
            }
            if (streamFetchTask != null) {
                streamFetchTask.shutdown();
            }
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(
                        READER_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    log.warn(
                            "Failed to close the stream fetcher in {} seconds. Service will execute force close(ExecutorService.shutdownNow)",
                            READER_CLOSE_TIMEOUT_SECONDS);
                    executorService.shutdownNow();
                }
            }
        } catch (Exception e) {
            log.error("Close stream fetcher error", e);
        }
    }

    /** Returns the record should emit or not. */
    private boolean shouldEmit(SourceRecord sourceRecord) {
        if (taskContext.isDataChangeRecord(sourceRecord)) {
            Offset position = taskContext.getStreamOffset(sourceRecord);
            // TODO: The sourceRecord from MongoDB CDC and MySQL CDC are inconsistent. For
            // compatibility, the getTableId method is commented out for now.
            // TableId tableId = getTableId(sourceRecord);
            if (!taskContext.isExactlyOnce()) {
                //                log.trace(
                //                        "The table {} is not support exactly-once, so ignore the
                // watermark check",
                //                        tableId);
                return position.isAfter(splitStartWatermark);
            }
            // TODO only the table who captured snapshot splits need to filter( Used to support
            // Exactly-Once )
            return position.isAfter(splitStartWatermark);
        }
        return true;
    }

    private void configureFilter() {
        splitStartWatermark = currentIncrementalSplit.getStartupOffset();
    }
}
