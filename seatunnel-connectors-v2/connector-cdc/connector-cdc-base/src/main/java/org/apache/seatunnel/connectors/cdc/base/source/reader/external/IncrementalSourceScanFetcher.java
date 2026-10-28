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

import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isEndWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isHighWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkState;

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
    private final Supplier<SnapshotBuffer> bufferFactory;
    private volatile SnapshotState snapshotState;
    private volatile SourceRecords pendingSnapshot;
    private volatile boolean closed;

    // task to read snapshot for current split
    private FetchTask<SourceSplitBase> snapshotSplitReadTask;
    private SnapshotSplit currentSnapshotSplit;

    private static final long READER_CLOSE_TIMEOUT_SECONDS = 30L;

    public IncrementalSourceScanFetcher(FetchTask.Context taskContext, int subtaskId) {
        this(taskContext, subtaskId, SnapshotBuffer::new);
    }

    IncrementalSourceScanFetcher(
            FetchTask.Context taskContext, int subtaskId, Supplier<SnapshotBuffer> bufferFactory) {
        this.taskContext = taskContext;
        this.bufferFactory = bufferFactory;
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
        checkState(!closed, "Snapshot fetcher is closed");
        checkState(snapshotState == null, "Previous snapshot output has not been released");
        this.snapshotSplitReadTask = fetchTask;
        this.currentSnapshotSplit = fetchTask.getSplit().asSnapshotSplit();
        taskContext.configure(currentSnapshotSplit);
        this.queue = taskContext.getQueue();
        this.hasNextElement.set(true);
        this.reachEnd.set(false);
        executorService.submit(
                () -> {
                    try {
                        log.info(
                                "Start snapshot read task for snapshot split: {} exactly-once: {}",
                                currentSnapshotSplit,
                                taskContext.isExactlyOnce());
                        snapshotSplitReadTask.execute(taskContext);
                    } catch (Throwable e) {
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
        return snapshotState == null
                && (currentSnapshotSplit == null
                        || !snapshotSplitReadTask.isRunning()
                                && !hasNextElement.get()
                                && reachEnd.get());
    }

    @Override
    public Iterator<SourceRecords> pollSplitRecords()
            throws InterruptedException, SeaTunnelException {
        try {
            checkState(!closed, "Snapshot fetcher is closed");
            checkReadException();
            if (pendingSnapshot != null) {
                // Do not reconfigure the context while queued output still normalizes its rows.
                return Collections.emptyIterator();
            }
            if (hasNextElement.get()) {
                if (taskContext.isExactlyOnce()) {
                    return pollSplitRecordsIfExactlyOnce();
                }
                return pollSplitRecordsIfNotExactlyOnce();
            }
            reachEnd.set(true);
            return null;
        } catch (InterruptedException | RuntimeException | Error failure) {
            SnapshotState state = snapshotState;
            if (state != null) {
                try {
                    releaseSnapshot(state);
                } catch (Exception cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
    }

    public Iterator<SourceRecords> pollSplitRecordsIfNotExactlyOnce() throws InterruptedException {
        // eg:
        // data input: [low watermark event][snapshot events][high watermark event]
        List<SourceRecord> sendRecords = new ArrayList<>();
        List<DataChangeEvent> batch = queue.poll();
        for (DataChangeEvent event : batch) {
            SourceRecord record = event.getRecord();
            sendRecords.add(record);
            if (isHighWatermarkEvent(record)) {
                hasNextElement.set(false);
            }
        }
        // snapshot split return its data once
        final List<SourceRecords> sourceRecordsSet = new ArrayList<>();
        sourceRecordsSet.add(new SourceRecords(sendRecords));
        return sourceRecordsSet.iterator();
    }

    public Iterator<SourceRecords> pollSplitRecordsIfExactlyOnce() throws InterruptedException {
        SnapshotState state = snapshotState;
        if (state == null) {
            state = new SnapshotState(bufferFactory.get());
            synchronized (this) {
                if (closed) {
                    try {
                        state.buffer.close();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    throw new IllegalStateException("Snapshot fetcher is closed");
                }
                snapshotState = state;
            }
        }
        // Consume one bounded queue batch per fetch. This lets shutdown run even before END.
        List<DataChangeEvent> batch = queue.poll();
        for (DataChangeEvent event : batch) {
            checkReadException();
            SourceRecord record = event.getRecord();
            if (state.lowWatermark == null) {
                assertLowWatermark(record);
                state.lowWatermark = record;
            } else if (state.highWatermark == null && isHighWatermarkEvent(record)) {
                state.highWatermark = record;
            } else if (state.highWatermark != null && isEndWatermarkEvent(record)) {
                try {
                    state.buffer.seal();
                } catch (IOException e) {
                    throw new UncheckedIOException("Cannot seal CDC snapshot records", e);
                }
                SourceRecords output = new SourceRecords(new SnapshotRecordList(state));
                synchronized (this) {
                    checkState(!closed && snapshotState == state, "Snapshot fetcher is closed");
                    pendingSnapshot = output;
                    hasNextElement.set(false);
                }
                return Collections.singletonList(output).iterator();
            } else if (state.highWatermark == null) {
                state.buffer.put((Struct) record.key(), record);
            } else if (isChangeRecordInChunkRange(record)) {
                taskContext.rewriteOutputBuffer(state.buffer, record);
            }
        }
        return Collections.emptyIterator();
    }

    private void releaseSnapshot(SnapshotState state) throws IOException {
        state.buffer.close();
        synchronized (this) {
            if (snapshotState == state) {
                snapshotState = null;
                pendingSnapshot = null;
            }
        }
    }

    private static final class SnapshotState {
        private final SnapshotBuffer buffer;
        private SourceRecord lowWatermark;
        private SourceRecord highWatermark;

        private SnapshotState(SnapshotBuffer buffer) {
            this.buffer = buffer;
        }
    }

    /** One checkpoint-atomic batch, with only one decoded/normalized row live at a time. */
    private final class SnapshotRecordList extends AbstractList<SourceRecord>
            implements AutoCloseable {
        private final SnapshotState state;
        private final int size;
        private volatile boolean released;

        private SnapshotRecordList(SnapshotState state) {
            this.state = state;
            this.size = Math.addExact(state.buffer.size(), 2);
        }

        private void checkAvailable() {
            checkState(!released && !closed, "Snapshot output has been released");
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public SourceRecord get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("Snapshot record index: " + index);
            }
            Iterator<SourceRecord> iterator = iterator();
            for (int i = 0; i < index; i++) {
                iterator.next();
            }
            return iterator.next();
        }

        @Override
        public Iterator<SourceRecord> iterator() {
            checkAvailable();
            Iterator<SourceRecord> rows = state.buffer.values().iterator();
            return new Iterator<SourceRecord>() {
                private int position;

                @Override
                public boolean hasNext() {
                    checkAvailable();
                    return position < size;
                }

                @Override
                public SourceRecord next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    if (position++ == 0) {
                        return state.lowWatermark;
                    }
                    if (position == size) {
                        checkState(!rows.hasNext(), "Unexpected records after snapshot output");
                        return state.highWatermark;
                    }
                    List<SourceRecord> normalized =
                            taskContext.formatMessageTimestamp(
                                    Collections.singletonList(rows.next()));
                    checkState(normalized.size() == 1, "Snapshot normalization changed row count");
                    return normalized.get(0);
                }
            };
        }

        @Override
        public synchronized void close() throws IOException {
            if (!released) {
                releaseSnapshot(state);
                released = true;
            }
        }
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
        closed = true;
        RuntimeException failure = null;
        try {
            if (snapshotSplitReadTask != null) {
                snapshotSplitReadTask.shutdown();
            }
        } catch (RuntimeException e) {
            failure = e;
        }
        executorService.shutdownNow();
        SnapshotState state = snapshotState;
        try {
            if (state != null) {
                releaseSnapshot(state);
            }
        } catch (IOException e) {
            RuntimeException cleanup =
                    new UncheckedIOException("Cannot clean CDC snapshot spill", e);
            if (failure == null) {
                failure = cleanup;
            } else {
                failure.addSuppressed(cleanup);
            }
        }
        try {
            // Close JDBC/binlog clients before waiting for a blocked snapshot query to stop.
            taskContext.close();
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            if (!executorService.awaitTermination(READER_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                RuntimeException timeout =
                        new IllegalStateException("CDC snapshot reader did not stop after close");
                if (failure == null) {
                    failure = timeout;
                } else {
                    failure.addSuppressed(timeout);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            RuntimeException interrupted =
                    new RuntimeException("Interrupted closing CDC snapshot", e);
            if (failure == null) {
                failure = interrupted;
            } else {
                failure.addSuppressed(interrupted);
            }
        }
        if (failure != null) {
            throw failure;
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
