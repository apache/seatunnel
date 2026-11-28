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
package org.apache.seatunnel.connectors.seatunnel.awinadvertiser;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Publishes the ordered resource plan and checkpoints the precise report window and row position.
 *
 * <p>A client held in a body read or request pacing does not block the engine's checkpoint work.
 */
final class AwinReader implements SourceReader<SeaTunnelRow, AwinCursor> {
    private static final int COLLECT_BATCH = 128;

    private final Context context;
    private final AwinSettings settings;
    private final String tableId;
    // State never waits on HTTP or throttling and is updated together with collector output.
    private final Object stateLock = new Object();
    private final Object transportLock = new Object();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    work -> {
                        Thread t = new Thread(work, "awin-advertiser-read");
                        t.setDaemon(true);
                        return t;
                    });

    private AwinCursor cursor;
    // Retain an already decoded response until every row in its window is emitted.
    private volatile Future<AwinRecords> pending;
    private volatile boolean stopped;
    private boolean exhausted;
    private AwinTransport http;
    private long deadline;

    AwinReader(Context context, AwinSettings settings, String tableId) {
        this.context = context;
        this.settings = settings;
        this.tableId = tableId;
    }

    /**
     * Opens the reader without issuing a separate remote account probe.
     *
     * <p>The HTTP client is created by the worker once the engine has assigned a split.
     */
    @Override
    public void open() {}

    /**
     * Drives the current read lazily so a slow response does not pin the engine thread.
     *
     * <p>Once a response is ready the current batch and continuation commit under the same lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (stopped || exhausted) {
            return;
        }
        AwinCursor position;
        synchronized (stateLock) {
            position = cursor;
        }
        if (position == null) {
            Thread.sleep(10L);
            return;
        }
        if (position.partitionIndex == settings.partitions.size()) {
            exhausted = true;
            context.signalNoMoreElement();
            return;
        }
        AwinSlice slice = new AwinSlice(settings, position);
        Future<AwinRecords> task = pending;
        if (task == null) {
            schedule(position, slice);
        } else if (task.isDone()) {
            AwinRecords page = result(task, slice);
            if (page != null) {
                publish(page, position, slice, output);
            }
        } else {
            if (System.nanoTime() - deadline >= 0L) {
                cancel();
                throw new AwinReadException(slice, "request deadline exceeded");
            }
            Thread.sleep(10L);
        }
    }

    private void schedule(AwinCursor position, AwinSlice slice) throws AwinReadException {
        if (position.completedRequests >= settings.maxRequests) {
            throw new AwinReadException(slice, "max_requests exhausted");
        }
        long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.timeout);
        deadline = limit;
        try {
            pending =
                    worker.submit(
                            () -> {
                                byte[] data = transport(slice).get(slice, limit);
                                return AwinRecords.decode(
                                        data, slice, settings, position, tableId, limit);
                            });
        } catch (RejectedExecutionException closed) {
            if (!stopped) {
                throw new AwinReadException(slice, "could not schedule the request");
            }
        }
    }

    private AwinTransport transport(AwinSlice slice) throws AwinReadException {
        synchronized (transportLock) {
            if (stopped) {
                throw new AwinReadException(slice, "reader was cancelled");
            }
            if (http != null) {
                return http;
            }
        }
        AwinTransport created = new AwinTransport(settings, slice);
        synchronized (transportLock) {
            if (stopped) {
                created.close();
                throw new AwinReadException(slice, "reader was cancelled");
            }
            http = created;
            return http;
        }
    }

    private AwinRecords result(Future<AwinRecords> task, AwinSlice slice) throws Exception {
        try {
            return task.get();
        } catch (ExecutionException failed) {
            if (stopped) {
                return null;
            }
            if (failed.getCause() instanceof AwinReadException) {
                throw (AwinReadException) failed.getCause();
            }
            throw new AwinReadException(slice, "report acquisition failed");
        } catch (CancellationException cancelled) {
            if (stopped) {
                return null;
            }
            throw new AwinReadException(slice, "request was cancelled");
        }
    }

    private void publish(
            AwinRecords page, AwinCursor position, AwinSlice slice, Collector<SeaTunnelRow> output)
            throws AwinReadException {
        if (position.emitted > page.rows.size()) {
            throw new AwinReadException(slice, "saved row count exceeds the report response");
        }
        int end = Math.min(position.emitted + COLLECT_BATCH, page.rows.size());
        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                if (stopped) {
                    return;
                }
                for (int row = position.emitted; row < end; row++) {
                    output.collect(page.rows.get(row));
                }
                if (end == page.rows.size()) {
                    pending = null;
                    cursor = page.next;
                } else {
                    cursor = position.trackResponse(page.responseDigest, end);
                }
            }
        }
    }

    /**
     * Stores a stable position including the response digest for a partly published report.
     *
     * <p>Neither the full report nor the credential enters checkpoint state.
     */
    @Override
    public List<AwinCursor> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return cursor == null ? List.of() : List.of(cursor);
        }
    }

    /**
     * Restores the single scan and validates its identity before any API request.
     *
     * <p>A saved region or report position cannot be reused with a different read scope.
     */
    @Override
    public void addSplits(List<AwinCursor> splits) {
        if (splits.isEmpty()) {
            return;
        }
        synchronized (stateLock) {
            if (cursor != null || splits.size() != 1) {
                throw AwinSettings.invalid("checkpoint", "expected one ordered scan");
            }
            if (!settings.scope.equals(splits.get(0).scope)) {
                throw AwinSettings.invalid("checkpoint", "read scope changed");
            }
            cursor = splits.get(0);
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    /**
     * Aborts both the acquisition worker and the HTTPS call without taking the collector lock.
     *
     * <p>Stop is sticky even when a split or the HTTP client has not yet been assigned.
     */
    @Override
    public void cancel() {
        stopped = true;
        Future<AwinRecords> task = pending;
        if (task != null) {
            task.cancel(true);
        }
        synchronized (transportLock) {
            if (http != null) {
                http.close();
            }
        }
        worker.shutdownNow();
    }

    /**
     * Releases the reader's resources at normal end or failure.
     *
     * <p>Close shares the idempotent interrupt and connection cancellation used for job stop.
     */
    @Override
    public void close() {
        cancel();
    }
}
