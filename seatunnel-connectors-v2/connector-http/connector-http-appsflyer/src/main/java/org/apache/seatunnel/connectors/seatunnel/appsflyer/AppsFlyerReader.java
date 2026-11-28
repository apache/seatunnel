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

package org.apache.seatunnel.connectors.seatunnel.appsflyer;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.appsflyer.AppsFlyerRecords.Saturated;
import org.apache.seatunnel.connectors.seatunnel.appsflyer.AppsFlyerRecords.Snapshot;
import org.apache.seatunnel.connectors.seatunnel.appsflyer.AppsFlyerSource.Cursor;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processes bounded report windows serially and checkpoints any request subdivisions. Network work
 * stays outside the engine checkpoint lock and cannot emit partially validated data.
 */
final class AppsFlyerReader implements SourceReader<SeaTunnelRow, Cursor> {
    private static final int ROWS_PER_POLL = 64;
    private static final int MAX_RESTORED_WINDOWS = 512;
    private final AppsFlyerConfig config;
    private final Context context;
    private final String table;
    private final AppsFlyerHttp http;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Deque<AppsFlyerWindow> windows = new ArrayDeque<>();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-appsflyer-https");
                        thread.setDaemon(true);
                        return thread;
                    });

    private boolean assigned;
    private boolean eof;
    private int row;
    private String digest;
    private Snapshot current;
    private long deadline;
    private volatile Future<Snapshot> pending;

    AppsFlyerReader(AppsFlyerConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        this.http = new AppsFlyerHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Updates the emitted position while holding the collector lock and then the progress monitor.
     * Only a completed HTTPS future is consulted while these locks are held.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            synchronized (progress) {
                if (closed.get() || !assigned || eof) {
                    return;
                }
                AppsFlyerWindow next = windows.peekFirst();
                if (next == null) {
                    eof = true;
                    context.signalNoMoreElement();
                    return;
                }
                if (current != null || prepare(next)) {
                    emit(output);
                }
            }
        }
    }

    private boolean prepare(AppsFlyerWindow next) throws IOException {
        Future<Snapshot> task = pending;
        if (task == null) {
            long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.requestTimeout);
            deadline = expires;
            try {
                pending = worker.submit(() -> http.fetch(next, expires));
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw AppsFlyerHttp.failure(next, "retrieval worker is unavailable");
                }
            }
            return false;
        }
        if (!task.isDone()) {
            if (System.nanoTime() - deadline >= 0) {
                close();
                throw AppsFlyerHttp.failure(next, "request deadline exceeded");
            }
            return false;
        }
        try {
            Snapshot snapshot = task.get();
            if (closed.get()) {
                return false;
            }
            verifyRestored(next, row, digest, snapshot);
            current = snapshot;
            pending = null;
            return true;
        } catch (CancellationException e) {
            if (!closed.get()) {
                throw AppsFlyerHttp.failure(next, "request canceled");
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closed.get()) {
                throw AppsFlyerHttp.failure(next, "request interrupted");
            }
            return false;
        } catch (ExecutionException e) {
            if (closed.get()) {
                return false;
            }
            if (e.getCause() instanceof Saturated) {
                subdivide(next, (Saturated) e.getCause());
                pending = null;
                return false;
            }
            if (e.getCause() instanceof AppsFlyerHttp.SafeFailure) {
                throw (AppsFlyerHttp.SafeFailure) e.getCause();
            }
            throw AppsFlyerHttp.failure(next, "report retrieval failed");
        }
    }

    private void subdivide(AppsFlyerWindow window, Saturated reason) throws IOException {
        if (row != 0) {
            throw AppsFlyerHttp.failure(window, "report changed since the checkpoint");
        }
        Instant middle = window.splitAt(config.zone);
        if (middle == null) {
            throw AppsFlyerHttp.failure(
                    window,
                    reason.getMessage() + " and the window cannot be divided without overlap");
        }
        windows.removeFirst();
        windows.addFirst(new AppsFlyerWindow(window.report, middle, window.end));
        windows.addFirst(new AppsFlyerWindow(window.report, window.start, middle));
    }

    /**
     * Reuses a partially emitted row offset only when the fetched report matches the checkpoint. A
     * report that changed, including its row order, cannot silently skip or duplicate records.
     */
    static void verifyRestored(AppsFlyerWindow window, int row, String digest, Snapshot report)
            throws IOException {
        if (row > 0 && (row >= report.rows.size() || !report.digest.equals(digest))) {
            throw AppsFlyerHttp.failure(window, "report changed since the checkpoint");
        }
    }

    private void emit(Collector<SeaTunnelRow> output) {
        int end = Math.min(current.rows.size(), row + ROWS_PER_POLL);
        for (int i = row; i < end; i++) {
            if (closed.get()) {
                return;
            }
            AppsFlyerRecords.Row record = current.rows.get(i);
            SeaTunnelRow result =
                    new SeaTunnelRow(
                            new Object[] {
                                record.resource.key(),
                                config.appId,
                                record.time,
                                config.zone.getId(),
                                record.payload
                            });
            result.setTableId(table);
            output.collect(result);
        }
        if (end < current.rows.size()) {
            digest = current.digest;
            row = end;
        } else {
            windows.removeFirst();
            digest = null;
            row = 0;
            current = null;
        }
    }

    @Override
    public List<Cursor> snapshotState(long checkpointId) {
        synchronized (progress) {
            return assigned
                    ? Collections.singletonList(
                            new Cursor(config.fingerprint, new ArrayList<>(windows), row, digest))
                    : Collections.emptyList();
        }
    }

    @Override
    public void addSplits(List<Cursor> splits) {
        synchronized (progress) {
            if (assigned || splits.size() != 1) {
                throw AppsFlyerConfig.invalid("expected one assigned scan");
            }
            Cursor state = splits.get(0);
            if (!config.fingerprint.equals(state.fingerprint)) {
                throw AppsFlyerConfig.invalid("query configuration changed during restore");
            }
            if (state.row < 0
                    || ((state.row == 0) != (state.digest == null))
                    || (state.windows.isEmpty() && state.row != 0)
                    || state.windows.size() > MAX_RESTORED_WINDOWS
                    || state.windows.stream()
                            .anyMatch(
                                    w ->
                                            !config.reports.contains(w.report)
                                                    || w.start.isBefore(config.start)
                                                    || !w.start.isBefore(w.end))) {
                throw AppsFlyerConfig.invalid("invalid report checkpoint state");
            }
            windows.addAll(state.windows);
            row = state.row;
            digest = state.digest;
            assigned = true;
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    /**
     * Aborts HTTPS reads and retry sleeps without waiting for checkpoint progress. Normal
     * completion and cancellation use the same resource disposal path.
     */
    @Override
    public void cancel() {
        close();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<?> task = pending;
            if (task != null) {
                task.cancel(true);
            }
            worker.shutdownNow();
        }
    }
}
