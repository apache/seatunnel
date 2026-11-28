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

package org.apache.seatunnel.connectors.seatunnel.appfigures;

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
 * Fetches and validates a response outside the engine thread.
 *
 * <p>Checkpoint and output updates share the collector lock. Cancellation has an independent
 * transport lock, so a blocked read cannot keep the engine from stopping it.
 */
final class AppfiguresReader implements SourceReader<SeaTunnelRow, AppfiguresSplit> {
    private static final int OUTPUT_BATCH = 256;
    private final Context context;
    private final AppfiguresSettings settings;
    private final String tableId;
    private final Object stateLock = new Object();
    private final Object transportLock = new Object();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "appfigures-source");
                        thread.setDaemon(true);
                        return thread;
                    });

    private AppfiguresSplit state;
    private volatile Future<AppfiguresRead.Response> pending;
    private volatile boolean cancelled;
    private boolean ended;
    private long deadline;
    private AppfiguresHttp http;
    private AppfiguresRead read;

    AppfiguresReader(Context context, AppfiguresSettings settings, String tableId) {
        this.context = context;
        this.settings = settings;
        this.tableId = tableId;
    }

    @Override
    public void open() {}

    /**
     * Emits a bounded portion of a validated provider page and its corresponding offset.
     *
     * <p>Network requests and retry waits happen without holding the collector lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (cancelled || ended) {
            return;
        }
        AppfiguresSplit current;
        synchronized (stateLock) {
            current = state;
        }
        if (current == null) {
            Thread.sleep(10L);
            return;
        }
        if (current.done) {
            ended = true;
            context.signalNoMoreElement();
            return;
        }
        Future<AppfiguresRead.Response> job = pending;
        if (job == null) {
            submit(current);
        } else if (job.isDone()) {
            AppfiguresRead.Response response = result(job);
            if (response != null) {
                collect(current, response, output);
            }
        } else if (System.nanoTime() - deadline >= 0L) {
            cancel();
            throw failure("request deadline exceeded");
        } else {
            Thread.sleep(10L);
        }
    }

    private void submit(AppfiguresSplit split) throws AppfiguresFailure {
        long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.timeout);
        deadline = expires;
        try {
            pending = worker.submit(() -> startRead().fetch(split, expires));
        } catch (RejectedExecutionException e) {
            if (!cancelled) {
                throw failure("could not schedule the request");
            }
        }
    }

    private AppfiguresRead startRead() throws AppfiguresFailure {
        if (read != null) {
            return read;
        }
        AppfiguresHttp transport = new AppfiguresHttp(settings);
        synchronized (transportLock) {
            if (cancelled) {
                transport.close();
                throw failure("reader was cancelled");
            }
            http = transport;
            read = new AppfiguresRead(settings, http, tableId);
            return read;
        }
    }

    private void collect(
            AppfiguresSplit current,
            AppfiguresRead.Response response,
            Collector<SeaTunnelRow> output)
            throws AppfiguresFailure {
        if (current.offset > response.rows.size()) {
            throw failure("checkpoint offset exceeds the original response");
        }
        int end = Math.min(current.offset + OUTPUT_BATCH, response.rows.size());
        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                if (cancelled) {
                    return;
                }
                for (int index = current.offset; index < end; index++) {
                    output.collect(response.rows.get(index));
                }
                if (end == response.rows.size()) {
                    state = response.next;
                    pending = null;
                } else {
                    AppfiguresSplit next = current.copy();
                    next.offset = end;
                    next.digest = response.digest;
                    state = next;
                }
            }
        }
    }

    private AppfiguresRead.Response result(Future<AppfiguresRead.Response> job) throws Exception {
        try {
            return job.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AppfiguresFailure) {
                throw (AppfiguresFailure) e.getCause();
            }
            if (cancelled) {
                return null;
            }
            throw failure("could not read the provider response");
        } catch (CancellationException e) {
            if (cancelled) {
                return null;
            }
            throw failure("request was cancelled");
        }
    }

    /**
     * Saves the current provider continuation or a partial response offset.
     *
     * <p>The snapshot excludes parsed data, credential headers, and response bodies.
     */
    @Override
    public List<AppfiguresSplit> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return state == null ? List.of() : List.of(state.copy());
        }
    }

    @Override
    public void addSplits(List<AppfiguresSplit> splits) {
        if (splits.isEmpty()) {
            return;
        }
        synchronized (stateLock) {
            if (splits.size() != 1 || state != null) {
                throw AppfiguresSettings.invalid(
                        "checkpoint", "received more than one source split");
            }
            if (!settings.fingerprint.equals(splits.get(0).fingerprint)) {
                throw AppfiguresSettings.invalid("checkpoint", "resource or read settings changed");
            }
            state = splits.get(0).copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    /**
     * Interrupts retries or body reads for this reader.
     *
     * <p>The cancellation remains effective if it arrives before the first request starts.
     */
    @Override
    public void cancel() {
        cancelled = true;
        Future<AppfiguresRead.Response> job = pending;
        if (job != null) {
            job.cancel(true);
        }
        synchronized (transportLock) {
            if (http != null) {
                http.close();
            }
        }
        worker.shutdownNow();
    }

    @Override
    public void close() {
        cancel();
    }

    private AppfiguresFailure failure(String reason) {
        return new AppfiguresFailure(settings.resource, reason);
    }
}
