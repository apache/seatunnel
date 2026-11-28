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

package org.apache.seatunnel.connectors.seatunnel.ashby;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Publishes Ashby rows and their corresponding position together.
 *
 * <p>Provider I/O is off the engine thread. Cancellation closes the transfer independently of the
 * collector and state locks used to checkpoint output.
 */
final class AshbyReader implements SourceReader<SeaTunnelRow, AshbySplit> {
    private static final int OUTPUT_BATCH = 256;

    private final Context context;
    private final AshbySettings settings;
    private final String tableId;
    // Collector locking precedes snapshot-state updates. Cancellation uses a separate lock.
    private final Object stateLock = new Object();
    private final Object transportLock = new Object();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    action -> {
                        Thread thread = new Thread(action, "ashby-source-reader");
                        thread.setDaemon(true);
                        return thread;
                    });

    private AshbySplit state;
    // The fulfilled response remains owned until every chunk is published.
    private volatile Future<AshbyRead.Response> pending;
    private volatile boolean cancelled;
    private boolean ended;
    private long deadline;
    private AshbyHttp http;
    private AshbyRead read;

    AshbyReader(Context context, AshbySettings settings, String tableId) {
        this.context = context;
        this.settings = settings;
        this.tableId = tableId;
    }

    /**
     * Defers networking until the reader owns a split and the engine asks for data.
     *
     * <p>No connections are opened during construction or before assignment.
     */
    @Override
    public void open() {}

    /**
     * Emits the next verified page portion or schedules its provider request.
     *
     * <p>The next pass's token becomes visible only after the final page and descendants commit
     * their row positions under the checkpoint lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws Exception {
        if (cancelled || ended) {
            return;
        }
        AshbySplit current;
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
        Future<AshbyRead.Response> task = pending;
        if (task == null) {
            if (current.readyAt != null && Instant.now().isBefore(current.readyAt)) {
                Thread.sleep(20L);
            } else {
                submit(current);
            }
        } else if (task.isDone()) {
            AshbyRead.Response obtained = result(task);
            if (obtained != null) {
                publish(current, obtained, collector);
            }
        } else if (System.nanoTime() - deadline >= 0L) {
            cancel();
            throw failure("request deadline exceeded");
        } else {
            Thread.sleep(10L);
        }
    }

    private void submit(AshbySplit current) throws AshbyFailure {
        long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.timeout);
        deadline = expires;
        try {
            pending = worker.submit(() -> startRead().fetch(current, expires));
        } catch (RejectedExecutionException e) {
            if (!cancelled) {
                throw failure("could not schedule the request");
            }
        }
    }

    private AshbyRead startRead() throws AshbyFailure {
        if (read != null) {
            return read;
        }
        AshbyHttp client = new AshbyHttp(settings);
        synchronized (transportLock) {
            if (cancelled) {
                client.close();
                throw failure("reader was cancelled");
            }
            http = client;
            read = new AshbyRead(settings, http, tableId);
            return read;
        }
    }

    private void publish(
            AshbySplit current, AshbyRead.Response response, Collector<SeaTunnelRow> collector)
            throws AshbyFailure {
        if (current.pageOffset > response.rows.size()) {
            throw failure("checkpoint offset exceeds the response");
        }
        int end = Math.min(current.pageOffset + OUTPUT_BATCH, response.rows.size());
        synchronized (collector.getCheckpointLock()) {
            synchronized (stateLock) {
                if (cancelled) {
                    return;
                }
                for (int index = current.pageOffset; index < end; index++) {
                    collector.collect(response.rows.get(index));
                }
                if (end == response.rows.size()) {
                    state = response.next;
                    pending = null;
                } else {
                    AshbySplit partial = current.copy();
                    partial.pageDigest = response.digest;
                    partial.pageOffset = end;
                    state = partial;
                }
            }
        }
    }

    private AshbyRead.Response result(Future<AshbyRead.Response> task) throws Exception {
        try {
            return task.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AshbyFailure) {
                throw (AshbyFailure) e.getCause();
            }
            if (cancelled) {
                return null;
            }
            throw failure("could not read the provider page");
        } catch (CancellationException e) {
            if (cancelled) {
                return null;
            }
            throw failure("request was cancelled");
        }
    }

    /**
     * Snapshots the committed page and token position without retaining a candidate payload.
     *
     * <p>A page in progress retains its digest and output count, rather than the HTTP response.
     */
    @Override
    public List<AshbySplit> snapshotState(long checkpoint) {
        synchronized (stateLock) {
            return state == null ? List.of() : List.of(state.copy());
        }
    }

    @Override
    public void addSplits(List<AshbySplit> splits) {
        if (splits.isEmpty()) {
            return;
        }
        synchronized (stateLock) {
            if (splits.size() != 1 || state != null) {
                throw AshbySettings.invalid("checkpoint", "requires exactly one split");
            }
            if (!settings.fingerprint.equals(splits.get(0).fingerprint)) {
                throw AshbySettings.invalid("checkpoint", "resource or read settings changed");
            }
            state = splits.get(0).copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    /**
     * Maintains a sticky cancellation from job stop or a read timeout.
     *
     * <p>An in-flight HTTPS call and any paced or retrying acquisition are both interrupted.
     */
    @Override
    public void cancel() {
        cancelled = true;
        Future<AshbyRead.Response> task = pending;
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
     * Releases outstanding I/O through the same lifecycle path as an explicit stop.
     *
     * <p>The cancellation remains effective if the acquisition worker is still starting.
     */
    @Override
    public void close() {
        cancel();
    }

    private AshbyFailure failure(String message) {
        return new AshbyFailure(settings.resource, message);
    }
}
