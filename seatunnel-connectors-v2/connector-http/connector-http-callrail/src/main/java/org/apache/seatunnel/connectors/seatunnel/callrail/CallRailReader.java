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
package org.apache.seatunnel.connectors.seatunnel.callrail;

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
 * Coordinates cancellable provider acquisition, row publication and split recovery.
 *
 * <p>The collector lock and then the progress monitor protect committed transitions. Waiting on
 * HTTPS never takes either lock.
 */
final class CallRailReader implements SourceReader<SeaTunnelRow, CallRailSplit> {
    private static final int CHUNK = 32;
    private final Context context;
    private final CallRailConfig options;
    private final String tableId;
    private final Object progressLock = new Object();
    private final Object clientLock = new Object();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "callrail-source-fetch");
                        thread.setDaemon(true);
                        return thread;
                    });

    private CallRailSplit position;
    // Assignment and the terminal assignment signal use the same progress monitor.
    private boolean noMoreSplits;
    private CallRailTransport transport;
    // The completed response remains available until all of its chunks are published.
    private volatile Future<CallRailPage> acquisition;
    private volatile boolean cancelled;
    private long deadline;
    private boolean ended;

    CallRailReader(Context context, CallRailConfig options, String tableId) {
        this.context = context;
        this.options = options;
        this.tableId = tableId;
    }

    /**
     * Opens the reader without issuing a provider connectivity probe.
     *
     * <p>Transport starts on demand when a request has been assigned.
     */
    @Override
    public void open() {}

    /**
     * Starts or collects one validated result without blocking engine checkpoint progress.
     *
     * <p>Publication processes a bounded number of rows under the checkpoint lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (cancelled || ended) {
            return;
        }
        CallRailSplit state;
        boolean assignmentComplete;
        synchronized (progressLock) {
            state = position;
            assignmentComplete = noMoreSplits;
        }
        if (state == null && !assignmentComplete) {
            Thread.sleep(10L);
            return;
        }
        if (state == null || state.resourceIndex == options.resources.size()) {
            ended = true;
            context.signalNoMoreElement();
            return;
        }

        CallRailResource request = options.resources.get(state.resourceIndex);
        Future<CallRailPage> current = acquisition;
        if (current == null) {
            schedule(state, request);
        } else if (current.isDone()) {
            CallRailPage data = completed(current, request, state.page);
            if (data != null) {
                publish(output, state, data, request);
            }
        } else {
            if (System.nanoTime() - deadline >= 0L) {
                cancel();
                throw request.failure(state.page, "request deadline exceeded");
            }
            Thread.sleep(10L);
        }
    }

    private void schedule(CallRailSplit state, CallRailResource request)
            throws CallRailReadException {
        if (state.requests >= options.maxRequests) {
            throw request.failure(state.page, "max_requests exhausted before the scan completed");
        }
        if (state.page > options.maxPages) {
            throw request.failure(
                    state.page, "max_pages_per_scope exhausted before the list ended");
        }
        long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.timeout);
        deadline = expires;
        try {
            acquisition =
                    worker.submit(
                            () ->
                                    CallRailPage.read(
                                            client(request, state.page),
                                            options,
                                            state,
                                            tableId,
                                            expires));
        } catch (RejectedExecutionException rejected) {
            if (!cancelled) {
                throw request.failure(state.page, "could not schedule the CallRail request");
            }
        }
    }

    private CallRailTransport client(CallRailResource work, int page) throws CallRailReadException {
        synchronized (clientLock) {
            if (cancelled) {
                throw work.failure(page, "reader was cancelled");
            }
            if (transport != null) {
                return transport;
            }
        }
        CallRailTransport created = new CallRailTransport(options);
        synchronized (clientLock) {
            if (cancelled) {
                created.close();
                throw work.failure(page, "reader was cancelled");
            }
            transport = created;
            return created;
        }
    }

    private CallRailPage completed(Future<CallRailPage> request, CallRailResource work, int page)
            throws CallRailReadException, InterruptedException {
        try {
            return request.get();
        } catch (CancellationException stopped) {
            if (cancelled) {
                return null;
            }
            throw work.failure(page, "provider acquisition was cancelled");
        } catch (ExecutionException failure) {
            if (cancelled) {
                return null;
            }
            Throwable reason = failure.getCause();
            if (reason instanceof CallRailReadException) {
                throw (CallRailReadException) reason;
            }
            throw work.failure(page, "provider acquisition failed");
        }
    }

    private void publish(
            Collector<SeaTunnelRow> output,
            CallRailSplit state,
            CallRailPage data,
            CallRailResource work)
            throws CallRailReadException {
        if (state.published < 0 || state.published > data.rows.size()) {
            throw work.failure(state.page, "invalid checkpoint row offset");
        }
        int end = Math.min(state.published + CHUNK, data.rows.size());
        synchronized (output.getCheckpointLock()) {
            synchronized (progressLock) {
                if (cancelled) {
                    return;
                }
                for (int i = state.published; i < end; i++) {
                    output.collect(data.rows.get(i));
                }
                if (end == data.rows.size()) {
                    position = data.next;
                    acquisition = null;
                } else {
                    position = state.partial(data.fingerprint, end);
                }
            }
        }
    }

    /**
     * Saves immutable progress even during an unfinished request.
     *
     * <p>The snapshot omits the transport, provider payloads and authentication token.
     */
    @Override
    public List<CallRailSplit> snapshotState(long checkpointId) {
        synchronized (progressLock) {
            return position == null ? List.of() : List.of(position);
        }
    }

    /**
     * Assigns or restores the worker's only scan after validating its configuration scope.
     *
     * <p>An invalid scope fails before a connection can open.
     */
    @Override
    public void addSplits(List<CallRailSplit> splits) {
        synchronized (progressLock) {
            if (splits.isEmpty()) {
                return;
            }
            if (position != null || splits.size() != 1) {
                throw CallRailConfig.invalid("checkpoint", "expected one CallRail split");
            }
            CallRailSplit restored = splits.get(0);
            if (!options.scope.equals(restored.scope)) {
                throw CallRailConfig.invalid("checkpoint", "read scope changed");
            }
            position = restored;
        }
    }

    /**
     * Records the coordinator's final assignment signal.
     *
     * <p>A reader without an assigned split can now complete without opening transport. An existing
     * split still has to run to its own terminal position.
     */
    @Override
    public void handleNoMoreSplits() {
        synchronized (progressLock) {
            noMoreSplits = true;
        }
    }

    /**
     * Makes cancellation sticky and interrupts network I/O independently of the collector.
     *
     * <p>A concurrent transport initialization cannot publish an uncancelled call.
     */
    @Override
    public void cancel() {
        cancelled = true;
        Future<CallRailPage> current = acquisition;
        if (current != null) {
            current.cancel(true);
        }
        synchronized (clientLock) {
            if (transport != null) {
                transport.close();
            }
        }
        worker.shutdownNow();
    }

    /**
     * Releases the same resources as explicit engine cancellation.
     *
     * <p>It is safe to close before assignment and after a terminal page.
     */
    @Override
    public void close() {
        cancel();
    }
}
