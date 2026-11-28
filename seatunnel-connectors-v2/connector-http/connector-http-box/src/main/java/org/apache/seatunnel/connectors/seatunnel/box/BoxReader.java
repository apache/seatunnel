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
package org.apache.seatunnel.connectors.seatunnel.box;

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
final class BoxReader implements SourceReader<SeaTunnelRow, BoxSplit> {
    private static final int CHUNK = 32;
    private final Context context;
    private final BoxConfig options;
    private final String tableId;
    private final Object progressLock = new Object();
    private final Object clientLock = new Object();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "box-source-fetch");
                        thread.setDaemon(true);
                        return thread;
                    });

    private BoxSplit position;
    // Assignment and the terminal assignment signal use the same progress monitor.
    private boolean noMoreSplits;
    private BoxHttp transport;
    // The completed response remains available until all of its chunks are published.
    private volatile Future<BoxData> acquisition;
    private volatile boolean cancelled;
    private long deadline;
    private boolean ended;

    BoxReader(Context context, BoxConfig options, String tableId) {
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
        BoxSplit state;
        boolean assignmentComplete;
        synchronized (progressLock) {
            state = position;
            assignmentComplete = noMoreSplits;
        }
        if (state == null && !assignmentComplete) {
            Thread.sleep(10L);
            return;
        }
        if (state == null || state.work.isEmpty()) {
            ended = true;
            context.signalNoMoreElement();
            return;
        }

        BoxWork request = state.work.get(0);
        Future<BoxData> current = acquisition;
        if (current == null) {
            schedule(state, request);
        } else if (current.isDone()) {
            BoxData data = completed(current, request);
            if (data != null) {
                publish(output, state, data, request);
            }
        } else {
            if (System.nanoTime() - deadline >= 0L) {
                cancel();
                throw request.failure("request deadline exceeded");
            }
            Thread.sleep(10L);
        }
    }

    private void schedule(BoxSplit state, BoxWork request) throws BoxReadException {
        if (state.requests >= options.maxRequests) {
            throw request.failure("max_requests exhausted before the scan completed");
        }
        if (request.pages >= options.maxPages) {
            throw request.failure("max_pages_per_parent exhausted before terminal position");
        }
        long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.timeout);
        deadline = expires;
        try {
            acquisition =
                    worker.submit(
                            () -> BoxData.read(client(request), options, state, tableId, expires));
        } catch (RejectedExecutionException rejected) {
            if (!cancelled) {
                throw request.failure("could not schedule the Box request");
            }
        }
    }

    private BoxHttp client(BoxWork work) throws BoxReadException {
        synchronized (clientLock) {
            if (cancelled) {
                throw work.failure("reader was cancelled");
            }
            if (transport != null) {
                return transport;
            }
        }
        BoxHttp created = new BoxHttp(options);
        synchronized (clientLock) {
            if (cancelled) {
                created.close();
                throw work.failure("reader was cancelled");
            }
            transport = created;
            return created;
        }
    }

    private BoxData completed(Future<BoxData> request, BoxWork work)
            throws BoxReadException, InterruptedException {
        try {
            return request.get();
        } catch (CancellationException stopped) {
            if (cancelled) {
                return null;
            }
            throw work.failure("provider acquisition was cancelled");
        } catch (ExecutionException failure) {
            if (cancelled) {
                return null;
            }
            Throwable reason = failure.getCause();
            if (reason instanceof BoxReadException) {
                throw (BoxReadException) reason;
            }
            throw work.failure("provider acquisition failed");
        }
    }

    private void publish(Collector<SeaTunnelRow> output, BoxSplit state, BoxData data, BoxWork work)
            throws BoxReadException {
        if (state.published < 0 || state.published > data.rows.size()) {
            throw work.failure("invalid checkpoint row offset");
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
    public List<BoxSplit> snapshotState(long checkpointId) {
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
    public void addSplits(List<BoxSplit> splits) {
        synchronized (progressLock) {
            if (splits.isEmpty()) {
                return;
            }
            if (position != null || splits.size() != 1) {
                throw BoxConfig.invalid("checkpoint", "expected one Box split");
            }
            BoxSplit restored = splits.get(0);
            if (!options.scope.equals(restored.scope)) {
                throw BoxConfig.invalid("checkpoint", "read scope changed");
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
        Future<BoxData> current = acquisition;
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
