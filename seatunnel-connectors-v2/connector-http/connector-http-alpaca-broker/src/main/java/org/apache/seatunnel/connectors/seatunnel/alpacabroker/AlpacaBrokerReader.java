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

package org.apache.seatunnel.connectors.seatunnel.alpacabroker;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerHttp.failure;

/**
 * Fetches one logical page at a time, then publishes its rows and cursor together.
 *
 * <p>A pending network result has no effect on the checkpoint until the engine thread can publish
 * it.
 */
final class AlpacaBrokerReader implements SourceReader<SeaTunnelRow, AlpacaBrokerPosition> {
    private final Context context;
    private final AlpacaBrokerSettings settings;
    private final String tableId;
    private final Object positionLock = new Object();
    private final Object clientLock = new Object();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    work -> {
                        Thread thread = new Thread(work, "alpaca-broker-retrieval");
                        thread.setDaemon(true);
                        return thread;
                    });
    private AlpacaBrokerPosition position;
    private AlpacaBrokerHttp http;
    private AlpacaBrokerPages pages;
    private volatile Future<AlpacaBrokerPages.Page> pending;
    private volatile boolean closed;
    private long deadline;
    private boolean signalled;

    AlpacaBrokerReader(Context context, AlpacaBrokerSettings settings, String tableId) {
        this.context = context;
        this.settings = settings;
        this.tableId = tableId;
    }

    /**
     * Open leaves transport creation to the assigned split's first request.
     *
     * <p>An unassigned or terminal checkpoint therefore does not create an HTTP client.
     */
    @Override
    public void open() {}

    /**
     * The engine thread can checkpoint and cancel while the request worker reads the provider.
     *
     * <p>No HTTP request or retry delay runs under the collector's checkpoint lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (closed || signalled) {
            return;
        }
        AlpacaBrokerPosition checkpoint;
        synchronized (positionLock) {
            checkpoint = position;
        }
        if (checkpoint == null) {
            Thread.sleep(10L);
            return;
        }
        if (checkpoint.done) {
            signalled = true;
            context.signalNoMoreElement();
            return;
        }
        Future<AlpacaBrokerPages.Page> request = pending;
        if (request == null) {
            startRequest(checkpoint);
            return;
        }
        if (request.isDone()) {
            AlpacaBrokerPages.Page page = completed(request);
            if (page != null) {
                synchronized (output.getCheckpointLock()) {
                    synchronized (positionLock) {
                        if (!closed) {
                            page.rows.forEach(output::collect);
                            position = page.position;
                        }
                    }
                }
            }
            pending = null;
        } else if (System.nanoTime() - deadline >= 0) {
            cancel();
            throw failure(settings.resource.key, "request deadline exceeded");
        } else {
            Thread.sleep(10L);
        }
    }

    private void startRequest(AlpacaBrokerPosition checkpoint) throws IOException {
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.timeoutMs);
        deadline = until;
        try {
            pending =
                    worker.submit(
                            () -> {
                                AlpacaBrokerPages.Page page = pages().next(checkpoint, until);
                                if (System.nanoTime() - until >= 0) {
                                    throw failure(
                                            settings.resource.key, "request deadline exceeded");
                                }
                                return page;
                            });
        } catch (RejectedExecutionException e) {
            if (!closed) {
                throw failure(settings.resource.key, "could not schedule the request");
            }
        }
    }

    private AlpacaBrokerPages pages() throws IOException {
        if (pages == null) {
            AlpacaBrokerHttp created = new AlpacaBrokerHttp(settings);
            synchronized (clientLock) {
                if (closed) {
                    created.close();
                    throw failure(settings.resource.key, "reader was cancelled");
                }
                http = created;
                pages = new AlpacaBrokerPages(settings, http, tableId);
            }
        }
        return pages;
    }

    private AlpacaBrokerPages.Page completed(Future<AlpacaBrokerPages.Page> request)
            throws Exception {
        try {
            return request.get();
        } catch (CancellationException e) {
            if (!closed) {
                throw failure(settings.resource.key, "request was cancelled");
            }
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AlpacaBrokerHttp.ReadFailure) {
                throw (AlpacaBrokerHttp.ReadFailure) e.getCause();
            }
            if (closed) {
                return null;
            }
            throw failure(settings.resource.key, "request processing failed");
        }
    }

    /**
     * Capture output progress, the frozen bounds, and the exact next request.
     *
     * <p>Before assignment, the enumerator still carries the unassigned split and the reader
     * returns an empty list.
     */
    @Override
    public List<AlpacaBrokerPosition> snapshotState(long checkpointId) {
        synchronized (positionLock) {
            return position == null ? List.of() : List.of(position);
        }
    }

    /**
     * Install a split once, retaining terminal state across a restart.
     *
     * <p>The saved settings identity prevents HTTP requests under a different account or selected
     * window.
     */
    @Override
    public void addSplits(List<AlpacaBrokerPosition> splits) {
        synchronized (positionLock) {
            for (AlpacaBrokerPosition split : splits) {
                if (position != null || !settings.stateKey.equals(split.stateKey)) {
                    throw AlpacaBrokerSettings.invalid(
                            "checkpoint", "unexpected split or changed source options");
                }
                position = split;
            }
        }
    }

    @Override
    public void handleNoMoreSplits() {
        // The assigned position determines EOF. An empty pre-assignment checkpoint is distinct.
    }

    /**
     * Interrupt sleep and abort the active response body without taking the checkpoint lock.
     *
     * <p>The transport gate also handles a client created concurrently with reader cancellation.
     */
    @Override
    public void cancel() {
        closed = true;
        Future<AlpacaBrokerPages.Page> request = pending;
        if (request != null) {
            request.cancel(true);
        }
        worker.shutdownNow();
        synchronized (clientLock) {
            if (http != null) {
                http.close();
            }
        }
    }

    /**
     * Close reader-owned transport and worker on normal completion and on failure.
     *
     * <p>The cancellation path is shared so it safely handles a repeated engine close.
     */
    @Override
    public void close() {
        cancel();
    }
}
