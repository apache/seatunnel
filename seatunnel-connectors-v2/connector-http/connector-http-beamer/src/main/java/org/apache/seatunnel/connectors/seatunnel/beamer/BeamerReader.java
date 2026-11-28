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
package org.apache.seatunnel.connectors.seatunnel.beamer;

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
 * Drives provider acquisition and publishes each page under the engine checkpoint lock.
 *
 * <p>The network worker can be stopped without acquiring the collector or split state lock. A
 * partly published response remains associated with the same provider position.
 */
final class BeamerReader implements SourceReader<SeaTunnelRow, BeamerSplit> {
    private static final int CHUNK_SIZE = 32;
    private final Context context;
    private final BeamerConfig config;
    private final String tableId;
    // Position snapshots do not acquire the transport lock or wait for provider I/O.
    private final Object stateLock = new Object();
    private final Object transportLock = new Object();
    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(
                    work -> {
                        Thread thread = new Thread(work, "beamer-nps-reader");
                        thread.setDaemon(true);
                        return thread;
                    });

    private BeamerSplit position;
    // A verified page can span several publications and must stay resident across those chunks.
    private volatile Future<BeamerPage> acquisition;
    private volatile boolean cancelled;
    private long deadline;
    private boolean ended;
    private BeamerTransport client;

    BeamerReader(Context context, BeamerConfig config, String tableId) {
        this.context = context;
        this.config = config;
        this.tableId = tableId;
    }

    /**
     * Opens the reader without charging a connectivity request to the account.
     *
     * <p>HTTPS is opened only when the first assigned page is requested.
     */
    @Override
    public void open() {}

    /**
     * Schedules the next position or publishes a verified portion of its response.
     *
     * <p>Polling returns while HTTPS is outstanding so the engine can take checkpoints and stop.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (cancelled || ended) {
            return;
        }
        BeamerSplit current;
        synchronized (stateLock) {
            current = position;
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
        Future<BeamerPage> task = acquisition;
        if (task == null) {
            request(current);
        } else if (task.isDone()) {
            BeamerPage page = readResult(current, task);
            if (page != null) {
                publish(current, page, output);
            }
        } else {
            if (System.nanoTime() - deadline >= 0L) {
                cancel();
                throw new BeamerReadException(current.page, "request deadline exceeded");
            }
            Thread.sleep(10L);
        }
    }

    private void request(BeamerSplit current) throws BeamerReadException {
        if (current.page >= config.maxPages) {
            throw new BeamerReadException(current.page, "max_pages exhausted");
        }
        final long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.timeout);
        deadline = expires;
        try {
            acquisition =
                    executor.submit(
                            () -> {
                                BeamerTransport active = transport(current.page);
                                return BeamerPage.verify(
                                        active.fetch(current, expires),
                                        config,
                                        current,
                                        tableId,
                                        expires);
                            });
        } catch (RejectedExecutionException stopped) {
            if (!cancelled) {
                throw new BeamerReadException(current.page, "could not schedule a NPS read");
            }
        }
    }

    private BeamerTransport transport(int page) throws BeamerReadException {
        synchronized (transportLock) {
            if (cancelled) {
                throw new BeamerReadException(page, "reader was cancelled");
            }
            if (client != null) {
                return client;
            }
        }
        BeamerTransport created = new BeamerTransport(config, page);
        synchronized (transportLock) {
            if (cancelled) {
                created.close();
                throw new BeamerReadException(page, "reader was cancelled");
            }
            client = created;
            return client;
        }
    }

    private BeamerPage readResult(BeamerSplit current, Future<BeamerPage> task) throws Exception {
        try {
            return task.get();
        } catch (CancellationException stopped) {
            if (cancelled) {
                return null;
            }
            throw new BeamerReadException(current.page, "request was cancelled");
        } catch (ExecutionException failed) {
            if (cancelled) {
                return null;
            }
            Throwable reason = failed.getCause();
            if (reason instanceof BeamerReadException) {
                throw (BeamerReadException) reason;
            }
            throw new BeamerReadException(current.page, "NPS acquisition failed");
        }
    }

    private void publish(BeamerSplit current, BeamerPage page, Collector<SeaTunnelRow> output)
            throws BeamerReadException {
        if (current.published > page.rows.size()) {
            throw new BeamerReadException(current.page, "invalid checkpoint row offset");
        }
        int end = Math.min(current.published + CHUNK_SIZE, page.rows.size());
        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                if (cancelled) {
                    return;
                }
                for (int index = current.published; index < end; index++) {
                    output.collect(page.rows.get(index));
                }
                if (end == page.rows.size()) {
                    position = page.next;
                    acquisition = null;
                } else {
                    position = current.partial(page.digest, end);
                }
            }
        }
    }

    /**
     * Snapshots the precise published position without serializing provider payloads.
     *
     * <p>The immutable split can be safely retained when the network worker is still active.
     */
    @Override
    public List<BeamerSplit> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return position == null ? List.of() : List.of(position);
        }
    }

    /**
     * Restores one ordered scan and detects configuration scope changes before any HTTPS.
     *
     * <p>The scope includes the key, endpoint, page size and creation window.
     */
    @Override
    public void addSplits(List<BeamerSplit> splits) {
        if (splits.isEmpty()) {
            return;
        }
        synchronized (stateLock) {
            if (position != null || splits.size() != 1) {
                throw BeamerConfig.invalid("checkpoint", "expected one ordered NPS split");
            }
            BeamerSplit received = splits.get(0);
            if (!config.scope.equals(received.scope)) {
                throw BeamerConfig.invalid("checkpoint", "read scope changed");
            }
            position = received;
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    /**
     * Stops the pending worker and the live connection independently of the output lock.
     *
     * <p>The sticky flag also prevents a client that is still starting from issuing a request.
     */
    @Override
    public void cancel() {
        cancelled = true;
        Future<BeamerPage> active = acquisition;
        if (active != null) {
            active.cancel(true);
        }
        synchronized (transportLock) {
            if (client != null) {
                client.close();
            }
        }
        executor.shutdownNow();
    }

    /**
     * Shares cancellation with job stop to release owned request and worker resources.
     *
     * <p>Repeated shutdown is safe even when no split was assigned.
     */
    @Override
    public void close() {
        cancel();
    }
}
