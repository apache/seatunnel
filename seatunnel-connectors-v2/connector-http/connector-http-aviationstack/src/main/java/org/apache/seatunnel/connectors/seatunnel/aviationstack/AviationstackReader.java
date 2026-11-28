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
package org.apache.seatunnel.connectors.seatunnel.aviationstack;

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
final class AviationstackReader implements SourceReader<SeaTunnelRow, AviationstackSplit> {
    private static final int CHUNK_SIZE = 32;
    private final Context context;
    private final AviationstackConfig config;
    private final String tableId;
    // Position snapshots do not acquire the transport lock or wait for provider I/O.
    private final Object stateLock = new Object();
    private final Object transportLock = new Object();
    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(
                    work -> {
                        Thread thread = new Thread(work, "aviationstack-catalog-reader");
                        thread.setDaemon(true);
                        return thread;
                    });

    private AviationstackSplit position;
    // A verified page can span several publications and must stay resident across those chunks.
    private volatile Future<AviationstackPage> acquisition;
    private volatile boolean cancelled;
    private long deadline;
    private boolean ended;
    private AviationstackTransport client;

    AviationstackReader(Context context, AviationstackConfig config, String tableId) {
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
        AviationstackSplit current;
        synchronized (stateLock) {
            current = position;
        }
        if (current == null) {
            Thread.sleep(10L);
            return;
        }
        if (current.resourceIndex == config.resources.size()) {
            ended = true;
            context.signalNoMoreElement();
            return;
        }
        AviationstackResource resource = config.resources.get(current.resourceIndex);
        Future<AviationstackPage> task = acquisition;
        if (task == null) {
            request(resource, current);
        } else if (task.isDone()) {
            AviationstackPage page = readResult(resource, current, task);
            if (page != null) {
                publish(resource, current, page, output);
            }
        } else {
            if (System.nanoTime() - deadline >= 0L) {
                cancel();
                throw new AviationstackReadException(
                        resource, current.offset, "request deadline exceeded");
            }
            Thread.sleep(10L);
        }
    }

    private void request(AviationstackResource resource, AviationstackSplit current)
            throws AviationstackReadException {
        if (current.pages >= config.maxPages) {
            throw new AviationstackReadException(
                    resource, current.offset, "max_pages_per_resource exhausted");
        }
        final long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.timeout);
        deadline = expires;
        try {
            acquisition =
                    executor.submit(
                            () -> {
                                AviationstackTransport active = transport(resource, current.offset);
                                return AviationstackPage.verify(
                                        active.fetch(resource, current.offset, expires),
                                        config,
                                        current,
                                        tableId,
                                        expires);
                            });
        } catch (RejectedExecutionException stopped) {
            if (!cancelled) {
                throw new AviationstackReadException(
                        resource, current.offset, "could not schedule a catalog read");
            }
        }
    }

    private AviationstackTransport transport(AviationstackResource resource, long offset)
            throws AviationstackReadException {
        synchronized (transportLock) {
            if (cancelled) {
                throw new AviationstackReadException(resource, offset, "reader was cancelled");
            }
            if (client != null) {
                return client;
            }
        }
        AviationstackTransport created = new AviationstackTransport(config, resource, offset);
        synchronized (transportLock) {
            if (cancelled) {
                created.close();
                throw new AviationstackReadException(resource, offset, "reader was cancelled");
            }
            client = created;
            return client;
        }
    }

    private AviationstackPage readResult(
            AviationstackResource resource,
            AviationstackSplit current,
            Future<AviationstackPage> task)
            throws Exception {
        try {
            return task.get();
        } catch (CancellationException stopped) {
            if (cancelled) {
                return null;
            }
            throw new AviationstackReadException(resource, current.offset, "request was cancelled");
        } catch (ExecutionException failed) {
            if (cancelled) {
                return null;
            }
            Throwable reason = failed.getCause();
            if (reason instanceof AviationstackReadException) {
                throw (AviationstackReadException) reason;
            }
            throw new AviationstackReadException(
                    resource, current.offset, "catalog acquisition failed");
        }
    }

    private void publish(
            AviationstackResource resource,
            AviationstackSplit current,
            AviationstackPage page,
            Collector<SeaTunnelRow> output)
            throws AviationstackReadException {
        if (current.published > page.rows.size()) {
            throw new AviationstackReadException(
                    resource, current.offset, "invalid checkpoint row offset");
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
    public List<AviationstackSplit> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return position == null ? List.of() : List.of(position);
        }
    }

    /**
     * Restores one ordered scan and detects configuration scope changes before any HTTPS.
     *
     * <p>The scope includes the resource order, key, endpoint, page size and delivery window.
     */
    @Override
    public void addSplits(List<AviationstackSplit> splits) {
        if (splits.isEmpty()) {
            return;
        }
        synchronized (stateLock) {
            if (position != null || splits.size() != 1) {
                throw AviationstackConfig.invalid(
                        "checkpoint", "expected one ordered catalog split");
            }
            AviationstackSplit received = splits.get(0);
            if (!config.scope.equals(received.scope)) {
                throw AviationstackConfig.invalid("checkpoint", "read scope changed");
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
        Future<AviationstackPage> active = acquisition;
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
