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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpPosition.Phase.DONE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpPosition.Phase.POLL;

/**
 * Executes one bounded provider step outside the engine thread.
 *
 * <p>Rows and their continuation become visible together under the collector's checkpoint lock.
 * Cancellation can close the active transfer independently of that lock.
 */
final class SpReader implements SourceReader<SeaTunnelRow, SpPosition> {
    private final Context context;
    private final SpSettings settings;
    private final String tableId;
    private final Object stateLock = new Object();
    private final Object transportLock = new Object();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    action -> {
                        Thread thread = new Thread(action, "amazon-sp-api-reader");
                        thread.setDaemon(true);
                        return thread;
                    });
    private SpPosition state;
    private volatile Future<Batch> pending;
    private volatile boolean cancelled;
    private boolean finished;
    private long deadline;
    private SpTransport transport;
    private SpApiPages api;
    private SpReports reports;

    SpReader(Context context, SpSettings settings, String tableId) {
        this.context = context;
        this.settings = settings;
        this.tableId = tableId;
    }

    @Override
    public void open() {}

    /**
     * Publishes completed steps and leaves pending I/O available for engine cancellation.
     *
     * <p>Provider poll waits and request retries never hold the checkpoint lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (cancelled || finished) {
            return;
        }
        SpPosition current;
        synchronized (stateLock) {
            current = state;
        }
        if (current == null) {
            Thread.sleep(10L);
            return;
        }
        if (current.phase == DONE) {
            finished = true;
            context.signalNoMoreElement();
            return;
        }
        Future<Batch> job = pending;
        if (job == null) {
            if (current.phase == POLL
                    && current.nextPoll != null
                    && Instant.now().isBefore(current.nextPoll)) {
                Thread.sleep(20L);
            } else {
                schedule(current);
            }
        } else if (job.isDone()) {
            Batch batch = resolved(job);
            if (batch != null) {
                synchronized (output.getCheckpointLock()) {
                    synchronized (stateLock) {
                        if (!cancelled) {
                            batch.rows.forEach(output::collect);
                            state = batch.position;
                        }
                    }
                }
            }
            pending = null;
        } else if (System.nanoTime() - deadline >= 0L) {
            cancel();
            throw failure("request deadline exceeded");
        } else {
            Thread.sleep(10L);
        }
    }

    private void schedule(SpPosition current) throws SpFailure {
        long bound = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.requestTimeout);
        deadline = bound;
        try {
            pending = worker.submit(() -> fetch(current.copy(), bound));
        } catch (RejectedExecutionException e) {
            if (!cancelled) {
                throw failure("could not schedule the next request");
            }
        }
    }

    private Batch fetch(SpPosition next, long bound) throws Exception {
        if (next.requests++ >= settings.maxRequests) {
            throw failure("max_requests exhausted before the resource ended");
        }
        startTransport();
        List<SpRecord> decoded =
                settings.resource.isReport() ? reports.read(next, bound) : api.read(next, bound);
        List<SeaTunnelRow> rows = new ArrayList<>(decoded.size());
        String marketplace =
                settings.resource == SpResource.GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2
                        ? null
                        : settings.marketplace;
        for (SpRecord record : decoded) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                settings.resource.name(),
                                record.id,
                                record.parent,
                                marketplace,
                                SpJson.write(record.payload, settings.resource.name())
                            });
            row.setTableId(tableId);
            rows.add(row);
        }
        SpTransport.requireTime(settings.resource.name(), bound);
        return new Batch(next, rows);
    }

    private void startTransport() throws IOException {
        if (transport != null) {
            return;
        }
        SpTransport client = new SpTransport(settings);
        synchronized (transportLock) {
            if (cancelled) {
                client.close();
                throw failure("reader was cancelled");
            }
            transport = client;
            SpAuthorization auth = new SpAuthorization(settings, transport);
            api = new SpApiPages(settings, auth);
            reports = new SpReports(settings, auth, transport);
        }
    }

    private Batch resolved(Future<Batch> job) throws Exception {
        try {
            return job.get();
        } catch (CancellationException e) {
            if (cancelled) {
                return null;
            }
            throw failure("request was cancelled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SpFailure) {
                throw (SpFailure) e.getCause();
            }
            if (cancelled) {
                return null;
            }
            throw failure("request processing failed");
        }
    }

    /**
     * Captures only completed output and its continuation.
     *
     * <p>Before assignment the enumerator remains responsible for the split.
     */
    @Override
    public List<SpPosition> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return state == null ? List.of() : List.of(state);
        }
    }

    @Override
    public void addSplits(List<SpPosition> splits) {
        synchronized (stateLock) {
            for (SpPosition restored : splits) {
                if (state != null || !settings.fingerprint.equals(restored.fingerprint)) {
                    throw SpSettings.invalid(
                            "checkpoint", "unexpected split or changed source options");
                }
                state = restored;
            }
        }
    }

    @Override
    public void handleNoMoreSplits() {
        // A pending assignment and a terminal saved split are different states.
    }

    /**
     * Stops the current provider operation and prevents a new transport from becoming active.
     *
     * <p>A cancelled network body or polling delay does not advance the position.
     */
    @Override
    public void cancel() {
        cancelled = true;
        Future<Batch> work = pending;
        if (work != null) {
            work.cancel(true);
        }
        worker.shutdownNow();
        synchronized (transportLock) {
            if (transport != null) {
                transport.close();
            }
        }
    }

    @Override
    public void close() {
        cancel();
    }

    private SpFailure failure(String reason) {
        return new SpFailure(settings.resource.name(), reason);
    }

    /**
     * A completed worker transition, before it is published to the engine.
     *
     * <p>The batch may contain no rows when a report is still processing or a parent page was
     * discovered.
     */
    private static final class Batch {
        final SpPosition position;
        final List<SeaTunnelRow> rows;

        private Batch(SpPosition position, List<SeaTunnelRow> rows) {
            this.position = position;
            this.rows = rows;
        }
    }
}
