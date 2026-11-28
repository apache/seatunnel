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

package org.apache.seatunnel.connectors.seatunnel.acuityscheduling;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Advances resource or window state only after all selected rows reach the collector. */
final class AcuityReader implements SourceReader<SeaTunnelRow, AcuityProgress> {
    private final AcuityConfig config;
    private final Context context;
    private final String tableId;
    private final AcuityHttp http;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<AcuityProgress> received = new AtomicReference<>();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    call -> {
                        Thread thread = new Thread(call, "acuity-source-https");
                        thread.setDaemon(true);
                        return thread;
                    });

    // Only the polling thread advances the crawl. Snapshotting uses the collector checkpoint lock.
    private AcuityProgress progress;
    private AcuityResource inFlightResource;
    private AcuityProgress.Window inFlightWindow;
    private long deadline;
    private boolean signaled;
    private volatile Future<JsonNode> inFlight;

    AcuityReader(AcuityConfig config, Context context, String tableId) {
        this.config = config;
        this.context = context;
        this.tableId = tableId;
        http = new AcuityHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Never waits on network I/O while holding the barrier lock. A pending request's window remains
     * the next window in checkpoint state until its rows are emitted.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws Exception {
        synchronized (collector.getCheckpointLock()) {
            if (closed.get()) {
                return;
            }
            if (progress == null) {
                AcuityProgress split = received.getAndSet(null);
                if (split == null) {
                    return;
                }
                if (!config.fingerprint.equals(split.fingerprint)) {
                    throw AcuityConfig.invalid("incompatible crawl checkpoint");
                }
                progress = new AcuityProgress(split);
            }
            if (progress.finished) {
                if (!signaled) {
                    signaled = true;
                    context.signalNoMoreElement();
                }
                return;
            }
            Future<JsonNode> pending = inFlight;
            if (pending == null) {
                launch();
            } else if (pending.isDone()) {
                JsonNode body = result(pending);
                if (!closed.get()) {
                    apply(body, collector);
                }
                inFlight = null;
            } else if (deadline - System.nanoTime() <= 0) {
                close();
                throw inFlightResource.failure("request deadline exceeded");
            }
        }
    }

    @Override
    public List<AcuityProgress> snapshotState(long checkpointId) {
        AcuityProgress state = progress == null ? received.get() : progress;
        return state == null
                ? Collections.emptyList()
                : Collections.singletonList(new AcuityProgress(state));
    }

    @Override
    public void addSplits(List<AcuityProgress> splits) {
        if (splits.size() != 1) {
            throw AcuityConfig.invalid("expected one assigned crawl split");
        }
        received.compareAndSet(null, splits.get(0));
    }

    @Override
    public void handleNoMoreSplits() {}

    /** Cancellation is sticky, including calls arriving before the request worker starts. */
    @Override
    public void cancel() {
        close();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<?> pending = inFlight;
            if (pending != null) {
                pending.cancel(true);
            }
            worker.shutdownNow();
        }
    }

    private void launch() throws IOException {
        AcuityResource resource = config.resources.get(progress.resourceIndex);
        AcuityProgress.Window window =
                resource.dated ? progress.nextWindow(config.windowDays) : null;
        if (resource.dated && window == null) {
            progress.nextResource(config);
            return;
        }
        if (progress.completedRequests >= config.maxRequests) {
            throw resource.failure("max_requests reached before the crawl finished");
        }
        String target = resource.request(window, config);
        long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.totalTimeout);
        inFlightResource = resource;
        inFlightWindow = window;
        deadline = expires;
        try {
            inFlight = worker.submit(() -> http.list(resource, target, expires));
        } catch (RejectedExecutionException e) {
            if (!closed.get()) {
                throw resource.failure("HTTPS request worker is unavailable");
            }
        }
    }

    private JsonNode result(Future<JsonNode> pending) throws Exception {
        try {
            return pending.get();
        } catch (CancellationException e) {
            if (!closed.get()) {
                throw inFlightResource.failure("HTTPS request canceled unexpectedly");
            }
        } catch (ExecutionException e) {
            if (!closed.get()) {
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                }
                throw inFlightResource.failure("unexpected HTTPS request error");
            }
        }
        return null;
    }

    private void apply(JsonNode body, Collector<SeaTunnelRow> collector) throws IOException {
        AcuityResource resource = inFlightResource;
        AcuityProgress.Window window = inFlightWindow;
        if (window != null && body.size() >= config.dateLimit) {
            if (window.singleDay()) {
                throw resource.failure(
                        "single-day window "
                                + window.start
                                + " reached max_date_results and may be truncated");
            }
            progress.subdivide();
            progress.completedRequests++;
            return;
        }
        // Validate the entire payload before the checkpoint lock permits another snapshot.
        List<SeaTunnelRow> rows = new ArrayList<>();
        String from = window == null ? null : window.start.toString();
        String to = window == null ? null : window.end.toString();
        for (JsonNode object : body) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                resource.name,
                                resource.identity(object),
                                from,
                                to,
                                object.toString()
                            });
            row.setTableId(tableId);
            rows.add(row);
        }
        for (SeaTunnelRow row : rows) {
            if (closed.get()) {
                return;
            }
            collector.collect(row);
        }
        if (window == null) {
            progress.nextResource(config);
        } else {
            progress.completeWindow();
        }
        progress.completedRequests++;
    }
}
