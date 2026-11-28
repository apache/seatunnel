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

package org.apache.seatunnel.connectors.seatunnel.aha;

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

/**
 * Fetches one page outside the checkpoint lock and advances only after validating and emitting it.
 */
final class AhaReader implements SourceReader<SeaTunnelRow, AhaProgress> {
    private final AhaConfig config;
    private final Context context;
    private final String tableId;
    private final AhaHttps http;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<AhaProgress> assignment = new AtomicReference<>();
    private final ExecutorService network =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "aha-v1-request");
                        thread.setDaemon(true);
                        return thread;
                    });

    // The poller changes the cursor and active request association. Cancel only interrupts I/O.
    private AhaProgress progress;
    private Request request;
    private boolean sentEof;
    private volatile Future<AhaResource.Page> pending;

    AhaReader(AhaConfig config, Context context, String tableId) {
        this.config = config;
        this.context = context;
        this.tableId = tableId;
        http = new AhaHttps(config);
    }

    @Override
    public void open() {}

    /** An asynchronous blocked response leaves the same page eligible for checkpoint recovery. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            if (closed.get()) {
                return;
            }
            if (progress == null) {
                AhaProgress initial = assignment.getAndSet(null);
                if (initial == null) {
                    return;
                }
                if (!config.fingerprint.equals(initial.fingerprint)) {
                    throw AhaConfig.invalid("incompatible checkpoint parameters");
                }
                progress = new AhaProgress(initial);
            }
            if (progress.finished) {
                if (!sentEof) {
                    sentEof = true;
                    context.signalNoMoreElement();
                }
                return;
            }
            Future<AhaResource.Page> active = pending;
            if (active == null) {
                beginPage();
                return;
            }
            if (!active.isDone()) {
                if (request.deadline - System.nanoTime() <= 0) {
                    close();
                    throw request.endpoint.problem("request deadline exceeded");
                }
                return;
            }
            AhaResource.Page page = result(active);
            if (!closed.get()) {
                apply(page, output);
            }
            pending = null;
            request = null;
        }
    }

    @Override
    public List<AhaProgress> snapshotState(long checkpointId) {
        AhaProgress state = progress == null ? assignment.get() : progress;
        return state == null
                ? Collections.emptyList()
                : Collections.singletonList(new AhaProgress(state));
    }

    @Override
    public void addSplits(List<AhaProgress> splits) {
        if (splits.size() != 1) {
            throw AhaConfig.invalid("expected one assigned crawl split");
        }
        assignment.compareAndSet(null, splits.get(0));
    }

    @Override
    public void handleNoMoreSplits() {}

    /** Closes the active network operation from the engine cancellation thread. */
    @Override
    public void cancel() {
        close();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<?> active = pending;
            if (active != null) {
                active.cancel(true);
            }
            network.shutdownNow();
        }
    }

    private void beginPage() throws IOException {
        AhaResource resource = config.resources.get(progress.index);
        boolean discovery =
                resource.parent != null && progress.parentIndex == progress.parents.size();
        if (discovery && progress.discoveryDone) {
            progress.nextResource(config.resources.size());
            return;
        }
        AhaResource endpoint = discovery ? resource.parent : resource;
        String parent =
                resource.parent != null && !discovery
                        ? progress.parents.get(progress.parentIndex)
                        : null;
        int pageIndex = discovery ? progress.discovery.page : progress.data.page;
        String target = endpoint.target(pageIndex, config.pageSize, parent);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.totalTimeout);
        request = new Request(endpoint, parent, discovery, deadline);
        try {
            pending = network.submit(() -> http.get(endpoint, target, deadline));
        } catch (RejectedExecutionException e) {
            if (!closed.get()) {
                throw endpoint.problem("HTTPS worker is unavailable");
            }
        }
    }

    private AhaResource.Page result(Future<AhaResource.Page> active) throws Exception {
        try {
            return active.get();
        } catch (CancellationException e) {
            if (!closed.get()) {
                throw request.endpoint.problem("HTTPS request canceled unexpectedly");
            }
        } catch (ExecutionException e) {
            if (!closed.get()) {
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                }
                throw request.endpoint.problem("unexpected HTTPS request failure");
            }
        }
        return null;
    }

    private void apply(AhaResource.Page page, Collector<SeaTunnelRow> output) throws IOException {
        AhaResource endpoint = request.endpoint;
        if (request.discovery) {
            acceptParents(page);
            return;
        }
        progress.data.validate(endpoint, page, config);
        for (AhaResource.Record record : page.records) {
            if (closed.get()) {
                return;
            }
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                endpoint.key(), record.id, request.parentId, record.data.toString()
                            });
            row.setTableId(tableId);
            output.collect(row);
        }
        progress.data.advance(page);
        if (page.last()) {
            if (endpoint.parent == null) {
                progress.nextResource(config.resources.size());
            } else {
                progress.parentIndex++;
                progress.data = new AhaProgress.Scan();
            }
        }
    }

    /** Consumes a complete parent page independently of any selected root resource. */
    private void acceptParents(AhaResource.Page page) throws IOException {
        AhaResource endpoint = request.endpoint;
        progress.discovery.validate(endpoint, page, config);
        List<String> upcoming = new ArrayList<>(page.records.size());
        for (AhaResource.Record entry : page.records) {
            if (!progress.discoveredIds.add(AhaConfig.digest(entry.id))) {
                throw endpoint.problem("parent discovery repeated a provider ID");
            }
            if (progress.discoveredIds.size() > config.maxParents) {
                throw endpoint.problem("parent discovery exceeds max_parents");
            }
            upcoming.add(entry.id);
        }
        progress.parents = upcoming;
        progress.parentIndex = 0;
        progress.discovery.advance(page);
        progress.discoveryDone = page.last();
    }

    /** Captures the exact parent and endpoint associated with a single response. */
    private static final class Request {
        final AhaResource endpoint;
        final String parentId;
        final boolean discovery;
        final long deadline;

        Request(AhaResource endpoint, String parentId, boolean discovery, long deadline) {
            this.endpoint = endpoint;
            this.parentId = parentId;
            this.discovery = discovery;
            this.deadline = deadline;
        }
    }
}
