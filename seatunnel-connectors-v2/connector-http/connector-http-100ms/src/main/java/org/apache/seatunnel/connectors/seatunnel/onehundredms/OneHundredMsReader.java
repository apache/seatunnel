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

package org.apache.seatunnel.connectors.seatunnel.onehundredms;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.time.Instant;
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

import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsConfig.digest;

/** Coordinates one asynchronous request at a time with a checkpoint-aligned page boundary. */
final class OneHundredMsReader implements SourceReader<SeaTunnelRow, OneHundredMsProgress> {
    private static final long ANALYTICS_LIFETIME_SECONDS = TimeUnit.HOURS.toSeconds(24);

    private final OneHundredMsConfig config;
    private final Context context;
    private final String tableId;
    private final OneHundredMsHttp http;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<OneHundredMsProgress> assigned = new AtomicReference<>();
    private final ExecutorService network =
            Executors.newSingleThreadExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "100ms-source-request");
                        thread.setDaemon(true);
                        return thread;
                    });

    // Updated only by the source poller under the collector checkpoint lock.
    private OneHundredMsProgress progress;
    private Request request;
    private volatile Future<JsonNode> pending;
    private boolean signaled;

    OneHundredMsReader(OneHundredMsConfig config, Context context, String tableId) {
        this.config = config;
        this.context = context;
        this.tableId = tableId;
        http = new OneHundredMsHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Starts or harvests one request and yields immediately while the network task is in flight.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            if (closed.get()) {
                return;
            }
            if (progress == null) {
                OneHundredMsProgress initial = assigned.getAndSet(null);
                if (initial == null) {
                    return;
                }
                if (!config.planFingerprint.equals(initial.fingerprint)) {
                    throw OneHundredMsConfig.invalid("incompatible crawl checkpoint");
                }
                progress = new OneHundredMsProgress(initial);
            }
            if (progress.finished) {
                if (!signaled) {
                    signaled = true;
                    context.signalNoMoreElement();
                }
                return;
            }
            Future<JsonNode> active = pending;
            if (active == null) {
                launch();
                return;
            }
            if (!active.isDone()) {
                if (request.deadline - System.nanoTime() <= 0) {
                    close();
                    throw request.resource.invalid("request deadline exceeded");
                }
                return;
            }
            JsonNode body = response(active);
            if (!closed.get()) {
                applyPage(body, output);
            }
            pending = null;
            request = null;
        }
    }

    @Override
    public List<OneHundredMsProgress> snapshotState(long checkpointId) {
        OneHundredMsProgress state = progress == null ? assigned.get() : progress;
        return state == null
                ? Collections.emptyList()
                : Collections.singletonList(new OneHundredMsProgress(state));
    }

    @Override
    public void addSplits(List<OneHundredMsProgress> splits) {
        if (splits.size() != 1) {
            throw OneHundredMsConfig.invalid("expected one crawl split");
        }
        assigned.compareAndSet(null, splits.get(0));
    }

    @Override
    public void handleNoMoreSplits() {}

    /**
     * The engine can interrupt a blocked socket from another thread without the checkpoint lock.
     */
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

    private void launch() throws IOException {
        OneHundredMsResource resource = config.resources.get(progress.resourceIndex);
        boolean discovery =
                resource.parent != null && progress.parentIndex == progress.parents.size();
        if (discovery && progress.discoveryFinished) {
            progress.nextResource(config.resources.size());
            return;
        }
        OneHundredMsResource endpoint = discovery ? resource.parent : resource;
        String parent =
                !discovery && resource.parent != null
                        ? progress.parents.get(progress.parentIndex)
                        : null;
        String cursor = discovery ? progress.discovery.current : progress.data.current;
        long startedAt = Instant.now().getEpochSecond();
        boolean analytics = resource == OneHundredMsResource.ANALYTICS_EVENTS && !discovery;
        boolean continuation = analytics && cursor != null;
        if (continuation
                && (progress.analyticsStartedAt <= 0
                        || startedAt - progress.analyticsStartedAt >= ANALYTICS_LIFETIME_SECONDS)) {
            throw resource.invalid(
                    "analytics continuation expired after its 24-hour snapshot window");
        }
        int phase = resource.parent == null || discovery ? progress.phase : 0;
        String target = endpoint.target(phase, config.pageSize, parent, cursor);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.requestTimeout);
        request = new Request(endpoint, parent, discovery, startedAt, deadline);
        try {
            pending = network.submit(() -> http.get(endpoint, target, continuation, deadline));
        } catch (RejectedExecutionException e) {
            if (!closed.get()) {
                throw resource.invalid("request worker is unavailable");
            }
        }
    }

    private JsonNode response(Future<JsonNode> active) throws Exception {
        try {
            return active.get();
        } catch (CancellationException e) {
            if (!closed.get()) {
                throw request.resource.invalid("request canceled unexpectedly");
            }
            return null;
        } catch (ExecutionException e) {
            if (closed.get()) {
                return null;
            }
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw request.resource.invalid("unexpected request worker failure");
        }
    }

    private void applyPage(JsonNode body, Collector<SeaTunnelRow> output) throws IOException {
        OneHundredMsResource endpoint = request.resource;
        OneHundredMsResource.Page page =
                endpoint.page(body, request.parentId, config.maxRecords, request.discovery);
        if (request.discovery) {
            discover(page);
            return;
        }
        progress.data.validate(endpoint, page, config.maxPages);
        // All records and the continuation are validated before the page can reach its collector.
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (OneHundredMsResource.Value record : page.rows) {
            if (include(endpoint, record)) {
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {
                                    endpoint.tableName(),
                                    record.id,
                                    request.parentId,
                                    record.updated == null ? null : record.updated.toString(),
                                    record.data.toString()
                                });
                row.setTableId(tableId);
                rows.add(row);
            }
        }
        for (SeaTunnelRow row : rows) {
            if (closed.get()) {
                return;
            }
            output.collect(row);
        }
        progress.data.advance(page);
        if (endpoint == OneHundredMsResource.ANALYTICS_EVENTS && progress.analyticsStartedAt == 0) {
            progress.analyticsStartedAt = request.startedAt;
        }
        if (page.next == null) {
            if (endpoint.parent != null) {
                progress.parentIndex++;
                progress.data = new OneHundredMsProgress.Scan();
                progress.analyticsStartedAt = 0;
            } else if (++progress.phase < endpoint.phases()) {
                progress.data = new OneHundredMsProgress.Scan();
            } else {
                progress.nextResource(config.resources.size());
            }
        }
    }

    /** Discovery ignores any update window applied to emitted room/template records. */
    private void discover(OneHundredMsResource.Page page) throws IOException {
        OneHundredMsResource parent = request.resource;
        progress.discovery.validate(parent, page, config.maxPages);
        List<String> parents = new ArrayList<>();
        for (OneHundredMsResource.Value row : page.rows) {
            if (progress.parentHistory.add(digest(row.id))) {
                if (progress.parentHistory.size() > config.maxParents) {
                    throw parent.invalid("max_parents was reached before discovery finished");
                }
                parents.add(row.id);
            }
        }
        progress.parents = parents;
        progress.parentIndex = 0;
        progress.discovery.advance(page);
        if (page.next == null) {
            progress.discoveryFinished = ++progress.phase == parent.phases();
            progress.discovery = new OneHundredMsProgress.Scan();
        }
    }

    private boolean include(OneHundredMsResource resource, OneHundredMsResource.Value record)
            throws IOException {
        if (config.startDate == null
                || (resource != OneHundredMsResource.ROOMS
                        && resource != OneHundredMsResource.TEMPLATES
                        && resource != OneHundredMsResource.RECORDINGS)) {
            return true;
        }
        if (record.updated == null) {
            throw resource.invalid("incremental record lacks both update and creation times");
        }
        return !record.updated.isBefore(config.startDate)
                && !record.updated.isAfter(progress.upperBound);
    }

    /** The page stays associated with the route and clock bound it was issued for. */
    private static final class Request {
        final OneHundredMsResource resource;
        final String parentId;
        final boolean discovery;
        final long startedAt;
        final long deadline;

        Request(
                OneHundredMsResource resource,
                String parentId,
                boolean discovery,
                long startedAt,
                long deadline) {
            this.resource = resource;
            this.parentId = parentId;
            this.discovery = discovery;
            this.startedAt = startedAt;
            this.deadline = deadline;
        }
    }
}
