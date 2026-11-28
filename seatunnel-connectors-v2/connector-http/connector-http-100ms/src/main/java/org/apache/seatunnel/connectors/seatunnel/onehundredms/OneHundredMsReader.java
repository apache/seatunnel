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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

/**
 * Owns one asynchronous request and one bounded decoded page. Network/decode work stays outside the
 * collector monitor; each accepted row and its resumable prefix are checkpointed atomically.
 * Cancellation closes the independent HTTP owner without waiting for collection or checkpointing.
 */
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

    // Only the poller mutates progress; snapshots take this short lock after the collector lock.
    private final Object stateLock = new Object();
    private OneHundredMsProgress progress;
    private Request request;
    private volatile Future<JsonNode> pending;
    private boolean signaled;
    private PreparedPage prepared;

    OneHundredMsReader(OneHundredMsConfig config, Context context, String tableId) {
        this.config = config;
        this.context = context;
        this.tableId = tableId;
        http = new OneHundredMsHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Yields during network I/O and between accepted rows, including when collection is throttled.
     * Full-page validation and prefix comparison finish before any row from that page is emitted.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (stateLock) {
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
        }
        if (progress.finished) {
            synchronized (output.getCheckpointLock()) {
                if (!closed.get() && !signaled) {
                    signaled = true;
                    context.signalNoMoreElement();
                }
            }
            return;
        }
        if (prepared != null) {
            synchronized (output.getCheckpointLock()) {
                synchronized (stateLock) {
                    if (closed.get()) {
                        return;
                    }
                    PreparedPage page = prepared;
                    int index = progress.data.acceptedCount;
                    if (index < page.rows.size()) {
                        output.collect(page.rows.get(index));
                        progress.data.acceptedCount = index + 1;
                        progress.data.acceptedHash = page.hashes.get(index);
                        rememberAnalyticsClock(page.request);
                    }
                    if (progress.data.acceptedCount == page.rows.size()) {
                        completePage(page);
                        prepared = null;
                    }
                }
            }
            return;
        }
        Future<JsonNode> active = pending;
        if (active == null) {
            synchronized (stateLock) {
                if (!closed.get()) {
                    launch();
                }
            }
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
            preparePage(body);
        }
        pending = null;
        request = null;
    }

    /**
     * Returns a detached current-page prefix; decoded rows and active request objects remain local.
     */
    @Override
    public List<OneHundredMsProgress> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            OneHundredMsProgress state = progress == null ? assigned.get() : progress;
            return state == null
                    ? Collections.emptyList()
                    : Collections.singletonList(new OneHundredMsProgress(state));
        }
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
        boolean continuation = analytics && (cursor != null || progress.data.acceptedCount > 0);
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

    /**
     * Materializes a bounded page without blocking checkpoint snapshots or changing their progress.
     */
    private void preparePage(JsonNode body) throws IOException {
        Request issued = request;
        OneHundredMsResource endpoint = issued.resource;
        OneHundredMsResource.Page page =
                endpoint.page(body, issued.parentId, config.maxRecords, issued.discovery);
        if (issued.discovery) {
            discover(page);
            return;
        }
        progress.data.validate(endpoint, page, config.maxPages);
        List<SeaTunnelRow> rows = new ArrayList<>();
        List<String> hashes = new ArrayList<>();
        String hash = null;
        for (OneHundredMsResource.Value record : page.rows) {
            if (include(endpoint, record)) {
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {
                                    endpoint.tableName(),
                                    record.id,
                                    issued.parentId,
                                    record.updated == null ? null : record.updated.toString(),
                                    record.data.toString()
                                });
                row.setTableId(tableId);
                rows.add(row);
                hash = prefixHash(hash, row);
                hashes.add(hash);
            }
        }
        int accepted = progress.data.acceptedCount;
        if (accepted < 0
                || accepted > rows.size()
                || (accepted > 0 && !hashes.get(accepted - 1).equals(progress.data.acceptedHash))) {
            throw endpoint.invalid("checkpoint page prefix changed or is missing");
        }
        PreparedPage ready = new PreparedPage(issued, page, rows, hashes, page.fingerprint());
        synchronized (stateLock) {
            if (!closed.get()) {
                prepared = ready;
            }
        }
    }

    /**
     * Advances only after the last included row, in the same critical section as its acceptance.
     */
    private void completePage(PreparedPage ready) {
        OneHundredMsResource endpoint = ready.request.resource;
        progress.data.advance(ready.page, ready.fingerprint);
        rememberAnalyticsClock(ready.request);
        if (ready.page.next == null) {
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

    private void rememberAnalyticsClock(Request issued) {
        if (issued.resource == OneHundredMsResource.ANALYTICS_EVENTS
                && progress.analyticsStartedAt == 0) {
            progress.analyticsStartedAt = issued.startedAt;
        }
    }

    /**
     * Discovery ignores emitted update windows and validates all parent additions before
     * committing.
     */
    private void discover(OneHundredMsResource.Page page) throws IOException {
        OneHundredMsResource parent = request.resource;
        OneHundredMsProgress.Scan scan = progress.discovery.copy();
        scan.validate(parent, page, config.maxPages);
        Set<String> history = new HashSet<>(progress.parentHistory);
        List<String> parents = new ArrayList<>();
        for (OneHundredMsResource.Value row : page.rows) {
            if (history.add(digest(row.id))) {
                if (history.size() > config.maxParents) {
                    throw parent.invalid("max_parents was reached before discovery finished");
                }
                parents.add(row.id);
            }
        }
        scan.advance(page, page.fingerprint());
        synchronized (stateLock) {
            if (closed.get()) {
                return;
            }
            progress.parentHistory.clear();
            progress.parentHistory.addAll(history);
            progress.parents = parents;
            progress.parentIndex = 0;
            progress.discovery = scan;
            if (page.next == null) {
                progress.discoveryFinished = ++progress.phase == parent.phases();
                progress.discovery = new OneHundredMsProgress.Scan();
            }
        }
    }

    /**
     * Length framing distinguishes null, empty and concatenated columns. Hashing exact UTF-16 code
     * units also distinguishes Java strings containing isolated surrogate escapes, without a lossy
     * charset replacement or retaining any column payload in the checkpoint.
     */
    private static String prefixHash(String previous, SeaTunnelRow row) {
        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            if (previous != null) {
                hash.update(previous.getBytes(StandardCharsets.US_ASCII));
            }
            byte[] buffer = new byte[8192];
            for (Object field : row.getFields()) {
                String value = (String) field;
                int length = value == null ? -1 : value.length();
                hash.update((byte) (length >>> 24));
                hash.update((byte) (length >>> 16));
                hash.update((byte) (length >>> 8));
                hash.update((byte) length);
                int used = 0;
                for (int i = 0; i < length; i++) {
                    char unit = value.charAt(i);
                    buffer[used++] = (byte) (unit >>> 8);
                    buffer[used++] = (byte) unit;
                    if (used == buffer.length) {
                        hash.update(buffer);
                        used = 0;
                    }
                }
                hash.update(buffer, 0, used);
            }
            return Base64.getUrlEncoder().withoutPadding().encodeToString(hash.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable");
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

    /**
     * Transient validated rows and prefix hashes are bounded by the configured response/record
     * caps.
     */
    private static final class PreparedPage {
        final Request request;
        final OneHundredMsResource.Page page;
        final List<SeaTunnelRow> rows;
        final List<String> hashes;
        final String fingerprint;

        PreparedPage(
                Request request,
                OneHundredMsResource.Page page,
                List<SeaTunnelRow> rows,
                List<String> hashes,
                String fingerprint) {
            this.request = request;
            this.page = page;
            this.rows = rows;
            this.hashes = hashes;
            this.fingerprint = fingerprint;
        }
    }

    /**
     * The page stays associated with the route and clock bound it was issued for. Prepared output
     * retains this request metadata even after the asynchronous future is released.
     */
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
