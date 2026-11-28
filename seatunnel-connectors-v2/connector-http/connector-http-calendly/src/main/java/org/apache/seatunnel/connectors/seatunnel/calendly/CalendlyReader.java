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

package org.apache.seatunnel.connectors.seatunnel.calendly;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlyJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlyJson.Page;
import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlyJson.Record;
import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlyJson.Reference;
import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlySource.Position;
import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlySource.Split;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Reads one Calendly resource with a dedicated asynchronous retrieval worker.
 *
 * <p>For child resources the worker walks an ordered parent inventory. It completes each child's
 * cursor chain before advancing the parent row. The collector lock protects only ready data and its
 * matching raw offset, so a checkpoint never waits on HTTPS or OAuth storage.
 */
final class CalendlyReader implements SourceReader<SeaTunnelRow, Split> {
    private static final int ROWS_PER_POLL = 32;
    private static final Pattern DIGEST = Pattern.compile("[0-9a-f]{64}");

    private final CalendlyConfig config;
    private final Context context;
    private final String table;
    private final CalendlyHttp http;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread t = new Thread(task, "seatunnel-calendly-retrieval");
                        t.setDaemon(true);
                        return t;
                    });

    private volatile boolean noMoreSplits;
    private volatile Future<Identity> identityTask;
    private volatile Future<LoadedPage> pageTask;
    private boolean assigned;
    private boolean finished;
    private boolean eof;
    private Instant ceiling;
    private String subject;
    private Reference organization;
    private int pages;
    private Frame parent;
    private Frame leaf;

    CalendlyReader(CalendlyConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        http = new CalendlyHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Publishes only previously fetched rows and atomically advances the matching raw position.
     *
     * @param collector the engine's row collector and checkpoint lock
     * @throws IOException if a REST page, credential, or restored state is unusable
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws IOException {
        synchronized (collector.getCheckpointLock()) {
            synchronized (progress) {
                if (closed.get() || eof) {
                    return;
                }
                if (finished || (!assigned && noMoreSplits)) {
                    eof = true;
                    context.signalNoMoreElement();
                    return;
                }
                if (!assigned) {
                    return;
                }
                try {
                    poll(collector);
                } catch (Failure e) {
                    if (!closed.get()) {
                        throw new IOException(
                                "Calendly resource " + config.resource.id + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    private void poll(Collector<SeaTunnelRow> collector) throws Failure {
        if (ceiling == null) {
            ceiling = config.end == null ? Instant.now() : config.end;
        }
        if (organization == null) {
            verifyIdentity(collector);
            return;
        }
        CalendlyResource resource = config.resource;
        if (resource.cursorField != null && config.lower.isAfter(ceiling)) {
            finish();
            return;
        }
        if (resource.parent == null) {
            if (load(leaf, resource, resource.directScope(organization), false)
                    && emit(collector)
                    && leaf.advance()) {
                finish();
            }
            return;
        }
        if (!load(parent, resource.parent, organization, true)) {
            return;
        }
        if (parent.offset == parent.loaded.count) {
            if (parent.advance()) {
                finish();
            }
            return;
        }
        Reference childScope = parent.loaded.parents.get(parent.offset);
        if (load(leaf, resource, childScope, false) && emit(collector) && leaf.advance()) {
            leaf = new Frame(new Position());
            parent.offset++;
        }
    }

    /**
     * Resolves the actual user and organization, including after a nonterminal restore.
     *
     * <p>Keeping this step independent of list pages prevents a valid cursor from being used for
     * another account when a credential file was reauthorized during downtime.
     */
    private void verifyIdentity(Collector<SeaTunnelRow> collector) throws Failure {
        if (identityTask == null) {
            Instant fixedCeiling = ceiling;
            identityTask = submit(() -> identity(fixedCeiling));
            return;
        }
        if (!identityTask.isDone()) {
            return;
        }
        Identity identity = result(identityTask);
        identityTask = null;
        if (closed.get()) {
            return;
        }
        if (subject != null && !subject.equals(identity.digest)) {
            throw CalendlyJson.failure(
                    "Calendly user or current organization changed during restore");
        }
        subject = identity.digest;
        organization = identity.organization;
        if (config.resource == CalendlyResource.API_USER) {
            collect(collector, identity.user);
            finish();
        }
    }

    private Identity identity(Instant fixedCeiling) throws IOException {
        JsonNode user = CalendlyJson.single(http.fetch(CalendlyResource.API_USER.path));
        Reference userRef = CalendlyJson.reference(user.get("uri"), "/users");
        Reference orgRef =
                CalendlyJson.reference(user.get("current_organization"), "/organizations");
        Record row =
                config.resource == CalendlyResource.API_USER
                        ? CalendlyJson.project(user, config.resource, null, config, fixedCeiling)
                        : null;
        return new Identity(
                orgRef, CalendlyJson.fingerprint(List.of(userRef.uri, orgRef.uri)), row);
    }

    /**
     * Starts or retrieves one immutable page fetch without blocking the checkpoint thread.
     *
     * <p>A replayed page must match the raw saved fingerprint. It already counts toward the
     * checkpointed page budget, so retries and restoration do not double-charge accepted pages.
     */
    private boolean load(Frame frame, CalendlyResource resource, Reference scope, boolean hidden)
            throws Failure {
        if (frame.loaded != null) {
            return true;
        }
        if (pageTask == null) {
            if (frame.digest == null && pages >= config.maxPages) {
                throw CalendlyJson.failure("pagination reached max_pages before completion");
            }
            Instant fixedCeiling = ceiling;
            String target = resource.target(config, scope, frame.cursor, fixedCeiling, hidden);
            boolean missingChild =
                    resource.parent != null && frame.cursor == null && frame.digest == null;
            pageTask =
                    submit(
                            () ->
                                    fetchPage(
                                            resource,
                                            scope,
                                            target,
                                            fixedCeiling,
                                            hidden,
                                            missingChild));
            return false;
        }
        if (!pageTask.isDone()) {
            return false;
        }
        LoadedPage loaded = result(pageTask);
        pageTask = null;
        if (closed.get()) {
            return false;
        }
        if (loaded.next != null && frame.seen.contains(cursorDigest(loaded.next))) {
            throw CalendlyJson.failure("pagination returned a repeated cursor");
        }
        if (frame.digest == null) {
            pages++;
        } else if (!frame.digest.equals(loaded.digest) || frame.offset > loaded.count) {
            throw CalendlyJson.failure("saved Calendly page changed since its checkpoint");
        }
        frame.digest = loaded.digest;
        frame.loaded = loaded;
        return true;
    }

    private LoadedPage fetchPage(
            CalendlyResource resource,
            Reference scope,
            String target,
            Instant fixedCeiling,
            boolean hidden,
            boolean missingChild)
            throws IOException {
        JsonNode result = http.fetch(target, missingChild ? resource : null);
        if (result == null) {
            return new LoadedPage(
                    new Page(List.of(), null, CalendlyJson.fingerprint(List.of("missing child"))),
                    List.of(),
                    List.of());
        }
        Page page = CalendlyJson.page(result, resource, config.pageSize);
        List<Reference> parents = hidden ? new ArrayList<>(page.rows.size()) : List.of();
        List<Record> records = hidden ? List.of() : new ArrayList<>(page.rows.size());
        for (JsonNode row : page.rows) {
            if (hidden) {
                CalendlyJson.checkScope(row, resource, scope);
                parents.add(resource.childScope(row));
            } else {
                records.add(CalendlyJson.project(row, resource, scope, config, fixedCeiling));
            }
        }
        return new LoadedPage(page, parents, records);
    }

    private <T> Future<T> submit(Callable<T> retrieval) throws Failure {
        try {
            return worker.submit(retrieval);
        } catch (RejectedExecutionException ignored) {
            throw CalendlyJson.failure("retrieval worker is unavailable");
        }
    }

    private static <T> T result(Future<T> future) throws Failure {
        try {
            return future.get();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            throw CalendlyJson.failure("retrieval interrupted");
        } catch (CancellationException ignored) {
            throw CalendlyJson.failure("retrieval canceled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Failure) {
                throw (Failure) e.getCause();
            }
            throw CalendlyJson.failure("retrieval worker failed");
        }
    }

    private boolean emit(Collector<SeaTunnelRow> collector) {
        int end = Math.min(leaf.loaded.count, leaf.offset + ROWS_PER_POLL);
        while (leaf.offset < end && !closed.get()) {
            Record value = leaf.loaded.records.get(leaf.offset);
            if (value != null) {
                collect(collector, value);
            }
            leaf.offset++;
        }
        return leaf.offset == leaf.loaded.count;
    }

    private void collect(Collector<SeaTunnelRow> collector, Record value) {
        SeaTunnelRow record =
                new SeaTunnelRow(
                        new Object[] {value.id, value.parentId, value.timestamp, value.json});
        record.setTableId(table);
        collector.collect(record);
    }

    private void finish() {
        finished = true;
        parent = new Frame(new Position());
        leaf = new Frame(new Position());
        pages = 0;
    }

    /**
     * Captures both cursor frames, the fixed window and the authenticated subject digest.
     *
     * @param checkpointId the engine checkpoint identifier
     * @return saved traversal state, or no state before assignment
     */
    @Override
    public List<Split> snapshotState(long checkpointId) {
        synchronized (progress) {
            return assigned
                    ? List.of(
                            new Split(
                                    config.scope,
                                    subject,
                                    ceiling,
                                    pages,
                                    parent.snapshot(),
                                    leaf.snapshot(),
                                    finished))
                    : List.of();
        }
    }

    /**
     * Restores the sole traversal and rejects an incompatible scope before any network work.
     *
     * @param splits the single assigned source split
     */
    @Override
    public void addSplits(List<Split> splits) {
        synchronized (progress) {
            if (assigned || eof || splits.size() != 1) {
                throw CalendlyConfig.invalid("expected one assigned Calendly traversal");
            }
            Split saved = splits.get(0);
            validate(config, saved);
            ceiling = saved.ceiling;
            subject = saved.subject;
            pages = saved.pages;
            parent = new Frame(saved.parent);
            leaf = new Frame(saved.leaf);
            finished = saved.finished;
            assigned = true;
        }
    }

    /**
     * Checks structural invariants of the two frames before contacting either origin.
     *
     * @param config restored source configuration
     * @param saved traversal to validate
     */
    static void validate(CalendlyConfig config, Split saved) {
        if (saved == null || !config.scope.equals(saved.scope)) {
            throw CalendlyConfig.invalid(
                    "source authentication scope, resource or read options changed during restore");
        }
        if (saved.parent == null
                || saved.leaf == null
                || saved.pages < 0
                || saved.pages > config.maxPages
                || (saved.subject != null && (!digest(saved.subject) || saved.ceiling == null))
                || (saved.subject == null && saved.pages != 0)
                || (saved.ceiling != null
                        && config.end != null
                        && !config.end.equals(saved.ceiling))
                || (saved.finished && saved.pages != 0)
                || (saved.pages == 0 && (!saved.parent.empty() || !saved.leaf.empty()))
                || (config.resource.parent == null && !saved.parent.empty())
                || (config.resource.parent != null
                        && saved.parent.digest == null
                        && !saved.leaf.empty())) {
            throw CalendlyConfig.invalid("invalid Calendly split position");
        }
        if (config.resource.parent != null) {
            validatePosition(saved.parent, config.resource.parent, config, saved.pages);
        }
        validatePosition(saved.leaf, config.resource, config, saved.pages);
    }

    private static void validatePosition(
            Position frame, CalendlyResource resource, CalendlyConfig config, int pages) {
        int maximumOffset = resource.counted ? config.pageSize : config.maxBytes;
        if (frame.seen.size() > pages
                || (frame.cursor == null) != frame.seen.isEmpty()
                || (frame.cursor != null
                        && (!resource.pageable()
                                || !CalendlyConfig.validCursor(frame.cursor)
                                || !frame.seen.contains(cursorDigest(frame.cursor))))
                || new HashSet<>(frame.seen).size() != frame.seen.size()
                || frame.seen.stream().anyMatch(value -> !digest(value))
                || frame.offset < 0
                || frame.offset > maximumOffset
                || (frame.offset != 0 && frame.digest == null)
                || (frame.digest != null && !digest(frame.digest))) {
            throw CalendlyConfig.invalid("invalid saved cursor or raw row position");
        }
    }

    private static boolean digest(String value) {
        return value != null && DIGEST.matcher(value).matches();
    }

    private static String cursorDigest(String value) {
        return CalendlyJson.fingerprint(List.of(value));
    }

    /**
     * Records coordinator exhaustion for handling on the reader's polling thread.
     *
     * <p>An unassigned restored reader sends its own end-of-input signal from pollNext.
     */
    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    /**
     * Cancels current or future exchanges without waiting on source progress locks.
     *
     * <p>The same cancellation flag also prevents work if the first split is assigned later.
     */
    @Override
    public void cancel() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            cancel(identityTask);
            cancel(pageTask);
            worker.shutdownNow();
        }
    }

    private static void cancel(Future<?> future) {
        if (future != null) {
            future.cancel(true);
        }
    }

    /**
     * Closes the reader and wakes outstanding API work.
     *
     * <p>Both engine shutdown paths share the same idempotent cancellation.
     */
    @Override
    public void close() {
        cancel();
    }

    /**
     * Holds a validated account identity and its optional selected user record.
     *
     * <p>The user record is carried only for the singleton selection and never enters a split.
     */
    private static final class Identity {
        final Reference organization;
        final String digest;
        final Record user;

        Identity(Reference organization, String digest, Record user) {
            this.organization = organization;
            this.digest = digest;
            this.user = user;
        }
    }

    /**
     * Holds one typed and validated page outside the serializable split.
     *
     * <p>The frame keeps either parent references or projected data until the raw offsets advance.
     */
    private static final class LoadedPage {
        final int count;
        final String next;
        final String digest;
        final List<Reference> parents;
        final List<Record> records;

        LoadedPage(Page page, List<Reference> parents, List<Record> records) {
            count = page.rows.size();
            next = page.next;
            digest = page.digest;
            this.parents = parents;
            this.records = records;
        }
    }

    /**
     * Holds the live state of either the hidden parent or selected resource cursor.
     *
     * <p>Its snapshot discards the loaded page and retains only the replay position.
     */
    private static final class Frame {
        private String cursor;
        private final Set<String> seen;
        private int offset;
        private String digest;
        private LoadedPage loaded;

        Frame(Position saved) {
            cursor = saved.cursor;
            seen = new HashSet<>(saved.seen);
            offset = saved.offset;
            digest = saved.digest;
        }

        Position snapshot() {
            return new Position(cursor, List.copyOf(seen), offset, digest);
        }

        boolean advance() {
            cursor = loaded.next;
            offset = 0;
            digest = null;
            loaded = null;
            if (cursor == null) {
                return true;
            }
            seen.add(cursorDigest(cursor));
            return false;
        }
    }
}
