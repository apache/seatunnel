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

package org.apache.seatunnel.connectors.seatunnel.bigmailer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerConfig.Resource;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerJson.Page;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerJson.Row;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSource.Cursor;
import org.apache.seatunnel.connectors.seatunnel.bigmailer.BigMailerSource.PageCheckpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Reads at most one parent and one child page at a time.
 *
 * <p>HTTP retrieval is asynchronous. Emission and its corresponding offset share the engine
 * checkpoint lock.
 */
final class BigMailerReader implements SourceReader<SeaTunnelRow, Cursor> {
    private static final int ROWS_PER_POLL = 16;
    private static final Pattern SHA256 = Pattern.compile("[a-f0-9]{64}");
    private final BigMailerConfig config;
    private final Context context;
    private final BigMailerHttp http;
    private final String table;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-bigmailer-retrieval");
                        thread.setDaemon(true);
                        return thread;
                    });

    private boolean assigned;
    private Pager parent;
    private Pager data;
    private boolean finished;
    private boolean eof;
    private volatile Future<Page> inFlight;

    BigMailerReader(BigMailerConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        http = new BigMailerHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Collects only already-loaded rows and moves their cursor within the same checkpoint boundary.
     *
     * @param output the engine collector and checkpoint lock
     * @throws IOException when discovery, pagination or provider I/O fails
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws IOException {
        synchronized (output.getCheckpointLock()) {
            synchronized (progress) {
                if (closed.get() || !assigned || eof) {
                    return;
                }
                if (finished) {
                    eof = true;
                    context.signalNoMoreElement();
                    return;
                }
                try {
                    if (parent == null) {
                        if (readRows(output, null)) {
                            finish();
                        }
                    } else {
                        readBrand(output);
                    }
                } catch (Failure e) {
                    throw new IOException(
                            "BigMailer resource " + config.resource.id + ": " + e.getMessage());
                }
            }
        }
    }

    /**
     * Keeps the discovery page pinned while traversing its current brand collection.
     *
     * <p>An empty discovery page with more results advances the parent cursor without a child call.
     */
    private void readBrand(Collector<SeaTunnelRow> output) throws IOException {
        if (!ready(parent, Resource.BRANDS, null, true)) {
            return;
        }
        if (parent.offset < parent.page.rows.size()) {
            parent.pin();
            String brand = parent.page.rows.get(parent.offset).brand;
            if (!readRows(output, brand)) {
                return;
            }
            parent.offset++;
            data = new Pager(PageCheckpoint.first());
        }
        if (parent.offset == parent.page.rows.size() && !parent.advance()) {
            finish();
        }
    }

    private boolean readRows(Collector<SeaTunnelRow> output, String brand) throws IOException {
        if (!ready(data, config.resource, brand, false)) {
            return false;
        }
        int stop = Math.min(data.page.rows.size(), data.offset + ROWS_PER_POLL);
        while (data.offset < stop && !closed.get()) {
            Row value = data.page.rows.get(data.offset);
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {value.brand, value.data});
            row.setTableId(table);
            output.collect(row);
            data.offset++;
            data.pin();
        }
        return data.offset == data.page.rows.size() && !data.advance();
    }

    /**
     * Submits a single immutable page request or applies its completed result to the active pager.
     *
     * <p>The pending schedule and network work are never included in a checkpoint.
     */
    private boolean ready(Pager pager, Resource resource, String brand, boolean discovering)
            throws IOException {
        if (pager.page != null) {
            return true;
        }
        Future<Page> task = inFlight;
        if (task == null) {
            String target = config.target(resource, brand, pager.token);
            try {
                inFlight =
                        worker.submit(
                                () -> {
                                    JsonNode root = http.fetch(target);
                                    return BigMailerJson.page(
                                            root, resource, config.pageSize, brand, discovering);
                                });
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw BigMailerJson.failure("retrieval worker is unavailable");
                }
            }
            return false;
        }
        if (!task.isDone()) {
            return false;
        }
        try {
            Page page = task.get();
            inFlight = null;
            if (closed.get()) {
                return false;
            }
            pager.accept(page, config.maxPages);
            return true;
        } catch (CancellationException e) {
            if (!closed.get()) {
                throw BigMailerJson.failure("retrieval canceled");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closed.get()) {
                throw BigMailerJson.failure("retrieval interrupted");
            }
        } catch (ExecutionException e) {
            if (!closed.get()) {
                if (e.getCause() instanceof Failure) {
                    throw (Failure) e.getCause();
                }
                throw BigMailerJson.failure("retrieval worker failed");
            }
        }
        return false;
    }

    private void finish() {
        parent = null;
        data = null;
        finished = true;
    }

    /**
     * Captures only consumed progress and the replay identity for any pinned parent or data page.
     *
     * @param checkpointId the engine checkpoint ID
     * @return no split before assignment, otherwise the current or terminal split
     */
    @Override
    public List<Cursor> snapshotState(long checkpointId) {
        synchronized (progress) {
            if (!assigned) {
                return List.of();
            }
            return List.of(
                    new Cursor(
                            config.scope,
                            parent == null ? null : parent.snapshot(),
                            data == null ? null : data.snapshot(),
                            finished));
        }
    }

    /**
     * Validates restored cursor scope and initializes the two pagers.
     *
     * @param splits the single traversal split assigned by the coordinator
     */
    @Override
    public void addSplits(List<Cursor> splits) {
        synchronized (progress) {
            if (assigned || splits.size() != 1) {
                throw BigMailerConfig.invalid("expected one assigned BigMailer traversal");
            }
            Cursor cursor = splits.get(0);
            validate(config, cursor);
            parent = cursor.parent == null ? null : new Pager(cursor.parent);
            data = cursor.data == null ? null : new Pager(cursor.data);
            finished = cursor.finished;
            assigned = true;
        }
    }

    static void validate(BigMailerConfig config, Cursor cursor) {
        if (cursor == null || !config.scope.equals(cursor.scope)) {
            throw BigMailerConfig.invalid("API key, resource or page_size changed during restore");
        }
        if (cursor.finished) {
            if (cursor.parent != null || cursor.data != null) {
                throw BigMailerConfig.invalid("terminal traversal contains live cursor state");
            }
            return;
        }
        if (cursor.data == null || config.resource.brandScoped != (cursor.parent != null)) {
            throw BigMailerConfig.invalid("missing expected parent or data cursor");
        }
        verifyPosition(config, cursor.data, false);
        if (cursor.parent != null) {
            verifyPosition(config, cursor.parent, true);
            if (cursor.parent.digest == null
                    && (cursor.data.pages != 0 || cursor.data.offset != 0)) {
                throw BigMailerConfig.invalid("child cursor lacks a pinned parent page");
            }
        }
    }

    private static void verifyPosition(
            BigMailerConfig config, PageCheckpoint state, boolean parent) {
        if (state.pages < 0
                || state.pages >= config.maxPages
                || state.offset < 0
                || state.offset >= config.pageSize
                || (state.token == null) != (state.pages == 0)
                || state.seen.size() != state.pages
                || (state.offset > 0 && state.digest == null)
                || (state.digest != null
                        && (!SHA256.matcher(state.digest).matches()
                                || (!parent && state.offset == 0)))) {
            throw BigMailerConfig.invalid("invalid paginated checkpoint position");
        }
        Set<String> unique = new HashSet<>(state.seen);
        if (unique.size() != state.seen.size()
                || unique.stream().anyMatch(hash -> !SHA256.matcher(hash).matches())
                || (state.token != null
                        && (!BigMailerConfig.validCursor(state.token)
                                || !state.seen
                                        .get(state.seen.size() - 1)
                                        .equals(BigMailerJson.digestText(state.token))))) {
            throw BigMailerConfig.invalid("invalid paginated checkpoint history");
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void cancel() {
        close();
    }

    /**
     * Stops HTTPS and any retry delay independently of the checkpoint lock.
     *
     * <p>Late provider results cannot emit after cancellation.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<Page> pending = inFlight;
            if (pending != null) {
                pending.cancel(true);
            }
            worker.shutdownNow();
        }
    }

    /**
     * Maintains the progress of one ordered API collection.
     *
     * <p>Separate instances prevent parent tokens and different brands' child tokens from mixing.
     */
    static final class Pager {
        private final Set<String> seen;
        private String token;
        private int pages;
        private int offset;
        private String digest;
        private Page page;

        Pager(PageCheckpoint state) {
            seen = new LinkedHashSet<>(state.seen);
            token = state.token;
            pages = state.pages;
            offset = state.offset;
            digest = state.digest;
        }

        void accept(Page reply, int maxPages) throws IOException {
            if (digest != null && (offset >= reply.rows.size() || !digest.equals(reply.digest))) {
                throw BigMailerJson.failure("the saved response page changed since its checkpoint");
            }
            if (reply.next != null) {
                if (seen.contains(BigMailerJson.digestText(reply.next))) {
                    throw BigMailerJson.failure("repeated continuation cursor");
                }
                if (pages + 1 >= maxPages) {
                    throw BigMailerJson.failure("listing requires more than max_pages");
                }
            }
            page = reply;
        }

        void pin() {
            digest = page.digest;
        }

        boolean advance() {
            if (page.next == null) {
                return false;
            }
            token = page.next;
            seen.add(BigMailerJson.digestText(token));
            pages++;
            offset = 0;
            digest = null;
            page = null;
            return true;
        }

        PageCheckpoint snapshot() {
            return new PageCheckpoint(token, pages, offset, digest, new ArrayList<>(seen));
        }
    }
}
