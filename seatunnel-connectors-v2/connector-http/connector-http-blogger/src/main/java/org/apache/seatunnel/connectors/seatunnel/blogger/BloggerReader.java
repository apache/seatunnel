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

package org.apache.seatunnel.connectors.seatunnel.blogger;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerConfig.Stage;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerJson.Page;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerJson.Row;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSource.Cursor;
import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerSource.PageCheckpoint;

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
 * Executes one Blogger resource as a depth-first traversal with at most three loaded pages.
 *
 * <p>Every child request inherits the owning blog and post from its active parent row, while
 * asynchronous HTTPS keeps the checkpoint lock free of network requests.
 */
final class BloggerReader implements SourceReader<SeaTunnelRow, Cursor> {
    private static final int ROWS_PER_POLL = 16;
    private static final Pattern SHA256 = Pattern.compile("[a-f0-9]{64}");
    private final BloggerConfig config;
    private final Context context;
    private final BloggerHttp http;
    private final String table;
    private final List<Stage> stages;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-blogger-retrieval");
                        thread.setDaemon(true);
                        return thread;
                    });

    private boolean assigned;
    private Pager[] positions;
    private int depth;
    private boolean finished;
    private boolean eof;
    private volatile Future<Page> inFlight;

    BloggerReader(BloggerConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        this.stages = config.resource.stages;
        this.http = new BloggerHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Publishes only ready rows, then records their cursor under the same checkpoint lock.
     *
     * @param collector engine output and checkpoint lock
     * @throws IOException when no safe continuation or response is available
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws IOException {
        synchronized (collector.getCheckpointLock()) {
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
                    pollStage(collector);
                } catch (Failure e) {
                    throw new IOException(
                            "Blogger resource " + config.resource.id + ": " + e.getMessage());
                }
            }
        }
    }

    private void pollStage(Collector<SeaTunnelRow> collector) throws IOException {
        Pager pager = positions[depth];
        boolean leaf = depth == stages.size() - 1;
        if (!ready(pager, stages.get(depth), !leaf)) {
            return;
        }
        if (pager.offset == pager.page.rows.size()) {
            advanceCompletedPage();
            return;
        }
        if (!leaf) {
            pager.pin();
            depth++;
            return;
        }
        int stop = Math.min(pager.page.rows.size(), pager.offset + ROWS_PER_POLL);
        while (pager.offset < stop && !closed.get()) {
            Row value = pager.page.rows.get(pager.offset);
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {value.blog, value.post, value.json});
            row.setTableId(table);
            collector.collect(row);
            pager.offset++;
            pager.pin();
        }
        if (pager.offset == pager.page.rows.size()) {
            advanceCompletedPage();
        }
    }

    /**
     * Advances the current continuation or finishes its collection and returns to the exact parent.
     *
     * <p>Fully consumed parent pages are also advanced here, so a snapshot cannot keep an offset at
     * the end of an already completed page.
     */
    private void advanceCompletedPage() {
        Pager pager = positions[depth];
        while (!pager.advance()) {
            if (depth == 0) {
                positions = null;
                finished = true;
                return;
            }
            positions[depth] = new Pager(PageCheckpoint.first());
            pager = positions[--depth];
            pager.offset++;
            if (pager.offset < pager.page.rows.size()) {
                return;
            }
        }
    }

    /**
     * Submits an immutable request plan or installs its finished result.
     *
     * <p>A restore rebuilds ancestor pages starting at depth zero, validating each replay before
     * using its saved child state.
     */
    private boolean ready(Pager pager, Stage stage, boolean discovery) throws IOException {
        if (pager.page != null) {
            return true;
        }
        Future<Page> task = inFlight;
        if (task == null) {
            Row parent = depth == 0 ? null : positions[depth - 1].current();
            String blog = parent == null ? null : parent.blog;
            String post = parent == null ? null : parent.post;
            String target = config.target(stage, blog, post, pager.token, discovery);
            try {
                inFlight =
                        worker.submit(
                                () -> {
                                    JsonNode response = http.fetch(target);
                                    return BloggerJson.page(
                                            response,
                                            stage,
                                            config.maximumRows(stage),
                                            blog,
                                            post,
                                            discovery);
                                });
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw BloggerJson.failure("retrieval worker is unavailable");
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
                throw BloggerJson.failure("retrieval canceled");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closed.get()) {
                throw BloggerJson.failure("retrieval interrupted");
            }
        } catch (ExecutionException e) {
            if (!closed.get()) {
                if (e.getCause() instanceof Failure) {
                    throw (Failure) e.getCause();
                }
                throw BloggerJson.failure("retrieval worker failed");
            }
        }
        return false;
    }

    /**
     * Captures bounded positions and digests at each traversal depth.
     *
     * @param checkpointId engine checkpoint identifier
     * @return empty before assignment, otherwise the active or terminal traversal split
     */
    @Override
    public List<Cursor> snapshotState(long checkpointId) {
        synchronized (progress) {
            if (!assigned) {
                return List.of();
            }
            List<PageCheckpoint> saved = new ArrayList<>();
            if (positions != null) {
                for (Pager pager : positions) {
                    saved.add(pager.snapshot());
                }
            }
            return List.of(new Cursor(config.scope, saved, finished));
        }
    }

    /**
     * Accepts the sole resource split and checks that each active child has a pinned parent page.
     *
     * @param splits the split assigned by the engine
     */
    @Override
    public void addSplits(List<Cursor> splits) {
        synchronized (progress) {
            if (assigned || splits.size() != 1) {
                throw BloggerConfig.invalid("expected one assigned Blogger traversal");
            }
            Cursor cursor = splits.get(0);
            validate(config, cursor);
            assigned = true;
            finished = cursor.finished;
            if (!finished) {
                positions = cursor.positions.stream().map(Pager::new).toArray(Pager[]::new);
            }
        }
    }

    static void validate(BloggerConfig config, Cursor cursor) {
        if (cursor == null || !config.scope.equals(cursor.scope)) {
            throw BloggerConfig.invalid(
                    "OAuth principal, resource or page_size changed during restore");
        }
        if (cursor.finished) {
            if (!cursor.positions.isEmpty()) {
                throw BloggerConfig.invalid("terminal traversal contains active positions");
            }
            return;
        }
        List<Stage> stages = config.resource.stages;
        if (cursor.positions.size() != stages.size()) {
            throw BloggerConfig.invalid("invalid checkpoint traversal depth");
        }
        for (int i = 0; i < stages.size(); i++) {
            Stage stage = stages.get(i);
            PageCheckpoint saved = cursor.positions.get(i);
            verifyPosition(config, stage, saved, i + 1 < stages.size());
            if (i > 0
                    && cursor.positions.get(i - 1).digest == null
                    && (saved.pages != 0 || saved.offset != 0 || saved.digest != null)) {
                throw BloggerConfig.invalid("a saved child cursor lacks its pinned parent");
            }
        }
    }

    private static void verifyPosition(
            BloggerConfig config, Stage stage, PageCheckpoint state, boolean parent) {
        if (state.pages < 0
                || state.pages >= config.maxPages
                || (!stage.paginated && state.pages != 0)
                || state.offset < 0
                || state.offset >= config.maximumRows(stage)
                || (state.token == null) != (state.pages == 0)
                || state.seen.size() != state.pages
                || (state.offset > 0 && state.digest == null)
                || (state.digest != null
                        && (!SHA256.matcher(state.digest).matches()
                                || (!parent && state.offset == 0)))) {
            throw BloggerConfig.invalid("invalid paginated checkpoint position");
        }
        Set<String> unique = new HashSet<>(state.seen);
        if (unique.size() != state.seen.size()
                || unique.stream().anyMatch(hash -> !SHA256.matcher(hash).matches())
                || (state.token != null
                        && (!BloggerConfig.validPageToken(state.token)
                                || !state.seen
                                        .get(state.pages - 1)
                                        .equals(BloggerJson.digest(state.token))))) {
            throw BloggerConfig.invalid("invalid paginated checkpoint history");
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void cancel() {
        close();
    }

    /**
     * Aborts both OAuth or data HTTPS and the retrieval worker.
     *
     * <p>Cancellation does not acquire the checkpoint lock, including before initial assignment.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<Page> running = inFlight;
            if (running != null) {
                running.cancel(true);
            }
            worker.shutdownNow();
        }
    }

    /**
     * Maintains the progress of one ordered API collection.
     *
     * <p>Independent pagers prevent cursors crossing blogs, posts, or nested API operations.
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
                throw BloggerJson.failure("the saved response page changed since its checkpoint");
            }
            if (reply.next != null) {
                if (seen.contains(BloggerJson.digest(reply.next))) {
                    throw BloggerJson.failure("repeated continuation cursor");
                }
                if (pages + 1 >= maxPages) {
                    throw BloggerJson.failure("listing requires more than max_pages");
                }
            }
            page = reply;
        }

        Row current() {
            return page.rows.get(offset);
        }

        void pin() {
            digest = page.digest;
        }

        boolean advance() {
            if (page.next == null) {
                return false;
            }
            token = page.next;
            seen.add(BloggerJson.digest(token));
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
