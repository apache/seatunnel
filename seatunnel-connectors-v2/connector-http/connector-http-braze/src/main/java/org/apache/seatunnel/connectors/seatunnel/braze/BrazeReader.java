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

package org.apache.seatunnel.connectors.seatunnel.braze;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.braze.BrazeConfig.Resource;
import org.apache.seatunnel.connectors.seatunnel.braze.BrazeJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.braze.BrazeJson.Page;
import org.apache.seatunnel.connectors.seatunnel.braze.BrazeJson.Record;
import org.apache.seatunnel.connectors.seatunnel.braze.BrazeSource.Split;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
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
 * Reads a Braze list or analytics traversal using one asynchronous request at a time.
 *
 * <p>Parent and response pages are independently fingerprinted so a restored offset cannot silently
 * skip changed or reordered data in either active page.
 */
final class BrazeReader implements SourceReader<SeaTunnelRow, Split> {
    private static final int ROWS_PER_POLL = 16;
    private static final Pattern FINGERPRINT = Pattern.compile("[0-9a-f]{64}");

    private final BrazeConfig config;
    private final Context context;
    private final String table;
    private final BrazeHttp http;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-braze-retrieval");
                        thread.setDaemon(true);
                        return thread;
                    });

    private boolean assigned;
    private Instant cutoff;
    private int page;
    private String previousList;
    private String parentList;
    private LocalDate from;
    private int parentOffset;
    private int offset;
    private String records;
    private boolean finished;
    private boolean eof;
    private Page parents;
    private Page current;
    private volatile Future<Page> pending;

    BrazeReader(BrazeConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        this.http = new BrazeHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Emits completed reads and their next offsets atomically with respect to checkpointing.
     *
     * @param collector the engine collector and checkpoint lock
     * @throws IOException if a request, response or saved position cannot be consumed safely
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
                    if (config.resource.parent != null && parents == null) {
                        discover();
                    } else if (current != null || prepare()) {
                        emit(collector);
                    }
                } catch (Failure failure) {
                    if (!closed.get()) {
                        throw new IOException(
                                "Braze resource "
                                        + config.resource.id
                                        + ": "
                                        + failure.getMessage());
                    }
                }
            }
        }
    }

    private void discover() throws Failure {
        Page discovered = readList(config.resource.parent, true);
        if (discovered == null) {
            return;
        }
        validateReplay(
                parentOffset,
                parentList,
                discovered.idDigest,
                discovered.ids.size(),
                "parent page");
        if (discovered.ids.isEmpty()) {
            finish();
            return;
        }
        parents = discovered;
        parentList = discovered.idDigest;
    }

    private boolean prepare() throws Failure {
        Page acquired;
        if (config.resource.listSize > 0) {
            acquired = readList(config.resource, false);
        } else {
            String id = parents == null ? null : parents.ids.get(parentOffset);
            LocalDate windowStart = from;
            LocalDate until = from == null ? null : config.lastDay(from, cutoff);
            String target = config.recordTarget(id, windowStart, cutoff);
            acquired =
                    ready(
                            () ->
                                    BrazeJson.records(
                                            http.fetch(target),
                                            config.resource,
                                            id,
                                            windowStart,
                                            until));
        }
        if (acquired == null) {
            return false;
        }
        validateReplay(
                offset, records, acquired.rowDigest, acquired.rows.size(), "record page or window");
        current = acquired;
        return true;
    }

    private Page readList(Resource collection, boolean discovery) throws Failure {
        if (page >= config.maxPages) {
            throw BrazeJson.failure("numbered pagination reached max_pages before completion");
        }
        String target = config.listTarget(collection, page, discovery);
        Page next =
                ready(
                        () -> {
                            JsonNode response = http.fetch(target);
                            return BrazeJson.list(response, collection, discovery);
                        });
        if (next != null && !next.ids.isEmpty() && next.idDigest.equals(previousList)) {
            throw BrazeJson.failure("numbered pagination returned a repeated page");
        }
        return next;
    }

    /**
     * Submits network and decoding work without waiting under the engine checkpoint lock.
     *
     * @param fetch a single and immutable request
     * @return the decoded response after it becomes available
     */
    private Page ready(Callable<Page> fetch) throws Failure {
        Future<Page> task = pending;
        if (task == null) {
            try {
                pending = worker.submit(fetch);
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw BrazeJson.failure("retrieval worker is unavailable");
                }
            }
            return null;
        }
        if (!task.isDone()) {
            return null;
        }
        try {
            Page result = task.get();
            pending = null;
            return closed.get() ? null : result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw BrazeJson.failure("retrieval interrupted");
        } catch (CancellationException e) {
            throw BrazeJson.failure("retrieval canceled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Failure) {
                throw (Failure) e.getCause();
            }
            throw BrazeJson.failure("retrieval worker failed");
        }
    }

    private void emit(Collector<SeaTunnelRow> collector) {
        int stop = Math.min(current.rows.size(), offset + ROWS_PER_POLL);
        while (offset < stop && !closed.get()) {
            Record value = current.rows.get(offset);
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {value.id, value.time, value.json});
            row.setTableId(table);
            collector.collect(row);
            offset++;
            records = current.rowDigest;
        }
        if (offset == current.rows.size()) {
            Page consumed = current;
            offset = 0;
            records = null;
            current = null;
            advance(consumed);
        }
    }

    /**
     * Advances a completed page, window or parent and leaves the next resumable coordinates.
     *
     * <p>The same parent's analytics continue until the frozen last day is consumed.
     */
    private void advance(Page consumed) {
        if (config.resource.listSize > 0) {
            advanceList(consumed);
            return;
        }
        if (from != null) {
            LocalDate until = config.lastDay(from, cutoff);
            if (until.isBefore(BrazeConfig.utcDay(cutoff))) {
                from = until.plusDays(1);
                return;
            }
        }
        if (parents == null) {
            finish();
            return;
        }
        from = config.resource.days == 0 ? null : config.start;
        parentOffset++;
        if (parentOffset == parents.ids.size()) {
            advanceList(parents);
        }
    }

    private void advanceList(Page consumed) {
        if (!consumed.more) {
            finish();
            return;
        }
        page++;
        previousList = consumed.idDigest;
        parentList = null;
        parents = null;
        parentOffset = 0;
    }

    private void finish() {
        finished = true;
        page = 0;
        previousList = null;
        parentList = null;
        from = null;
        parentOffset = 0;
        offset = 0;
        records = null;
        parents = null;
    }

    static void validateReplay(int offset, String saved, String actual, int count, String role)
            throws Failure {
        if (saved != null && (offset >= count || !saved.equals(actual))) {
            throw BrazeJson.failure("saved " + role + " changed since its checkpoint");
        }
    }

    /**
     * Captures only completed output progress, even while a GET is running.
     *
     * @param checkpointId the engine checkpoint identifier
     * @return the current assigned position or an empty list before assignment
     */
    @Override
    public List<Split> snapshotState(long checkpointId) {
        synchronized (progress) {
            if (!assigned) {
                return List.of();
            }
            return List.of(
                    new Split(
                            config.scope,
                            cutoff,
                            page,
                            previousList,
                            parentList,
                            from,
                            parentOffset,
                            offset,
                            records,
                            finished));
        }
    }

    /**
     * Accepts the only active split and restores its next unread position.
     *
     * @param splits the engine's registered traversal split
     */
    @Override
    public void addSplits(List<Split> splits) {
        synchronized (progress) {
            if (assigned || splits.size() != 1) {
                throw BrazeConfig.invalid("expected one assigned Braze traversal");
            }
            Split saved = splits.get(0);
            validate(config, saved);
            cutoff = saved.cutoff;
            page = saved.page;
            previousList = saved.previousList;
            parentList = saved.parentList;
            from = saved.from;
            parentOffset = saved.parentOffset;
            offset = saved.offset;
            records = saved.records;
            finished = saved.finished;
            assigned = true;
        }
    }

    /**
     * Checks that a checkpoint has valid coordinates for the selected resource and workspace.
     *
     * @param config the reconstructed source configuration
     * @param saved the coordinator or reader checkpoint
     */
    static void validate(BrazeConfig config, Split saved) {
        if (saved == null || !config.scope.equals(saved.scope)) {
            throw BrazeConfig.invalid("workspace, resource or date options changed during restore");
        }

        boolean series = config.resource.days > 0;
        if (series != (saved.cutoff != null)
                || (series
                        && (BrazeConfig.utcDay(saved.cutoff).isBefore(config.start)
                                || (config.end != null
                                        && !config.end.equals(
                                                BrazeConfig.utcDay(saved.cutoff)))))) {
            throw BrazeConfig.invalid("invalid frozen analytics cutoff");
        }
        if (saved.finished) {
            if (saved.page != 0
                    || saved.previousList != null
                    || saved.parentList != null
                    || saved.from != null
                    || saved.parentOffset != 0
                    || saved.offset != 0
                    || saved.records != null) {
                throw BrazeConfig.invalid("terminal traversal contains active positions");
            }
            return;
        }

        Resource list = config.resource.parent == null ? config.resource : config.resource.parent;
        boolean hasParent = config.resource.parent != null;
        int pageBound = list.listSize > 0 ? config.maxPages : 0;
        int rows = series ? config.resource.days : Math.max(1, config.resource.listSize);
        if (saved.page < 0
                || saved.page > pageBound
                || (saved.page == 0) != (saved.previousList == null)
                || saved.offset < 0
                || saved.offset >= rows
                || (saved.offset == 0) != (saved.records == null)
                || saved.parentOffset < 0
                || (hasParent && saved.parentOffset >= list.listSize)
                || (!hasParent && (saved.parentList != null || saved.parentOffset != 0))
                || (hasParent
                        && (saved.parentOffset > 0 || saved.offset > 0)
                        && saved.parentList == null)
                || !fingerprint(saved.previousList)
                || !fingerprint(saved.parentList)
                || !fingerprint(saved.records)) {
            throw BrazeConfig.invalid("invalid saved page, parent or record position");
        }
        if (series != (saved.from != null)
                || (series
                        && (saved.from.isBefore(config.start)
                                || saved.from.isAfter(BrazeConfig.utcDay(saved.cutoff))
                                || ChronoUnit.DAYS.between(config.start, saved.from)
                                                % config.resource.days
                                        != 0))) {
            throw BrazeConfig.invalid("invalid saved analytics window position");
        }
    }

    private static boolean fingerprint(String value) {
        return value == null || FINGERPRINT.matcher(value).matches();
    }

    @Override
    public void handleNoMoreSplits() {
        // The bounded scan signals completion after its final response is consumed.
    }

    /**
     * Cancels I/O and shuts down the worker without waiting for an engine checkpoint lock.
     *
     * <p>The reader can close while the provider is sending headers or response data.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<Page> task = pending;
            if (task != null) {
                task.cancel(true);
            }
            worker.shutdownNow();
        }
    }
}
