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

package org.apache.seatunnel.connectors.seatunnel.brevo;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.brevo.BrevoJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.brevo.BrevoJson.Page;
import org.apache.seatunnel.connectors.seatunnel.brevo.BrevoJson.Record;
import org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSource.Cursor;
import org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSource.Split;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
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
 * Reads one Brevo export with bounded page and request lifecycle.
 *
 * <p>Parent discovery is independent from child selection. Checkpoint progress advances only after
 * consumed records are under the collector's checkpoint lock.
 */
final class BrevoReader implements SourceReader<SeaTunnelRow, Split> {
    private static final int ROWS_PER_POLL = 16;
    private static final Pattern FINGERPRINT = Pattern.compile("[0-9a-f]{64}");

    private final BrevoConfig config;
    private final Context context;
    private final String table;
    private final BrevoHttp http;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-brevo-retrieval");
                        thread.setDaemon(true);
                        return thread;
                    });

    private boolean assigned;
    private volatile boolean noMoreSplits;
    private Instant cutoff;
    private Cursor parent;
    private Cursor data;
    private Instant windowStart;
    private int parentIndex;
    private String parentPage;
    private int row;
    private String rowPage;
    private boolean finished;
    private boolean finishedSignalled;
    private Page parents;
    private Page current;
    private volatile Future<Page> pending;

    BrevoReader(BrevoConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        this.http = new BrevoHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Emits completed GETs and saves their next offsets without waiting for I/O.
     *
     * @param collector engine collector and checkpoint lock
     * @throws IOException when the API or a saved response position is invalid
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws IOException {
        synchronized (collector.getCheckpointLock()) {
            synchronized (progress) {
                if (closed.get() || finishedSignalled) {
                    return;
                }
                if (finished || (!assigned && noMoreSplits)) {
                    finishedSignalled = true;
                    context.signalNoMoreElement();
                    return;
                }
                if (!assigned) {
                    return;
                }
                try {
                    if (config.resource.parent != null && parents == null) {
                        discoverParents();
                    } else if (current != null || prepare()) {
                        emit(collector);
                    }
                } catch (Failure failure) {
                    if (!closed.get()) {
                        throw new IOException(
                                "Brevo resource "
                                        + config.resource.id
                                        + ": "
                                        + failure.getMessage());
                    }
                }
            }
        }
    }

    private void discoverParents() throws Failure {
        Page discovered = acquire(config.resource.parent, parent, true, null, null, null);
        if (discovered == null) {
            return;
        }
        validateReplay(
                parentIndex,
                parentPage,
                discovered.replayDigest,
                discovered.ids.size(),
                "parent page");
        if (discovered.ids.isEmpty()) {
            finish();
            return;
        }
        parents = discovered;
        parentPage = discovered.replayDigest;
    }

    private boolean prepare() throws Failure {
        Long parentId = parents == null ? null : parents.ids.get(parentIndex);
        Instant from = windowStart == null ? config.start : windowStart;
        Instant until = windowStart == null ? cutoff : config.last(windowStart, cutoff);
        Page result = acquire(config.resource, data, false, parentId, from, until);
        if (result == null) {
            return false;
        }
        validateReplay(row, rowPage, result.replayDigest, result.rows.size(), "record page");
        current = result;
        return true;
    }

    private Page acquire(
            BrevoResource resource,
            Cursor position,
            boolean discovery,
            Long parentId,
            Instant from,
            Instant until)
            throws Failure {
        if (position.completed >= config.maxPages) {
            throw BrevoJson.failure("pagination reached max_pages before completion");
        }
        int size = config.pageSize(resource);
        String target = config.target(resource, parentId, position.position, from, until);
        Page response =
                ready(
                        () ->
                                BrevoJson.page(
                                        http.fetch(target),
                                        resource,
                                        size,
                                        position.position,
                                        discovery,
                                        parentId,
                                        from,
                                        until));
        // SMTP logs have no unique event ID. Identical records can legitimately cover adjacent
        // pages.
        if (response != null
                && resource != BrevoResource.TRANSACTIONAL_EVENTS
                && response.rawSize != 0
                && response.rawDigest.equals(position.previous)) {
            throw BrevoJson.failure("pagination returned a repeated page");
        }
        return response;
    }

    /**
     * Runs network and decoding work off the engine reader's checkpoint thread.
     *
     * @param fetch a single immutable request and decoder
     * @return the decoded response once available
     */
    private Page ready(Callable<Page> fetch) throws Failure {
        Future<Page> task = pending;
        if (task == null) {
            try {
                pending = worker.submit(fetch);
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw BrevoJson.failure("retrieval worker is unavailable");
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
            throw BrevoJson.failure("retrieval interrupted");
        } catch (CancellationException e) {
            throw BrevoJson.failure("retrieval canceled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Failure) {
                throw (Failure) e.getCause();
            }
            throw BrevoJson.failure("retrieval worker failed");
        }
    }

    private void emit(Collector<SeaTunnelRow> collector) {
        int stop = Math.min(current.rows.size(), row + ROWS_PER_POLL);
        while (row < stop && !closed.get()) {
            Record value = current.rows.get(row);
            SeaTunnelRow record =
                    new SeaTunnelRow(
                            new Object[] {value.id, value.parent, value.modifiedAt, value.json});
            record.setTableId(table);
            collector.collect(record);
            row++;
            rowPage = current.replayDigest;
        }
        if (row == current.rows.size()) {
            Page consumed = current;
            current = null;
            row = 0;
            rowPage = null;
            advance(consumed);
        }
    }

    /**
     * Advances raw positions before stepping through windows and parents.
     *
     * <p>Filtered-out rows still consume their provider positions.
     */
    private void advance(Page consumed) {
        BrevoResource resource = config.resource;
        if (consumed.more) {
            data = data.advance(resource, consumed);
            return;
        }
        if (windowStart != null) {
            Instant until = config.last(windowStart, cutoff);
            if (until.isBefore(cutoff)) {
                windowStart = until.plusMillis(1);
                data = Cursor.first(resource);
                return;
            }
        }
        if (parents == null) {
            finish();
            return;
        }
        data = Cursor.first(resource);
        parentIndex++;
        if (parentIndex == parents.ids.size()) {
            if (!parents.more) {
                finish();
                return;
            }
            parent = parent.advance(resource.parent, parents);
            parents = null;
            parentIndex = 0;
            parentPage = null;
        }
    }

    private void finish() {
        finished = true;
        parents = null;
        parent = null;
        data = null;
        windowStart = null;
        parentIndex = 0;
        parentPage = null;
        row = 0;
        rowPage = null;
    }

    static void validateReplay(int offset, String saved, String actual, int available, String role)
            throws Failure {
        if (saved != null && (offset >= available || !saved.equals(actual))) {
            throw BrevoJson.failure("saved " + role + " changed since its checkpoint");
        }
    }

    /**
     * Captures the next provider position without including in-flight responses or credentials.
     *
     * @param checkpointId engine checkpoint identifier
     * @return the current source split, or an empty list before assignment
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
                            parent,
                            data,
                            windowStart,
                            parentIndex,
                            parentPage,
                            row,
                            rowPage,
                            finished));
        }
    }

    /**
     * Restores the next request and row offset when the engine assigns the only split.
     *
     * @param splits the single assigned split
     */
    @Override
    public void addSplits(List<Split> splits) {
        synchronized (progress) {
            if (assigned || finishedSignalled || splits.size() != 1) {
                throw BrevoConfig.invalid("expected one assigned Brevo traversal");
            }
            Split saved = splits.get(0);
            validate(config, saved);
            cutoff = saved.cutoff;
            parent = saved.parent;
            data = saved.data;
            windowStart = saved.windowStart;
            parentIndex = saved.parentIndex;
            parentPage = saved.parentPage;
            row = saved.row;
            rowPage = saved.rowPage;
            finished = saved.finished;
            assigned = true;
        }
    }

    /**
     * Rejects incompatible and impossible checkpoint coordinates before using them.
     *
     * @param config the current source config
     * @param saved the saved traversal
     */
    static void validate(BrevoConfig config, Split saved) {
        if (saved == null || !config.scope.equals(saved.scope)) {
            throw BrevoConfig.invalid(
                    "workspace, resource, page_size or date options changed during restore");
        }
        boolean bounded = config.start != null || config.end != null;
        if (bounded != (saved.cutoff != null)
                || (config.start != null && saved.cutoff.isBefore(config.start))
                || (config.end != null && !config.end.equals(saved.cutoff))) {
            throw BrevoConfig.invalid("invalid saved interval cutoff");
        }
        if (saved.finished) {
            if (saved.parent != null
                    || saved.data != null
                    || saved.windowStart != null
                    || saved.parentIndex != 0
                    || saved.parentPage != null
                    || saved.row != 0
                    || saved.rowPage != null) {
                throw BrevoConfig.invalid("terminal split contains active positions");
            }
            return;
        }
        BrevoResource resource = config.resource;
        int rows = config.pageSize == 0 ? BrevoConfig.MAX_UNPAGED_ROWS : config.pageSize;
        if (!validCursor(saved.data, resource, config.pageSize, config.maxPages)
                || saved.row < 0
                || saved.row >= rows
                || (saved.row == 0) != (saved.rowPage == null)
                || !digest(saved.rowPage)
                || !digest(saved.parentPage)) {
            throw BrevoConfig.invalid("invalid saved data position");
        }
        if (resource.parent == null) {
            if (saved.parent != null || saved.parentIndex != 0 || saved.parentPage != null) {
                throw BrevoConfig.invalid("saved position contains an unexpected parent");
            }
        } else if (!validCursor(
                        saved.parent, resource.parent, resource.parent.maxPageSize, config.maxPages)
                || saved.parentIndex < 0
                || saved.parentIndex >= resource.parent.maxPageSize
                || ((saved.parentIndex > 0 || saved.data.completed > 0 || saved.row > 0)
                        && saved.parentPage == null)) {
            throw BrevoConfig.invalid("invalid saved parent position");
        }
        if (resource.filter.windowed() != (saved.windowStart != null)
                || (saved.windowStart != null
                        && !validWindow(config, saved.windowStart, saved.cutoff))) {
            throw BrevoConfig.invalid("invalid saved report window");
        }
    }

    private static boolean validCursor(Cursor cursor, BrevoResource resource, int size, int cap) {
        if (cursor == null
                || cursor.completed < 0
                || cursor.completed > cap
                || (cursor.completed == 0) != (cursor.previous == null)
                || !digest(cursor.previous)) {
            return false;
        }
        if (resource == BrevoResource.COMPANIES) {
            return cursor.position == cursor.completed + 1;
        }
        return cursor.position >= cursor.completed && cursor.position <= cursor.completed * size;
    }

    private static boolean validWindow(BrevoConfig config, Instant start, Instant cutoff) {
        if (start.isBefore(config.start) || start.isAfter(cutoff)) {
            return false;
        }
        if (config.resource.filter == BrevoResource.TimeFilter.INBOUND) {
            return Duration.between(config.start, start).toMillis() % Duration.ofDays(30).toMillis()
                    == 0;
        }
        return start.equals(config.start)
                || (start.equals(start.truncatedTo(ChronoUnit.DAYS))
                        && ChronoUnit.DAYS.between(
                                                BrevoConfig.utcDate(config.start),
                                                BrevoConfig.utcDate(start))
                                        % 30
                                == 0);
    }

    private static boolean digest(String value) {
        return value == null || FINGERPRINT.matcher(value).matches();
    }

    /**
     * Marks that the enumerator has no more assignments.
     *
     * <p>An unassigned restored reader finishes from its polling thread. Calling the engine back
     * from this notification can deadlock the coordinator waiting for the notification to return.
     */
    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    /**
     * Interrupts pending I/O and decode work regardless of reader checkpoint progress.
     *
     * <p>Cancellation is sticky even if the engine calls it before assigning a split.
     */
    @Override
    public void cancel() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<Page> task = pending;
            if (task != null) {
                task.cancel(true);
            }
            worker.shutdownNow();
        }
    }

    /**
     * Shuts down the same resources as cancellation.
     *
     * <p>Closing the source after cancellation remains safe.
     */
    @Override
    public void close() {
        cancel();
    }
}
