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

package org.apache.seatunnel.connectors.seatunnel.boldsign;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignConfig.Resource;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignConfig.Window;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignJson.RawPage;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignPagination.Cursor;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignPagination.DocumentState;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignPagination.Page;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignSource.Split;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Reads the selected list with one outstanding GET and checkpointed page progress.
 *
 * <p>Custom fields use one bounded brand discovery and one child list per brand. HTTPS never runs
 * under the engine checkpoint lock.
 */
final class BoldSignReader implements SourceReader<SeaTunnelRow, Split> {
    private static final int ROWS_PER_POLL = 16;
    private static final Pattern FINGERPRINT = Pattern.compile("[0-9a-f]{64}");

    private final BoldSignConfig config;
    private final Context context;
    private final String table;
    private final BoldSignHttp http;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-boldsign-retrieval");
                        thread.setDaemon(true);
                        return thread;
                    });

    private Window window;
    private int brandOffset;
    private String brandDigest;
    private Cursor position;
    private int offset;
    private String digest;
    private boolean finished;
    private boolean eof;
    private List<String> brands;
    private Page current;
    private volatile Future<Result> pending;

    BoldSignReader(BoldSignConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        this.http = new BoldSignHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Publishes only finished reads and updates emitted progress under the same lock.
     *
     * @param collector output collector and checkpoint lock
     * @throws IOException when a provider response or saved replay cannot continue
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws IOException {
        synchronized (collector.getCheckpointLock()) {
            synchronized (progress) {
                if (closed.get() || window == null || eof) {
                    return;
                }
                if (finished) {
                    eof = true;
                    context.signalNoMoreElement();
                    return;
                }
                try {
                    if (config.resource == Resource.CUSTOM_FIELDS && brands == null) {
                        discover();
                    } else {
                        String brand = brands == null ? null : brands.get(brandOffset);
                        if (current != null || prepare(brand)) {
                            emit(collector, brand);
                        }
                    }
                } catch (Failure e) {
                    throw new IOException(
                            "BoldSign resource " + config.resource.id + ": " + e.getMessage());
                }
            }
        }
    }

    /**
     * Loads the provider's actual unpaginated brand list before reading any custom fields.
     *
     * <p>Restore verifies brand identity and order even at offset zero if a child had been started.
     */
    private void discover() throws IOException {
        Result result = ready(Resource.BRANDS, null, Cursor.first(Resource.BRANDS), true);
        if (result == null) {
            return;
        }
        verifyReplay(
                brandOffset,
                brandDigest,
                result.brandDigest,
                result.brands.size(),
                "brand inventory");
        if (result.brands.isEmpty()) {
            finish();
            return;
        }
        brands = result.brands;
        brandDigest = result.brandDigest;
    }

    private boolean prepare(String brand) throws IOException {
        Result result = ready(config.resource, brand, position, false);
        if (result == null) {
            return false;
        }
        verifyReplay(offset, digest, result.page.digest, result.page.output.size(), "data page");
        current = result.page;
        return true;
    }

    static void verifyReplay(int offset, String saved, String actual, int count, String reason)
            throws IOException {
        if (saved != null && (offset >= count || !saved.equals(actual))) {
            throw BoldSignJson.failure("the saved " + reason + " changed since its checkpoint");
        }
    }

    private Result ready(Resource resource, String brand, Cursor request, boolean discover)
            throws IOException {
        Future<Result> task = pending;
        if (task == null) {
            Window frozen = window;
            String target = config.target(resource, brand, request.page, request.anchor());
            try {
                pending =
                        worker.submit(
                                () -> {
                                    JsonNode response = http.fetch(target);
                                    RawPage raw =
                                            BoldSignJson.page(
                                                    response,
                                                    resource,
                                                    request.page,
                                                    config.pageSize,
                                                    config.maxUnpaged);
                                    if (discover) {
                                        List<String> ids = BoldSignJson.brands(raw.rows);
                                        return new Result(
                                                ids, BoldSignJson.fingerprint(ids, "brands"), null);
                                    }
                                    return new Result(
                                            null,
                                            null,
                                            BoldSignPagination.prepare(
                                                    config, resource, frozen, brand, request, raw));
                                });
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw BoldSignJson.failure("retrieval worker is unavailable");
                }
            }
            return null;
        }
        if (!task.isDone()) {
            return null;
        }
        try {
            Result result = task.get();
            pending = null;
            return closed.get() ? null : result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closed.get()) {
                throw BoldSignJson.failure("retrieval interrupted");
            }
        } catch (CancellationException e) {
            if (!closed.get()) {
                throw BoldSignJson.failure("retrieval canceled");
            }
        } catch (ExecutionException e) {
            if (!closed.get()) {
                if (e.getCause() instanceof Failure) {
                    throw (Failure) e.getCause();
                }
                throw BoldSignJson.failure("retrieval worker failed");
            }
        }
        return null;
    }

    private void emit(Collector<SeaTunnelRow> collector, String brand) {
        int stop = Math.min(current.output.size(), offset + ROWS_PER_POLL);
        while (offset < stop && !closed.get()) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {brand, current.output.get(offset)});
            row.setTableId(table);
            collector.collect(row);
            offset++;
            digest = current.digest;
        }
        if (offset == current.output.size()) {
            position = current.next;
            offset = 0;
            digest = null;
            current = null;
            if (position == null) {
                if (brands != null && ++brandOffset < brands.size()) {
                    position = Cursor.first(config.resource);
                } else {
                    finish();
                }
            }
        }
    }

    private void finish() {
        position = null;
        brands = null;
        brandOffset = 0;
        brandDigest = null;
        offset = 0;
        digest = null;
        finished = true;
    }

    /**
     * Saves positions, frozen bounds and fingerprints independently of any in-flight request.
     *
     * @param checkpointId the engine snapshot identifier
     * @return an empty list before assignment, otherwise the current position
     */
    @Override
    public List<Split> snapshotState(long checkpointId) {
        synchronized (progress) {
            if (window == null) {
                return List.of();
            }
            return List.of(
                    new Split(
                            config.scope,
                            window,
                            brandOffset,
                            brandDigest,
                            position,
                            offset,
                            digest,
                            finished));
        }
    }

    /**
     * Confirms checkpoint scope and restores exactly one assigned scan.
     *
     * @param splits the scan assigned by the engine coordinator
     */
    @Override
    public void addSplits(List<Split> splits) {
        synchronized (progress) {
            if (window != null || splits.size() != 1) {
                throw BoldSignConfig.invalid("expected one assigned BoldSign traversal");
            }
            Split saved = splits.get(0);
            validate(config, saved);
            window = saved.window;
            brandOffset = saved.brandOffset;
            brandDigest = saved.brandDigest;
            position = saved.position;
            offset = saved.offset;
            digest = saved.digest;
            finished = saved.finished;
        }
    }

    static void validate(BoldSignConfig config, Split saved) {
        if (saved == null || !config.scope.equals(saved.scope)) {
            throw BoldSignConfig.invalid(
                    "account, region, resource, date window or page_size changed during restore");
        }
        if (saved.window == null
                || (saved.window.from != null
                        && saved.window.until != null
                        && saved.window.from > saved.window.until)) {
            throw BoldSignConfig.invalid("invalid frozen creation window");
        }
        if (saved.finished) {
            if (saved.position != null
                    || saved.offset != 0
                    || saved.digest != null
                    || saved.brandDigest != null
                    || saved.brandOffset != 0) {
                throw BoldSignConfig.invalid("terminal traversal contains active positions");
            }
            return;
        }
        int maxRows = config.resource.paged ? config.pageSize : config.maxUnpaged;
        if (saved.offset < 0
                || saved.offset >= maxRows
                || (saved.offset == 0) != (saved.digest == null)
                || (saved.digest != null && !FINGERPRINT.matcher(saved.digest).matches())
                || saved.brandOffset < 0
                || saved.brandOffset >= config.maxUnpaged
                || (config.resource != Resource.CUSTOM_FIELDS
                        && (saved.brandOffset != 0 || saved.brandDigest != null))
                || (config.resource == Resource.CUSTOM_FIELDS
                        && saved.brandDigest == null
                        && (saved.brandOffset != 0 || saved.offset != 0))
                || (saved.brandDigest != null
                        && !FINGERPRINT.matcher(saved.brandDigest).matches())) {
            throw BoldSignConfig.invalid("invalid checkpoint row or brand position");
        }
        Cursor p = saved.position;
        if (p == null
                || p.page < 1
                || p.completed < 0
                || p.completed >= config.maxPages
                || p.page > p.completed + 1
                || (config.resource == Resource.DOCUMENTS) != (p.documents != null)
                || (!config.resource.paged && (p.page != 1 || p.completed != 0))) {
            throw BoldSignConfig.invalid("invalid checkpoint request position");
        }
        if (p.documents != null) {
            verifyDocuments(config, p);
        }
    }

    private static void verifyDocuments(BoldSignConfig config, Cursor cursor) {
        DocumentState d = cursor.documents;
        if ((long) cursor.page * config.pageSize > BoldSignConfig.DOCUMENT_WINDOW_LIMIT
                || (d.anchor == null) != d.seenAnchors.isEmpty()
                || d.seenAnchors.size() > cursor.completed
                || (d.anchor != null
                        && !d.seenAnchors.get(d.seenAnchors.size() - 1).equals(d.anchor))
                || (d.current == null) != d.tail.isEmpty()
                || (d.overlapCursor == null) != d.overlap.isEmpty()
                || (!d.overlap.isEmpty() && d.anchor == null)
                || !validHashes(d.tail)
                || !validHashes(d.overlap)
                || new HashSet<>(d.seenAnchors).size() != d.seenAnchors.size()) {
            throw BoldSignConfig.invalid("invalid document cursor rollover state");
        }
    }

    private static boolean validHashes(List<String> values) {
        return values.size() <= BoldSignConfig.DOCUMENT_WINDOW_LIMIT
                && new HashSet<>(values).size() == values.size()
                && values.stream().allMatch(value -> FINGERPRINT.matcher(value).matches());
    }

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void cancel() {
        close();
    }

    /**
     * Closes the active HTTPS socket and wakes any retry wait.
     *
     * <p>Cancellation never waits for progress or checkpoints.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<Result> task = pending;
            if (task != null) {
                task.cancel(true);
            }
            worker.shutdownNow();
        }
    }

    /**
     * An asynchronously validated brand inventory or selected output page.
     *
     * <p>Payloads remain transient and are excluded from serialized checkpoints.
     */
    private static final class Result {
        final List<String> brands;
        final String brandDigest;
        final Page page;

        Result(List<String> brands, String brandDigest, Page page) {
            this.brands = brands;
            this.brandDigest = brandDigest;
            this.page = page;
        }
    }
}
