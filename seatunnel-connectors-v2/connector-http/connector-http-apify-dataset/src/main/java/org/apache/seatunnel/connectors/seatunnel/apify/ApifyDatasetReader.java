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

package org.apache.seatunnel.connectors.seatunnel.apify;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.apify.ApifyDatasetConfig.Resource;
import org.apache.seatunnel.connectors.seatunnel.apify.ApifyDatasetSource.Cursor;

import java.io.IOException;
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
 * Keeps HTTPS work off the engine thread and publishes scanned progress with collected rows. A
 * checkpoint can proceed while the provider is servicing a page request or retry.
 */
final class ApifyDatasetReader implements SourceReader<SeaTunnelRow, Cursor> {
    private static final int ROWS_PER_POLL = 64;

    private final ApifyDatasetConfig config;
    private final Context context;
    private final String tablePath;
    private final ApifyHttp http;
    private final AtomicReference<Cursor> position = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-apify-dataset-https");
                        thread.setDaemon(true);
                        return thread;
                    });

    private volatile Future<ApifyPages.Page> pending;
    private long deadline;
    private ApifyPages.Page page;
    private boolean eof;

    ApifyDatasetReader(ApifyDatasetConfig config, Context context, String tablePath) {
        this.config = config;
        this.context = context;
        this.tablePath = tablePath;
        this.http = new ApifyHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Emits a bounded number of rows while holding the engine checkpoint lock. The cursor advances
     * through the raw range only after the complete page has been emitted.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            Cursor cursor = position.get();
            if (closed.get() || eof || cursor == null) {
                return;
            }
            verifyCursor(cursor);
            if (cursor.resourceIndex == config.resources.size()) {
                eof = true;
                context.signalNoMoreElement();
                return;
            }
            Resource resource = config.resources.get(cursor.resourceIndex);
            if (page == null && !prepare(cursor, resource)) {
                return;
            }
            emit(cursor, resource, output);
        }
    }

    private void verifyCursor(Cursor cursor) {
        if (!config.fingerprint.equals(cursor.fingerprint)) {
            throw ApifyDatasetConfig.invalid("query configuration changed during restore");
        }
        if (cursor.resourceIndex < 0
                || cursor.resourceIndex > config.resources.size()
                || cursor.offset < 0
                || cursor.end < -1
                || cursor.completedPages < 0
                || cursor.rowOffset < 0
                || (cursor.end < 0
                        && (cursor.offset != 0
                                || cursor.completedPages != 0
                                || cursor.rowOffset != 0))
                || (cursor.end >= 0 && cursor.offset >= cursor.end)
                || ((cursor.rowOffset == 0) != (cursor.pageDigest == null))
                || (cursor.resourceIndex == config.resources.size() && cursor.end >= 0)) {
            throw ApifyDatasetConfig.invalid("invalid scan checkpoint");
        }
    }

    private boolean prepare(Cursor cursor, Resource resource) throws IOException {
        Future<ApifyPages.Page> retrieval = pending;
        if (retrieval == null) {
            if (cursor.completedPages >= config.maxPages) {
                throw ApifyHttp.failure(resource, cursor.offset, "max_pages exceeded");
            }
            int limit =
                    cursor.end < 0
                            ? config.pageSize
                            : (int) Math.min(config.pageSize, cursor.end - cursor.offset);
            long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.requestTimeout);
            deadline = expires;
            try {
                pending =
                        worker.submit(
                                () ->
                                        http.fetch(
                                                resource,
                                                cursor.offset,
                                                limit,
                                                cursor.end,
                                                expires));
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw ApifyHttp.failure(
                            resource, cursor.offset, "retrieval worker is unavailable");
                }
            }
            return false;
        }
        if (!retrieval.isDone()) {
            if (System.nanoTime() - deadline >= 0) {
                close();
                throw ApifyHttp.failure(resource, cursor.offset, "request deadline exceeded");
            }
            return false;
        }
        try {
            ApifyPages.Page fetched = retrieval.get();
            if (closed.get()) {
                return false;
            }
            verifyRestoredPage(cursor, resource, fetched);
            page = fetched;
            pending = null;
            return true;
        } catch (CancellationException e) {
            if (!closed.get()) {
                throw ApifyHttp.failure(resource, cursor.offset, "request canceled");
            }
            return false;
        } catch (ExecutionException e) {
            if (closed.get()) {
                return false;
            }
            if (e.getCause() instanceof ApifyHttp.PageFailure) {
                throw (ApifyHttp.PageFailure) e.getCause();
            }
            throw ApifyHttp.failure(resource, cursor.offset, "page retrieval failed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closed.get()) {
                throw ApifyHttp.failure(resource, cursor.offset, "request interrupted");
            }
            return false;
        }
    }

    /**
     * A partially emitted page must be identical before its saved logical offset can be applied.
     * The transport already verifies the raw start, consumed scan range and fixed first-page end.
     */
    static void verifyRestoredPage(Cursor cursor, Resource resource, ApifyPages.Page fetched)
            throws IOException {
        if (cursor.rowOffset > 0
                && (cursor.rowOffset >= fetched.rows.size()
                        || !fetched.digest.equals(cursor.pageDigest))) {
            throw ApifyHttp.failure(resource, cursor.offset, "page changed since the checkpoint");
        }
    }

    private void emit(Cursor cursor, Resource resource, Collector<SeaTunnelRow> output) {
        int end = Math.min(page.rows.size(), cursor.rowOffset + ROWS_PER_POLL);
        for (int i = cursor.rowOffset; i < end; i++) {
            if (closed.get()) {
                return;
            }
            ApifyPages.Row row = page.rows.get(i);
            SeaTunnelRow value =
                    new SeaTunnelRow(new Object[] {resource.key(), row.datasetId, row.payload});
            value.setTableId(tablePath);
            output.collect(value);
        }
        if (end < page.rows.size()) {
            position.set(
                    new Cursor(
                            config.fingerprint,
                            cursor.resourceIndex,
                            cursor.completedPages,
                            cursor.offset,
                            page.end,
                            end,
                            page.digest));
        } else {
            position.set(
                    page.nextOffset >= page.end
                            ? Cursor.start(config.fingerprint, cursor.resourceIndex + 1)
                            : new Cursor(
                                    config.fingerprint,
                                    cursor.resourceIndex,
                                    cursor.completedPages + 1,
                                    page.nextOffset,
                                    page.end,
                                    0,
                                    null));
            page = null;
        }
    }

    @Override
    public List<Cursor> snapshotState(long checkpointId) {
        Cursor cursor = position.get();
        return cursor == null ? Collections.emptyList() : Collections.singletonList(cursor);
    }

    @Override
    public void addSplits(List<Cursor> splits) {
        if (splits.size() != 1 || !position.compareAndSet(null, splits.get(0))) {
            throw ApifyDatasetConfig.invalid("expected one assigned scan");
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    /**
     * Cancels active HTTPS I/O and retry delays without holding the engine checkpoint lock. The
     * same cleanup runs at normal close and on a request deadline.
     */
    @Override
    public void cancel() {
        close();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<?> request = pending;
            if (request != null) {
                request.cancel(true);
            }
            worker.shutdownNow();
        }
    }
}
