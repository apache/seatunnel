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

package org.apache.seatunnel.connectors.seatunnel.datadog;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Polling owns emission and checkpoint progress. One cancellable worker fetches immutable pages.
 */
final class DatadogReader implements SourceReader<SeaTunnelRow, DatadogSplit> {
    private static final int MAX_PAGES = 10000;
    private static final int MAX_RECORDS_PER_RESPONSE = 10000;
    private static final int ROWS_PER_POLL = 64;
    private final DatadogConfig config;
    private final Context context;
    private final Supplier<PageClient> clientFactory;
    private final Object stateLock = new Object();
    private final Object lifecycleLock = new Object();
    private volatile boolean closed;
    private PageClient http;
    private ExecutorService worker;

    // Split and page ownership is protected independently of the I/O lifecycle.
    private DatadogSplit split;
    private Future<Page> request;
    private long requestDeadline;
    private Page page;
    private boolean noMoreSplits;
    private boolean signaled;

    DatadogReader(DatadogConfig config, Context context) {
        this(config, context, () -> new DatadogHttp(config));
    }

    DatadogReader(DatadogConfig config, Context context, Supplier<PageClient> clientFactory) {
        this.config = config;
        this.context = context;
        this.clientFactory = clientFactory;
    }

    @Override
    public void open() {
        synchronized (lifecycleLock) {
            if (!closed && http == null) {
                http = clientFactory.get();
                worker =
                        Executors.newSingleThreadExecutor(
                                task -> DatadogHttp.daemon(task, "datadog-http"));
            }
        }
    }

    /**
     * Waits briefly for a page, then yields so the engine can take a checkpoint or stop the job
     * while the network is blocked. Cursor advancement and collection share the engine lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                if (closed || signaled) {
                    return;
                }
                if (split == null ? noMoreSplits : split.complete) {
                    signaled = true;
                } else {
                    if (split != null && (page != null || acquirePage())) {
                        emit(output);
                    }
                    return;
                }
            }
        }
        if (!closed) {
            context.signalNoMoreElement();
        }
    }

    private boolean acquirePage() throws Exception {
        if (request == null) {
            if (split.page >= MAX_PAGES) {
                throw config.resource.invalid("page limit exceeded");
            }
            DatadogSplit view = split.requestView();
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.deadline);
            synchronized (lifecycleLock) {
                if (closed) {
                    return false;
                }
                requestDeadline = deadline;
                request = worker.submit(() -> preparePage(view, deadline));
            }
        }
        try {
            page = request.get(5, TimeUnit.MILLISECONDS);
            request = null;
        } catch (TimeoutException e) {
            if (requestDeadline - System.nanoTime() <= 0) {
                cancel();
                throw config.resource.invalid("page deadline exceeded");
            }
            return false;
        } catch (CancellationException e) {
            if (closed) {
                return false;
            }
            throw config.resource.invalid("request canceled");
        } catch (ExecutionException e) {
            if (closed) {
                return false;
            }
            if (e.getCause() instanceof DatadogResource.ProviderFailure) {
                throw (DatadogResource.ProviderFailure) e.getCause();
            }
            throw config.resource.invalid("request could not be completed");
        }
        if (requestDeadline - System.nanoTime() <= 0) {
            throw config.resource.invalid("page deadline exceeded");
        }
        if (page.cursorId != null && split.seen.contains(page.cursorId)) {
            throw config.resource.invalid("pagination cursor cycle");
        }
        return true;
    }

    private Page preparePage(DatadogSplit view, long deadline) throws InterruptedException {
        DatadogResource resource = config.resource;
        JsonNode response = http.fetch(resource.target(config, view), deadline);
        JsonNode records = resource.records(response);
        if (records.size() > MAX_RECORDS_PER_RESPONSE) {
            throw resource.invalid("result row limit exceeded");
        }
        DatadogResource.Next next = resource.next(config, view, response, records);
        List<SeaTunnelRow> rows = new ArrayList<>(records.size());
        byte[] prefix = new byte[0];
        for (JsonNode item : records) {
            checkPreparationDeadline(deadline);
            String id = resource.id(item);
            String time = resource.timestamp(item);
            if (resource.inWindow(time, config, view)) {
                String raw = item.toString();
                SeaTunnelRow row = new SeaTunnelRow(new Object[] {resource.label, id, time, raw});
                row.setTableId(TablePath.DEFAULT.getFullName());
                rows.add(row);
                if (rows.size() <= view.prefix) {
                    prefix = DatadogConfig.digest(prefix, raw);
                }
            }
        }
        checkPreparationDeadline(deadline);
        if (rows.size() < view.prefix || !MessageDigest.isEqual(prefix, view.prefixDigest)) {
            throw resource.invalid("provider changed the checkpointed page prefix");
        }
        return new Page(rows, next);
    }

    private void checkPreparationDeadline(long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Datadog reader closed");
        }
        if (deadline - System.nanoTime() <= 0) {
            throw config.resource.invalid("page deadline exceeded");
        }
    }

    private void emit(Collector<SeaTunnelRow> output) {
        int end = Math.min(page.rows.size(), split.prefix + ROWS_PER_POLL);
        while (!closed && split.prefix < end) {
            SeaTunnelRow row = page.rows.get(split.prefix);
            byte[] digest = DatadogConfig.digest(split.prefixDigest, (String) row.getField(3));
            output.collect(row);
            split.prefixDigest = digest;
            split.prefix++;
        }
        if (!closed && split.prefix == page.rows.size()) {
            finishPage();
        }
    }

    /** A completed page and its successor become visible as one checkpoint state transition. */
    private void finishPage() {
        split.prefix = 0;
        split.prefixDigest = new byte[0];
        if (page.next.done) {
            split.from = split.until(config.sliceDays);
            split.complete = split.from == null || split.from.equals(split.end);
            split.cursor = null;
            split.position = 0;
            split.page = 0;
            split.seen.clear();
        } else {
            split.cursor = page.next.cursor;
            split.position = page.next.position;
            if (page.cursorId != null) {
                split.seen.add(page.cursorId);
            }
            split.page++;
        }
        page = null;
    }

    @Override
    public List<DatadogSplit> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return split == null
                    ? Collections.emptyList()
                    : Collections.singletonList(split.copy());
        }
    }

    /** Only one active scan may own rows. */
    @Override
    public void addSplits(List<DatadogSplit> splits) {
        if (splits.size() != 1) {
            throw config.resource.invalid("invalid split assignment");
        }
        DatadogSplit incoming = splits.get(0);
        config.verify(incoming);
        synchronized (stateLock) {
            if (split != null) {
                throw config.resource.invalid("received a duplicate active split");
            }
            split = incoming.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (stateLock) {
            noMoreSplits = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /** Interrupts retry waits and aborts network reads without acquiring the checkpoint lock. */
    @Override
    public void cancel() {
        synchronized (lifecycleLock) {
            closed = true;
            if (http != null) {
                http.close();
                worker.shutdownNow();
            }
        }
    }

    @Override
    public void close() {
        cancel();
    }

    /** The worker owns the page client; cancel may close it independently from an engine thread. */
    interface PageClient extends AutoCloseable {
        JsonNode fetch(String target, long expires) throws InterruptedException;

        @Override
        void close();
    }

    private static final class Page {
        private final List<SeaTunnelRow> rows;
        private final DatadogResource.Next next;
        private final String cursorId;

        private Page(List<SeaTunnelRow> rows, DatadogResource.Next next) {
            this.rows = rows;
            this.next = next;
            cursorId =
                    next.cursor == null
                            ? null
                            : Base64.getEncoder().encodeToString(DatadogConfig.digest(next.cursor));
        }
    }
}
