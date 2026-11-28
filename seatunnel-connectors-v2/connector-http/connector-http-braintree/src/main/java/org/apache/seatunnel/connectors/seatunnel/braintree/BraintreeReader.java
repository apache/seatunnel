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
package org.apache.seatunnel.connectors.seatunnel.braintree;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeGateway.IdSet;
import org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeGateway.Page;
import org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeGateway.Row;
import org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeResource.Mode;
import org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSplit.Window;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads the seven Braintree catalog resources with restartable pages and ID batches.
 *
 * <p>Blocking HTTPS work belongs to a single cancellable worker. Progress and accepted rows move
 * together on the poll thread under the checkpoint lock.
 */
final class BraintreeReader implements SourceReader<SeaTunnelRow, BraintreeSplit> {
    private final BraintreeConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();

    private volatile boolean closed;
    private boolean idle;
    private boolean signaled;
    private BraintreeSplit progress;
    private Page page;

    private BraintreeHttp http;
    private BraintreeGateway gateway;
    private ExecutorService executor;
    private Future<?> pending;
    private long expires;

    BraintreeReader(BraintreeConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Prepares the network worker without contacting the provider.
     *
     * <p>Unassigned or terminal readers never initiate any requests.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new BraintreeHttp(config);
                gateway = new BraintreeGateway(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "braintree-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Advances one logical search operation, collection page or accepted row.
     *
     * <p>The scan position moves only after a complete provider response is available.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || signaled) {
                return;
            }
            if (progress == null ? idle : progress.finished(config)) {
                signaled = true;
                context.signalNoMoreElement();
                return;
            }
            if (progress == null) {
                return;
            }
        }
        BraintreeResource resource = config.resources.get(progress.resourceIndex);
        if (resource.dated() && progress.window == null) {
            commit(output, () -> nextWindow(resource));
        } else if (resource.mode == Mode.IDS && progress.ids == null) {
            search(output, resource);
        } else if (page == null) {
            fetch(output, resource);
        } else if (progress.row == page.rows.size()) {
            commit(output, () -> finishPage(resource));
        } else {
            Row row = page.rows.get(progress.row);
            boolean selected = selected(resource, row);
            commit(
                    output,
                    () -> {
                        if (selected) {
                            emit(output, resource, row);
                        }
                        ++progress.row;
                    });
        }
    }

    private void nextWindow(BraintreeResource resource) {
        if (!progress.subdivisions.isEmpty()) {
            useWindow(resource, progress.subdivisions.removeFirst());
            return;
        }
        boolean daily = resource.daily();
        if (progress.nextWindow == Long.MIN_VALUE) {
            progress.nextWindow = Window.unit(progress.start, daily);
        }
        long upper = Window.unit(progress.end, daily);
        if (progress.nextWindow > upper) {
            progress.nextResource();
            return;
        }
        long width = daily ? config.windowDays : config.windowDays * 1440L;
        long last = Math.min(upper, progress.nextWindow + width - 1);
        useWindow(resource, new Window(progress.nextWindow, last));
        progress.nextWindow = last + 1;
    }

    private void useWindow(BraintreeResource resource, Window window) {
        if (progress.windows == config.maxWindows) {
            throw resource.failure("search exceeds max_search_windows");
        }
        ++progress.windows;
        progress.window = window;
    }

    /**
     * Splits a saturated minute or day range before any record in it is emitted.
     *
     * <p>A single saturated native search unit cannot be divided reliably. Reporting a failure
     * prevents a truncated list of IDs from being mistaken for a complete extraction.
     */
    private void divide(BraintreeResource resource) {
        Window window = progress.window;
        if (window.first == window.last) {
            throw resource.failure(
                    resource.daily()
                            ? "search limit reached within one date"
                            : "search limit reached within one minute");
        }
        long middle = window.middle();
        progress.clearWindow();
        progress.subdivisions.addFirst(new Window(middle + 1, window.last));
        progress.subdivisions.addFirst(new Window(window.first, middle));
    }

    private void search(Collector<SeaTunnelRow> output, BraintreeResource resource)
            throws Exception {
        IdSet list =
                get(
                        resource,
                        IdSet.class,
                        deadline -> gateway.searchIds(resource, progress.window, deadline));
        if (list == null) {
            return;
        }
        commit(
                output,
                () -> {
                    if (list.ids.size() >= Math.min(config.resultLimit, resource.providerLimit)) {
                        divide(resource);
                    } else if (list.ids.isEmpty()) {
                        progress.clearWindow();
                    } else {
                        progress.ids = list.ids;
                        progress.batchSize = list.batchSize;
                    }
                });
    }

    private void fetch(Collector<SeaTunnelRow> output, BraintreeResource resource)
            throws Exception {
        if (progress.digest == null && progress.pages == config.maxPages) {
            throw resource.failure("page or batch count exceeds max_pages");
        }
        List<String> batch =
                progress.ids == null
                        ? List.of()
                        : progress.ids.subList(
                                progress.idOffset,
                                Math.min(
                                        progress.ids.size(),
                                        progress.idOffset + progress.batchSize));
        Page received =
                get(
                        resource,
                        Page.class,
                        deadline ->
                                gateway.list(
                                        resource, progress.window, progress.page, batch, deadline));
        if (received == null) {
            return;
        }
        if (received.saturated) {
            if (progress.digest != null) {
                throw resource.failure("checkpoint dispute page became saturated before replay");
            }
            commit(
                    output,
                    () -> {
                        ++progress.pages;
                        divide(resource);
                    });
            return;
        }
        progress.validatePage(resource, received.digest, received.rows.size());
        if (resource.dated()) {
            for (Row row : received.rows) {
                long actual = Window.unit(row.timestamp, resource.daily());
                if (actual < progress.window.first || actual > progress.window.last) {
                    throw resource.failure("record is outside its native search window");
                }
            }
        }
        if (resource.mode == Mode.PAGED) {
            if (progress.total >= 0
                    && (received.total != progress.total || received.size != progress.size)) {
                throw resource.failure("paged collection metadata changed during the scan");
            }
            if (progress.digest == null && progress.pageDigests.contains(received.digest)) {
                throw resource.failure("pagination repeated a previously accepted page");
            }
        }
        commit(
                output,
                () -> {
                    if (progress.digest == null) {
                        ++progress.pages;
                    }
                    progress.digest = received.digest;
                    progress.total = received.total;
                    progress.size = received.size;
                    page = received;
                });
    }

    private void finishPage(BraintreeResource resource) {
        if (resource.mode == Mode.IDS) {
            progress.idOffset =
                    Math.min(progress.ids.size(), progress.idOffset + progress.batchSize);
            if (progress.idOffset == progress.ids.size()) {
                progress.clearWindow();
            }
        } else if (resource.mode == Mode.PAGED && (long) progress.page * page.size < page.total) {
            progress.pageDigests.add(page.digest);
            ++progress.page;
        } else if (resource.dated()) {
            progress.clearWindow();
        } else {
            progress.nextResource();
        }
        progress.clearPosition();
        page = null;
    }

    private boolean selected(BraintreeResource resource, Row row) {
        if (!resource.dated()) {
            return true;
        }
        return resource.daily()
                || !row.timestamp.isBefore(progress.start) && !row.timestamp.isAfter(progress.end);
    }

    private static void emit(
            Collector<SeaTunnelRow> output, BraintreeResource resource, Row record) {
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {resource.name, record.id, record.cursor, record.json});
        row.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(row);
    }

    private <T> T get(BraintreeResource resource, Class<T> kind, Request<T> read) throws Exception {
        Future<?> task;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                expires = http.deadline();
                long deadline = expires;
                pending = executor.submit(() -> read.run(deadline));
            }
            task = pending;
        }
        try {
            T reply = kind.cast(task.get(5, TimeUnit.MILLISECONDS));
            synchronized (lifecycle) {
                pending = null;
                return closed ? null : reply;
            }
        } catch (TimeoutException ex) {
            if (!closed && expires - System.nanoTime() <= 0) {
                synchronized (lifecycle) {
                    abortWorker();
                }
                throw resource.failure("HTTPS operation deadline exceeded");
            }
            return null;
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw resource.failure("provider read was canceled");
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof BraintreeFailure) {
                throw (BraintreeFailure) ex.getCause();
            }
            throw resource.failure("provider read failed");
        }
    }

    private void commit(Collector<SeaTunnelRow> output, Runnable update) {
        synchronized (output.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (!closed) {
                    update.run();
                }
            }
        }
    }

    /**
     * Captures a detached request plan and exact emitted row position.
     *
     * <p>Access keys, raw payloads and open connections are excluded from the split.
     */
    @Override
    public List<BraintreeSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return progress == null ? List.of() : List.of(progress.copy());
        }
    }

    /**
     * Accepts one merchant split and validates scope before scheduling provider work.
     *
     * <p>Restored page or batch data is reloaded and matched to the saved digest.
     */
    @Override
    public void addSplits(List<BraintreeSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new BraintreeFailure("invalid source split assignment");
            }
            BraintreeSplit split = splits.get(0);
            split.validate(config);
            progress = split.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (checkpoint) {
            idle = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Aborts the active HTTPS operation and makes cancellation sticky before or after open.
     *
     * <p>Cancellation does not wait for the engine checkpoint lock.
     */
    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
            abortWorker();
        }
    }

    private void abortWorker() {
        if (http != null) {
            http.close();
        }
        if (pending != null) {
            pending.cancel(true);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public void close() {
        cancel();
    }

    /**
     * Represents a read operation with its shared retry and I/O deadline.
     *
     * <p>Only the worker uses this interface to reach the gateway.
     */
    @FunctionalInterface
    private interface Request<T> {
        T run(long deadline) throws InterruptedException;
    }
}
