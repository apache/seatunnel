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
package org.apache.seatunnel.connectors.seatunnel.avni;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads Avni ordered pages through a single worker and commits them row by row.
 *
 * <p>The poll thread owns the mutable checkpoint. HTTPS never holds the collector lock, so a
 * savepoint or cancellation can interrupt discovery, login or an in-flight page.
 */
final class AvniReader implements SourceReader<SeaTunnelRow, AvniSplit> {
    private final AvniConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();

    // Cancellation may race with engine initialization or an active network exchange.
    private volatile boolean closed;
    private boolean idle;
    private boolean signaled;
    private AvniSplit progress;
    private AvniApi.Page page;
    private AvniHttp http;
    private AvniApi api;
    private ExecutorService executor;
    private Future<?> pending;
    private long expires;

    AvniReader(AvniConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Prepares a worker on the subtask that owns the Avni split.
     *
     * <p>No provider socket opens until the first scheduled data read.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new AvniHttp(config);
                api = new AvniApi(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "avni-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Fetches a page, accepts one row, or advances the resource cursor on each engine poll.
     *
     * <p>Only a validated page may reach the collector. Terminal and empty windows require no
     * authentication or data requests.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || signaled) {
                return;
            }
            if (progress == null ? idle : progress.finished()) {
                signaled = true;
                context.signalNoMoreElement();
                return;
            }
            if (progress == null) {
                return;
            }
        }
        AvniResource resource = config.resources.get(progress.resource);
        if (page == null) {
            page = fetch(output, resource);
        } else if (progress.row < page.rows.size()) {
            commit(
                    output,
                    () -> {
                        AvniApi.Row value = page.rows.get(progress.row);
                        SeaTunnelRow row =
                                new SeaTunnelRow(
                                        new Object[] {
                                            resource.label,
                                            value.id,
                                            value.timestamp,
                                            value.record.toString()
                                        });
                        row.setTableId(TablePath.DEFAULT.getFullName());
                        output.collect(row);
                        ++progress.row;
                    });
        } else {
            commit(
                    output,
                    () -> {
                        progress.advance(page, config);
                        page = null;
                    });
        }
    }

    private AvniApi.Page fetch(Collector<SeaTunnelRow> output, AvniResource resource)
            throws Exception {
        if (progress.page >= config.maxPages) {
            throw resource.failure("exceeded max_pages_per_resource");
        }
        AvniApi.Page received =
                get(
                        resource,
                        AvniApi.Page.class,
                        deadline -> api.page(resource, progress, deadline));
        if (received == null) {
            return null;
        }
        if (progress.row > received.rows.size()
                || progress.digest != null && !progress.digest.equals(received.digest)) {
            throw resource.failure("page changed since checkpoint");
        }
        if (progress.totalPages >= 0
                && (progress.totalPages != received.totalPages
                        || progress.effectiveSize != received.pageSize)) {
            throw resource.failure("page metadata changed within the fixed audit window");
        }
        // Bind metadata and payload before any row can be included in a subsequent savepoint.
        commit(
                output,
                () -> {
                    progress.totalPages = received.totalPages;
                    progress.effectiveSize = received.pageSize;
                    progress.digest = received.digest;
                });
        return received;
    }

    private <T> T get(AvniResource resource, Class<T> kind, Network<T> work) throws Exception {
        Future<?> request;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                expires = http.deadline();
                long deadline = expires;
                Callable<T> task = () -> work.read(deadline);
                pending = executor.submit(task);
            }
            request = pending;
        }
        try {
            T result = kind.cast(request.get(5, TimeUnit.MILLISECONDS));
            synchronized (lifecycle) {
                pending = null;
                return closed ? null : result;
            }
        } catch (TimeoutException ex) {
            if (!closed && expires - System.nanoTime() <= 0) {
                cancel();
                throw resource.failure("HTTPS operation deadline exceeded");
            }
            return null;
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw resource.failure("read canceled");
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof AvniResource.Failure) {
                throw (AvniResource.Failure) ex.getCause();
            }
            throw resource.failure("provider response could not be read");
        }
    }

    private void commit(Collector<SeaTunnelRow> output, Runnable change) {
        synchronized (output.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (!closed) {
                    change.run();
                }
            }
        }
    }

    /**
     * Captures a detached position after the current accepted row.
     *
     * <p>Only fixed query bounds, pagination and the committed row are retained.
     */
    @Override
    public List<AvniSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return progress == null ? List.of() : List.of(progress.copy());
        }
    }

    /**
     * Accepts one account scan on the active subtask and verifies its logical scope.
     *
     * <p>Restore preserves the original audit time fence.
     */
    @Override
    public void addSplits(List<AvniSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid Avni reader assignment");
            }
            AvniSplit split = splits.get(0);
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
     * Aborts current HTTPS and prevents later work on the canceled reader.
     *
     * <p>The source checkpoint lock is independent from this hook.
     */
    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
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
    }

    @Override
    public void close() {
        cancel();
    }

    /**
     * Carries one bounded remote read and leaves state updates on the engine poll thread.
     *
     * <p>One absolute deadline applies to authentication and all attempts of the operation.
     */
    private interface Network<T> {
        T read(long deadline) throws InterruptedException;
    }
}
