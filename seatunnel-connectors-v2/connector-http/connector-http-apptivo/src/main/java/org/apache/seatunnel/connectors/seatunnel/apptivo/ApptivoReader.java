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
package org.apache.seatunnel.connectors.seatunnel.apptivo;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Streams each Apptivo page through a single engine reader.
 *
 * <p>Independent HTTPS work keeps the checkpoint lock available during stalled or retrying reads.
 * Accepted source rows and offsets advance together against the collector barrier.
 */
final class ApptivoReader implements SourceReader<SeaTunnelRow, ApptivoSplit> {
    private final ApptivoConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();

    // Cancellation may be requested from a different engine thread at any point.
    private volatile boolean closed;
    private boolean inactive;
    private boolean signaled;
    private ApptivoSplit current;
    private ApptivoApi.Page page;
    private ApptivoHttp http;
    private ApptivoApi api;
    private ExecutorService executor;
    private Future<ApptivoApi.Page> pending;
    private long deadline;

    ApptivoReader(ApptivoConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Creates resources only on the active account reader.
     *
     * <p>A cancellation arriving before initialization prevents any network task allocation.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new ApptivoHttp(config);
                api = new ApptivoApi(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "apptivo-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Emits at most one validated row per poll and commits its position.
     *
     * <p>The active HTTPS request is not part of state until its validated page is consumed.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || signaled) {
                return;
            }
            if (current == null ? inactive : current.finished(config)) {
                signaled = true;
                context.signalNoMoreElement();
                return;
            }
            if (current == null) {
                return;
            }
        }
        ApptivoResource resource = config.resources.get(current.resource);
        if (page == null) {
            page = fetch(resource);
            if (page == null) {
                return;
            }
            if (current.row > page.rows.size()
                    || current.digest != null && !current.digest.equals(page.digest)) {
                throw resource.failure("saved page changed since checkpoint");
            }
            if (page.digest.equals(current.preceding)) {
                throw resource.failure("pagination repeated provider rows");
            }
        }
        ApptivoApi.Page ready = page;
        commit(
                output,
                () -> {
                    current.firm = ready.firm;
                    if (current.row < ready.rows.size()) {
                        ApptivoApi.Record record = ready.rows.get(current.row);
                        SeaTunnelRow row =
                                new SeaTunnelRow(
                                        new Object[] {
                                            resource.label, record.id, record.value.toString()
                                        });
                        row.setTableId(TablePath.DEFAULT.getFullName());
                        output.collect(row);
                        current.total = ready.total;
                        current.digest = ready.digest;
                        ++current.row;
                    } else {
                        if (ready.more) {
                            current.nextPage(resource, config.pageSize, config.maxPages, ready);
                        } else {
                            current.nextResource();
                        }
                        page = null;
                    }
                });
    }

    private ApptivoApi.Page fetch(ApptivoResource resource) throws Exception {
        Future<ApptivoApi.Page> request;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                deadline = http.deadline();
                int offset = current.offset;
                Long total = current.total;
                String firm = current.firm;
                pending = executor.submit(() -> api.fetch(resource, offset, total, firm, deadline));
            }
            request = pending;
        }
        try {
            ApptivoApi.Page response = request.get(5, TimeUnit.MILLISECONDS);
            synchronized (lifecycle) {
                pending = null;
                return closed ? null : response;
            }
        } catch (TimeoutException ex) {
            if (!closed && deadline - System.nanoTime() <= 0) {
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
            if (ex.getCause() instanceof ApptivoResource.Failure) {
                throw (ApptivoResource.Failure) ex.getCause();
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
     * Captures the accepted source position under the same lock as emission.
     *
     * <p>A reader before assignment produces an empty reader-side checkpoint.
     */
    @Override
    public List<ApptivoSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return current == null ? List.of() : List.of(current.copy());
        }
    }

    /**
     * Installs a fresh or restored account split without provider side effects.
     *
     * <p>The source can bind only one resource scan to the active reader.
     */
    @Override
    public void addSplits(List<ApptivoSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || current != null || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid Apptivo reader assignment");
            }
            ApptivoSplit received = splits.get(0);
            received.validate(config);
            current = received.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (checkpoint) {
            inactive = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Aborts the in-flight TLS call and fails no further queues.
     *
     * <p>This hook is sticky and does not need the reader or collector checkpoint locks.
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
}
