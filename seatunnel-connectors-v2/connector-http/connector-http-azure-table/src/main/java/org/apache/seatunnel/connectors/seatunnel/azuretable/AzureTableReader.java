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
package org.apache.seatunnel.connectors.seatunnel.azuretable;

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
 * Drives discovery and table reads through a single bounded network worker.
 *
 * <p>The poll thread commits source state under the collector checkpoint lock. Authentication,
 * transfer and response verification never hold that lock.
 */
final class AzureTableReader implements SourceReader<SeaTunnelRow, AzureTableSplit> {
    private final AzureTableConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();

    private volatile boolean closed;
    private boolean idle;
    private boolean signaled;

    // Progress belongs to the poll thread. A callback may snapshot it during an HTTPS request.
    private AzureTableSplit progress;
    private AzureTableApi.EntityPage page;
    private AzureTableHttp http;
    private AzureTableApi api;
    private ExecutorService executor;
    private Future<?> pending;
    private long expires;

    AzureTableReader(AzureTableConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Prepares the sole account request worker for the assigned reader.
     *
     * <p>Closing before open leaves cancellation sticky.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new AzureTableHttp(config);
                api = new AzureTableApi(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "azure-table-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Advances one discovery page, entity page, or accepted entity per engine poll.
     *
     * <p>A terminal checkpoint signals completion without opening fresh provider requests.
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
        if (progress.discovering) {
            String collection = AzureTableApi.DISCOVERY;
            AzureTableApi.DiscoveryPage discovered =
                    get(
                            collection,
                            AzureTableApi.DiscoveryPage.class,
                            deadline -> api.discover(progress.cursor, deadline));
            if (discovered != null) {
                commit(output, () -> progress.commitDiscovery(discovered, config));
            }
            return;
        }
        String table = progress.table();
        if (page == null) {
            page = fetch(output, table);
        } else if (progress.row < page.rows.size()) {
            commit(
                    output,
                    () -> {
                        AzureTableApi.Entity value = page.rows.get(progress.row);
                        SeaTunnelRow row =
                                new SeaTunnelRow(
                                        new Object[] {
                                            table,
                                            value.partition,
                                            value.row,
                                            value.timestamp,
                                            value.etag,
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
                        progress.commitEntityPage(page, config);
                        page = null;
                    });
        }
    }

    private AzureTableApi.EntityPage fetch(Collector<SeaTunnelRow> output, String table)
            throws Exception {
        AzureTableApi.EntityPage received =
                get(
                        table,
                        AzureTableApi.EntityPage.class,
                        deadline -> api.entities(table, progress.cursor, deadline));
        if (received == null) {
            return null;
        }
        if (progress.row > received.rows.size()
                || progress.digest != null && !progress.digest.equals(received.digest)) {
            throw new AzureTableApi.Failure(table, "page changed since checkpoint");
        }
        if (received.rows.stream()
                .anyMatch(entity -> progress.previous.contains(entity.identity))) {
            throw new AzureTableApi.Failure(table, "entity keys overlap the previous page");
        }
        commit(output, () -> progress.digest = received.digest);
        return received;
    }

    private <T> T get(String collection, Class<T> kind, Network<T> work) throws Exception {
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
                throw new AzureTableApi.Failure(collection, "HTTPS operation deadline exceeded");
            }
            return null;
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw new AzureTableApi.Failure(collection, "read canceled");
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof AzureTableApi.Failure) {
                throw (AzureTableApi.Failure) ex.getCause();
            }
            throw new AzureTableApi.Failure(collection, "provider response could not be read");
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
     * <p>Only the catalog names, opaque page tokens and committed row are retained.
     */
    @Override
    public List<AzureTableSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return progress == null ? List.of() : List.of(progress.copy());
        }
    }

    /**
     * Accepts one account scan on the active subtask and verifies its logical scope.
     *
     * <p>Restore retains the selection and cursor of the original account scan.
     */
    @Override
    public void addSplits(List<AzureTableSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid AzureTable reader assignment");
            }
            AzureTableSplit split = splits.get(0);
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
