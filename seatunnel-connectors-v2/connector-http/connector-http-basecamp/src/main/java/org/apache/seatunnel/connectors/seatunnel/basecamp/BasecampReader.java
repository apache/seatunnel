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
package org.apache.seatunnel.connectors.seatunnel.basecamp;

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
 * Walks the Basecamp project tree and recording feeds in one bounded source split.
 *
 * <p>Provider requests run outside checkpoint locks. Accepted rows and cursor updates are committed
 * together on the poll thread.
 */
final class BasecampReader implements SourceReader<SeaTunnelRow, BasecampSplit> {
    private final BasecampConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();

    // Closing a worker remains sticky, including before its open callback.
    private volatile boolean closed;
    private boolean idle;
    private boolean signaled;
    private BasecampSplit progress;
    private BasecampApi.Page topPage;
    private BasecampApi.Dock dock;
    private BasecampApi.Page entryPage;
    private BasecampHttp http;
    private BasecampApi api;
    private ExecutorService executor;
    private Future<?> pending;
    private long expires;

    BasecampReader(BasecampConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Prepares only the worker that owns the account split.
     *
     * <p>Credentials are handed to the worker after the engine assigns a live position.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new BasecampHttp(config);
                api = new BasecampApi(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "basecamp-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Advances one collection, parent binding, or accepted row per engine poll.
     *
     * <p>A completed split returns without refreshing authentication.
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
        if (progress.phase == BasecampResource.PROJECTS) {
            projects(output);
        } else {
            feed(output);
        }
    }

    private void projects(Collector<SeaTunnelRow> output) throws Exception {
        BasecampResource resource = BasecampResource.PROJECTS;
        BasecampSplit.Position position = progress.top;
        if (topPage == null) {
            topPage = fetch(output, resource, position, null, null);
            if (topPage != null
                    && progress.projectId != null
                    && (position.row == topPage.rows.size()
                            || !progress.projectId.equals(topPage.rows.get(position.row).id))) {
                throw resource.failure("project identity changed since checkpoint");
            }
        } else if (position.row == topPage.rows.size()) {
            commit(output, () -> finishTopPage(resource));
        } else if (progress.projectId == null) {
            BasecampApi.Row parent = topPage.rows.get(position.row);
            commit(
                    output,
                    () -> {
                        if (config.includes(resource)) {
                            emit(output, resource, parent);
                        }
                        if (config.needsSchedules()) {
                            progress.projectId = parent.id;
                        } else {
                            ++position.row;
                        }
                    });
        } else {
            schedules(output);
        }
    }

    private void schedules(Collector<SeaTunnelRow> output) throws Exception {
        BasecampResource resource = BasecampResource.SCHEDULES;
        if (dock == null) {
            BasecampApi.Dock received =
                    get(
                            resource,
                            BasecampApi.Dock.class,
                            deadline -> api.dock(progress.projectId, deadline));
            if (received == null) {
                return;
            }
            if (progress.dockRow > received.schedules.size()
                    || progress.dockDigest != null && !progress.dockDigest.equals(received.digest)
                    || progress.scheduleId != null
                            && (progress.dockRow == received.schedules.size()
                                    || !progress.scheduleId.equals(
                                            received.schedules.get(progress.dockRow).row.id))) {
                throw resource.failure("project dock changed since checkpoint");
            }
            commit(output, () -> progress.dockDigest = received.digest);
            dock = received;
        } else if (progress.dockRow == dock.schedules.size()) {
            commit(
                    output,
                    () -> {
                        progress.completeProject();
                        dock = null;
                    });
        } else if (progress.scheduleId == null) {
            BasecampApi.Schedule current = dock.schedules.get(progress.dockRow);
            commit(
                    output,
                    () -> {
                        if (config.includes(resource)) {
                            emit(output, resource, current.row);
                        }
                        if (current.enabled && config.includes(BasecampResource.SCHEDULE_ENTRIES)) {
                            progress.scheduleId = current.row.id;
                        } else {
                            ++progress.dockRow;
                        }
                    });
        } else {
            entries(output);
        }
    }

    private void entries(Collector<SeaTunnelRow> output) throws Exception {
        BasecampResource resource = BasecampResource.SCHEDULE_ENTRIES;
        BasecampSplit.Position position = progress.entries;
        if (entryPage == null) {
            entryPage = fetch(output, resource, position, progress.projectId, progress.scheduleId);
        } else if (position.row < entryPage.rows.size()) {
            commit(
                    output,
                    () -> {
                        emit(output, resource, entryPage.rows.get(position.row));
                        ++position.row;
                    });
        } else {
            commit(
                    output,
                    () -> {
                        if (entryPage.next == null) {
                            progress.completeSchedule();
                        } else {
                            position.advance(resource, entryPage, config.maxPages);
                        }
                        entryPage = null;
                    });
        }
    }

    /**
     * Applies the inclusive recording window after validating descending provider order.
     *
     * <p>A future record cannot advance the date fence or skip a later page. Equal timestamps stay
     * in the same window until every matching record has been consumed.
     */
    private void feed(Collector<SeaTunnelRow> output) throws Exception {
        BasecampResource resource = progress.phase;
        BasecampSplit.Position position = progress.top;
        if (topPage == null) {
            topPage = fetch(output, resource, position, null, null);
        } else if (position.row == topPage.rows.size()) {
            commit(output, () -> finishTopPage(resource));
        } else {
            BasecampApi.Row row = topPage.rows.get(position.row);
            commit(
                    output,
                    () -> {
                        if (row.when.isBefore(progress.start)) {
                            progress.completePhase(config);
                            topPage = null;
                        } else {
                            if (!row.when.isAfter(progress.end)) {
                                emit(output, resource, row);
                            }
                            ++position.row;
                        }
                    });
        }
    }

    private void finishTopPage(BasecampResource resource) {
        if (topPage.next == null) {
            progress.completePhase(config);
        } else {
            progress.top.advance(resource, topPage, config.maxPages);
        }
        topPage = null;
    }

    private BasecampApi.Page fetch(
            Collector<SeaTunnelRow> output,
            BasecampResource resource,
            BasecampSplit.Position position,
            String project,
            String schedule)
            throws Exception {
        BasecampApi.Page received =
                get(
                        resource,
                        BasecampApi.Page.class,
                        deadline -> api.page(resource, position, project, schedule, deadline));
        if (received == null) {
            return null;
        }
        if (position.row > received.rows.size()
                || position.digest != null && !position.digest.equals(received.digest)) {
            throw resource.failure("page changed since checkpoint");
        }
        commit(output, () -> position.digest = received.digest);
        return received;
    }

    private static void emit(
            Collector<SeaTunnelRow> output, BasecampResource resource, BasecampApi.Row item) {
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            resource.label,
                            item.id,
                            item.project,
                            item.schedule,
                            item.updated,
                            item.raw.toString()
                        });
        row.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(row);
    }

    private <T> T get(BasecampResource resource, Class<T> kind, Network<T> work) throws Exception {
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
            if (ex.getCause() instanceof BasecampResource.Failure) {
                throw (BasecampResource.Failure) ex.getCause();
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
     * <p>Only validated query and parent positions are retained.
     */
    @Override
    public List<BasecampSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return progress == null ? List.of() : List.of(progress.copy());
        }
    }

    /**
     * Accepts one account scan on the active subtask and verifies its logical scope.
     *
     * <p>Restore keeps the original feed cutoff and the two nested parent positions.
     */
    @Override
    public void addSplits(List<BasecampSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid Basecamp reader assignment");
            }
            BasecampSplit split = splits.get(0);
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
