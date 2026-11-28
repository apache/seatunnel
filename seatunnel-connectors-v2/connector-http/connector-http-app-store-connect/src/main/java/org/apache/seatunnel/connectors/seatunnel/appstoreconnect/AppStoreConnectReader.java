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
package org.apache.seatunnel.connectors.seatunnel.appstoreconnect;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectResource.Stage;

/**
 * Traverses one resource at a time with a bounded stack of parent collection pages.
 *
 * <p>The I/O worker resolves pages and stages report data. Only the engine reader emits records and
 * commits the corresponding source offsets under the checkpoint lock.
 */
final class AppStoreConnectReader implements SourceReader<SeaTunnelRow, AppStoreConnectSplit> {
    private final Context context;
    private final AppStoreConnectConfig config;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();
    private final Map<AppStoreConnectSplit.Frame, AppStoreConnectApi.Page> pages =
            new IdentityHashMap<>();

    // The cancel hook may run concurrently with a request or report slice.
    private volatile boolean closed;

    private boolean noMoreSplits;
    private boolean signaled;
    private AppStoreConnectSplit position;
    private AppStoreConnectHttp http;
    private AppStoreConnectApi api;
    private ExecutorService worker;
    private Future<?> pending;
    private long pendingDeadline;

    // Holds the single live or staged report until emission finishes or the reader is canceled.
    private AppStoreConnectReport ownedReport;

    private AppStoreConnectReport emitting;

    AppStoreConnectReader(AppStoreConnectConfig config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Initializes I/O only on the active reader assigned the serial account scan.
     *
     * <p>Idle subtasks allocate no HTTPS worker, and a prior cancellation remains sticky.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && worker == null && context.getIndexOfSubtask() == 0) {
                http = new AppStoreConnectHttp(config);
                api = new AppStoreConnectApi(config, http);
                worker =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "app-store-connect-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Advances one traversal step or emitted row while returning promptly for engine barriers.
     *
     * <p>Network requests and spool validation run on the I/O worker without the checkpoint lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || signaled) {
                return;
            }
            if (position == null ? noMoreSplits : position.finished(config)) {
                signaled = true;
                context.signalNoMoreElement();
                return;
            }
            if (position == null) {
                return;
            }
        }
        AppStoreConnectResource resource = config.resources.get(position.resource);
        LocalDate today = AppStoreConnectConfig.utcDay(position.cutoff);
        if (resource.dated()) {
            dated(output, resource, today);
            return;
        }
        if (position.stack.isEmpty()) {
            commit(
                    output,
                    () ->
                            position.stack.add(
                                    new AppStoreConnectSplit.Frame(
                                            Stage.APPS, AppStoreConnectApi.Parents.root())));
            return;
        }
        AppStoreConnectSplit.Frame frame = position.stack.get(position.stack.size() - 1);
        if (frame.stage == Stage.DETAIL) {
            ObjectNode row = fetch(resource, () -> api.detail(resource, frame.parents));
            if (row != null) {
                commit(
                        output,
                        () -> {
                            emit(output, resource, frame.parents.segment, frame.parents, null, row);
                            pop(frame);
                        });
            }
        } else if (frame.stage == Stage.DOWNLOAD) {
            report(
                    output,
                    resource,
                    frame.parents,
                    null,
                    () -> api.analytics(resource, frame.parents, http.deadline()),
                    () -> pop(frame));
        } else {
            collection(output, resource, frame, today);
        }
    }

    private void dated(
            Collector<SeaTunnelRow> output, AppStoreConnectResource resource, LocalDate today)
            throws Exception {
        LocalDate date = position.reportDate;
        if (date == null) {
            commit(output, () -> position.reportDate = config.firstDate(resource, today));
        } else if (date.isAfter(config.lastDate(resource, today))) {
            commit(output, position::nextResource);
        } else if (resource == AppStoreConnectResource.SUBSCRIPTION_EVENT_REPORT
                && config.eventSkip.contains(date)) {
            commit(output, () -> position.reportDate = date.plusDays(1));
        } else {
            report(
                    output,
                    resource,
                    AppStoreConnectApi.Parents.root(),
                    date,
                    () -> api.dated(resource, date, http.deadline()),
                    () -> {
                        position.reportDate =
                                resource.terminal == Stage.FINANCE
                                        ? date.plusMonths(1)
                                        : date.plusDays(1);
                        position.clearReport();
                    });
        }
    }

    private void collection(
            Collector<SeaTunnelRow> output,
            AppStoreConnectResource resource,
            AppStoreConnectSplit.Frame frame,
            LocalDate today)
            throws Exception {
        AppStoreConnectApi.Page cached = pages.get(frame);
        if (cached == null) {
            cached =
                    fetch(
                            resource,
                            () ->
                                    api.collection(
                                            resource, frame.stage, frame.parents, frame.cursor));
            if (cached == null) {
                return;
            }
            if (frame.offset > cached.rows.size()
                    || frame.digest != null && !frame.digest.equals(cached.digest)) {
                throw resource.failure("collection page changed since checkpoint");
            }
            pages.put(frame, cached);
        }
        AppStoreConnectApi.Page current = cached;
        if (frame.offset == current.rows.size()) {
            commit(
                    output,
                    () -> {
                        if (current.next == null) {
                            pop(frame);
                        } else {
                            frame.advance(resource, current.next, config.maxPages);
                            pages.remove(frame);
                        }
                    });
            return;
        }
        ObjectNode item = (ObjectNode) current.rows.get(frame.offset);
        String id = AppStoreConnectApi.id(resource, item);
        JsonNode attributes = item.path("attributes");
        LocalDate processing =
                frame.stage == Stage.INSTANCES
                        ? AppStoreConnectApi.processingDate(resource, attributes)
                        : frame.parents.processing;
        boolean selected = selected(resource, frame.stage, attributes, processing, today);
        commit(
                output,
                () -> {
                    if (selected) {
                        AppStoreConnectApi.Parents lineage =
                                frame.parents.descend(frame.stage, id, processing);
                        if (frame.stage == resource.terminal) {
                            emit(output, resource, id, lineage, null, item);
                        } else {
                            position.stack.add(
                                    new AppStoreConnectSplit.Frame(
                                            nextStage(resource, frame.stage), lineage));
                        }
                    }
                    frame.digest = current.digest;
                    frame.offset++;
                });
    }

    private boolean selected(
            AppStoreConnectResource resource,
            Stage stage,
            JsonNode attr,
            LocalDate processing,
            LocalDate today) {
        switch (stage) {
            case REVIEWS:
                Instant created = AppStoreConnectApi.reviewDate(resource, attr);
                return !created.isAfter(position.cutoff)
                        && (config.reviewsStart == null || !created.isBefore(config.reviewsStart));
            case REQUESTS:
                return resource.access().equals(attr.path("accessType").asText(""));
            case REPORTS:
                return resource.family.title.equals(attr.path("name").asText(""));
            case INSTANCES:
                return "DAILY".equals(attr.path("granularity").asText(""))
                        && (resource.historical
                                || !processing.isBefore(config.analyticsFirst(today))
                                        && processing.isBefore(today));
            case SEGMENTS:
                return AppStoreConnectApi.positiveSize(resource, attr) > 0;
            default:
                return true;
        }
    }

    private static Stage nextStage(AppStoreConnectResource resource, Stage current) {
        switch (current) {
            case APPS:
                return resource.terminal == Stage.REVIEWS ? Stage.REVIEWS : Stage.REQUESTS;
            case REQUESTS:
                return Stage.REPORTS;
            case REPORTS:
                return Stage.INSTANCES;
            case INSTANCES:
                return Stage.SEGMENTS;
            case SEGMENTS:
                return resource.terminal;
            default:
                throw resource.failure("unexpected traversal stage");
        }
    }

    private void pop(AppStoreConnectSplit.Frame frame) {
        pages.remove(frame);
        position.stack.remove(position.stack.size() - 1);
        position.clearReport();
        if (position.stack.isEmpty()) {
            position.nextResource();
        }
    }

    /**
     * Validates content against any saved report digest before seeking or exposing it.
     *
     * <p>Local files have one owner even when cancellation races with a finished download.
     */
    private void report(
            Collector<SeaTunnelRow> output,
            AppStoreConnectResource resource,
            AppStoreConnectApi.Parents parents,
            LocalDate date,
            Callable<AppStoreConnectReport> request,
            Runnable complete)
            throws Exception {
        if (emitting == null) {
            String savedDigest = position.reportDigest;
            long savedOffset = position.reportOffset;
            Optional<AppStoreConnectReport> result =
                    fetch(
                            resource,
                            () -> {
                                AppStoreConnectReport downloaded = request.call();
                                if (downloaded == null) {
                                    if (savedDigest != null) {
                                        throw resource.failure("saved report is unavailable");
                                    }
                                    return Optional.empty();
                                }
                                boolean transferred = false;
                                try {
                                    if (savedDigest != null
                                            && !savedDigest.equals(downloaded.digest)) {
                                        throw resource.failure("report changed since checkpoint");
                                    }
                                    downloaded.seek(http, savedOffset, http.deadline());
                                    synchronized (lifecycle) {
                                        if (closed) {
                                            throw new InterruptedException(
                                                    "AppStoreConnect report canceled");
                                        }
                                        ownedReport = downloaded;
                                        transferred = true;
                                    }
                                    return Optional.of(downloaded);
                                } finally {
                                    if (!transferred) {
                                        downloaded.close();
                                    }
                                }
                            });
            if (result == null) {
                return;
            }
            if (!result.isPresent()) {
                commit(output, complete);
                return;
            }
            emitting = result.get();
        }
        String reportDate =
                date == null
                        ? parents.processing.toString()
                        : resource.terminal == Stage.FINANCE
                                ? YearMonth.from(date).toString()
                                : date.toString();
        AppStoreConnectReport current = emitting;
        if (position.reportOffset == current.size) {
            commit(output, complete);
            synchronized (lifecycle) {
                if (ownedReport == current) {
                    current.close();
                    ownedReport = null;
                }
                emitting = null;
            }
            return;
        }
        ObjectNode row;
        synchronized (lifecycle) {
            if (closed) {
                return;
            }
            row = current.next(http.json);
        }
        commit(
                output,
                () -> {
                    if (date != null
                            || !row.path("Date").asText("").isEmpty()
                            || resource
                                    == AppStoreConnectResource
                                            .APP_STORE_INSTALLATIONS_AND_DELETIONS_HISTORICAL) {
                        String id =
                                (date == null ? parents.segment : reportDate)
                                        + ":"
                                        + (position.reportOffset + 1);
                        emit(output, resource, id, parents, reportDate, row);
                    }
                    position.reportDigest = current.digest;
                    position.reportOffset++;
                });
    }

    private static void emit(
            Collector<SeaTunnelRow> output,
            AppStoreConnectResource resource,
            String id,
            AppStoreConnectApi.Parents parents,
            String reportDate,
            ObjectNode row) {
        SeaTunnelRow event =
                new SeaTunnelRow(
                        new Object[] {
                            resource.key(),
                            id,
                            parents.app,
                            parents.request,
                            parents.report,
                            parents.instance,
                            parents.segment,
                            reportDate == null && parents.processing != null
                                    ? parents.processing.toString()
                                    : reportDate,
                            row.toString()
                        });
        event.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(event);
    }

    private <T> T fetch(AppStoreConnectResource resource, Callable<T> request) throws Exception {
        Future<?> current;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                // Report staging and a second bounded pass to seek a restored row may both run.
                pendingDeadline =
                        http.deadline() + TimeUnit.MILLISECONDS.toNanos(config.operationTimeout);
                pending = worker.submit(request);
            }
            current = pending;
        }
        try {
            @SuppressWarnings("unchecked")
            T result = (T) current.get(5, TimeUnit.MILLISECONDS);
            synchronized (lifecycle) {
                pending = null;
                return closed ? null : result;
            }
        } catch (TimeoutException ex) {
            if (!closed && pendingDeadline - System.nanoTime() <= 0) {
                cancel();
                throw resource.failure("worker operation deadline exceeded");
            }
            return null;
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw resource.failure("request canceled");
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof AppStoreConnectResource.Failure) {
                throw (AppStoreConnectResource.Failure) ex.getCause();
            }
            throw resource.failure("request could not be completed");
        }
    }

    /**
     * Keeps the committed offset consistent with the engine's accepted records.
     *
     * <p>The collector lock orders each state transition with the engine's checkpoint barrier.
     */
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
     * Captures the complete DFS path after the latest accepted record.
     *
     * <p>The detached copy prevents subsequent traversal from mutating an in-flight snapshot.
     */
    @Override
    public List<AppStoreConnectSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return position == null
                    ? Collections.emptyList()
                    : Collections.singletonList(position.copy());
        }
    }

    /**
     * Applies the assigned or restored split without querying the provider.
     *
     * <p>Options are checked before the reader can use the saved IDs and offsets.
     */
    @Override
    public void addSplits(List<AppStoreConnectSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (position != null || splits.size() != 1 || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid AppStoreConnect reader assignment");
            }
            AppStoreConnectSplit incoming = splits.get(0);
            incoming.validate(config);
            position = incoming.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (checkpoint) {
            noMoreSplits = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Immediately closes the active socket and cancels network or file staging work.
     *
     * <p>The hook never waits for the engine's checkpoint or collector lock.
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
            if (worker != null) {
                worker.shutdownNow();
            }
            if (ownedReport != null) {
                ownedReport.close();
                ownedReport = null;
            }
        }
    }

    @Override
    public void close() {
        cancel();
    }
}
