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

package org.apache.seatunnel.connectors.seatunnel.appfollow;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Batch;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Binding;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Request;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Resource;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowSource.Cursor;

import java.io.IOException;
import java.util.ArrayList;
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

/**
 * Walks collections and their application attachments without retaining prior response bodies.
 * Active network waits stay off the engine thread and outside the checkpoint lock.
 */
final class AppfollowReader implements SourceReader<SeaTunnelRow, Cursor> {
    private static final Resource[] STAGES = Resource.values();
    private static final int ROWS_PER_POLL = 64;

    private final AppfollowConfig config;
    private final Context context;
    private final String tablePath;
    private final AppfollowHttp http;
    private final Object progressLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<String> collections = new ArrayList<>();
    private final List<Binding> bindings = new ArrayList<>();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-appfollow-https");
                        thread.setDaemon(true);
                        return thread;
                    });

    private boolean assigned;
    private boolean eof;
    private int stage;
    private int parent;
    private int row;
    private String digest;
    private Batch response;
    private long deadline;
    private volatile Future<Batch> pending;

    AppfollowReader(AppfollowConfig config, Context context, String tablePath) {
        this.config = config;
        this.context = context;
        this.tablePath = tablePath;
        this.http = new AppfollowHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Emits bounded row batches and advances discovery with the matching checkpoint position. The
     * collector lock is always obtained before the private progress lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            synchronized (progressLock) {
                if (closed.get() || !assigned || eof) {
                    return;
                }
                Request request = nextRequest();
                if (request == null) {
                    eof = true;
                    context.signalNoMoreElement();
                    return;
                }
                if (response != null || prepare(request)) {
                    emit(request, output);
                }
            }
        }
    }

    private Request nextRequest() {
        while (stage < STAGES.length) {
            Resource resource = STAGES[stage];
            if (config.reads(resource)) {
                if (resource == Resource.APP_LISTS) {
                    if (parent < collections.size()) {
                        return new Request(resource, collections.get(parent), null);
                    }
                } else if (resource == Resource.STAT_REVIEWS || resource == Resource.RATINGS) {
                    if (parent < bindings.size()) {
                        Binding binding = bindings.get(parent);
                        return new Request(resource, binding.collection, binding.external);
                    }
                } else {
                    return new Request(resource, null, null);
                }
            }
            finishStage();
        }
        return null;
    }

    private void finishStage() {
        if (STAGES[stage] == Resource.APP_LISTS) {
            collections.clear();
        } else if (STAGES[stage] == Resource.RATINGS) {
            bindings.clear();
        }
        stage++;
        parent = 0;
    }

    private boolean prepare(Request request) throws IOException {
        Future<Batch> task = pending;
        if (task == null) {
            long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.requestTimeout);
            deadline = expires;
            try {
                pending = worker.submit(() -> http.fetch(request, expires));
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw AppfollowHttp.failure(request, "retrieval worker is unavailable");
                }
            }
            return false;
        }
        if (!task.isDone()) {
            if (System.nanoTime() - deadline >= 0) {
                close();
                throw AppfollowHttp.failure(request, "request deadline exceeded");
            }
            return false;
        }
        try {
            Batch fetched = task.get();
            if (closed.get()) {
                return false;
            }
            verifyRestored(request, row, digest, fetched);
            if (collections.size() + fetched.collections.size() > config.maxEntities
                    || bindings.size() + fetched.bindings.size() > config.maxEntities) {
                throw AppfollowHttp.failure(request, "discovery exceeds max_entities");
            }
            response = fetched;
            pending = null;
            return true;
        } catch (CancellationException e) {
            if (!closed.get()) {
                throw AppfollowHttp.failure(request, "request canceled");
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closed.get()) {
                throw AppfollowHttp.failure(request, "request interrupted");
            }
            return false;
        } catch (ExecutionException e) {
            if (closed.get()) {
                return false;
            }
            if (e.getCause() instanceof AppfollowHttp.SafeFailure) {
                throw (AppfollowHttp.SafeFailure) e.getCause();
            }
            throw AppfollowHttp.failure(request, "request failed");
        }
    }

    /**
     * A checkpointed row position can be applied only to its original response and discovery list.
     * Completed parent requests use their saved attachment IDs and are not replayed on restore.
     */
    static void verifyRestored(Request request, int row, String digest, Batch fetched)
            throws IOException {
        if (row > 0 && (row >= fetched.rows.size() || !fetched.digest.equals(digest))) {
            throw AppfollowHttp.failure(request, "response changed since the checkpoint");
        }
    }

    private void emit(Request request, Collector<SeaTunnelRow> output) {
        int end = Math.min(response.rows.size(), row + ROWS_PER_POLL);
        for (int i = row; i < end; i++) {
            if (closed.get()) {
                return;
            }
            AppfollowData.Row record = response.rows.get(i);
            SeaTunnelRow result =
                    new SeaTunnelRow(
                            new Object[] {
                                request.resource.key(),
                                record.collection,
                                record.external,
                                record.payload
                            });
            result.setTableId(tablePath);
            output.collect(result);
        }
        if (end < response.rows.size()) {
            row = end;
            digest = response.digest;
            return;
        }
        collections.addAll(response.collections);
        bindings.addAll(response.bindings);
        response = null;
        row = 0;
        digest = null;
        if (request.resource == Resource.USERS || request.resource == Resource.APP_COLLECTIONS) {
            finishStage();
        } else {
            parent++;
        }
    }

    @Override
    public List<Cursor> snapshotState(long checkpointId) {
        synchronized (progressLock) {
            return assigned
                    ? Collections.singletonList(
                            new Cursor(
                                    config.fingerprint,
                                    stage,
                                    parent,
                                    row,
                                    digest,
                                    collections,
                                    bindings))
                    : Collections.emptyList();
        }
    }

    @Override
    public void addSplits(List<Cursor> splits) {
        synchronized (progressLock) {
            if (assigned || splits.size() != 1) {
                throw AppfollowConfig.invalid("expected one assigned scan");
            }
            Cursor state = splits.get(0);
            if (!config.fingerprint.equals(state.fingerprint)) {
                throw AppfollowConfig.invalid("query configuration changed during restore");
            }
            if (state.stage < 0
                    || state.stage > STAGES.length
                    || state.parent < 0
                    || state.row < 0
                    || ((state.row == 0) != (state.digest == null))
                    || state.collections.size() > config.maxEntities
                    || state.bindings.size() > config.maxEntities
                    || !validParentPosition(state)) {
                throw AppfollowConfig.invalid("invalid scan checkpoint");
            }
            collections.addAll(state.collections);
            bindings.addAll(state.bindings);
            stage = state.stage;
            parent = state.parent;
            row = state.row;
            digest = state.digest;
            assigned = true;
        }
    }

    private static boolean validParentPosition(Cursor state) {
        if (state.stage == STAGES.length) {
            return state.parent == 0
                    && state.row == 0
                    && state.collections.isEmpty()
                    && state.bindings.isEmpty();
        }
        switch (STAGES[state.stage]) {
            case APP_LISTS:
                return state.parent < state.collections.size()
                        || (state.parent == state.collections.size() && state.row == 0);
            case STAT_REVIEWS:
            case RATINGS:
                return state.parent < state.bindings.size()
                        || (state.parent == state.bindings.size() && state.row == 0);
            default:
                return state.parent == 0;
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    /**
     * Cancels the request and retry delay without waiting on the engine collector lock. The same
     * cleanup runs on normal close.
     */
    @Override
    public void cancel() {
        close();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<?> task = pending;
            if (task != null) {
                task.cancel(true);
            }
            worker.shutdownNow();
        }
    }
}
