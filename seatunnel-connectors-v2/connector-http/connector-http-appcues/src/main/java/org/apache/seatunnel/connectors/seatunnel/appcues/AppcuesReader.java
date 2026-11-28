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

package org.apache.seatunnel.connectors.seatunnel.appcues;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.appcues.AppcuesSource.Split;

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
 * Reads bounded resource snapshots outside the checkpoint lock and emits them in small batches.
 * Checkpoints record only positions and the fingerprint needed for safe partial replay.
 */
final class AppcuesReader implements SourceReader<SeaTunnelRow, Split> {
    private static final int ROWS_PER_POLL = 64;
    private final AppcuesConfig config;
    private final Context context;
    private final String tablePath;
    private final AppcuesHttp http;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Split> position = new AtomicReference<>();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-appcues-https");
                        thread.setDaemon(true);
                        return thread;
                    });

    private volatile Future<AppcuesResource.Snapshot> pending;
    private long deadline;
    private AppcuesResource.Snapshot response;
    private boolean eof;

    AppcuesReader(AppcuesConfig config, Context context, String tablePath) {
        this.config = config;
        this.context = context;
        this.tablePath = tablePath;
        this.http = new AppcuesHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Collects rows and publishes the matching next position under the engine checkpoint lock. An
     * outstanding HTTP operation never holds the lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            Split split = position.get();
            if (closed.get() || eof || split == null) {
                return;
            }
            verify(split);
            if (split.resource == config.resources.size()) {
                eof = true;
                context.signalNoMoreElement();
                return;
            }
            AppcuesResource resource = config.resources.get(split.resource);
            if (response == null && !prepare(resource, split)) {
                return;
            }
            emit(resource, split, output);
        }
    }

    private void verify(Split split) {
        if (!config.fingerprint.equals(split.fingerprint)
                || split.end == null
                || split.end.isBefore(config.start)
                || (config.configuredEnd != null && !config.configuredEnd.equals(split.end))) {
            throw AppcuesConfig.invalid("query configuration changed during restore");
        }
        if (split.resource < 0
                || split.resource > config.resources.size()
                || split.offset < 0
                || ((split.offset == 0) != (split.digest == null))
                || (split.resource == config.resources.size() && split.offset > 0)) {
            throw AppcuesConfig.invalid("invalid scan checkpoint");
        }
    }

    private boolean prepare(AppcuesResource resource, Split split) throws IOException {
        Future<AppcuesResource.Snapshot> task = pending;
        if (task == null) {
            long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.requestTimeout);
            deadline = expires;
            try {
                pending = worker.submit(() -> http.fetch(resource, split.end, expires));
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw AppcuesHttp.failure(resource, "retrieval worker is unavailable");
                }
            }
            return false;
        }
        if (!task.isDone()) {
            if (System.nanoTime() - deadline >= 0) {
                close();
                throw AppcuesHttp.failure(resource, "request deadline exceeded");
            }
            return false;
        }
        try {
            AppcuesResource.Snapshot fetched = task.get();
            if (closed.get()) {
                return false;
            }
            verifyRestored(resource, split, fetched);
            response = fetched;
            pending = null;
            return true;
        } catch (CancellationException e) {
            if (!closed.get()) {
                throw AppcuesHttp.failure(resource, "request canceled");
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closed.get()) {
                throw AppcuesHttp.failure(resource, "request interrupted");
            }
            return false;
        } catch (ExecutionException e) {
            if (closed.get()) {
                return false;
            }
            if (e.getCause() instanceof AppcuesHttp.SafeFailure) {
                throw (AppcuesHttp.SafeFailure) e.getCause();
            }
            throw AppcuesHttp.failure(resource, "resource retrieval failed");
        }
    }

    /**
     * Refuses to apply a saved offset to a different set or distribution of selected records.
     * Canonically equivalent orderings produce the same digest.
     */
    static void verifyRestored(
            AppcuesResource resource, Split split, AppcuesResource.Snapshot snapshot)
            throws IOException {
        if (split.offset > 0
                && (split.offset >= snapshot.rows.size()
                        || !snapshot.digest.equals(split.digest))) {
            throw AppcuesHttp.failure(resource, "filtered response changed since the checkpoint");
        }
    }

    private void emit(AppcuesResource resource, Split split, Collector<SeaTunnelRow> output) {
        int end = Math.min(response.rows.size(), split.offset + ROWS_PER_POLL);
        for (int i = split.offset; i < end; i++) {
            if (closed.get()) {
                return;
            }
            AppcuesResource.Row record = response.rows.get(i);
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                resource.key(), record.id, record.time.toString(), record.payload
                            });
            row.setTableId(tablePath);
            output.collect(row);
        }
        if (end == response.rows.size()) {
            position.set(new Split(config.fingerprint, split.end, split.resource + 1, 0, null));
            response = null;
        } else {
            position.set(
                    new Split(config.fingerprint, split.end, split.resource, end, response.digest));
        }
    }

    @Override
    public List<Split> snapshotState(long checkpointId) {
        Split split = position.get();
        return split == null ? Collections.emptyList() : Collections.singletonList(split);
    }

    @Override
    public void addSplits(List<Split> splits) {
        if (splits.size() != 1 || !position.compareAndSet(null, splits.get(0))) {
            throw AppcuesConfig.invalid("expected one assigned scan");
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    /**
     * Closes active TLS I/O and interrupts retry delays without taking the checkpoint lock. Normal
     * close and cancellation follow the same path.
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
