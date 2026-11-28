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

package org.apache.seatunnel.connectors.seatunnel.alphavantage;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

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
 * The retrieval worker handles HTTPS pacing and I/O. The engine thread records progress under the
 * checkpoint lock after emitting rows.
 */
final class AlphaVantageReader implements SourceReader<SeaTunnelRow, AlphaVantageSource.Split> {
    private static final int ROWS_PER_POLL = 128;

    private final AlphaVantageConfig config;
    private final Context context;
    private final String tableId;
    private final AlphaVantageHttps transport;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<AlphaVantageSource.Split> assignment = new AtomicReference<>();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-alpha-vantage-https");
                        thread.setDaemon(true);
                        return thread;
                    });

    private AlphaVantageSource.Split progress;
    private AlphaVantageResource.Snapshot snapshot;
    private volatile Future<AlphaVantageResource.Snapshot> pending;
    private long deadline;
    private boolean eof;

    AlphaVantageReader(AlphaVantageConfig config, Context context, String tableId) {
        this.config = config;
        this.context = context;
        this.tableId = tableId;
        this.transport = new AlphaVantageHttps(config);
    }

    @Override
    public void open() {}

    /**
     * A checkpoint during a pending request leaves the current response eligible for refetch
     * because the reader has emitted zero observations from it.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            if (closed.get() || eof) {
                return;
            }
            if (progress == null) {
                progress = assignment.getAndSet(null);
                if (progress == null) {
                    return;
                }
                if (!config.fingerprint.equals(progress.fingerprint)) {
                    throw AlphaVantageConfig.invalid("query configuration changed during restore");
                }
                if (progress.resource < 0
                        || progress.resource > config.resources.size()
                        || (progress.resource == config.resources.size()
                                && (progress.offset != 0 || progress.responseId != null))) {
                    throw AlphaVantageConfig.invalid("invalid resource position in checkpoint");
                }
            }
            if (progress.resource == config.resources.size()) {
                eof = true;
                context.signalNoMoreElement();
                return;
            }
            AlphaVantageResource resource = config.resources.get(progress.resource);
            if (snapshot == null && !prepare(resource)) {
                return;
            }
            deliver(resource, output);
        }
    }

    private boolean prepare(AlphaVantageResource resource) throws Exception {
        Future<AlphaVantageResource.Snapshot> active = pending;
        if (active == null) {
            long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.requestTimeout);
            deadline = expires;
            try {
                pending = worker.submit(() -> transport.get(resource, expires));
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw resource.failure("HTTPS worker is unavailable");
                }
            }
            return false;
        }
        if (!active.isDone()) {
            if (deadline - System.nanoTime() <= 0) {
                close();
                throw resource.failure("request deadline exceeded");
            }
            return false;
        }
        try {
            AlphaVantageResource.Snapshot fetched = active.get();
            if (closed.get()) {
                return false;
            }
            validateRestored(resource, progress, fetched);
            snapshot = fetched;
            pending = null;
            return true;
        } catch (CancellationException e) {
            if (!closed.get()) {
                throw resource.failure("unexpected request cancellation");
            }
            return false;
        } catch (ExecutionException e) {
            if (closed.get()) {
                return false;
            }
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw resource.failure("request failed");
        }
    }

    /**
     * Bind a saved offset to the snapshot that produced it. This prevents changes in observation
     * membership, values, or time zone from shifting the meaning of the offset.
     */
    static void validateRestored(
            AlphaVantageResource resource,
            AlphaVantageSource.Split split,
            AlphaVantageResource.Snapshot fetched)
            throws IOException {
        if (split.offset < 0
                || (split.offset == 0 && split.responseId != null)
                || (split.offset > 0
                        && (split.offset >= fetched.points.size()
                                || !fetched.identity.equals(split.responseId)))) {
            throw resource.failure("response changed since the checkpoint");
        }
    }

    private void deliver(AlphaVantageResource resource, Collector<SeaTunnelRow> output) {
        int end = Math.min(snapshot.points.size(), progress.offset + ROWS_PER_POLL);
        for (int i = progress.offset; i < end; i++) {
            if (closed.get()) {
                return;
            }
            AlphaVantageResource.Point point = snapshot.points.get(i);
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                resource.key(),
                                config.symbol,
                                point.date,
                                point.timestamp,
                                snapshot.timeZone,
                                point.data
                            });
            row.setTableId(tableId);
            output.collect(row);
        }
        if (end == snapshot.points.size()) {
            progress =
                    new AlphaVantageSource.Split(
                            config.fingerprint, progress.resource + 1, 0, null);
            snapshot = null;
        } else {
            progress =
                    new AlphaVantageSource.Split(
                            config.fingerprint, progress.resource, end, snapshot.identity);
        }
    }

    @Override
    public List<AlphaVantageSource.Split> snapshotState(long checkpointId) {
        AlphaVantageSource.Split current = progress == null ? assignment.get() : progress;
        return current == null ? Collections.emptyList() : Collections.singletonList(current);
    }

    @Override
    public void addSplits(List<AlphaVantageSource.Split> splits) {
        if (splits.size() != 1 || !assignment.compareAndSet(null, splits.get(0))) {
            throw AlphaVantageConfig.invalid("expected one assigned selection");
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    /**
     * Interrupts the retrieval worker and closes its socket without taking the checkpoint monitor.
     */
    @Override
    public void cancel() {
        close();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            transport.close();
            Future<?> active = pending;
            if (active != null) {
                active.cancel(true);
            }
            worker.shutdownNow();
        }
    }
}
