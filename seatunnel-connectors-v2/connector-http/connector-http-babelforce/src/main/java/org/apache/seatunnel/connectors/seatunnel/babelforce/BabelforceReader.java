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
package org.apache.seatunnel.connectors.seatunnel.babelforce;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Acquires report pages asynchronously and publishes them at precise checkpoint boundaries.
 *
 * <p>The output lock guards publication and advancement together. Job cancellation uses a separate
 * transport lock and never waits for the collector.
 */
final class BabelforceReader implements SourceReader<SeaTunnelRow, BabelforceCheckpoint> {
    private static final int ROWS_PER_POLL = 32;
    private final Context context;
    private final BabelforceOptions options;
    private final String table;
    private final Object stateLock = new Object();
    private final Object transportLock = new Object();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "babelforce-simple-report");
                        thread.setDaemon(true);
                        return thread;
                    });

    private BabelforceCheckpoint position;
    private BabelforceTransport client;
    // Keep the fully validated page until its last row, even when one poll emits a prefix.
    private volatile Future<BabelforcePage> pending;
    private volatile boolean cancelled;
    private boolean ended;
    private long expires;

    BabelforceReader(Context context, BabelforceOptions options, String table) {
        this.context = context;
        this.options = options;
        this.table = table;
    }

    /**
     * Opens without using provider credentials before the reader has received a split.
     *
     * <p>Actual acquisition starts at the first read of the assigned page.
     */
    @Override
    public void open() {}

    /**
     * Schedules or publishes the next response without blocking the engine on HTTPS.
     *
     * <p>The current poll produces a bounded number of rows before checkpoint work can proceed.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (ended || cancelled) {
            return;
        }
        final BabelforceCheckpoint current;
        synchronized (stateLock) {
            current = position;
        }
        if (current == null) {
            Thread.sleep(10L);
            return;
        }
        if (current.done) {
            ended = true;
            context.signalNoMoreElement();
            return;
        }
        Future<BabelforcePage> task = pending;
        if (task == null) {
            schedule(current);
        } else if (task.isDone()) {
            BabelforcePage page = awaitPage(task, current);
            if (page != null) {
                publish(current, page, output);
            }
        } else {
            if (System.nanoTime() - expires >= 0L) {
                cancel();
                throw new BabelforceException(current.page, "request deadline exceeded");
            }
            Thread.sleep(10L);
        }
    }

    private void schedule(BabelforceCheckpoint current) throws BabelforceException {
        if (current.page > options.maxPages) {
            throw new BabelforceException(current.page, "max_pages exhausted");
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.timeout);
        expires = deadline;
        try {
            pending =
                    worker.submit(
                            () ->
                                    BabelforcePage.read(
                                            client(current).get(current, deadline),
                                            options,
                                            current,
                                            table,
                                            deadline));
        } catch (RejectedExecutionException e) {
            if (!cancelled) {
                throw new BabelforceException(
                        current.page, "could not schedule report acquisition");
            }
        }
    }

    private BabelforceTransport client(BabelforceCheckpoint current) throws BabelforceException {
        synchronized (transportLock) {
            if (cancelled) {
                throw new BabelforceException(current.page, "reader was cancelled");
            }
            if (client != null) {
                return client;
            }
        }
        BabelforceTransport created = new BabelforceTransport(options, current);
        synchronized (transportLock) {
            if (cancelled) {
                created.close();
                throw new BabelforceException(current.page, "reader was cancelled");
            }
            client = created;
            return client;
        }
    }

    private BabelforcePage awaitPage(Future<BabelforcePage> task, BabelforceCheckpoint current)
            throws Exception {
        try {
            return task.get();
        } catch (CancellationException stopped) {
            if (cancelled) {
                return null;
            }
            throw new BabelforceException(current.page, "request was cancelled");
        } catch (ExecutionException failed) {
            if (cancelled) {
                return null;
            }
            if (failed.getCause() instanceof BabelforceException) {
                throw (BabelforceException) failed.getCause();
            }
            throw new BabelforceException(current.page, "report acquisition failed");
        }
    }

    private void publish(
            BabelforceCheckpoint current, BabelforcePage page, Collector<SeaTunnelRow> output)
            throws BabelforceException {
        if (current.emitted > page.rows.size()) {
            throw new BabelforceException(
                    current.page, "checkpoint row offset exceeds the response");
        }
        int end = Math.min(current.emitted + ROWS_PER_POLL, page.rows.size());
        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                if (cancelled) {
                    return;
                }
                for (int i = current.emitted; i < end; i++) {
                    output.collect(page.rows.get(i));
                }
                if (end == page.rows.size()) {
                    position = page.next;
                    pending = null;
                } else {
                    position = current.partial(page.digest, end);
                }
            }
        }
    }

    /**
     * Finishes checkpoint capture with an immutable page position rather than the call payload.
     *
     * <p>The page's digest is sufficient to check an exact resume after partial delivery.
     */
    @Override
    public List<BabelforceCheckpoint> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return position == null ? List.of() : List.of(position);
        }
    }

    /**
     * Restores a single fixed-window scan after checking the endpoint, account and date scope.
     *
     * <p>Changing those settings requires a separate scan.
     */
    @Override
    public void addSplits(List<BabelforceCheckpoint> splits) {
        if (splits.isEmpty()) {
            return;
        }
        synchronized (stateLock) {
            if (splits.size() != 1 || position != null) {
                throw BabelforceOptions.invalid("checkpoint", "expected one calls split");
            }
            if (!options.fingerprint.equals(splits.get(0).fingerprint)) {
                throw BabelforceOptions.invalid("checkpoint", "read scope changed");
            }
            position = splits.get(0);
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    /**
     * Interrupts retry delay or response consumption without taking the collector lock.
     *
     * <p>Early cancellation stays active through later open and split callbacks.
     */
    @Override
    public void cancel() {
        cancelled = true;
        Future<BabelforcePage> task = pending;
        if (task != null) {
            task.cancel(true);
        }
        synchronized (transportLock) {
            if (client != null) {
                client.close();
            }
        }
        worker.shutdownNow();
    }

    /**
     * Releases the transport through the same cancellation used by asynchronous engine stop.
     *
     * <p>Closing a reader that never acquired a client remains safe.
     */
    @Override
    public void close() {
        cancel();
    }
}
