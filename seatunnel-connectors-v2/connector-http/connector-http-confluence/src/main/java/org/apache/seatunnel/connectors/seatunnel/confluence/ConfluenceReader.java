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

package org.apache.seatunnel.connectors.seatunnel.confluence;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Keeps remote I/O and retry waits outside the engine's source checkpoint lock.
 *
 * @see SourceReader
 */
final class ConfluenceReader implements SourceReader<SeaTunnelRow, ConfluenceSplit> {
    private static final int ROWS_PER_POLL = 16;
    private final ConfluenceConfig config;
    private final Context context;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final ConfluenceHttp http;
    private final Object splitStateLock = new Object();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "confluence-source-fetch");
                        thread.setDaemon(true);
                        return thread;
                    });

    // The poll thread owns request/page progress; only cancellation accesses the future
    // concurrently.
    private volatile Future<ConfluencePage> request;
    private ConfluencePage page;
    // splitStateLock protects the current page position and its committed ordered prefix.
    // Snapshots copy these values while emission also owns the collector checkpoint lock.
    private String position;
    private Set<String> visited;
    private int pages;
    private int emitted;
    private String prefix;
    // Assignment publishes restored progress before polling is allowed to submit any request.
    private volatile boolean assigned;
    private boolean finished;
    private boolean completionSignalled;
    // Retry timing is local to one page and intentionally restarts after checkpoint recovery.
    private int attempts;
    private long deadline;
    private long nextAttempt;

    ConfluenceReader(ConfluenceConfig config, Context context) {
        this.config = config;
        this.context = context;
        this.http = new ConfluenceHttp(config, cancelled);
        this.position = config.initialPosition();
        this.visited = new HashSet<>(ConfluenceSplit.initial(config).visited);
    }

    @Override
    public void open() {}

    /**
     * Yields while remote work is pending and commits at most one small batch under checkpoint
     * lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (cancelled.get() || !assigned) {
            return;
        }
        if (finished) {
            signalCompletion();
            return;
        }
        if (page == null && !loadPage()) {
            return;
        }
        synchronized (output.getCheckpointLock()) {
            synchronized (splitStateLock) {
                emitBatch(output);
            }
        }
        signalCompletion();
    }

    /**
     * Polls the worker without blocking checkpoint progress and validates any restored committed
     * prefix before attaching the decoded page to reader state.
     */
    private boolean loadPage() throws InterruptedException {
        long now = System.nanoTime();
        if (attempts > 0 && now - deadline >= 0) {
            http.abort();
            throw config.failure("page deadline exceeded");
        }
        Future<ConfluencePage> pending = request;
        if (pending == null) {
            if (attempts == 0 || now - nextAttempt >= 0) {
                submit(now);
            }
            return false;
        }
        if (!pending.isDone()) {
            return false;
        }
        request = null;
        try {
            ConfluencePage received = pending.get();
            if (received == null || cancelled.get()) {
                return false;
            }
            if (emitted > received.rows.size()
                    || (emitted > 0
                            && !Objects.equals(prefix, received.rows.get(emitted - 1).prefix))) {
                throw config.failure("restored page changed; start a new full refresh");
            }
            if (received.next != null) {
                if (pages + 1 >= config.maxPages) {
                    throw config.failure("scan exceeds max_pages");
                }
                if (config.resource.cursor()
                        && visited.contains(ConfluencePagination.fingerprint(received.next))) {
                    throw config.failure("cursor pagination cycle");
                }
            }
            page = received;
            return true;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ConfluenceHttp.Retry) {
                retry((ConfluenceHttp.Retry) cause);
                return false;
            }
            if (cause instanceof ConfluenceConfig.Failure) {
                throw (ConfluenceConfig.Failure) cause;
            }
            throw config.failure("page request failed (details withheld)");
        } catch (CancellationException e) {
            if (!cancelled.get()) {
                throw config.failure("page request was unexpectedly cancelled");
            }
            return false;
        }
    }

    /*
     * Starts one page attempt; all retries share the deadline established by the first attempt.
     */
    private void submit(long now) {
        if (attempts == 0) {
            deadline = now + TimeUnit.MILLISECONDS.toNanos(config.pageTimeout);
        }
        attempts++;
        String requested = position;
        try {
            request = worker.submit(() -> cancelled.get() ? null : http.fetch(requested));
        } catch (RejectedExecutionException e) {
            if (cancelled.get()) {
                return;
            }
            throw config.failure("fetch worker is unavailable");
        }
        if (cancelled.get()) {
            request.cancel(true);
        }
    }

    /*
     * Honors provider delay within the remaining page deadline and finite attempt budget.
     */
    private void retry(ConfluenceHttp.Retry retry) {
        if (cancelled.get()) {
            return;
        }
        if (attempts >= config.maxAttempts) {
            throw config.failure("request attempts exhausted after " + retry.getMessage());
        }
        long base = Math.min(10000L, 250L << (attempts - 1));
        long delay =
                Math.max(retry.delayMillis, base + ThreadLocalRandom.current().nextLong(base + 1));
        long now = System.nanoTime();
        if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - now)) {
            throw config.failure(
                    "page deadline cannot accommodate retry after " + retry.getMessage());
        }
        nextAttempt = now + TimeUnit.MILLISECONDS.toNanos(delay);
    }

    /**
     * Advances committed prefix only after collect succeeds, then advances the position at page
     * end. The caller owns both checkpoint and split-state locks throughout these transitions.
     */
    private void emitBatch(Collector<SeaTunnelRow> output) {
        int end = Math.min(emitted + ROWS_PER_POLL, page.rows.size());
        while (!cancelled.get() && emitted < end) {
            ConfluencePage.Row row = page.rows.get(emitted);
            output.collect(row.value);
            emitted++;
            prefix = row.prefix;
        }
        if (!cancelled.get() && emitted == page.rows.size()) {
            pages++;
            if (page.next == null) {
                finished = true;
            } else {
                position = page.next;
                if (config.resource.cursor()) {
                    visited.add(ConfluencePagination.fingerprint(position));
                }
            }
            emitted = 0;
            prefix = null;
            attempts = 0;
            page = null;
        }
    }

    /*
     * Signals terminal completion once, including when a finished checkpoint is restored.
     */
    private void signalCompletion() {
        if (finished && !completionSignalled && !cancelled.get()) {
            completionSignalled = true;
            context.signalNoMoreElement();
        }
    }

    /**
     * Copies secret-free progress, including a terminal or pre-assignment position, for engine
     * recovery.
     */
    @Override
    public List<ConfluenceSplit> snapshotState(long checkpointId) {
        // A pre-assignment barrier owns the initial position. On restore it can be handed back
        // to an enumerator whose original initial assignment had not reached the reader.
        synchronized (splitStateLock) {
            return Collections.singletonList(
                    new ConfluenceSplit(
                            config.binding, position, pages, emitted, prefix, finished, visited));
        }
    }

    /*
     * Installs exactly one validated initial or restored position before publishing assignment.
     */
    @Override
    public void addSplits(List<ConfluenceSplit> splits) {
        if (splits.isEmpty()) {
            return;
        }
        synchronized (splitStateLock) {
            if (assigned || splits.size() != 1) {
                throw config.failure("expected exactly one pagination split");
            }
            ConfluenceSplit split = splits.get(0);
            split.check(config);
            position = split.position;
            pages = split.pages;
            emitted = split.emitted;
            prefix = split.prefix;
            finished = split.finished;
            visited = new HashSet<>(split.visited);
            assigned = true;
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Closes active I/O as well as cancelling the future, so blocked socket reads can leave
     * promptly.
     */
    @Override
    public void cancel() {
        cancelled.set(true);
        http.abort();
        Future<ConfluencePage> pending = request;
        if (pending != null) {
            pending.cancel(true);
        }
    }

    /*
     * Cancels I/O and stops the reader-owned worker; no shared executor is affected.
     */
    @Override
    public void close() {
        cancel();
        worker.shutdownNow();
    }
}
