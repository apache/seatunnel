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

package org.apache.seatunnel.connectors.seatunnel.bunnyinc;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncJson.Page;
import org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncJson.Record;
import org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSource.Split;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Reads one bounded Bunny billing connection without blocking the engine checkpoint thread.
 *
 * <p>A partially emitted page is accepted on replay only if its records and continuation still
 * match. Bounded cursor digests also reject cycles longer than immediate repeats.
 */
final class BunnyIncReader implements SourceReader<SeaTunnelRow, Split> {
    private static final int ROWS_PER_POLL = 4;
    private static final Pattern DIGEST = Pattern.compile("[0-9a-f]{64}");

    private final BunnyIncConfig config;
    private final Context context;
    private final String table;
    private final BunnyIncHttp http;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<String> visited = new HashSet<>();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-bunnyinc-retrieval");
                        thread.setDaemon(true);
                        return thread;
                    });

    private boolean assigned;
    private volatile boolean noMoreSplits;
    private boolean finished;
    private boolean eof;
    private String after;
    private int row;
    private String pageDigest;
    private Page current;
    private volatile Future<Page> pending;

    BunnyIncReader(BunnyIncConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        this.http = new BunnyIncHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Publishes only ready records and advances the split under the collector checkpoint lock.
     *
     * @param collector the row collector and its checkpoint lock
     * @throws IOException if the provider response or saved cursor cannot be used
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> collector) throws IOException {
        synchronized (collector.getCheckpointLock()) {
            synchronized (progress) {
                if (closed.get() || eof) {
                    return;
                }
                if (finished || (!assigned && noMoreSplits)) {
                    eof = true;
                    context.signalNoMoreElement();
                    return;
                }
                if (!assigned) {
                    return;
                }
                try {
                    if (current != null || loadPage()) {
                        emit(collector);
                    }
                } catch (Failure e) {
                    if (!closed.get()) {
                        throw new IOException(
                                "BunnyInc resource " + config.resource.id + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Runs one immutable GraphQL request outside the engine checkpoint lock.
     *
     * <p>Validating the next cursor before returning any rows prevents repeating a corrupt page.
     */
    private boolean loadPage() throws Failure {
        Future<Page> task = pending;
        if (task == null) {
            if (visited.size() >= config.maxPages) {
                throw BunnyIncJson.failure("pagination reached max_pages before completion");
            }
            String continuation = after;
            try {
                pending =
                        worker.submit(
                                () ->
                                        BunnyIncJson.page(
                                                http.fetch(
                                                        BunnyIncJson.request(
                                                                config.resource,
                                                                config.pageSize,
                                                                continuation)),
                                                config.resource,
                                                config.pageSize));
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    throw BunnyIncJson.failure("retrieval worker is unavailable");
                }
            }
            return false;
        }
        if (!task.isDone()) {
            return false;
        }
        try {
            Page loaded = task.get();
            pending = null;
            if (closed.get()) {
                return false;
            }
            if (loaded.next != null && visited.contains(cursorDigest(loaded.next))) {
                throw BunnyIncJson.failure("GraphQL pagination returned a repeated cursor");
            }
            verifyReplay(row, pageDigest, loaded.digest, loaded.rows.size());
            current = loaded;
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw BunnyIncJson.failure("retrieval interrupted");
        } catch (CancellationException e) {
            throw BunnyIncJson.failure("retrieval canceled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Failure) {
                throw (Failure) e.getCause();
            }
            throw BunnyIncJson.failure("retrieval worker failed");
        }
    }

    private void emit(Collector<SeaTunnelRow> collector) {
        int end = Math.min(current.rows.size(), row + ROWS_PER_POLL);
        while (row < end && !closed.get()) {
            Record value = current.rows.get(row);
            SeaTunnelRow record = new SeaTunnelRow(new Object[] {value.id, value.json});
            record.setTableId(table);
            collector.collect(record);
            row++;
            pageDigest = current.digest;
        }
        if (row == current.rows.size()) {
            after = current.next;
            if (after == null) {
                finished = true;
                visited.clear();
            } else {
                visited.add(cursorDigest(after));
            }
            current = null;
            row = 0;
            pageDigest = null;
        }
    }

    static void verifyReplay(int offset, String saved, String actual, int available)
            throws Failure {
        if (saved != null && (offset >= available || !saved.equals(actual))) {
            throw BunnyIncJson.failure("saved GraphQL page changed since its checkpoint");
        }
    }

    private static String cursorDigest(String cursor) {
        return BunnyIncJson.fingerprint(List.of(cursor));
    }

    /**
     * Saves only the assigned source position and the page fingerprint.
     *
     * @param checkpointId the engine checkpoint identifier
     * @return current progress, or no state before split assignment
     */
    @Override
    public List<Split> snapshotState(long checkpointId) {
        synchronized (progress) {
            return assigned
                    ? List.of(
                            new Split(
                                    config.scope,
                                    after,
                                    List.copyOf(visited),
                                    row,
                                    pageDigest,
                                    finished))
                    : List.of();
        }
    }

    /**
     * Restores the ordered traversal using the engine's assigned split.
     *
     * @param splits one source split bound to the configured tenant and resource
     */
    @Override
    public void addSplits(List<Split> splits) {
        synchronized (progress) {
            if (assigned || eof || splits.size() != 1) {
                throw BunnyIncConfig.invalid("expected one assigned BunnyInc traversal");
            }
            Split saved = splits.get(0);
            validate(config, saved);
            after = saved.after;
            visited.addAll(saved.visited);
            row = saved.row;
            pageDigest = saved.pageDigest;
            finished = saved.finished;
            assigned = true;
        }
    }

    /**
     * Rejects malformed or incompatible split state before using a stored cursor.
     *
     * @param config the reconstructed source config
     * @param saved the checkpointed split
     */
    static void validate(BunnyIncConfig config, Split saved) {
        if (saved == null || !config.scope.equals(saved.scope)) {
            throw BunnyIncConfig.invalid(
                    "workspace, token, resource or page_size changed during restore");
        }
        if (saved.finished
                && (saved.after != null
                        || !saved.visited.isEmpty()
                        || saved.row != 0
                        || saved.pageDigest != null)) {
            throw BunnyIncConfig.invalid("terminal split contains an active GraphQL position");
        }
        if (saved.visited.size() > config.maxPages
                || (saved.after == null) != saved.visited.isEmpty()
                || (saved.after != null
                        && (!BunnyIncConfig.validCursor(saved.after)
                                || !saved.visited.contains(cursorDigest(saved.after))))
                || new HashSet<>(saved.visited).size() != saved.visited.size()
                || saved.visited.stream().anyMatch(value -> !DIGEST.matcher(value).matches())
                || saved.row < 0
                || saved.row >= config.pageSize
                || (saved.row == 0) != (saved.pageDigest == null)
                || (saved.pageDigest != null && !DIGEST.matcher(saved.pageDigest).matches())) {
            throw BunnyIncConfig.invalid("invalid saved GraphQL cursor or row position");
        }
    }

    /**
     * Records exhaustion without invoking a coordinator callback from the notification thread.
     *
     * <p>An unassigned restored reader announces EOF from its own poll loop.
     */
    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    /**
     * Interrupts the current socket and any pending retry regardless of reader progress.
     *
     * <p>Cancellation stays active even before the first split is assigned.
     */
    @Override
    public void cancel() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<Page> task = pending;
            if (task != null) {
                task.cancel(true);
            }
            worker.shutdownNow();
        }
    }

    /**
     * Shuts down the same resources as cancellation.
     *
     * <p>The engine may close the reader after it already canceled it.
     */
    @Override
    public void close() {
        cancel();
    }
}
