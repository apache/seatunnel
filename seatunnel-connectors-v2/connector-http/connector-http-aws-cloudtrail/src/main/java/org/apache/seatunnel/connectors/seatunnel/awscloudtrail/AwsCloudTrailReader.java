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

package org.apache.seatunnel.connectors.seatunnel.awscloudtrail;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailConfig.Window;
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailJson.Event;
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailJson.Page;
import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailSource.Cursor;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reads one validated CloudTrail event page at a time with asynchronous, cancelable HTTPS I/O.
 * Emission and checkpoint position updates have one lock order.
 */
final class AwsCloudTrailReader implements SourceReader<SeaTunnelRow, Cursor> {
    private static final int EMIT_PER_POLL = 16;
    private final AwsCloudTrailConfig config;
    private final Context context;
    private final String table;
    private final AwsCloudTrailHttp http;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<String> tokenHashes = new LinkedHashSet<>();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-cloudtrail-https");
                        thread.setDaemon(true);
                        return thread;
                    });

    private Window window;
    private String token;
    private int completedPages;
    private int offset;
    private String digest;
    private BigDecimal preceding;
    private boolean finished;
    private boolean eof;
    private Page page;
    private long deadline;
    private volatile Future<Page> pending;

    AwsCloudTrailReader(AwsCloudTrailConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        this.http = new AwsCloudTrailHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Checks only a completed retrieval future while holding the checkpoint lock. At most a small
     * page segment is emitted per poll.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            synchronized (progress) {
                if (closed.get() || window == null || eof) {
                    return;
                }
                if (finished) {
                    eof = true;
                    context.signalNoMoreElement();
                    return;
                }
                if (page != null || prepare()) {
                    emit(output);
                }
            }
        }
    }

    private boolean prepare() throws IOException {
        Future<Page> task = pending;
        if (task == null) {
            Window query = window;
            String continuation = token;
            BigDecimal oldest = preceding;
            long expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.requestTimeout);
            deadline = expires;
            try {
                pending = worker.submit(() -> http.fetch(query, continuation, oldest, expires));
            } catch (RejectedExecutionException ignored) {
                if (!closed.get()) {
                    throw AwsCloudTrailJson.failure("retrieval worker is unavailable");
                }
            }
            return false;
        }
        if (!task.isDone()) {
            if (System.nanoTime() - deadline >= 0) {
                close();
                throw AwsCloudTrailJson.failure("request deadline exceeded");
            }
            return false;
        }
        try {
            Page incoming = task.get();
            if (closed.get()) {
                return false;
            }
            verifyRestored(offset, digest, incoming);
            if (incoming.next != null) {
                if (tokenHashes.contains(AwsCloudTrailJson.identity(incoming.next))) {
                    throw AwsCloudTrailJson.failure("repeated continuation token");
                }
                if (completedPages + 1 >= config.maxPages) {
                    throw AwsCloudTrailJson.failure("lookup requires more than max_pages");
                }
            }
            page = incoming;
            pending = null;
            return true;
        } catch (CancellationException ignored) {
            if (!closed.get()) {
                throw AwsCloudTrailJson.failure("request canceled");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closed.get()) {
                throw AwsCloudTrailJson.failure("request interrupted");
            }
        } catch (ExecutionException e) {
            if (!closed.get()) {
                if (e.getCause() instanceof Failure) {
                    throw (Failure) e.getCause();
                }
                throw AwsCloudTrailJson.failure("retrieval worker failed");
            }
        }
        return false;
    }

    /**
     * Reuses an in-page offset only when every event and the next token still match. Timestamp
     * equality is allowed and does not replace checkpoint progress.
     */
    static void verifyRestored(int offset, String digest, Page received) throws IOException {
        if (offset > 0 && (offset >= received.events.size() || !received.digest.equals(digest))) {
            throw AwsCloudTrailJson.failure("the event page changed since its checkpoint");
        }
    }

    private void emit(Collector<SeaTunnelRow> output) {
        int stop = Math.min(page.events.size(), offset + EMIT_PER_POLL);
        for (int i = offset; i < stop; i++) {
            if (closed.get()) {
                return;
            }
            Event event = page.events.get(i);
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                AwsCloudTrailConfig.RESOURCE,
                                config.region,
                                event.id,
                                event.time,
                                event.audit,
                                event.json
                            });
            row.setTableId(table);
            output.collect(row);
        }
        if (stop < page.events.size()) {
            offset = stop;
            digest = page.digest;
        } else {
            completedPages++;
            preceding = page.oldest;
            token = page.next;
            if (token == null) {
                finished = true;
            } else {
                tokenHashes.add(AwsCloudTrailJson.identity(token));
            }
            page = null;
            offset = 0;
            digest = null;
        }
    }

    /**
     * Stores the position only after rows at that position were emitted. Ready and in-flight HTTP
     * responses are fetched again if their rows were never emitted.
     */
    @Override
    public List<Cursor> snapshotState(long checkpointId) {
        synchronized (progress) {
            if (window == null) {
                return List.of();
            }
            return List.of(
                    new Cursor(
                            config.identity,
                            window,
                            token,
                            new ArrayList<>(tokenHashes),
                            completedPages,
                            offset,
                            digest,
                            preceding,
                            finished));
        }
    }

    @Override
    public void addSplits(List<Cursor> splits) {
        synchronized (progress) {
            if (window != null || splits.size() != 1) {
                throw AwsCloudTrailConfig.invalid("expected one assigned lookup scan");
            }
            Cursor state = splits.get(0);
            if (!config.identity.equals(state.identity)) {
                throw AwsCloudTrailConfig.invalid(
                        "lookup account, region or parameters changed during restore");
            }
            validateCheckpoint(state, config.maxPages, config.pageSize);
            window = state.window;
            token = state.token;
            tokenHashes.addAll(state.tokenHashes);
            completedPages = state.completedPages;
            offset = state.offset;
            digest = state.digest;
            preceding = state.preceding;
            finished = state.finished;
        }
    }

    private static void validateCheckpoint(Cursor cursor, int maxPages, int maxEvents) {
        if (cursor.window == null
                || cursor.window.start > cursor.window.end
                || cursor.completedPages < 0
                || cursor.completedPages > maxPages
                || cursor.offset < 0
                || cursor.offset >= maxEvents
                || ((cursor.offset == 0) != (cursor.digest == null))
                || cursor.finished
                        && (cursor.token != null
                                || cursor.offset != 0
                                || cursor.completedPages == 0)
                || !cursor.finished && cursor.completedPages >= maxPages
                || !cursor.finished && ((cursor.completedPages > 0) != (cursor.token != null))
                || cursor.tokenHashes.size() != cursor.completedPages - (cursor.finished ? 1 : 0)
                || new HashSet<>(cursor.tokenHashes).size() != cursor.tokenHashes.size()
                || (cursor.token != null
                        && (!AwsCloudTrailJson.validToken(cursor.token)
                                || !cursor.tokenHashes.contains(
                                        AwsCloudTrailJson.identity(cursor.token))))) {
            throw AwsCloudTrailConfig.invalid("invalid lookup checkpoint position");
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void cancel() {
        close();
    }

    /**
     * Cancels the socket and deadline wait without acquiring the checkpoint lock. A JVM resolver
     * can finish DNS later on the daemon, which cannot emit any rows.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<Page> running = pending;
            if (running != null) {
                running.cancel(true);
            }
            worker.shutdownNow();
        }
    }
}
