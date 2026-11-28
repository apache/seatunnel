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

package org.apache.seatunnel.connectors.seatunnel.feishu.source;

import org.apache.seatunnel.shade.com.google.common.hash.Hashing;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.feishu.source.FeishuApiClient.failure;

@Slf4j
final class FeishuSourceReader implements SourceReader<SeaTunnelRow, SingleSplit> {

    private static final int CHECKPOINT_VERSION = 1;

    private final FeishuSourceOptions config;
    private final Context context;
    private final String checkpointScope;
    private final Object lifecycle = new Object();
    private final Object progressLock = new Object();

    // Assignment publishes the state; only the polling thread commits and advances pages.
    private volatile Progress progress;
    private boolean endSignalled;
    private volatile boolean closed;
    private FeishuApiClient client;
    private ExecutorService executor;
    private Future<FeishuApiClient.Page> pending;
    private long deadline;

    FeishuSourceReader(FeishuSourceOptions config, Context context) {
        this.config = config;
        this.context = context;
        checkpointScope = digest(config.recordsUri.toASCIIString() + "\n" + config.appId);
    }

    /** The client and its worker are owned by this reader and created without network I/O. */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed) {
                client = new FeishuApiClient(config);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "feishu-source-io");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /** Submit or reap a single page without blocking the engine's poll or checkpoint lock. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        Progress current = progress;
        if (closed || current == null || endSignalled) {
            return;
        }
        if (current.finished) {
            signalEnd();
            return;
        }

        Future<FeishuApiClient.Page> task;
        synchronized (lifecycle) {
            if (closed) {
                return;
            }
            if (pending == null) {
                FeishuApiClient workerClient = client;
                String cursor = current.nextCursor;
                long pageDeadline =
                        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.pageTimeoutMs);
                deadline = pageDeadline;
                pending = executor.submit(() -> workerClient.fetchPage(cursor, pageDeadline));
                return;
            }
            task = pending;
        }

        if (!task.isDone()) {
            if (System.nanoTime() - deadline >= 0L && !closed) {
                cancel();
                throw failure("page deadline expired");
            }
            return;
        }

        FeishuApiClient.Page page;
        try {
            page = task.get();
        } catch (CancellationException | ExecutionException e) {
            if (closed) {
                return;
            }
            if (e.getCause() instanceof HttpConnectorException) {
                throw (HttpConnectorException) e.getCause();
            }
            throw failure("page worker failed");
        }

        String nextDigest = page.nextCursor == null ? null : digest(page.nextCursor);
        // The engine serializes its checkpoint barrier with this emission lock. The progress lock
        // also makes snapshots safe on runners that call snapshotState directly.
        synchronized (output.getCheckpointLock()) {
            if (closed) {
                return;
            }
            synchronized (progressLock) {
                if (nextDigest != null && current.seen.contains(nextDigest)) {
                    throw failure("Bitable pagination repeated a cursor");
                }
                if (nextDigest != null && current.seen.size() + 1 >= config.maxPages) {
                    throw failure("Bitable scan exceeded max_pages");
                }
                for (SeaTunnelRow row : page.rows) {
                    output.collect(row);
                }
                if (nextDigest != null) {
                    current.seen.add(nextDigest);
                }
                current.nextCursor = page.nextCursor;
                current.finished = page.nextCursor == null;
            }
        }

        synchronized (lifecycle) {
            pending = null;
        }
        if (current.finished && !closed) {
            signalEnd();
        }
    }

    private void signalEnd() {
        endSignalled = true;
        context.signalNoMoreElement();
    }

    /**
     * Persist only fully emitted pages, with an opaque cursor and bounded cycle detection state.
     */
    @Override
    public List<SingleSplit> snapshotState(long checkpointId) throws IOException {
        synchronized (progressLock) {
            Progress current = progress;
            if (current == null) {
                return Collections.emptyList();
            }
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(CHECKPOINT_VERSION);
                output.writeUTF(checkpointScope);
                output.writeBoolean(current.finished);
                output.writeUTF(current.nextCursor == null ? "" : current.nextCursor);
                output.writeInt(current.seen.size());
                for (String cursorDigest : current.seen) {
                    output.writeUTF(cursorDigest);
                }
            }
            return Collections.singletonList(new SingleSplit(bytes.toByteArray()));
        }
    }

    /**
     * Wait for coordinator assignment so a restored reader cannot start at the first page early.
     */
    @Override
    public void addSplits(List<SingleSplit> splits) {
        if (splits.size() != 1) {
            throw failure("expected exactly one Bitable split");
        }
        byte[] state = splits.get(0).getState();
        synchronized (progressLock) {
            if (progress != null) {
                if (state == null) {
                    return;
                }
                throw failure("received a second restored Bitable split");
            }
            progress = state == null ? new Progress() : restoreProgress(state);
        }
    }

    private Progress restoreProgress(byte[] state) {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(state))) {
            if (input.readInt() != CHECKPOINT_VERSION || !checkpointScope.equals(input.readUTF())) {
                throw failure("checkpoint does not match this Feishu source or state version");
            }
            Progress restored = new Progress();
            restored.finished = input.readBoolean();
            String cursor = input.readUTF();
            int seenCount = input.readInt();
            if (cursor.length() > FeishuApiClient.MAX_CURSOR_LENGTH
                    || (restored.finished && !cursor.isEmpty())
                    || seenCount < 0
                    || seenCount >= FeishuSourceOptions.MAX_SCAN_PAGES
                    || (!restored.finished && seenCount >= config.maxPages)) {
                throw failure("invalid Feishu checkpoint progress");
            }
            for (int i = 0; i < seenCount; i++) {
                String hash = input.readUTF();
                if (!hash.matches("[0-9a-f]{64}") || !restored.seen.add(hash)) {
                    throw failure("invalid Feishu checkpoint cursor history");
                }
            }
            if (input.available() != 0
                    || (!cursor.isEmpty() && !restored.seen.contains(digest(cursor)))
                    || (!restored.finished && seenCount > 0 && cursor.isEmpty())) {
                throw failure("inconsistent Feishu checkpoint cursor");
            }
            restored.nextCursor = cursor.isEmpty() ? null : cursor;
            return restored;
        } catch (IOException e) {
            throw failure("invalid Feishu checkpoint data");
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void cancel() {
        try {
            close();
        } catch (IOException e) {
            log.warn("Failed to close the Feishu source HTTP client");
        }
    }

    /** Interrupt retry backoff and abort any active HTTP request without the checkpoint lock. */
    @Override
    public void close() throws IOException {
        FeishuApiClient toClose;
        synchronized (lifecycle) {
            if (closed) {
                return;
            }
            closed = true;
            if (pending != null) {
                pending.cancel(true);
                pending = null;
            }
            if (executor != null) {
                executor.shutdownNow();
            }
            toClose = client;
        }
        if (toClose != null) {
            toClose.close();
        }
    }

    private static String digest(String value) {
        return Hashing.sha256().hashString(value, StandardCharsets.UTF_8).toString();
    }

    private static final class Progress {
        private final Set<String> seen = new HashSet<>();
        private String nextCursor;
        private boolean finished;
    }
}
