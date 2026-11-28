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
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
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

/**
 * Owns one bounded table scan with cancellable page I/O and exact accepted-prefix recovery. Each
 * poll returns after one row so the engine can hand the checkpoint lock to waiting injectors.
 */
@Slf4j
final class FeishuSourceReader implements SourceReader<SeaTunnelRow, SingleSplit> {

    private static final int CHECKPOINT_VERSION = 2;

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
    // Only polling owns the decoded tail; count/hash, never rows, enter durable Progress.
    private PreparedPage prepared;

    FeishuSourceReader(FeishuSourceOptions config, Context context) {
        this.config = config;
        this.context = context;
        checkpointScope = digest(config.recordsUri.toASCIIString() + "\n" + config.appId);
    }

    /**
     * Creates the reader-owned client and worker without network I/O. A prior sticky close cannot
     * be reversed by a later open, and only polling submits work to these owned resources.
     */
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

    /**
     * Fetches and validates one page without engine locks, then accepts at most one row per poll.
     * Each accepted prefix shares collection's checkpoint boundary before returning to handoff.
     */
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

        if (prepared == null) {
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
            int count;
            String prefix;
            Set<String> seen;
            synchronized (progressLock) {
                count = current.acceptedCount;
                prefix = current.acceptedHash;
                seen = new HashSet<>(current.seen);
            }
            String nextDigest = page.nextCursor == null ? null : digest(page.nextCursor);
            if (nextDigest != null && seen.contains(nextDigest)) {
                throw failure("Bitable pagination repeated a cursor");
            }
            if (nextDigest != null && seen.size() + 1 >= config.maxPages) {
                throw failure("Bitable scan exceeded max_pages");
            }
            prepared = prepare(page, nextDigest, count, prefix);
            synchronized (lifecycle) {
                if (pending == task) {
                    pending = null;
                }
            }
        }
        synchronized (output.getCheckpointLock()) {
            synchronized (progressLock) {
                if (closed) {
                    return;
                }
                PreparedPage page = prepared;
                if (page.index < page.page.rows.size()) {
                    output.collect(page.page.rows.get(page.index));
                    current.acceptedHash = page.prefixes.get(page.index++);
                    current.acceptedCount = page.index;
                }
                if (page.index == page.page.rows.size()) {
                    // Last-row acceptance, next cursor/terminal state and reset are indivisible.
                    if (page.nextDigest != null) {
                        current.seen.add(page.nextDigest);
                    }
                    current.nextCursor = page.page.nextCursor;
                    current.finished = page.page.nextCursor == null;
                    current.acceptedCount = 0;
                    current.acceptedHash = null;
                    prepared = null;
                }
            }
        }
        if (current.finished && !closed) {
            signalEnd();
        }
    }

    /**
     * Persists the current page cursor and exact accepted output prefix without response records.
     * The final row advances continuation and resets the prefix under the same checkpoint boundary.
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
                output.writeInt(current.acceptedCount);
                output.writeUTF(current.acceptedHash == null ? "" : current.acceptedHash);
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

    /**
     * Interrupts retry backoff and aborts the active request without the collector/progress locks.
     * Worker and client ownership remain independent of a potentially blocked downstream collect.
     */
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

    private void signalEnd() {
        endSignalled = true;
        context.signalNoMoreElement();
    }

    /**
     * Accepts only this unreleased version and source scope. The prefix is bounded by the existing
     * decoder row cap and verified against a fully refetched page before any restored tail output.
     */
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
            restored.acceptedCount = input.readInt();
            String prefix = input.readUTF();
            restored.acceptedHash = prefix.isEmpty() ? null : prefix;
            if (restored.acceptedCount < 0
                    // decodePage already enforces this actual row cap before any collection.
                    || restored.acceptedCount > config.pageSize
                    || (restored.acceptedCount == 0 && restored.acceptedHash != null)
                    || (restored.acceptedCount > 0
                            && (restored.acceptedHash == null
                                    || !restored.acceptedHash.matches("[A-Za-z0-9_-]{43}")))
                    || (restored.finished && restored.acceptedCount != 0)
                    || input.available() != 0
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

    /**
     * Validates the entire saved output prefix before a recovered page can emit its tail. Exact
     * String framing includes nulls and UTF-16 code units without replacement-character aliases.
     */
    private PreparedPage prepare(
            FeishuApiClient.Page page, String nextDigest, int count, String saved)
            throws Exception {
        PreparedPage result = new PreparedPage(page, nextDigest, count);
        String previous = null;
        for (SeaTunnelRow row : page.rows) {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            if (previous != null) {
                hash.update(previous.getBytes(StandardCharsets.US_ASCII));
            }
            for (int column = 0; column < 4; column++) {
                hashString(hash, (String) row.getField(column));
            }
            previous = Base64.getUrlEncoder().withoutPadding().encodeToString(hash.digest());
            result.prefixes.add(previous);
        }
        if (count > page.rows.size()
                || (count == 0 && saved != null)
                || (count > 0 && !result.prefixes.get(count - 1).equals(saved))) {
            throw failure("checkpoint page prefix changed or is missing");
        }
        return result;
    }

    private static void hashString(MessageDigest hash, String value) {
        int length = value == null ? -1 : value.length();
        for (int shift = 24; shift >= 0; shift -= 8) {
            hash.update((byte) (length >>> shift));
        }
        if (value != null) {
            for (int i = 0; i < value.length(); i++) {
                char unit = value.charAt(i);
                hash.update((byte) (unit >>> 8));
                hash.update((byte) unit);
            }
        }
    }

    private static String digest(String value) {
        return Hashing.sha256().hashString(value, StandardCharsets.UTF_8).toString();
    }

    /**
     * Transient fully validated page and row digests survive only the current reader invocation.
     * The index is durable only through Progress after a successful checkpoint-coordinated collect.
     */
    private static final class PreparedPage {
        private final FeishuApiClient.Page page;
        private final String nextDigest;
        private final List<String> prefixes = new ArrayList<>();
        private int index;

        private PreparedPage(FeishuApiClient.Page page, String nextDigest, int index) {
            this.page = page;
            this.nextDigest = nextDigest;
            this.index = index;
        }
    }

    /**
     * Bounded durable page coordinates and accepted-prefix digest contain no record data or token.
     * Only polling updates them; snapshotting captures them under the independent progress monitor.
     */
    private static final class Progress {
        private final Set<String> seen = new HashSet<>();
        private String nextCursor;
        private boolean finished;
        private int acceptedCount;
        private String acceptedHash;
    }
}
