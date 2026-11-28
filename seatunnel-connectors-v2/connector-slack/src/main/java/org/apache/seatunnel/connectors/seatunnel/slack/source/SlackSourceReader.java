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

package org.apache.seatunnel.connectors.seatunnel.slack.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultManagedHttpClientConnection;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Fetches bounded Slack history on one owned HTTP worker. Only the polling thread emits complete
 * pages and commits checkpoint progress.
 */
public class SlackSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    // A poll must yield to the engine's graceful-close state machine even while HTTP is blocked.
    private static final long POLL_WAIT_MILLIS = 50;
    private static final int CLOSE_WAIT_SECONDS = 5;

    private static final Set<String> SAFE_ERROR_CODES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    "invalid_auth",
                                    "not_authed",
                                    "token_revoked",
                                    "account_inactive",
                                    "missing_scope",
                                    "not_in_channel",
                                    "channel_not_found",
                                    "invalid_cursor",
                                    "ratelimited")));

    private final SlackSourceConfig config;
    private final SingleSplitReaderContext context;
    private final ObjectMapper mapper =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    /**
     * Protects committed progress only. The sole polling thread acquires the collector checkpoint
     * lock before this lock; snapshots take only this lock. HTTP never holds either lock.
     */
    private final Object stateLock = new Object();

    /**
     * Protects worker/request ownership and retry waits. Never nest it with the checkpoint/state
     * locks: close can abort HTTP and wake backoff without waiting for downstream collection.
     */
    private final Object lifecycleLock = new Object();

    /**
     * Digests of continuations already accepted in this channel; captured in state, retained on
     * restore, and cleared only when committing advancement to the next channel.
     */
    private final Set<String> seenCursors = new HashSet<>();

    // Sticky across open/poll/close; the HTTP worker also checks it while reading and retrying.
    private volatile boolean closed;
    // Published under lifecycleLock so close can abort an active request from another thread.
    private HttpGet activeRequest;
    private CloseableHttpClient client;
    private RequestConfig requestConfig;
    private ScheduledThreadPoolExecutor deadlines;
    private ExecutorService httpWorker;

    /**
     * At most one page is pending, guarded by lifecycleLock. Only pollNext submits/consumes it;
     * close cancels it. The worker publishes a validated Page through Future, never rows or state.
     */
    private Future<Page> pendingPage;

    // Committed progress belongs to the polling thread under stateLock; restore runs before
    // polling.
    private int channelIndex;
    private int pages;
    private String cursor = "";
    private String latest;

    /**
     * Captures the frozen source window; open owns creation of the worker, client and deadline
     * timer.
     */
    public SlackSourceReader(SlackSourceConfig config, SingleSplitReaderContext context) {
        this.config = config;
        this.context = context;
        latest = config.latest;
    }

    /**
     * Creates the owned HTTP worker, client and deadline timer once; a prior close stays sticky.
     */
    @Override
    public void open() {
        synchronized (lifecycleLock) {
            if (closed || client != null) {
                return;
            }
            requestConfig =
                    RequestConfig.custom()
                            .setConnectTimeout(config.timeout)
                            .setSocketTimeout(config.timeout)
                            .setConnectionRequestTimeout(config.timeout)
                            .build();
            // Keep token/cursor/message data out of HTTP header, wire and request-line DEBUG.
            // The minimal chain retains TLS and abortable I/O without implicit redirects/retries.
            PoolingHttpClientConnectionManager connections =
                    new PoolingHttpClientConnectionManager(
                            (route, connectionConfig) ->
                                    new DefaultManagedHttpClientConnection(
                                            "slack-source",
                                            connectionConfig.getBufferSize(),
                                            connectionConfig.getFragmentSizeHint(),
                                            null,
                                            null,
                                            connectionConfig.getMessageConstraints(),
                                            null,
                                            null,
                                            null,
                                            null));
            connections.setMaxTotal(1);
            connections.setDefaultMaxPerRoute(1);
            connections.setDefaultConnectionConfig(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(100)
                                            .setMaxLineLength(16 * 1024)
                                            .build())
                            .build());
            client = HttpClients.createMinimal(connections);
            deadlines =
                    new ScheduledThreadPoolExecutor(
                            1,
                            runnable -> {
                                Thread thread = new Thread(runnable, "slack-source-deadline");
                                thread.setDaemon(true);
                                return thread;
                            });
            deadlines.setRemoveOnCancelPolicy(true);
            httpWorker =
                    Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread = new Thread(runnable, "slack-source-http");
                                thread.setDaemon(true);
                                return thread;
                            });
        }
    }

    /**
     * Waits briefly for one owned HTTP task, then yields even if a response or retry wait is
     * blocked. Only this polling thread may emit a completed page and commit its continuation
     * together. Completion callbacks run after releasing the checkpoint/state locks.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        String channel;
        String requestCursor;
        String requestLatest;
        synchronized (stateLock) {
            if (closed || noMoreSplits) {
                return;
            }
            if (channelIndex == config.channels.size()) {
                noMoreSplits = true;
                channel = null;
                requestCursor = null;
                requestLatest = null;
            } else {
                if (pages >= config.maxPages) {
                    throw failure("max_pages_per_channel exceeded");
                }
                channel = config.channels.get(channelIndex);
                requestCursor = cursor;
                requestLatest = latest;
            }
        }
        if (channel == null) {
            context.signalNoMoreElement();
            return;
        }
        Future<Page> pending;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            if (pendingPage == null) {
                pendingPage =
                        httpWorker.submit(() -> readPage(channel, requestCursor, requestLatest));
            }
            pending = pendingPage;
        }
        Page page;
        try {
            page = pending.get(POLL_WAIT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // Returning allows a successful savepoint stop to reach the engine's close hook.
            return;
        } catch (CancellationException e) {
            if (closed) {
                return;
            }
            throw failure("HTTP page task canceled");
        } catch (ExecutionException e) {
            if (closed) {
                return;
            }
            Throwable cause = e.getCause();
            // Only our own sanitized protocol errors may cross the worker boundary unchanged.
            if (cause instanceof SlackSourceException) {
                throw (SlackSourceException) cause;
            }
            throw failure("HTTP page task failed");
        }
        if (closed) {
            return;
        }
        boolean finished;
        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                if (closed) {
                    return;
                }
                String digest = page.nextCursor.isEmpty() ? null : digest(page.nextCursor);
                if (digest != null
                        && (page.nextCursor.equals(cursor) || seenCursors.contains(digest))) {
                    throw failure("non-progressing pagination cursor");
                }
                for (SeaTunnelRow row : page.rows) {
                    output.collect(row);
                }
                pages++;
                cursor = page.nextCursor;
                if (cursor.isEmpty()) {
                    channelIndex++;
                    pages = 0;
                    seenCursors.clear();
                } else {
                    seenCursors.add(digest);
                }
                finished = channelIndex == config.channels.size();
                noMoreSplits = finished;
            }
        }
        synchronized (lifecycleLock) {
            if (pendingPage == pending) {
                pendingPage = null;
            }
        }
        if (finished && !closed) {
            context.signalNoMoreElement();
        }
    }

    /**
     * Runs only on the owned HTTP worker with an immutable channel/cursor/window. Retries the same
     * URI within finite attempt/deadline/backoff limits and never emits rows or advances committed
     * state. lifecycleLock is used only to publish an abortable request or wait interruptibly
     * between tries. External transport/parser causes are excluded because they can contain
     * sensitive API data.
     */
    private Page readPage(String channel, String requestCursor, String requestLatest)
            throws Exception {
        URIBuilder uri =
                new URIBuilder(config.apiRoot + "conversations.history")
                        .addParameter("channel", channel)
                        .addParameter("limit", Integer.toString(config.pageSize))
                        .addParameter("latest", requestLatest)
                        .addParameter("inclusive", "false");
        if (config.oldest != null) {
            uri.addParameter("oldest", config.oldest);
        }
        if (!requestCursor.isEmpty()) {
            uri.addParameter("cursor", requestCursor);
        }
        for (int attempt = 0; attempt <= config.retries; attempt++) {
            if (closed) {
                throw new InterruptedIOException("Slack source closed");
            }
            HttpGet request = new HttpGet(uri.build());
            request.setConfig(requestConfig);
            request.setHeader("Authorization", "Bearer " + config.token);
            request.setHeader("Accept", "application/json");
            request.setHeader("Accept-Encoding", "identity");
            ScheduledFuture<?> deadline;
            synchronized (lifecycleLock) {
                if (closed) {
                    throw new InterruptedIOException("Slack source closed");
                }
                activeRequest = request;
                deadline =
                        deadlines.schedule(request::abort, config.timeout, TimeUnit.MILLISECONDS);
            }
            long delay = config.backoff;
            try (CloseableHttpResponse response = client.execute(request)) {
                int status = response.getStatusLine().getStatusCode();
                if (status == 429 || status >= 500 && status <= 599) {
                    if (attempt == config.retries) {
                        throw failure("HTTP retry budget exhausted (status " + status + ")");
                    }
                    Header retryAfter = response.getFirstHeader("Retry-After");
                    if (retryAfter != null) {
                        delay = retryDelay(retryAfter.getValue());
                    }
                } else if (status != 200) {
                    throw failure("permanent HTTP status " + status);
                } else {
                    if (response.getEntity() == null) {
                        throw failure("missing JSON response");
                    }
                    try (InputStream input = response.getEntity().getContent()) {
                        return parsePage(channel, readBounded(input));
                    }
                }
            } catch (IOException e) {
                if (closed) {
                    throw new InterruptedIOException("Slack source closed");
                }
                if (attempt == config.retries) {
                    throw failure("HTTP transport retry budget exhausted");
                }
            } finally {
                deadline.cancel(false);
                request.abort();
                synchronized (lifecycleLock) {
                    if (activeRequest == request) {
                        activeRequest = null;
                    }
                }
            }
            synchronized (lifecycleLock) {
                long remaining = TimeUnit.MILLISECONDS.toNanos(delay);
                long end = System.nanoTime() + remaining;
                while (!closed && remaining > 0) {
                    TimeUnit.NANOSECONDS.timedWait(lifecycleLock, remaining);
                    remaining = end - System.nanoTime();
                }
            }
        }
        throw failure("HTTP retry budget exhausted");
    }

    /**
     * Honors Slack's numeric Retry-After without retrying earlier when the requested wait is too
     * large: invalid or over-budget server instructions fail instead of being silently capped.
     */
    private long retryDelay(String value) {
        try {
            long seconds = Long.parseLong(value);
            if (seconds < 0 || seconds > config.maxDelay / 1000L) {
                throw failure("Retry-After exceeds max_retry_delay_ms");
            }
            return Math.max(config.backoff, seconds * 1000L);
        } catch (NumberFormatException e) {
            throw failure("invalid Retry-After");
        }
    }

    /**
     * Bounds uncompressed response memory before JSON parsing; abort/close releases a blocked read.
     */
    private byte[] readBounded(InputStream input) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(Math.min(config.maxBytes, 8192));
        byte[] buffer = new byte[8192];
        int read;
        while ((read = input.read(buffer)) != -1) {
            if (closed) {
                throw new InterruptedIOException("Slack source closed");
            }
            if (read > config.maxBytes - bytes.size()) {
                throw failure("max_response_bytes exceeded");
            }
            bytes.write(buffer, 0, read);
        }
        return bytes.toByteArray();
    }

    /**
     * Validates the entire response and every message before handing a page to the polling thread.
     * Slack HTTP-200 errors are failures; arbitrary API strings and parser causes stay out of
     * errors.
     */
    private Page parsePage(String channel, byte[] bytes) {
        JsonNode root;
        try {
            root =
                    mapper.reader()
                            .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                            .readTree(bytes);
        } catch (IOException e) {
            throw failure("malformed JSON response");
        }
        if (root == null || !root.isObject() || !root.path("ok").isBoolean()) {
            throw failure("missing boolean ok");
        }
        if (!root.get("ok").booleanValue()) {
            if (!root.path("error").isTextual() || root.path("error").textValue().isEmpty()) {
                throw failure("malformed Slack error response");
            }
            // Only known codes are exposed. An arbitrary response string can contain credentials.
            String code = root.get("error").textValue();
            String safe = SAFE_ERROR_CODES.contains(code) ? code : "unrecognized_error";
            throw failure("Slack API error " + safe);
        }
        if (!root.path("messages").isArray()) {
            throw failure("missing messages array");
        }
        if (root.get("messages").size() > config.pageSize) {
            throw failure("response exceeds page_size");
        }
        if (root.has("has_more") && !root.get("has_more").isBoolean()) {
            throw failure("invalid has_more");
        }
        String next = "";
        JsonNode metadata = root.get("response_metadata");
        if (metadata != null) {
            if (!metadata.isObject()) {
                throw failure("invalid response_metadata");
            }
            JsonNode continuation = metadata.get("next_cursor");
            if (continuation != null) {
                if (!continuation.isTextual()) {
                    throw failure("invalid next_cursor");
                }
                next = continuation.textValue();
                if (next.length() > 4096 || !next.isEmpty() && next.trim().isEmpty()) {
                    throw failure("invalid next_cursor");
                }
            }
        }
        if (root.path("has_more").asBoolean(false) && next.isEmpty()) {
            throw failure("has_more without next_cursor");
        }
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (JsonNode message : root.get("messages")) {
            if (!message.isObject() || !message.path("ts").isTextual()) {
                throw failure("message missing string ts");
            }
            String ts = message.get("ts").textValue();
            try {
                SlackSourceConfig.validateTimestamp(ts, "message ts");
            } catch (IllegalArgumentException e) {
                throw failure("invalid message ts");
            }
            rows.add(
                    new SeaTunnelRow(
                            new Object[] {
                                channel,
                                ts,
                                optionalString(message, "thread_ts"),
                                optionalString(message, "user"),
                                optionalString(message, "text"),
                                optionalString(message, "type"),
                                optionalString(message, "subtype"),
                                message.toString()
                            }));
        }
        return new Page(rows, next);
    }

    private String optionalString(JsonNode message, String field) {
        JsonNode value = message.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isTextual()) {
            throw failure("invalid message field " + field);
        }
        return value.textValue();
    }

    /**
     * State is immutable bytes, atomically captured with the last emitted page. A fetched pending
     * page is excluded until the polling thread emits it and commits its continuation.
     */
    @Override
    protected byte[] snapshotStateToBytes(long checkpointId) throws Exception {
        synchronized (stateLock) {
            ObjectNode state = mapper.createObjectNode();
            state.put("version", 1)
                    .put("channel", channelIndex)
                    .put("pages", pages)
                    .put("cursor", cursor)
                    .put("latest", latest);
            state.put("oldest", config.oldest);
            state.set("channels", mapper.valueToTree(config.channels));
            ArrayNode seen = state.putArray("seen");
            seenCursors.forEach(seen::add);
            return mapper.writeValueAsBytes(state);
        }
    }

    /**
     * Restores committed progress before polling starts. Pages fetched after the checkpoint are
     * refetched; downstream idempotence is required for pages emitted after that checkpoint.
     */
    @Override
    protected void restoreState(byte[] restoredState) {
        synchronized (stateLock) {
            try {
                JsonNode state = mapper.readTree(restoredState);
                if (state == null
                        || !state.isObject()
                        || !state.path("version").isInt()
                        || state.path("version").intValue() != 1
                        || !state.path("channel").isInt()
                        || !state.path("pages").isInt()
                        || !state.path("cursor").isTextual()
                        || !state.path("latest").isTextual()
                        || !state.path("seen").isArray()) {
                    throw failure("invalid checkpoint state");
                }
                if (!mapper.valueToTree(config.channels).equals(state.get("channels"))
                        || !Objects.equals(
                                config.oldest,
                                state.path("oldest").isNull()
                                        ? null
                                        : state.path("oldest").asText())) {
                    throw failure("checkpoint conversation/window settings changed");
                }
                int restoredChannel = state.get("channel").intValue();
                int restoredPages = state.get("pages").intValue();
                String restoredCursor = state.get("cursor").textValue();
                String restoredLatest = state.get("latest").textValue();
                SlackSourceConfig.validateTimestamp(restoredLatest, "checkpoint latest");
                if (restoredChannel < 0
                        || restoredChannel > config.channels.size()
                        || restoredPages < 0
                        || restoredPages > config.maxPages
                        || restoredCursor.length() > 4096
                        || state.get("seen").size() > config.maxPages) {
                    throw failure("invalid checkpoint bounds");
                }
                seenCursors.clear();
                for (JsonNode seen : state.get("seen")) {
                    if (!seen.isTextual() || !seen.textValue().matches("[0-9a-f]{64}")) {
                        throw failure("invalid checkpoint cursor digest");
                    }
                    seenCursors.add(seen.textValue());
                }
                channelIndex = restoredChannel;
                pages = restoredPages;
                cursor = restoredCursor;
                latest = restoredLatest;
                noMoreSplits = false;
            } catch (IOException | IllegalArgumentException e) {
                throw failure("invalid checkpoint state");
            }
        }
    }

    private static String digest(String cursor) throws Exception {
        byte[] digest =
                MessageDigest.getInstance("SHA-256")
                        .digest(cursor.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder(64);
        for (byte b : digest) {
            hex.append(Character.forDigit((b >> 4) & 15, 16))
                    .append(Character.forDigit(b & 15, 16));
        }
        return hex.toString();
    }

    private SlackSourceException failure(String reason) {
        return new SlackSourceException(
                "Slack conversations.history: " + reason + " (channel index " + channelIndex + ")");
    }

    /**
     * Cancellation shares normal close cleanup and aborts blocked HTTP or retry waits. It must
     * never acquire the checkpoint or committed-state locks.
     */
    @Override
    public void cancel() {
        try {
            close();
        } catch (IOException e) {
            throw failure("HTTP resource cleanup failed");
        }
    }

    /**
     * Owns cleanup for both graceful stop and external cancel. Abort/cancel/shutdown are requested
     * under lifecycleLock; client close and bounded termination waits happen outside every lock.
     */
    @Override
    public void close() throws IOException {
        synchronized (lifecycleLock) {
            closed = true;
            if (activeRequest != null) {
                activeRequest.abort();
            }
            if (pendingPage != null) {
                pendingPage.cancel(true);
                pendingPage = null;
            }
            if (httpWorker != null) {
                httpWorker.shutdownNow();
            }
            if (deadlines != null) {
                deadlines.shutdownNow();
            }
            lifecycleLock.notifyAll();
        }
        IOException cleanupFailure = null;
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                cleanupFailure = new IOException("Slack source HTTP client close failed");
            }
        }
        for (ExecutorService executor : Arrays.asList(httpWorker, deadlines)) {
            if (executor != null) {
                try {
                    if (!executor.awaitTermination(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS)) {
                        cleanupFailure =
                                new IOException("Slack source HTTP resources did not terminate");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    cleanupFailure = new IOException("Slack source HTTP cleanup interrupted");
                }
            }
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    /**
     * Marks connector-owned sanitized failures so the polling thread can rethrow them without
     * exposing arbitrary client/parser exception messages from the HTTP worker.
     */
    private static final class SlackSourceException extends IllegalStateException {
        private SlackSourceException(String message) {
            super(message);
        }
    }

    /**
     * Whole-page handoff from the HTTP worker to the polling thread. Its final fields and Future
     * publication expose validated rows without letting the worker emit or commit progress.
     */
    private static final class Page {
        private final List<SeaTunnelRow> rows;
        private final String nextCursor;

        private Page(List<SeaTunnelRow> rows, String nextCursor) {
            this.rows = Collections.unmodifiableList(rows);
            this.nextCursor = nextCursor;
        }
    }
}
