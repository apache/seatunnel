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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactoryBuilder;
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
import java.util.Base64;
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
 * Fetches bounded Slack history on one owned HTTP worker. Only the polling thread emits prepared
 * rows and commits accepted-prefix progress.
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
    // Checkpoint/settings owner only; each response copy owns independent parser-name tables.
    private final ObjectMapper mapper =
            new ObjectMapper(
                            new JsonFactoryBuilder()
                                    .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
                                    .build())
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
    // Poll-owned decoded tail; only its accepted count/digest are durable checkpoint fields.
    private PreparedPage preparedPage;

    // Committed progress belongs to the polling thread under stateLock; restore runs before
    // polling.
    private int channelIndex;
    private int pages;
    private String cursor = "";
    private String latest;
    private int acceptedCount;
    private String acceptedHash;

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
     * blocked. Complete-page validation is outside engine locks; one accepted row and its prefix
     * share each poll boundary. The final row advances continuation and completion atomically.
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
        if (preparedPage == null) {
            Future<Page> pending;
            synchronized (lifecycleLock) {
                if (closed) {
                    return;
                }
                if (pendingPage == null) {
                    pendingPage =
                            httpWorker.submit(
                                    () -> readPage(channel, requestCursor, requestLatest));
                }
                pending = pendingPage;
            }
            Page page;
            try {
                page = pending.get(POLL_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
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
                // Only sanitized connector errors cross the worker boundary unchanged.
                if (cause instanceof SlackSourceException) {
                    throw (SlackSourceException) cause;
                }
                throw failure("HTTP page task failed");
            }
            int count;
            String prefix;
            Set<String> seen;
            synchronized (stateLock) {
                count = acceptedCount;
                prefix = acceptedHash;
                seen = new HashSet<>(seenCursors);
            }
            String nextDigest = page.nextCursor.isEmpty() ? null : digest(page.nextCursor);
            if (nextDigest != null
                    && (page.nextCursor.equals(requestCursor) || seen.contains(nextDigest))) {
                throw failure("non-progressing pagination cursor");
            }
            // Full response and accepted-prefix validation finish before acquiring engine locks.
            preparedPage = prepare(page, nextDigest, count, prefix);
            synchronized (lifecycleLock) {
                if (pendingPage == pending) {
                    pendingPage = null;
                }
            }
        }
        boolean finished = false;
        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                if (closed) {
                    return;
                }
                PreparedPage current = preparedPage;
                if (current.index < current.page.rows.size()) {
                    output.collect(current.page.rows.get(current.index));
                    acceptedHash = current.prefixes.get(current.index++);
                    acceptedCount = current.index;
                }
                if (current.index == current.page.rows.size()) {
                    // Last-row acceptance, continuation and prefix reset share one boundary.
                    pages++;
                    cursor = current.page.nextCursor;
                    if (cursor.isEmpty()) {
                        channelIndex++;
                        pages = 0;
                        seenCursors.clear();
                    } else {
                        seenCursors.add(current.nextDigest);
                    }
                    acceptedCount = 0;
                    acceptedHash = null;
                    preparedPage = null;
                    finished = channelIndex == config.channels.size();
                    noMoreSplits = finished;
                }
            }
        }
        if (finished && !closed) {
            context.signalNoMoreElement();
        }
    }

    /**
     * State is immutable bytes captured with the accepted row prefix. The complete page stays
     * transient; its cursor advances only with the final accepted row and a joint prefix reset.
     */
    @Override
    protected byte[] snapshotStateToBytes(long checkpointId) throws Exception {
        synchronized (stateLock) {
            ObjectNode state = mapper.createObjectNode();
            state.put("version", 2)
                    .put("channel", channelIndex)
                    .put("pages", pages)
                    .put("cursor", cursor)
                    .put("latest", latest)
                    .put("accepted_count", acceptedCount)
                    .put("accepted_hash", acceptedHash);
            state.put("oldest", config.oldest);
            state.set("channels", mapper.valueToTree(config.channels));
            ArrayNode seen = state.putArray("seen");
            seenCursors.forEach(seen::add);
            return mapper.writeValueAsBytes(state);
        }
    }

    /**
     * Restores the frozen window and accepted prefix before polling starts. The current page is
     * refetched and checked before its saved prefix can be skipped; later rows retain replay
     * limits.
     */
    @Override
    protected void restoreState(byte[] restoredState) {
        synchronized (stateLock) {
            try {
                JsonNode state = mapper.readTree(restoredState);
                if (state == null
                        || !state.isObject()
                        || !state.path("version").isInt()
                        || state.path("version").intValue() != 2
                        || !state.path("channel").isInt()
                        || !state.path("pages").isInt()
                        || !state.path("cursor").isTextual()
                        || !state.path("latest").isTextual()
                        || !state.path("seen").isArray()
                        || !state.path("accepted_count").isInt()
                        || !(state.path("accepted_hash").isNull()
                                || state.path("accepted_hash").isTextual())) {
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
                int restoredCount = state.get("accepted_count").intValue();
                String restoredHash = state.get("accepted_hash").textValue();
                SlackSourceConfig.validateTimestamp(restoredLatest, "checkpoint latest");
                if (restoredChannel < 0
                        || restoredChannel > config.channels.size()
                        || restoredPages < 0
                        || restoredPages > config.maxPages
                        || restoredCursor.length() > 4096
                        || state.get("seen").size() > config.maxPages
                        || restoredCount < 0
                        // parsePage already enforces this existing decoded-row cap.
                        || restoredCount > config.pageSize
                        || (restoredCount == 0 && restoredHash != null)
                        || (restoredCount > 0
                                && (restoredHash == null
                                        || !restoredHash.matches("[A-Za-z0-9_-]{43}")))
                        || (restoredChannel == config.channels.size() && restoredCount != 0)) {
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
                acceptedCount = restoredCount;
                acceptedHash = restoredHash;
                preparedPage = null;
                noMoreSplits = false;
            } catch (IOException | IllegalArgumentException e) {
                throw failure("invalid checkpoint state");
            }
        }
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
        // Keep native byte/BOM detection, but never merge response names into checkpoint roots.
        // Interning is disabled on the template so copies cannot fill the global name cache.
        ObjectMapper responseMapper = mapper.copy();
        JsonNode root;
        try {
            root =
                    responseMapper
                            .reader()
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
     * Verifies the exact accepted output prefix before exposing any refetched tail. Every logical
     * occurrence is hashed, including nulls, Unicode code units and complete raw JSON strings.
     */
    private PreparedPage prepare(Page page, String nextDigest, int count, String saved)
            throws Exception {
        PreparedPage prepared = new PreparedPage(page, nextDigest, count);
        String previous = null;
        for (SeaTunnelRow row : page.rows) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            if (previous != null) {
                digest.update(previous.getBytes(StandardCharsets.US_ASCII));
            }
            for (int column = 0; column < 8; column++) {
                hashString(digest, (String) row.getField(column));
            }
            previous = Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
            prepared.prefixes.add(previous);
        }
        if (count > page.rows.size()
                || (count == 0 && saved != null)
                || (count > 0 && !prepared.prefixes.get(count - 1).equals(saved))) {
            throw failure("checkpoint page prefix changed or is missing");
        }
        return prepared;
    }

    /**
     * Frames each nullable String with a signed big-endian length (-1 for null), followed by exact
     * UTF-16 big-endian code units. This keeps null, empty and malformed-surrogate values distinct
     * without allocating an encoded copy or changing the v2 accepted-prefix contract.
     */
    private static void hashString(MessageDigest digest, String value) {
        int length = value == null ? -1 : value.length();
        for (int shift = 24; shift >= 0; shift -= 8) {
            digest.update((byte) (length >>> shift));
        }
        if (value != null) {
            for (int i = 0; i < value.length(); i++) {
                char unit = value.charAt(i);
                digest.update((byte) (unit >>> 8));
                digest.update((byte) unit);
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
     * Marks connector-owned sanitized failures so the polling thread can rethrow them without
     * exposing arbitrary client/parser exception messages from the HTTP worker.
     */
    private static final class SlackSourceException extends IllegalStateException {
        private SlackSourceException(String message) {
            super(message);
        }
    }

    /**
     * Poll-owned decoded page and per-row hashes are transient. The current index is published as
     * durable count/hash only after collect succeeds, and discarded with complete-page advancement.
     */
    private static final class PreparedPage {
        private final Page page;
        private final String nextDigest;
        private final List<String> prefixes = new ArrayList<>();
        private int index;

        private PreparedPage(Page page, String nextDigest, int index) {
            this.page = page;
            this.nextDigest = nextDigest;
            this.index = index;
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
