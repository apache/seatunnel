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
package org.apache.seatunnel.connectors.seatunnel.discourse;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultManagedHttpClientConnection;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.SystemDefaultRoutePlanner;

import java.io.IOException;
import java.io.InputStream;
import java.net.ProxySelector;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fetches and validates one bounded metadata response before emitting any rows. No remote cursor is
 * checkpointed; a restored split requests the current response again.
 */
final class DiscourseSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int SOCKET_TIMEOUT_MS = 30_000;
    // Elapsed fetch budget also bounds peers that keep the socket active with a slow body.
    private static final long FETCH_TIMEOUT_MS = 30_000;

    private final URI endpoint;
    private final String apiKey;
    private final String apiUsername;
    private final DiscourseSourceOptions.Resource resource;
    private final boolean includeSubcategories;
    private final CatalogTable table;
    private final SingleSplitReaderContext context;
    // Fixed in production; the package-private constructor permits short real-I/O regressions.
    private final long fetchTimeoutMs;
    private final ObjectMapper mapper =
            new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    /*
     * Sticky cancellation is checked before dispatch and while decoding category/topic rows.
     */
    private final AtomicBoolean cancelled = new AtomicBoolean();
    /*
     * Published before checking cancellation so cancel either aborts or prevents dispatch.
     */
    private final AtomicReference<HttpGet> activeRequest = new AtomicReference<>();
    /*
     * Created by open and closed by close under the normal engine lifecycle.
     * Client publication is not atomically coordinated with a concurrent open/close call.
     */
    private CloseableHttpClient client;
    /*
     * Owned only by the polling thread, never included in the inherited null split snapshot.
     * Retaining the validated response allows each poll to yield after one accepted row without
     * fetching again; a restored reader still requests and replays the whole response.
     */
    private List<SeaTunnelRow> preparedRows;

    // Advance only after collect succeeds, retaining a failed row for a subsequent explicit poll.
    private int nextRow;

    DiscourseSourceReader(
            URI endpoint,
            String apiKey,
            String apiUsername,
            DiscourseSourceOptions.Resource resource,
            boolean includeSubcategories,
            CatalogTable table,
            SingleSplitReaderContext context) {
        this(
                endpoint,
                apiKey,
                apiUsername,
                resource,
                includeSubcategories,
                table,
                context,
                FETCH_TIMEOUT_MS);
    }

    /*
     * Keeps the production budget fixed while tests exercise the same socket/abort path promptly.
     */
    DiscourseSourceReader(
            URI endpoint,
            String apiKey,
            String apiUsername,
            DiscourseSourceOptions.Resource resource,
            boolean includeSubcategories,
            CatalogTable table,
            SingleSplitReaderContext context,
            long fetchTimeoutMs) {
        if (fetchTimeoutMs <= 0) {
            throw new IllegalArgumentException("Discourse fetch timeout must be positive");
        }
        this.endpoint = endpoint;
        this.apiKey = apiKey;
        this.apiUsername = apiUsername;
        this.resource = resource;
        this.includeSubcategories = includeSubcategories;
        this.table = table;
        this.context = context;
        this.fetchTimeoutMs = fetchTimeoutMs;
    }

    /*
     * Creates the reader-owned HTTP client with finite inactivity timeouts and no redirects or retries.
     * This method relies on the engine calling open before polling and close after use.
     */
    @Override
    public void open() {
        // The default factory logs every API header and wire byte at DEBUG. Replace only that
        // factory, retaining the full client chain and this version's default TLS socket registry.
        PoolingHttpClientConnectionManager connections =
                new PoolingHttpClientConnectionManager(
                        (route, config) ->
                                new DefaultManagedHttpClientConnection(
                                        "discourse-source",
                                        config.getBufferSize(),
                                        config.getFragmentSizeHint(),
                                        null,
                                        null,
                                        config.getMessageConstraints(),
                                        null,
                                        null,
                                        null,
                                        null));
        connections.setDefaultConnectionConfig(
                ConnectionConfig.custom()
                        .setMessageConstraints(
                                MessageConstraints.custom()
                                        .setMaxHeaderCount(100)
                                        .setMaxLineLength(16 * 1024)
                                        .build())
                        .build());
        client =
                HttpClients.custom()
                        .setConnectionManager(connections)
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(CONNECT_TIMEOUT_MS)
                                        .setConnectionRequestTimeout(CONNECT_TIMEOUT_MS)
                                        .setSocketTimeout(SOCKET_TIMEOUT_MS)
                                        .build())
                        .disableRedirectHandling()
                        .disableAutomaticRetries()
                        .disableCookieManagement()
                        .disableAuthCaching()
                        .setRoutePlanner(new SystemDefaultRoutePlanner(ProxySelector.getDefault()))
                        .build();
    }

    /*
     * Permanently marks cancellation and aborts any currently published request.
     */
    @Override
    public void cancel() {
        cancelled.set(true);
        HttpGet request = activeRequest.get();
        if (request != null) {
            request.abort();
        }
    }

    /*
     * Cancels active I/O before closing the client created by open.
     */
    @Override
    public void close() throws IOException {
        cancel();
        if (client != null) {
            client.close();
        }
    }

    /*
     * The engine owns one polling thread. HTTP and whole-response validation do not hold the
     * collector lock, so a checkpoint can retain the existing null-state/whole-response replay
     * contract during slow I/O. At most one row is collected before returning to the engine's
     * cooperative checkpoint handoff; individual collection can still wait on downstream progress.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws IOException {
        if (noMoreSplits) {
            return;
        }
        checkCancelled();
        if (preparedRows == null) {
            preparedRows = fetchRows();
        }
        synchronized (output.getCheckpointLock()) {
            checkCancelled();
            if (nextRow < preparedRows.size()) {
                output.collect(preparedRows.get(nextRow));
                nextRow++;
            }
            if (nextRow == preparedRows.size()) {
                checkCancelled();
                context.signalNoMoreElement();
                noMoreSplits = true;
                preparedRows = null;
            }
        }
    }

    /*
     * Publishes the request before checking the sticky cancellation flag.
     * Always detaches and releases the request, including validation and cancellation failures.
     */
    List<SeaTunnelRow> fetchRows() throws IOException {
        HttpGet request = new HttpGet(endpoint);
        activeRequest.set(request);
        // The daemon owns only this fetch; its abort never contends for the checkpoint monitor.
        ScheduledExecutorService deadline =
                Executors.newSingleThreadScheduledExecutor(
                        task -> {
                            Thread thread = new Thread(task, "discourse-fetch-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        // Completion and expiry compete once, so a late timer cannot abort a completed fetch.
        AtomicBoolean expired = new AtomicBoolean();
        AtomicBoolean terminal = new AtomicBoolean();
        long startedNanos = System.nanoTime();
        long budgetNanos = TimeUnit.MILLISECONDS.toNanos(fetchTimeoutMs);
        try {
            checkCancelled();
            deadline.schedule(
                    () -> {
                        if (terminal.compareAndSet(false, true)) {
                            expired.set(true);
                            request.abort();
                        }
                    },
                    fetchTimeoutMs,
                    TimeUnit.MILLISECONDS);
            try {
                List<SeaTunnelRow> rows = fetchRows(request);
                // A delayed scheduler must not turn an already expired fetch into success.
                if (System.nanoTime() - startedNanos >= budgetNanos
                        || !terminal.compareAndSet(false, true)) {
                    throw new IOException("Discourse request exceeded the elapsed fetch timeout");
                }
                return rows;
            } catch (IOException e) {
                if (expired.get() || System.nanoTime() - startedNanos >= budgetNanos) {
                    // Do not retain transport diagnostics or upstream content in the timeout.
                    throw new IOException("Discourse request exceeded the elapsed fetch timeout");
                }
                throw e;
            }
        } finally {
            terminal.set(true);
            deadline.shutdownNow();
            activeRequest.compareAndSet(request, null);
            request.releaseConnection();
        }
    }

    /*
     * Stops the decoded tail and normal completion after cancel, allowing an in-flight collect to end.
     */
    private void checkCancelled() throws IOException {
        if (cancelled.get() || Thread.currentThread().isInterrupted()) {
            // Observe interruption without clearing the engine-owned interrupt status.
            throw new IOException("Discourse source was cancelled");
        }
    }

    /*
     * Sends optional credentials only as headers and accepts exactly HTTP 200.
     * Bounds the decoded response to 16 MiB before conversion; no continuation link is followed.
     */
    private List<SeaTunnelRow> fetchRows(HttpGet request) throws IOException {
        request.setHeader("Accept", "application/json");
        if (apiKey != null) {
            request.setHeader("Api-Key", apiKey);
            request.setHeader("Api-Username", apiUsername);
        }
        byte[] bytes;
        try (CloseableHttpResponse response = execute(request)) {
            int status = response.getStatusLine().getStatusCode();
            if (status != 200) {
                request.abort();
                throw new IOException("Discourse " + resource + " request returned HTTP " + status);
            }
            if (response.getEntity() == null) {
                throw new IOException("Discourse response has no body");
            }
            try (InputStream stream = response.getEntity().getContent()) {
                bytes = stream.readNBytes(MAX_RESPONSE_BYTES + 1);
                if (bytes.length > MAX_RESPONSE_BYTES) {
                    // Abort before stream close can try to drain an oversized response.
                    request.abort();
                }
            } catch (IOException e) {
                request.abort();
                checkCancelled();
                // Chunk/framing exceptions can include private response bytes in their message.
                throw new IOException("Discourse response could not be read");
            }
            if (bytes.length > MAX_RESPONSE_BYTES) {
                throw new IOException("Discourse response exceeds the 16 MiB limit");
            }
        }
        return decodeRows(bytes);
    }

    /*
     * HTTP status-line/parser exceptions may quote upstream text, so discard their cause chain.
     * Connector-owned status, size and JSON diagnostics remain outside this transport boundary.
     */
    private CloseableHttpResponse execute(HttpGet request) throws IOException {
        try {
            return client.execute(request);
        } catch (IOException e) {
            request.abort();
            checkCancelled();
            throw new IOException("Discourse HTTP exchange failed");
        }
    }

    /*
     * Validates all field types, required values and unique IDs before returning rows.
     * An explicit iterator stack flattens requested nested categories without Java recursion;
     * latest topics remain the single returned array. Diagnostics never echo parser payloads.
     */
    private List<SeaTunnelRow> decodeRows(byte[] bytes) throws IOException {
        final JsonNode body;
        try {
            body = mapper.readTree(bytes);
        } catch (IOException e) {
            // Do not attach parser diagnostics, which may quote the upstream response.
            throw new IOException("Discourse response is not valid JSON");
        }
        JsonNode rows = body == null ? null : body.path(resource.envelope).path(resource.rows);
        if (rows == null || !rows.isArray()) {
            throw new IOException(
                    "Discourse response lacks the "
                            + resource.envelope
                            + "."
                            + resource.rows
                            + " array");
        }
        List<SeaTunnelRow> result = new ArrayList<>(rows.size());
        List<Column> columns = table.getTableSchema().getColumns();
        Set<Long> ids = new HashSet<>();
        Deque<Iterator<JsonNode>> pending = new ArrayDeque<>();
        pending.push(rows.elements());
        while (!pending.isEmpty()) {
            checkCancelled();
            Iterator<JsonNode> iterator = pending.peek();
            if (!iterator.hasNext()) {
                pending.pop();
                continue;
            }
            JsonNode entry = iterator.next();
            Object[] values = new Object[columns.size()];
            for (int i = 0; i < values.length; i++) {
                Column column = columns.get(i);
                JsonNode value = entry.get(column.getName());
                if (value == null || value.isNull()) {
                    if (!column.isNullable()) {
                        throw new IOException(
                                "Discourse record has no required field " + column.getName());
                    }
                    continue;
                }
                if (column.getDataType().equals(BasicType.STRING_TYPE) && value.isTextual()) {
                    values[i] = value.textValue();
                } else if (column.getDataType().equals(BasicType.LONG_TYPE)
                        && value.isIntegralNumber()
                        && value.canConvertToLong()) {
                    values[i] = value.longValue();
                } else if (column.getDataType().equals(BasicType.BOOLEAN_TYPE)
                        && value.isBoolean()) {
                    values[i] = value.booleanValue();
                } else {
                    throw new IOException(
                            "Discourse record has an invalid type for field " + column.getName());
                }
            }
            if (!ids.add((Long) values[0])) {
                throw new IOException("Discourse response contains a duplicate id");
            }
            SeaTunnelRow row = new SeaTunnelRow(values);
            row.setTableId(table.getTablePath().toString());
            result.add(row);
            if (includeSubcategories) {
                JsonNode children = entry.get("subcategory_list");
                if (children != null && !children.isNull()) {
                    if (!children.isArray()) {
                        throw new IOException(
                                "Discourse category subcategory_list is not an array");
                    }
                    pending.push(children.elements());
                }
            }
        }
        return result;
    }
}
