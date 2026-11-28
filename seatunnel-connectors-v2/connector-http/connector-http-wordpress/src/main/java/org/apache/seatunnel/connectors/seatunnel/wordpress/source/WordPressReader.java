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

package org.apache.seatunnel.connectors.seatunnel.wordpress.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reads a bounded public-post window with whole-page validation and cancellable HTTP. No page
 * cursor is checkpointed; a restored split replays the requested window.
 */
final class WordPressReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private static final String API = "https://public-api.wordpress.com/rest/v1/sites/";
    private static final int TIMEOUT_MS = 30000;
    private static final int MAX_BYTES = 4 * 1024 * 1024;
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private final URI endpoint;
    private final Instant start;
    private final Instant end;
    private final int pageSize;
    private final int maxPages;
    private final CatalogTable table;
    private final SingleSplitReaderContext context;
    // Reader-local scan progress only. The inherited null split state still replays the window.
    private int total = -1;
    private int offset;
    private long previousId;
    // A validated page stays reader-local; inherited checkpoints still replay the full window.
    private PreparedPage prepared;
    /*
     * Tracks the dispatched request so cancellation can abort I/O without taking lifecycleLock.
     */
    private final AtomicReference<HttpGet> activeRequest = new AtomicReference<>();
    /**
     * Sticky across open and close; checked at resource publication and before further I/O.
     * cancel() does not acquire lifecycleLock or wait for resource detachment.
     */
    private volatile boolean cancelled;

    // Only resource publication and detachment use this monitor. In particular, never abort,
    // execute or close a request under this lock, or acquire it from cancel().
    private final Object lifecycleLock = new Object();
    /*
     * Published and detached under lifecycleLock; the detaching owner closes both resources.
     */
    private volatile ClientResources resources;

    WordPressReader(
            String site,
            Instant start,
            Instant end,
            int pageSize,
            int maxPages,
            CatalogTable table,
            SingleSplitReaderContext context) {
        this(URI.create(API + site + "/posts/"), start, end, pageSize, maxPages, table, context);
    }

    WordPressReader(
            URI endpoint,
            Instant start,
            Instant end,
            int pageSize,
            int maxPages,
            CatalogTable table,
            SingleSplitReaderContext context) {
        this.endpoint = endpoint;
        this.start = start;
        this.end = end;
        this.pageSize = pageSize;
        this.maxPages = maxPages;
        this.table = table;
        this.context = context;
    }

    /**
     * Creates the HTTP client and request-deadline executor as one owned resource pair. A competing
     * open or cancellation closes the unpublished pair outside lifecycleLock.
     */
    @Override
    public void open() throws IOException {
        if (cancelled || resources != null) {
            throw new IllegalStateException("WordPress reader cannot be opened");
        }
        CloseableHttpClient client =
                HttpClients.custom()
                        .useSystemProperties()
                        .disableAutomaticRetries()
                        .disableCookieManagement()
                        .disableRedirectHandling()
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(TIMEOUT_MS)
                                        .setSocketTimeout(TIMEOUT_MS)
                                        .setConnectionRequestTimeout(TIMEOUT_MS)
                                        .build())
                        .build();
        ScheduledThreadPoolExecutor deadlineExecutor =
                new ScheduledThreadPoolExecutor(
                        1,
                        runnable -> {
                            Thread thread = new Thread(runnable, "wordpress-request-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        deadlineExecutor.setRemoveOnCancelPolicy(true);

        ClientResources opened = new ClientResources(client, deadlineExecutor);
        synchronized (lifecycleLock) {
            if (!cancelled && resources == null) {
                resources = opened;
                return;
            }
        }
        opened.close();
        throw new IllegalStateException("WordPress reader cannot be opened");
    }

    /**
     * Fetches and validates one page outside the checkpoint monitor, then emits at most one row.
     * Returning between rows reaches the engine's checkpoint handoff even with output throttling.
     * Earlier pages may already be emitted when a later page fails; this is not a snapshot.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (noMoreSplits) {
            return;
        }
        requireOpenResources();
        if (prepared == null) {
            prepared = preparePage();
        }
        synchronized (output.getCheckpointLock()) {
            requireOpenResources();
            PreparedPage page = prepared;
            if (page.index < page.rows.size()) {
                output.collect(page.rows.get(page.index));
                page.index++;
            }
            if (page.index == page.rows.size()) {
                total = page.total;
                previousId = page.lastId;
                offset += page.rows.size();
                prepared = null;
                if (offset >= total) {
                    requireOpenResources();
                    context.signalNoMoreElement();
                    noMoreSplits = true;
                }
            }
        }
    }

    /*
     * Permanently marks cancellation and aborts any published request without locking.
     */
    @Override
    public void cancel() {
        cancelled = true;
        HttpGet request = activeRequest.get();
        if (request != null) {
            request.abort();
        }
    }

    /*
     * Detaches resources once under lifecycleLock, then shuts them down outside the lock.
     */
    @Override
    public void close() throws IOException {
        cancel();
        ClientResources closing;
        synchronized (lifecycleLock) {
            closing = resources;
            resources = null;
        }
        if (closing != null) {
            closing.close();
        }
    }

    /**
     * Validates every row before the first output, preserving whole-page failure atomicity. The
     * transient page keeps the next offset unpublished until its final row is accepted.
     */
    private PreparedPage preparePage() throws Exception {
        JsonNode page = readPage(offset);
        JsonNode found = page.path("found");
        JsonNode posts = page.path("posts");
        if (!found.isIntegralNumber()
                || !found.canConvertToInt()
                || found.intValue() < 0
                || !posts.isArray()) {
            throw failure("Invalid posts pagination envelope");
        }
        int pageTotal = found.intValue();
        if (total < 0) {
            if (pageTotal > maxPages * pageSize) {
                throw failure("Result exceeds max_pages; choose a smaller date window");
            }
        } else if (total != pageTotal) {
            throw failure(
                    "Post count changed during pagination; retry a stable publication window");
        }
        if (posts.size() != Math.min(pageSize, pageTotal - offset)) {
            throw failure("Unexpected page size; retry a stable publication window");
        }
        List<SeaTunnelRow> rows = new ArrayList<>(posts.size());
        long pagePreviousId = previousId;
        for (JsonNode post : posts) {
            JsonNode id = post.path("ID");
            if (!id.isIntegralNumber()
                    || !id.canConvertToLong()
                    || id.longValue() <= pagePreviousId) {
                throw failure("Post IDs must be positive and strictly increasing across pages");
            }
            pagePreviousId = id.longValue();
            LocalDateTime published = timestamp(post, "date");
            Instant publication = published.toInstant(ZoneOffset.UTC);
            if (publication.isBefore(start) || publication.isAfter(end)) {
                throw failure("Received a publication timestamp outside the requested window");
            }
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                pagePreviousId,
                                string(post, "slug"),
                                string(post, "title"),
                                string(post, "URL"),
                                published,
                                timestamp(post, "modified")
                            });
            row.setTableId(table.getTableId().toTablePath().toString());
            rows.add(row);
        }
        return new PreparedPage(rows, pageTotal, pagePreviousId);
    }

    /**
     * Fetches an ordered, projected page with a 30-second abort deadline. Always cancels the
     * deadline and aborts/detaches its request, including close races.
     */
    private JsonNode readPage(int offset) throws Exception {
        HttpGet request =
                new HttpGet(
                        new URIBuilder(endpoint)
                                .addParameter("number", Integer.toString(pageSize))
                                .addParameter("offset", Integer.toString(offset))
                                .addParameter("after", start.toString())
                                .addParameter("before", end.toString())
                                .addParameter("order", "ASC")
                                .addParameter("order_by", "ID")
                                .addParameter("status", "publish")
                                .addParameter("type", "post")
                                .addParameter("fields", "ID,slug,title,URL,date,modified")
                                .build());
        request.setHeader("Accept", "application/json");
        activeRequest.set(request);
        ScheduledFuture<?> deadline = null;
        try {
            // Publish before checking the sticky flag. Either cancel() finds the request or
            // this check observes cancellation. An abort before execute() is also sticky.
            ClientResources current = requireOpenResources();
            deadline =
                    current.deadlineExecutor.schedule(
                            request::abort, TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return executePage(current.client, request, offset);
        } catch (RejectedExecutionException error) {
            // close() can shut down the captured executor before schedule() runs.
            requireOpenResources();
            throw error;
        } finally {
            if (deadline != null) {
                deadline.cancel(false);
            }
            request.abort();
            activeRequest.compareAndSet(request, null);
        }
    }

    /**
     * Bounds the decoded page to 4 MiB and rejects non-200 or malformed responses. Parser errors
     * expose the offset and error type, not response content.
     */
    private JsonNode executePage(CloseableHttpClient client, HttpGet request, int offset) {
        try (CloseableHttpResponse response = client.execute(request)) {
            int status = response.getStatusLine().getStatusCode();
            if (status != 200) {
                throw failure("HTTP " + status + " when reading public posts at offset " + offset);
            }
            if (response.getEntity() == null
                    || response.getEntity().getContentLength() > MAX_BYTES) {
                throw failure("Missing response body or page exceeds 4 MiB");
            }
            InputStream input = response.getEntity().getContent();
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int size;
            while ((size = input.read(buffer)) != -1) {
                requireOpenResources();
                if (body.size() > MAX_BYTES - size) {
                    throw failure("Response page exceeds 4 MiB");
                }
                body.write(buffer, 0, size);
            }
            JsonNode value = MAPPER.readTree(body.toByteArray());
            if (value == null || !value.isObject()) {
                throw failure("Expected a JSON response object");
            }
            return value;
        } catch (IOException e) {
            requireOpenResources();
            throw failure(
                    "Could not read or parse response at offset "
                            + offset
                            + " ("
                            + e.getClass().getSimpleName()
                            + ")");
        }
    }

    private static String string(JsonNode post, String field) {
        JsonNode value = post.path(field);
        if (!value.isTextual()) {
            throw failure("Missing or invalid post field: " + field);
        }
        return value.textValue();
    }

    /*
     * Normalizes API timestamps to UTC before output and publication-window validation.
     */
    private static LocalDateTime timestamp(JsonNode post, String field) {
        try {
            return OffsetDateTime.parse(string(post, field))
                    .withOffsetSameInstant(ZoneOffset.UTC)
                    .toLocalDateTime();
        } catch (DateTimeParseException e) {
            throw failure("Invalid post timestamp: " + field);
        }
    }

    private ClientResources requireOpenResources() {
        ClientResources current = resources;
        if (cancelled || current == null || Thread.currentThread().isInterrupted()) {
            throw failure("Reader is not open or was cancelled");
        }
        return current;
    }

    private static IllegalStateException failure(String message) {
        return new IllegalStateException("WordPress: " + message);
    }

    /**
     * Holds one fully validated page between poll calls. Only the polling thread changes index;
     * checkpoints retain the inherited null split and restore the original full-window replay.
     */
    private static final class PreparedPage {
        private final List<SeaTunnelRow> rows;
        private final int total;
        private final long lastId;
        private int index;

        private PreparedPage(List<SeaTunnelRow> rows, int total, long lastId) {
            this.rows = rows;
            this.total = total;
            this.lastId = lastId;
        }
    }

    /**
     * Owns one HTTP client and its deadline executor; both share one close lifecycle. The owner
     * detaches the pair before closing it, without holding the reader lifecycle lock.
     */
    private static final class ClientResources {
        private final CloseableHttpClient client;
        private final ScheduledThreadPoolExecutor deadlineExecutor;

        private ClientResources(
                CloseableHttpClient client, ScheduledThreadPoolExecutor deadlineExecutor) {
            this.client = client;
            this.deadlineExecutor = deadlineExecutor;
        }

        private void close() throws IOException {
            deadlineExecutor.shutdownNow();
            client.close();
        }
    }
}
