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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    private volatile CloseableHttpClient client;
    private volatile boolean closed;
    private ScheduledThreadPoolExecutor deadlineExecutor;
    private HttpGet activeRequest;

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

    @Override
    public synchronized void open() {
        if (closed || client != null) {
            throw new IllegalStateException("WordPress reader cannot be opened");
        }
        client =
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
        deadlineExecutor =
                new ScheduledThreadPoolExecutor(
                        1,
                        runnable -> {
                            Thread thread = new Thread(runnable, "wordpress-request-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        deadlineExecutor.setRemoveOnCancelPolicy(true);
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        int total = -1;
        long previousId = 0;
        int offset = 0;
        do {
            ensureOpen();
            JsonNode page = readPage(offset);
            JsonNode found = page.path("found");
            JsonNode posts = page.path("posts");
            if (!found.isIntegralNumber()
                    || !found.canConvertToInt()
                    || found.intValue() < 0
                    || !posts.isArray()) {
                throw failure("Invalid posts pagination envelope");
            }
            if (total < 0) {
                total = found.intValue();
                if (total > maxPages * pageSize) {
                    throw failure("Result exceeds max_pages; choose a smaller date window");
                }
            } else if (total != found.intValue()) {
                throw failure(
                        "Post count changed during pagination; retry a stable publication window");
            }
            if (posts.size() != Math.min(pageSize, total - offset)) {
                throw failure("Unexpected page size; retry a stable publication window");
            }
            List<SeaTunnelRow> rows = new ArrayList<>(posts.size());
            for (JsonNode post : posts) {
                JsonNode id = post.path("ID");
                if (!id.isIntegralNumber()
                        || !id.canConvertToLong()
                        || id.longValue() <= previousId) {
                    throw failure("Post IDs must be positive and strictly increasing across pages");
                }
                previousId = id.longValue();
                LocalDateTime published = timestamp(post, "date");
                Instant publication = published.toInstant(ZoneOffset.UTC);
                if (publication.isBefore(start) || publication.isAfter(end)) {
                    throw failure("Received a publication timestamp outside the requested window");
                }
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {
                                    previousId,
                                    string(post, "slug"),
                                    string(post, "title"),
                                    string(post, "URL"),
                                    published,
                                    timestamp(post, "modified")
                                });
                row.setTableId(table.getTableId().toTablePath().toString());
                rows.add(row);
            }
            for (SeaTunnelRow row : rows) {
                ensureOpen();
                output.collect(row);
            }
            offset += posts.size();
        } while (offset < total);
        ensureOpen();
        context.signalNoMoreElement();
    }

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
        ScheduledFuture<?> deadline;
        synchronized (this) {
            ensureOpen();
            activeRequest = request;
            deadline = deadlineExecutor.schedule(request::abort, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
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
                ensureOpen();
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
            throw failure(
                    "Could not read or parse response at offset "
                            + offset
                            + " ("
                            + e.getClass().getSimpleName()
                            + ")");
        } finally {
            deadline.cancel(false);
            request.abort();
            synchronized (this) {
                activeRequest = null;
            }
        }
    }

    private static String string(JsonNode post, String field) {
        JsonNode value = post.path(field);
        if (!value.isTextual()) {
            throw failure("Missing or invalid post field: " + field);
        }
        return value.textValue();
    }

    private static LocalDateTime timestamp(JsonNode post, String field) {
        try {
            return OffsetDateTime.parse(string(post, field))
                    .withOffsetSameInstant(ZoneOffset.UTC)
                    .toLocalDateTime();
        } catch (DateTimeParseException e) {
            throw failure("Invalid post timestamp: " + field);
        }
    }

    private void ensureOpen() {
        if (closed || client == null || Thread.currentThread().isInterrupted()) {
            throw failure("Reader is not open or was cancelled");
        }
    }

    private static IllegalStateException failure(String message) {
        return new IllegalStateException("WordPress: " + message);
    }

    @Override
    public synchronized void cancel() {
        closed = true;
        if (activeRequest != null) {
            activeRequest.abort();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        cancel();
        if (deadlineExecutor != null) {
            deadlineExecutor.shutdownNow();
        }
        if (client != null) {
            client.close();
        }
    }
}
