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

package org.apache.seatunnel.connectors.seatunnel.woocommerce.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Read-only orders transport with a per-attempt deadline and bounded decompressed responses. */
final class WooCommerceClient implements AutoCloseable {
    static final MessageConstraints MESSAGE_CONSTRAINTS =
            MessageConstraints.custom().setMaxHeaderCount(100).setMaxLineLength(8192).build();
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private final WooCommerceConfig config;
    private final CloseableHttpClient client;
    private final ScheduledThreadPoolExecutor timer;
    private volatile boolean closed;
    private HttpGet active;

    WooCommerceClient(WooCommerceConfig config) {
        this(
                config,
                HttpClients.custom()
                        .disableAutomaticRetries()
                        .disableRedirectHandling()
                        .setDefaultConnectionConfig(
                                ConnectionConfig.custom()
                                        .setMessageConstraints(MESSAGE_CONSTRAINTS)
                                        .build())
                        .disableCookieManagement()
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(config.timeout)
                                        .setSocketTimeout(config.timeout)
                                        .setConnectionRequestTimeout(config.timeout)
                                        .build())
                        .build());
    }

    WooCommerceClient(WooCommerceConfig config, CloseableHttpClient client) {
        this.config = config;
        this.client = client;
        timer =
                new ScheduledThreadPoolExecutor(
                        1,
                        task -> {
                            Thread thread = new Thread(task, "woocommerce-request-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        timer.setRemoveOnCancelPolicy(true);
    }

    Page page(int page) throws Exception {
        for (int attempt = 0; ; attempt++) {
            checkOpen();
            HttpGet request =
                    new HttpGet(
                            new URIBuilder(config.url + "/wp-json/wc/v3/orders")
                                    .addParameter("page", Integer.toString(page))
                                    .addParameter("per_page", Integer.toString(config.pageSize))
                                    .addParameter("after", config.start)
                                    .addParameter("before", config.end)
                                    .addParameter("dates_are_gmt", "true")
                                    .addParameter("orderby", "id")
                                    .addParameter("order", "asc")
                                    .addParameter("status", "any")
                                    .addParameter("dp", Integer.toString(config.decimalPlaces))
                                    .build());
            request.setHeader(
                    "Authorization",
                    "Basic "
                            + Base64.getEncoder()
                                    .encodeToString(
                                            (config.key + ":" + config.secret)
                                                    .getBytes(StandardCharsets.UTF_8)));
            request.setHeader("Accept", "application/json");
            int delay = config.retryDelay;
            try {
                Reply reply = execute(request);
                if (transientStatus(reply.status)) {
                    delay = retryDelay(reply.retryAfter, delay);
                } else if (reply.status != 200) {
                    throw failure(
                            "HTTP "
                                    + reply.status
                                    + "; check store URL and read permission (body withheld)");
                } else {
                    return parse(reply, page);
                }
            } catch (IOException e) {
                // Transport exceptions can contain credentials or remote response data.
            }
            retry(attempt, delay);
        }
    }

    private Reply execute(HttpGet request) throws IOException {
        ScheduledFuture<?> deadline;
        synchronized (this) {
            checkOpen();
            active = request;
            deadline = timer.schedule(request::abort, config.timeout, TimeUnit.MILLISECONDS);
        }
        CloseableHttpResponse response = null;
        try {
            response = client.execute(request);
            int status = response.getStatusLine().getStatusCode();
            String retryAfter = header(response, "Retry-After");
            if (status != 200) {
                return new Reply(status, retryAfter, null, null, null);
            }
            if (response.getEntity() == null) {
                throw failure("Missing response body");
            }
            if (response.getEntity().getContentLength() > config.maxBytes) {
                throw failure("Response exceeds max_response_bytes");
            }
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            InputStream input = response.getEntity().getContent();
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) != -1) {
                checkOpen();
                if (bytes.size() > config.maxBytes - count) {
                    throw failure("Response exceeds max_response_bytes");
                }
                bytes.write(buffer, 0, count);
            }
            return new Reply(
                    status,
                    retryAfter,
                    header(response, "X-WP-Total"),
                    header(response, "X-WP-TotalPages"),
                    bytes.toByteArray());
        } finally {
            request.abort();
            deadline.cancel(false);
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ignored) {
                    /* Request already aborted. */
                }
            }
            synchronized (this) {
                active = null;
            }
        }
    }

    private static String header(CloseableHttpResponse response, String name) {
        Header[] values = response.getHeaders(name);
        if (values.length > 1) {
            throw failure("Ambiguous HTTP pagination or retry headers");
        }
        return values.length == 0 ? null : values[0].getValue();
    }

    private Page parse(Reply reply, int page) {
        long total = number(reply.total, "X-WP-Total");
        long pages = number(reply.pages, "X-WP-TotalPages");
        if (pages > config.maxPages) {
            throw failure("Scan exceeds max_pages; narrow the date window or increase the limit");
        }
        if (pages != (total + config.pageSize - 1) / config.pageSize) {
            throw failure("Inconsistent pagination totals");
        }
        try {
            JsonNode rows = MAPPER.readTree(reply.body);
            long expected =
                    total == 0 && page == 1
                            ? 0
                            : Math.min(
                                    config.pageSize, total - (long) (page - 1) * config.pageSize);
            if (rows == null || !rows.isArray() || expected < 0 || rows.size() != expected) {
                throw failure("Response count does not match pagination headers");
            }
            for (JsonNode row : rows) {
                if (!row.isObject()
                        || !row.path("id").isIntegralNumber()
                        || !row.path("id").canConvertToLong()
                        || row.path("id").longValue() <= 0) {
                    throw failure("Invalid order record");
                }
            }
            return new Page(total, (int) pages, rows);
        } catch (IOException e) {
            throw failure("Invalid JSON order response (body withheld)");
        }
    }

    private static long number(String value, String name) {
        try {
            if (value == null || !value.matches("[0-9]{1,10}")) {
                throw new NumberFormatException();
            }
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw failure("Missing or invalid " + name + " pagination header");
        }
    }

    static int retryDelay(String value, int minimum) {
        if (value == null) {
            return minimum;
        }
        try {
            long millis;
            if (value.matches("[0-9]+")) {
                millis = Math.multiplyExact(Long.parseLong(value), 1000);
            } else {
                long seconds =
                        ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                        .toEpochSecond()
                                - Instant.now().getEpochSecond();
                millis = Math.multiplyExact(Math.max(0, seconds), 1000);
            }
            if (millis > 60000) {
                throw new IllegalArgumentException();
            }
            return Math.max(minimum, (int) millis);
        } catch (RuntimeException e) {
            throw failure(
                    "Retry-After exceeds bounded retry policy or is invalid; retry the job later");
        }
    }

    private static boolean transientStatus(int status) {
        return status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
    }

    private synchronized void retry(int attempt, int delay) throws InterruptedException {
        checkOpen();
        if (attempt >= config.retries) {
            throw failure("HTTP retry budget exhausted (transport details withheld)");
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay);
        long remaining;
        while (!closed && (remaining = deadline - System.nanoTime()) > 0) {
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
        checkOpen();
    }

    private void checkOpen() {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw failure("Request cancelled");
        }
    }

    static HttpConnectorException failure(String message) {
        return new HttpConnectorException(
                HttpConnectorErrorCode.REQUEST_FAILED, "WooCommerce: " + message);
    }

    /** Abort active IO and wake retry waits before releasing worker-local resources. */
    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (active != null) {
            active.abort();
        }
        notifyAll();
        timer.shutdownNow();
        client.close();
    }

    static final class Page {
        final long total;
        final int pages;
        final JsonNode rows;

        Page(long total, int pages, JsonNode rows) {
            this.total = total;
            this.pages = pages;
            this.rows = rows;
        }
    }

    private static final class Reply {
        final int status;
        final String retryAfter;
        final String total;
        final String pages;
        final byte[] body;

        Reply(int status, String retryAfter, String total, String pages, byte[] body) {
            this.status = status;
            this.retryAfter = retryAfter;
            this.total = total;
            this.pages = pages;
            this.body = body;
        }
    }
}
