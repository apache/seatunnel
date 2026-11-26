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

package org.apache.seatunnel.connectors.seatunnel.sentry.source;

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.sentry.exception.SentryConnectorException;

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
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** A cancellable, size-bounded client for Sentry's header-based pagination contract. */
final class SentryClient implements AutoCloseable {
    static SentryConnectorException failure(String message) {
        return new SentryConnectorException(
                CommonErrorCodeDeprecated.HTTP_OPERATION_FAILED, message);
    }

    private final SentrySourceConfig config;
    private final CloseableHttpClient client;
    private final ScheduledThreadPoolExecutor timer;
    private volatile boolean closed;
    private HttpGet active;

    SentryClient(SentrySourceConfig config) {
        this.config = config;
        client =
                HttpClients.custom()
                        .setDefaultConnectionConfig(
                                ConnectionConfig.custom()
                                        .setMessageConstraints(
                                                MessageConstraints.custom()
                                                        .setMaxLineLength(16384)
                                                        .setMaxHeaderCount(64)
                                                        .build())
                                        .build())
                        .disableAutomaticRetries()
                        .disableRedirectHandling()
                        .disableCookieManagement()
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(config.timeout)
                                        .setSocketTimeout(config.timeout)
                                        .setConnectionRequestTimeout(config.timeout)
                                        .build())
                        .build();
        timer =
                new ScheduledThreadPoolExecutor(
                        1,
                        r -> {
                            Thread thread = new Thread(r, "sentry-source-request-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        timer.setRemoveOnCancelPolicy(true);
    }

    SentryPage page(String cursor) throws Exception {
        for (int attempt = 0; ; attempt++) {
            checkOpen();
            URIBuilder uri =
                    new URIBuilder(config.endpoint)
                            .addParameter("start", config.start.toString())
                            .addParameter("end", config.end.toString())
                            .addParameter("per_page", Integer.toString(config.pageSize))
                            .addParameter("full", "false")
                            .addParameter("sample", "false");
            if (cursor != null) {
                uri.addParameter("cursor", cursor);
            }
            HttpGet request = new HttpGet(uri.build());
            request.setHeader("Authorization", "Bearer " + config.token);
            request.setHeader("Accept", "application/json");
            Reply reply;
            try {
                reply = execute(request);
            } catch (IOException e) {
                retry(attempt, config.retryDelay);
                continue;
            }
            if (transientStatus(reply.status)) {
                retry(attempt, reply.delay);
                continue;
            }
            if (reply.status != 200) {
                throw failure(
                        "Sentry HTTP "
                                + reply.status
                                + "; check endpoint, project and project:read permission (response withheld)");
            }
            return new SentryPage(reply.body, reply.link, config.endpoint);
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
            if (status != 200) {
                int delay = config.retryDelay;
                if (transientStatus(status) && response.getFirstHeader("Retry-After") != null) {
                    delay =
                            retryDelay(
                                    response.getFirstHeader("Retry-After").getValue(),
                                    delay,
                                    Instant.now());
                }
                return new Reply(status, null, null, delay);
            }
            Header[] headers = response.getHeaders("Link");
            StringBuilder links = new StringBuilder();
            for (Header header : headers) {
                if (links.length() + header.getValue().length() + 2 > 16384) {
                    throw failure("Oversized Sentry Link header");
                }
                if (links.length() > 0) {
                    links.append(", ");
                }
                links.append(header.getValue());
            }
            if (response.getEntity() == null) {
                throw failure("Missing Sentry response body");
            }
            if (response.getEntity().getContentLength() > config.maxBytes) {
                throw failure("Sentry response exceeds max_response_bytes");
            }
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            InputStream input = response.getEntity().getContent();
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) != -1) {
                checkOpen();
                if (bytes.size() > config.maxBytes - count) {
                    throw failure("Sentry response exceeds max_response_bytes");
                }
                bytes.write(buffer, 0, count);
            }
            return new Reply(
                    status, bytes.toByteArray(), headers.length == 0 ? null : links.toString(), 0);
        } finally {
            request.abort();
            deadline.cancel(false);
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ignored) {
                    // Request already aborted; never attach transport details containing
                    // credentials.
                }
            }
            synchronized (this) {
                active = null;
            }
        }
    }

    static int retryDelay(String value, int minimum, Instant now) {
        long millis;
        try {
            if (value.matches("[0-9]+")) {
                millis = Math.multiplyExact(Long.parseLong(value), 1000L);
            } else {
                millis =
                        Math.max(
                                0,
                                Duration.between(
                                                now,
                                                ZonedDateTime.parse(
                                                                value,
                                                                DateTimeFormatter
                                                                        .RFC_1123_DATE_TIME)
                                                        .toInstant())
                                        .toMillis());
            }
            if (millis > 60000) {
                throw new IllegalArgumentException();
            }
        } catch (RuntimeException e) {
            throw failure("Retry-After exceeds 60 seconds or is invalid; retry the job later");
        }
        return Math.max(minimum, (int) millis);
    }

    private static boolean transientStatus(int status) {
        return status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
    }

    private synchronized void retry(int attempt, int delay) throws InterruptedException {
        checkOpen();
        if (attempt >= config.retries) {
            throw failure("Sentry HTTP retry budget exhausted (transport details withheld)");
        }
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay);
        long remaining;
        while (!closed && (remaining = end - System.nanoTime()) > 0) {
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
        checkOpen();
    }

    private void checkOpen() {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw failure("Sentry request cancelled");
        }
    }

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

    private static final class Reply {
        final int status;
        final byte[] body;
        final String link;
        final int delay;

        Reply(int status, byte[] body, String link, int delay) {
            this.status = status;
            this.body = body;
            this.link = link;
            this.delay = delay;
        }
    }
}
