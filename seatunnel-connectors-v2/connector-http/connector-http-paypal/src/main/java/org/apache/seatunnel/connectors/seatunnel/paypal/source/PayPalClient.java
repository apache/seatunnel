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

package org.apache.seatunnel.connectors.seatunnel.paypal.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

final class PayPalClient implements AutoCloseable {
    private final PayPalConfig config;
    private final CloseableHttpClient client;
    private final ScheduledThreadPoolExecutor timer;
    private volatile HttpRequestBase active;
    private volatile boolean closed;
    private String token;
    private long expiresAt;

    PayPalClient(PayPalConfig config) {
        this.config = config;
        client =
                HttpClients.custom()
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
                            Thread thread = new Thread(r, "paypal-request-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        timer.setRemoveOnCancelPolicy(true);
    }

    JsonNode page(int page) throws Exception {
        checkOpen();
        boolean refreshed = false;
        for (int attempt = 0; ; attempt++) {
            if (token == null || System.nanoTime() >= expiresAt) {
                authenticate();
            }
            HttpGet request =
                    new HttpGet(
                            new URIBuilder(config.origin + "/v1/reporting/transactions")
                                    .addParameter("start_date", config.start.toString())
                                    .addParameter("end_date", config.end.toString())
                                    .addParameter("fields", "all")
                                    .addParameter("balance_affecting_records_only", "N")
                                    .addParameter("page_size", Integer.toString(config.pageSize))
                                    .addParameter("page", Integer.toString(page))
                                    .build());
            request.setHeader("Authorization", "Bearer " + token);
            Reply reply;
            try {
                reply = execute(request);
            } catch (IOException e) {
                retry(attempt, config.retryDelay);
                continue;
            }
            if (reply.status == 401 && !refreshed) {
                token = null;
                refreshed = true;
                continue;
            }
            if (transientStatus(reply.status)) {
                retry(attempt, reply.delay);
                continue;
            }
            return success(reply);
        }
    }

    private void authenticate() throws Exception {
        for (int attempt = 0; ; attempt++) {
            HttpPost request = new HttpPost(config.origin + "/v1/oauth2/token");
            String basic =
                    Base64.getEncoder()
                            .encodeToString(
                                    (config.clientId + ":" + config.clientSecret)
                                            .getBytes(StandardCharsets.UTF_8));
            request.setHeader("Authorization", "Basic " + basic);
            request.setEntity(
                    new StringEntity(
                            "grant_type=client_credentials",
                            ContentType.APPLICATION_FORM_URLENCODED));
            long issuedAt = System.nanoTime();
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
            JsonNode root = success(reply);
            String accessToken = PayPalResponse.text(root, "access_token", true);
            int lifetime = PayPalResponse.integer(root, "expires_in");
            if (!"Bearer".equalsIgnoreCase(PayPalResponse.text(root, "token_type", true))
                    || lifetime < 1
                    || lifetime > 86400
                    || !accessToken.matches("[A-Za-z0-9._~+/-]+=*")
                    || accessToken.length() > 8192) {
                throw PayPalResponse.failure("Invalid OAuth token response");
            }
            // Count lifetime from before token exchange, never from after a slow response.
            expiresAt = issuedAt + TimeUnit.SECONDS.toNanos(lifetime);
            token = accessToken;
            return;
        }
    }

    private Reply execute(HttpRequestBase request) throws IOException {
        ScheduledFuture<?> deadline;
        synchronized (this) {
            checkOpen();
            active = request;
            deadline = timer.schedule(request::abort, config.timeout, TimeUnit.MILLISECONDS);
        }
        request.setHeader("Accept", "application/json");
        request.setHeader("PayPal-Enforce-ISO8601-Format", "true");
        CloseableHttpResponse response = null;
        try {
            response = client.execute(request);
            int status = response.getStatusLine().getStatusCode();
            int delay = config.retryDelay;
            if (transientStatus(status) && response.getFirstHeader("Retry-After") != null) {
                try {
                    long seconds =
                            Long.parseLong(response.getFirstHeader("Retry-After").getValue());
                    if (seconds < 0 || seconds > 60) {
                        throw new NumberFormatException();
                    }
                    delay = Math.max(delay, (int) seconds * 1000);
                } catch (NumberFormatException e) {
                    request.abort();
                    throw PayPalResponse.failure(
                            "Retry-After exceeds bounded retry policy or is unsupported; retry the job later");
                }
            }
            if (transientStatus(status) || status == 401 || status == 403) {
                return new Reply(status, new byte[0], delay);
            }
            if (response.getEntity() == null) {
                throw PayPalResponse.failure("Empty HTTP response");
            }
            if (response.getEntity().getContentLength() > config.maxBytes) {
                request.abort();
                throw PayPalResponse.failure("HTTP response exceeds max_response_bytes");
            }
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            {
                InputStream input = response.getEntity().getContent();
                byte[] buffer = new byte[8192];
                int count;
                while ((count = input.read(buffer)) != -1) {
                    checkOpen();
                    if (bytes.size() > config.maxBytes - count) {
                        request.abort();
                        throw PayPalResponse.failure("HTTP response exceeds max_response_bytes");
                    }
                    bytes.write(buffer, 0, count);
                }
            }
            return new Reply(status, bytes.toByteArray(), delay);
        } finally {
            // Abort before closing prevents draining oversized or malformed responses.
            request.abort();
            deadline.cancel(false);
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ignored) {
                    // The request is already aborted; never attach transport details to errors.
                }
            }
            synchronized (this) {
                active = null;
            }
        }
    }

    private JsonNode success(Reply reply) {
        if (reply.status == 401 || reply.status == 403) {
            throw PayPalResponse.failure(
                    "HTTP "
                            + reply.status
                            + "; check app credentials and Transaction Search permission (body withheld)");
        }
        JsonNode root = PayPalResponse.parse(reply.body);
        PayPalResponse.rejectError(root);
        if (reply.status != 200) {
            throw PayPalResponse.failure(
                    "HTTP "
                            + reply.status
                            + "; check app credentials and Transaction Search permission (body withheld)");
        }
        return root;
    }

    private static boolean transientStatus(int status) {
        return status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
    }

    private synchronized void retry(int attempt, int delay) throws InterruptedException {
        checkOpen();
        if (attempt >= config.retries) {
            throw PayPalResponse.failure(
                    "HTTP retry budget exhausted (transport details withheld)");
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
            throw PayPalResponse.failure("Request cancelled");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        token = null;
        if (active != null) {
            active.abort();
        }
        notifyAll();
        timer.shutdownNow();
        client.close();
    }

    private static final class Reply {
        private final int status;
        private final byte[] body;
        private final int delay;

        private Reply(int status, byte[] body, int delay) {
            this.status = status;
            this.body = body;
            this.delay = delay;
        }
    }
}
