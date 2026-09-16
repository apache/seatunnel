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

package org.apache.seatunnel.connectors.seatunnel.tiktok.ads;

import org.apache.seatunnel.common.utils.JsonUtils;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsReport.failure;

final class TikTokAdsClient implements Closeable {
    private final TikTokAdsConfig config;
    private final CloseableHttpClient client =
            HttpClients.custom()
                    .disableAutomaticRetries()
                    .disableRedirectHandling()
                    .disableCookieManagement()
                    .disableContentCompression()
                    .build();
    private final ScheduledThreadPoolExecutor deadlines =
            new ScheduledThreadPoolExecutor(
                    1,
                    task -> {
                        Thread thread = new Thread(task, "tiktok-ads-request-deadline");
                        thread.setDaemon(true);
                        return thread;
                    });
    private volatile boolean closed;
    private volatile HttpGet active;
    private long reportDeadline;

    TikTokAdsClient(TikTokAdsConfig config) {
        this.config = config;
        deadlines.setRemoveOnCancelPolicy(true);
    }

    void startReport() {
        reportDeadline =
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getReportTimeout());
    }

    void checkActive() throws IOException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw failure("request cancelled");
        }
        if (System.nanoTime() >= reportDeadline) {
            throw failure("report_timeout_ms exceeded");
        }
    }

    URI requestUri(int page) throws IOException {
        try {
            return new URIBuilder(config.getEndpoint())
                    .addParameter("advertiser_id", config.getAdvertiserId())
                    .addParameter("report_type", "BASIC")
                    .addParameter("service_type", "AUCTION")
                    .addParameter("data_level", "AUCTION_AD")
                    .addParameter("dimensions", JsonUtils.toJsonString(config.getDimensions()))
                    .addParameter("metrics", JsonUtils.toJsonString(config.getMetrics()))
                    .addParameter("start_date", config.getStartDate())
                    .addParameter("end_date", config.getEndDate())
                    .addParameter("query_lifetime", "false")
                    .addParameter("query_mode", "REGULAR")
                    .addParameter("page", Integer.toString(page))
                    .addParameter("page_size", Integer.toString(config.getPageSize()))
                    .build();
        } catch (URISyntaxException e) {
            throw failure("invalid endpoint");
        }
    }

    Response fetch(int page) throws IOException {
        checkActive();
        HttpGet request = new HttpGet(requestUri(page));
        active = request;
        ScheduledFuture<?> abort = null;
        try {
            checkActive();
            int timeout =
                    (int)
                            Math.max(
                                    1,
                                    Math.min(
                                            config.getTimeout(),
                                            TimeUnit.NANOSECONDS.toMillis(
                                                    reportDeadline - System.nanoTime())));
            request.setConfig(
                    RequestConfig.custom()
                            .setConnectTimeout(timeout)
                            .setConnectionRequestTimeout(timeout)
                            .setSocketTimeout(timeout)
                            .build());
            request.setHeader("Access-Token", config.getToken());
            request.setHeader("Accept", "application/json");
            abort = deadlines.schedule(request::abort, timeout, TimeUnit.MILLISECONDS);
            try (CloseableHttpResponse response = client.execute(request)) {
                // Conservative fail-closed policy: do not interpret undocumented header payloads.
                for (Header warning : response.getHeaders("X-Tt-Ads-Throttle")) {
                    if (!warning.getValue().trim().isEmpty()) {
                        request.abort();
                        throw new InvalidResponseException(
                                "nonempty X-Tt-Ads-Throttle; report may be truncated");
                    }
                }
                HttpEntity entity = response.getEntity();
                byte[] body = new byte[0];
                if (entity != null) {
                    try {
                        if (entity.getContentLength() > config.getMaxBytes()) {
                            throw new InvalidResponseException("max_response_bytes exceeded");
                        }
                        body = readBounded(entity.getContent(), config.getMaxBytes());
                    } catch (IOException e) {
                        request.abort();
                        throw e;
                    }
                }
                checkActive();
                if (request.isAborted()) {
                    throw failure("request_timeout_ms exceeded");
                }
                return new Response(
                        response.getStatusLine().getStatusCode(),
                        body,
                        response.getFirstHeader("Retry-After") == null
                                ? null
                                : response.getFirstHeader("Retry-After").getValue());
            }
        } catch (InvalidResponseException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            checkActive();
            throw failure(
                    request.isAborted() ? "request_timeout_ms exceeded" : "HTTP transport failed");
        } finally {
            if (abort != null) {
                abort.cancel(false);
            }
            request.abort();
            active = null;
        }
    }

    static byte[] readBounded(InputStream input, int limit) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream(Math.min(8192, limit));
        byte[] buffer = new byte[8192];
        int count;
        while ((count = input.read(buffer, 0, Math.min(buffer.length, limit - output.size() + 1)))
                != -1) {
            if (count > limit - output.size()) {
                throw new InvalidResponseException("max_response_bytes exceeded");
            }
            output.write(buffer, 0, count);
        }
        return output.toByteArray();
    }

    synchronized void awaitRetry(long millis) throws IOException {
        checkActive();
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < end) {
            try {
                TimeUnit.NANOSECONDS.timedWait(
                        this, Math.max(1, Math.min(end, reportDeadline) - System.nanoTime()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw failure("retry interrupted");
            }
            checkActive();
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        synchronized (this) {
            notifyAll();
        }
        HttpGet request = active;
        if (request != null) {
            request.abort();
        }
        deadlines.shutdownNow();
        client.close();
    }

    static final class Response {
        final int status;
        final byte[] body;
        final String retryAfter;

        Response(int status, byte[] body, String retryAfter) {
            this.status = status;
            this.body = body;
            this.retryAfter = retryAfter;
        }
    }

    private static final class InvalidResponseException extends IOException {
        InvalidResponseException(String message) {
            super("TikTokAds: " + message);
        }
    }
}
