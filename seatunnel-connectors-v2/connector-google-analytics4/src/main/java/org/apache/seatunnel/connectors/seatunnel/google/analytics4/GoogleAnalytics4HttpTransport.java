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

package org.apache.seatunnel.connectors.seatunnel.google.analytics4;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4Report.failure;

/** One bounded HTTP path for report requests and the Google library's token exchange. */
class GoogleAnalytics4HttpTransport extends HttpTransport implements Closeable {
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
                        Thread thread = new Thread(task, "google-analytics4-request-deadline");
                        thread.setDaemon(true);
                        return thread;
                    });
    private final int timeout;
    private final int maxBytes;
    private volatile boolean closed;
    private volatile HttpPost active;
    private volatile long reportDeadline = Long.MAX_VALUE;

    GoogleAnalytics4HttpTransport(GoogleAnalytics4Config config) {
        timeout = config.getTimeout();
        maxBytes = config.getMaxBytes();
        deadlines.setRemoveOnCancelPolicy(true);
    }

    void setReportDeadline(long deadline) {
        reportDeadline = deadline;
    }

    void checkActive() throws IOException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw failure("request cancelled");
        }
        if (System.nanoTime() >= reportDeadline) {
            throw failure("report_timeout_ms exceeded");
        }
    }

    Response post(String url, byte[] body, String token, int limit) throws IOException {
        checkActive();
        HttpPost request = new HttpPost(url);
        active = request;
        ScheduledFuture<?> abort = null;
        try {
            checkActive();
            long remaining =
                    reportDeadline == Long.MAX_VALUE
                            ? timeout
                            : TimeUnit.NANOSECONDS.toMillis(reportDeadline - System.nanoTime());
            int requestTimeout = (int) Math.max(1, Math.min(timeout, remaining));
            request.setConfig(
                    RequestConfig.custom()
                            .setConnectTimeout(requestTimeout)
                            .setConnectionRequestTimeout(requestTimeout)
                            .setSocketTimeout(requestTimeout)
                            .setRedirectsEnabled(false)
                            .build());
            request.setHeader(
                    "Content-Type",
                    token == null && url.endsWith("/token")
                            ? "application/x-www-form-urlencoded"
                            : "application/json");
            if (token != null) {
                request.setHeader("Authorization", "Bearer " + token);
            }
            request.setEntity(new ByteArrayEntity(body));
            abort = deadlines.schedule(request::abort, requestTimeout, TimeUnit.MILLISECONDS);
            try (CloseableHttpResponse response = client.execute(request)) {
                HttpEntity entity = response.getEntity();
                byte[] bytes = new byte[0];
                if (entity != null) {
                    try {
                        if (entity.getContentLength() > limit) {
                            throw new ResponseLimitException();
                        }
                        bytes = readBounded(entity.getContent(), limit);
                    } catch (ResponseLimitException e) {
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
                        bytes,
                        response.getFirstHeader("Retry-After") == null
                                ? null
                                : response.getFirstHeader("Retry-After").getValue());
            }
        } catch (ResponseLimitException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            checkActive();
            throw failure(
                    request.isAborted()
                            ? "request_timeout_ms exceeded"
                            : "HTTP transport request failed");
        } finally {
            if (abort != null) {
                abort.cancel(false);
            }
            request.abort();
            active = null;
        }
    }

    Response report(String url, byte[] body, String token) throws IOException {
        return post(url, body, token, maxBytes);
    }

    static byte[] readBounded(InputStream input, int limit) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream(Math.min(8192, limit));
        byte[] buffer = new byte[8192];
        int count;
        while ((count = input.read(buffer, 0, Math.min(buffer.length, limit - output.size() + 1)))
                != -1) {
            if (count > limit - output.size()) {
                throw new ResponseLimitException();
            }
            output.write(buffer, 0, count);
        }
        return output.toByteArray();
    }

    @Override
    public boolean supportsMethod(String method) {
        return "POST".equals(method);
    }

    @Override
    protected LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
        if (!"POST".equals(method) || !"https://oauth2.googleapis.com/token".equals(url)) {
            throw failure("unsupported token endpoint or method");
        }
        return new LowLevelHttpRequest() {
            @Override
            public void addHeader(String name, String value) {
                // Google auth headers are not needed for this fixed OAuth endpoint.
            }

            @Override
            public LowLevelHttpResponse execute() throws IOException {
                ByteArrayOutputStream body = new ByteArrayOutputStream();
                getStreamingContent().writeTo(body);
                Response response = post(url, body.toByteArray(), null, 65536);
                return new LowLevelHttpResponse() {
                    @Override
                    public InputStream getContent() {
                        return new ByteArrayInputStream(response.body);
                    }

                    @Override
                    public String getContentEncoding() {
                        return null;
                    }

                    @Override
                    public long getContentLength() {
                        return response.body.length;
                    }

                    @Override
                    public String getContentType() {
                        return "application/json";
                    }

                    @Override
                    public String getStatusLine() {
                        return "HTTP/1.1 " + response.status;
                    }

                    @Override
                    public int getStatusCode() {
                        return response.status;
                    }

                    @Override
                    public String getReasonPhrase() {
                        return "";
                    }

                    @Override
                    public int getHeaderCount() {
                        return 0;
                    }

                    @Override
                    public String getHeaderName(int index) {
                        return null;
                    }

                    @Override
                    public String getHeaderValue(int index) {
                        return null;
                    }
                };
            }
        };
    }

    @Override
    public void close() throws IOException {
        closed = true;
        HttpPost request = active;
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

    static final class ResponseLimitException extends IOException {
        ResponseLimitException() {
            super("GoogleAnalytics4: response size limit exceeded");
        }
    }
}
