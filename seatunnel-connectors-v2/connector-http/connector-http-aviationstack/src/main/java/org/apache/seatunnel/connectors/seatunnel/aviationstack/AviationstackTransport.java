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
package org.apache.seatunnel.connectors.seatunnel.aviationstack;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * A reader-owned Aviationstack client with bounded retries and protected query credentials.
 *
 * <p>Provider messages, raw URLs and underlying transport exceptions stay inside this client. Calls
 * are cancellable while the worker is consuming or waiting for an HTTPS response.
 */
final class AviationstackTransport implements AutoCloseable {
    private static final int MAX_ERROR_BYTES = 16384;
    private static final int MAX_CERTIFICATE_BYTES = 65536;
    private final AviationstackConfig options;
    private final OkHttpClient client;
    private final HttpUrl origin;

    // Guarded by this instance while the engine may close an active request.
    private Call current;
    private boolean closed;

    AviationstackTransport(AviationstackConfig options, AviationstackResource catalog, long offset)
            throws AviationstackReadException {
        this.options = options;
        origin = HttpUrl.get(options.origin);
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .proxy(Proxy.NO_PROXY)
                        .followRedirects(false)
                        .followSslRedirects(false)
                        .retryOnConnectionFailure(false)
                        .connectionPool(new ConnectionPool(1, 20, TimeUnit.SECONDS))
                        .connectTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .readTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .callTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .addNetworkInterceptor(
                                chain -> {
                                    Response response = chain.proceed(chain.request());
                                    String retry = response.header("Retry-After");
                                    // OkHttp otherwise replays 503 with Retry-After: 0 internally.
                                    // An
                                    // independent retry would consume an attempt outside the
                                    // request budget.
                                    if (response.code() == 503
                                            && retry != null
                                            && retry.matches("0+")) {
                                        return response.newBuilder()
                                                .removeHeader("Retry-After")
                                                .build();
                                    }
                                    return response;
                                });
        if (options.trustCertificate != null) {
            useCustomTrust(builder, catalog, offset);
        }
        client = builder.build();
    }

    /**
     * Fetches one page within the caller's absolute deadline.
     *
     * <p>A positive provider quota error wins over the HTTP status. Only recognized burst
     * throttling, transient transport errors and server failures get another attempt.
     */
    JsonNode fetch(AviationstackResource catalog, long offset, long deadline)
            throws AviationstackReadException, InterruptedException {
        HttpUrl url =
                origin.newBuilder()
                        .addPathSegment("v1")
                        .addPathSegment(catalog.name)
                        .addQueryParameter("access_key", options.accessKey)
                        .addQueryParameter("limit", Integer.toString(options.pageSize))
                        .addQueryParameter("offset", Long.toString(offset))
                        .build();
        Request request =
                new Request.Builder()
                        .url(url)
                        .get()
                        .header("Accept", "application/json")
                        .header("User-Agent", "Apache-SeaTunnel-Aviationstack/1.0")
                        .build();
        String exhausted = "transport retry budget exhausted";
        for (int attempt = 0; attempt < options.attempts; attempt++) {
            deadline(catalog, offset, deadline);
            Call call = begin(catalog, offset, request, deadline);
            long delay = 200L << attempt;
            try (Response response = call.execute()) {
                int status = response.code();
                boolean success = status == 200;
                JsonNode envelope = body(response, catalog, offset, success);
                String error = recognizedError(envelope);
                if (isQuota(error)) {
                    throw fail(catalog, offset, "account request quota exhausted (" + error + ")");
                }
                boolean throttled = "rate_limit_reached".equals(error);
                if (throttled
                        || "internal_error".equals(error)
                        || "maintenance_mode".equals(error)
                        || (error == null && (status == 408 || status >= 500))) {
                    exhausted =
                            throttled
                                    ? "rate_limit_reached retry budget exhausted"
                                    : "HTTP " + status + " retry budget exhausted";
                    delay = Math.max(delay, retryAfter(response.header("Retry-After")));
                } else if (success
                        && error == null
                        && envelope != null
                        && envelope.isObject()
                        && !envelope.hasNonNull("error")
                        && (!envelope.has("success")
                                || (envelope.path("success").isBoolean()
                                        && envelope.path("success").booleanValue()))) {
                    return envelope;
                } else {
                    throw fail(catalog, offset, rejection(status, error));
                }
            } catch (AviationstackReadException failure) {
                throw failure;
            } catch (SSLException failure) {
                throw fail(catalog, offset, "TLS certificate or hostname validation failed");
            } catch (IOException failure) {
                deadline(catalog, offset, deadline);
                if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                    throw fail(catalog, offset, "HTTPS request was cancelled");
                }
                exhausted = "transport retry budget exhausted";
            } finally {
                end(call);
            }
            if (attempt + 1 < options.attempts) {
                // Watch both the initial request and all subsequent retry delays against one clock.
                if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
                    throw fail(catalog, offset, "retry delay exceeds request deadline");
                }
                Thread.sleep(delay);
            }
        }
        throw fail(catalog, offset, exhausted);
    }

    private JsonNode body(
            Response response, AviationstackResource catalog, long offset, boolean success)
            throws IOException {
        ResponseBody body = response.body();
        int maximum = success ? options.maxBytes : MAX_ERROR_BYTES;
        if (body == null) {
            return null;
        }
        String encoding = response.header("Content-Encoding");
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            if (success) {
                throw fail(catalog, offset, "unsupported response compression");
            }
            return null;
        }
        // Transparent gzip decoding removes Content-Encoding and Content-Length before this point.
        if (body.contentLength() > maximum) {
            if (success) {
                throw fail(catalog, offset, "response exceeds max_response_bytes");
            }
            return null;
        }
        try (InputStream stream = body.byteStream();
                ByteArrayOutputStream decoded = new ByteArrayOutputStream()) {
            byte[] block = new byte[8192];
            for (int count; (count = stream.read(block)) != -1; ) {
                if (count > maximum - decoded.size()) {
                    if (success) {
                        throw fail(catalog, offset, "response exceeds max_response_bytes");
                    }
                    return null;
                }
                decoded.write(block, 0, count);
            }
            return AviationstackPage.parse(decoded.toByteArray());
        }
    }

    private static String recognizedError(JsonNode body) {
        if (body == null || !body.path("error").isObject()) {
            return null;
        }
        JsonNode error = body.path("error");
        String type = classify(error.path("type").asText());
        String code = classify(error.path("code").asText());
        if (isQuota(type)) {
            return type;
        }
        return isQuota(code) || type == null ? code : type;
    }

    private static String classify(String value) {
        switch (value) {
            case "usage_limit_reached":
            case "daily_usage_limit_reached":
            case "fair_use_limit_reached":
            case "rate_limit_reached":
            case "account_on_hold":
            case "invalid_access_key":
            case "missing_access_key":
            case "inactive_user":
            case "https_access_restricted":
            case "function_access_restricted":
            case "api_access_blocked":
            case "invalid_api_function":
            case "404_not_found":
            case "internal_error":
            case "maintenance_mode":
            case "validation_error":
                return value;
            default:
                return null;
        }
    }

    private static boolean isQuota(String error) {
        return "usage_limit_reached".equals(error)
                || "daily_usage_limit_reached".equals(error)
                || "fair_use_limit_reached".equals(error);
    }

    private static String rejection(int status, String known) {
        if ("function_access_restricted".equals(known)) {
            return "the account plan denied this resource (function_access_restricted)";
        }
        if (known != null) {
            return "provider rejected the request (" + known + ")";
        }
        if (status == 200) {
            return "provider did not return a successful catalog envelope";
        }
        if (status == 429) {
            return "HTTP 429 without a recognized transient rate limit";
        }
        return "provider rejected the request (HTTP " + status + ")";
    }

    private synchronized Call begin(
            AviationstackResource catalog, long offset, Request request, long deadline)
            throws AviationstackReadException {
        if (closed) {
            throw fail(catalog, offset, "HTTPS request was cancelled");
        }
        current = client.newCall(request);
        current.timeout().deadlineNanoTime(deadline);
        return current;
    }

    private synchronized void end(Call finished) {
        if (current == finished) {
            current = null;
        }
    }

    static void deadline(AviationstackResource catalog, long offset, long deadline)
            throws AviationstackReadException {
        if (System.nanoTime() - deadline >= 0L) {
            throw fail(catalog, offset, "request deadline exceeded");
        }
    }

    /**
     * Cancels the active HTTPS transfer and releases this reader's transport resources.
     *
     * <p>The connector does not alter JVM-wide trust or connection settings.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (current != null) {
            current.cancel();
        }
        client.connectionPool().evictAll();
        client.dispatcher().executorService().shutdownNow();
    }

    private void useCustomTrust(
            OkHttpClient.Builder builder, AviationstackResource catalog, long offset)
            throws AviationstackReadException {
        try {
            Path path = Path.of(options.trustCertificate);
            if (!Files.isRegularFile(path)) {
                throw fail(catalog, offset, "trust_certificate is not a regular file");
            }
            byte[] pem;
            try (InputStream input = Files.newInputStream(path)) {
                pem = input.readNBytes(MAX_CERTIFICATE_BYTES + 1);
            }
            if (pem.length == 0 || pem.length > MAX_CERTIFICATE_BYTES) {
                throw fail(catalog, offset, "trust_certificate has an invalid size");
            }
            Collection<? extends Certificate> certificates =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(pem));
            if (certificates.isEmpty() || certificates.size() > 16) {
                throw fail(catalog, offset, "trust_certificate must contain a bounded X.509 chain");
            }
            KeyStore anchors = KeyStore.getInstance(KeyStore.getDefaultType());
            anchors.load(null);
            int index = 0;
            for (Certificate certificate : certificates) {
                anchors.setCertificateEntry("anchor-" + index++, certificate);
            }
            TrustManagerFactory factory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(anchors);
            for (TrustManager manager : factory.getTrustManagers()) {
                if (manager instanceof X509TrustManager) {
                    SSLContext context = SSLContext.getInstance("TLS");
                    context.init(null, new TrustManager[] {manager}, null);
                    builder.sslSocketFactory(
                            context.getSocketFactory(), (X509TrustManager) manager);
                    return;
                }
            }
        } catch (AviationstackReadException failure) {
            throw failure;
        } catch (IOException | GeneralSecurityException | RuntimeException failure) {
            throw fail(catalog, offset, "could not load trust_certificate");
        }
        throw fail(catalog, offset, "could not establish X.509 trust");
    }

    private static long retryAfter(String header) {
        if (header == null || header.length() > 96) {
            return 0L;
        }
        try {
            if (header.matches("[0-9]+")) {
                return Math.multiplyExact(Long.parseLong(header), 1000L);
            }
            return Math.max(
                    0L,
                    Duration.between(
                                    Instant.now(),
                                    ZonedDateTime.parse(
                                                    header, DateTimeFormatter.RFC_1123_DATE_TIME)
                                            .toInstant())
                            .toMillis());
        } catch (ArithmeticException | NumberFormatException tooLarge) {
            return Long.MAX_VALUE;
        } catch (RuntimeException invalid) {
            return 0L;
        }
    }

    private static AviationstackReadException fail(
            AviationstackResource catalog, long offset, String detail) {
        return new AviationstackReadException(catalog, offset, detail);
    }
}
