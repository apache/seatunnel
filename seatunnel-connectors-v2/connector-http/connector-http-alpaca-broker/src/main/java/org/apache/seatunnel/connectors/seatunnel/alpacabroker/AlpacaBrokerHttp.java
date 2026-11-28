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

package org.apache.seatunnel.connectors.seatunnel.alpacabroker;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ProtocolException;
import java.net.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Reader-owned HTTP transport with a total request budget and redacted diagnostics.
 *
 * <p>The transport encapsulates TLS trust, credential headers, response size limits, and
 * cancellation.
 */
final class AlpacaBrokerHttp implements AutoCloseable {
    static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final Set<Integer> RETRYABLE = Set.of(408, 429, 500, 502, 503, 504);
    private static final int MAX_PEM_BYTES = 65536;

    private final AlpacaBrokerSettings settings;
    private final OkHttpClient client;
    private final HttpUrl origin;
    private final String authorization;
    private boolean stopped;
    private Call active;

    AlpacaBrokerHttp(AlpacaBrokerSettings settings) throws IOException {
        this.settings = settings;
        origin = HttpUrl.get(settings.origin);
        authorization = Credentials.basic(settings.username, settings.password);
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .followRedirects(false)
                        .followSslRedirects(false)
                        .retryOnConnectionFailure(false)
                        .protocols(List.of(Protocol.HTTP_1_1))
                        .proxy(Proxy.NO_PROXY)
                        .addNetworkInterceptor(
                                chain -> {
                                    Response response = chain.proceed(chain.request());
                                    // OkHttp can transparently reissue a 503 with Retry-After: 0.
                                    // Move all 503s into this connector's explicit attempt/deadline
                                    // policy.
                                    if (response.code() == 503) {
                                        long delay = providerDelay(response, 503);
                                        response.close();
                                        throw new ServiceUnavailable(delay);
                                    }
                                    return response;
                                })
                        .connectionPool(new ConnectionPool(1, 1L, TimeUnit.MINUTES))
                        .connectTimeout(settings.timeoutMs, TimeUnit.MILLISECONDS)
                        .readTimeout(settings.timeoutMs, TimeUnit.MILLISECONDS)
                        .callTimeout(settings.timeoutMs, TimeUnit.MILLISECONDS);
        if (settings.trustCertificate != null) {
            installTrustAnchor(builder, settings.trustCertificate);
        }
        client = builder.build();
    }

    JsonNode get(String operation, List<String> path, Map<String, String> query, long deadline)
            throws IOException, InterruptedException {
        HttpUrl.Builder target = origin.newBuilder();
        path.forEach(target::addPathSegment);
        query.forEach(target::addQueryParameter);
        Request request =
                new Request.Builder()
                        .url(target.build())
                        .header("Accept", "application/json")
                        .header("Authorization", authorization)
                        .get()
                        .build();
        IOException last = failure(operation, "request could not start");
        for (int attempt = 1; attempt <= settings.maxAttempts; attempt++) {
            requireTime(operation, deadline);
            long wait = 200L << (attempt - 1);
            Call call = begin(request, deadline);
            try (Response response = call.execute()) {
                int status = response.code();
                if (status == 200) {
                    byte[] bytes = readBody(response, operation);
                    requireTime(operation, deadline);
                    JsonNode document;
                    try {
                        document = JSON.readTree(bytes);
                    } catch (IOException | RuntimeException e) {
                        throw failure(operation, "response is not a complete JSON document");
                    }
                    requireTime(operation, deadline);
                    if (document == null || document.isNull()) {
                        throw failure(operation, "response is missing its JSON payload");
                    }
                    return document;
                }
                last = failure(operation, "HTTP " + status);
                if (!RETRYABLE.contains(status)) {
                    throw last;
                }
                wait = Math.max(wait, providerDelay(response, status));
            } catch (ServiceUnavailable e) {
                last = failure(operation, "HTTP 503");
                wait = Math.max(wait, e.delay);
            } catch (ReadFailure e) {
                throw e;
            } catch (SSLException | ProtocolException e) {
                throw failure(operation, "TLS trust or HTTP protocol validation failed");
            } catch (IOException e) {
                if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                    requireTime(operation, deadline);
                    throw failure(operation, "request cancelled");
                }
                last = failure(operation, "transport failure");
            } finally {
                end(call);
            }
            if (attempt == settings.maxAttempts) {
                throw last;
            }
            long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (wait >= remaining) {
                throw failure(operation, "retry delay exceeds request deadline");
            }
            Thread.sleep(wait);
        }
        throw last;
    }

    private byte[] readBody(Response response, String operation) throws IOException {
        ResponseBody body = response.body();
        String encoding = response.header("Content-Encoding");
        // OkHttp expands supported gzip responses and removes their encoding and length headers.
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw failure(operation, "unsupported response content encoding");
        }
        if (body == null) {
            throw failure(operation, "response body is missing");
        }
        if (body.contentLength() > settings.maxResponseBytes) {
            throw failure(operation, "response exceeds max_response_bytes");
        }
        byte[] bytes;
        try (InputStream stream = body.byteStream()) {
            bytes = stream.readNBytes(settings.maxResponseBytes + 1);
        }
        if (bytes.length > settings.maxResponseBytes) {
            throw failure(operation, "response exceeds max_response_bytes");
        }
        return bytes;
    }

    private static long providerDelay(Response response, int status) {
        Instant now = Instant.now();
        long delay = 0L;
        String retry = response.header("Retry-After");
        if (retry != null) {
            try {
                delay = millisFromSeconds(Long.parseLong(retry.trim()));
            } catch (NumberFormatException e) {
                try {
                    delay =
                            betterDelay(
                                    now,
                                    ZonedDateTime.parse(retry, DateTimeFormatter.RFC_1123_DATE_TIME)
                                            .toInstant());
                } catch (DateTimeException ignored) {
                    // Invalid headers do not replace the connector backoff.
                }
            }
        }
        if (status == 429) {
            String reset = response.header("X-RateLimit-Reset");
            if (reset != null) {
                try {
                    delay =
                            Math.max(
                                    delay,
                                    betterDelay(
                                            now,
                                            Instant.ofEpochSecond(Long.parseLong(reset.trim()))));
                } catch (NumberFormatException | DateTimeException ignored) {
                    // A retry-after value or connector backoff still applies.
                }
            }
        }
        return delay;
    }

    private static long betterDelay(Instant now, Instant until) {
        Duration remaining = Duration.between(now, until);
        if (remaining.isNegative()) {
            return 0L;
        }
        if (remaining.getSeconds() > 300L) {
            return Long.MAX_VALUE;
        }
        return remaining.toMillis() + 1L;
    }

    private static long millisFromSeconds(long seconds) {
        return seconds <= 0L ? 0L : seconds <= 300L ? seconds * 1000L : Long.MAX_VALUE;
    }

    private synchronized Call begin(Request request, long deadline) throws IOException {
        if (stopped) {
            throw failure("HTTP", "reader was closed");
        }
        Call call = client.newCall(request);
        call.timeout().deadlineNanoTime(deadline);
        active = call;
        return call;
    }

    private synchronized void end(Call finished) {
        if (active == finished) {
            active = null;
        }
    }

    private static void requireTime(String operation, long deadline) throws IOException {
        if (System.nanoTime() - deadline >= 0L) {
            throw failure(operation, "request deadline exceeded");
        }
    }

    private static void installTrustAnchor(OkHttpClient.Builder builder, String file)
            throws IOException {
        try {
            Path path = Path.of(file);
            if (!Files.isRegularFile(path)) {
                throw failure("TLS", "trust_certificate must be a regular PEM file");
            }
            byte[] contents;
            try (InputStream input = Files.newInputStream(path)) {
                contents = input.readNBytes(MAX_PEM_BYTES + 1);
            }
            if (contents.length > MAX_PEM_BYTES) {
                throw failure("TLS", "trust_certificate exceeds its size limit");
            }
            Collection<? extends Certificate> anchors =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(contents));
            if (anchors.size() != 1 || !(anchors.iterator().next() instanceof X509Certificate)) {
                throw failure("TLS", "trust_certificate must contain one X.509 certificate");
            }
            X509Certificate anchor = (X509Certificate) anchors.iterator().next();
            anchor.checkValidity();
            KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
            trust.load(null, null);
            trust.setCertificateEntry("alpaca-broker", anchor);
            TrustManagerFactory managers =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            managers.init(trust);
            X509TrustManager manager = (X509TrustManager) managers.getTrustManagers()[0];
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, new X509TrustManager[] {manager}, null);
            builder.sslSocketFactory(context.getSocketFactory(), manager);
        } catch (ReadFailure e) {
            throw e;
        } catch (Exception e) {
            throw failure("TLS", "could not load or validate trust_certificate");
        }
    }

    /**
     * Cancel the only in-flight call and evict idle connections without waiting for a body.
     *
     * <p>Marking the transport stopped prevents a new call after cancellation.
     */
    @Override
    public synchronized void close() {
        if (!stopped) {
            stopped = true;
            if (active != null) {
                active.cancel();
            }
            client.connectionPool().evictAll();
        }
    }

    static ReadFailure failure(String operation, String detail) {
        return new ReadFailure("AlpacaBroker " + operation + ": " + detail);
    }

    /**
     * Carries a 503 through the underlying automatic HTTP follow-up layer.
     *
     * <p>This lets the connector own every service-unavailable retry and provider delay.
     */
    private static final class ServiceUnavailable extends IOException {
        private static final long serialVersionUID = 1L;
        private final long delay;

        private ServiceUnavailable(long delay) {
            this.delay = delay;
        }
    }

    /**
     * A fixed diagnostic with no underlying URI, credential, or response content.
     *
     * <p>Runtime failures carry only the operation label and a controlled reason.
     */
    static final class ReadFailure extends IOException {
        private static final long serialVersionUID = 1L;

        private ReadFailure(String message) {
            super(message);
        }
    }
}
