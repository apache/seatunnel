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
package org.apache.seatunnel.connectors.seatunnel.babelforce;

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
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * A bounded read-only HTTPS client for Babelforce simple reporting.
 *
 * <p>Credentials are private headers on this one locally constructed route. Response diagnostics
 * omit upstream URLs and data, even when the provider refuses a request.
 */
final class BabelforceTransport implements AutoCloseable {
    static final String CALL_ROUTE = "/api/v2/calls/reporting/simple";
    private static final int MAX_CERT_BYTES = 65536;
    private final BabelforceOptions options;
    private final OkHttpClient http;
    private final HttpUrl origin;
    // The transport monitor protects the current transfer from concurrent stop calls.
    private Call call;
    private boolean closed;

    BabelforceTransport(BabelforceOptions options, BabelforceCheckpoint current)
            throws BabelforceException {
        this.options = options;
        origin = HttpUrl.get(options.origin);
        OkHttpClient.Builder builder =
                new OkHttpClient.Builder()
                        .proxy(Proxy.NO_PROXY)
                        .followRedirects(false)
                        .followSslRedirects(false)
                        .retryOnConnectionFailure(false)
                        .connectionPool(new ConnectionPool(1, 15, TimeUnit.SECONDS))
                        .connectTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .readTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .callTimeout(options.timeout, TimeUnit.MILLISECONDS)
                        .addNetworkInterceptor(
                                chain -> {
                                    Response response = chain.proceed(chain.request());
                                    String retry = response.header("Retry-After");
                                    // Disable a hidden OkHttp retry on a 503 with zero server
                                    // delay.
                                    if (response.code() == 503
                                            && retry != null
                                            && retry.matches("0+")) {
                                        return response.newBuilder()
                                                .removeHeader("Retry-After")
                                                .build();
                                    }
                                    return response;
                                });
        if (options.certificate != null) {
            installTrust(builder, options.certificate, current);
        }
        http = builder.build();
    }

    /**
     * Acquires the current page using the same window and identity on every attempt.
     *
     * <p>The deadline covers retries and response consumption, so a trickling body cannot keep a
     * worker busy indefinitely.
     */
    byte[] get(BabelforceCheckpoint current, long deadline)
            throws BabelforceException, InterruptedException {
        HttpUrl endpoint =
                origin.newBuilder()
                        .encodedPath(CALL_ROUTE)
                        .addQueryParameter("page", Integer.toString(current.page))
                        .addQueryParameter("max", Integer.toString(options.pageSize))
                        .addQueryParameter(
                                options.timePrefix + ".start", Long.toString(options.from))
                        .addQueryParameter(
                                options.timePrefix + ".end", Long.toString(current.until))
                        .build();
        Request.Builder request =
                new Request.Builder()
                        .url(endpoint)
                        .get()
                        .header("Accept", "application/json")
                        .header("User-Agent", "Apache-SeaTunnel-Babelforce/1.0");
        if (options.bearer) {
            request.header("Authorization", "Bearer " + options.token);
        } else {
            request.header("X-Auth-Access-ID", options.accessKey)
                    .header("X-Auth-Access-Token", options.token);
        }
        Request immutable = request.build();
        String exhausted = "transport retry budget exhausted";
        for (int attempt = 0; attempt < options.attempts; attempt++) {
            deadline(current, deadline);
            Call active = begin(immutable, current, deadline);
            long delay = 200L << attempt;
            try (Response response = active.execute()) {
                int status = response.code();
                if (status == 206 || response.header("Content-Range") != null) {
                    throw failure(current, "provider returned a partial HTTP call report");
                }
                if (status == 200) {
                    return data(response, current);
                }
                if (status == 401 || status == 403) {
                    throw failure(
                            current,
                            "HTTP " + status + " requires authorized call-report credentials");
                }
                if (status != 408 && status != 429 && status < 500) {
                    throw failure(current, "unexpected HTTP " + status);
                }
                exhausted = "HTTP " + status + " retry budget exhausted";
                delay = Math.max(delay, retryAfter(response.header("Retry-After")));
            } catch (BabelforceException safe) {
                throw safe;
            } catch (SSLException denied) {
                throw failure(current, "TLS certificate or hostname validation failed");
            } catch (IOException io) {
                deadline(current, deadline);
                if (active.isCanceled() || Thread.currentThread().isInterrupted()) {
                    throw failure(current, "request was cancelled");
                }
                exhausted = "transport retry budget exhausted";
            } finally {
                complete(active);
            }
            if (attempt + 1 < options.attempts) {
                if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
                    throw failure(current, "retry delay exceeds request deadline");
                }
                Thread.sleep(delay);
            }
        }
        throw failure(current, exhausted);
    }

    private byte[] data(Response reply, BabelforceCheckpoint current) throws IOException {
        ResponseBody body = reply.body();
        if (body == null) {
            throw failure(current, "missing call-report response body");
        }
        String encoding = reply.header("Content-Encoding");
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw failure(current, "unsupported response compression");
        }
        // Transparent gzip decoding removes the compressed size and encoding headers.
        if (body.contentLength() > options.maxBytes) {
            throw failure(current, "response exceeds max_response_bytes");
        }
        try (InputStream input = body.byteStream();
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] block = new byte[8192];
            for (int size; (size = input.read(block)) != -1; ) {
                if (size > options.maxBytes - output.size()) {
                    throw failure(current, "response exceeds max_response_bytes");
                }
                output.write(block, 0, size);
            }
            return output.toByteArray();
        }
    }

    private synchronized Call begin(Request request, BabelforceCheckpoint current, long deadline)
            throws BabelforceException {
        if (closed) {
            throw failure(current, "reader was cancelled");
        }
        call = http.newCall(request);
        call.timeout().deadlineNanoTime(deadline);
        return call;
    }

    private synchronized void complete(Call active) {
        if (call == active) {
            call = null;
        }
    }

    static void deadline(BabelforceCheckpoint current, long limit) throws BabelforceException {
        if (System.nanoTime() - limit >= 0L) {
            throw failure(current, "request deadline exceeded");
        }
    }

    /**
     * Cancels any currently streaming response and clears this reader's owned connections.
     *
     * <p>Trust, proxy and lifecycle configuration are local to the client.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (call != null) {
            call.cancel();
        }
        http.connectionPool().evictAll();
        http.dispatcher().executorService().shutdownNow();
    }

    private static void installTrust(
            OkHttpClient.Builder builder, String pem, BabelforceCheckpoint current)
            throws BabelforceException {
        try {
            Path certificate = Path.of(pem);
            if (!Files.isRegularFile(certificate)) {
                throw failure(current, "trust_certificate is not a regular file");
            }
            byte[] bytes;
            try (InputStream source = Files.newInputStream(certificate)) {
                bytes = source.readNBytes(MAX_CERT_BYTES + 1);
            }
            if (bytes.length == 0 || bytes.length > MAX_CERT_BYTES) {
                throw failure(current, "trust_certificate has an invalid size");
            }
            Collection<? extends Certificate> anchors =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(bytes));
            if (anchors.isEmpty() || anchors.size() > 16) {
                throw failure(current, "trust_certificate has an invalid certificate count");
            }
            KeyStore certificates = KeyStore.getInstance(KeyStore.getDefaultType());
            certificates.load(null);
            int counter = 0;
            for (Certificate anchor : anchors) {
                certificates.setCertificateEntry("babelforce-" + counter++, anchor);
            }
            TrustManagerFactory factory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(certificates);
            for (TrustManager trust : factory.getTrustManagers()) {
                if (trust instanceof X509TrustManager) {
                    SSLContext context = SSLContext.getInstance("TLS");
                    context.init(null, new TrustManager[] {trust}, null);
                    builder.sslSocketFactory(context.getSocketFactory(), (X509TrustManager) trust);
                    return;
                }
            }
        } catch (BabelforceException safe) {
            throw safe;
        } catch (IOException | GeneralSecurityException | RuntimeException invalid) {
            throw failure(current, "could not load trust_certificate");
        }
        throw failure(current, "could not establish X.509 trust");
    }

    private static long retryAfter(String header) {
        if (header == null || header.length() > 96) {
            return 0L;
        }
        try {
            if (header.matches("[0-9]+")) {
                return Math.multiplyExact(Long.parseLong(header), 1000L);
            }
            Instant when =
                    ZonedDateTime.parse(header, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            return Math.max(0L, Duration.between(Instant.now(), when).toMillis());
        } catch (ArithmeticException | NumberFormatException overflow) {
            return Long.MAX_VALUE;
        } catch (DateTimeException invalid) {
            return 0L;
        }
    }

    private static BabelforceException failure(BabelforceCheckpoint position, String problem) {
        return new BabelforceException(position.page, problem);
    }
}
