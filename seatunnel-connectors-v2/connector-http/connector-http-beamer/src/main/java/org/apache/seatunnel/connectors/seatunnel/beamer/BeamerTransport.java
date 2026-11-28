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
package org.apache.seatunnel.connectors.seatunnel.beamer;

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
 * A reader-owned Beamer client with bounded retries and header-only authentication.
 *
 * <p>Provider messages, raw URLs and underlying transport exceptions stay inside this client. Calls
 * are cancellable while the worker is consuming or waiting for an HTTPS response.
 */
final class BeamerTransport implements AutoCloseable {
    private static final int MAX_CERTIFICATE_BYTES = 65536;
    private final BeamerConfig options;
    private final OkHttpClient client;
    private final HttpUrl origin;

    // Guarded by this instance while the engine may close an active request.
    private Call current;
    private boolean closed;

    BeamerTransport(BeamerConfig options, int page) throws BeamerReadException {
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
            useCustomTrust(builder, page);
        }
        client = builder.build();
    }

    /**
     * Fetches one read-only NPS page within the caller's absolute deadline.
     *
     * <p>Beamer documents an NPS 429 as the monthly account quota. Only the enumerated transient
     * transport and server failures are retried.
     */
    JsonNode fetch(BeamerSplit split, long deadline)
            throws BeamerReadException, InterruptedException {
        int page = split.page;
        HttpUrl url =
                origin.newBuilder()
                        .addPathSegment("v0")
                        .addPathSegment("nps")
                        .addQueryParameter("dateFrom", options.queryStart().toString())
                        .addQueryParameter(
                                "dateTo", BeamerConfig.queryEnd(split.windowEnd).toString())
                        .addQueryParameter("maxResults", Integer.toString(options.pageSize))
                        .addQueryParameter("page", Integer.toString(page))
                        .build();
        Request request =
                new Request.Builder()
                        .url(url)
                        .get()
                        .header("Beamer-Api-Key", options.token)
                        .header("Accept", "application/json")
                        .header("User-Agent", "Apache-SeaTunnel-Beamer/1.0")
                        .build();

        String exhausted = "transport retry budget exhausted";
        for (int attempt = 0; attempt < options.attempts; attempt++) {
            deadline(page, deadline);
            Call call = begin(page, request, deadline);
            long delay = 200L << attempt;
            try (Response response = call.execute()) {
                int status = response.code();
                if (status == 200) {
                    return body(response, page);
                }
                if (status == 429) {
                    throw fail(page, "HTTP 429 account request quota exhausted");
                }
                if (!retryable(status)) {
                    throw fail(page, "provider rejected the request (HTTP " + status + ")");
                }
                exhausted = "HTTP " + status + " retry budget exhausted";
                delay = Math.max(delay, retryAfter(response.header("Retry-After")));
            } catch (BeamerReadException failure) {
                throw failure;
            } catch (SSLException failure) {
                throw fail(page, "TLS certificate or hostname validation failed");
            } catch (IOException failure) {
                deadline(page, deadline);
                if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                    throw fail(page, "HTTPS request was cancelled");
                }
                exhausted = "transport retry budget exhausted";
            } finally {
                end(call);
            }
            if (attempt + 1 < options.attempts) {
                if (delay >= TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())) {
                    throw fail(page, "retry delay exceeds request deadline");
                }
                Thread.sleep(delay);
            }
        }
        throw fail(page, exhausted);
    }

    private static boolean retryable(int status) {
        return status == 408 || status == 500 || status == 502 || status == 503 || status == 504;
    }

    private JsonNode body(Response response, int page) throws IOException {
        ResponseBody body = response.body();
        if (body == null) {
            throw fail(page, "NPS response body is missing");
        }
        String encoding = response.header("Content-Encoding");
        if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
            throw fail(page, "unsupported response compression");
        }
        // Transparent gzip decoding removes Content-Length and Content-Encoding.
        if (body.contentLength() > options.maxBytes) {
            throw fail(page, "response exceeds max_response_bytes");
        }
        try (InputStream stream = body.byteStream();
                ByteArrayOutputStream decoded = new ByteArrayOutputStream()) {
            byte[] block = new byte[8192];
            for (int count; (count = stream.read(block)) != -1; ) {
                if (count > options.maxBytes - decoded.size()) {
                    throw fail(page, "response exceeds max_response_bytes");
                }
                decoded.write(block, 0, count);
            }
            return BeamerPage.parse(decoded.toByteArray(), page);
        }
    }

    private synchronized Call begin(int page, Request request, long deadline)
            throws BeamerReadException {
        if (closed) {
            throw fail(page, "HTTPS request was cancelled");
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

    static void deadline(int page, long deadline) throws BeamerReadException {
        if (System.nanoTime() - deadline >= 0L) {
            throw fail(page, "request deadline exceeded");
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

    private void useCustomTrust(OkHttpClient.Builder builder, int page) throws BeamerReadException {
        try {
            Path path = Path.of(options.trustCertificate);
            if (!Files.isRegularFile(path)) {
                throw fail(page, "trust_certificate is not a regular file");
            }
            byte[] pem;
            try (InputStream input = Files.newInputStream(path)) {
                pem = input.readNBytes(MAX_CERTIFICATE_BYTES + 1);
            }
            if (pem.length == 0 || pem.length > MAX_CERTIFICATE_BYTES) {
                throw fail(page, "trust_certificate has an invalid size");
            }
            Collection<? extends Certificate> certificates =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(pem));
            if (certificates.isEmpty() || certificates.size() > 16) {
                throw fail(page, "trust_certificate must contain a bounded X.509 chain");
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
        } catch (BeamerReadException failure) {
            throw failure;
        } catch (IOException | GeneralSecurityException | RuntimeException failure) {
            throw fail(page, "could not load trust_certificate");
        }
        throw fail(page, "could not establish X.509 trust");
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

    private static BeamerReadException fail(int page, String detail) {
        return new BeamerReadException(page, detail);
    }
}
