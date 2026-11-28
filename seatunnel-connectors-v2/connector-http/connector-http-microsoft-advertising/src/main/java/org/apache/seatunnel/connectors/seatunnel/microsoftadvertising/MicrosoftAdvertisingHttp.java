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
package org.apache.seatunnel.connectors.seatunnel.microsoftadvertising;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.cfg.JsonNodeFeature;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.DateTimeException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingConfig.BULK_HOST;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingConfig.CAMPAIGN_HOST;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingConfig.CUSTOMER_HOST;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingConfig.REPORT_HOST;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingConfig.TOKEN_HOST;

/**
 * Quiet, synchronous HTTPS transport for v13, Entra, and provider-signed files.
 *
 * <p>Each phase builds independent headers. The source closes the active socket when its deadline
 * expires or the reader is cancelled. No HTTP redirects are followed.
 */
final class MicrosoftAdvertisingHttp implements AutoCloseable {
    private static final MessageConstraints HEADERS =
            MessageConstraints.custom().setMaxLineLength(32768).setMaxHeaderCount(64).build();
    private static final Set<Integer> TRANSIENT = Set.of(408, 429, 500, 502, 503, 504);
    private static final Set<String> API_HOSTS =
            Set.of(CUSTOMER_HOST, CAMPAIGN_HOST, REPORT_HOST, BULK_HOST);
    private final MicrosoftAdvertisingConfig config;
    private final SSLSocketFactory tls;
    private final ScheduledThreadPoolExecutor watchdog;
    final ObjectMapper json;

    private volatile boolean closed;
    private Socket active;
    private MicrosoftAdvertisingFile heldFile;

    MicrosoftAdvertisingHttp(MicrosoftAdvertisingConfig config) {
        this.config = config;
        tls = config.socketFactory();
        JsonFactory factory =
                new JsonFactory()
                        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                        .setStreamReadConstraints(
                                StreamReadConstraints.builder()
                                        .maxNestingDepth(100)
                                        .maxNumberLength(1000)
                                        .maxStringLength(config.jsonBytes)
                                        .build());
        json =
                new ObjectMapper(factory)
                        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                        .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
        watchdog =
                new ScheduledThreadPoolExecutor(
                        1,
                        task -> {
                            Thread thread =
                                    new Thread(task, "microsoft-advertising-https-deadline");
                            thread.setDaemon(true);
                            return thread;
                        });
        watchdog.setRemoveOnCancelPolicy(true);
    }

    static long deadline(int milliseconds) {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(milliseconds);
    }

    Reply token(byte[] form, long deadline) throws InterruptedException {
        Request request =
                new Request(
                        TOKEN_HOST,
                        "/" + config.tenant + "/oauth2/v2.0/token",
                        null,
                        null,
                        form,
                        Mode.GRANT);
        return json(request, deadline);
    }

    Reply post(
            String host,
            String path,
            String bearer,
            MicrosoftAdvertisingSplit.Account account,
            JsonNode payload,
            boolean create,
            long deadline)
            throws InterruptedException {
        if (!API_HOSTS.contains(host)) {
            throw new IllegalArgumentException("Unknown Microsoft Advertising API host");
        }
        byte[] body;
        try {
            body = json.writeValueAsBytes(payload);
        } catch (JsonProcessingException ex) {
            throw new MicrosoftAdvertisingFailure("could not encode API request");
        }
        Request request =
                new Request(host, path, bearer, account, body, create ? Mode.SUBMIT : Mode.READ);
        return json(request, deadline);
    }

    private Reply json(Request request, long deadline) throws InterruptedException {
        return execute(
                request,
                deadline,
                (response, expires) -> {
                    int status = response.getStatusLine().getStatusCode();
                    if (status == 401
                            || status == 403
                            || status == 404
                            || status >= 300 && status < 400) {
                        return new Reply(status, null, 0);
                    }
                    if (status != 200 && status != 400 && status != 409) {
                        return new Reply(status, null, 0);
                    }
                    int limit =
                            request.mode == Mode.GRANT
                                    ? Math.min(config.jsonBytes, 65536)
                                    : config.jsonBytes;
                    JsonNode result = jsonBody(response, expires, limit);
                    return new Reply(status, result, retryAfter(response));
                });
    }

    /**
     * Opens a provider-produced location using a fresh request with no credential headers.
     *
     * <p>Only explicit expiration-like statuses allow the caller to get a new signed URL by polling
     * the existing provider job.
     */
    MicrosoftAdvertisingFile download(
            String location,
            MicrosoftAdvertisingCatalog.Resource resource,
            MicrosoftAdvertisingSplit split,
            long deadline)
            throws InterruptedException {
        URI uri = signedLocation(location);
        String target =
                uri.getRawPath() + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
        Request request = new Request(uri.getHost(), target, null, null, null, Mode.DOWNLOAD);
        return execute(
                request,
                deadline,
                (response, expires) -> {
                    int code = response.getStatusLine().getStatusCode();
                    if (code == 401 || code == 403 || code == 404) {
                        return null;
                    }
                    if (code != 200) {
                        throw new MicrosoftAdvertisingFailure(
                                "signed download returned HTTP " + code);
                    }
                    Header[] encodings = response.getHeaders("Content-Encoding");
                    boolean gzip = gzip(encodings);
                    HttpEntity entity = response.getEntity();
                    if (entity == null || entity.getContentLength() > config.compressedBytes) {
                        throw new MicrosoftAdvertisingFailure(
                                "missing signed body or max_compressed_bytes exceeded");
                    }
                    InputStream wire =
                            new WireLimit(
                                    entity.getContent(),
                                    config.compressedBytes,
                                    "max_compressed_bytes");
                    if (gzip && resource.kind == MicrosoftAdvertisingCatalog.Kind.REPORT) {
                        wire = new GZIPInputStream(wire);
                    }
                    MicrosoftAdvertisingFile file =
                            MicrosoftAdvertisingFile.read(
                                    config, resource, split, json, wire, () -> remaining(expires));
                    synchronized (this) {
                        if (closed) {
                            file.close();
                            throw new InterruptedException("MicrosoftAdvertising source closed");
                        }
                        heldFile = file;
                        return file;
                    }
                });
    }

    private static URI signedLocation(String location) {
        try {
            if (location == null || location.length() > 16384) {
                throw new MicrosoftAdvertisingFailure("invalid signed download URL");
            }
            URI uri = new URI(location);
            String host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(Locale.ROOT);
            if (!"https".equalsIgnoreCase(uri.getScheme())
                    || host.length() > 253
                    || !host.matches(
                            "(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\\.)+[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?")
                    || host.endsWith(".localhost")
                    || host.endsWith(".local")
                    || uri.getPort() != -1 && uri.getPort() != 443
                    || uri.getRawUserInfo() != null
                    || uri.getRawFragment() != null
                    || uri.getRawPath().isEmpty()
                    || uri.getRawPath().startsWith("//")
                    || URI.create(uri.toASCIIString()).getHost() == null) {
                throw new MicrosoftAdvertisingFailure(
                        "invalid signed download HTTPS origin or path");
            }
            return URI.create(uri.toASCIIString());
        } catch (URISyntaxException | IllegalArgumentException ex) {
            throw new MicrosoftAdvertisingFailure("invalid signed download URL");
        }
    }

    private <T> T execute(Request request, long deadline, ResponseConsumer<T> consumer)
            throws InterruptedException {
        for (int attempt = 0; ; attempt++) {
            String reason;
            long hint;
            try {
                return exchange(request, deadline, consumer);
            } catch (Retry ex) {
                if (!request.mode.replay && ex.status != 429) {
                    throw failure(request, "HTTP " + ex.status);
                }
                reason = "HTTP " + ex.status;
                hint = ex.delay;
            } catch (SSLException ex) {
                remaining(deadline);
                throw new MicrosoftAdvertisingFailure(
                        "HTTPS certificate verification or negotiation failed");
            } catch (ZipException ex) {
                throw new MicrosoftAdvertisingFailure("invalid compressed response or archive");
            } catch (JsonProcessingException ex) {
                throw failure(request, "invalid JSON response");
            } catch (HttpException ex) {
                throw failure(request, "malformed HTTP response");
            } catch (IOException ex) {
                if (!request.mode.replay) {
                    throw failure(request, "HTTPS I/O outcome was unknown");
                }
                remaining(deadline);
                reason = "HTTPS I/O failure or timeout";
                hint = 0;
            }
            if (attempt == config.retries) {
                throw failure(request, reason + " after " + (attempt + 1) + " attempts");
            }
            backoff(hint, attempt, deadline);
        }
    }

    private <T> T exchange(Request request, long deadline, ResponseConsumer<T> consumer)
            throws IOException, HttpException, InterruptedException, Retry {
        long duration = remaining(deadline);
        if (request.mode != Mode.DOWNLOAD) {
            duration = Math.min(duration, TimeUnit.MILLISECONDS.toNanos(config.requestTimeout));
        }
        long expires = System.nanoTime() + duration;
        Socket raw = new Socket(Proxy.NO_PROXY);
        ScheduledFuture<?> abort = null;
        try {
            synchronized (this) {
                remaining(expires);
                active = raw;
                abort = watchdog.schedule(() -> closeSocket(raw), duration, TimeUnit.NANOSECONDS);
            }
            int timeout = (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(duration));
            raw.connect(
                    new InetSocketAddress(request.host, 443),
                    Math.min(timeout, config.connectTimeout));
            raw.setSoTimeout(timeout);
            try (SSLSocket socket = (SSLSocket) tls.createSocket(raw, request.host, 443, true);
                    DefaultBHttpClientConnection wire =
                            new DefaultBHttpClientConnection(8192, null, null, HEADERS)) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                wire.bind(socket);
                HttpResponse response =
                        new HttpRequestExecutor()
                                .execute(headers(request), wire, HttpCoreContext.create());
                int status = response.getStatusLine().getStatusCode();
                if (TRANSIENT.contains(status)) {
                    throw new Retry(status, retryAfter(response));
                }
                return consumer.read(response, expires);
            }
        } finally {
            closeSocket(raw);
            if (abort != null) {
                abort.cancel(false);
            }
            synchronized (this) {
                active = null;
            }
        }
    }

    private HttpRequest headers(Request request) {
        HttpRequest message;
        if (request.body == null) {
            message = new BasicHttpRequest("GET", request.target);
        } else {
            BasicHttpEntityEnclosingRequest post =
                    new BasicHttpEntityEnclosingRequest("POST", request.target);
            post.setEntity(new ByteArrayEntity(request.body));
            post.addHeader("Content-Length", Integer.toString(request.body.length));
            post.addHeader(
                    "Content-Type",
                    request.mode == Mode.GRANT
                            ? "application/x-www-form-urlencoded"
                            : "application/json");
            message = post;
        }
        if (request.mode == Mode.READ || request.mode == Mode.SUBMIT) {
            message.addHeader("Authorization", "Bearer " + request.bearer);
            message.addHeader("DeveloperToken", config.developerToken);
            if (request.account != null) {
                message.addHeader("CustomerAccountId", request.account.id);
                message.addHeader("CustomerId", request.account.customer);
            }
        }
        message.addHeader("Host", request.host);
        message.addHeader("User-Agent", "Apache SeaTunnel MicrosoftAdvertising");
        message.addHeader(
                "Accept",
                request.mode == Mode.DOWNLOAD ? "application/octet-stream" : "application/json");
        message.addHeader("Accept-Encoding", request.mode == Mode.DOWNLOAD ? "identity" : "gzip");
        message.addHeader("Connection", "close");
        return message;
    }

    private JsonNode jsonBody(HttpResponse response, long expires, int limit)
            throws IOException, InterruptedException {
        HttpEntity entity = response.getEntity();
        if (entity == null || entity.getContentLength() > limit) {
            throw new MicrosoftAdvertisingFailure(
                    "missing JSON body or max_response_bytes exceeded");
        }
        boolean gzip = gzip(response.getHeaders("Content-Encoding"));
        InputStream raw = new WireLimit(entity.getContent(), limit, "max_response_bytes");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] block = new byte[8192];
        try (InputStream input = gzip ? new GZIPInputStream(raw) : raw) {
            for (int size; (size = input.read(block)) >= 0; ) {
                remaining(expires);
                if (size > limit - bytes.size()) {
                    throw new MicrosoftAdvertisingFailure(
                            "decoded response exceeds max_response_bytes");
                }
                bytes.write(block, 0, size);
            }
        }
        byte[] body = bytes.toByteArray();
        try (JsonParser parser = json.getFactory().createParser(body)) {
            int tokens = 0;
            while (parser.nextToken() != null) {
                if (++tokens > 500000) {
                    throw new MicrosoftAdvertisingFailure("JSON token limit exceeded");
                }
                if ((tokens & 1023) == 0) {
                    remaining(expires);
                }
            }
        }
        JsonNode root = json.readTree(body);
        remaining(expires);
        if (root == null || !root.isObject()) {
            throw new MicrosoftAdvertisingFailure("expected a JSON response object");
        }
        return root;
    }

    private static boolean gzip(Header[] encodings) {
        if (encodings.length == 0
                || encodings.length == 1
                        && "identity".equalsIgnoreCase(encodings[0].getValue().trim())) {
            return false;
        }
        if (encodings.length == 1 && "gzip".equalsIgnoreCase(encodings[0].getValue().trim())) {
            return true;
        }
        throw new MicrosoftAdvertisingFailure("unsupported HTTPS content encoding");
    }

    private static long retryAfter(HttpResponse response) {
        long wait = 0;
        for (Header header : response.getHeaders("Retry-After")) {
            String value = header.getValue().trim();
            try {
                long parsed =
                        value.matches("[0-9]+")
                                ? Math.multiplyExact(Long.parseLong(value), 1000L)
                                : ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                                .toInstant()
                                                .toEpochMilli()
                                        - System.currentTimeMillis();
                wait = Math.max(wait, parsed);
            } catch (ArithmeticException | NumberFormatException ex) {
                return Long.MAX_VALUE;
            } catch (DateTimeException ex) {
                // A malformed Retry-After cannot extend the local backoff.
            }
        }
        return wait;
    }

    void backoff(long hint, int attempt, long deadline) throws InterruptedException {
        if (hint > config.retryDelay) {
            throw new MicrosoftAdvertisingFailure("Retry-After exceeds max_retry_delay_ms");
        }
        long milliseconds = Math.max(hint, Math.min(config.retryDelay, 250L << attempt));
        long remaining = TimeUnit.MILLISECONDS.toNanos(milliseconds);
        if (remaining >= remaining(deadline)) {
            throw new MicrosoftAdvertisingFailure("retry delay exceeds HTTPS operation deadline");
        }
        if (remaining > 0) {
            TimeUnit.NANOSECONDS.sleep(remaining);
        }
        remaining(deadline);
    }

    long remaining(long deadline) throws InterruptedException {
        if (closed || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("MicrosoftAdvertising source closed");
        }
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new MicrosoftAdvertisingFailure("HTTPS operation deadline exceeded");
        }
        return nanos;
    }

    synchronized void release(MicrosoftAdvertisingFile file) {
        if (heldFile == file) {
            heldFile.close();
            heldFile = null;
        }
    }

    /**
     * Cancels the socket and deadline scheduler without retaining connection state for retry.
     *
     * <p>The reader separately interrupts its single provider worker.
     */
    @Override
    public synchronized void close() {
        closed = true;
        closeSocket(active);
        if (heldFile != null) {
            heldFile.close();
            heldFile = null;
        }
        watchdog.shutdownNow();
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // A closed socket is never reused.
            }
        }
    }

    private static MicrosoftAdvertisingFailure failure(Request request, String reason) {
        if (request.mode == Mode.SUBMIT) {
            return new MicrosoftAdvertisingFailure(
                    "async submit " + reason + "; outcome may be unknown");
        }
        return new MicrosoftAdvertisingFailure(reason);
    }

    /**
     * Classifies a request by its credential authority and replay safety.
     *
     * <p>Grant rotation and job submissions are replayed only after definite throttling.
     */
    private enum Mode {
        GRANT(false),
        SUBMIT(false),
        READ(true),
        DOWNLOAD(true);

        final boolean replay;

        Mode(boolean replay) {
            this.replay = replay;
        }
    }

    /**
     * One isolated request, including only the body and credentials needed for its phase.
     *
     * <p>Request values are never rendered as a diagnostic or serialized to checkpoint state.
     */
    private static final class Request {
        final String host;
        final String target;
        final String bearer;
        final MicrosoftAdvertisingSplit.Account account;
        final byte[] body;
        final Mode mode;

        Request(
                String host,
                String target,
                String bearer,
                MicrosoftAdvertisingSplit.Account account,
                byte[] body,
                Mode mode) {
            this.host = host;
            this.target = target;
            this.bearer = bearer;
            this.account = account;
            this.body = body;
            this.mode = mode;
        }
    }

    /**
     * Bounded data returned to the provider protocol layer.
     *
     * <p>Only status, parsed JSON and a numeric retry hint are retained.
     */
    static final class Reply {
        final int status;
        final JsonNode body;
        final long retryAfter;

        Reply(int status, JsonNode body, long retryAfter) {
            this.status = status;
            this.body = body;
            this.retryAfter = retryAfter;
        }
    }

    /**
     * Reads the response while its verified socket and deadline are still owned by the exchange.
     *
     * <p>A downloader consumes its complete file before returning a local handle.
     */
    @FunctionalInterface
    private interface ResponseConsumer<T> {
        T read(HttpResponse response, long expires) throws IOException, InterruptedException;
    }

    /**
     * Internal transient status with a bounded retry hint.
     *
     * <p>Raw headers and exception causes stay off the engine's diagnostic path.
     */
    private static final class Retry extends Exception {
        private static final long serialVersionUID = 1L;

        final int status;
        final long delay;

        Retry(int status, long delay) {
            this.status = status;
            this.delay = delay;
        }
    }

    /**
     * Caps an HTTP entity before any JSON parsing or archive expansion.
     *
     * <p>Closing this wrapper never drains the underlying network body.
     */
    private static final class WireLimit extends FilterInputStream {
        private final int limit;
        private final String option;
        private long count;

        WireLimit(InputStream input, int limit, String option) {
            super(input);
            this.limit = limit;
            this.option = option;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value >= 0) {
                check(1);
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int countRead = in.read(bytes, offset, length);
            if (countRead > 0) {
                check(countRead);
            }
            return countRead;
        }

        private void check(int size) {
            count += size;
            if (count > limit) {
                throw new MicrosoftAdvertisingFailure("wire response exceeds " + option);
            }
        }

        @Override
        public void close() {
            // The enclosing exchange owns socket shutdown.
        }
    }
}
