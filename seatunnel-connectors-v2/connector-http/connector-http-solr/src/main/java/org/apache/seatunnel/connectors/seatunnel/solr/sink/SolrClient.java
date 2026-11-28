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

package org.apache.seatunnel.connectors.seatunnel.solr.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.entity.DeflateInputStream;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.URI;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

/**
 * Owns one direct, non-logging HTTP exchange at a time under an absolute operation deadline. Close
 * independently aborts the published Future/socket; uncertain mutations fail without replay.
 */
final class SolrClient implements AutoCloseable {
    private static final int MAX_RESPONSE_BYTES = 256 * 1024;
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(32)
                                                    .build())
                                    .build())
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final DefaultBHttpClientConnectionFactory CONNECTIONS =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(16384)
                                            .build())
                            .build());

    private final URI origin;
    private final String corePath;
    private final String authorization;
    private final int timeoutMillis;
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    action -> {
                        Thread thread = new Thread(action, "seatunnel-solr-sink");
                        thread.setDaemon(true);
                        return thread;
                    });

    // Guarded by this monitor; publication/close never holds it across connect, TLS or exchange
    // I/O.
    private boolean closed;
    // Guarded by this monitor; only one submitted operation may own the worker at a time.
    private Future<JsonNode> activeRequest;
    // Guarded by this monitor and published before connect so close can abort active I/O.
    private Socket activeSocket;

    SolrClient(URI origin, String corePath, String authorization, int timeoutMillis) {
        this.origin = origin;
        this.corePath = corePath;
        this.authorization = authorization;
        this.timeoutMillis = timeoutMillis;
    }

    /* Verify the schema contract before the writer sends its first update, without changing it. */
    void verifyUniqueKey(String expected) throws IOException {
        JsonNode response = request("schema", "/schema/uniquekey?wt=json&omitHeader=false", null);
        JsonNode uniqueKey = response.path("uniqueKey");
        if (!uniqueKey.isTextual() || !expected.equals(uniqueKey.asText())) {
            throw failure("schema", "configured unique_key does not match the target core", false);
        }
    }

    /* Mutation results must be acknowledged completely; the caller owns pending data on failure. */
    void update(String operation, byte[] body) throws IOException {
        request(operation, "/update?wt=json&omitHeader=false&overwrite=true", body);
    }

    /* Abort abandoned work without flushing, retrying or rolling back any core updates. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (activeRequest != null) {
            activeRequest.cancel(true);
        }
        worker.shutdownNow();
        if (activeSocket != null) {
            try {
                activeSocket.close();
            } catch (IOException ignored) {
                // The exchange also owns the socket.
            }
        }
    }

    /*
     * One monotonic budget covers scheduling, transport, decoding and validation. The waiting
     * caller can close the published socket independently of a blocked worker or slow response.
     */
    private JsonNode request(String operation, String route, byte[] body) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        boolean mutating = body != null;
        Future<JsonNode> result;
        synchronized (this) {
            if (closed || activeRequest != null) {
                throw failure(operation, "writer is closed or already sending a request", false);
            }
            result = worker.submit(() -> connect(operation, corePath + route, body));
            activeRequest = result;
        }
        try {
            JsonNode response = result.get(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
            if (deadline - System.nanoTime() <= 0) {
                throw new TimeoutException();
            }
            return response;
        } catch (TimeoutException e) {
            close();
            throw failure(operation, "request deadline exceeded", mutating);
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            throw failure(operation, "request interrupted", mutating);
        } catch (CancellationException e) {
            throw failure(operation, "request cancelled", mutating);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SolrFailure) {
                throw (SolrFailure) e.getCause();
            }
            throw failure(operation, "request failed", mutating);
        } finally {
            synchronized (this) {
                activeRequest = null;
            }
        }
    }

    /* Publish the raw socket before connect/TLS; reject late registration after close. */
    private JsonNode connect(String operation, String path, byte[] body) throws IOException {
        boolean mutating = body != null;
        try (Socket socket = new Socket(Proxy.NO_PROXY)) {
            synchronized (this) {
                if (closed || Thread.currentThread().isInterrupted()) {
                    throw failure(operation, "request cancelled", false);
                }
                activeSocket = socket;
            }
            boolean tls = "https".equalsIgnoreCase(origin.getScheme());
            int port = origin.getPort() < 0 ? (tls ? 443 : 80) : origin.getPort();
            String host = origin.getHost();
            if (host.startsWith("[")) {
                host = host.substring(1, host.length() - 1);
            }
            socket.connect(new InetSocketAddress(host, port), Math.min(timeoutMillis, 10000));
            socket.setSoTimeout(timeoutMillis);
            if (!tls) {
                return exchange(socket, operation, path, body);
            }
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket secure = (SSLSocket) factory.createSocket(socket, host, port, true)) {
                SSLParameters parameters = secure.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                secure.setSSLParameters(parameters);
                secure.startHandshake();
                return exchange(secure, operation, path, body);
            }
        } catch (SolrFailure e) {
            throw e;
        } catch (SSLException e) {
            throw failure(operation, "TLS transport failed", mutating);
        } catch (IOException | HttpException e) {
            throw failure(operation, "transport failed or returned invalid HTTP framing", mutating);
        } finally {
            synchronized (this) {
                activeSocket = null;
            }
        }
    }

    /*
     * The core connection does not log request headers or body. HTTP success is insufficient:
     * bounded JSON must also contain a complete successful Solr acknowledgement.
     */
    private JsonNode exchange(Socket socket, String operation, String path, byte[] body)
            throws IOException, HttpException {
        BasicHttpRequest request;
        if (body == null) {
            request = new BasicHttpRequest("GET", path, HttpVersion.HTTP_1_1);
        } else {
            BasicHttpEntityEnclosingRequest post =
                    new BasicHttpEntityEnclosingRequest("POST", path, HttpVersion.HTTP_1_1);
            post.setEntity(new ByteArrayEntity(body));
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Content-Length", Integer.toString(body.length));
            request = post;
        }
        request.setHeader("Host", origin.getRawAuthority());
        request.setHeader("Connection", "close");
        request.setHeader("Accept", "application/json");
        request.setHeader("Accept-Encoding", "gzip, deflate");
        if (authorization != null) {
            request.setHeader("Authorization", authorization);
        }
        HttpResponse response =
                new HttpRequestExecutor()
                        .execute(
                                request,
                                CONNECTIONS.createConnection(socket),
                                HttpCoreContext.create());
        boolean mutating = body != null;
        int status = response.getStatusLine().getStatusCode();
        if (status != 200) {
            throw failure(operation, "unexpected HTTP " + status, mutating);
        }
        JsonNode result;
        byte[] decoded = decode(response, operation, mutating);
        try {
            result = JSON.readTree(decoded);
        } catch (IOException e) {
            throw failure(operation, "invalid JSON acknowledgement", mutating);
        }
        if (result == null || !result.isObject()) {
            throw failure(operation, "invalid JSON acknowledgement", mutating);
        }
        JsonNode header = result.path("responseHeader");
        JsonNode code = header.path("status");
        if (!code.isIntegralNumber()
                || !code.canConvertToInt()
                || code.intValue() != 0
                || containsError(result)
                || containsError(header)
                || (header.has("partialResults") && !header.path("partialResults").isBoolean())
                || header.path("partialResults").asBoolean()) {
            throw failure(operation, "Solr returned a failed or partial acknowledgement", mutating);
        }
        return result;
    }

    private static boolean containsError(JsonNode node) {
        return nonempty(node.path("error")) || nonempty(node.path("errors"));
    }

    private static boolean nonempty(JsonNode node) {
        return !node.isMissingNode()
                && !node.isNull()
                && (!node.isContainerNode() || !node.isEmpty());
    }

    /* Bound both received and decompressed bytes before JSON tree construction. */
    private static byte[] decode(HttpResponse response, String operation, boolean mutating)
            throws IOException {
        String length = header(response, "Content-Length", operation, mutating);
        String transfer = header(response, "Transfer-Encoding", operation, mutating);
        if (length != null
                && (!length.matches("[0-9]{1,18}")
                        || Long.parseLong(length) > MAX_RESPONSE_BYTES)) {
            throw failure(operation, "invalid or oversized acknowledgement length", mutating);
        }
        if (transfer != null && (length != null || !"chunked".equalsIgnoreCase(transfer))) {
            throw failure(operation, "invalid acknowledgement framing", mutating);
        }
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw failure(operation, "missing acknowledgement body", mutating);
        }
        String encoding = header(response, "Content-Encoding", operation, mutating);
        byte[] wire = readBounded(entity.getContent(), operation, mutating);
        if (encoding == null || "identity".equalsIgnoreCase(encoding)) {
            return wire;
        }
        InputStream in = new ByteArrayInputStream(wire);
        if ("gzip".equalsIgnoreCase(encoding)) {
            try (InputStream uncompressed = new GZIPInputStream(in)) {
                return readBounded(uncompressed, operation, mutating);
            }
        }
        if ("deflate".equalsIgnoreCase(encoding)) {
            try (InputStream uncompressed = new DeflateInputStream(in)) {
                return readBounded(uncompressed, operation, mutating);
            }
        }
        throw failure(operation, "unsupported acknowledgement encoding", mutating);
    }

    private static String header(
            HttpResponse response, String name, String operation, boolean mutating)
            throws IOException {
        Header[] values = response.getHeaders(name);
        if (values.length > 1) {
            throw failure(operation, "duplicate acknowledgement header", mutating);
        }
        return values.length == 0 || values[0].getValue() == null
                ? null
                : values[0].getValue().trim();
    }

    private static byte[] readBounded(InputStream input, String operation, boolean mutating)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int size;
        while ((size = input.read(buffer)) != -1) {
            if (size > MAX_RESPONSE_BYTES - out.size()) {
                throw failure(operation, "acknowledgement exceeds 256 KiB", mutating);
            }
            out.write(buffer, 0, size);
        }
        return out.toByteArray();
    }

    private static SolrFailure failure(String operation, String reason, boolean mutating) {
        return new SolrFailure(
                "Solr Sink "
                        + operation
                        + " "
                        + reason
                        + (mutating ? "; Solr may have applied some changes" : ""));
    }

    /**
     * Carries only connector-owned operation diagnostics, without upstream response/parser causes
     * or private payloads; mutating failures warn that Solr may already have applied changes.
     */
    private static final class SolrFailure extends IOException {
        private static final long serialVersionUID = 1L;

        private SolrFailure(String message) {
            super(message);
        }
    }
}
