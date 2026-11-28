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

package org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink;

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
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

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
import java.util.zip.GZIPInputStream;

/**
 * Executes one directly connected Firehose exchange with bounded framing and decoded response size.
 * It does not redirect, retry or log payloads; the owning batch deadline closes any active socket.
 */
final class FirehoseTransport implements SdkHttpClient {
    private static final int RESPONSE_MAX_BYTES = 1024 * 1024;
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

    private final URI endpoint;
    private final int socketTimeoutMillis;
    // Guarded by this transport monitor; prevents registration after client shutdown.
    private boolean closed;
    // Guarded by this transport monitor; only one exchange may publish a closeable raw socket.
    private Call active;

    FirehoseTransport(URI endpoint, int socketTimeoutMillis) {
        this.endpoint = endpoint;
        this.socketTimeoutMillis = socketTimeoutMillis;
    }

    /*
     * Allocate a cancellable call without starting I/O; abort may arrive before call registration.
     */
    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
        return new Call(request);
    }

    /**
     * Owns one request/socket registration; abort and publication share the outer transport
     * monitor. Connect, TLS and exchange I/O run outside that monitor so cancellation can close the
     * socket.
     */
    private final class Call implements ExecutableHttpRequest {
        private final HttpExecuteRequest request;
        // Guarded by the outer transport monitor, including abort-before-call races.
        private boolean aborted;
        // Published before connect so close/timeout can interrupt later socket I/O.
        private Socket socket;

        private Call(HttpExecuteRequest request) {
            this.request = request;
        }

        /*
         * Publish the raw socket before DNS/connect and restrict the signed origin before sending.
         * Failures discard raw parser/TLS causes so private response data cannot enter diagnostics.
         */
        @Override
        public HttpExecuteResponse call() throws IOException {
            try (Socket raw = new Socket(Proxy.NO_PROXY)) {
                synchronized (FirehoseTransport.this) {
                    if (closed
                            || aborted
                            || active != null
                            || Thread.currentThread().isInterrupted()) {
                        throw failure("request is closed, cancelled or already in flight");
                    }
                    socket = raw;
                    active = this;
                }
                SdkHttpRequest signed = request.httpRequest();
                URI target = signed.getUri();
                boolean tls = "https".equalsIgnoreCase(target.getScheme());
                if (signed.method() != SdkHttpMethod.POST
                        || !"/".equals(target.getRawPath())
                        || target.getRawQuery() != null
                        || target.getRawUserInfo() != null
                        || !signed.firstMatchingHeader("X-Amz-Target")
                                .filter("Firehose_20150804.PutRecordBatch"::equals)
                                .isPresent()
                        || (endpoint != null
                                && (!endpoint.getHost().equalsIgnoreCase(target.getHost())
                                        || !endpoint.getScheme()
                                                .equalsIgnoreCase(target.getScheme())
                                        || port(endpoint) != port(target)))
                        || (!tls
                                && (endpoint == null
                                        || !"http".equalsIgnoreCase(target.getScheme())))) {
                    throw failure("unexpected Firehose request target");
                }
                String host = target.getHost();
                if (host == null) {
                    throw failure("invalid Firehose request host");
                }
                if (host.startsWith("[")) {
                    host = host.substring(1, host.length() - 1);
                }
                raw.connect(
                        new InetSocketAddress(host, port(target)),
                        Math.min(socketTimeoutMillis, 10000));
                raw.setSoTimeout(socketTimeoutMillis);
                if (!tls) {
                    return exchange(raw, request);
                }
                SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                try (SSLSocket secure =
                        (SSLSocket) factory.createSocket(raw, host, port(target), true)) {
                    SSLParameters parameters = secure.getSSLParameters();
                    parameters.setEndpointIdentificationAlgorithm("HTTPS");
                    secure.setSSLParameters(parameters);
                    secure.startHandshake();
                    return exchange(secure, request);
                }
            } catch (TransportFailure e) {
                throw e;
            } catch (IOException | HttpException e) {
                throw failure("transport failed or returned invalid framing");
            } finally {
                synchronized (FirehoseTransport.this) {
                    if (active == this) {
                        active = null;
                    }
                }
            }
        }

        /*
         * Close independently of the exchange thread; repeated abort and owner close are safe.
         */
        @Override
        public void abort() {
            synchronized (FirehoseTransport.this) {
                aborted = true;
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException ignored) {
                        // The same socket is also owned by call().
                    }
                }
            }
        }
    }

    private static int port(URI uri) {
        if (uri.getPort() >= 0) {
            return uri.getPort();
        }
        return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
    }

    /*
     * Send only the already signed root POST with a checked content length and no redirect path.
     * Validate/decode the complete bounded response before returning a detached body to SDK unmarshalling.
     */
    private static HttpExecuteResponse exchange(Socket socket, HttpExecuteRequest request)
            throws IOException, HttpException {
        SdkHttpRequest signed = request.httpRequest();
        String lengthHeader = signed.firstMatchingHeader("Content-Length").orElse("");
        if (!lengthHeader.matches("[0-9]{1,7}")
                || Integer.parseInt(lengthHeader) == 0
                || Integer.parseInt(lengthHeader)
                        > AmazonDataFirehoseSinkWriter.REQUEST_MAX_BYTES) {
            throw failure("request has an invalid or oversized content length");
        }
        BasicHttpEntityEnclosingRequest outbound =
                new BasicHttpEntityEnclosingRequest("POST", "/", HttpVersion.HTTP_1_1);
        signed.forEachHeader(
                (key, values) -> values.forEach(value -> outbound.addHeader(key, value)));
        outbound.setHeader("Connection", "close");
        outbound.setHeader("Accept-Encoding", "gzip, deflate");
        try (InputStream payload =
                request.contentStreamProvider()
                        .orElseThrow(() -> failure("request payload is missing"))
                        .newStream()) {
            outbound.setEntity(new InputStreamEntity(payload, Integer.parseInt(lengthHeader)));
            HttpResponse response =
                    new HttpRequestExecutor()
                            .execute(
                                    outbound,
                                    CONNECTIONS.createConnection(socket),
                                    HttpCoreContext.create());
            int status = response.getStatusLine().getStatusCode();
            if (status != 200) {
                throw failure("unexpected HTTP " + status);
            }
            byte[] body = decode(response);
            validateJsonShape(body);
            return HttpExecuteResponse.builder()
                    .response(
                            SdkHttpResponse.builder()
                                    .statusCode(status)
                                    .putHeader("Content-Type", "application/x-amz-json-1.1")
                                    .build())
                    .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
                    .build();
        }
    }

    /*
     * Reject conflicting framing and unsupported encodings, with independent wire/decoded bounds.
     * Compression cannot turn a small acknowledgement into unbounded decoder output.
     */
    private static byte[] decode(HttpResponse response) throws IOException {
        String length = header(response, "Content-Length");
        String transfer = header(response, "Transfer-Encoding");
        if (length != null
                && (!length.matches("[0-9]{1,18}")
                        || Long.parseLong(length) > RESPONSE_MAX_BYTES)) {
            throw failure("invalid or oversized acknowledgement length");
        }
        if (transfer != null && (length != null || !"chunked".equalsIgnoreCase(transfer))) {
            throw failure("invalid acknowledgement framing");
        }
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw failure("missing acknowledgement body");
        }
        String encoding = header(response, "Content-Encoding");
        byte[] wireBody = readBounded(entity.getContent());
        if (encoding == null || "identity".equalsIgnoreCase(encoding)) {
            return wireBody;
        }
        InputStream in = new ByteArrayInputStream(wireBody);
        if ("gzip".equalsIgnoreCase(encoding)) {
            try (InputStream decoder = new GZIPInputStream(in)) {
                return readBounded(decoder);
            }
        }
        if ("deflate".equalsIgnoreCase(encoding)) {
            try (InputStream decoder = new DeflateInputStream(in)) {
                return readBounded(decoder);
            }
        }
        throw failure("unsupported acknowledgement encoding");
    }

    /*
     * Headers governing framing or decoding must be unambiguous even if the HTTP parser accepted them.
     */
    private static String header(HttpResponse response, String name) throws IOException {
        Header[] values = response.getHeaders(name);
        if (values.length > 1) {
            throw failure("duplicate acknowledgement header");
        }
        return values.length == 0 || values[0].getValue() == null
                ? null
                : values[0].getValue().trim();
    }

    /*
     * Apply the same finite limit to wire and decompressed streams before allocating the next output chunk.
     */
    private static byte[] readBounded(InputStream input) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] bytes = new byte[8192];
        int n;
        while ((n = input.read(bytes)) != -1) {
            if (n > RESPONSE_MAX_BYTES - out.size()) {
                throw failure("acknowledgement exceeds 1 MiB");
            }
            out.write(bytes, 0, n);
        }
        return out.toByteArray();
    }

    /*
     * Reject duplicate/ill-typed response fields before SDK coercion can hide an invalid acknowledgement.
     * Only record result text is permitted; no upstream parser diagnostic is retained.
     */
    private static void validateJsonShape(byte[] body) throws IOException {
        JsonNode result;
        try {
            result = JSON.readTree(body);
        } catch (IOException e) {
            throw failure("invalid JSON acknowledgement");
        }
        if (result == null
                || !result.isObject()
                || !result.path("FailedPutCount").isIntegralNumber()
                || !result.path("FailedPutCount").canConvertToInt()
                || !result.path("RequestResponses").isArray()) {
            throw failure("invalid acknowledgement result structure");
        }
        for (JsonNode entry : result.path("RequestResponses")) {
            if (!entry.isObject()
                    || !optionalText(entry, "RecordId")
                    || !optionalText(entry, "ErrorCode")
                    || !optionalText(entry, "ErrorMessage")) {
                throw failure("invalid acknowledgement entry structure");
            }
        }
    }

    private static boolean optionalText(JsonNode object, String member) {
        return !object.has(member) || object.path(member).isTextual();
    }

    /*
     * Prevent further call registration and independently abort the socket owned by the current call.
     */
    @Override
    public synchronized void close() {
        closed = true;
        if (active != null) {
            active.abort();
        }
    }

    private static TransportFailure failure(String reason) {
        return new TransportFailure(reason);
    }

    /**
     * Contains only local transport reasons safe for the batch client to expose without a cause
     * chain. Raw framing, TLS or response-parser exceptions are discarded before crossing this
     * boundary.
     */
    static final class TransportFailure extends IOException {
        private static final long serialVersionUID = 1L;

        private TransportFailure(String reason) {
            super(reason);
        }
    }
}
