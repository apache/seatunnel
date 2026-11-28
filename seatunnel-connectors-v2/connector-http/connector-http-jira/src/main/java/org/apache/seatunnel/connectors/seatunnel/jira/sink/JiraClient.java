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

package org.apache.seatunnel.connectors.seatunnel.jira.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.connectors.seatunnel.jira.sink.config.JiraSinkOptions.Operation;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.entity.DeflateInputStream;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultBHttpClientConnection;
import org.apache.http.impl.DefaultBHttpClientConnectionFactory;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpRequestExecutor;

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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

/** Owns the request worker and active socket so close can interrupt I/O or throttling waits. */
final class JiraClient implements AutoCloseable {
    private static final int MAX_RESPONSE_BYTES = 64 * 1024;
    private static final DefaultBHttpClientConnectionFactory CONNECTION_FACTORY =
            new DefaultBHttpClientConnectionFactory(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(64)
                                            .setMaxLineLength(8192)
                                            .build())
                            .build());

    private final URI origin;
    private final String authorization;
    private final Operation operation;
    private final int timeoutMillis;
    private final int maxRetries;
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "seatunnel-jira-sink");
                        thread.setDaemon(true);
                        return thread;
                    });

    private boolean closed;
    private Future<Void> activeRequest;
    private Socket activeSocket;

    JiraClient(
            URI origin,
            String authorization,
            Operation operation,
            int timeoutMillis,
            int maxRetries) {
        this.origin = origin;
        this.authorization = authorization;
        this.operation = operation;
        this.timeoutMillis = timeoutMillis;
        this.maxRetries = maxRetries;
    }

    void mutate(String path, byte[] body) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        Future<Void> request;
        synchronized (this) {
            if (closed || activeRequest != null) {
                throw failure("writer is closed or already has a request in flight");
            }
            request =
                    worker.submit(
                            () -> {
                                send(path, body, deadline);
                                return null;
                            });
            activeRequest = request;
        }
        try {
            request.get(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
            if (deadline - System.nanoTime() <= 0) {
                throw new TimeoutException();
            }
        } catch (TimeoutException e) {
            close();
            throw uncertain("request deadline exceeded");
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            throw uncertain("request interrupted");
        } catch (CancellationException e) {
            throw uncertain("request cancelled");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof JiraRequestException) {
                throw (JiraRequestException) e.getCause();
            }
            throw uncertain("request failed");
        } finally {
            synchronized (this) {
                activeRequest = null;
            }
        }
    }

    private void send(String path, byte[] body, long deadline)
            throws IOException, InterruptedException {
        for (int attempt = 0; ; attempt++) {
            if (Thread.currentThread().isInterrupted()) {
                throw uncertain("request interrupted");
            }
            if (deadline - System.nanoTime() <= 0) {
                throw uncertain("request deadline exceeded");
            }
            long retryAfter = exchange(path, body);
            if (retryAfter < 0) {
                return;
            }
            if (attempt >= maxRetries) {
                throw failure("rejected with HTTP 429; maximum retries exhausted");
            }
            long backoff =
                    Math.min(250L << attempt, 5000) + ThreadLocalRandom.current().nextInt(100);
            long delay = Math.max(retryAfter, backoff);
            long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (delay >= remaining) {
                throw failure(
                        "rejected with HTTP 429; Retry-After exceeds the remaining request deadline");
            }
            Thread.sleep(delay);
        }
    }

    private long exchange(String path, byte[] body) throws IOException {
        try (Socket socket = new Socket(Proxy.NO_PROXY)) {
            synchronized (this) {
                if (closed || Thread.currentThread().isInterrupted()) {
                    throw uncertain("request cancelled");
                }
                activeSocket = socket;
            }
            boolean tls = "https".equalsIgnoreCase(origin.getScheme());
            int port = origin.getPort() < 0 ? (tls ? 443 : 80) : origin.getPort();
            String host = origin.getHost();
            if (host.startsWith("[")) {
                host = host.substring(1, host.length() - 1);
            }
            socket.connect(new InetSocketAddress(host, port), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);
            if (!tls) {
                return execute(socket, path, body);
            }
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket secure = (SSLSocket) factory.createSocket(socket, host, port, true)) {
                SSLParameters parameters = secure.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                secure.setSSLParameters(parameters);
                secure.startHandshake();
                return execute(secure, path, body);
            }
        } catch (JiraRequestException e) {
            throw e;
        } catch (HttpException | IOException e) {
            throw uncertain("invalid HTTP response or transport failure");
        } finally {
            synchronized (this) {
                activeSocket = null;
            }
        }
    }

    private long execute(Socket socket, String path, byte[] body)
            throws IOException, HttpException {
        BasicHttpRequest request;
        if (body == null) {
            request = new BasicHttpRequest("DELETE", path, HttpVersion.HTTP_1_1);
        } else {
            BasicHttpEntityEnclosingRequest withBody =
                    new BasicHttpEntityEnclosingRequest(
                            operation == Operation.CREATE ? "POST" : "PUT",
                            path,
                            HttpVersion.HTTP_1_1);
            withBody.setEntity(new ByteArrayEntity(body));
            withBody.setHeader("Content-Type", "application/json");
            withBody.setHeader("Content-Length", Integer.toString(body.length));
            request = withBody;
        }
        request.setHeader("Host", origin.getRawAuthority());
        request.setHeader("Authorization", authorization);
        request.setHeader("Accept", "application/json");
        request.setHeader("Accept-Encoding", "gzip, deflate");
        request.setHeader("Connection", "close");

        // HttpCore performs one exchange without redirects, retries, or client header/wire logging.
        DefaultBHttpClientConnection connection = CONNECTION_FACTORY.createConnection(socket);
        HttpResponse response =
                new HttpRequestExecutor().execute(request, connection, HttpCoreContext.create());
        int status = response.getStatusLine().getStatusCode();
        int success = operation == Operation.CREATE ? 201 : 204;
        if (status == success) {
            String length = header(response, "Content-Length");
            String transfer = header(response, "Transfer-Encoding");
            if ((length != null && !length.matches("[0-9]{1,18}"))
                    || (transfer != null
                            && (length != null || !"chunked".equalsIgnoreCase(transfer)))) {
                throw uncertain("invalid HTTP acknowledgement framing");
            }
            if (operation == Operation.CREATE) {
                if (length != null && Long.parseLong(length) > MAX_RESPONSE_BYTES) {
                    throw uncertain("acknowledgement exceeds 64 KiB");
                }
                validateCreatedIssue(readBody(response));
            } else if (transfer != null || (length != null && Long.parseLong(length) != 0)) {
                throw uncertain("expected a bodyless HTTP 204 acknowledgement");
            }
            return -1;
        }
        if (operation == Operation.UPDATE && status == 429 && maxRetries > 0) {
            long wait = retryAfter(header(response, "Retry-After"));
            if (wait >= 0) {
                return wait;
            }
            throw failure(
                    "rejected with HTTP 429; a valid Retry-After header is required to retry");
        }
        if (status >= 500 || status == 408 || status < 400) {
            throw uncertain("unexpected HTTP status " + status);
        }
        throw failure(
                "rejected with HTTP " + status + "; check the Jira permissions, issue and fields");
    }

    private byte[] readBody(HttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw uncertain("missing CREATE acknowledgement");
        }
        byte[] wireBody = readBytes(entity.getContent());
        String encoding = header(response, "Content-Encoding");
        if (encoding == null || "identity".equalsIgnoreCase(encoding)) {
            return wireBody;
        }
        InputStream encoded = new ByteArrayInputStream(wireBody);
        if ("gzip".equalsIgnoreCase(encoding)) {
            try (InputStream decoder = new GZIPInputStream(encoded)) {
                return readBytes(decoder);
            }
        }
        if ("deflate".equalsIgnoreCase(encoding)) {
            try (InputStream decoder = new DeflateInputStream(encoded)) {
                return readBytes(decoder);
            }
        }
        throw uncertain("unsupported acknowledgement content encoding");
    }

    private byte[] readBytes(InputStream input) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int count;
        while ((count = input.read(buffer)) != -1) {
            if (count > MAX_RESPONSE_BYTES - body.size()) {
                throw uncertain("acknowledgement exceeds 64 KiB");
            }
            body.write(buffer, 0, count);
        }
        return body.toByteArray();
    }

    private void validateCreatedIssue(byte[] body) throws IOException {
        JsonNode issue;
        try {
            issue = JiraSinkWriter.JSON.readTree(body);
        } catch (IOException e) {
            throw uncertain("malformed CREATE acknowledgement");
        }
        if (issue == null
                || !issue.isObject()
                || !issue.path("id").isTextual()
                || !JiraSinkWriter.ISSUE_ID.matcher(issue.path("id").asText()).matches()
                || !issue.path("key").isTextual()
                || !JiraSinkWriter.ISSUE_KEY.matcher(issue.path("key").asText()).matches()
                || hasContent(issue.path("errors"))
                || hasContent(issue.path("errorMessages"))
                || hasContent(issue.path("transition"))
                || hasContent(issue.path("watchers"))) {
            throw uncertain("invalid or contradictory CREATE acknowledgement");
        }
    }

    private static boolean hasContent(JsonNode node) {
        return !node.isMissingNode()
                && !node.isNull()
                && (!node.isContainerNode() || !node.isEmpty());
    }

    private String header(HttpResponse response, String name) throws IOException {
        Header[] values = response.getHeaders(name);
        if (values.length > 1) {
            throw uncertain("duplicate " + name + " response header");
        }
        return values.length == 0 || values[0].getValue() == null
                ? null
                : values[0].getValue().trim();
    }

    private static long retryAfter(String value) {
        if (value == null) {
            return -1;
        }
        if (value.matches("[0-9]+")) {
            try {
                return TimeUnit.SECONDS.toMillis(Long.parseLong(value));
            } catch (NumberFormatException e) {
                return Long.MAX_VALUE;
            }
        }
        try {
            return Math.max(
                    0,
                    ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                    .toInstant()
                                    .toEpochMilli()
                            - System.currentTimeMillis());
        } catch (DateTimeParseException e) {
            return -1;
        }
    }

    private JiraRequestException failure(String message) {
        return new JiraRequestException("Jira Sink " + operation + " " + message);
    }

    private JiraRequestException uncertain(String message) {
        return failure(
                message + "; mutation outcome may be unknown; reconcile the issue before replay");
    }

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
                // The request worker also owns and closes the socket.
            }
        }
    }

    private static final class JiraRequestException extends IOException {
        private static final long serialVersionUID = 1L;

        private JiraRequestException(String message) {
            super(message);
        }
    }
}
