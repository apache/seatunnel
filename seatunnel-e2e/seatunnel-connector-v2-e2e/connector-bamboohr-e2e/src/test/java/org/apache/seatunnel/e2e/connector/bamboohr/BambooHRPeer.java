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

package org.apache.seatunnel.e2e.connector.bamboohr;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

/**
 * An independent scripted BambooHR HTTPS peer. It verifies the wire contract and records only
 * method, request target, timestamp and the nonsecret synthetic report payload.
 */
public final class BambooHRPeer {
    static final String PASSWORD = "bamboo-fixture-store";
    static final String KEY = "fixture-bamboo-credential-22d9";
    static final String PRIVATE = "fixture-sensitive-hr-comment";
    static final String MAIN = "tenant-fixture.bamboohr.com";
    static final String OTHER = "redirect-fixture.bamboohr.com";
    static final String UNTRUSTED = "untrusted-fixture.bamboohr.com";
    static final String WRONG = "wrong-fixture.bamboohr.com";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Path directory;

    private BambooHRPeer(Path directory) {
        this.directory = directory;
    }

    /**
     * Hosts two TLS identities inside an isolated real Zeta container.
     *
     * <p>The listeners share one independently validated request script for each engine job.
     */
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        BambooHRPeer peer = new BambooHRPeer(root);
        ExecutorService workers = Executors.newFixedThreadPool(6);
        HttpsServer trusted = peer.listen("127.0.0.1", "trusted.p12", workers);
        HttpsServer untrusted = peer.listen("127.0.0.2", "untrusted.p12", workers);
        try {
            Files.createFile(root.resolve("ready"));
            while (!Files.exists(root.resolve("stop"))) {
                Thread.sleep(50);
            }
        } finally {
            trusted.stop(0);
            untrusted.stop(0);
            workers.shutdownNow();
        }
    }

    private HttpsServer listen(String address, String identity, ExecutorService workers)
            throws Exception {
        char[] password = PASSWORD.toCharArray();
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(directory.resolve(identity))) {
            keys.load(input, password);
        }
        KeyManagerFactory manager =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(keys, password);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(manager.getKeyManagers(), null, null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress(address, 443), 32);
        server.setHttpsConfigurator(new HttpsConfigurator(context));
        server.setExecutor(workers);
        server.createContext("/", this::handle);
        server.start();
        return server;
    }

    private void handle(HttpExchange exchange) throws IOException {
        String active = Files.readString(directory.resolve("active")).trim();
        try {
            byte[] bytes = exchange.getRequestBody().readNBytes(54000);
            if (bytes.length == 54000) {
                throw new IllegalArgumentException("unexpected request body size");
            }
            JsonNode data = bytes.length == 0 ? null : JSON.readTree(bytes);
            JsonNode step = next(active, exchange, data);
            reply(active, exchange, step);
        } catch (IllegalArgumentException e) {
            Files.writeString(
                    directory.resolve(active + ".invalid"),
                    e.getMessage(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
            exchange.sendResponseHeaders(400, -1);
        } finally {
            exchange.close();
        }
    }

    /**
     * Validates and reserves a scripted response before waiting or writing its body.
     *
     * <p>Other requests can progress while a savepoint test holds a response.
     */
    private synchronized JsonNode next(String active, HttpExchange exchange, JsonNode data)
            throws IOException {
        Path record = directory.resolve(active + ".requests");
        long offset = 0;
        if (Files.isRegularFile(record)) {
            try (Stream<String> lines = Files.lines(record)) {
                offset = lines.count();
            }
        }
        JsonNode step =
                JSON.readTree(Files.readString(directory.resolve(active + ".program")))
                        .path((int) offset);
        String method = exchange.getRequestMethod();
        String target = exchange.getRequestURI().toString();
        var headers = exchange.getRequestHeaders();
        exact(!step.isMissingNode(), "received an unexpected additional request");
        exact(step.path("method").asText().equals(method), "request method mismatch");
        exact(step.path("target").asText().equals(target), "request target mismatch");
        exact(MAIN.equals(headers.getFirst("Host")), "host mismatch or redirected request");
        List<String> auth = headers.get("Authorization");
        exact(
                auth != null && auth.size() == 1 && auth.get(0).startsWith("Basic "),
                "missing or ambiguous Basic authentication");
        byte[] decoded = Base64.getDecoder().decode(auth.get(0).substring(6));
        exact(
                (KEY + ":x").equals(new String(decoded, StandardCharsets.US_ASCII)),
                "Basic username or password mismatch");
        exact("application/json".equals(headers.getFirst("Accept")), "missing JSON Accept");
        if ("POST".equals(method)) {
            exact(
                    "application/json".equals(headers.getFirst("Content-Type")),
                    "custom-report Content-Type is not exactly application/json");
            exact(step.path("body").equals(data), "custom-report body mismatch");
        } else {
            exact(data == null, "GET unexpectedly carried a request body");
        }
        ObjectNode entry =
                JSON.createObjectNode()
                        .put("method", method)
                        .put("target", target)
                        .put("nanoTime", System.nanoTime());
        if (data != null) {
            entry.set("body", data);
        }
        Files.writeString(
                record, entry + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        return step;
    }

    private void reply(String active, HttpExchange exchange, JsonNode step) throws IOException {
        int status = step.path("status").asInt();
        String mode = step.path("mode").asText();
        var headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "application/json");
        if (step.has("retryAfter")) {
            headers.set("Retry-After", step.path("retryAfter").asText());
        }
        if (status / 100 == 3) {
            headers.set("Location", "https://" + OTHER + "/api/v1/meta/fields");
        }
        try {
            if ("hold".equals(mode)) {
                waitForRelease(active);
            }
            byte[] body = step.path("response").asText().getBytes(StandardCharsets.UTF_8);
            if ("gzip".equals(mode)) {
                ByteArrayOutputStream compressed = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
                    gzip.write(body);
                }
                body = compressed.toByteArray();
                headers.set("Content-Encoding", "gzip");
            }
            if ("trickle".equals(mode)) {
                exchange.sendResponseHeaders(status, 0);
                try (OutputStream output = exchange.getResponseBody()) {
                    long until = System.nanoTime() + 25_000_000_000L;
                    while (System.nanoTime() < until) {
                        output.write(' ');
                        output.flush();
                        Thread.sleep(100);
                    }
                }
                return;
            }
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            if (!"hold".equals(mode)
                    && !"trickle".equals(mode)
                    && !"gzip".equals(mode)
                    && bodySmall(step)) {
                throw e;
            }
            Files.writeString(directory.resolve(active + ".disconnect"), "closed");
        }
    }

    private static boolean bodySmall(JsonNode step) {
        return step.path("response").asText().length() < 1024;
    }

    private void waitForRelease(String active) throws InterruptedException {
        long deadline = System.nanoTime() + 65_000_000_000L;
        while (!Files.exists(directory.resolve(active + ".release"))
                && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
    }

    private static void exact(boolean value, String message) {
        if (!value) {
            throw new IllegalArgumentException(message + "\n");
        }
    }
}
