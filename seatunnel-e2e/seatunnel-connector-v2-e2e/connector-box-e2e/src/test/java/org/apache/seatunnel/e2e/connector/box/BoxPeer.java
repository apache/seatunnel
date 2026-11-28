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
package org.apache.seatunnel.e2e.connector.box;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

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
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * Independent TLS contract peer for catalog reads, CCG, representations and AI reads.
 *
 * <p>Only the owned container resolves its two test origins. No test contacts Box or incurs an AI
 * operation, and the expected requests do not import production descriptors or serializers.
 */
public final class BoxPeer {
    static final String API = "box-api.test";
    static final String CONTENT = "box-content.test";
    static final String PASSWORD = "test-only-box-keystore-password";
    static final String TOKEN = "synthetic-box-access-token";
    static final String RENEWED = "synthetic-box-renewed-token";
    static final String CLIENT = "synthetic-box-client";
    static final String SECRET = "synthetic-box-secret+&=%";
    static final String SUBJECT = "9007199254740993";
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path directory;
    private final Map<String, AtomicInteger> positions = new ConcurrentHashMap<>();

    private BoxPeer(Path directory) {
        this.directory = directory;
    }

    /**
     * Starts both logical origins on one loopback TLS listener until the owning test stops it.
     *
     * <p>Separate hostnames preserve origin checks while avoiding a second service container.
     */
    public static void main(String[] args) throws Exception {
        BoxPeer peer = new BoxPeer(Path.of(args[0]));
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(peer.directory.resolve("trusted.p12"))) {
            keys.load(input, PASSWORD.toCharArray());
        }
        KeyManagerFactory manager =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(keys, PASSWORD.toCharArray());
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(manager.getKeyManagers(), null, null);
        ExecutorService threads = Executors.newCachedThreadPool();
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 443), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));
        server.setExecutor(threads);
        server.createContext("/", peer::handle);
        server.start();
        try {
            Files.writeString(peer.directory.resolve("ready"), "ready");
            while (!Files.exists(peer.directory.resolve("stop"))) {
                Thread.sleep(25L);
            }
        } finally {
            server.stop(0);
            threads.shutdownNow();
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        String scenario = Files.readString(directory.resolve("active")).trim();
        try {
            JsonNode script =
                    JSON.readTree(Files.readString(directory.resolve(scenario + ".json")));
            int position =
                    positions
                            .computeIfAbsent(scenario, ignored -> new AtomicInteger())
                            .getAndIncrement();
            if (position >= script.size()) {
                reject(exchange, scenario, "unexpected request");
                return;
            }
            JsonNode step = script.get(position);
            byte[] body = exchange.getRequestBody().readNBytes(1048577);
            if (body.length > 1048576
                    || !step.path("host")
                            .asText(API)
                            .equals(exchange.getRequestHeaders().getFirst("Host"))
                    || !step.path("path").asText().equals(exchange.getRequestURI().getRawPath())
                    || !step.path("method").asText("GET").equals(exchange.getRequestMethod())
                    || !form(step.path("query").asText())
                            .equals(form(exchange.getRequestURI().getRawQuery()))
                    || !authorized(exchange, step, body)) {
                reject(exchange, scenario, "request contract mismatch at step " + position);
                return;
            }
            append(
                    scenario,
                    ".requests",
                    position
                            + " "
                            + step.path("host").asText(API)
                            + " "
                            + step.path("path").asText());
            append(scenario, ".times", Long.toString(System.currentTimeMillis()));
            int status = step.path("status").asInt(200);
            String mode = step.path("mode").asText("json");
            if (step.has("retry")) {
                exchange.getResponseHeaders().set("Retry-After", step.path("retry").asText());
            }
            if (mode.equals("redirect")) {
                exchange.getResponseHeaders().set("Location", "https://box-trap.test/trap");
                exchange.sendResponseHeaders(status, -1);
                return;
            }
            if (mode.equals("hold") && !release(scenario)) {
                return;
            }
            byte[] response =
                    step.has("bytes")
                            ? Base64.getDecoder().decode(step.path("bytes").asText())
                            : step.path("response").asText().getBytes(StandardCharsets.UTF_8);
            if (mode.equals("partial") || mode.equals("trickle")) {
                stream(exchange, scenario, response, mode.equals("partial"));
                return;
            }
            if (mode.equals("gzip")) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(buffer)) {
                    gzip.write(response);
                }
                response = buffer.toByteArray();
                exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            }
            exchange.getResponseHeaders()
                    .set("Content-Type", step.path("content_type").asText("application/json"));
            exchange.sendResponseHeaders(status, response.length == 0 ? -1 : response.length);
            if (response.length != 0) {
                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(response);
                }
            }
        } catch (IllegalArgumentException e) {
            reject(exchange, scenario, "invalid request encoding");
        } finally {
            exchange.close();
        }
    }

    private boolean authorized(HttpExchange exchange, JsonNode step, byte[] body)
            throws IOException {
        String path = step.path("path").asText();
        if (path.equals("/oauth2/token")) {
            if (exchange.getRequestHeaders().containsKey("Authorization")) {
                return false;
            }
            Map<String, String> expected = new HashMap<>();
            expected.put("grant_type", "client_credentials");
            expected.put("client_id", CLIENT);
            expected.put("client_secret", SECRET);
            expected.put("box_subject_type", step.path("subject_type").asText("user"));
            expected.put("box_subject_id", SUBJECT);
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            return contentType != null
                    && contentType.startsWith("application/x-www-form-urlencoded")
                    && expected.equals(form(new String(body, StandardCharsets.UTF_8)));
        }
        if (!("Bearer " + step.path("token").asText(TOKEN))
                .equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
            return false;
        }
        String hint = exchange.getRequestHeaders().getFirst("x-rep-hints");
        if (!step.path("hint").asText().equals(hint == null ? "" : hint)) {
            return false;
        }
        if (step.path("method").asText("GET").equals("GET")) {
            return body.length == 0;
        }
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        return contentType != null
                && contentType.startsWith("application/json")
                && step.path("body").equals(JSON.readTree(body));
    }

    private void stream(HttpExchange exchange, String scenario, byte[] response, boolean partial)
            throws IOException {
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(response.length == 0 ? '{' : response[0]);
            output.flush();
            append(scenario, ".started", Long.toString(System.currentTimeMillis()));
            if (partial) {
                if (release(scenario)) {
                    output.write(response, 1, response.length - 1);
                }
            } else {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
                while (System.nanoTime() - deadline < 0) {
                    output.write(' ');
                    output.flush();
                    Thread.sleep(50L);
                }
            }
        } catch (IOException closed) {
            append(scenario, ".disconnect", Long.toString(System.currentTimeMillis()));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean release(String scenario) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        try {
            while (!Files.exists(directory.resolve(scenario + ".release"))) {
                if (Files.exists(directory.resolve("stop")) || System.nanoTime() - deadline >= 0) {
                    return false;
                }
                Thread.sleep(25L);
            }
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static Map<String, String> form(String value) {
        Map<String, String> fields = new HashMap<>();
        if (value == null || value.isEmpty()) {
            return fields;
        }
        for (String pair : value.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String decoded =
                    URLDecoder.decode(parts.length == 2 ? parts[1] : "", StandardCharsets.UTF_8);
            if (key.equals("fields")) {
                String[] projection = decoded.split(",", -1);
                Arrays.sort(projection);
                decoded = String.join(",", projection);
            }
            if (fields.put(key, decoded) != null) {
                throw new IllegalArgumentException("duplicate query or form parameter");
            }
        }
        return fields;
    }

    private void reject(HttpExchange exchange, String scenario, String reason) throws IOException {
        append(scenario, ".invalid", reason);
        exchange.sendResponseHeaders(500, -1);
    }

    private void append(String scenario, String suffix, String value) throws IOException {
        Files.writeString(
                directory.resolve(scenario + suffix),
                value + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }
}
