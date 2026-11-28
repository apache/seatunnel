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
package org.apache.seatunnel.e2e.connector.brevo;

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
 * Serves an independent sequence of read-only Brevo contract responses over real TLS.
 *
 * <p>The production hostname resolves to this loopback peer only in the owned engine container.
 * Literal path/query expectations come from provider contracts, not connector resource classes.
 */
public final class BrevoPeer {
    static final String HOST = "api.brevo.com";
    static final String TOKEN = "synthetic-brevo-token";
    static final String PASSWORD = "test-only-brevo-keystore-password";
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path directory;
    private final Map<String, AtomicInteger> indices = new ConcurrentHashMap<>();

    private BrevoPeer(Path directory) {
        this.directory = directory;
    }

    /**
     * Starts a container-local HTTPS peer and stops when its owner writes a shutdown marker.
     *
     * <p>The test CA is passed explicitly to the source and never changes the machine trust store.
     */
    public static void main(String[] args) throws Exception {
        BrevoPeer peer = new BrevoPeer(Path.of(args[0]));
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
        String name = Files.readString(directory.resolve("active")).trim();
        try {
            JsonNode script = JSON.readTree(Files.readString(directory.resolve(name + ".json")));
            int index =
                    indices.computeIfAbsent(name, ignored -> new AtomicInteger()).getAndIncrement();
            if (index >= script.size()) {
                reject(exchange, name, "unexpected request");
                return;
            }
            JsonNode step = script.get(index);
            if (!"GET".equals(exchange.getRequestMethod())
                    || exchange.getRequestBody().readNBytes(1).length != 0
                    || !HOST.equals(exchange.getRequestHeaders().getFirst("Host"))
                    || !TOKEN.equals(exchange.getRequestHeaders().getFirst("api-key"))
                    || exchange.getRequestHeaders().containsKey("Authorization")
                    || exchange.getRequestHeaders().containsKey("Cookie")
                    || !"application/json".equals(exchange.getRequestHeaders().getFirst("Accept"))
                    || !step.path("path").asText().equals(exchange.getRequestURI().getRawPath())
                    || !query(step.path("query").asText())
                            .equals(query(exchange.getRequestURI().getRawQuery()))) {
                reject(exchange, name, "request contract mismatch at step " + index);
                return;
            }
            append(name, ".requests", index + " " + step.path("path").asText());
            append(name, ".times", Long.toString(System.currentTimeMillis()));
            for (String header : new String[] {"Retry-After", "x-sib-ratelimit-reset"}) {
                if (step.has(header)) {
                    exchange.getResponseHeaders().set(header, step.path(header).asText());
                }
            }
            int status = step.path("status").asInt(200);
            String mode = step.path("mode").asText("json");
            if (mode.equals("redirect")) {
                exchange.getResponseHeaders().set("Location", "https://brevo-trap.invalid/private");
                exchange.sendResponseHeaders(status, -1);
                return;
            }
            byte[] body =
                    step.has("bytes")
                            ? Base64.getDecoder().decode(step.path("bytes").asText())
                            : step.path("response").asText().getBytes(StandardCharsets.UTF_8);
            if (mode.equals("partial") || mode.equals("trickle")) {
                stream(exchange, name, body, mode.equals("partial"));
                return;
            }
            if (mode.equals("gzip")) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(buffer)) {
                    gzip.write(body);
                }
                body = buffer.toByteArray();
                exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            }
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length != 0) {
                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
            }
        } catch (IllegalArgumentException invalid) {
            reject(exchange, name, "invalid request encoding");
        } finally {
            exchange.close();
        }
    }

    private void stream(HttpExchange exchange, String name, byte[] body, boolean partial)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body.length == 0 ? '{' : body[0]);
            output.flush();
            append(name, ".started", Long.toString(System.currentTimeMillis()));
            if (partial) {
                if (release(name)) {
                    output.write(body, 1, body.length - 1);
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
            append(name, ".disconnect", Long.toString(System.currentTimeMillis()));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean release(String name) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        try {
            while (!Files.exists(directory.resolve(name + ".release"))) {
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

    private static Map<String, String> query(String query) {
        Map<String, String> values = new HashMap<>();
        if (query == null || query.isEmpty()) {
            return values;
        }
        for (String pair : query.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            if (values.put(
                            key,
                            URLDecoder.decode(
                                    parts.length == 2 ? parts[1] : "", StandardCharsets.UTF_8))
                    != null) {
                throw new IllegalArgumentException("duplicate query parameter");
            }
        }
        return values;
    }

    private void reject(HttpExchange exchange, String name, String reason) throws IOException {
        append(name, ".invalid", reason);
        exchange.sendResponseHeaders(500, -1);
    }

    private void append(String name, String suffix, String value) throws IOException {
        Files.writeString(
                directory.resolve(name + suffix),
                value + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }
}
