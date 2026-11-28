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
package org.apache.seatunnel.e2e.connector.campaignmonitor;

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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * Independent HTTPS peer for the Campaign Monitor v3.3 read contract.
 *
 * <p>Literal expectations come from the scenario script. All certificate and user-token data are
 * synthetic, and API host aliases exist only inside the owned test container.
 */
public final class CampaignMonitorPeer {
    static final String HOST = "api.createsend.com";
    static final String TRAP = "credential-trap.invalid";
    static final String PASSWORD = "campaignmonitor-test-only";
    static final String TOKEN = "synthetic-campaignmonitor-api-key";
    static final String OTHER_TOKEN = "synthetic-campaignmonitor-rotated-key";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Path root;
    private final Map<String, AtomicInteger> indices = new ConcurrentHashMap<>();

    private CampaignMonitorPeer(Path root) {
        this.root = root;
    }

    /**
     * Runs the loopback TLS peer until the owner touches its stop marker.
     *
     * <p>This process never contacts an external service.
     */
    public static void main(String[] args) throws Exception {
        CampaignMonitorPeer peer = new CampaignMonitorPeer(Path.of(args[0]));
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(peer.root.resolve("trusted.p12"))) {
            keys.load(input, PASSWORD.toCharArray());
        }
        KeyManagerFactory manager =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(keys, PASSWORD.toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(manager.getKeyManagers(), null, null);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 443), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(context));
        server.setExecutor(executor);
        server.createContext("/", peer::handle);
        server.start();
        try {
            Files.writeString(peer.root.resolve("ready"), "ready");
            while (!Files.exists(peer.root.resolve("stop"))) {
                Thread.sleep(25);
            }
        } finally {
            server.stop(0);
            executor.shutdownNow();
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        String name = Files.readString(root.resolve("active")).trim();
        try {
            int index =
                    indices.computeIfAbsent(name, ignored -> new AtomicInteger()).getAndIncrement();
            JsonNode script = JSON.readTree(Files.readString(root.resolve(name + ".json")));
            JsonNode step = script.path(index);
            if (step.isMissingNode() || !matches(exchange, step)) {
                journal(name, ".invalid", "request contract mismatch at " + index);
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            journal(name, ".requests", index + " " + exchange.getRequestURI().getRawPath());
            journal(name, ".times", Long.toString(System.currentTimeMillis()));
            respond(exchange, name, step);
        } finally {
            exchange.close();
        }
    }

    private boolean matches(HttpExchange exchange, JsonNode step) throws IOException {
        String wire = exchange.getRequestURI().getRawPath();
        if (exchange.getRequestURI().getRawQuery() != null) {
            wire += "?" + exchange.getRequestURI().getRawQuery();
        }
        return HOST.equals(exchange.getRequestHeaders().getFirst("Host"))
                && step.path("target").asText().equals(wire)
                && "GET".equals(exchange.getRequestMethod())
                && ("Basic "
                                + Base64.getEncoder()
                                        .encodeToString(
                                                (step.path("token").asText(TOKEN) + ":")
                                                        .getBytes(StandardCharsets.US_ASCII)))
                        .equals(exchange.getRequestHeaders().getFirst("Authorization"))
                && "application/json".equals(exchange.getRequestHeaders().getFirst("Accept"))
                && "gzip".equals(exchange.getRequestHeaders().getFirst("Accept-Encoding"))
                && exchange.getRequestBody().read() == -1;
    }

    private void respond(HttpExchange exchange, String name, JsonNode step) throws IOException {
        String mode = step.path("mode").asText("json");
        int code = step.path("status").asInt(200);
        if (step.has("rateReset")) {
            exchange.getResponseHeaders().set("X-RateLimit-Reset", step.path("rateReset").asText());
        }
        if (step.has("retryAfter")) {
            exchange.getResponseHeaders().set("Retry-After", step.path("retryAfter").asText());
        }
        if (mode.equals("redirect")) {
            exchange.getResponseHeaders().set("Location", "https://" + TRAP + "/unexpected");
            exchange.sendResponseHeaders(code, -1);
            return;
        }
        byte[] bytes = step.path("response").asText().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        if (mode.equals("trickle") || mode.equals("partial")) {
            stream(exchange, name, bytes, mode.equals("partial"));
            return;
        }
        if (mode.equals("gzip") || mode.equals("bad-gzip")) {
            ByteArrayOutputStream compressed = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
                gzip.write(bytes);
            }
            bytes = compressed.toByteArray();
            if (mode.equals("bad-gzip")) {
                bytes[bytes.length - 8] ^= 1;
            }
            exchange.getResponseHeaders().set("Content-Encoding", "gzip");
        }
        exchange.sendResponseHeaders(code, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            try (OutputStream response = exchange.getResponseBody()) {
                response.write(bytes);
            }
        }
    }

    private void stream(HttpExchange exchange, String name, byte[] bytes, boolean partial)
            throws IOException {
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream response = exchange.getResponseBody()) {
            int offset = 0;
            if (partial) {
                offset = Math.min(1, bytes.length);
                response.write(bytes, 0, offset);
                response.flush();
            }
            journal(name, ".started", Long.toString(System.currentTimeMillis()));
            long deadline = System.nanoTime() + 20_000_000_000L;
            while (System.nanoTime() < deadline && !Files.exists(root.resolve("stop"))) {
                if (partial && !Files.exists(root.resolve(name + ".release"))) {
                    response.write(' ');
                } else {
                    if (offset >= bytes.length) {
                        break;
                    }
                    response.write(bytes[offset++]);
                }
                response.flush();
                Thread.sleep(40);
            }
        } catch (IOException ex) {
            journal(name, ".disconnect", Long.toString(System.currentTimeMillis()));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void journal(String name, String suffix, String line) throws IOException {
        Files.writeString(
                root.resolve(name + suffix),
                line + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }
}
