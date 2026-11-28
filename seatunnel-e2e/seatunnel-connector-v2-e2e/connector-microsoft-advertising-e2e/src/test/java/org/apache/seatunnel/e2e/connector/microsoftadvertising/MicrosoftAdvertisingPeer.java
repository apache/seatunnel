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
package org.apache.seatunnel.e2e.connector.microsoftadvertising;

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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Independent REST, OAuth and signed-download contract peer.
 *
 * <p>Its scripts contain literal provider requests and expected records, never connector
 * descriptors. Fixed production hostnames resolve only inside the owned Java 11 test container.
 */
public final class MicrosoftAdvertisingPeer {
    static final String PASSWORD = "test-only-store-password";
    static final String AUTH = "login.microsoftonline.com";
    static final String CUSTOMER = "clientcenter.api.bingads.microsoft.com";
    static final String CAMPAIGN = "campaign.api.bingads.microsoft.com";
    static final String REPORT = "reporting.api.bingads.microsoft.com";
    static final String BULK = "bulk.api.bingads.microsoft.com";
    static final String DOWNLOAD = "signed-download.example";
    static final String TOKEN = "synthetic-advertising-access-token";
    static final String DEVELOPER = "synthetic-advertising-developer-token";
    static final String CLIENT = "synthetic-advertising-client";
    static final String SECRET = "synthetic-advertising-secret+&=%";
    static final String REFRESH = "synthetic-advertising-refresh+&=%";
    static final String ACCOUNT = "9007199254740993";
    static final String CUSTOMER_ID = "9007199254740995";
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path root;
    private final Map<String, AtomicInteger> indices = new ConcurrentHashMap<>();

    private MicrosoftAdvertisingPeer(Path root) {
        this.root = root;
    }

    /**
     * Starts one isolated TLS peer and stops it when the owning test requests shutdown.
     *
     * <p>The process binds loopback only and never contacts the public provider.
     */
    public static void main(String[] args) throws Exception {
        MicrosoftAdvertisingPeer peer = new MicrosoftAdvertisingPeer(Path.of(args[0]));
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(peer.root.resolve("trusted.p12"))) {
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
            Files.writeString(peer.root.resolve("ready"), "ready");
            while (!Files.exists(peer.root.resolve("stop"))) {
                Thread.sleep(25L);
            }
        } finally {
            server.stop(0);
            threads.shutdownNow();
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        String name = Files.readString(root.resolve("active")).trim();
        try {
            JsonNode script = JSON.readTree(Files.readString(root.resolve(name + ".json")));
            int index =
                    indices.computeIfAbsent(name, ignored -> new AtomicInteger()).getAndIncrement();
            if (index >= script.size()) {
                reject(exchange, name, "unexpected request");
                return;
            }
            JsonNode step = script.get(index);
            String host = exchange.getRequestHeaders().getFirst("Host");
            byte[] input = exchange.getRequestBody().readNBytes(1048577);
            if (input.length > 1048576
                    || !step.path("host").asText().equals(host)
                    || !step.path("path").asText().equals(exchange.getRequestURI().getRawPath())
                    || !step.path("method").asText().equals(exchange.getRequestMethod())
                    || !form(step.path("query").asText())
                            .equals(form(exchange.getRequestURI().getRawQuery()))
                    || !credentials(exchange, step, input)) {
                reject(exchange, name, "request contract mismatch at " + index);
                return;
            }
            journal(name, ".requests", index + " " + host + " " + step.path("path").asText());
            journal(name, ".times", Long.toString(System.currentTimeMillis()));
            int status = step.path("status").asInt(200);
            String mode = step.path("mode").asText("json");
            if (step.has("retry")) {
                exchange.getResponseHeaders().set("Retry-After", step.path("retry").asText());
            }
            if (mode.equals("redirect")) {
                exchange.getResponseHeaders()
                        .set("Location", "https://credential-trap.invalid/trap");
                exchange.sendResponseHeaders(status, -1);
                return;
            }
            if (mode.equals("hold") && !release(name)) {
                return;
            }
            byte[] bytes = step.path("response").asText().getBytes(StandardCharsets.UTF_8);
            if (mode.equals("trickle") || mode.equals("partial")) {
                stream(exchange, name, bytes, mode.equals("partial"));
                return;
            }
            if (mode.equals("zip")
                    || mode.equals("two-zip")
                    || mode.equals("corrupt-zip")
                    || mode.equals("zip-trickle")) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                try (ZipOutputStream zip = new ZipOutputStream(buffer)) {
                    zip.putNextEntry(new ZipEntry("report.csv"));
                    zip.write(bytes);
                    zip.closeEntry();
                    if (mode.equals("two-zip")) {
                        zip.putNextEntry(new ZipEntry("second.csv"));
                        zip.write(bytes);
                        zip.closeEntry();
                    }
                }
                bytes = buffer.toByteArray();
                if (mode.equals("zip-trickle")) {
                    stream(exchange, name, bytes, false);
                    return;
                }
                if (mode.equals("corrupt-zip")) {
                    bytes = new byte[] {'P', 'K', 3, 4, 0};
                }
                exchange.getResponseHeaders().set("Content-Type", "application/zip");
            } else if (mode.equals("gzip") || mode.equals("json-gzip")) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(buffer)) {
                    gzip.write(bytes);
                }
                bytes = buffer.toByteArray();
                if (mode.equals("json-gzip")) {
                    exchange.getResponseHeaders().set("Content-Encoding", "gzip");
                }
            } else {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
            }
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(bytes);
                }
            }
        } catch (IllegalArgumentException e) {
            reject(exchange, name, "invalid request encoding");
        } finally {
            exchange.close();
        }
    }

    private boolean credentials(HttpExchange exchange, JsonNode step, byte[] body)
            throws IOException {
        String host = step.path("host").asText();
        String accept = exchange.getRequestHeaders().getFirst("Accept");
        if (!(host.equals(DOWNLOAD) ? "application/octet-stream" : "application/json")
                .equals(accept)) {
            return false;
        }
        if (host.equals(AUTH)) {
            if (exchange.getRequestHeaders().containsKey("Authorization")
                    || exchange.getRequestHeaders().containsKey("DeveloperToken")
                    || !"application/x-www-form-urlencoded"
                            .equals(exchange.getRequestHeaders().getFirst("Content-Type"))) {
                return false;
            }
            Map<String, String> expected = new HashMap<>();
            expected.put("client_id", CLIENT);
            expected.put("client_secret", SECRET);
            expected.put("refresh_token", REFRESH);
            expected.put("grant_type", "refresh_token");
            expected.put("scope", "https://ads.microsoft.com/msads.manage offline_access");
            return expected.equals(form(new String(body, StandardCharsets.UTF_8)));
        }
        if (host.equals(DOWNLOAD) || host.equals("credential-trap.invalid")) {
            for (String header :
                    new String[] {
                        "Authorization",
                        "AuthenticationToken",
                        "DeveloperToken",
                        "CustomerId",
                        "CustomerAccountId",
                        "Cookie"
                    }) {
                if (exchange.getRequestHeaders().containsKey(header)) {
                    return false;
                }
            }
            return body.length == 0;
        }
        String token = step.path("token").asText(TOKEN);
        if (!("Bearer " + token).equals(exchange.getRequestHeaders().getFirst("Authorization"))
                || !DEVELOPER.equals(exchange.getRequestHeaders().getFirst("DeveloperToken"))
                || !"application/json".equals(exchange.getRequestHeaders().getFirst("Content-Type"))
                || exchange.getRequestHeaders().containsKey("AuthenticationToken")) {
            return false;
        }
        String account = step.path("account").asText();
        String customer = step.path("customer").asText();
        if (!equalHeader(exchange, "CustomerAccountId", account)
                || !equalHeader(exchange, "CustomerId", customer)) {
            return false;
        }
        JsonNode actual = JSON.readTree(body);
        JsonNode expected = step.path("body");
        return expected.equals(actual);
    }

    private static boolean equalHeader(HttpExchange exchange, String name, String expected) {
        String value = exchange.getRequestHeaders().getFirst(name);
        return expected.isEmpty() ? value == null : expected.equals(value);
    }

    private void stream(HttpExchange exchange, String name, byte[] body, boolean partial)
            throws IOException {
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream output = exchange.getResponseBody()) {
            int prefix =
                    body.length >= 4 && body[0] == 'P' && body[1] == 'K'
                            ? Math.min(32, body.length)
                            : Math.min(1, body.length);
            if (prefix == 0) {
                output.write('{');
            } else {
                output.write(body, 0, prefix);
            }
            output.flush();
            journal(name, ".started", Long.toString(System.currentTimeMillis()));
            if (partial) {
                if (release(name)) {
                    output.write(body, prefix, body.length - prefix);
                }
            } else {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
                while (System.nanoTime() - deadline < 0) {
                    output.write(' ');
                    output.flush();
                    Thread.sleep(50L);
                }
            }
        } catch (IOException disconnected) {
            journal(name, ".disconnect", Long.toString(System.currentTimeMillis()));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean release(String name) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        try {
            while (!Files.exists(root.resolve(name + ".release"))) {
                if (Files.exists(root.resolve("stop")) || System.nanoTime() - deadline >= 0) {
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
        Map<String, String> result = new HashMap<>();
        if (value == null || value.isEmpty()) {
            return result;
        }
        for (String part : value.split("&")) {
            String[] pair = part.split("=", 2);
            String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
            if (result.put(
                            key,
                            URLDecoder.decode(
                                    pair.length == 2 ? pair[1] : "", StandardCharsets.UTF_8))
                    != null) {
                throw new IllegalArgumentException("duplicate form field");
            }
        }
        return result;
    }

    private void reject(HttpExchange exchange, String name, String reason) throws IOException {
        journal(name, ".invalid", reason);
        exchange.sendResponseHeaders(500, -1);
    }

    private void journal(String name, String suffix, String value) throws IOException {
        Files.writeString(
                root.resolve(name + suffix),
                value + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }
}
