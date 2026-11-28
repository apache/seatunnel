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

package org.apache.seatunnel.e2e.connector.azuretable;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
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
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * Independent HTTPS provider with per-scenario request scripts.
 *
 * <p>Only validated requests are journaled. Response data and expected routes are supplied by the
 * integration test without importing the connector's resource or parser definitions.
 */
public final class AzureTablePeer {
    static final String ACCOUNT = "fixturetableaccount";
    static final String HOST = ACCOUNT + ".table.core.windows.net";
    static final String CHINA_HOST = ACCOUNT + ".table.core.chinacloudapi.cn";
    static final String PASSWORD = "fixture-store-password";
    static final String KEY =
            Base64.getEncoder()
                    .encodeToString(
                            "independent-table-fixture-key-000000000000000000000000000000000000"
                                    .getBytes(StandardCharsets.UTF_8));
    private final Path root;
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    private AzureTablePeer(Path root) {
        this.root = root;
    }

    /**
     * Starts isolated trusted and untrusted peers until the test requests shutdown.
     *
     * <p>The same protocol process serves the engine and reader recovery scenarios.
     */
    public static void main(String[] args) throws Exception {
        AzureTablePeer peer = new AzureTablePeer(Path.of(args[0]));
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpsServer trusted = peer.server(443, "trusted", executor);
        try {
            Files.writeString(peer.root.resolve("ready"), "ready");
            while (!Files.exists(peer.root.resolve("stop"))) {
                Thread.sleep(25L);
            }
        } finally {
            trusted.stop(0);
            executor.shutdownNow();
        }
    }

    private HttpsServer server(int port, String name, ExecutorService executor) throws Exception {
        char[] password = PASSWORD.toCharArray();
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(root.resolve(name + ".p12"))) {
            keys.load(input, password);
        }
        KeyManagerFactory manager =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(keys, password);
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(manager.getKeyManagers(), null, null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));
        server.setExecutor(executor);
        server.createContext("/", this::handle);
        server.start();
        return server;
    }

    private void handle(HttpExchange request) throws IOException {
        String scenario = Files.readString(root.resolve("active")).trim();
        try {
            String[] lines = Files.readString(root.resolve(scenario + ".program")).split("\n");
            int index =
                    attempts.computeIfAbsent(scenario, ignored -> new AtomicInteger())
                            .getAndIncrement();
            if (index >= lines.length) {
                invalid(request, scenario);
                return;
            }
            String[] script = lines[index].split("\t", -1);
            if (script.length != 10
                    || !script[0].equals(request.getRequestURI().getRawPath())
                    || !query(script[1]).equals(query(request.getRequestURI().getRawQuery()))
                    || !"application/json;odata=fullmetadata"
                            .equals(request.getRequestHeaders().getFirst("Accept"))
                    || !credentialsAndBody(request, script[9])) {
                invalid(request, scenario);
                return;
            }
            journal(
                    scenario,
                    ".requests",
                    script[0] + (script[1].isEmpty() ? "" : "?" + script[1]));
            journal(scenario, ".times", Long.toString(System.currentTimeMillis()));
            int status = Integer.parseInt(script[2]);
            String mode = script[3];
            if (!script[4].isEmpty()) {
                request.getResponseHeaders().set("Retry-After", script[4]);
            }
            String[] tokens = {"NextTableName", "NextPartitionKey", "NextRowKey"};
            for (int token = 0; token < tokens.length; token++) {
                if (!script[6 + token].isEmpty()) {
                    request.getResponseHeaders()
                            .set("x-ms-continuation-" + tokens[token], script[6 + token]);
                }
            }
            byte[] body = Base64.getDecoder().decode(script[5]);
            if ("delay".equals(mode)) {
                try {
                    Thread.sleep(1500L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            if ("redirect".equals(mode)) {
                request.getResponseHeaders()
                        .set("Location", "https://" + HOST + "/credential-trap");
                request.sendResponseHeaders(status, -1);
                return;
            }
            if ("hold".equals(mode) && !release(scenario)) {
                return;
            }
            if ("stream".equals(mode) || "partial".equals(mode)) {
                stream(request, scenario, body, "partial".equals(mode));
                return;
            }
            if ("gzip".equals(mode)) {
                ByteArrayOutputStream encoded = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(encoded)) {
                    gzip.write(body);
                }
                body = encoded.toByteArray();
                request.getResponseHeaders().set("Content-Encoding", "gzip");
            }
            request.getResponseHeaders().set("Content-Type", "application/json;odata=fullmetadata");
            request.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length != 0) {
                try (OutputStream output = request.getResponseBody()) {
                    output.write(body);
                } catch (IOException disconnected) {
                    journal(scenario, ".disconnect", "body disconnected");
                }
            }
        } catch (IllegalArgumentException e) {
            invalid(request, scenario);
        } finally {
            request.close();
        }
    }

    private void stream(HttpExchange request, String scenario, byte[] body, boolean partial)
            throws IOException {
        request.getResponseHeaders().set("Content-Type", "application/json;odata=fullmetadata");
        request.sendResponseHeaders(200, 0);
        try (OutputStream output = request.getResponseBody()) {
            output.write('{');
            output.flush();
            journal(scenario, ".started", Long.toString(System.currentTimeMillis()));
            if (partial) {
                if (release(scenario)) {
                    output.write(body, 1, body.length - 1);
                }
            } else {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
                while (System.nanoTime() - deadline < 0) {
                    output.write(' ');
                    output.flush();
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        } catch (IOException disconnected) {
            journal(scenario, ".disconnect", Long.toString(System.currentTimeMillis()));
        }
    }

    private boolean release(String scenario) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(180);
        while (!Files.exists(root.resolve(scenario + ".release"))
                && System.nanoTime() - deadline < 0) {
            try {
                Thread.sleep(25L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return Files.exists(root.resolve(scenario + ".release"));
    }

    private void invalid(HttpExchange request, String scenario) throws IOException {
        journal(
                scenario,
                ".invalid",
                "unexpected method, origin path, query, headers, or request body");
        request.sendResponseHeaders(400, -1);
    }

    private synchronized void journal(String scenario, String suffix, String data)
            throws IOException {
        Files.writeString(
                root.resolve(scenario + suffix),
                data + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    private static boolean credentialsAndBody(HttpExchange request, String host)
            throws IOException {
        if (!"GET".equals(request.getRequestMethod())
                || !"gzip".equals(request.getRequestHeaders().getFirst("Accept-Encoding"))
                || !host.equals(request.getRequestHeaders().getFirst("Host"))
                || !"2019-02-02".equals(request.getRequestHeaders().getFirst("x-ms-version"))
                || !"3.0".equals(request.getRequestHeaders().getFirst("DataServiceVersion"))
                || !"3.0".equals(request.getRequestHeaders().getFirst("MaxDataServiceVersion"))
                || request.getRequestHeaders().containsKey("Content-MD5")
                || request.getRequestHeaders().containsKey("Content-Type")
                || request.getRequestHeaders().containsKey("auth-token")
                || request.getRequestBody().read() != -1) {
            return false;
        }
        String date = request.getRequestHeaders().getFirst("x-ms-date");
        if (date == null) {
            return false;
        }
        try {
            Instant sent =
                    ZonedDateTime.parse(date, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            if (Math.abs(Duration.between(sent, Instant.now()).getSeconds()) > 300) {
                return false;
            }
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(Base64.getDecoder().decode(KEY), "HmacSHA256"));
            String signed =
                    "GET\n\n\n" + date + "\n/" + ACCOUNT + request.getRequestURI().getRawPath();
            String expected =
                    "SharedKey "
                            + ACCOUNT
                            + ":"
                            + Base64.getEncoder()
                                    .encodeToString(
                                            mac.doFinal(signed.getBytes(StandardCharsets.UTF_8)));
            return expected.equals(request.getRequestHeaders().getFirst("Authorization"));
        } catch (GeneralSecurityException | DateTimeException error) {
            return false;
        }
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> values = new TreeMap<>();
        if (raw != null && !raw.isEmpty()) {
            for (String entry : raw.split("&")) {
                String[] pair = entry.split("=", 2);
                String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
                String value =
                        pair.length == 2 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "";
                if (values.put(key, value) != null) {
                    throw new IllegalArgumentException("duplicate query parameter");
                }
            }
        }
        return values;
    }
}
