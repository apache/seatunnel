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

package org.apache.seatunnel.e2e.connector.avni;

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
import java.util.Locale;
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
public final class AvniPeer {
    static final String HOST = "avni-peer";
    static final int PORT = 22443;
    static final int UNTRUSTED_PORT = 22445;
    static final int IDENTITY_PORT = 22444;
    static final String IDENTITY_HOST = "avni-idp";
    static final String PASSWORD = "fixture-store-password";
    static final String TOKEN = "synthetic-avni-management-token";
    static final String RENEWED = "synthetic-avni-renewed-token";
    static final String CLIENT = "synthetic_avni_client";
    static final String USERNAME = "synthetic-user+reserved@example.invalid";
    static final String SECRET = "synthetic-password-+&=%";
    private final Path root;
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    private AvniPeer(Path root) {
        this.root = root;
    }

    /**
     * Starts isolated trusted and untrusted peers until the test requests shutdown.
     *
     * <p>The same protocol process serves the engine and reader recovery scenarios.
     */
    public static void main(String[] args) throws Exception {
        AvniPeer peer = new AvniPeer(Path.of(args[0]));
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpsServer trusted = peer.server(PORT, "trusted", executor);
        HttpsServer untrusted = peer.server(UNTRUSTED_PORT, "untrusted", executor);
        HttpsServer identity = peer.server(IDENTITY_PORT, "trusted", executor);
        try {
            Files.writeString(peer.root.resolve("ready"), "ready");
            while (!Files.exists(peer.root.resolve("stop"))) {
                Thread.sleep(25L);
            }
        } finally {
            trusted.stop(0);
            untrusted.stop(0);
            identity.stop(0);
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
            if (script.length != 7
                    || !script[0].equals(request.getRequestURI().getRawPath())
                    || !query(script[1]).equals(query(request.getRequestURI().getRawQuery()))
                    || !"application/json".equals(request.getRequestHeaders().getFirst("Accept"))
                    || !credentialsAndBody(request, script[6])) {
                invalid(request, scenario);
                return;
            }
            journal(
                    scenario,
                    ".requests",
                    ("login".equals(script[6]) ? "POST " : "")
                            + script[0]
                            + (script[1].isEmpty() ? "" : "?" + script[1]));
            journal(scenario, ".times", Long.toString(System.currentTimeMillis()));
            int status = Integer.parseInt(script[2]);
            String mode = script[3];
            if (!script[4].isEmpty()) {
                request.getResponseHeaders().set("Retry-After", script[4]);
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
                        .set("Location", "https://" + HOST + ":" + PORT + "/credential-trap");
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
            request.getResponseHeaders().set("Content-Type", "application/json");
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
        request.getResponseHeaders().set("Content-Type", "application/json");
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

    private static boolean credentialsAndBody(HttpExchange request, String expected)
            throws IOException {
        String path = request.getRequestURI().getRawPath();
        boolean login = "login".equals(expected);
        int port = request.getLocalAddress().getPort();
        if ((login && port != IDENTITY_PORT)
                || (!login && port == IDENTITY_PORT)
                || request.getRequestHeaders().containsKey("Authorization")) {
            return false;
        }
        if (!login) {
            String token = request.getRequestHeaders().getFirst("auth-token");
            boolean discovery = "none".equals(expected);
            return "GET".equals(request.getRequestMethod())
                    && (discovery
                            ? "/idp-details".equals(path) && token == null
                            : ("renewed".equals(expected) ? RENEWED : TOKEN).equals(token))
                    && !request.getRequestHeaders().containsKey("X-Amz-Target")
                    && request.getRequestBody().read() == -1;
        }
        String type = request.getRequestHeaders().getFirst("Content-Type");
        if (!"POST".equals(request.getRequestMethod())
                || !"/".equals(path)
                || request.getRequestHeaders().containsKey("auth-token")
                || !"AWSCognitoIdentityProviderService.InitiateAuth"
                        .equals(request.getRequestHeaders().getFirst("X-Amz-Target"))
                || type == null
                || !type.toLowerCase(Locale.ROOT).startsWith("application/x-amz-json-1.1")) {
            return false;
        }
        byte[] bytes = request.getRequestBody().readNBytes(8193);
        if (bytes.length > 8192) {
            return false;
        }
        String text = new String(bytes, StandardCharsets.UTF_8);
        int[] at = {0};
        Map<String, Object> body = object(text, at, 0);
        whitespace(text, at);
        return at[0] == text.length()
                && body.equals(
                        Map.of(
                                "AuthFlow",
                                "USER_PASSWORD_AUTH",
                                "ClientId",
                                CLIENT,
                                "AuthParameters",
                                Map.of("USERNAME", USERNAME, "PASSWORD", SECRET)));
    }

    // A small independent parser accepts only bounded nested string objects used by Cognito auth.
    private static Map<String, Object> object(String text, int[] at, int depth) {
        if (depth > 2) {
            throw new IllegalArgumentException("authentication depth");
        }
        expect(text, at, '{');
        Map<String, Object> result = new TreeMap<>();
        whitespace(text, at);
        if (at[0] < text.length() && text.charAt(at[0]) == '}') {
            at[0]++;
            return result;
        }
        while (true) {
            String key = string(text, at);
            expect(text, at, ':');
            whitespace(text, at);
            Object value =
                    at[0] < text.length() && text.charAt(at[0]) == '{'
                            ? object(text, at, depth + 1)
                            : string(text, at);
            if (result.put(key, value) != null) {
                throw new IllegalArgumentException("duplicate authentication field");
            }
            whitespace(text, at);
            if (at[0] < text.length() && text.charAt(at[0]) == '}') {
                at[0]++;
                return result;
            }
            expect(text, at, ',');
        }
    }

    private static String string(String text, int[] at) {
        expect(text, at, '"');
        StringBuilder value = new StringBuilder();
        while (at[0] < text.length()) {
            char ch = text.charAt(at[0]++);
            if (ch == '"') {
                return value.toString();
            }
            if (ch < 32) {
                throw new IllegalArgumentException("control in token field");
            }
            if (ch != '\\') {
                value.append(ch);
                continue;
            }
            if (at[0] >= text.length()) {
                throw new IllegalArgumentException("unterminated escape");
            }
            char escaped = text.charAt(at[0]++);
            switch (escaped) {
                case '"':
                case '\\':
                case '/':
                    value.append(escaped);
                    break;
                case 'b':
                    value.append('\b');
                    break;
                case 'f':
                    value.append('\f');
                    break;
                case 'n':
                    value.append('\n');
                    break;
                case 'r':
                    value.append('\r');
                    break;
                case 't':
                    value.append('\t');
                    break;
                case 'u':
                    if (at[0] + 4 > text.length()) {
                        throw new IllegalArgumentException("short Unicode escape");
                    }
                    int code = 0;
                    for (int index = 0; index < 4; index++) {
                        int digit = Character.digit(text.charAt(at[0]++), 16);
                        if (digit < 0) {
                            throw new IllegalArgumentException("invalid Unicode escape");
                        }
                        code = 16 * code + digit;
                    }
                    value.append((char) code);
                    break;
                default:
                    throw new IllegalArgumentException("invalid token escape");
            }
        }
        throw new IllegalArgumentException("unterminated token field");
    }

    private static void whitespace(String text, int[] at) {
        while (at[0] < text.length() && " \t\n\r".indexOf(text.charAt(at[0])) >= 0) {
            at[0]++;
        }
    }

    private static void expect(String text, int[] at, char expected) {
        whitespace(text, at);
        if (at[0] >= text.length() || text.charAt(at[0]++) != expected) {
            throw new IllegalArgumentException("invalid token JSON");
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
