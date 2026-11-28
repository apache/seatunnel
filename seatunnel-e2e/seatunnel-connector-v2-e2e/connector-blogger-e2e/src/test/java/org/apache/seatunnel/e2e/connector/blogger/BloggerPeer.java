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

package org.apache.seatunnel.e2e.connector.blogger;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.X509ExtendedKeyManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

/**
 * Independent scripted Java HTTPS peer for Blogger API reads and the OAuth grant.
 *
 * <p>Host-specific checks keep request credentials out of fixture logs.
 */
public final class BloggerPeer {
    static final String PASSWORD = "blogger-fixture-keystore";
    static final String CLIENT_ID = "fixture-id.apps.googleusercontent.com";
    static final String CLIENT_SECRET = "fixture-client+secret&=/";
    static final String REFRESH = "fixture-refresh token+&=/";
    static final String TOKEN = "fixture-access-TX000";
    static final String NEXT_ACCESS = "fixture-access-TXnext";
    static final String PRIVATE = "fixture-private-article-c5";
    static final String API = "blogger.googleapis.com";
    static final String OAUTH = "oauth2.googleapis.com";
    static final String REDIRECT = "blogger-fixture-redirect.example";
    static final String MISMATCH = "blogger-fixture-wrong.example";
    static final String SCOPE = "https://www.googleapis.com/auth/blogger.readonly";
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path directory;

    private BloggerPeer(Path directory) {
        this.directory = directory;
    }

    /**
     * Starts the fixture locally to the same container as the normal Zeta engine.
     *
     * @param args path to the scripted job fixtures and TLS identity store
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("jdk.tls.server.enableSessionTicketExtension", "false");
        Path root = Path.of(args[0]);
        BloggerPeer peer = new BloggerPeer(root);
        ExecutorService workers = Executors.newFixedThreadPool(6);
        AtomicReference<String> alias = new AtomicReference<>("trusted");
        HttpsServer server = peer.listen(workers, alias);
        try {
            Files.createFile(root.resolve("ready"));
            Path requested = root.resolve("certificate");
            while (!Files.exists(root.resolve("stop"))) {
                if (Files.isRegularFile(requested)) {
                    String selected = Files.readString(requested).trim();
                    if (!selected.isBlank() && !alias.get().equals(selected)) {
                        alias.set(selected);
                        invalidateSessions(server.getHttpsConfigurator().getSSLContext());
                        Files.writeString(root.resolve("certificate-applied"), selected);
                    }
                }
                Thread.sleep(50);
            }
        } finally {
            server.stop(0);
            workers.shutdownNow();
        }
    }

    private static void invalidateSessions(SSLContext context) {
        SSLSessionContext cache = context.getServerSessionContext();
        Enumeration<byte[]> ids = cache.getIds();
        while (ids.hasMoreElements()) {
            SSLSession session = cache.getSession(ids.nextElement());
            if (session != null) {
                session.invalidate();
            }
        }
    }

    private HttpsServer listen(ExecutorService workers, AtomicReference<String> alias)
            throws Exception {
        char[] password = PASSWORD.toCharArray();
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(directory.resolve("identities.p12"))) {
            keys.load(input, password);
        }
        KeyManagerFactory managers =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        managers.init(keys, password);
        X509ExtendedKeyManager provider = (X509ExtendedKeyManager) managers.getKeyManagers()[0];
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(new KeyManager[] {new SwitchingKeyManager(provider, alias)}, null, null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 443), 32);
        server.setHttpsConfigurator(new HttpsConfigurator(context));
        server.setExecutor(workers);
        server.createContext("/", this::handle);
        server.start();
        return server;
    }

    private void handle(HttpExchange exchange) throws IOException {
        String active = Files.readString(directory.resolve("active")).trim();
        try {
            JsonNode step = next(active, exchange);
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
     * Verifies and reserves a scripted reply before it waits or writes any response bytes.
     *
     * <p>Other requests may progress while a savepoint test holds its response.
     */
    private synchronized JsonNode next(String active, HttpExchange exchange) throws IOException {
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
        String target = exchange.getRequestURI().toString();
        String method = exchange.getRequestMethod();
        var headers = exchange.getRequestHeaders();
        String host = headers.getFirst("Host");
        exact(!step.isMissingNode(), "unexpected additional request");
        exact(step.path("host").asText().equals(host), "host or redirect mismatch");
        exact(step.path("target").asText().equals(target), "request target mismatch");
        exact("application/json".equals(headers.getFirst("Accept")), "missing JSON Accept");
        if (OAUTH.equals(host)) {
            exact("POST".equals(method) && "/token".equals(target), "invalid OAuth grant request");
            exact(headers.get("Authorization") == null, "bearer appeared on the OAuth endpoint");
            exact(
                    "application/x-www-form-urlencoded".equals(headers.getFirst("Content-Type")),
                    "OAuth request did not declare its form media type");
            Map<String, String> expected =
                    Map.of(
                            "grant_type",
                            "refresh_token",
                            "client_id",
                            CLIENT_ID,
                            "client_secret",
                            CLIENT_SECRET,
                            "refresh_token",
                            REFRESH);
            exact(
                    expected.equals(decodeForm(exchange.getRequestBody())),
                    "wrong OAuth refresh form");
        } else {
            exact(API.equals(host) && "GET".equals(method), "Blogger API request is not a read");
            exact(exchange.getRequestBody().read() < 0, "Blogger GET unexpectedly sent a body");
            String bearer = "Bearer " + step.path("bearer").asText();
            exact(
                    List.of(bearer).equals(headers.get("Authorization")),
                    "wrong Blogger bearer token");
            exact(headers.get("X-Goog-Api-Key") == null, "unexpected API key header");
        }
        ObjectNode line =
                JSON.createObjectNode()
                        .put("method", method)
                        .put("host", host)
                        .put("target", target)
                        .put("nanoTime", System.nanoTime());
        Files.writeString(
                record, line + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        return step;
    }

    private static Map<String, String> decodeForm(InputStream input) throws IOException {
        byte[] raw = input.readNBytes(25000);
        exact(raw.length < 25000, "unexpected OAuth form length");
        Map<String, String> values = new LinkedHashMap<>();
        for (String element : new String(raw, StandardCharsets.US_ASCII).split("&")) {
            int equal = element.indexOf('=');
            exact(equal > 0, "invalid OAuth form parameter");
            String key = URLDecoder.decode(element.substring(0, equal), StandardCharsets.UTF_8);
            String value = URLDecoder.decode(element.substring(equal + 1), StandardCharsets.UTF_8);
            exact(values.put(key, value) == null, "duplicate OAuth form parameter");
        }
        return values;
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
            headers.set("Location", "https://" + REDIRECT + "/v3/users/self/blogs");
        }
        try {
            if (step.has("delayMs")) {
                Thread.sleep(step.path("delayMs").asLong());
            }
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
            if ("hold".equals(mode)
                    || "trickle".equals(mode)
                    || "gzip".equals(mode)
                    || step.path("response").asText().length() >= 1024) {
                Files.writeString(directory.resolve(active + ".disconnect"), "closed");
            } else {
                throw e;
            }
        }
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

    /**
     * Selects the current fixture certificate without changing the production host or DNS.
     *
     * <p>Certificate and hostname failures can therefore be exercised independently on a real TLS
     * handshake without restarting the engine.
     */
    static final class SwitchingKeyManager extends X509ExtendedKeyManager {
        private final X509ExtendedKeyManager provider;
        private final AtomicReference<String> alias;

        SwitchingKeyManager(X509ExtendedKeyManager provider, AtomicReference<String> alias) {
            this.provider = provider;
            this.alias = alias;
        }

        @Override
        public String[] getClientAliases(String type, Principal[] issuers) {
            return provider.getClientAliases(type, issuers);
        }

        @Override
        public String chooseClientAlias(String[] types, Principal[] issuers, Socket socket) {
            return provider.chooseClientAlias(types, issuers, socket);
        }

        @Override
        public String[] getServerAliases(String type, Principal[] issuers) {
            return provider.getServerAliases(type, issuers);
        }

        @Override
        public String chooseServerAlias(String type, Principal[] issuers, Socket socket) {
            return choose(type, issuers);
        }

        @Override
        public String chooseEngineServerAlias(String type, Principal[] issuers, SSLEngine engine) {
            return choose(type, issuers);
        }

        private String choose(String type, Principal[] issuers) {
            String[] available = provider.getServerAliases(type, issuers);
            String requested = alias.get();
            if (available != null) {
                for (String candidate : available) {
                    if (requested.equals(candidate)) {
                        return candidate;
                    }
                }
            }
            return null;
        }

        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            return provider.getCertificateChain(alias);
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            return provider.getPrivateKey(alias);
        }
    }
}
