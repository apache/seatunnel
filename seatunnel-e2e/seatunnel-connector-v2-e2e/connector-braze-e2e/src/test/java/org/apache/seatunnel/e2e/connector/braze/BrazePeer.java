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

package org.apache.seatunnel.e2e.connector.braze;

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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

/**
 * Independent scripted Java HTTPS peer for Braze list requests.
 *
 * <p>It accepts only expected GET requests and never records credentials.
 */
public final class BrazePeer {
    static final String PASSWORD = "braze-fixture-keystore";
    static final String KEY = "fixture-braze-private-key-418b";
    static final String PRIVATE = "fixture-private-contact-note";
    static final String HOST = "rest.iad-01.braze.com";
    static final String EU = "rest.fra-01.braze.eu";
    static final String JP = "rest.jp-01.braze.com";
    static final String AU = "rest.au-01.braze.com";
    static final String REDIRECT = "braze-fixture-redirect.example";
    static final String MISMATCH = "braze-fixture-wrong.example";
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path directory;

    private BrazePeer(Path directory) {
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
        BrazePeer peer = new BrazePeer(root);
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
            exact(exchange.getRequestBody().read() < 0, "GET unexpectedly carried a body");
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
        var headers = exchange.getRequestHeaders();
        exact(
                "GET".equals(exchange.getRequestMethod()),
                "provider operation was not read-only GET");
        exact(
                step.path("host").asText().equals(headers.getFirst("Host")),
                "host mismatch or redirected request");
        String key = step.path("key").asText(KEY);
        exact(
                List.of("Bearer " + key).equals(headers.get("Authorization")),
                "missing or incorrect Bearer API key");
        exact(headers.get("X-API-Key") == null, "unexpected X-API-Key header");
        exact("application/json".equals(headers.getFirst("Accept")), "missing JSON Accept");
        exact(!step.isMissingNode(), "unexpected additional request");
        exact(step.path("target").asText().equals(target), "request target mismatch");
        ObjectNode entry =
                JSON.createObjectNode()
                        .put("method", "GET")
                        .put("target", target)
                        .put("nanoTime", System.nanoTime());
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
        if (step.has("brazeRetryAfter")) {
            headers.set("X-RateLimit-Retry-After", step.path("brazeRetryAfter").asText());
        }
        if (step.has("brazeResetIn")) {
            long reset = Instant.now().getEpochSecond() + step.path("brazeResetIn").asLong();
            headers.set("X-RateLimit-Reset", Long.toString(reset));
        }
        if (status / 100 == 3) {
            headers.set("Location", "https://" + REDIRECT + "/campaigns/list");
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
