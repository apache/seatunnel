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

package org.apache.seatunnel.e2e.connector.bunnyinc;

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
import java.util.Arrays;
import java.util.Base64;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

/**
 * Independent scripted Java HTTPS peer for BunnyInc list requests.
 *
 * <p>It validates fixed read-only GraphQL operations and never records credentials.
 */
public final class BunnyIncPeer {
    static final String PASSWORD = "bunnyinc-fixture-keystore";
    static final String KEY = "fixture-bunnyinc-private-key-418b";
    static final String SECOND_KEY = "fixture-bunnyinc-private-key-98ab";
    static final String PRIVATE = "fixture-private-contact-note";
    static final String HOST = "fixture-east.bunny.com";
    static final String SECOND = "fixture-west.bunny.com";
    static final String REDIRECT = "bunnyinc-fixture-redirect.example";
    static final String MISMATCH = "bunnyinc-fixture-wrong.example";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern READ_OPERATION =
            Pattern.compile(
                    "^query SeaTunnelRead\\s*\\(\\$first:\\s*Int!,\\s*\\$after:\\s*String\\)\\s*"
                            + "\\{\\s*(\\w+)\\s*\\(first:\\s*\\$first,\\s*after:\\s*\\$after\\)\\s*"
                            + "\\{\\s*nodes\\s*\\{([^{}]+)\\}\\s*pageInfo\\s*\\{([^{}]+)\\}\\s*\\}\\s*\\}$");

    private final Path directory;

    private BunnyIncPeer(Path directory) {
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
        BunnyIncPeer peer = new BunnyIncPeer(root);
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
        exact(!step.isMissingNode(), "unexpected additional request");
        exact("POST".equals(exchange.getRequestMethod()), "expected HTTP POST");
        exact("/graphql".equals(exchange.getRequestURI().toString()), "unexpected billing path");
        var headers = exchange.getRequestHeaders();
        exact(step.path("host").asText().equals(headers.getFirst("Host")), "request host mismatch");
        exact(
                List.of("Bearer " + step.path("key").asText(KEY))
                        .equals(headers.get("Authorization")),
                "incorrect tenant Bearer key");
        exact("application/json".equals(headers.getFirst("Content-Type")), "expected JSON request");
        byte[] bytes = exchange.getRequestBody().readNBytes(65537);
        exact(bytes.length <= 65536, "query request exceeded the fixture budget");
        JsonNode request;
        try {
            request = JSON.readTree(bytes);
        } catch (IOException error) {
            throw new IllegalArgumentException("invalid query JSON");
        }
        exact(request != null && request.isObject(), "expected GraphQL request object");
        exact(
                "SeaTunnelRead".equals(request.path("operationName").asText()),
                "wrong operation name");
        JsonNode variables = request.path("variables");
        exact(variables.isObject() && variables.size() == 2, "variables must be a JSON object");
        JsonNode first = variables.path("first");
        exact(
                first.isInt() && first.intValue() == step.path("first").intValue(),
                "wrong page size");
        exact(variables.path("after").equals(step.path("after")), "wrong opaque cursor");
        Matcher query = READ_OPERATION.matcher(request.path("query").asText());
        exact(query.matches(), "unexpected GraphQL query structure");
        String field = step.path("field").asText();
        exact(field.equals(query.group(1)), "requested the wrong GraphQL connection");
        List<String> expectedFields = Arrays.asList(step.path("fields").asText().split(" "));
        List<String> actualFields =
                Arrays.asList(query.group(2).trim().replaceAll("\\s*:\\s*", ":").split("\\s+"));
        exact(
                expectedFields.size() == actualFields.size()
                        && new HashSet<>(expectedFields).equals(new HashSet<>(actualFields)),
                "GraphQL node field projection mismatch");
        exact(
                List.of("startCursor", "endCursor", "hasNextPage", "hasPreviousPage")
                        .equals(Arrays.asList(query.group(3).trim().split("\\s+"))),
                "GraphQL pageInfo projection mismatch");
        ObjectNode entry =
                JSON.createObjectNode()
                        .put("field", field)
                        .put("first", first.intValue())
                        .put("nanoTime", System.nanoTime());
        entry.set("after", variables.get("after"));
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
            headers.set("Location", "https://" + REDIRECT + "/graphql");
        }
        try {
            if ("hold".equals(mode)) {
                waitForRelease(active);
            }
            byte[] body =
                    step.has("bodyBase64")
                            ? Base64.getDecoder().decode(step.path("bodyBase64").asText())
                            : step.path("response").asText().getBytes(StandardCharsets.UTF_8);
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
