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

package org.apache.seatunnel.e2e.connector.onehundredms;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.Testcontainers;

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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

/**
 * Independent HTTPS API peer. Verifies credentials and records each inbound route. Each scenario
 * controls actual response bytes and timing without reusing the connector transport.
 */
final class OneHundredMsFixture implements AutoCloseable {
    static final String HOST = "host.testcontainers.internal";
    static final String TOKEN = "fixture-only-management-token";
    static final String KEY = "fixture-only-access-key";
    static final String SECRET = "fixture-only-signing-secret";
    static final String STORE_PASSWORD = "fixture-local-certificate";
    static final String PRIVATE_ERROR = "fixture-service-error-body";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpsServer server;
    private final ExecutorService handlers = Executors.newCachedThreadPool();
    private volatile Scenario current;

    OneHundredMsFixture(Path path) throws Exception {
        KeyStore keys = makeStore(path);
        KeyManagerFactory k =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        k.init(keys, STORE_PASSWORD.toCharArray());
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(k.getKeyManagers(), null, null);
        server = HttpsServer.create(new InetSocketAddress(0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));
        server.createContext("/", this::receive);
        server.setExecutor(handlers);
        Testcontainers.exposeHostPorts(server.getAddress().getPort());
        server.start();
    }

    String origin() {
        return "https://" + host();
    }

    Scenario set(boolean app, Handler action) {
        Scenario next = new Scenario(app, action);
        current = next;
        return next;
    }

    static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    static void compressed(HttpExchange exchange, String body) throws IOException {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(encoded)) {
            gzip.write(body.getBytes(StandardCharsets.UTF_8));
        }
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Content-Encoding", "gzip");
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write(encoded.toByteArray());
    }

    @Override
    public void close() throws InterruptedException {
        server.stop(0);
        handlers.shutdownNow();
        Assertions.assertTrue(handlers.awaitTermination(15, TimeUnit.SECONDS));
    }

    private String host() {
        return HOST + ":" + server.getAddress().getPort();
    }

    private void receive(HttpExchange exchange) throws IOException {
        Scenario scenario = current;
        try {
            if (scenario == null) {
                respond(exchange, 404, "{}");
                return;
            }
            if (!"GET".equals(exchange.getRequestMethod())
                    || !host().equals(exchange.getRequestHeaders().getFirst("Host"))
                    || !"gzip".equals(exchange.getRequestHeaders().getFirst("Accept-Encoding"))) {
                throw new IllegalStateException("Unexpected method, authority or content encoding");
            }
            String bearer = exchange.getRequestHeaders().getFirst("Authorization");
            String jti = authenticate(scenario.app, bearer);
            String uri = exchange.getRequestURI().getRawPath();
            String query = exchange.getRequestURI().getRawQuery();
            String target = query == null ? uri : uri + "?" + query;
            Request incoming = new Request(target, jti, System.nanoTime());
            int index = scenario.record(incoming);
            scenario.action.handle(incoming, exchange, index);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IllegalStateException | AssertionError e) {
            if (scenario != null) {
                scenario.problems.add(e.getMessage());
            }
            respond(exchange, 400, "{\"error\":\"unexpected fixture request\"}");
        } finally {
            exchange.close();
        }
    }

    private static String authenticate(boolean app, String auth) {
        if (!app) {
            if (!("Bearer " + TOKEN).equals(auth)) {
                throw new IllegalStateException("Wrong configured management token");
            }
            return null;
        }
        if (auth == null || !auth.startsWith("Bearer ")) {
            throw new IllegalStateException("Missing JWT header");
        }
        try {
            String[] parts = auth.substring(7).split("\\.", -1);
            if (parts.length != 3) {
                throw new IllegalStateException("Malformed management JWT");
            }
            Base64.Decoder decode = Base64.getUrlDecoder();
            JsonNode header = JSON.readTree(decode.decode(parts[0]));
            JsonNode claims = JSON.readTree(decode.decode(parts[1]));
            long iat = claims.path("iat").asLong();
            long current = Instant.now().getEpochSecond();
            String id = claims.path("jti").asText();
            UUID.fromString(id);
            if (!"HS256".equals(header.path("alg").asText())
                    || !"JWT".equals(header.path("typ").asText())
                    || !KEY.equals(claims.path("access_key").asText())
                    || !"management".equals(claims.path("type").asText())
                    || claims.path("version").asInt() != 2
                    || claims.path("nbf").asLong() != iat
                    || claims.path("exp").asLong() != iat + 86400
                    || Math.abs(current - iat) > 10) {
                throw new IllegalStateException("Wrong management claims");
            }
            Mac verifier = Mac.getInstance("HmacSHA256");
            verifier.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] signature =
                    verifier.doFinal(
                            (parts[0] + "." + parts[1]).getBytes(StandardCharsets.US_ASCII));
            if (!MessageDigest.isEqual(signature, decode.decode(parts[2]))) {
                throw new IllegalStateException("Wrong management signature");
            }
            return id;
        } catch (Exception e) {
            throw new IllegalStateException("Independent management JWT verification failed");
        }
    }

    private static KeyStore makeStore(Path path) throws Exception {
        Process keytool =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-alias",
                                "fixture",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-dname",
                                "CN=" + HOST,
                                "-ext",
                                "SAN=dns:" + HOST,
                                "-validity",
                                "2",
                                "-storetype",
                                "JKS",
                                "-keystore",
                                path.toString(),
                                "-storepass",
                                STORE_PASSWORD,
                                "-keypass",
                                STORE_PASSWORD,
                                "-noprompt")
                        .redirectErrorStream(true)
                        .redirectOutput(path.resolveSibling(path.getFileName() + ".log").toFile())
                        .start();
        if (!keytool.waitFor(30, TimeUnit.SECONDS)) {
            keytool.destroyForcibly();
            throw new IllegalStateException("Fixture keytool did not finish");
        }
        Assertions.assertEquals(0, keytool.exitValue());
        KeyStore store = KeyStore.getInstance("JKS");
        try (InputStream input = Files.newInputStream(path)) {
            store.load(input, STORE_PASSWORD.toCharArray());
        }
        return store;
    }

    /**
     * Handles one verified route with independently chosen response timing and payload. Exceptions
     * are recorded by the fixture and returned as a private error response to the caller.
     */
    @FunctionalInterface
    interface Handler {
        void handle(Request request, HttpExchange exchange, int index)
                throws IOException, InterruptedException;
    }

    /**
     * Security-relevant request observations used by the tests. Captured routes and token IDs
     * establish encoding, retry and credential reuse without storing secrets.
     */
    static final class Request {
        final String target;
        final String jti;
        final long arrival;

        Request(String target, String jti, long arrival) {
            this.target = target;
            this.jti = jti;
            this.arrival = arrival;
        }
    }

    /**
     * Per-job route history and fixture assertions. Concurrent handlers record requests while test
     * threads obtain detached history and surface fixture failures.
     */
    static final class Scenario {
        final boolean app;
        final Handler action;
        final List<String> problems = new CopyOnWriteArrayList<>();
        private final List<Request> requests = new ArrayList<>();

        Scenario(boolean app, Handler action) {
            this.app = app;
            this.action = action;
        }

        synchronized int record(Request request) {
            requests.add(request);
            return requests.size() - 1;
        }

        synchronized List<Request> requests() {
            Assertions.assertTrue(problems.isEmpty(), problems.toString());
            return new ArrayList<>(requests);
        }
    }
}
