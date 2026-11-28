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

package org.apache.seatunnel.e2e.connector.acuityscheduling;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.Testcontainers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

/** Real HTTPS mock service that independently verifies Basic authentication and request scope. */
final class AcuityHttpsFixture implements AutoCloseable {
    static final String HOST = "host.testcontainers.internal";
    static final String PASSWORD = "fixture-only+:/=key";
    static final String BASIC_VALUE =
            Base64.getEncoder()
                    .encodeToString(("834101:" + PASSWORD).getBytes(StandardCharsets.US_ASCII));
    static final String STORE_PASSWORD = "acuity-fixture-certificate";
    static final String PRIVATE_ERROR = "acuity-fixture-service-detail";

    private final HttpsServer server;
    private final ExecutorService connections = Executors.newCachedThreadPool();
    private volatile Scenario selected;

    AcuityHttpsFixture(Path certificate) throws Exception {
        KeyManagerFactory km =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        km.init(store(certificate), STORE_PASSWORD.toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(km.getKeyManagers(), null, null);
        server = HttpsServer.create(new InetSocketAddress(0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(context));
        server.setExecutor(connections);
        server.createContext("/", this::handle);
        Testcontainers.exposeHostPorts(server.getAddress().getPort());
        server.start();
    }

    String origin() {
        return "https://" + authority();
    }

    Scenario select(Action action) {
        Scenario scenario = new Scenario(action);
        selected = scenario;
        return scenario;
    }

    private String authority() {
        return HOST + ":" + server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        Scenario scenario = selected;
        try {
            if (scenario == null) {
                respond(exchange, 404, "{}");
                return;
            }
            if (!"GET".equals(exchange.getRequestMethod())
                    || !authority().equals(exchange.getRequestHeaders().getFirst("Host"))
                    || !"gzip".equals(exchange.getRequestHeaders().getFirst("Accept-Encoding"))
                    || !("Basic " + BASIC_VALUE)
                            .equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                throw new IllegalStateException(
                        "Wrong method, authority, gzip advertisement or Basic credentials");
            }
            String query = exchange.getRequestURI().getRawQuery();
            String path = exchange.getRequestURI().getRawPath();
            String target = query == null ? path : path + "?" + query;
            Request request = new Request(target, System.nanoTime());
            scenario.action.respond(request, exchange, scenario.record(request));
        } catch (AssertionError | IllegalStateException e) {
            if (scenario != null) {
                scenario.failures.add(e.getMessage());
            }
            respond(exchange, 400, "{\"error\":\"fixture expectation failed\"}");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            exchange.close();
        }
    }

    static void respond(HttpExchange exchange, int code, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(code, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    static void gzip(HttpExchange exchange, String json) throws IOException {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        try (GZIPOutputStream encoded = new GZIPOutputStream(wire)) {
            encoded.write(json.getBytes(StandardCharsets.UTF_8));
        }
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Content-Encoding", "gzip");
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write(wire.toByteArray());
    }

    @Override
    public void close() throws InterruptedException {
        server.stop(0);
        connections.shutdownNow();
        Assertions.assertTrue(connections.awaitTermination(15, TimeUnit.SECONDS));
    }

    private static KeyStore store(Path path) throws Exception {
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

    /** A response script sees the verified target and exact request sequence. */
    @FunctionalInterface
    interface Action {
        void respond(Request request, HttpExchange exchange, int index)
                throws IOException, InterruptedException;
    }

    /** Timestamped path emitted after successful independent Basic verification. */
    static final class Request {
        final String target;
        final long arrival;

        Request(String target, long arrival) {
            this.target = target;
            this.arrival = arrival;
        }
    }

    /** Per-test script and request history, isolated across job attempts. */
    static final class Scenario {
        final Action action;
        final List<String> failures = new CopyOnWriteArrayList<>();
        private final List<Request> requests = new ArrayList<>();

        Scenario(Action action) {
            this.action = action;
        }

        synchronized int record(Request request) {
            requests.add(request);
            return requests.size() - 1;
        }

        synchronized List<Request> requests() {
            Assertions.assertTrue(failures.isEmpty(), failures.toString());
            return new ArrayList<>(requests);
        }
    }
}
