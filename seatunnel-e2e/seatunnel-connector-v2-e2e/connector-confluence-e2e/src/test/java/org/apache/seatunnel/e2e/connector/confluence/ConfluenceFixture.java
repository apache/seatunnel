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

package org.apache.seatunnel.e2e.connector.confluence;

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
import java.io.OutputStream;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

/**
 * Provides a local verified-TLS endpoint with scripted responses, exact request recording and held
 * I/O.
 */
final class ConfluenceFixture implements AutoCloseable {
    static final String HOST = "host.testcontainers.internal";
    static final String USERNAME = "fixture@example.test";
    static final String TOKEN = "fixture-confluence-test-token";
    static final String PASSWORD = "fixture-store-password";
    static final String AUTHORIZATION =
            "Basic "
                    + Base64.getEncoder()
                            .encodeToString(
                                    (USERNAME + ":" + TOKEN).getBytes(StandardCharsets.UTF_8));

    private final HttpsServer server;
    private final ExecutorService handlers = Executors.newCachedThreadPool();

    private volatile Scenario scenario;

    ConfluenceFixture(Path directory) throws Exception {
        Path keys = directory.resolve("key.jks");
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
                                keys.toString(),
                                "-storepass",
                                PASSWORD,
                                "-keypass",
                                PASSWORD,
                                "-noprompt")
                        .redirectErrorStream(true)
                        .redirectOutput(directory.resolve("keytool.log").toFile())
                        .start();
        Assertions.assertTrue(keytool.waitFor(30, TimeUnit.SECONDS), "fixture keytool timeout");
        Assertions.assertEquals(0, keytool.exitValue(), "fixture certificate generation");

        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (InputStream input = Files.newInputStream(keys)) {
            keyStore.load(input, PASSWORD.toCharArray());
        }
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, null);
        trust.setCertificateEntry("fixture", keyStore.getCertificate("fixture"));
        try (OutputStream output = Files.newOutputStream(directory.resolve("trust.jks"))) {
            trust.store(output, PASSWORD.toCharArray());
        }
        KeyManagerFactory manager =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(keyStore, PASSWORD.toCharArray());
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(manager.getKeyManagers(), null, null);
        server = HttpsServer.create(new InetSocketAddress(0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(ssl));
        server.setExecutor(handlers);
        server.createContext("/", this::dispatch);
        Testcontainers.exposeHostPorts(server.getAddress().getPort());
        server.start();
    }

    String origin() {
        return "https://" + HOST + ":" + server.getAddress().getPort();
    }

    void serve(Scenario plan) {
        scenario = plan;
    }

    private void dispatch(HttpExchange exchange) throws IOException {
        try {
            Scenario current = scenario;
            if (current == null) {
                status(exchange, 500);
                return;
            }
            int index = current.record(exchange);
            if (!"GET".equals(exchange.getRequestMethod())
                    || !AUTHORIZATION.equals(
                            exchange.getRequestHeaders().getFirst("Authorization"))) {
                current.problems.add("Invalid method or Authorization header");
                status(exchange, 401);
                return;
            }
            current.script.reply(index, exchange);
        } finally {
            exchange.close();
        }
    }

    @Override
    public void close() throws InterruptedException {
        server.stop(0);
        handlers.shutdownNow();
        Assertions.assertTrue(
                handlers.awaitTermination(10, TimeUnit.SECONDS), "fixture handlers closed");
    }

    static void status(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
    }

    static void json(HttpExchange exchange, String body) throws IOException {
        body(exchange, body.getBytes(StandardCharsets.UTF_8));
    }

    static void gzip(HttpExchange exchange, String body) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
            gzip.write(body.getBytes(StandardCharsets.UTF_8));
        }
        exchange.getResponseHeaders().set("Content-Encoding", "gzip");
        body(exchange, bytes.toByteArray());
    }

    private static void body(HttpExchange exchange, byte[] bytes) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    /**
     * Trickles a partial response until release, exposing socket disconnects for deadline/cancel
     * checks.
     */
    static final class Hold {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch disconnected = new CountDownLatch(1);

        void send(HttpExchange exchange, String json) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write('{');
                output.flush();
                entered.countDown();
                while (!release.await(150, TimeUnit.MILLISECONDS)) {
                    output.write(' ');
                    output.flush();
                }
                output.write(json.substring(1).getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                disconnected.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Supplies one response for the recorded request index in the active scenario.
     *
     * @see ConfluenceIT
     */
    @FunctionalInterface
    interface Script {
        void reply(int index, HttpExchange exchange) throws IOException;
    }

    /**
     * Records ordered requests and authentication problems shared with the engine assertion thread.
     */
    static final class Scenario {
        final Script script;
        final List<String> problems = new CopyOnWriteArrayList<>();
        final List<Long> times = new CopyOnWriteArrayList<>();
        private final List<String> requests = new ArrayList<>();

        Scenario(Script script) {
            this.script = script;
        }

        synchronized int record(HttpExchange exchange) {
            times.add(System.nanoTime());
            requests.add(exchange.getRequestURI().toASCIIString());
            return requests.size() - 1;
        }

        synchronized List<String> requests() {
            Assertions.assertTrue(problems.isEmpty(), problems.toString());
            return new ArrayList<>(requests);
        }
    }
}
