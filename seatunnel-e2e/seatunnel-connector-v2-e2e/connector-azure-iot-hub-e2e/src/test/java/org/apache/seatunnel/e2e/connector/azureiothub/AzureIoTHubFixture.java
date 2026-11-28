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

package org.apache.seatunnel.e2e.connector.azureiothub;

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
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Independent HTTPS peer that verifies scope/HMAC, product property syntax and exact payloads. File
 * receipts observe accepted attempts; the fixture does not claim live-service acceptance.
 */
final class AzureIoTHubFixture implements AutoCloseable {
    static final String HOST = "host.testcontainers.internal";
    static final String ENCODED_PATH = "/devices/Sensor%3F%25%3A/messages/events";
    static final String PUBLIC_TEST_KEY = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
    static final String STORE_PASSWORD = "iot-fixture-certificate";

    private final HttpsServer server;
    private final ExecutorService threads = Executors.newCachedThreadPool();
    // Each sequential test publishes its own script and receipt window to HTTPS callbacks.
    private volatile Scenario scenario;

    AzureIoTHubFixture(KeyStore keys) throws Exception {
        KeyManagerFactory managers =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        managers.init(keys, STORE_PASSWORD.toCharArray());
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(managers.getKeyManagers(), null, null);
        server = HttpsServer.create(new InetSocketAddress(0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(ssl));
        server.createContext("/", this::dispatch);
        server.setExecutor(threads);
        Testcontainers.exposeHostPorts(server.getAddress().getPort());
        server.start();
    }

    String origin() {
        return "https://" + hostHeader();
    }

    @Override
    public void close() throws InterruptedException {
        server.stop(0);
        threads.shutdownNow();
        Assertions.assertTrue(threads.awaitTermination(15, TimeUnit.SECONDS));
    }

    void serve(Scenario script) {
        scenario = script;
    }

    static void reply(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
    }

    private String hostHeader() {
        return HOST + ":" + server.getAddress().getPort();
    }

    private void dispatch(HttpExchange exchange) throws IOException {
        try {
            Scenario current = scenario;
            if (current == null) {
                reply(exchange, 404);
                return;
            }
            long arrival = System.nanoTime();
            current.started.countDown();
            long expiry;
            try {
                expiry = verify(exchange);
            } catch (Exception e) {
                current.problems.add(
                        "Request did not match the device endpoint, properties or SAS");
                reply(exchange, 401);
                return;
            }
            byte[] bytes = readBody(exchange.getRequestBody());
            Attempt attempt =
                    new Attempt(
                            arrival,
                            expiry,
                            bytes,
                            exchange.getRequestHeaders().getFirst("iothub-contenttype"),
                            exchange.getRequestHeaders().getFirst("iothub-contentencoding"),
                            exchange.getRequestHeaders().getFirst("Content-Type"));
            int index = current.add(attempt);
            current.script.respond(current, index, attempt, exchange);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            exchange.close();
        }
    }

    private long verify(HttpExchange exchange) throws Exception {
        if (!"POST".equals(exchange.getRequestMethod())
                || !ENCODED_PATH.equals(exchange.getRequestURI().getRawPath())
                || !"api-version=2021-04-12".equals(exchange.getRequestURI().getRawQuery())
                || !hostHeader().equals(exchange.getRequestHeaders().getFirst("Host"))
                || !"sensor".equals(exchange.getRequestHeaders().getFirst("iothub-app-route"))) {
            throw new IllegalArgumentException("Unexpected request metadata");
        }
        // Verify the documented HTTPS property alphabet independently of production validation.
        for (Map.Entry<String, List<String>> header : exchange.getRequestHeaders().entrySet()) {
            if (header.getKey().toLowerCase(Locale.ROOT).startsWith("iothub-app-")) {
                for (String value : header.getValue()) {
                    if (!value.matches("[A-Za-z0-9!#$%&'*+.^_`|~-]*")) {
                        throw new IllegalArgumentException("Unsupported HTTPS property value");
                    }
                }
            }
        }
        String auth = exchange.getRequestHeaders().getFirst("Authorization");
        String prefix = "SharedAccessSignature ";
        if (auth == null || !auth.startsWith(prefix)) {
            throw new IllegalArgumentException("Missing signature");
        }
        Map<String, String> fields = new HashMap<>();
        for (String component : auth.substring(prefix.length()).split("&")) {
            String[] pair = component.split("=", 2);
            if (pair.length != 2 || fields.put(pair[0], pair[1]) != null) {
                throw new IllegalArgumentException("Ambiguous signature");
            }
        }
        if (fields.size() != 3
                || !fields.containsKey("sig")
                || !fields.containsKey("se")
                || !fields.containsKey("sr")) {
            throw new IllegalArgumentException("Unexpected signature fields");
        }
        String scope = URLDecoder.decode(fields.get("sr"), "UTF-8");
        long expiry = Long.parseLong(fields.get("se"));
        long ttl = expiry - Instant.now().getEpochSecond();
        if (!(HOST + "/devices/Sensor?%:").equals(scope) || ttl < 50 || ttl > 61) {
            throw new IllegalArgumentException("Unexpected scope or expiry");
        }
        byte[] key = "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.US_ASCII);
        Mac verifier = Mac.getInstance("HmacSHA256");
        verifier.init(new SecretKeySpec(key, "HmacSHA256"));
        verifier.update(fields.get("sr").getBytes(StandardCharsets.UTF_8));
        verifier.update((byte) '\n');
        byte[] wanted = verifier.doFinal(fields.get("se").getBytes(StandardCharsets.US_ASCII));
        byte[] supplied = Base64.getDecoder().decode(URLDecoder.decode(fields.get("sig"), "UTF-8"));
        if (!MessageDigest.isEqual(wanted, supplied)) {
            throw new IllegalArgumentException("Invalid device signature");
        }
        return expiry;
    }

    private static byte[] readBody(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            if (read > 262144 - output.size()) {
                throw new IOException("Fixture rejected an oversize request");
            }
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    /**
     * Controls fault responses only after independent authentication and bounded request
     * validation. The test decides acceptance separately from whether an HTTP acknowledgement is
     * returned.
     */
    @FunctionalInterface
    interface Script {
        void respond(Scenario scenario, int index, Attempt attempt, HttpExchange exchange)
                throws IOException, InterruptedException;
    }

    /**
     * Records one independently validated request and its observed wire metadata. Receipt
     * acceptance is separate because an acknowledgement can be lost after the service stores the
     * event.
     */
    static final class Attempt {
        final long arrival;
        final long expiry;
        final byte[] body;
        final String type;
        final String encoding;
        final String contentType;

        Attempt(
                long arrival,
                long expiry,
                byte[] body,
                String type,
                String encoding,
                String contentType) {
            this.arrival = arrival;
            this.expiry = expiry;
            this.body = body;
            this.type = type;
            this.encoding = encoding;
            this.contentType = contentType;
        }
    }

    /**
     * Owns synchronized request and receipt observations for one isolated test. Script failures
     * remain visible to test assertions rather than being mistaken for a successful endpoint
     * response.
     */
    static final class Scenario {
        final Script script;
        final Path receipts;
        final CountDownLatch started = new CountDownLatch(1);
        final List<String> problems = new CopyOnWriteArrayList<>();
        private final List<Attempt> attempts = new ArrayList<>();

        Scenario(Path receipts, Script script) {
            this.receipts = receipts;
            this.script = script;
        }

        synchronized int add(Attempt attempt) {
            attempts.add(attempt);
            return attempts.size() - 1;
        }

        synchronized List<Attempt> attempts() {
            Assertions.assertTrue(problems.isEmpty(), problems.toString());
            return new ArrayList<>(attempts);
        }

        synchronized void persist(Attempt attempt) throws IOException {
            Files.write(
                    receipts,
                    (Base64.getEncoder().encodeToString(attempt.body) + "\n")
                            .getBytes(StandardCharsets.US_ASCII),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        }

        synchronized List<byte[]> receipts() throws IOException {
            Assertions.assertTrue(problems.isEmpty(), problems.toString());
            List<byte[]> accepted = new ArrayList<>();
            if (Files.exists(receipts)) {
                for (String line : Files.readAllLines(receipts, StandardCharsets.US_ASCII)) {
                    accepted.add(Base64.getDecoder().decode(line));
                }
            }
            return accepted;
        }
    }
}
