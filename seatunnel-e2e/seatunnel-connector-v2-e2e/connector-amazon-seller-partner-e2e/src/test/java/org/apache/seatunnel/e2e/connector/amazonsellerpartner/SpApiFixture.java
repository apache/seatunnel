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

package org.apache.seatunnel.e2e.connector.amazonsellerpartner;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
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
import java.security.KeyStore;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

/**
 * Scripted Amazon SP-API HTTPS service for real SeaTunnel integration jobs.
 *
 * <p>The fixture runs independently on JDK 11. Its control listener exposes request observations
 * without exposing the scripted credentials to the connector diagnostics.
 */
public final class SpApiFixture {
    static final String HOST = "sp-api-fixture";
    static final int TLS_PORT = 8444;
    static final int CONTROL_PORT = 8080;
    static final String ORIGIN = "https://" + HOST + ":" + TLS_PORT;
    static final String SECRET = "fixture-client-secret";
    static final String REFRESH = "fixture-refresh-value";
    static final String CLIENT = "fixture-client-id";
    static final String KEYSTORE_PASSWORD = "sp-fixture-pass";
    static final String PRIVATE_ERROR = "sensitive-sp-provider-body";

    private static final ObjectMapper JSON = new ObjectMapper();
    private final Object monitor = new Object();
    private ObjectNode program = JSON.createObjectNode();
    private final ArrayNode trace = JSON.createArrayNode();
    private int index;
    private int lwaCalls;
    private int rdtCalls;

    /**
     * Runs the simulated token, API, document, and control paths.
     *
     * <p>The service uses a test-generated PKCS12 identity and binds no external account.
     */
    public static void main(String[] args) throws Exception {
        if (Runtime.version().feature() != 11) {
            throw new IllegalStateException("SP-API fixture requires JDK 11");
        }
        SpApiFixture fixture = new SpApiFixture();
        char[] password = KEYSTORE_PASSWORD.toCharArray();
        KeyStore store = KeyStore.getInstance("PKCS12");
        KeyManagerFactory keys =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        try (InputStream stream = Files.newInputStream(Path.of(args[0]))) {
            store.load(stream, password);
            keys.init(store, password);
        } finally {
            Arrays.fill(password, '\0');
        }
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(keys.getKeyManagers(), null, null);

        HttpsServer service = HttpsServer.create(new InetSocketAddress(TLS_PORT), 16);
        service.setHttpsConfigurator(new HttpsConfigurator(tls));
        service.setExecutor(Executors.newFixedThreadPool(8));
        service.createContext("/", fixture::provider);
        service.start();

        HttpServer control = HttpServer.create(new InetSocketAddress(CONTROL_PORT), 8);
        control.setExecutor(Executors.newFixedThreadPool(2));
        control.createContext("/", fixture::control);
        control.start();
    }

    private void control(HttpExchange exchange) throws IOException {
        try {
            String route = exchange.getRequestURI().getPath();
            if ("/trace".equals(route)) {
                ArrayNode snapshot;
                synchronized (monitor) {
                    snapshot = trace.deepCopy();
                }
                json(exchange, 200, snapshot);
            } else if ("/setup".equals(route)) {
                ObjectNode received =
                        (ObjectNode) JSON.readTree(exchange.getRequestBody().readNBytes(4000001));
                synchronized (monitor) {
                    program = received;
                    trace.removeAll();
                    index = 0;
                    lwaCalls = 0;
                    rdtCalls = 0;
                }
                json(exchange, 200, JSON.createObjectNode().put("ready", true));
            } else if ("/release".equals(route)) {
                synchronized (monitor) {
                    program.put("released", true);
                }
                json(exchange, 200, JSON.createObjectNode().put("ready", true));
            } else if ("/ready".equals(route)) {
                json(exchange, 200, JSON.createObjectNode().put("ready", true));
            } else {
                json(exchange, 404, JSON.createObjectNode());
            }
        } finally {
            exchange.close();
        }
    }

    private void provider(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            String token = exchange.getRequestHeaders().getFirst("x-amz-access-token");
            String auth =
                    token == null
                            ? "none"
                            : token.startsWith("fixture-lwa-")
                                    ? "lwa"
                                    : token.startsWith("fixture-rdt-") ? "rdt" : "invalid";
            String body =
                    new String(
                            exchange.getRequestBody().readNBytes(1048577), StandardCharsets.UTF_8);
            ObjectNode observation =
                    JSON.createObjectNode()
                            .put("path", path)
                            .put("method", method)
                            .put("auth", auth)
                            .put(
                                    "signed",
                                    exchange.getRequestHeaders().containsKey("Authorization"))
                            .put("host", exchange.getRequestHeaders().getFirst("Host"))
                            .put("amz_date", exchange.getRequestHeaders().containsKey("x-amz-date"))
                            .put("fixture_ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
            observation.set("query", form(exchange.getRequestURI().getRawQuery()));
            boolean lwa = "/auth/o2/token".equals(path);
            boolean rdt = "/tokens/2021-03-01/restrictedDataToken".equals(path);
            JsonNode step;
            int issued;
            synchronized (monitor) {
                if (lwa) {
                    ObjectNode parameters = form(body);
                    observation.put(
                            "form_ok",
                            "refresh_token".equals(parameters.path("grant_type").asText())
                                    && CLIENT.equals(parameters.path("client_id").asText())
                                    && SECRET.equals(parameters.path("client_secret").asText())
                                    && REFRESH.equals(parameters.path("refresh_token").asText()));
                    issued = ++lwaCalls;
                    step = program.path("lwa");
                } else if (rdt) {
                    observation.set("body", JSON.readTree(body));
                    issued = ++rdtCalls;
                    step = program.path("rdt");
                } else {
                    if (!body.isEmpty()) {
                        observation.set("body", JSON.readTree(body));
                    }
                    issued = 0;
                    step = program.path("steps").path(index++);
                }
                trace.add(observation);
            }
            if (lwa || rdt) {
                if (lwa && !observation.path("form_ok").asBoolean() || rdt && !"lwa".equals(auth)) {
                    rejected(exchange, 401);
                } else if (step.has("status")) {
                    rejected(exchange, step.path("status").asInt());
                } else {
                    json(
                            exchange,
                            200,
                            JSON.createObjectNode()
                                    .put(
                                            lwa ? "access_token" : "restrictedDataToken",
                                            (lwa ? "fixture-lwa-" : "fixture-rdt-") + issued)
                                    .put(
                                            lwa ? "expires_in" : "expiresIn",
                                            step.path("expires").asInt(3600)));
                }
            } else if (!method.equals(step.path("method").asText("GET"))
                    || !path.equals(step.path("path").asText())
                    || !auth.equals(step.path("auth").asText("lwa"))) {
                rejected(exchange, 422);
            } else {
                respond(exchange, step, observation);
            }
        } finally {
            exchange.close();
        }
    }

    private void respond(HttpExchange exchange, JsonNode step, ObjectNode observation)
            throws IOException {
        step.path("headers")
                .fields()
                .forEachRemaining(
                        header ->
                                exchange.getResponseHeaders()
                                        .set(header.getKey(), header.getValue().asText()));
        int status = step.path("status").asInt(200);
        int pause = step.path("pause_ms").asInt(0);
        if (pause > 0) {
            try {
                Thread.sleep(Math.min(pause, 1500));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        if (step.path("hold").asBoolean()) {
            held(exchange, observation, step.path("trickle").asBoolean());
            return;
        }
        if (step.has("raw")) {
            byte[] bytes = step.path("raw").asText().getBytes(StandardCharsets.UTF_8);
            if (step.path("gzip").asBoolean()) {
                ByteArrayOutputStream zip = new ByteArrayOutputStream();
                try (GZIPOutputStream stream = new GZIPOutputStream(zip)) {
                    stream.write(bytes);
                }
                bytes = zip.toByteArray();
            }
            if (!exchange.getResponseHeaders().containsKey("Content-Type")) {
                exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
            }
            send(exchange, status, bytes);
        } else if (step.has("json")) {
            json(exchange, status, step.path("json"));
        } else {
            rejected(exchange, status);
        }
    }

    private void held(HttpExchange exchange, ObjectNode observation, boolean trickle)
            throws IOException {
        long start = System.nanoTime();
        synchronized (monitor) {
            observation.put("held", true);
        }
        exchange.sendResponseHeaders(200, 1_000_000);
        try (OutputStream out = exchange.getResponseBody()) {
            if (!trickle) {
                Thread.sleep(2100L);
            }
            for (int i = 0; i < 2400; i++) {
                out.write(' ');
                out.flush();
                Thread.sleep(50L);
            }
        } catch (IOException e) {
            synchronized (monitor) {
                observation.put(
                        "disconnect_ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ObjectNode form(String input) {
        ObjectNode result = JSON.createObjectNode();
        if (input != null && !input.isEmpty()) {
            for (String param : input.split("&")) {
                String[] parts = param.split("=", 2);
                if (parts.length == 2) {
                    result.put(
                            URLDecoder.decode(parts[0], StandardCharsets.UTF_8),
                            URLDecoder.decode(parts[1], StandardCharsets.UTF_8));
                }
            }
        }
        return result;
    }

    private static void rejected(HttpExchange exchange, int code) throws IOException {
        ObjectNode message = JSON.createObjectNode();
        message.putArray("errors")
                .addObject()
                .put("message", PRIVATE_ERROR)
                .put("code", "FixtureError");
        json(exchange, code, message);
    }

    private static void json(HttpExchange exchange, int code, JsonNode document)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        send(exchange, code, JSON.writeValueAsBytes(document));
    }

    private static void send(HttpExchange exchange, int code, byte[] bytes) throws IOException {
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }
}
