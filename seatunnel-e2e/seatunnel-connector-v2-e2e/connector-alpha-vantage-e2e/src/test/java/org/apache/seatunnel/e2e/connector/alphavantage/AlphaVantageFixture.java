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

package org.apache.seatunnel.e2e.connector.alphavantage;

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
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * Serves independent Alpha Vantage responses in a separate JDK11 process next to the Zeta worker.
 */
public final class AlphaVantageFixture {
    static final String HOST = "www.alphavantage.co";
    static final String TOKEN = "vantage-fixture-key%&+";
    static final String SYMBOL = "TSCO.LON+R&X";
    static final String PASSWORD = "vantage-fixture-trust";
    static final int PORT = 18443;
    static final int UNTRUSTED_PORT = 18444;
    private final Path root;
    private final Map<String, AtomicInteger> hits = new ConcurrentHashMap<>();

    private AlphaVantageFixture(Path root) {
        this.root = root;
    }

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(args[0]);
        AlphaVantageFixture fixture = new AlphaVantageFixture(root);
        ExecutorService workers = Executors.newCachedThreadPool();
        HttpsServer trusted = fixture.listen(PORT, root.resolve("trusted.p12"), workers);
        try {
            HttpsServer untrusted =
                    fixture.listen(UNTRUSTED_PORT, root.resolve("untrusted.p12"), workers);
            try {
                Files.createFile(root.resolve("ready"));
                long expires = System.nanoTime() + TimeUnit.MINUTES.toNanos(40);
                while (System.nanoTime() - expires < 0 && !Files.exists(root.resolve("stop"))) {
                    Thread.sleep(100);
                }
            } finally {
                untrusted.stop(0);
            }
        } finally {
            trusted.stop(0);
            workers.shutdownNow();
        }
    }

    private HttpsServer listen(int port, Path keyFile, ExecutorService workers) throws Exception {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream data = Files.newInputStream(keyFile)) {
            keys.load(data, PASSWORD.toCharArray());
        }
        KeyManagerFactory manager =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(keys, PASSWORD.toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(manager.getKeyManagers(), null, null);
        HttpsServer service = HttpsServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        service.setExecutor(workers);
        service.setHttpsConfigurator(new HttpsConfigurator(context));
        service.createContext("/", this::serve);
        service.start();
        return service;
    }

    private void serve(HttpExchange request) throws IOException {
        List<String> program =
                Files.readAllLines(root.resolve("active.tsv"), StandardCharsets.UTF_8);
        String scenario = program.get(0);
        try {
            Map<String, String> parameters = query(request.getRequestURI().getRawQuery());
            String function = parameters.get("function");
            List<Step> actions = new ArrayList<>();
            for (int i = 1; i < program.size(); i++) {
                Step action = new Step(program.get(i));
                if (action.function.equals(function)) {
                    actions.add(action);
                }
            }
            if (actions.isEmpty()) {
                invalid(request, scenario);
                return;
            }
            int step =
                    hits.computeIfAbsent(scenario + "/" + function, key -> new AtomicInteger())
                            .getAndIncrement();
            Step action = actions.get(Math.min(step, actions.size() - 1));
            if (!valid(request, parameters, action)) {
                invalid(request, scenario);
                return;
            }
            journal(
                    scenario,
                    ".attempts",
                    function
                            + "\t"
                            + System.currentTimeMillis()
                            + "\t"
                            + request.getLocalAddress().getPort());
            send(request, scenario, action);
        } catch (IllegalArgumentException e) {
            invalid(request, scenario);
        } finally {
            request.close();
        }
    }

    private static boolean valid(HttpExchange request, Map<String, String> parameters, Step step) {
        Map<String, String> expected = new HashMap<>();
        expected.put("apikey", TOKEN);
        expected.put("symbol", SYMBOL);
        expected.put("function", step.function);
        expected.put("datatype", "json");
        addIfPresent(expected, "outputsize", step.size);
        addIfPresent(expected, "interval", step.interval);
        addIfPresent(expected, "adjusted", step.adjusted);
        int port = request.getLocalAddress().getPort();
        String query = request.getRequestURI().getRawQuery();
        return "/query".equals(request.getRequestURI().getRawPath())
                && "GET".equals(request.getRequestMethod())
                && (HOST + ":" + port).equals(request.getRequestHeaders().getFirst("Host"))
                && expected.equals(parameters)
                && query.contains("symbol=TSCO.LON%2BR%26X")
                && query.contains("apikey=vantage-fixture-key%25%26%2B");
    }

    private static void addIfPresent(Map<String, String> parameters, String key, String value) {
        if (!value.isEmpty()) {
            parameters.put(key, value);
        }
    }

    private static Map<String, String> query(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("missing query");
        }
        Map<String, String> result = new HashMap<>();
        for (String field : raw.split("&", -1)) {
            int eq = field.indexOf('=');
            if (eq < 1
                    || result.put(
                                    URLDecoder.decode(
                                            field.substring(0, eq), StandardCharsets.UTF_8),
                                    URLDecoder.decode(
                                            field.substring(eq + 1), StandardCharsets.UTF_8))
                            != null) {
                throw new IllegalArgumentException("malformed query");
            }
        }
        return result;
    }

    private void invalid(HttpExchange request, String scenario) throws IOException {
        journal(scenario, ".invalid", "invalid");
        request.sendResponseHeaders(400, -1);
    }

    private synchronized void journal(String scenario, String event, String line)
            throws IOException {
        Files.writeString(
                root.resolve(scenario + event),
                line + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    private void send(HttpExchange request, String scenario, Step action) throws IOException {
        if (!action.retry.isEmpty()) {
            String retry = action.retry;
            if (retry.startsWith("date+")) {
                retry =
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(
                                Instant.now()
                                        .plusSeconds(Long.parseLong(retry.substring(5)))
                                        .atOffset(ZoneOffset.UTC));
            }
            request.getResponseHeaders().add("Retry-After", retry);
        }
        if ("redirect".equals(action.mode)) {
            request.getResponseHeaders()
                    .add("Location", "https://" + HOST + ":" + UNTRUSTED_PORT + "/foreign");
            request.sendResponseHeaders(302, -1);
            return;
        }
        if ("trickle".equals(action.mode)) {
            trickle(request, scenario);
            return;
        }
        if ("hold".equals(action.mode)) {
            long expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (System.nanoTime() - expires < 0
                    && !Files.exists(root.resolve(scenario + ".release"))) {
                if (!sleep(25)) {
                    return;
                }
            }
            if (!Files.exists(root.resolve(scenario + ".release"))) {
                return;
            }
        }
        byte[] bytes = action.body;
        if ("gzip".equals(action.mode)) {
            ByteArrayOutputStream compressed = new ByteArrayOutputStream();
            try (GZIPOutputStream output = new GZIPOutputStream(compressed)) {
                output.write(bytes);
            }
            bytes = compressed.toByteArray();
            request.getResponseHeaders().add("Content-Encoding", "gzip");
        }
        request.getResponseHeaders().add("Content-Type", "application/json");
        if (bytes.length == 0) {
            request.sendResponseHeaders(action.status, -1);
        } else {
            request.sendResponseHeaders(action.status, bytes.length);
            request.getResponseBody().write(bytes);
        }
    }

    private void trickle(HttpExchange request, String scenario) throws IOException {
        request.sendResponseHeaders(200, 0);
        try (OutputStream body = request.getResponseBody()) {
            for (int i = 0; i < 150; i++) {
                body.write(' ');
                body.flush();
                if (!sleep(100)) {
                    break;
                }
            }
        } catch (IOException canceled) {
            journal(scenario, ".disconnect", "disconnected");
        }
    }

    private static boolean sleep(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Defines one scripted provider response and the exact query fields it expects. A scenario can
     * set a response mode and a Retry-After header.
     */
    private static final class Step {
        private final String function;
        private final String size;
        private final String interval;
        private final String adjusted;
        private final int status;
        private final String mode;
        private final String retry;
        private final byte[] body;

        private Step(String line) {
            String[] values = line.split("\\t", -1);
            function = values[0];
            size = values[1];
            interval = values[2];
            adjusted = values[3];
            status = Integer.parseInt(values[4]);
            mode = values[5];
            retry = values[6];
            body = Base64.getDecoder().decode(values[7]);
        }
    }
}
