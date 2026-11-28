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

package org.apache.seatunnel.e2e.connector.apify;

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
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * Supplies independent Apify HTTPS responses to real engine jobs. It records accepted requests and
 * independently checks route, raw query and bearer authentication.
 */
public final class ApifyProvider {
    static final String HOST = "api.apify.com";
    static final String TOKEN = "synthetic-apify-fixture-token";
    static final String PASSWORD = "fixture-pass";
    static final int TRUSTED_PORT = 18443;
    static final int UNTRUSTED_PORT = 18444;

    private final Path root;
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    private ApifyProvider(Path root) {
        this.root = root;
    }

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(args[0]);
        ApifyProvider provider = new ApifyProvider(root);
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpsServer trusted = provider.start(TRUSTED_PORT, "trusted", executor);
        HttpsServer untrusted = provider.start(UNTRUSTED_PORT, "untrusted", executor);
        try {
            Files.writeString(root.resolve("ready"), "ready");
            while (!Files.exists(root.resolve("stop"))) {
                Thread.sleep(25);
            }
        } finally {
            trusted.stop(0);
            untrusted.stop(0);
            executor.shutdownNow();
        }
    }

    private HttpsServer start(int port, String identity, ExecutorService executor)
            throws Exception {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        char[] password = PASSWORD.toCharArray();
        try (InputStream input = Files.newInputStream(root.resolve(identity + ".p12"))) {
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
            String[] program = Files.readString(root.resolve(scenario + ".program")).split("\n");
            int index =
                    attempts.computeIfAbsent(scenario, ignored -> new AtomicInteger())
                            .getAndIncrement();
            if (index >= program.length) {
                invalid(request, scenario);
                return;
            }
            Reply reply = new Reply(program[index]);
            String target = request.getRequestURI().getRawPath();
            if (!"GET".equals(request.getRequestMethod())
                    || !reply.route.equals(target)
                    || !(HOST + ":" + request.getLocalAddress().getPort())
                            .equals(request.getRequestHeaders().getFirst("Host"))
                    || !("Bearer " + TOKEN)
                            .equals(request.getRequestHeaders().getFirst("Authorization"))
                    || !parameters(reply).equals(query(request.getRequestURI().getRawQuery()))) {
                invalid(request, scenario);
                return;
            }
            journal(scenario, ".requests", target + "|" + reply.offset + "|" + reply.limit);
            send(request, scenario, reply);
        } finally {
            request.close();
        }
    }

    private static Map<String, String> parameters(Reply reply) {
        Map<String, String> expected = new HashMap<>();
        if (!reply.offset.isEmpty()) {
            expected.put("offset", reply.offset);
            expected.put("limit", reply.limit);
            expected.put("desc", "false");
        }
        if (reply.route.endsWith("/items")) {
            expected.put("format", "json");
        }
        return expected;
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> params = new HashMap<>();
        if (raw == null) {
            return params;
        }
        for (String parameter : raw.split("&", -1)) {
            int equals = parameter.indexOf('=');
            if (equals <= 0
                    || params.put(parameter.substring(0, equals), parameter.substring(equals + 1))
                            != null) {
                return null;
            }
        }
        return params;
    }

    private void invalid(HttpExchange request, String scenario) throws IOException {
        journal(scenario, ".invalid", "unexpected request");
        request.sendResponseHeaders(400, -1);
    }

    private synchronized void journal(String scenario, String suffix, String value)
            throws IOException {
        Files.writeString(
                root.resolve(scenario + suffix),
                value + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    private void send(HttpExchange request, String scenario, Reply reply) throws IOException {
        if (!reply.count.isEmpty()) {
            request.getResponseHeaders().set("X-Apify-Pagination-Offset", reply.returnedOffset);
            request.getResponseHeaders().set("X-Apify-Pagination-Limit", reply.limit);
            request.getResponseHeaders().set("X-Apify-Pagination-Count", reply.count);
            request.getResponseHeaders().set("X-Apify-Pagination-Total", reply.total);
            request.getResponseHeaders().set("X-Apify-Pagination-Desc", "false");
        }
        if (!reply.retryAfter.isEmpty()) {
            request.getResponseHeaders().set("Retry-After", reply.retryAfter);
        }
        if ("redirect".equals(reply.mode)) {
            request.getResponseHeaders()
                    .set("Location", "https://" + HOST + ":" + UNTRUSTED_PORT + "/foreign");
            request.sendResponseHeaders(302, -1);
            return;
        }
        if ("trickle".equals(reply.mode)) {
            trickle(request, scenario);
            return;
        }
        if ("hold".equals(reply.mode)) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (!Files.exists(root.resolve(scenario + ".release"))
                    && System.nanoTime() - deadline < 0) {
                if (!sleep(25)) {
                    return;
                }
            }
        }
        byte[] body = reply.body;
        if ("gzip".equals(reply.mode)) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            try (GZIPOutputStream compressed = new GZIPOutputStream(buffer)) {
                compressed.write(body);
            }
            body = buffer.toByteArray();
            request.getResponseHeaders().set("Content-Encoding", "gzip");
        }
        request.getResponseHeaders().set("Content-Type", "application/json");
        request.sendResponseHeaders(reply.status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream output = request.getResponseBody()) {
                output.write(body);
            } catch (IOException disconnected) {
                journal(scenario, ".disconnect", "body disconnected");
            }
        }
    }

    private void trickle(HttpExchange request, String scenario) throws IOException {
        request.sendResponseHeaders(200, 0);
        try (OutputStream output = request.getResponseBody()) {
            output.write('[');
            for (int i = 0; i < 1800; i++) {
                output.write(' ');
                output.flush();
                if (!sleep(50)) {
                    return;
                }
            }
        } catch (IOException disconnected) {
            journal(scenario, ".disconnect", "stream disconnected");
        }
    }

    private static boolean sleep(long delay) {
        try {
            Thread.sleep(delay);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Binds one independent expected request to its response and pagination scan metadata. The
     * fixture can hold or slowly stream a body for lifecycle tests.
     */
    private static final class Reply {
        private final String route;
        private final String offset;
        private final String limit;
        private final int status;
        private final String mode;
        private final String returnedOffset;
        private final String count;
        private final String total;
        private final String retryAfter;
        private final byte[] body;

        private Reply(String request) {
            String[] fields = request.split("\t", -1);
            route = fields[0];
            offset = fields[1];
            limit = fields[2];
            status = Integer.parseInt(fields[3]);
            mode = fields[4];
            returnedOffset = fields[5];
            count = fields[6];
            total = fields[7];
            retryAfter = fields[8];
            body = Base64.getDecoder().decode(fields[9]);
        }
    }
}
