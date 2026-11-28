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

package org.apache.seatunnel.e2e.connector.appcues;

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * Serves the documented Appcues array paths independently of the connector. The fixture records
 * only validated requests and never echoes authorization to the job.
 */
public final class AppcuesPeer {
    static final String US = "api.appcues.com";
    static final String EU = "api.eu.appcues.com";
    static final String KEY = "synthetic-appcues-api-key";
    static final String SECRET = "synthetic-appcues:api-secret";
    static final String PASSWORD = "local-fixture-pass";
    static final String ACCOUNT = "test+account";
    static final String ACCOUNT_PATH = "/v2/accounts/test%2Baccount/";
    static final int TRUSTED_PORT = 18443;
    static final int UNTRUSTED_PORT = 18444;
    static final String AUTHORIZATION =
            "Basic "
                    + Base64.getEncoder()
                            .encodeToString(
                                    (KEY + ":" + SECRET).getBytes(StandardCharsets.US_ASCII));

    private final Path root;
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    private AppcuesPeer(Path root) {
        this.root = root;
    }

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(args[0]);
        AppcuesPeer fixture = new AppcuesPeer(root);
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpsServer trusted = fixture.start(TRUSTED_PORT, "trusted", executor);
        HttpsServer untrusted = fixture.start(UNTRUSTED_PORT, "untrusted", executor);
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
            String[] script = Files.readString(root.resolve(scenario + ".program")).split("\n");
            int index =
                    attempts.computeIfAbsent(scenario, ignored -> new AtomicInteger())
                            .getAndIncrement();
            if (index >= script.length) {
                invalid(request, scenario);
                return;
            }
            Reply expected = new Reply(script[index]);
            if (!"GET".equals(request.getRequestMethod())
                    || !(ACCOUNT_PATH + expected.resource)
                            .equals(request.getRequestURI().getRawPath())
                    || request.getRequestURI().getRawQuery() != null
                    || !(expected.host + ":" + request.getLocalAddress().getPort())
                            .equals(request.getRequestHeaders().getFirst("Host"))
                    || !AUTHORIZATION.equals(request.getRequestHeaders().getFirst("Authorization"))
                    || !"application/json".equals(request.getRequestHeaders().getFirst("Accept"))) {
                invalid(request, scenario);
                return;
            }
            journal(scenario, ".requests", expected.host + "/" + expected.resource);
            send(request, scenario, expected);
        } finally {
            request.close();
        }
    }

    private synchronized void journal(String scenario, String extension, String value)
            throws IOException {
        Files.writeString(
                root.resolve(scenario + extension),
                value + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    private void invalid(HttpExchange request, String scenario) throws IOException {
        journal(scenario, ".invalid", "unexpected route, headers, or query");
        request.sendResponseHeaders(400, -1);
    }

    private void send(HttpExchange request, String scenario, Reply reply) throws IOException {
        if (!reply.retryAfter.isEmpty()) {
            request.getResponseHeaders().set("Retry-After", reply.retryAfter);
        }
        if ("next-link".equals(reply.mode)) {
            request.getResponseHeaders()
                    .set("Link", "<https://api.appcues.com/v2/continuation>; rel=\"next\"");
        } else if ("next-token".equals(reply.mode)) {
            request.getResponseHeaders().set("X-Next-Page-Token", "unknown-continuation-token");
        } else if ("redirect".equals(reply.mode)) {
            request.getResponseHeaders()
                    .set("Location", "https://" + EU + ":" + TRUSTED_PORT + "/foreign");
            request.sendResponseHeaders(302, -1);
            return;
        } else if ("slow".equals(reply.mode) || "partial".equals(reply.mode)) {
            stream(request, scenario, reply);
            return;
        } else if ("hold".equals(reply.mode) && !waitForRelease(scenario)) {
            return;
        }
        byte[] body = reply.body;
        if ("gzip".equals(reply.mode)) {
            ByteArrayOutputStream compressed = new ByteArrayOutputStream();
            try (GZIPOutputStream output = new GZIPOutputStream(compressed)) {
                output.write(body);
            }
            body = compressed.toByteArray();
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

    private boolean waitForRelease(String scenario) throws IOException {
        long expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (!Files.exists(root.resolve(scenario + ".release"))
                && System.nanoTime() - expires < 0) {
            if (!sleep(25)) {
                return false;
            }
        }
        return Files.exists(root.resolve(scenario + ".release"));
    }

    private void stream(HttpExchange request, String scenario, Reply reply) throws IOException {
        request.sendResponseHeaders(200, 0);
        try (OutputStream output = request.getResponseBody()) {
            output.write('[');
            output.flush();
            if ("partial".equals(reply.mode)) {
                journal(scenario, ".partial", "first bytes sent");
                if (waitForRelease(scenario)) {
                    output.write(reply.body, 1, reply.body.length - 1);
                }
                return;
            }
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
     * Defines one expected resource request and the independent HTTP response to return. Some modes
     * intentionally hold or truncate a response for lifecycle coverage.
     */
    private static final class Reply {
        private final String host;
        private final String resource;
        private final int status;
        private final String mode;
        private final String retryAfter;
        private final byte[] body;

        private Reply(String instruction) {
            String[] fields = instruction.split("\t", -1);
            host = fields[0];
            resource = fields[1];
            status = Integer.parseInt(fields[2]);
            mode = fields[3];
            retryAfter = fields[4];
            body = Base64.getDecoder().decode(fields[5]);
        }
    }
}
