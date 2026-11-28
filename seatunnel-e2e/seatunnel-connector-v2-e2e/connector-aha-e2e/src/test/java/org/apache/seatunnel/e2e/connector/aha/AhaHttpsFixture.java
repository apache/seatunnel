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

package org.apache.seatunnel.e2e.connector.aha;

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
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/** Independent Aha HTTPS fixture keyed by exact request targets and attempt numbers. */
final class AhaHttpsFixture implements AutoCloseable {
    static final String TENANT = "fixture.aha.io";
    static final String TOKEN = "fixture-only-aha-token";
    static final String STORE_PASSWORD = "fixture-password";
    private final HttpsServer server;
    private final ExecutorService workers = Executors.newCachedThreadPool();
    private final Map<String, AtomicInteger> hits = new ConcurrentHashMap<>();
    private final AtomicInteger invalid = new AtomicInteger();
    private volatile Map<String, Reply> replies = Collections.emptyMap();

    AhaHttpsFixture(Path directory, String name) throws Exception {
        KeyStore identity = identity(directory, name);
        KeyManagerFactory managers =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        managers.init(identity, STORE_PASSWORD.toCharArray());
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(managers.getKeyManagers(), null, null);
        server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));
        server.setExecutor(workers);
        server.createContext("/", this::serve);
        server.start();
        Certificate cert = identity.getCertificate("fixture");
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, null);
        trust.setCertificateEntry("fixture", cert);
        try (OutputStream output = Files.newOutputStream(directory.resolve(name + "-trust.jks"))) {
            trust.store(output, STORE_PASSWORD.toCharArray());
        }
    }

    int port() {
        return server.getAddress().getPort();
    }

    void script(Map<String, Reply> script) {
        hits.clear();
        invalid.set(0);
        replies = script;
    }

    int hits(String target) {
        AtomicInteger counter = hits.get(target);
        return counter == null ? 0 : counter.get();
    }

    int invalidRequests() {
        return invalid.get();
    }

    private void serve(HttpExchange exchange) throws IOException {
        try {
            String target = exchange.getRequestURI().toASCIIString();
            String host = exchange.getRequestHeaders().getFirst("Host");
            if (!"GET".equals(exchange.getRequestMethod())
                    || (!(TENANT + ":8443").equals(host) && !(TENANT + ":8444").equals(host))
                    || !("Bearer " + TOKEN)
                            .equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                invalid.incrementAndGet();
                status(exchange, 401);
                return;
            }
            Reply reply = replies.get(target);
            if (reply == null) {
                invalid.incrementAndGet();
                status(exchange, 404);
                return;
            }
            reply.send(
                    exchange,
                    hits.computeIfAbsent(target, key -> new AtomicInteger()).incrementAndGet());
        } finally {
            exchange.close();
        }
    }

    static void status(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
    }

    static void json(HttpExchange exchange, String body) throws IOException {
        send(exchange, body.getBytes(StandardCharsets.UTF_8));
    }

    static void gzip(HttpExchange exchange, String body) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
            gzip.write(body.getBytes(StandardCharsets.UTF_8));
        }
        exchange.getResponseHeaders().add("Content-Encoding", "gzip");
        send(exchange, bytes.toByteArray());
    }

    private static void send(HttpExchange exchange, byte[] body) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
    }

    private static KeyStore identity(Path directory, String name) throws Exception {
        Path key = directory.resolve(name + ".p12");
        Process process =
                new ProcessBuilder(
                                java.nio.file.Paths.get(
                                                System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-alias",
                                "fixture",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "3",
                                "-dname",
                                "CN=" + TENANT,
                                "-ext",
                                "SAN=dns:" + TENANT,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                key.toString(),
                                "-storepass",
                                STORE_PASSWORD,
                                "-keypass",
                                STORE_PASSWORD,
                                "-noprompt")
                        .redirectErrorStream(true)
                        .redirectOutput(directory.resolve(name + "-keytool.log").toFile())
                        .start();
        if (!process.waitFor(30, TimeUnit.SECONDS) || process.exitValue() != 0) {
            process.destroyForcibly();
            throw new IOException("Fixture keytool did not create an HTTPS identity");
        }
        KeyStore identity = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(key)) {
            identity.load(input, STORE_PASSWORD.toCharArray());
        }
        return identity;
    }

    @Override
    public void close() {
        server.stop(0);
        workers.shutdownNow();
    }

    @FunctionalInterface
    interface Reply {
        void send(HttpExchange exchange, int attempt) throws IOException;
    }
}
