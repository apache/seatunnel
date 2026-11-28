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

package org.apache.seatunnel.e2e.connector.appsflyer;

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
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

/**
 * Serves scripted reports in the engine container using certificates unrelated to production. It
 * independently validates each URL, decoded query and credential boundary before responding.
 */
public final class AppsFlyerPeer {
    static final String HOST = "hq1.appsflyer.com";
    static final String DOWNLOAD = "download.appsflyer-fixture.invalid";
    static final String TOKEN = "fixture-appsflyer-token";
    static final String PASSWORD = "fixture-password";
    static final int TRUSTED_PORT = 18443;
    static final int UNTRUSTED_PORT = 18444;

    private AppsFlyerPeer() {}

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpsServer trusted = listen(dir, "trusted", TRUSTED_PORT, executor);
        HttpsServer untrusted = listen(dir, "untrusted", UNTRUSTED_PORT, executor);
        try {
            Files.createFile(dir.resolve("ready"));
            while (!Files.exists(dir.resolve("stop"))) {
                Thread.sleep(50);
            }
        } finally {
            trusted.stop(0);
            untrusted.stop(0);
            executor.shutdownNow();
        }
    }

    private static HttpsServer listen(Path dir, String identity, int port, ExecutorService executor)
            throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        char[] password = PASSWORD.toCharArray();
        try (InputStream input = Files.newInputStream(dir.resolve(identity + ".p12"))) {
            store.load(input, password);
        }
        KeyManagerFactory keys =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, password);
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(keys.getKeyManagers(), null, null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", port), 16);
        server.setHttpsConfigurator(new HttpsConfigurator(ssl));
        server.createContext("/", exchange -> respond(dir, exchange));
        server.setExecutor(executor);
        server.start();
        return server;
    }

    private static void respond(Path dir, HttpExchange exchange) throws IOException {
        String name = Files.readString(dir.resolve("active")).trim();
        try {
            String[] reply = next(dir, name, exchange);
            if (reply == null) {
                exchange.sendResponseHeaders(500, -1);
                return;
            }
            byte[] body = Base64.getDecoder().decode(reply[6]);
            int status = Integer.parseInt(reply[2]);
            String mode = reply[3];
            if (!reply[4].isEmpty()) {
                exchange.getResponseHeaders().set("Location", reply[4]);
            }
            if (!reply[5].isEmpty()) {
                exchange.getResponseHeaders().set("Retry-After", reply[5]);
            }
            exchange.getResponseHeaders()
                    .set(
                            "Content-Type",
                            "json".equals(mode) ? "application/json" : "text/csv; charset=utf-8");
            if ("hold".equals(mode)) {
                try {
                    while (!Files.exists(dir.resolve(name + ".release"))) {
                        Thread.sleep(30);
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            if ("trickle".equals(mode)) {
                exchange.sendResponseHeaders(status, 0);
                try (OutputStream output = exchange.getResponseBody()) {
                    for (int i = 0; i < 400; i++) {
                        output.write('x');
                        output.flush();
                        Thread.sleep(60);
                    }
                } catch (IOException closed) {
                    Files.writeString(dir.resolve(name + ".disconnect"), "closed");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            if ("gzip".equals(mode)) {
                ByteArrayOutputStream encoded = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(encoded)) {
                    gzip.write(body);
                }
                body = encoded.toByteArray();
                exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            }
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length != 0) {
                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
            }
        } finally {
            exchange.close();
        }
    }

    private static synchronized String[] next(Path dir, String name, HttpExchange exchange)
            throws IOException {
        Path calls = dir.resolve(name + ".requests");
        String actual =
                exchange.getRequestHeaders().getFirst("Host").replaceFirst(":\\d+$", "")
                        + " "
                        + exchange.getRequestURI().getRawPath()
                        + " "
                        + query(exchange.getRequestURI().getRawQuery());
        int index = Files.exists(calls) ? Files.readAllLines(calls).size() : 0;
        List<String> program = Files.readAllLines(dir.resolve(name + ".program"));
        String[] reply =
                index < program.size() ? program.get(index).split("\\t", -1) : new String[0];
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        boolean matches =
                reply.length == 7
                        && actual.equals(reply[0])
                        && "GET".equals(exchange.getRequestMethod())
                        && ("bearer".equals(reply[1])
                                ? ("Bearer " + TOKEN).equals(authorization)
                                : authorization == null);
        if (!matches) {
            Files.writeString(
                    dir.resolve(name + ".invalid"), actual + " unexpected request " + index);
        }
        Files.writeString(
                calls, actual + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        return matches ? reply : null;
    }

    private static Map<String, String> query(String raw) throws IOException {
        Map<String, String> parsed = new TreeMap<>();
        if (raw == null) {
            return parsed;
        }
        for (String component : raw.split("&")) {
            String[] tokens = component.split("=", 2);
            String key = URLDecoder.decode(tokens[0], StandardCharsets.UTF_8);
            if (tokens.length != 2
                    || parsed.putIfAbsent(key, URLDecoder.decode(tokens[1], StandardCharsets.UTF_8))
                            != null) {
                throw new IOException("Invalid query in provider fixture");
            }
        }
        return parsed;
    }
}
