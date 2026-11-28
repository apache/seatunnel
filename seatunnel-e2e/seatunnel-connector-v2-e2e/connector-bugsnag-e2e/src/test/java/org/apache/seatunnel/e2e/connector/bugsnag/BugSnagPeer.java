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
package org.apache.seatunnel.e2e.connector.bugsnag;

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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

/**
 * Standalone Java 11 HTTPS peer for the selected BugSnag Data Access v2 routes.
 *
 * <p>The HTTPS listener records what Zeta actually sends. The separate test control socket queues
 * bounded scripted responses and exposes request timing and disconnect observations.
 */
public final class BugSnagPeer {
    private final Queue<String[]> responses = new ConcurrentLinkedQueue<>();
    private final List<String> trace = new ArrayList<>();

    /**
     * Starts both test listeners using a container-local certificate.
     *
     * <p>This class can run as a standalone program with only JDK modules on its classpath.
     */
    public static void main(String[] args) throws Exception {
        char[] pass = "bugsnag-fixture".toCharArray();
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream bytes = Files.newInputStream(Path.of(args[0]))) {
            store.load(bytes, pass);
        }
        KeyManagerFactory keys =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, pass);
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(keys.getKeyManagers(), null, null);

        BugSnagPeer fixture = new BugSnagPeer();
        HttpsServer provider = HttpsServer.create(new InetSocketAddress(18443), 24);
        provider.setHttpsConfigurator(new HttpsConfigurator(tls));
        provider.setExecutor(Executors.newFixedThreadPool(6));
        provider.createContext("/", fixture::vendor);
        provider.start();

        HttpServer control = HttpServer.create(new InetSocketAddress(18080), 8);
        control.setExecutor(Executors.newSingleThreadExecutor());
        control.createContext("/", fixture::admin);
        control.start();
    }

    private void admin(HttpExchange exchange) throws IOException {
        try {
            switch (exchange.getRequestURI().getPath()) {
                case "/requests":
                    byte[] snapshot;
                    synchronized (trace) {
                        snapshot = String.join("\n", trace).getBytes(StandardCharsets.UTF_8);
                    }
                    send(exchange, 200, snapshot);
                    return;
                case "/reset":
                    synchronized (trace) {
                        trace.clear();
                        responses.clear();
                    }
                    break;
                case "/queue":
                    break;
                default:
                    send(exchange, 404, new byte[0]);
                    return;
            }
            String plan =
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            for (String reply : plan.split("\n")) {
                if (!reply.isEmpty()) {
                    responses.add(reply.split("\t", -1));
                }
            }
            send(exchange, 200, new byte[0]);
        } finally {
            exchange.close();
        }
    }

    private void vendor(HttpExchange exchange) throws IOException {
        try {
            String[] entry;
            String target = exchange.getRequestURI().toASCIIString();
            synchronized (trace) {
                trace.add(
                        "call\t"
                                + exchange.getRequestMethod()
                                + "\t"
                                + target
                                + "\t"
                                + System.nanoTime()
                                + "\t"
                                + "token bugsnag-fixture-private+/=?"
                                        .equals(
                                                exchange.getRequestHeaders()
                                                        .getFirst("Authorization"))
                                + "\t"
                                + "application/json"
                                        .equals(exchange.getRequestHeaders().getFirst("Accept"))
                                + "\t"
                                + "2".equals(exchange.getRequestHeaders().getFirst("X-Version")));
                entry = responses.poll();
            }
            if (entry == null || !entry[4].equals(exchange.getRequestURI().getRawPath())) {
                send(exchange, 418, new byte[0]);
                return;
            }
            int status = Integer.parseInt(entry[0]);
            String mode = entry[1];
            byte[] content = Base64.getDecoder().decode(entry[3]);
            exchange.getResponseHeaders()
                    .set(
                            "Content-Type",
                            "binary".equals(mode) ? "image/svg+xml" : "application/json");
            if (!entry[2].isEmpty()) {
                exchange.getResponseHeaders().set("Retry-After", entry[2]);
            }
            if (!entry[5].isEmpty()) {
                String encoded =
                        new String(Base64.getDecoder().decode(entry[5]), StandardCharsets.UTF_8);
                for (String link : encoded.split("\n")) {
                    exchange.getResponseHeaders().add("Link", link);
                }
            }
            if ("truncated".equals(mode)) {
                exchange.sendResponseHeaders(status, content.length + 64);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(content);
                }
                return;
            }
            if ("chunked".equals(mode)) {
                drip(exchange, status, exchange.getRequestURI().getPath());
                return;
            }
            if ("redirect".equals(mode)) {
                exchange.getResponseHeaders()
                        .set("Location", "https://bugsnag-redirect-host:18443/redirect-receiver");
            } else if ("unsupported".equals(mode)) {
                exchange.getResponseHeaders().set("Content-Encoding", "br");
            } else if ("gzip".equals(mode)) {
                exchange.getResponseHeaders().set("Content-Encoding", "gzip");
                ByteArrayOutputStream compressed = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
                    gzip.write(content);
                }
                content = compressed.toByteArray();
            }
            send(exchange, status, content);
        } finally {
            exchange.close();
        }
    }

    private void drip(HttpExchange exchange, int status, String path) throws IOException {
        exchange.sendResponseHeaders(status, 0);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write('[');
            for (int i = 0; i < 800; i++) {
                body.write(' ');
                body.flush();
                Thread.sleep(50L);
            }
        } catch (IOException closed) {
            synchronized (trace) {
                trace.add("disconnect\t" + path);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void send(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }
}
