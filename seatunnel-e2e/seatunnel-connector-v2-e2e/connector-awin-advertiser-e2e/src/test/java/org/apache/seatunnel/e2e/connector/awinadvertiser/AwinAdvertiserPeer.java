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
package org.apache.seatunnel.e2e.connector.awinadvertiser;

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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

/**
 * Standalone JDK HTTPS peer for read-only legacy Awin requests.
 *
 * <p>The control listener steps through a test's programmed responses and records real request
 * timing, authorization success, and transfer termination without exposing a bearer token.
 */
public final class AwinAdvertiserPeer {
    private final Deque<String[]> scheduled = new ArrayDeque<>();
    private final List<String> observations = new ArrayList<>();

    /**
     * Runs the peer in its own JDK 11 container.
     *
     * <p>Only the fixture process sees this certificate's private key.
     */
    public static void main(String[] args) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        char[] password = "awin-fixture".toCharArray();
        try (InputStream keys = Files.newInputStream(Path.of(args[0]))) {
            store.load(keys, password);
        }
        KeyManagerFactory manager =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(store, password);
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(manager.getKeyManagers(), null, null);

        AwinAdvertiserPeer peer = new AwinAdvertiserPeer();
        HttpsServer service = HttpsServer.create(new InetSocketAddress(18443), 16);
        service.setHttpsConfigurator(new HttpsConfigurator(ssl));
        service.setExecutor(Executors.newFixedThreadPool(8));
        service.createContext("/", peer::receive);
        service.start();

        HttpServer control = HttpServer.create(new InetSocketAddress(18080), 8);
        control.setExecutor(Executors.newSingleThreadExecutor());
        control.createContext("/", peer::configure);
        control.start();
    }

    private void configure(HttpExchange exchange) throws IOException {
        try {
            String action = exchange.getRequestURI().getPath();
            if ("/log".equals(action)) {
                byte[] snapshot;
                synchronized (this) {
                    snapshot = String.join("\n", observations).getBytes(StandardCharsets.UTF_8);
                }
                send(exchange, 200, snapshot);
                return;
            }
            if (!"/replace".equals(action) && !"/add".equals(action)) {
                send(exchange, 404, new byte[0]);
                return;
            }
            String script =
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            synchronized (this) {
                if ("/replace".equals(action)) {
                    observations.clear();
                    scheduled.clear();
                }
                for (String line : script.split("\n")) {
                    if (!line.isEmpty()) {
                        scheduled.addLast(line.split("\t", -1));
                    }
                }
            }
            send(exchange, 200, new byte[0]);
        } finally {
            exchange.close();
        }
    }

    private void receive(HttpExchange exchange) throws IOException {
        try {
            String route = exchange.getRequestURI().toASCIIString();
            boolean auth =
                    "Bearer awin-test-token-private"
                            .equals(exchange.getRequestHeaders().getFirst("Authorization"));
            String[] next;
            synchronized (this) {
                observations.add(
                        "call\t"
                                + route
                                + "\t"
                                + exchange.getRequestMethod()
                                + "\t"
                                + auth
                                + "\t"
                                + System.nanoTime()
                                + "\t"
                                + exchange.getRequestBody().readAllBytes().length);
                next = scheduled.pollFirst();
            }
            if (!auth) {
                send(exchange, 401, new byte[0]);
                return;
            }
            if (next == null) {
                send(exchange, 418, new byte[0]);
                return;
            }
            int status = Integer.parseInt(next[0]);
            String mode = next[1];
            byte[] json = Base64.getDecoder().decode(next[3]);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            if (!next[2].isEmpty()) {
                exchange.getResponseHeaders().set("Retry-After", next[2]);
            }
            if ("drip".equals(mode)) {
                stream(exchange, status);
                return;
            }
            if ("location".equals(mode)) {
                exchange.getResponseHeaders().set("Location", "https://awin-peer:18443/unexpected");
            } else if ("range".equals(mode)) {
                exchange.getResponseHeaders().set("Content-Range", "items 0-0/200");
            } else if ("link".equals(mode)) {
                exchange.getResponseHeaders().add("Link", "</more>; rel=\"next\"");
            } else if ("gzip".equals(mode)) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
                    gzip.write(json);
                }
                json = bytes.toByteArray();
                exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            }
            send(exchange, status, json);
        } finally {
            exchange.close();
        }
    }

    private void stream(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, 0);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write('[');
            for (int i = 0; i < 800; i++) {
                output.write(' ');
                output.flush();
                Thread.sleep(50L);
            }
        } catch (IOException disconnected) {
            synchronized (this) {
                observations.add("closed\t" + exchange.getRequestURI().getPath());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void send(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        }
    }
}
