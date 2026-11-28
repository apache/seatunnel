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
package org.apache.seatunnel.e2e.connector.ashby;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

/**
 * Standalone Java HTTPS peer with independently programmed replies and an exact request trace.
 *
 * <p>The control socket is exposed to the test runner only through the container port, while Zeta
 * calls the TLS socket on the isolated test network. The peer needs only JDK 11 classes.
 */
public final class AshbyPeer {
    private static final String AUTH =
            "Basic " + encode("ashby-fixture-auth:".getBytes(StandardCharsets.UTF_8));

    private final Map<String, Deque<Reply>> replies = new HashMap<>();
    private final List<String> trace = new ArrayList<>();

    /**
     * Starts the HTTPS vendor endpoint and the HTTP test control endpoint.
     *
     * <p>The provider receives real TLS handshakes and parses traffic independently of the
     * connector.
     */
    public static void main(String[] args) throws Exception {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        char[] password = "ashby-fixture".toCharArray();
        try (InputStream data = Files.newInputStream(Path.of(args[0]))) {
            keys.load(data, password);
        }
        KeyManagerFactory managers =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        managers.init(keys, password);
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(managers.getKeyManagers(), null, null);

        AshbyPeer peer = new AshbyPeer();
        HttpsServer api = HttpsServer.create(new InetSocketAddress(8444), 16);
        api.setHttpsConfigurator(new HttpsConfigurator(tls));
        api.setExecutor(Executors.newFixedThreadPool(12));
        api.createContext("/", peer::read);
        api.start();

        HttpServer control = HttpServer.create(new InetSocketAddress(8080), 16);
        control.setExecutor(Executors.newFixedThreadPool(2));
        control.createContext("/", peer::control);
        control.start();
    }

    private void control(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            if ("/trace".equals(path)) {
                byte[] snapshot;
                synchronized (this) {
                    snapshot = String.join("\n", trace).getBytes(StandardCharsets.UTF_8);
                }
                send(exchange, 200, snapshot);
                return;
            }
            if ("/program".equals(path) || "/append".equals(path)) {
                String body =
                        new String(
                                exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                synchronized (this) {
                    if ("/program".equals(path)) {
                        trace.clear();
                        replies.clear();
                    }
                    for (String line : body.split("\n")) {
                        if (!line.isEmpty()) {
                            String[] fields = line.split("\t", -1);
                            replies.computeIfAbsent(fields[0], route -> new ArrayDeque<>())
                                    .addLast(new Reply(fields));
                        }
                    }
                }
                send(exchange, 200, new byte[0]);
                return;
            }
            send(exchange, 404, new byte[0]);
        } finally {
            exchange.close();
        }
    }

    private void read(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().toString();
            byte[] request = exchange.getRequestBody().readAllBytes();
            boolean authenticated =
                    AUTH.equals(exchange.getRequestHeaders().getFirst("Authorization"));
            Reply reply;
            synchronized (this) {
                trace.add(
                        String.join(
                                "\t",
                                "request",
                                path,
                                exchange.getRequestMethod(),
                                Boolean.toString(authenticated),
                                Long.toString(System.nanoTime()),
                                encode(request),
                                Boolean.toString(
                                        exchange.getRequestHeaders().getFirst("X-On-Behalf-Of")
                                                == null)));
                Deque<Reply> queue = replies.get(path);
                reply = queue == null ? null : queue.pollFirst();
            }
            if (!authenticated || !"POST".equals(exchange.getRequestMethod())) {
                send(exchange, 401, new byte[0]);
                return;
            }
            if (reply == null) {
                send(exchange, 418, new byte[0]);
                return;
            }
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            if (!reply.retryAfter.isEmpty()) {
                exchange.getResponseHeaders().set("Retry-After", reply.retryAfter);
            }
            if ("redirect".equals(reply.mode)) {
                exchange.getResponseHeaders().set("Location", "https://ashby-peer:8444/escape");
            }
            if ("gzip".equals(reply.mode)) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                try (GZIPOutputStream compressed = new GZIPOutputStream(bytes)) {
                    compressed.write(reply.body);
                }
                exchange.getResponseHeaders().set("Content-Encoding", "gzip");
                send(exchange, reply.status, bytes.toByteArray());
                return;
            }
            if ("slow".equals(reply.mode)) {
                sendSlowly(exchange, reply.status, path);
                return;
            }
            send(exchange, reply.status, reply.body);
        } finally {
            exchange.close();
        }
    }

    private void sendSlowly(HttpExchange exchange, int status, String path) throws IOException {
        exchange.sendResponseHeaders(status, 0);
        try (OutputStream target = exchange.getResponseBody()) {
            target.write('{');
            target.flush();
            // Valid JSON whitespace keeps arriving so a per-read timeout cannot enforce the
            // deadline.
            for (int i = 0; i < 600; i++) {
                Thread.sleep(50);
                target.write(' ');
                target.flush();
            }
        } catch (IOException disconnected) {
            synchronized (this) {
                trace.add("disconnect\t" + path);
            }
        } catch (InterruptedException shutdown) {
            Thread.currentThread().interrupt();
        }
    }

    private static void send(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream target = exchange.getResponseBody()) {
                target.write(body);
            }
        }
    }

    private static String encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * One programmed response for a specific RPC, taken in request order for that path.
     *
     * <p>The control channel can add new replies while a saved job's connection is closing.
     */
    private static final class Reply {
        private final int status;
        private final String retryAfter;
        private final String mode;
        private final byte[] body;

        private Reply(String[] fields) {
            status = Integer.parseInt(fields[1]);
            retryAfter = fields[2];
            mode = fields[3];
            body = Base64.getDecoder().decode(fields[4]);
        }
    }
}
