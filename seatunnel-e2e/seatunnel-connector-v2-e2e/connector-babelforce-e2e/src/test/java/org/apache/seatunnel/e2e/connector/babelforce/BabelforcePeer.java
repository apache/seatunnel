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
package org.apache.seatunnel.e2e.connector.babelforce;

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
 * Isolated HTTPS Babelforce peer for both supported credential modes.
 *
 * <p>The peer records exact wire paths and queries, whether the expected credential headers were
 * present, and connection timing. It uses only JDK 11 classes.
 */
public final class BabelforcePeer {
    private final Deque<String[]> frames = new ArrayDeque<>();
    private final List<String> log = new ArrayList<>();

    /**
     * Starts a private TLS reporting endpoint and separate test control listener.
     *
     * <p>Only this process holds the private key used for the reporting listener.
     */
    public static void main(String[] args) throws Exception {
        char[] password = "babelforce-fixture".toCharArray();
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream data = Files.newInputStream(Path.of(args[0]))) {
            store.load(data, password);
        }
        KeyManagerFactory keys =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, password);
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(keys.getKeyManagers(), null, null);

        BabelforcePeer peer = new BabelforcePeer();
        HttpsServer api = HttpsServer.create(new InetSocketAddress(19443), 16);
        api.setHttpsConfigurator(new HttpsConfigurator(tls));
        api.setExecutor(Executors.newFixedThreadPool(6));
        api.createContext("/", peer::reply);
        api.start();

        HttpServer control = HttpServer.create(new InetSocketAddress(19080), 8);
        control.setExecutor(Executors.newSingleThreadExecutor());
        control.createContext("/", peer::control);
        control.start();
    }

    private void control(HttpExchange exchange) throws IOException {
        try {
            String resource = exchange.getRequestURI().getPath();
            if ("/log".equals(resource)) {
                byte[] lines;
                synchronized (this) {
                    lines = String.join("\n", log).getBytes(StandardCharsets.UTF_8);
                }
                send(exchange, 200, lines);
                return;
            }
            if (!"/program".equals(resource) && !"/append".equals(resource)) {
                send(exchange, 404, new byte[0]);
                return;
            }
            String patch =
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            synchronized (this) {
                if ("/program".equals(resource)) {
                    log.clear();
                    frames.clear();
                }
                for (String line : patch.split("\n")) {
                    if (!line.isEmpty()) {
                        frames.addLast(line.split("\t", -1));
                    }
                }
            }
            send(exchange, 200, new byte[0]);
        } finally {
            exchange.close();
        }
    }

    private void reply(HttpExchange exchange) throws IOException {
        try {
            String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            String id = exchange.getRequestHeaders().getFirst("X-Auth-Access-Id");
            String token = exchange.getRequestHeaders().getFirst("X-Auth-Access-Token");
            String auth;
            if (authorization == null
                    && "fixture-access-id-private".equals(id)
                    && "fixture-token-private".equals(token)) {
                auth = "keys";
            } else if (id == null
                    && token == null
                    && "Bearer fixture-token-private".equals(authorization)) {
                auth = "bearer";
            } else {
                auth = "invalid";
            }
            int submitted = exchange.getRequestBody().readAllBytes().length;
            String[] frame;
            synchronized (this) {
                log.add(
                        String.join(
                                "\t",
                                "call",
                                exchange.getRequestMethod(),
                                exchange.getRequestURI().toASCIIString(),
                                auth,
                                Long.toString(System.nanoTime()),
                                Integer.toString(submitted)));
                frame = frames.pollFirst();
            }
            if ("invalid".equals(auth)) {
                send(exchange, 401, new byte[0]);
                return;
            }
            if (frame == null) {
                send(exchange, 418, new byte[0]);
                return;
            }
            int status = Integer.parseInt(frame[0]);
            String behaviour = frame[1];
            byte[] payload = Base64.getDecoder().decode(frame[3]);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            if (!frame[2].isEmpty()) {
                exchange.getResponseHeaders().set("Retry-After", frame[2]);
            }
            if ("trickle".equals(behaviour)) {
                trickle(exchange, status);
                return;
            }
            if ("redirect".equals(behaviour)) {
                exchange.getResponseHeaders().add("Location", "https://babel-peer:19443/foreign");
            } else if ("gzip".equals(behaviour)) {
                ByteArrayOutputStream compressed = new ByteArrayOutputStream();
                try (GZIPOutputStream zip = new GZIPOutputStream(compressed)) {
                    zip.write(payload);
                }
                exchange.getResponseHeaders().set("Content-Encoding", "gzip");
                payload = compressed.toByteArray();
            }
            send(exchange, status, payload);
        } finally {
            exchange.close();
        }
    }

    private void trickle(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, 0);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write('{');
            for (int i = 0; i < 800; i++) {
                out.write(' ');
                out.flush();
                Thread.sleep(50L);
            }
        } catch (IOException disconnected) {
            synchronized (this) {
                log.add("closed\t" + exchange.getRequestURI().getPath());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void send(HttpExchange exchange, int status, byte[] payload) throws IOException {
        exchange.sendResponseHeaders(status, payload.length == 0 ? -1 : payload.length);
        if (payload.length > 0) {
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(payload);
            }
        }
    }
}
