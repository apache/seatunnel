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

package org.apache.seatunnel.e2e.connector.appfollow;

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
 * Serves independent AppFollow HTTPS responses for actual SeaTunnel engine jobs. It verifies the
 * precise v2 target and credential header before recording each request.
 */
public final class AppfollowPeer {
    static final String HOST = "api.appfollow.io";
    static final String TOKEN = "synthetic-appfollow-api-token";
    static final String PASSWORD = "local-fixture-pass";
    static final int TRUSTED_PORT = 18443;
    static final int UNTRUSTED_PORT = 18444;

    private final Path root;
    private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();

    private AppfollowPeer(Path root) {
        this.root = root;
    }

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(args[0]);
        AppfollowPeer peer = new AppfollowPeer(root);
        ExecutorService executor = Executors.newCachedThreadPool();
        HttpsServer trusted = peer.start(TRUSTED_PORT, "trusted", executor);
        HttpsServer untrusted = peer.start(UNTRUSTED_PORT, "untrusted", executor);
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
        char[] password = PASSWORD.toCharArray();
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(root.resolve(identity + ".p12"))) {
            key.load(input, password);
        }
        KeyManagerFactory manager =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(key, password);
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
                    calls.computeIfAbsent(scenario, ignored -> new AtomicInteger())
                            .getAndIncrement();
            if (index >= program.length) {
                invalid(request, scenario);
                return;
            }
            Reply reply = new Reply(program[index]);
            String query = request.getRequestURI().getRawQuery();
            String target =
                    request.getRequestURI().getRawPath() + (query == null ? "" : "?" + query);
            if (!"GET".equals(request.getRequestMethod())
                    || !reply.target.equals(target)
                    || !(HOST + ":" + request.getLocalAddress().getPort())
                            .equals(request.getRequestHeaders().getFirst("Host"))
                    || !TOKEN.equals(request.getRequestHeaders().getFirst("X-AppFollow-API-Token"))
                    || request.getRequestHeaders().getFirst("Authorization") != null) {
                invalid(request, scenario);
                return;
            }
            record(scenario, ".requests", target);
            send(request, scenario, reply);
        } finally {
            request.close();
        }
    }

    private void invalid(HttpExchange request, String scenario) throws IOException {
        record(scenario, ".invalid", "unexpected method, URL, or credentials");
        request.sendResponseHeaders(400, -1);
    }

    private synchronized void record(String scenario, String file, String content)
            throws IOException {
        Files.writeString(
                root.resolve(scenario + file),
                content + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    private void send(HttpExchange request, String scenario, Reply reply) throws IOException {
        if (!reply.retryAfter.isEmpty()) {
            request.getResponseHeaders().set("Retry-After", reply.retryAfter);
        }
        if ("redirect".equals(reply.mode)) {
            request.getResponseHeaders()
                    .set("Location", "https://" + HOST + ":" + UNTRUSTED_PORT + "/foreign");
            request.sendResponseHeaders(302, -1);
            return;
        }
        if ("next".equals(reply.mode)) {
            request.getResponseHeaders()
                    .set("Link", "<https://api.appfollow.io/api/v2/internal>; rel=\"next\"");
        }
        if ("slow".equals(reply.mode)) {
            trickle(request, scenario);
            return;
        }
        if ("hold".equals(reply.mode)) {
            long expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (!Files.exists(root.resolve(scenario + ".release"))
                    && System.nanoTime() - expires < 0) {
                if (!sleep(25)) {
                    return;
                }
            }
        }
        byte[] body = reply.body;
        if ("gzip".equals(reply.mode)) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(buffer)) {
                gzip.write(body);
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
                record(scenario, ".disconnect", "response disconnected");
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
                if (!sleep(40)) {
                    return;
                }
            }
        } catch (IOException disconnected) {
            record(scenario, ".disconnect", "slow response disconnected");
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
     * Associates an independently specified HTTP target with one scripted response. The fixture can
     * hold or trickle a response to place engine lifecycle events.
     */
    private static final class Reply {
        final String target;
        final int status;
        final String mode;
        final String retryAfter;
        final byte[] body;

        Reply(String instruction) {
            String[] fields = instruction.split("\t", -1);
            target = fields[0];
            status = Integer.parseInt(fields[1]);
            mode = fields[2];
            retryAfter = fields[3];
            body = Base64.getDecoder().decode(fields[4]);
        }
    }
}
