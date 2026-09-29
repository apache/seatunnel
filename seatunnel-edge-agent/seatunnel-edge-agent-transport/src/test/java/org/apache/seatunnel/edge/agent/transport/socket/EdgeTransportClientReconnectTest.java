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

package org.apache.seatunnel.edge.agent.transport.socket;

import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportConfig;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportConfigTestHelper;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class EdgeTransportClientReconnectTest {

    @Test
    void sendReconnectsWhenFirstSessionFails() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            int port = serverSocket.getLocalPort();
            AtomicReference<Throwable> serverFailure = new AtomicReference<>();
            Thread serverThread =
                    new Thread(
                            () -> {
                                try {
                                    serveFirstSessionDropsAfterAuth(serverSocket.accept());
                                    serveSuccessfulReceived(serverSocket.accept());
                                } catch (Throwable ex) {
                                    serverFailure.set(ex);
                                }
                            },
                            "edge-ingress-test-server");
            serverThread.setDaemon(true);
            serverThread.start();

            EdgeTransportConfig config = reconnectClientConfig("127.0.0.1:" + port, "secret");
            EdgeTransportClient client =
                    new EdgeTransportClient(config, EdgeSocketSocketFactory.DEFAULT);
            client.sendUntilReceived(11L, "{}");

            serverThread.join(30_000);
            Assertions.assertFalse(serverThread.isAlive(), "server thread still running");
            Assertions.assertNull(serverFailure.get(), describe(serverFailure.get()));
            client.close();
        }
    }

    @Test
    void openFailsFastOnRejectedWithoutReconnectStorm() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            int port = serverSocket.getLocalPort();
            AtomicInteger acceptCount = new AtomicInteger();
            AtomicReference<Throwable> serverFailure = new AtomicReference<>();
            Thread serverThread =
                    new Thread(
                            () -> {
                                try {
                                    serveRejectedOnAuth(serverSocket.accept());
                                    acceptCount.incrementAndGet();
                                } catch (Throwable ex) {
                                    serverFailure.set(ex);
                                }
                            },
                            "edge-ingress-rejected-server");
            serverThread.setDaemon(true);
            serverThread.start();

            EdgeTransportConfig config = reconnectClientConfig("127.0.0.1:" + port, "token");
            EdgeTransportClient client =
                    new EdgeTransportClient(config, EdgeSocketSocketFactory.DEFAULT);
            Assertions.assertThrows(EdgeSocketCollectorRejectedException.class, client::open);

            serverThread.join(30_000);
            Assertions.assertFalse(serverThread.isAlive(), "server thread still running");
            Assertions.assertNull(serverFailure.get(), describe(serverFailure.get()));
            Assertions.assertEquals(
                    1, acceptCount.get(), "REJECTED must not trigger reconnect storm");
            client.close();
        }
    }

    @Test
    void socketFactoryTransientFailureStillOpensAuthenticatedSession() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            int port = serverSocket.getLocalPort();
            AtomicReference<Throwable> serverFailure = new AtomicReference<>();
            Thread serverThread =
                    new Thread(
                            () -> {
                                try {
                                    serveAuthenticateAckOnly(serverSocket.accept());
                                } catch (Throwable ex) {
                                    serverFailure.set(ex);
                                }
                            },
                            "edge-ingress-flaky-server");
            serverThread.setDaemon(true);
            serverThread.start();

            AtomicInteger connectAttempts = new AtomicInteger();
            EdgeSocketSocketFactory flakyFactory =
                    (address, timeoutMs) -> {
                        if (connectAttempts.incrementAndGet() == 1) {
                            throw new IOException("simulated connect failure");
                        }
                        return EdgeSocketSocketFactory.DEFAULT.connect(address, timeoutMs);
                    };

            EdgeTransportConfig config = reconnectClientConfig("127.0.0.1:" + port, "token");
            EdgeTransportClient client = new EdgeTransportClient(config, flakyFactory);
            client.open();

            serverThread.join(30_000);
            Assertions.assertFalse(serverThread.isAlive(), "server thread still running");
            Assertions.assertNull(serverFailure.get(), describe(serverFailure.get()));
            Assertions.assertTrue(
                    connectAttempts.get() >= 2,
                    "expected at least one failed connect before success");
            client.close();
        }
    }

    private static void serveFirstSessionDropsAfterAuth(Socket socket) throws IOException {
        try (Socket autoClose = socket) {
            BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    autoClose.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter writer =
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    autoClose.getOutputStream(), StandardCharsets.UTF_8));
            expectPrefix(reader, EdgeSocketProtocol.AUTH_LINE_PREFIX);
            writeLine(writer, EdgeSocketProtocol.RESP_ACK);
            expectPrefix(reader, EdgeSocketProtocol.BATCH_PREFIX);
            autoClose.shutdownOutput();
        }
    }

    private static void serveRejectedOnAuth(Socket socket) throws IOException {
        try (Socket autoClose = socket) {
            BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    autoClose.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter writer =
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    autoClose.getOutputStream(), StandardCharsets.UTF_8));
            expectPrefix(reader, EdgeSocketProtocol.AUTH_LINE_PREFIX);
            writeLine(writer, EdgeSocketProtocol.RESP_REJECTED);
        }
    }

    private static void serveAuthenticateAckOnly(Socket socket) throws IOException {
        try (Socket autoClose = socket) {
            BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    autoClose.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter writer =
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    autoClose.getOutputStream(), StandardCharsets.UTF_8));
            expectPrefix(reader, EdgeSocketProtocol.AUTH_LINE_PREFIX);
            writeLine(writer, EdgeSocketProtocol.RESP_ACK);
        }
    }

    private static void serveSuccessfulReceived(Socket socket) throws IOException {
        try (Socket autoClose = socket) {
            BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    autoClose.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter writer =
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    autoClose.getOutputStream(), StandardCharsets.UTF_8));
            expectPrefix(reader, EdgeSocketProtocol.AUTH_LINE_PREFIX);
            writeLine(writer, EdgeSocketProtocol.RESP_ACK);
            expectPrefix(reader, EdgeSocketProtocol.BATCH_PREFIX);
            writeLine(writer, EdgeSocketProtocol.RESP_RECEIVED);
        }
    }

    private static void expectPrefix(BufferedReader reader, String prefix) throws IOException {
        String line = reader.readLine();
        Assertions.assertNotNull(line, "EOF before expected line prefix=" + prefix);
        Assertions.assertTrue(
                line.startsWith(prefix),
                () -> "unexpected ingress line=" + line + " expected prefix=" + prefix);
    }

    private static void writeLine(BufferedWriter writer, String value) throws IOException {
        writer.write(value);
        writer.newLine();
        writer.flush();
    }

    private static String describe(Throwable throwable) {
        return throwable == null ? "" : throwable.toString();
    }

    private static EdgeTransportConfig reconnectClientConfig(String endpoint, String token) {
        Map<String, Object> overrides = new HashMap<>();
        overrides.put(EdgeTransportOptions.CONNECT_TIMEOUT_MS.key(), 10_000);
        overrides.put(EdgeTransportOptions.READ_TIMEOUT_MS.key(), 10_000);
        overrides.put(EdgeTransportOptions.MAX_BATCH_SEND_ATTEMPTS.key(), 8);
        overrides.put(EdgeTransportOptions.INITIAL_BACKOFF_MS.key(), 1L);
        overrides.put(EdgeTransportOptions.MAX_BACKOFF_MS.key(), 8L);
        overrides.put(EdgeTransportOptions.MAX_RECONNECT_CYCLES.key(), 8);
        return EdgeTransportConfigTestHelper.config(endpoint, token, overrides);
    }
}
