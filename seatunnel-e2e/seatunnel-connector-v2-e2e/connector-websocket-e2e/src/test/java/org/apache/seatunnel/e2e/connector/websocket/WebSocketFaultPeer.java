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

package org.apache.seatunnel.e2e.connector.websocket;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.Testcontainers;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** A fixture that can withhold reads after the handshake or reset the underlying TCP connection. */
final class WebSocketFaultPeer implements AutoCloseable {
    private static final String HOST = "host.testcontainers.internal";
    private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    enum Mode {
        STALL,
        RESET_AFTER_UPGRADE,
        RESET_AFTER_MESSAGE
    }

    static final class Scenario {
        final Mode mode;
        final AtomicInteger connections = new AtomicInteger();
        final CountDownLatch upgraded = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        volatile long upgradedAt;
        volatile String message;
        volatile IOException failure;

        Scenario(Mode mode) {
            this.mode = mode;
        }
    }

    private final ServerSocket listener = new ServerSocket();
    private final ExecutorService workers = Executors.newCachedThreadPool();
    private volatile Scenario scenario;

    WebSocketFaultPeer() throws IOException {
        listener.setReceiveBufferSize(1024);
        listener.bind(new InetSocketAddress(0));
        Testcontainers.exposeHostPorts(listener.getLocalPort());
        workers.execute(this::accept);
    }

    String url() {
        return "ws://" + HOST + ":" + listener.getLocalPort() + "/sink";
    }

    void use(Scenario scenario) {
        this.scenario = scenario;
    }

    private void accept() {
        try {
            while (!listener.isClosed()) {
                Socket client = listener.accept();
                Scenario delivery = scenario;
                if (delivery == null) {
                    client.close();
                    continue;
                }
                delivery.connections.incrementAndGet();
                workers.execute(() -> serve(delivery, client));
            }
        } catch (IOException e) {
            if (!listener.isClosed() && scenario != null) {
                scenario.failure = e;
            }
        }
    }

    private void serve(Scenario plan, Socket client) {
        try (Socket socket = client) {
            socket.setSoTimeout(15000);
            socket.setReceiveBufferSize(1024);
            String key = key(socket.getInputStream());
            String response =
                    "HTTP/1.1 101 Switching Protocols\r\n"
                            + "Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: "
                            + acceptHeader(key)
                            + "\r\n\r\n";
            socket.getOutputStream().write(response.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            plan.upgradedAt = System.nanoTime();
            plan.upgraded.countDown();
            if (plan.mode == Mode.STALL) {
                plan.release.await(60, TimeUnit.SECONDS);
            } else if (plan.mode == Mode.RESET_AFTER_MESSAGE) {
                plan.message = readSmallTextFrame(socket.getInputStream());
            }
            socket.setSoLinger(true, 0);
        } catch (IOException e) {
            plan.failure = e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String key(InputStream input) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int end = 0;
        while (bytes.size() < 8192) {
            int next = input.read();
            if (next < 0) {
                break;
            }
            bytes.write(next);
            end = (end << 8) | next;
            if (end == 0x0d0a0d0a) {
                String headers = new String(bytes.toByteArray(), StandardCharsets.US_ASCII);
                for (String header : headers.split("\r\n")) {
                    if (header.regionMatches(true, 0, "Sec-WebSocket-Key:", 0, 18)) {
                        return header.substring(18).trim();
                    }
                }
                break;
            }
        }
        throw new IOException("Fixture did not receive a complete WebSocket opening request");
    }

    private static String acceptHeader(String key) {
        try {
            return Base64.getEncoder()
                    .encodeToString(
                            MessageDigest.getInstance("SHA-1")
                                    .digest((key + GUID).getBytes(StandardCharsets.US_ASCII)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String readSmallTextFrame(InputStream stream) throws IOException {
        DataInputStream input = new DataInputStream(stream);
        int opcode = input.readUnsignedByte();
        int maskedLength = input.readUnsignedByte();
        int length = maskedLength & 127;
        if (opcode != 0x81 || (maskedLength & 128) == 0 || length > 125) {
            throw new IOException("Expected one short, masked text message");
        }
        byte[] mask = new byte[4];
        input.readFully(mask);
        byte[] payload = new byte[length];
        input.readFully(payload);
        for (int index = 0; index < length; index++) {
            payload[index] ^= mask[index % mask.length];
        }
        return new String(payload, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws Exception {
        if (scenario != null) {
            scenario.release.countDown();
        }
        listener.close();
        workers.shutdownNow();
        Assertions.assertTrue(workers.awaitTermination(20, TimeUnit.SECONDS));
    }
}
