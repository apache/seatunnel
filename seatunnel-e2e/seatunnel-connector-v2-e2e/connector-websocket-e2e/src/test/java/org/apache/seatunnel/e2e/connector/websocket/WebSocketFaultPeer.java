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
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides real TCP stall/reset and incomplete incoming-frame scenarios for Sink engine assertions.
 *
 * @see WebSocketSinkIT
 */
final class WebSocketFaultPeer implements AutoCloseable {
    private static final String HOST = "host.testcontainers.internal";
    private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    /**
     * Selects one wire failure independently of the Sink implementation under test.
     *
     * @see WebSocketSinkIT
     */
    enum Mode {
        STALL,
        RESET_AFTER_UPGRADE,
        RESET_AFTER_MESSAGE,
        OVERSIZED_INBOUND,
        FRAGMENTED_INBOUND,
        EXTENSION_INBOUND
    }

    /**
     * Exposes observed connections, challenge transmission and disconnect to the assertion thread.
     */
    static final class Scenario {
        final Mode mode;
        final AtomicInteger connections = new AtomicInteger();
        final AtomicInteger applicationFramesSent = new AtomicInteger();
        final CountDownLatch upgraded = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        volatile long upgradedAt;
        volatile String message;
        volatile IOException failure;
        // Incoming challenges remain incomplete; successful rejection cannot wait for a final
        // message.
        final CountDownLatch challengeSent = new CountDownLatch(1);
        final CountDownLatch disconnected = new CountDownLatch(1);
        volatile boolean extensionOffered;

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
            String key = key(socket.getInputStream(), plan);
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
                sendWhileStalled(plan, socket);
            } else if (plan.mode == Mode.RESET_AFTER_MESSAGE) {
                plan.message = readSmallTextFrame(socket.getInputStream());
            } else if (plan.mode == Mode.OVERSIZED_INBOUND
                    || plan.mode == Mode.FRAGMENTED_INBOUND
                    || plan.mode == Mode.EXTENSION_INBOUND) {
                sendInboundChallenge(plan, socket);
            }
            socket.setSoLinger(true, 0);
        } catch (IOException e) {
            plan.failure = e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /*
     * Withholds all upstream reads while sending harmless complete application frames. They require
     * no control response and must not replace the writer's precise queue/drain deadline diagnostic.
     */
    private static void sendWhileStalled(Scenario plan, Socket socket) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline && plan.release.getCount() != 0) {
            try {
                socket.getOutputStream().write(new byte[] {(byte) 0x81, 2, 'o', 'k'});
                socket.getOutputStream().flush();
                plan.applicationFramesSent.incrementAndGet();
            } catch (IOException ignored) {
                // The timed-out writer closes while this mode is still withholding upstream reads.
                plan.disconnected.countDown();
                return;
            }
            if (plan.release.await(100, TimeUnit.MILLISECONDS)) {
                return;
            }
        }
    }

    /*
     * Sends only an oversized length header, nonterminal fragments, or an unsupported extension bit.
     * The engine must fail without waiting for a completed incoming application message.
     */
    private static void sendInboundChallenge(Scenario plan, Socket socket) throws IOException {
        if (plan.mode == Mode.OVERSIZED_INBOUND) {
            // 129 bytes declared against the case's 128-byte limit; no payload is sent.
            socket.getOutputStream().write(new byte[] {(byte) 0x82, 126, 0, (byte) 129});
        } else if (plan.mode == Mode.FRAGMENTED_INBOUND) {
            byte[] payload = new byte[80];
            Arrays.fill(payload, (byte) 'a');
            socket.getOutputStream().write(new byte[] {2, 80});
            socket.getOutputStream().write(payload);
            socket.getOutputStream().write(new byte[] {0, 80});
            socket.getOutputStream().write(payload);
        } else {
            // RSV1 must be rejected before reading this data: no extension was negotiated.
            socket.getOutputStream().write(new byte[] {(byte) 0xc2, 1});
        }
        socket.getOutputStream().flush();
        plan.challengeSent.countDown();
        try {
            while (socket.getInputStream().read() != -1) {
                // Drain only the client's already-sent data/close frames while observing
                // disconnect.
            }
        } catch (SocketTimeoutException timeout) {
            throw timeout;
        } catch (IOException ignored) {
            // An abortive disconnect is also the expected rejection outcome.
        }
        plan.disconnected.countDown();
    }

    private static String key(InputStream input, Scenario plan) throws IOException {
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
                String key = null;
                for (String header : headers.split("\r\n")) {
                    if (header.regionMatches(true, 0, "Sec-WebSocket-Key:", 0, 18)) {
                        key = header.substring(18).trim();
                    }
                    if (header.regionMatches(true, 0, "Sec-WebSocket-Extensions:", 0, 25)) {
                        plan.extensionOffered = true;
                    }
                }
                if (key != null) {
                    return key;
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
