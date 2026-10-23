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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorException;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.protocol.EdgeSocketResponseCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.protocol.IncomingRecordHandler;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

class EdgeSocketIngressServerTest {

    @Test
    void startAndStopLifecycle() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);

        Assertions.assertFalse(server.isListening());
        server.start();
        awaitListening(server);
        Assertions.assertTrue(server.isListening());

        server.stop();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !server.isListening());
        Assertions.assertFalse(server.isListening());
    }

    @Test
    void duplicateStartIsIgnored() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);

        server.start();
        awaitListening(server);
        server.start();
        Assertions.assertTrue(server.isListening());

        server.stop();
    }

    @Test
    void authenticatesCollectorWithCorrectToken() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));
        } finally {
            server.stop();
        }
    }

    @Test
    void rejectsWrongToken() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:wrong-token");
            Assertions.assertEquals("AUTH_FAILED", readLine(reader));
            Assertions.assertNull(reader.readLine());
        } finally {
            server.stop();
        }
    }

    @Test
    void rejectsAuthWithoutPrefix() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "wrong-token-no-prefix");
            Assertions.assertEquals("AUTH_FAILED", readLine(reader));
        } finally {
            server.stop();
        }
    }

    @Test
    void dispatchesBatchRecordToHandler() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            writeLine(writer, "__BATCH__:42:payload-data");
            Assertions.assertEquals("RECEIVED", readLine(reader));

            Assertions.assertEquals(1, handler.batchRecords.size());
            Assertions.assertEquals(42L, handler.batchRecords.get(0).batchId);
            Assertions.assertEquals("payload-data", handler.batchRecords.get(0).payload);
        } finally {
            server.stop();
        }
    }

    @Test
    void dispatchesCommitRequestToHandler() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        handler.commitResponse = "ACK:10";
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            writeLine(writer, "__COMMIT__:5");
            Assertions.assertEquals("ACK:10", readLine(reader));

            Assertions.assertEquals(1, handler.commitRequests.size());
            Assertions.assertEquals(5L, (long) handler.commitRequests.get(0));
        } finally {
            server.stop();
        }
    }

    @Test
    void rejectsMalformedBatchRequest() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            writeLine(writer, "__BATCH__:notanumber:data");
            Assertions.assertEquals("INVALID_PARAM", readLine(reader));

            writeLine(writer, "__BATCH__:1");
            Assertions.assertEquals("BAD_REQUEST", readLine(reader));

            writeLine(writer, "UNKNOWN_PREFIX:something");
            Assertions.assertEquals("BAD_REQUEST", readLine(reader));

            Assertions.assertTrue(handler.batchRecords.isEmpty());
        } finally {
            server.stop();
        }
    }

    @Test
    void rejectsInvalidCommitBatchId() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            writeLine(writer, "__COMMIT__:abc");
            Assertions.assertEquals("INVALID_PARAM", readLine(reader));

            writeLine(writer, "__COMMIT__:0");
            Assertions.assertEquals("INVALID_PARAM", readLine(reader));

            writeLine(writer, "__COMMIT__:-1");
            Assertions.assertEquals("INVALID_PARAM", readLine(reader));

            Assertions.assertTrue(handler.commitRequests.isEmpty());
        } finally {
            server.stop();
        }
    }

    @Test
    void rejectsAuthCommandAfterAuthentication() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("BAD_REQUEST", readLine(reader));
        } finally {
            server.stop();
        }
    }

    @Test
    void singleCollectorEnforcement() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket first = connect(port);
                BufferedWriter writer = writer(first);
                BufferedReader reader = reader(first)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .pollInterval(50, TimeUnit.MILLISECONDS)
                    .until(() -> !server.isListening());

            Assertions.assertThrows(ConnectException.class, () -> new Socket("127.0.0.1", port));
        }

        awaitListening(server);
        try (Socket second = connect(port);
                BufferedWriter writer = writer(second);
                BufferedReader reader = reader(second)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));
        } finally {
            server.stop();
        }
    }

    @Test
    void serverReopensAfterCollectorDisconnects() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));
        }

        awaitListening(server);
        Assertions.assertTrue(server.isListening());
        server.stop();
    }

    @Test
    void fatalExceptionPropagatedViaRethrow() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();

        try (ServerSocket blocker = new ServerSocket(port)) {
            EdgeSocketIngressServer server = createServer(port, 0, handler);
            server.start();

            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                EdgeSocketConnectorException ex =
                                        Assertions.assertThrows(
                                                EdgeSocketConnectorException.class,
                                                server::rethrowFatalIfNeeded);
                                Assertions.assertEquals(
                                        EdgeSocketConnectorErrorCode.SOURCE_REOPEN_EXHAUSTED,
                                        ex.getSeaTunnelErrorCode());
                            });
            server.stop();
        }
    }

    @Test
    void handlesMultipleBatchesInSequence() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            for (int i = 1; i <= 10; i++) {
                writeLine(writer, "__BATCH__:" + i + ":msg-" + i);
                Assertions.assertEquals("RECEIVED", readLine(reader));
            }

            Assertions.assertEquals(10, handler.batchRecords.size());
            for (int i = 0; i < 10; i++) {
                Assertions.assertEquals(i + 1, handler.batchRecords.get(i).batchId);
                Assertions.assertEquals("msg-" + (i + 1), handler.batchRecords.get(i).payload);
            }
        } finally {
            server.stop();
        }
    }

    @Test
    void handlerReturnValueSentAsResponse() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        handler.batchResponse = "DECRYPT_FAILED";
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            writeLine(writer, "__BATCH__:1:encrypted-data");
            Assertions.assertEquals("DECRYPT_FAILED", readLine(reader));
        } finally {
            server.stop();
        }
    }

    @Test
    void commitResendResponsePassthrough() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        handler.commitResponse = EdgeSocketResponseCode.RESEND.getCode();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            writeLine(writer, "__COMMIT__:1");
            Assertions.assertEquals("RESEND", readLine(reader));
        } finally {
            server.stop();
        }
    }

    @Test
    void batchQueueFullResponsePassthrough() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        handler.batchResponse = EdgeSocketResponseCode.QUEUE_FULL.withPayload(500);
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            writeLine(writer, "__BATCH__:1:data");
            Assertions.assertEquals("QUEUE_FULL:500", readLine(reader));
        } finally {
            server.stop();
        }
    }

    @Test
    void batchDecodeFailedResponsePassthrough() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        handler.batchResponse = EdgeSocketResponseCode.DECODE_FAILED.getCode();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            writeLine(writer, "__BATCH__:1:data");
            Assertions.assertEquals("DECODE_FAILED", readLine(reader));
        } finally {
            server.stop();
        }
    }

    @Test
    void commitRetryResponsePassthrough() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        handler.commitResponse = EdgeSocketResponseCode.RETRY.getCode();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            writeLine(writer, "__COMMIT__:1");
            Assertions.assertEquals("RETRY", readLine(reader));
        } finally {
            server.stop();
        }
    }

    @Test
    void handlesCarriageReturnInProtocol() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket socket = connect(port);
                BufferedWriter writer = writer(socket);
                BufferedReader reader = reader(socket)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));

            writer.write("__BATCH__:1:payload-with-cr\r\n");
            writer.flush();
            Assertions.assertEquals("RECEIVED", readLine(reader));

            Assertions.assertEquals("payload-with-cr", handler.batchRecords.get(0).payload);
        } finally {
            server.stop();
        }
    }

    @Test
    void reconnectsAfterAuthFailure() throws Exception {
        int port = allocateFreePort();
        RecordingHandler handler = new RecordingHandler();
        EdgeSocketIngressServer server = createServer(port, 5, handler);
        server.start();
        awaitListening(server);

        try (Socket bad = connect(port);
                BufferedWriter writer = writer(bad);
                BufferedReader reader = reader(bad)) {
            writeLine(writer, "__AUTH__:bad-token");
            Assertions.assertEquals("AUTH_FAILED", readLine(reader));
        }

        awaitListening(server);
        try (Socket good = connect(port);
                BufferedWriter writer = writer(good);
                BufferedReader reader = reader(good)) {
            writeLine(writer, "__AUTH__:test-token-123");
            Assertions.assertEquals("ACK", readLine(reader));
        } finally {
            server.stop();
        }
    }

    private EdgeSocketIngressServer createServer(
            int port, int maxRetries, IncomingRecordHandler handler) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), port);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "test-token-123");
        configMap.put(EdgeSocketSourceOptions.MAX_RETRIES.key(), maxRetries);
        configMap.put(EdgeSocketSourceOptions.RECONNECT_INTERVAL_MS.key(), 50);
        configMap.put(EdgeSocketSourceOptions.ACCEPT_TIMEOUT_MS.key(), 100);
        configMap.put(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY.key(), 8);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        return new EdgeSocketIngressServer(new EdgeSocketConfig(config), handler);
    }

    private static int allocateFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void awaitListening(EdgeSocketIngressServer server) {
        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .until(server::isListening);
    }

    private static Socket connect(int port) throws IOException {
        Socket socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(3000);
        return socket;
    }

    private static BufferedWriter writer(Socket socket) throws IOException {
        return new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
    }

    private static BufferedReader reader(Socket socket) throws IOException {
        return new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
    }

    private static void writeLine(BufferedWriter writer, String value) throws IOException {
        writer.write(value);
        writer.newLine();
        writer.flush();
    }

    private static String readLine(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            throw new IOException("Read EOF");
        }
        return line.trim();
    }

    private static class RecordingHandler implements IncomingRecordHandler {

        final CopyOnWriteArrayList<BatchRecord> batchRecords = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<Long> commitRequests = new CopyOnWriteArrayList<>();
        volatile String batchResponse = EdgeSocketResponseCode.RECEIVED.getCode();
        volatile String commitResponse = EdgeSocketResponseCode.PENDING.getCode();

        @Override
        public String handleBatchRecord(long batchId, String payload) {
            batchRecords.add(new BatchRecord(batchId, payload));
            return batchResponse;
        }

        @Override
        public String handleCommitRequest(long batchId) {
            commitRequests.add(batchId);
            return commitResponse;
        }

        static class BatchRecord {
            final long batchId;
            final String payload;

            BatchRecord(long batchId, String payload) {
                this.batchId = batchId;
                this.payload = payload;
            }
        }
    }
}
