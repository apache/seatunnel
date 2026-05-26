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
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorException;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class EdgeSocketSourceReaderTest {

    @Test
    void shouldAckBatchAfterCheckpointComplete() throws Exception {
        int port = allocateFreePort();
        try (EdgeSocketSourceReader reader = createReader(port, 5)) {
            reader.open();
            awaitServerReady(reader);
            try (Socket socket = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals("ACK", readLine(bufferedReader));

                writeLine(writer, "__BATCH__:1:message-1");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));

                writeLine(writer, "__COMMIT__:1");
                Assertions.assertEquals("PENDING", readLine(bufferedReader));

                TestCollector collector = new TestCollector();
                reader.pollNext(collector);
                Assertions.assertEquals(1, collector.rows.size());

                reader.snapshotState(1L);
                reader.notifyCheckpointComplete(1L);

                writeLine(writer, "__COMMIT__:1");
                String ackReply = readLine(bufferedReader);
                Assertions.assertTrue(ackReply.startsWith("ACK:"));
                Assertions.assertTrue(Long.parseLong(ackReply.substring("ACK:".length())) >= 1L);
            }
        }
    }

    @Test
    void shouldFailWhenRetryBudgetExhausted() throws Exception {
        int port = allocateFreePort();
        try (ServerSocket blocked = new ServerSocket(port);
                EdgeSocketSourceReader reader = createReader(port, 0)) {
            reader.open();
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                EdgeSocketConnectorException caught =
                                        Assertions.assertThrows(
                                                EdgeSocketConnectorException.class,
                                                () -> reader.pollNext(new TestCollector()));
                                Assertions.assertEquals(
                                        EdgeSocketConnectorErrorCode.SOURCE_REOPEN_EXHAUSTED,
                                        caught.getSeaTunnelErrorCode());
                            });
        }
    }

    /**
     * Simulates a worker restart: send a batch, snapshot before pollNext (record still in queue),
     * then restore the snapshot into a fresh reader. The restored reader must be able to emit the
     * queued record without receiving it again from the collector, proving At-Least-Once delivery
     * for in-queue records across restarts.
     */
    @Test
    void shouldReplayQueuedRecordsAfterRestore() throws Exception {
        int port = allocateFreePort();
        EdgeSocketSourceReader reader = createReader(port, 5);
        List<SingleSplit> snapshot;
        try {
            reader.open();
            awaitServerReady(reader);
            try (Socket socket = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals("ACK", readLine(bufferedReader));

                writeLine(writer, "__BATCH__:1:restore-payload");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));

                snapshot = reader.snapshotState(1L);
            }
        } finally {
            reader.close();
        }

        int newPort = allocateFreePort();
        EdgeSocketSourceReader restored = createReader(newPort, 5);
        restored.addSplits(snapshot);
        restored.open();
        try {
            TestCollector collector = new TestCollector();
            restored.pollNext(collector);
            Assertions.assertEquals(
                    1, collector.rows.size(), "Restored queue record must be replayed");
            Object value = collector.rows.get(0).getField(0);
            Assertions.assertEquals(
                    "restore-payload", value, "Replayed record payload must match original");
        } finally {
            restored.close();
        }
    }

    /**
     * When the local queue reaches the backpressure watermark, the source must respond
     * QUEUE_FULL:ms; without accepting more records. After pollNext drains one slot, the next send
     * must succeed with RECEIVED.
     */
    @Test
    void shouldRespondQueueFullWhenAtHighWatermark() throws Exception {
        int port = allocateFreePort();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), port);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "edge-test-token");
        configMap.put(EdgeSocketSourceOptions.MAX_RETRIES.key(), 5);
        configMap.put(EdgeSocketSourceOptions.RECONNECT_INTERVAL_MS.key(), 50);
        configMap.put(EdgeSocketSourceOptions.ACCEPT_TIMEOUT_MS.key(), 100);
        configMap.put(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY.key(), 1);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        SourceReader.Context ctx = Mockito.mock(SourceReader.Context.class);
        Mockito.when(ctx.getBoundedness()).thenReturn(Boundedness.UNBOUNDED);
        SingleSplitReaderContext readerContext = new SingleSplitReaderContext(ctx);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        try (EdgeSocketSourceReader reader =
                new EdgeSocketSourceReader(
                        new EdgeSocketConfig(config),
                        readerContext,
                        new EdgeSocketTextDeserializationSchema(rowType))) {
            reader.open();
            awaitServerReady(reader);
            try (Socket socket = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals("ACK", readLine(bufferedReader));

                writeLine(writer, "__BATCH__:1:record-first");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));

                writeLine(writer, "__BATCH__:2:record-second");
                Assertions.assertEquals("QUEUE_FULL:500", readLine(bufferedReader));

                TestCollector collector = new TestCollector();
                reader.pollNext(collector);
                Assertions.assertEquals(1, collector.rows.size());

                writeLine(writer, "__BATCH__:2:record-second");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));
            }
        }
    }

    /**
     * Snapshot after partially draining a multi-batch queue, then restore into a fresh reader.
     * Verifies that watermark, pendingBatchRecordCounts, and un-emitted records are all correct.
     */
    @Test
    void shouldRestoreSnapshotWithPartiallyDrainedQueue() throws Exception {
        int port = allocateFreePort();
        EdgeSocketSourceReader reader = createReader(port, 5);
        List<SingleSplit> snapshot;
        try {
            reader.open();
            awaitServerReady(reader);
            try (Socket socket = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals("ACK", readLine(bufferedReader));

                writeLine(writer, "__BATCH__:10:batch10-msg1");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));
                writeLine(writer, "__BATCH__:10:batch10-msg2");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));
                writeLine(writer, "__BATCH__:11:batch11-msg1");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));

                TestCollector collector = new TestCollector();
                reader.pollNext(collector);
                reader.pollNext(collector);
                Assertions.assertEquals(2, collector.rows.size());

                snapshot = reader.snapshotState(42L);
            }
        } finally {
            reader.close();
        }

        int newPort = allocateFreePort();
        EdgeSocketSourceReader restored = createReader(newPort, 5);
        restored.addSplits(snapshot);
        restored.open();
        try {
            TestCollector collector = new TestCollector();
            restored.pollNext(collector);
            Assertions.assertEquals(1, collector.rows.size(), "batch11 record must be replayed");
            Assertions.assertEquals(
                    "batch11-msg1",
                    collector.rows.get(0).getField(0),
                    "Replayed record must match original payload");

            restored.notifyCheckpointComplete(42L);
        } finally {
            restored.close();
        }
    }

    /**
     * While one collector is connected, the server socket is suspended so that a second connection
     * attempt is refused at TCP level ({@link ConnectException}). If the second connection were to
     * slip through the TCP backlog race window, it would receive a {@code REJECTED} response at
     * application level. After the first collector disconnects, the server socket reopens and a new
     * collector can connect normally.
     */
    @Test
    void shouldRefuseSecondCollectorWhileFirstIsConnected() throws Exception {
        int port = allocateFreePort();
        try (EdgeSocketSourceReader reader = createReader(port, 5)) {
            reader.open();
            awaitServerReady(reader);
            try (Socket first = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            first.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            first.getInputStream(), StandardCharsets.UTF_8))) {
                first.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals("ACK", readLine(bufferedReader));

                Awaitility.await()
                        .atMost(15, TimeUnit.SECONDS)
                        .pollInterval(100, TimeUnit.MILLISECONDS)
                        .until(() -> !reader.isListening());
                Assertions.assertThrows(
                        ConnectException.class,
                        () -> new Socket("127.0.0.1", port),
                        "Second collector must be refused while first is connected");
            }

            awaitServerReady(reader);
            try (Socket second = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            second.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            second.getInputStream(), StandardCharsets.UTF_8))) {
                second.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals(
                        "ACK",
                        readLine(bufferedReader),
                        "New collector must be accepted after previous one disconnects");
            }
        }
    }

    /** Sending a wrong auth token must result in AUTH_FAILED response and closed connection. */
    @Test
    void shouldRejectWrongAuthToken() throws Exception {
        int port = allocateFreePort();
        try (EdgeSocketSourceReader reader = createReader(port, 5)) {
            reader.open();
            awaitServerReady(reader);
            try (Socket socket = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:wrong-token");
                String reply = readLine(bufferedReader);
                Assertions.assertEquals(
                        "AUTH_FAILED", reply, "Server must reject invalid token with AUTH_FAILED");
                Assertions.assertNull(
                        bufferedReader.readLine(),
                        "Server must close the connection after authentication failure");
            }
        }
    }

    /**
     * After restore from a snapshot that contains a batch-ID gap (1,2,3,5 — missing 4), a {@code
     * __COMMIT__:4} must receive {@code RESEND} because batch 4 was never received in the original
     * session and has not been re-sent after restore.
     */
    @Test
    void shouldReturnResendForGappedBatchAfterRestore() throws Exception {
        int port = allocateFreePort();
        EdgeSocketSourceReader reader = createReader(port, 5);
        List<SingleSplit> snapshot;
        try {
            reader.open();
            awaitServerReady(reader);
            try (Socket socket = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals("ACK", readLine(bufferedReader));

                writeLine(writer, "__BATCH__:1:msg-1");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));
                writeLine(writer, "__BATCH__:2:msg-2");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));
                writeLine(writer, "__BATCH__:3:msg-3");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));
                writeLine(writer, "__BATCH__:5:msg-5");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));

                TestCollector collector = new TestCollector();
                reader.pollNext(collector);
                reader.pollNext(collector);
                reader.pollNext(collector);
                reader.pollNext(collector);
                Assertions.assertEquals(4, collector.rows.size());

                reader.snapshotState(1L);
                reader.notifyCheckpointComplete(1L);

                snapshot = reader.snapshotState(2L);
            }
        } finally {
            reader.close();
        }

        int newPort = allocateFreePort();
        EdgeSocketSourceReader restored = createReader(newPort, 5);
        restored.addSplits(snapshot);
        restored.open();
        try {
            awaitServerReady(restored);
            try (Socket socket = new Socket("127.0.0.1", newPort);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals("ACK", readLine(bufferedReader));

                writeLine(writer, "__COMMIT__:4");
                Assertions.assertEquals(
                        "RESEND",
                        readLine(bufferedReader),
                        "Batch 4 was never received — source must request RESEND");
            }
        } finally {
            restored.close();
        }
    }

    @Test
    void shouldReturnDecodeFailedForInvalidPacketPayload() throws Exception {
        int port = allocateFreePort();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), port);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "edge-test-token");
        configMap.put(EdgeSocketSourceOptions.MAX_RETRIES.key(), 5);
        configMap.put(EdgeSocketSourceOptions.RECONNECT_INTERVAL_MS.key(), 50);
        configMap.put(EdgeSocketSourceOptions.ACCEPT_TIMEOUT_MS.key(), 100);
        configMap.put(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY.key(), 8);
        configMap.put(EdgeSocketSourceOptions.PACKET_MODE.key(), "PACKET");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        SourceReader.Context ctx = Mockito.mock(SourceReader.Context.class);
        Mockito.when(ctx.getBoundedness()).thenReturn(Boundedness.UNBOUNDED);
        SingleSplitReaderContext readerContext = new SingleSplitReaderContext(ctx);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        try (EdgeSocketSourceReader reader =
                new EdgeSocketSourceReader(
                        new EdgeSocketConfig(config),
                        readerContext,
                        new EdgeSocketTextDeserializationSchema(rowType))) {
            reader.open();
            awaitServerReady(reader);
            try (Socket socket = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals("ACK", readLine(bufferedReader));

                writeLine(writer, "__BATCH__:1:this-is-not-valid-json");
                Assertions.assertEquals("DECODE_FAILED", readLine(bufferedReader));
            }
        }
    }

    /**
     * PACKET mode with AES_GCM encryption configured. Sending a packet whose ciphertext is tampered
     * (too short for a valid GCM auth-tag) must trigger a {@link
     * java.security.GeneralSecurityException} inside the deserializer, which the reader maps to
     * {@code DECRYPT_FAILED}.
     */
    @Test
    void shouldReturnDecryptFailedForTamperedAesPayload() throws Exception {
        int port = allocateFreePort();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), port);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "edge-test-token");
        configMap.put(EdgeSocketSourceOptions.MAX_RETRIES.key(), 5);
        configMap.put(EdgeSocketSourceOptions.RECONNECT_INTERVAL_MS.key(), 50);
        configMap.put(EdgeSocketSourceOptions.ACCEPT_TIMEOUT_MS.key(), 100);
        configMap.put(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY.key(), 8);
        configMap.put(EdgeSocketSourceOptions.PACKET_MODE.key(), "PACKET");
        configMap.put(
                EdgeSocketSourceOptions.SECRET_KEY.key(),
                java.util.Base64.getEncoder().encodeToString(new byte[32]));
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        SourceReader.Context ctx = Mockito.mock(SourceReader.Context.class);
        Mockito.when(ctx.getBoundedness()).thenReturn(Boundedness.UNBOUNDED);
        SingleSplitReaderContext readerContext = new SingleSplitReaderContext(ctx);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        try (EdgeSocketSourceReader reader =
                new EdgeSocketSourceReader(
                        new EdgeSocketConfig(config),
                        readerContext,
                        new EdgeSocketTextDeserializationSchema(rowType))) {
            reader.open();
            awaitServerReady(reader);
            try (Socket socket = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals("ACK", readLine(bufferedReader));

                String ivB64 = java.util.Base64.getEncoder().encodeToString(new byte[12]);
                String packetJson =
                        "{\"payload\":\"dGVzdA==\",\"encryption\":\"aes_gcm\",\"iv\":\""
                                + ivB64
                                + "\"}";
                writeLine(writer, "__BATCH__:1:" + packetJson);
                Assertions.assertEquals("DECRYPT_FAILED", readLine(bufferedReader));
            }
        }
    }

    /**
     * PACKET mode without a configured {@code secret_key}. When the incoming packet declares {@code
     * encryption=aes_gcm}, the deserializer throws {@code PACKET_AES_KEY_MISSING}, which the reader
     * maps to {@code DECRYPT_FAILED}.
     */
    @Test
    void shouldReturnDecryptFailedWhenSecretKeyMissing() throws Exception {
        int port = allocateFreePort();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), port);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "edge-test-token");
        configMap.put(EdgeSocketSourceOptions.MAX_RETRIES.key(), 5);
        configMap.put(EdgeSocketSourceOptions.RECONNECT_INTERVAL_MS.key(), 50);
        configMap.put(EdgeSocketSourceOptions.ACCEPT_TIMEOUT_MS.key(), 100);
        configMap.put(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY.key(), 8);
        configMap.put(EdgeSocketSourceOptions.PACKET_MODE.key(), "PACKET");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        SourceReader.Context ctx = Mockito.mock(SourceReader.Context.class);
        Mockito.when(ctx.getBoundedness()).thenReturn(Boundedness.UNBOUNDED);
        SingleSplitReaderContext readerContext = new SingleSplitReaderContext(ctx);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        try (EdgeSocketSourceReader reader =
                new EdgeSocketSourceReader(
                        new EdgeSocketConfig(config),
                        readerContext,
                        new EdgeSocketTextDeserializationSchema(rowType))) {
            reader.open();
            awaitServerReady(reader);
            try (Socket socket = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals("ACK", readLine(bufferedReader));

                String packetJson =
                        "{\"payload\":\"dGVzdA==\",\"encryption\":\"aes_gcm\","
                                + "\"iv\":\"AAAAAAAAAAAAAAAA\"}";
                writeLine(writer, "__BATCH__:1:" + packetJson);
                Assertions.assertEquals("DECRYPT_FAILED", readLine(bufferedReader));
            }
        }
    }

    private EdgeSocketSourceReader createReader(int port, int maxRetries) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), port);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "edge-test-token");
        configMap.put(EdgeSocketSourceOptions.MAX_RETRIES.key(), maxRetries);
        configMap.put(EdgeSocketSourceOptions.RECONNECT_INTERVAL_MS.key(), 50);
        configMap.put(EdgeSocketSourceOptions.ACCEPT_TIMEOUT_MS.key(), 100);
        configMap.put(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY.key(), 8);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getBoundedness()).thenReturn(Boundedness.UNBOUNDED);
        SingleSplitReaderContext readerContext = new SingleSplitReaderContext(context);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        return new EdgeSocketSourceReader(
                new org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig(
                        config),
                readerContext,
                new EdgeSocketTextDeserializationSchema(rowType));
    }

    private static int allocateFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * Polls {@link EdgeSocketSourceReader#isListening()} until the server socket is bound and
     * accepting connections. This avoids creating a real TCP connection that the server would treat
     * as a collector, which would interfere with the single-collector enforcement logic.
     */
    private static void awaitServerReady(EdgeSocketSourceReader reader) {
        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(reader::isListening);
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

    private static class TestCollector implements Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow row) {
            rows.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }
}
