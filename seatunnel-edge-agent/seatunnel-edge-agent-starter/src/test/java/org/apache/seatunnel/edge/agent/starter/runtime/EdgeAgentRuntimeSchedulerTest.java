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

package org.apache.seatunnel.edge.agent.starter.runtime;

import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.connector.EdgeInputReader;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePosition;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore;
import org.apache.seatunnel.edge.agent.starter.config.AgentRuntimeConfig;
import org.apache.seatunnel.edge.agent.starter.parse.EdgeAgentConfigLoader;
import org.apache.seatunnel.edge.agent.starter.parse.EdgeAgentResolvedConfig;
import org.apache.seatunnel.edge.agent.starter.wal.WalRecord;
import org.apache.seatunnel.edge.agent.starter.wal.WalRecordStatus;
import org.apache.seatunnel.edge.agent.starter.wal.WalStore;
import org.apache.seatunnel.edge.agent.starter.wal.mem.MemWalStore;
import org.apache.seatunnel.edge.agent.transport.EdgeCollectorTransport;
import org.apache.seatunnel.edge.agent.transport.serialize.RawPayloadSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class EdgeAgentRuntimeSchedulerTest {

    @TempDir Path tempDir;

    @Test
    void schedulerUsesWalRecordIdAsTransportBatchIdAndAcksAfterSend() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(
                EdgeEvent.builder()
                        .sourceId("file-input")
                        .payload("hello".getBytes(StandardCharsets.UTF_8))
                        .eventTime(1L)
                        .sourcePosition(
                                EdgeSourcePosition.builder()
                                        .sourceId("file-input")
                                        .partition("/var/log/app.log")
                                        .offset(1024L)
                                        .updatedAt(2L)
                                        .build())
                        .build());
        FakeSourcePositionStore positionStore = new FakeSourcePositionStore();
        FakeWalStore walStore = new FakeWalStore(positionStore);
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadRuntimeConfigWithBulkSize(tempDir.resolve("wal.db"), 1);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        walStore,
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        Assertions.assertTrue(scheduler.runOnce());
        Assertions.assertEquals(1L, walStore.appendedIds.get(0));
        Assertions.assertEquals(1L, transport.lastBatchId);
        Assertions.assertEquals("hello", transport.lastPayload);
        Assertions.assertEquals(1L, walStore.ackedIds.get(0));
        Assertions.assertEquals(1024L, positionStore.saved.get(0).getOffset());
    }

    @Test
    void schedulerSurvivesRecoverableSendFailureWithoutAck() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(
                EdgeEvent.builder()
                        .sourceId("file-input")
                        .payload("x".getBytes(StandardCharsets.UTF_8))
                        .eventTime(1L)
                        .build());
        FakeWalStore walStore = new FakeWalStore();
        FakeTransport transport = new FakeTransport();
        transport.failNextSend = true;

        AgentRuntimeConfig config = loadRuntimeConfigWithBulkSize(tempDir.resolve("wal.db"), 1);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        walStore,
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        Assertions.assertTrue(scheduler.runOnce());
        Assertions.assertTrue(walStore.ackedIds.isEmpty());
    }

    @Test
    void nonModeSendSucceedsWithMemWal() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(
                EdgeEvent.builder()
                        .sourceId("file-input")
                        .payload("hello-non".getBytes(StandardCharsets.UTF_8))
                        .eventTime(1L)
                        .build());
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadNonModeConfig();
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        new MemWalStore(),
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        Assertions.assertTrue(scheduler.runOnce());
        Assertions.assertEquals(1L, transport.lastBatchId);
        Assertions.assertEquals("hello-non", transport.lastPayload);
    }

    @Test
    void nonModeDropsEventOnSendFailure() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(
                EdgeEvent.builder()
                        .sourceId("file-input")
                        .payload("drop-me".getBytes(StandardCharsets.UTF_8))
                        .eventTime(1L)
                        .build());
        reader.events.add(
                EdgeEvent.builder()
                        .sourceId("file-input")
                        .payload("keep-me".getBytes(StandardCharsets.UTF_8))
                        .eventTime(2L)
                        .build());
        FakeTransport transport = new FakeTransport();
        transport.failNextSend = true;

        AgentRuntimeConfig config = loadNonModeConfig();
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        new MemWalStore(),
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        Assertions.assertTrue(scheduler.runOnce());
        Assertions.assertEquals(0L, transport.lastBatchId, "no event sent successfully");
        Assertions.assertNull(transport.lastPayload, "no payload delivered");
    }

    @Test
    void nonModeDecryptFailedStillFatal() {
        FakeReader reader = new FakeReader();
        reader.events.add(
                EdgeEvent.builder()
                        .sourceId("file-input")
                        .payload("x".getBytes(StandardCharsets.UTF_8))
                        .eventTime(1L)
                        .build());
        FakeTransport transport = new FakeTransport();
        transport.decryptFailOnNextSend = true;

        AgentRuntimeConfig config = loadNonModeConfig();
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        new MemWalStore(),
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        Assertions.assertThrows(IOException.class, scheduler::runOnce);
    }

    @Test
    void emptyPollReturnsNoProgress() throws Exception {
        FakeReader reader = new FakeReader();
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadRuntimeConfigWithBulkSize(tempDir.resolve("wal.db"), 1);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        new FakeWalStore(),
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        Assertions.assertFalse(scheduler.runOnce());
        Assertions.assertEquals(0L, transport.lastBatchId);
    }

    @Test
    void bufferDoesNotFlushBelowBatchSize() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(event("a"));
        FakeWalStore walStore = new FakeWalStore();
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadRuntimeConfigWithBulkSize(tempDir.resolve("wal.db"), 3);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        walStore,
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        Assertions.assertTrue(scheduler.runOnce(), "polled events → progress");
        Assertions.assertTrue(walStore.appendedIds.isEmpty(), "below batch size → not flushed");
        Assertions.assertEquals(0L, transport.lastBatchId, "nothing sent");

        reader.events.add(event("b"));
        scheduler.runOnce();
        Assertions.assertTrue(walStore.appendedIds.isEmpty(), "still below batch size");

        reader.events.add(event("c"));
        scheduler.runOnce();
        Assertions.assertEquals(3, walStore.appendedIds.size(), "batch size reached → flushed");
        Assertions.assertEquals(3, transport.sendCount, "all 3 sent");
    }

    @Test
    void bufferFlushesOnTimeout() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(event("timeout-event"));
        FakeWalStore walStore = new FakeWalStore();
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config =
                loadConfigWithBulkSizeAndFlushInterval(tempDir.resolve("wal.db"), 100, 500);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        walStore,
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        long t0 = 1000L;
        scheduler.runOnce(t0);
        Assertions.assertTrue(walStore.appendedIds.isEmpty(), "below batch size, no timeout yet");

        scheduler.runOnce(t0 + 499);
        Assertions.assertTrue(walStore.appendedIds.isEmpty(), "still before timeout");

        scheduler.runOnce(t0 + 500);
        Assertions.assertEquals(1, walStore.appendedIds.size(), "flush triggered by timeout");
        Assertions.assertEquals("timeout-event", transport.lastPayload);
    }

    @Test
    void multipleEventsAllSentAndAcked() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(eventWithPosition("e1", "/a.log", 100));
        reader.events.add(eventWithPosition("e2", "/a.log", 200));
        reader.events.add(eventWithPosition("e3", "/b.log", 50));
        FakeSourcePositionStore positionStore = new FakeSourcePositionStore();
        FakeWalStore walStore = new FakeWalStore(positionStore);
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadRuntimeConfigWithBulkSize(tempDir.resolve("wal.db"), 3);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        walStore,
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        scheduler.runOnce();
        Assertions.assertEquals(3, walStore.appendedIds.size());
        Assertions.assertEquals(3, walStore.ackedIds.size());
        Assertions.assertEquals(3, positionStore.saved.size());
        Assertions.assertEquals(3, transport.sendCount);
    }

    @Test
    void partialBatchFailureAcksOnlySuccessful() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(event("ok"));
        reader.events.add(event("fail"));
        reader.events.add(event("skip"));
        FakeWalStore walStore = new FakeWalStore();
        FakeTransport transport = new FakeTransport();
        transport.failAtSendCount = 2;

        AgentRuntimeConfig config = loadRuntimeConfigWithBulkSize(tempDir.resolve("wal.db"), 3);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        walStore,
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        scheduler.runOnce();
        Assertions.assertEquals(3, walStore.appendedIds.size(), "all 3 appended to WAL");
        Assertions.assertEquals(1, walStore.ackedIds.size(), "only first acked");
        Assertions.assertEquals(walStore.appendedIds.get(0), walStore.ackedIds.get(0));
    }

    @Test
    void closeFlushesRemainingBufferToWal() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(event("buffered"));
        FakeWalStore walStore = new FakeWalStore();
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadRuntimeConfigWithBulkSize(tempDir.resolve("wal.db"), 100);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        walStore,
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        scheduler.runOnce();
        Assertions.assertTrue(walStore.appendedIds.isEmpty(), "below batch size → buffered");

        scheduler.close();
        Assertions.assertEquals(1, walStore.appendedIds.size(), "close flushed buffer to WAL");
    }

    @Test
    void eventWithoutSourcePositionDoesNotFail() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(event("no-pos"));
        FakeSourcePositionStore positionStore = new FakeSourcePositionStore();
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadRuntimeConfigWithBulkSize(tempDir.resolve("wal.db"), 1);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        new FakeWalStore(positionStore),
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        scheduler.runOnce();
        Assertions.assertEquals("no-pos", transport.lastPayload);
        Assertions.assertTrue(positionStore.saved.isEmpty(), "no position saved");
    }

    @Test
    void bestEffortDecryptFailedIsFatal() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(event("x"));
        FakeTransport transport = new FakeTransport();
        transport.decryptFailOnNextSend = true;

        AgentRuntimeConfig config = loadRuntimeConfigWithBulkSize(tempDir.resolve("wal.db"), 1);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        new FakeWalStore(),
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        Assertions.assertThrows(IOException.class, scheduler::runOnce);
    }

    @Test
    void nonModeSavesSourcePositions() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(eventWithPosition("ev", "/data/app.log", 4096));
        FakeSourcePositionStore positionStore = new FakeSourcePositionStore();
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadNonModeConfig();
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        new FakeWalStore(positionStore),
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        scheduler.runOnce();
        Assertions.assertEquals(1, positionStore.saved.size());
        Assertions.assertEquals(4096L, positionStore.saved.get(0).getOffset());
    }

    @Test
    void memWalStoreDoesNotAccumulateAcrossIterations() throws Exception {
        FakeReader reader = new FakeReader();
        MemWalStore memWal = new MemWalStore();
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadNonModeConfig();
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        memWal,
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        for (int i = 0; i < 5; i++) {
            reader.events.add(event("iter-" + i));
            scheduler.runOnce();
        }
        Assertions.assertEquals(5, transport.sendCount, "all 5 iterations sent");
        Assertions.assertEquals(
                0, memWal.claimPending(100, 10).size(), "no pending records left in MemWalStore");
    }

    @Test
    void nonModeRecoversSendingAfterTransientFailure() throws Exception {
        FakeReader reader = new FakeReader();
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadNonModeConfig();
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        new MemWalStore(),
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        reader.events.add(event("fail-me"));
        transport.failNextSend = true;
        scheduler.runOnce();
        Assertions.assertNull(transport.lastPayload, "first iteration failed");

        reader.events.add(event("succeed"));
        scheduler.runOnce();
        Assertions.assertEquals("succeed", transport.lastPayload, "second iteration recovered");
    }

    @Test
    void nonModeCloseFlushesBufferToInMemoryWal() throws Exception {
        FakeReader reader = new FakeReader();
        reader.events.add(event("buf"));
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadNonModeConfigWithBulkSize(100);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        new MemWalStore(),
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        scheduler.runOnce();
        Assertions.assertEquals(0L, transport.lastBatchId, "below batch size → not sent");

        scheduler.close();
    }

    private static EdgeEvent event(String payload) {
        return EdgeEvent.builder()
                .sourceId("file-input")
                .payload(payload.getBytes(StandardCharsets.UTF_8))
                .eventTime(System.nanoTime())
                .build();
    }

    private static EdgeEvent eventWithPosition(String payload, String path, long offset) {
        return EdgeEvent.builder()
                .sourceId("file-input")
                .payload(payload.getBytes(StandardCharsets.UTF_8))
                .eventTime(System.nanoTime())
                .sourcePosition(
                        EdgeSourcePosition.builder()
                                .sourceId("file-input")
                                .partition(path)
                                .offset(offset)
                                .updatedAt(System.currentTimeMillis())
                                .build())
                .build();
    }

    private AgentRuntimeConfig loadNonModeConfig() {
        return loadNonModeConfigWithBulkSize(1);
    }

    private AgentRuntimeConfig loadNonModeConfigWithBulkSize(int bulkMaxSize) {
        try {
            Path yamlPath = tempDir.resolve("agent-non-" + bulkMaxSize + ".yaml");
            String yaml =
                    "agent:\n"
                            + "  delivery-guarantee: NON\n"
                            + "  bulk-max-size: "
                            + bulkMaxSize
                            + "\n"
                            + "input:\n"
                            + "  id: in-1\n"
                            + "  paths: [\"/tmp/a.log\"]\n"
                            + "output:\n"
                            + "  type: console\n";
            Files.write(yamlPath, yaml.getBytes(StandardCharsets.UTF_8));
            EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(yamlPath);
            return resolved.getRuntimeConfig();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private AgentRuntimeConfig loadRuntimeConfigWithBulkSize(Path sqlitePath, int bulkMaxSize)
            throws Exception {
        Path yamlPath = tempDir.resolve("agent.yaml");
        String yaml =
                "agent:\n"
                        + "  bulk-max-size: "
                        + bulkMaxSize
                        + "\n"
                        + "input:\n"
                        + "  id: in-1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "queue:\n"
                        + "  sqlite-path: "
                        + sqlitePath
                        + "\n"
                        + "output:\n"
                        + "  type: console\n";
        Files.write(yamlPath, yaml.getBytes(StandardCharsets.UTF_8));
        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(yamlPath);
        return resolved.getRuntimeConfig();
    }

    private AgentRuntimeConfig loadConfigWithBulkSizeAndFlushInterval(
            Path sqlitePath, int bulkMaxSize, long flushIntervalMs) throws Exception {
        Path yamlPath = tempDir.resolve("agent-flush.yaml");
        String yaml =
                "agent:\n"
                        + "  bulk-max-size: "
                        + bulkMaxSize
                        + "\n"
                        + "  flush-interval-ms: "
                        + flushIntervalMs
                        + "\n"
                        + "input:\n"
                        + "  id: in-1\n"
                        + "  paths: [\"/tmp/a.log\"]\n"
                        + "queue:\n"
                        + "  sqlite-path: "
                        + sqlitePath
                        + "\n"
                        + "output:\n"
                        + "  type: console\n";
        Files.write(yamlPath, yaml.getBytes(StandardCharsets.UTF_8));
        EdgeAgentResolvedConfig resolved = EdgeAgentConfigLoader.load(yamlPath);
        return resolved.getRuntimeConfig();
    }

    private static final class FakeReader implements EdgeInputReader {
        private final List<EdgeEvent> events = new ArrayList<>();

        @Override
        public List<EdgeEvent> poll(int maxRecords) {
            List<EdgeEvent> out = new ArrayList<>(events);
            events.clear();
            return out;
        }
    }

    private static final class FakeWalStore implements WalStore {
        private final EdgeSourcePositionStore posStore;
        private final List<Long> appendedIds = new ArrayList<>();
        private final List<Long> ackedIds = new ArrayList<>();
        private final List<WalRecord> pending = new ArrayList<>();
        private long nextId = 1;

        FakeWalStore() {
            this(new FakeSourcePositionStore());
        }

        FakeWalStore(EdgeSourcePositionStore posStore) {
            this.posStore = posStore;
        }

        @Override
        public EdgeSourcePositionStore sourcePositionStore() {
            return posStore;
        }

        @Override
        public long append(EdgeEvent event) {
            long id = nextId++;
            appendedIds.add(id);
            pending.add(
                    WalRecord.builder()
                            .id(id)
                            .batchId(id)
                            .sourceId(event.getSourceId())
                            .payload(event.getPayload())
                            .eventTime(event.getEventTime())
                            .status(WalRecordStatus.PENDING)
                            .build());
            return id;
        }

        @Override
        public List<WalRecord> claimPending(int maxRecords, int maxAttempts) {
            List<WalRecord> claimed = new ArrayList<>(pending);
            pending.clear();
            return claimed;
        }

        @Override
        public int markExceededAsDead(int maxAttempts, int maxRecords) {
            return 0;
        }

        @Override
        public void ack(long recordId) {
            ackedIds.add(recordId);
        }

        @Override
        public int resurrectSending(int maxRecords) {
            return 0;
        }

        @Override
        public int resurrectSending(int maxRecords, long staleThresholdMs) {
            return 0;
        }

        @Override
        public int cleanupAcked(long retentionMs, int maxRecords) {
            return 0;
        }
    }

    private static final class FakeSourcePositionStore implements EdgeSourcePositionStore {
        private final List<EdgeSourcePosition> saved = new ArrayList<>();

        @Override
        public EdgeSourcePosition load(String sourceId, String partition) {
            return null;
        }

        @Override
        public Map<String, EdgeSourcePosition> loadBySource(String sourceId) {
            return Collections.emptyMap();
        }

        @Override
        public void save(EdgeSourcePosition position) {
            saved.add(position);
        }
    }

    private static final class FakeTransport implements EdgeCollectorTransport {
        private long lastBatchId;
        private String lastPayload;
        private int sendCount;
        private boolean failNextSend;
        private boolean decryptFailOnNextSend;
        private int failAtSendCount = -1;

        @Override
        public void open() {}

        @Override
        public void sendUntilReceived(long batchId, String payload) throws IOException {
            sendCount++;
            if (decryptFailOnNextSend) {
                decryptFailOnNextSend = false;
                throw new IOException("DECRYPT_FAILED");
            }
            if (failNextSend) {
                failNextSend = false;
                throw new IOException("simulated send failure");
            }
            if (failAtSendCount > 0 && sendCount == failAtSendCount) {
                throw new IOException("simulated failure at send #" + sendCount);
            }
            this.lastBatchId = batchId;
            this.lastPayload = payload;
        }

        @Override
        public boolean probeReachable() {
            return true;
        }

        @Override
        public void close() {}
    }
}
