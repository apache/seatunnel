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
        FakeWalStore walStore = new FakeWalStore();
        FakeSourcePositionStore positionStore = new FakeSourcePositionStore();
        FakeTransport transport = new FakeTransport();

        AgentRuntimeConfig config = loadRuntimeConfigWithBulkSize(tempDir.resolve("wal.db"), 1);
        EdgeAgentRuntimeContext ctx =
                new EdgeAgentRuntimeContext(
                        reader,
                        walStore,
                        positionStore,
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
                        new FakeSourcePositionStore(),
                        transport,
                        new RawPayloadSerializer(),
                        new AtomicBoolean(true));
        EdgeAgentRuntimeScheduler scheduler = EdgeAgentRuntimeScheduler.create(config, ctx);

        Assertions.assertTrue(scheduler.runOnce());
        Assertions.assertTrue(walStore.ackedIds.isEmpty());
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
        private final List<Long> appendedIds = new ArrayList<>();
        private final List<Long> ackedIds = new ArrayList<>();
        private final List<WalRecord> pending = new ArrayList<>();
        private long nextId = 1L;

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
                            .metadata(event.getMetadata())
                            .status(WalRecordStatus.PENDING)
                            .build());
            return id;
        }

        @Override
        public List<WalRecord> claimPending(int maxRecords, int maxAttempts) {
            List<WalRecord> claimed = new ArrayList<>(pending);
            pending.clear();
            for (WalRecord record : claimed) {
                record.setStatus(WalRecordStatus.SENDING);
            }
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
        public java.util.Map<String, EdgeSourcePosition> loadBySource(String sourceId) {
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
        private boolean failNextSend;

        @Override
        public void open() {}

        @Override
        public void sendUntilReceived(long batchId, String payload) throws IOException {
            if (failNextSend) {
                failNextSend = false;
                throw new IOException("simulated send failure");
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
