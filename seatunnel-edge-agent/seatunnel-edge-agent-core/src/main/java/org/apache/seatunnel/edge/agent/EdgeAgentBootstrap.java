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

package org.apache.seatunnel.edge.agent;

import org.apache.seatunnel.edge.agent.batch.AccumulatedRecord;
import org.apache.seatunnel.edge.agent.batch.RecordBatchAccumulator;
import org.apache.seatunnel.edge.agent.config.AgentYamlConfig;
import org.apache.seatunnel.edge.agent.config.AgentYamlLoader;
import org.apache.seatunnel.edge.agent.input.AgentInputBinding;
import org.apache.seatunnel.edge.agent.input.AgentYamlInputBinder;
import org.apache.seatunnel.edge.agent.transport.EdgeTransportClient;
import org.apache.seatunnel.edge.agent.transport.EdgeTransportConfigFactory;
import org.apache.seatunnel.edge.agent.transport.SeaTunnelEdgeTransportClients;
import org.apache.seatunnel.edge.agent.wal.SqliteOutboundWal;
import org.apache.seatunnel.edge.agent.wal.WalRecord;
import org.apache.seatunnel.engine.client.SeaTunnelClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Core runtime loop: poll inputs, accumulate, WAL enqueue, EdgeSocket batch send with ACK, WAL
 * ACK/retry.
 */
public final class EdgeAgentBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(EdgeAgentBootstrap.class);
    private static final long IDLE_SLEEP_MS = 50L;

    private EdgeAgentBootstrap() {}

    public static void start(Path agentYamlPath) throws Exception {
        AgentYamlConfig cfg = AgentYamlLoader.load(agentYamlPath);
        Path workDir = Paths.get("").toAbsolutePath();
        Path sqlite = SqliteOutboundWal.resolveSqlitePath(cfg.getQueue().getSqlitePath(), workDir);
        int pollBatch =
                cfg.getQueue().getPollBatchSize() != null ? cfg.getQueue().getPollBatchSize() : 128;
        int bulkMax =
                cfg.getBatch().getBulkMaxSize() != null ? cfg.getBatch().getBulkMaxSize() : 256;
        long flushMs =
                cfg.getBatch().getFlushIntervalMs() != null
                        ? cfg.getBatch().getFlushIntervalMs()
                        : 1000L;
        int maxAttempts =
                cfg.getRetry().getMaxAttempts() != null ? cfg.getRetry().getMaxAttempts() : 16;
        long backoffMs =
                cfg.getRetry().getBackoffMs() != null ? cfg.getRetry().getBackoffMs() : 250L;

        List<AgentInputBinding> bindings = AgentYamlInputBinder.bindAll(cfg.getInputs());
        RecordBatchAccumulator accumulator = new RecordBatchAccumulator(bulkMax, flushMs);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong nextBatchId = new AtomicLong(1L);
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    running.set(false);
                                    LOG.info("Shutdown signal received; stopping main loop.");
                                },
                                "edge-agent-shutdown"));

        try (SqliteOutboundWal wal = new SqliteOutboundWal(sqlite)) {
            wal.open();
            wal.recoverStaleSending();
            try (SeaTunnelClient seaTunnelClient =
                            SeaTunnelEdgeTransportClients.newSeaTunnelClient(
                                    cfg.getOutput().getClusterName(),
                                    cfg.getOutput().getClusterAddresses());
                    EdgeTransportClient transport =
                            new EdgeTransportClient(
                                    EdgeTransportConfigFactory.toEdgeTransportConfig(
                                            cfg.getOutput()),
                                    SeaTunnelEdgeTransportClients.jobTaskGroupAddressesLookup(
                                            seaTunnelClient))) {
                transport.open();
                for (AgentInputBinding binding : bindings) {
                    binding.getAgentInput().open();
                }
                try {
                    runLoop(
                            bindings,
                            accumulator,
                            wal,
                            transport,
                            nextBatchId,
                            pollBatch,
                            maxAttempts,
                            backoffMs,
                            running);
                } finally {
                    flushAccumulatorOnShutdown(accumulator, wal);
                    for (AgentInputBinding binding : bindings) {
                        try {
                            binding.getAgentInput().close();
                        } catch (Exception e) {
                            LOG.warn("Failed closing input {}", binding.getLogicalId(), e);
                        }
                    }
                }
            }
        }
    }

    private static void flushAccumulatorOnShutdown(
            RecordBatchAccumulator accumulator, SqliteOutboundWal wal) {
        try {
            List<AccumulatedRecord> tail = accumulator.drainAll(System.currentTimeMillis());
            if (!tail.isEmpty()) {
                wal.enqueuePending(tail);
            }
        } catch (SQLException e) {
            LOG.error("Failed flushing accumulator during shutdown.", e);
        }
    }

    /**
     * Joins WAL row payloads as newline-delimited records (NDJSON) for one EdgeSocket batch line.
     */
    static String mergeWalPayloadsNdjson(List<WalRecord> claimed) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < claimed.size(); i++) {
            if (i > 0) {
                sb.append('\n');
            }
            sb.append(claimed.get(i).getPayload());
        }
        return sb.toString();
    }

    private static void runLoop(
            List<AgentInputBinding> bindings,
            RecordBatchAccumulator accumulator,
            SqliteOutboundWal wal,
            EdgeTransportClient transport,
            AtomicLong nextBatchId,
            int pollBatchSize,
            int maxAttempts,
            long backoffMs,
            AtomicBoolean running)
            throws Exception {

        while (running.get()) {
            boolean progressed = false;

            for (AgentInputBinding binding : bindings) {
                List<String> polled = binding.getAgentInput().poll(pollBatchSize);
                if (!polled.isEmpty()) {
                    progressed = true;
                }
                for (String payload : polled) {
                    accumulator.offer(payload, binding.getLogicalId());
                    List<AccumulatedRecord> ready =
                            accumulator.drainIfReady(System.currentTimeMillis());
                    if (!ready.isEmpty()) {
                        wal.enqueuePending(ready);
                        progressed = true;
                    }
                }
            }

            List<AccumulatedRecord> timeFlush =
                    accumulator.drainIfReady(System.currentTimeMillis());
            if (!timeFlush.isEmpty()) {
                wal.enqueuePending(timeFlush);
                progressed = true;
            }

            List<WalRecord> claimed = wal.claimSendingBatch(pollBatchSize, maxAttempts);
            if (!claimed.isEmpty()) {
                progressed = true;
                List<Long> ids = new ArrayList<>(claimed.size());
                for (WalRecord row : claimed) {
                    ids.add(row.getId());
                }
                long batchId = nextBatchId.getAndIncrement();
                String ndjsonPayload = mergeWalPayloadsNdjson(claimed);
                try {
                    transport.sendBatchAndAwaitAck(batchId, ndjsonPayload);
                    wal.ackSending(ids);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    wal.revertSendingWithAttemptIncrement(ids);
                    throw e;
                } catch (Exception e) {
                    LOG.warn(
                            "Batch send failed for {} wal rows (batchId={}); reverting to PENDING.",
                            claimed.size(),
                            batchId,
                            e);
                    wal.revertSendingWithAttemptIncrement(ids);
                    sleepQuietly(backoffMs);
                }
            }

            if (!progressed) {
                sleepQuietly(IDLE_SLEEP_MS);
            }
        }
    }

    private static void sleepQuietly(long ms) throws InterruptedException {
        if (ms <= 0) {
            return;
        }
        Thread.sleep(ms);
    }
}
