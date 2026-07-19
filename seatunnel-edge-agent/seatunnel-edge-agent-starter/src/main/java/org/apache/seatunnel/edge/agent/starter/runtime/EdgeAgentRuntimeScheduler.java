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
import org.apache.seatunnel.edge.agent.starter.config.AgentRuntimeConfig;
import org.apache.seatunnel.edge.agent.starter.wal.WalRecord;
import org.apache.seatunnel.edge.agent.starter.wal.WalStore;
import org.apache.seatunnel.edge.agent.transport.EdgeCollectorTransport;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportConfig;
import org.apache.seatunnel.edge.agent.transport.serialize.PayloadSerializer;
import org.apache.seatunnel.edge.agent.transport.socket.EdgeSocketProtocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class EdgeAgentRuntimeScheduler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EdgeAgentRuntimeScheduler.class);

    private final EdgeAgentRuntimeContext ctx;
    private final EdgeInputReader reader;
    private final WalStore walStore;
    private final EdgeCollectorTransport transport;
    private final PayloadSerializer payloadSerializer;
    private final int maxPollRecords;
    private final int maxAttempts;
    private final long backoffMs;
    private final long backoffMaxMs;
    private final int resurrectBatchSize;
    private final long resurrectIntervalMs;
    private final int cleanupBatchSize;
    private final long ackedRetentionMs;
    private final long idleSleepMs;
    private final int batchBulkMaxSize;
    private final long batchFlushIntervalMs;

    private final List<EdgeEvent> pendingBuffer = new ArrayList<>();
    private long bufferNonEmptySinceMs;
    private int sendFailureAttempt;

    static EdgeAgentRuntimeScheduler create(
            AgentRuntimeConfig config, EdgeAgentRuntimeContext ctx) {
        return new EdgeAgentRuntimeScheduler(config, ctx);
    }

    EdgeAgentRuntimeScheduler(AgentRuntimeConfig config, EdgeAgentRuntimeContext ctx) {
        this.ctx = ctx;
        this.reader = ctx.getReader();
        this.walStore = ctx.getWalStore();
        this.transport = ctx.getTransport();
        this.payloadSerializer = ctx.getPayloadSerializer();
        this.maxPollRecords = config.getMaxPollRecords();
        this.maxAttempts = config.getRetryMaxAttempts();
        this.backoffMs = config.getRetryBackoffMs();
        this.backoffMaxMs = config.getRetryBackoffMaxMs();
        this.resurrectBatchSize = config.getResurrectBatchSize();
        this.resurrectIntervalMs = config.getResurrectIntervalMs();
        this.cleanupBatchSize = config.getCleanupBatchSize();
        this.ackedRetentionMs = config.getAckedRetentionMs();
        this.idleSleepMs = config.getIdleSleepMs();
        this.batchBulkMaxSize = config.getBatchBulkMaxSize();
        this.batchFlushIntervalMs = config.getBatchFlushIntervalMs();
    }

    /**
     * Main loop: poll input, flush/send, and manage WAL lifecycle.
     *
     * <p>Each iteration resurrects stale SENDING rows, marks exceeded rows as DEAD, and cleans up
     * ACKED rows. For the in-memory {@code MemWalStore} these calls are no-ops.
     *
     * @param running shared termination flag
     * @throws Exception on WAL, transport, or reader failures
     */
    public void runUntilStopped(AtomicBoolean running) throws Exception {
        long nextResurrectAt = 0L;
        try {
            while (running.get()) {
                long now = System.currentTimeMillis();
                if (shouldResurrect(now, nextResurrectAt)) {
                    walStore.resurrectSending(resurrectBatchSize, resurrectIntervalMs);
                    nextResurrectAt = now + resurrectIntervalMs;
                }
                walStore.markExceededAsDead(maxAttempts, cleanupBatchSize);
                walStore.cleanupAcked(ackedRetentionMs, cleanupBatchSize);

                boolean progressed = runOnce(now);
                if (!progressed) {
                    sleepIdle();
                }
            }
        } finally {
            flushBufferToWal();
        }
    }

    @Override
    public void close() throws Exception {
        flushBufferToWal();
        Exception closeException = null;
        try {
            reader.close();
        } catch (Exception ex) {
            closeException = ex;
        }
        try {
            transport.close();
        } catch (Exception ex) {
            if (closeException == null) {
                closeException = ex;
            }
        }
        try {
            walStore.close();
        } catch (Exception ex) {
            if (closeException == null) {
                closeException = ex;
            }
        }
        if (closeException != null) {
            throw closeException;
        }
    }

    /**
     * Executes one scheduler iteration: poll → flush buffer to WAL → send claimed records.
     *
     * <p>Exposed for unit tests.
     *
     * @return whether the iteration did useful work
     * @throws Exception on reader, WAL, or transport errors
     */
    public boolean runOnce() throws Exception {
        return runOnce(System.currentTimeMillis());
    }

    boolean runOnce(long now) throws Exception {
        List<EdgeEvent> events = reader.poll(maxPollRecords);
        if (!events.isEmpty()) {
            if (pendingBuffer.isEmpty()) {
                bufferNonEmptySinceMs = now;
            }
            pendingBuffer.addAll(events);
        }

        flushBufferIfTimedOut(now);
        int appended = 0;
        if (pendingBuffer.size() >= batchBulkMaxSize) {
            appended = flushBufferToWal();
        }
        int sent = sendClaimedRecords();
        return appended > 0 || sent > 0 || !events.isEmpty();
    }

    private int sendClaimedRecords() throws Exception {
        List<WalRecord> records = walStore.claimPending(maxPollRecords, maxAttempts);
        int sent = 0;
        for (WalRecord record : records) {
            long batchId = record.getBatchId() > 0 ? record.getBatchId() : record.getId();
            try {
                transport.send(batchId, payloadSerializer.serialize(record.getPayload()));
                walStore.ack(record.getId());
                sent++;
                sendFailureAttempt = 0;
            } catch (IOException ex) {
                if (isDecryptFailed(ex)) {
                    throw ex;
                }
                LOG.warn(
                        "WAL send failed for record id={}, batchId={}; row stays SENDING for"
                                + " resurrect/retry",
                        record.getId(),
                        batchId,
                        ex);
                Thread.sleep(
                        EdgeTransportConfig.computeBackoffMillis(
                                sendFailureAttempt++, backoffMs, backoffMaxMs));
                break;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw ex;
            }
        }
        return sent;
    }

    private static boolean isDecryptFailed(IOException ex) {
        String message = ex.getMessage();
        return message != null && message.contains(EdgeSocketProtocol.RESP_DECRYPT_FAILED);
    }

    private void flushBufferIfTimedOut(long now) throws Exception {
        if (!pendingBuffer.isEmpty()
                && batchFlushIntervalMs > 0
                && now - bufferNonEmptySinceMs >= batchFlushIntervalMs) {
            flushBufferToWal();
        }
    }

    private int flushBufferToWal() throws Exception {
        if (pendingBuffer.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (EdgeEvent event : pendingBuffer) {
            walStore.append(event);
            saveSourcePositionIfPresent(event);
            count++;
        }
        pendingBuffer.clear();
        bufferNonEmptySinceMs = 0L;
        return count;
    }

    private void saveSourcePositionIfPresent(EdgeEvent event) throws Exception {
        if (event.getSourcePosition() != null) {
            ctx.getWalStore().sourcePositionStore().save(event.getSourcePosition());
        }
    }

    private boolean shouldResurrect(long now, long nextResurrectAt) {
        return resurrectBatchSize > 0 && resurrectIntervalMs >= 0 && now >= nextResurrectAt;
    }

    private void sleepIdle() throws InterruptedException {
        if (idleSleepMs > 0) {
            Thread.sleep(idleSleepMs);
        }
    }
}
