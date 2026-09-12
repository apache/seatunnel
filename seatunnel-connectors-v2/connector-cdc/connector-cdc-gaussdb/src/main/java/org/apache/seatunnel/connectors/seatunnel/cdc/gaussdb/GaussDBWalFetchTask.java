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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset.LsnOffset;

import io.debezium.connector.postgresql.PostgresChangeRecordEmitter;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Incremental fetch task that emits mppdb changes through Debezium's record schemas. */
@Slf4j
final class GaussDBWalFetchTask implements FetchTask<SourceSplitBase> {

    /** Incremental split carrying startup, stopping, and captured-table state. */
    private final IncrementalSplit split;

    /** Reader loop state observed by SeaTunnel's stream fetcher. */
    private volatile boolean taskRunning;

    /** Active task-owned mppdb stream. */
    private volatile MppdbReplicationStream stream;

    /** Largest LSN acknowledged to the server after a completed checkpoint. */
    private Long acknowledgedLsn;

    /** Transaction assembler preventing records and checkpoints from crossing an open COMMIT. */
    private final MppdbTransactionBuffer transactionBuffer = new MppdbTransactionBuffer();

    /** Largest contiguous committed LSN released to the SeaTunnel source reader. */
    private long lastCompletedLsn;

    /** Creates an incremental fetch task for one stream split. */
    GaussDBWalFetchTask(IncrementalSplit split) {
        this.split = split;
    }

    /** Starts the checkpoint-aware polling loop and dispatches normalized SourceRecords. */
    @Override
    public void execute(FetchTask.Context context) throws Exception {
        GaussDBSourceFetchTaskContext sourceContext = (GaussDBSourceFetchTaskContext) context;
        this.stream = sourceContext.getMppdbStream();
        this.taskRunning = true;
        long startupLsn = ((LsnOffset) split.getStartupOffset()).getLsn().asLong();
        this.lastCompletedLsn = startupLsn;
        stream.start(startupLsn);
        int maxBatchSize = sourceContext.getDbzConnectorConfig().getMaxBatchSize();
        long pollIntervalMillis =
                sourceContext.getDbzConnectorConfig().getPollInterval().toMillis();
        log.info(
                "Start streaming GaussDB mppdb_decoding split from {}",
                Lsn.valueOf(startupLsn).asString());

        try {
            while (taskRunning && stream.isRunning()) {
                List<MppdbWalChange> changes = stream.readPending(maxBatchSize);
                transactionBuffer.warnIfStalled();
                if (changes.isEmpty()) {
                    Thread.sleep(pollIntervalMillis);
                    continue;
                }
                for (MppdbWalChange change : changes) {
                    if (!taskRunning) {
                        return;
                    }
                    Long completedLsn = processChange(sourceContext, change);
                    if (completedLsn != null && reachedStopOffset(completedLsn)) {
                        return;
                    }
                }
            }
        } finally {
            taskRunning = false;
            stream.close();
        }
    }

    /** Acknowledges only monotonically increasing offsets from completed checkpoints. */
    synchronized void commitCurrentOffset(LsnOffset offset) throws SQLException {
        if (stream == null || offset == null) {
            return;
        }
        long checkpointLsn = offset.getLsn().asLong();
        if (acknowledgedLsn == null
                || Lsn.valueOf(checkpointLsn).compareTo(Lsn.valueOf(acknowledgedLsn)) > 0) {
            stream.acknowledge(checkpointLsn);
            acknowledgedLsn = checkpointLsn;
            log.info(
                    "Acknowledged GaussDB mppdb_decoding slot at checkpoint LSN {}",
                    Lsn.valueOf(checkpointLsn).asString());
        }
    }

    /** Returns whether the background task is actively reading its split. */
    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    /** Stops polling and closes the task-owned replication connection. */
    @Override
    public void shutdown() {
        taskRunning = false;
        MppdbReplicationStream activeStream = stream;
        if (activeStream != null) {
            activeStream.close();
        }
    }

    /** Returns the incremental split assigned to this task. */
    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    /** Buffers decoded records and emits only the newly committed contiguous transaction prefix. */
    private Long processChange(GaussDBSourceFetchTaskContext sourceContext, MppdbWalChange change)
            throws InterruptedException {
        List<MppdbTransactionBuffer.CommittedTransaction> committedPrefix =
                transactionBuffer.add(change);
        if (committedPrefix.isEmpty()) {
            return null;
        }

        long previousCompletedLsn = lastCompletedLsn;
        List<MppdbTransactionBuffer.CommittedTransaction> unreplayedTransactions =
                new ArrayList<>();
        for (MppdbTransactionBuffer.CommittedTransaction transaction : committedPrefix) {
            if (Lsn.valueOf(transaction.getCommitLsn()).compareTo(Lsn.valueOf(previousCompletedLsn))
                    <= 0) {
                log.debug(
                        "Discard replayed GaussDB transaction {} at COMMIT LSN {}",
                        transaction.getTransactionId(),
                        Lsn.valueOf(transaction.getCommitLsn()).asString());
                continue;
            }
            unreplayedTransactions.add(transaction);
        }
        if (unreplayedTransactions.isEmpty()) {
            return null;
        }

        long safeLsn = maximumCommitLsn(unreplayedTransactions, previousCompletedLsn);
        emitCommittedPrefix(sourceContext, unreplayedTransactions, previousCompletedLsn, safeLsn);
        lastCompletedLsn = safeLsn;
        return safeLsn;
    }

    /** Emits an entire committed prefix before exposing its maximum checkpoint-safe COMMIT LSN. */
    private void emitCommittedPrefix(
            GaussDBSourceFetchTaskContext sourceContext,
            List<MppdbTransactionBuffer.CommittedTransaction> transactions,
            long previousCompletedLsn,
            long safeLsn)
            throws InterruptedException {
        List<MppdbWalChange> capturedChanges = new ArrayList<>();
        List<Long> transactionIds = new ArrayList<>();
        long safeTransactionId = 0;
        for (MppdbTransactionBuffer.CommittedTransaction transaction : transactions) {
            if (transaction.getCommitLsn() == safeLsn) {
                safeTransactionId = transaction.getTransactionId();
            }
            for (MppdbWalChange change : transaction.getChanges()) {
                TableId tableId = new TableId(null, change.getSchema(), change.getTable());
                if (sourceContext
                        .getDbzConnectorConfig()
                        .getTableFilters()
                        .dataCollectionFilter()
                        .isIncluded(tableId)) {
                    capturedChanges.add(change);
                    transactionIds.add(transaction.getTransactionId());
                }
            }
        }

        Lsn checkpointLsn = Lsn.valueOf(safeLsn);
        Instant eventTime = Clock.SYSTEM.currentTimeAsInstant();
        PostgresOffsetContext offsetContext = sourceContext.getOffsetContext();
        for (int index = 0; index < capturedChanges.size(); index++) {
            MppdbWalChange change = capturedChanges.get(index);
            Lsn completelyProcessedLsn =
                    index == capturedChanges.size() - 1
                            ? checkpointLsn
                            : Lsn.valueOf(previousCompletedLsn);
            emitDataChange(
                    sourceContext,
                    change,
                    transactionIds.get(index),
                    completelyProcessedLsn,
                    eventTime);
        }

        Long optionalTransactionId = safeTransactionId == 0 ? null : safeTransactionId;
        // Publish the prefix position only after every captured DML record has been enqueued. If no
        // captured table changed, a configured heartbeat still prevents slot growth.
        offsetContext.updateWalPosition(
                checkpointLsn, checkpointLsn, eventTime, optionalTransactionId, null);
        offsetContext.updateCommitPosition(checkpointLsn, checkpointLsn);
        sourceContext
                .getPgEventDispatcher()
                .dispatchHeartbeatEvent(sourceContext.getPartition(), offsetContext);
    }

    /** Returns the greatest COMMIT LSN in a released prefix without depending on BEGIN order. */
    static long maximumCommitLsn(
            List<MppdbTransactionBuffer.CommittedTransaction> transactions, long baselineLsn) {
        long maximumLsn = baselineLsn;
        for (MppdbTransactionBuffer.CommittedTransaction transaction : transactions) {
            if (Lsn.valueOf(transaction.getCommitLsn()).compareTo(Lsn.valueOf(maximumLsn)) > 0) {
                maximumLsn = transaction.getCommitLsn();
            }
        }
        return maximumLsn;
    }

    /** Adapts and dispatches one captured row while retaining a transaction-safe checkpoint LSN. */
    private void emitDataChange(
            GaussDBSourceFetchTaskContext sourceContext,
            MppdbWalChange change,
            long transactionId,
            Lsn completelyProcessedLsn,
            Instant eventTime)
            throws InterruptedException {
        TableId tableId = new TableId(null, change.getSchema(), change.getTable());
        Long optionalTransactionId = transactionId == 0 ? null : transactionId;
        PostgresOffsetContext offsetContext = sourceContext.getOffsetContext();
        Lsn eventLsn = Lsn.valueOf(change.getLsn());
        offsetContext.updateWalPosition(
                eventLsn, completelyProcessedLsn, eventTime, optionalTransactionId, null, tableId);
        sourceContext
                .getPgEventDispatcher()
                .dispatchDataChangeEvent(
                        sourceContext.getPartition(),
                        tableId,
                        new PostgresChangeRecordEmitter(
                                sourceContext.getPartition(),
                                offsetContext,
                                Clock.SYSTEM,
                                sourceContext.getDbzConnectorConfig(),
                                sourceContext.getDatabaseSchema(),
                                sourceContext.getDataConnection(),
                                tableId,
                                new MppdbReplicationMessage(
                                        new MppdbWalChange(
                                                change.getLsn(),
                                                transactionId,
                                                change.getType(),
                                                change.getSchema(),
                                                change.getTable(),
                                                change.getOldColumns(),
                                                change.getNewColumns()),
                                        sourceContext.getTypeRegistry(),
                                        eventTime)));
    }

    /** Returns whether a bounded incremental split reached its configured stopping LSN. */
    private boolean reachedStopOffset(long currentLsn) {
        if (split.getStopOffset() == null || split.getStopOffset().isNeverStop()) {
            return false;
        }
        return new LsnOffset(currentLsn, null, Instant.MIN).compareTo(split.getStopOffset()) >= 0;
    }
}
