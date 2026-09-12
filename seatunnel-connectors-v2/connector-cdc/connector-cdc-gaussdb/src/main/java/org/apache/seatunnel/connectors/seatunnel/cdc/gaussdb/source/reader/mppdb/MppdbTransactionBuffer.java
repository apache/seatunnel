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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb.source.reader.mppdb;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Buffers mppdb records until their transaction commits and preserves transaction order.
 *
 * <p>Parallel decoding may complete a later transaction before an earlier transaction. Only the
 * contiguous committed prefix is released so a checkpoint can never acknowledge past an unfinished
 * transaction. Binary protocol records omit transaction ids before COMMIT, so one anonymous
 * transaction is also supported when records are delivered as a contiguous group.
 */
@Slf4j
public final class MppdbTransactionBuffer {

    // Warn every five minutes while an unfinished transaction prevents prefix release.
    private static final long STALL_WARNING_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(5);

    // Monotonic clock used only for diagnostics, never for transaction ordering.
    private final LongSupplier nanoClock;

    /**
     * Last diagnostic timestamp shared across head changes to bound task-wide warning frequency.
     */
    private long lastWarningNanos;

    // Whether a diagnostic timestamp has been recorded, including a possible zero clock value.
    private boolean warned;

    // Number of DML records held across open and blocked committed transactions.
    private long bufferedChanges;

    /** Transactions indexed by the non-zero id carried by text and SQL-fallback records. */
    private final Map<Long, PendingTransaction> transactionsById = new HashMap<>();

    /** Transactions in first-observed order for contiguous-prefix release. */
    private final Deque<PendingTransaction> transactionOrder = new ArrayDeque<>();

    /** Active transaction whose binary records do not expose an id until COMMIT. */
    private PendingTransaction anonymousTransaction;

    // Creates a task-owned buffer with a monotonic diagnostic clock.
    public MppdbTransactionBuffer() {
        this(System::nanoTime);
    }

    // Supplies a monotonic clock for deterministic warning-boundary tests.
    public MppdbTransactionBuffer(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
    }

    /**
     * Adds one decoded record and returns all newly releasable committed transactions.
     *
     * @param change decoded BEGIN, DML, or COMMIT record
     * @return committed transactions in safe checkpoint order
     */
    public List<CommittedTransaction> add(MppdbWalChange change) {
        switch (change.getType()) {
            case BEGIN:
                begin(change.getTransactionId());
                break;
            case COMMIT:
                commit(change.getTransactionId(), change.getLsn());
                break;
            case INSERT:
            case UPDATE:
            case DELETE:
                transactionForData(change.getTransactionId()).changes.add(change);
                bufferedChanges++;
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported mppdb transaction record " + change.getType());
        }
        return drainCommittedPrefix();
    }

    /**
     * Reports stalled prefix release without dropping records or advancing checkpoint positions.
     * Called on every reader poll, including polls that return no WAL records.
     *
     * @return whether this poll emitted a warning
     */
    public boolean warnIfStalled() {
        PendingTransaction oldest = transactionOrder.peekFirst();
        if (oldest == null) {
            return false;
        }
        long now = nanoClock.getAsLong();
        if (now - oldest.beginNanos < STALL_WARNING_INTERVAL_NANOS
                || (warned && now - lastWarningNanos < STALL_WARNING_INTERVAL_NANOS)) {
            return false;
        }
        lastWarningNanos = now;
        warned = true;
        log.warn(
                "GaussDB mppdb transaction {} has blocked checkpoint progress for {} seconds; "
                        + "{} transactions and {} row changes are buffered. Check long-running source "
                        + "transactions and COMMIT delivery; buffered rows cannot be released safely.",
                oldest.transactionId,
                TimeUnit.NANOSECONDS.toSeconds(now - oldest.beginNanos),
                transactionOrder.size(),
                bufferedChanges);
        return true;
    }

    /** Starts a named or binary anonymous transaction. */
    private void begin(long transactionId) {
        if (transactionId == 0) {
            if (anonymousTransaction != null) {
                throw new IllegalStateException(
                        "Received overlapping anonymous mppdb transactions");
            }
            anonymousTransaction = new PendingTransaction(0, false, nanoClock.getAsLong());
            transactionOrder.addLast(anonymousTransaction);
            return;
        }
        if (transactionsById.containsKey(transactionId)) {
            throw new IllegalStateException(
                    "Received duplicate BEGIN for mppdb transaction " + transactionId);
        }
        PendingTransaction transaction =
                new PendingTransaction(transactionId, true, nanoClock.getAsLong());
        transactionsById.put(transactionId, transaction);
        transactionOrder.addLast(transaction);
    }

    /** Resolves the transaction owning a DML record without guessing across parallel streams. */
    private PendingTransaction transactionForData(long transactionId) {
        if (transactionId != 0) {
            PendingTransaction transaction = transactionsById.get(transactionId);
            if (transaction == null) {
                throw new IllegalStateException(
                        "Received mppdb DML without BEGIN for transaction " + transactionId);
            }
            return transaction;
        }
        if (anonymousTransaction != null) {
            return anonymousTransaction;
        }
        return singleOpenTransaction("DML");
    }

    /** Marks the matching transaction committed at its server-provided COMMIT LSN. */
    private void commit(long transactionId, long commitLsn) {
        if (commitLsn == 0) {
            throw new IllegalStateException("Received mppdb COMMIT without a valid LSN");
        }
        PendingTransaction transaction = null;
        if (transactionId != 0) {
            transaction = transactionsById.get(transactionId);
        }
        if (transaction == null && anonymousTransaction != null) {
            transaction = anonymousTransaction;
            transaction.transactionId = transactionId;
        }
        if (transaction == null) {
            transaction = singleOpenTransaction("COMMIT");
            if (transactionId != 0 && transaction.transactionId != transactionId) {
                throw new IllegalStateException(
                        "Received COMMIT for unknown mppdb transaction " + transactionId);
            }
        }
        if (transaction.committed) {
            throw new IllegalStateException(
                    "Received duplicate COMMIT for mppdb transaction " + transactionId);
        }
        transaction.committed = true;
        transaction.commitLsn = commitLsn;
        if (transaction == anonymousTransaction) {
            anonymousTransaction = null;
        }
    }

    /** Returns the only unfinished named transaction or rejects ambiguous protocol input. */
    private PendingTransaction singleOpenTransaction(String recordType) {
        PendingTransaction candidate = null;
        for (PendingTransaction transaction : transactionOrder) {
            if (transaction.committed) {
                continue;
            }
            if (candidate != null) {
                throw new IllegalStateException(
                        "Cannot associate anonymous mppdb "
                                + recordType
                                + " with multiple open transactions");
            }
            candidate = transaction;
        }
        if (candidate == null) {
            throw new IllegalStateException(
                    "Received mppdb " + recordType + " without an open transaction");
        }
        return candidate;
    }

    /** Removes and returns only the committed prefix at the head of the transaction queue. */
    private List<CommittedTransaction> drainCommittedPrefix() {
        List<CommittedTransaction> committed = new ArrayList<>();
        while (!transactionOrder.isEmpty() && transactionOrder.peekFirst().committed) {
            PendingTransaction transaction = transactionOrder.removeFirst();
            bufferedChanges -= transaction.changes.size();
            if (transaction.indexed) {
                transactionsById.remove(transaction.transactionId, transaction);
            }
            committed.add(
                    new CommittedTransaction(
                            transaction.transactionId, transaction.commitLsn, transaction.changes));
        }
        return committed;
    }

    /** Mutable transaction state retained only until its COMMIT becomes safely releasable. */
    private static final class PendingTransaction {

        /** Transaction id, assigned at COMMIT for anonymous binary records. */
        private long transactionId;

        /** Whether this transaction was registered in the id lookup map. */
        private final boolean indexed;

        /** DML records in their original order. */
        private final List<MppdbWalChange> changes = new ArrayList<>();

        /** Whether a matching COMMIT has been decoded. */
        private boolean committed;

        /** Server LSN of the transaction COMMIT record. */
        private long commitLsn;

        // Monotonic arrival time of BEGIN, used to report how long release has been blocked.
        private final long beginNanos;

        /** Creates pending state for one named or anonymous transaction. */
        private PendingTransaction(long transactionId, boolean indexed, long beginNanos) {
            this.transactionId = transactionId;
            this.indexed = indexed;
            this.beginNanos = beginNanos;
        }
    }

    /** Immutable committed transaction released to the WAL fetch task. */
    public static final class CommittedTransaction {

        /** Transaction id propagated to Debezium source metadata. */
        private final long transactionId;

        /** LSN that can become checkpoint-acknowledgeable after all records are emitted. */
        private final long commitLsn;

        /** Ordered DML records belonging to this transaction. */
        private final List<MppdbWalChange> changes;

        /** Creates an immutable committed transaction snapshot. */
        private CommittedTransaction(
                long transactionId, long commitLsn, List<MppdbWalChange> changes) {
            this.transactionId = transactionId;
            this.commitLsn = commitLsn;
            this.changes = Collections.unmodifiableList(new ArrayList<>(changes));
        }

        /** Returns the transaction id, or zero when the protocol omitted it. */
        public long getTransactionId() {
            return transactionId;
        }

        /** Returns the server COMMIT LSN. */
        public long getCommitLsn() {
            return commitLsn;
        }

        /** Returns the transaction DML records in decoding order. */
        public List<MppdbWalChange> getChanges() {
            return changes;
        }
    }
}
