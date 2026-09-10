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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Buffers mppdb records until their transaction commits and preserves transaction order.
 *
 * <p>Parallel decoding may complete a later transaction before an earlier transaction. Only the
 * contiguous committed prefix is released so a checkpoint can never acknowledge past an unfinished
 * transaction. Binary protocol records omit transaction ids before COMMIT, so one anonymous
 * transaction is also supported when records are delivered as a contiguous group.
 */
final class MppdbTransactionBuffer {

    /** Transactions indexed by the non-zero id carried by text and SQL-fallback records. */
    private final Map<Long, PendingTransaction> transactionsById = new HashMap<>();

    /** Transactions in first-observed order for contiguous-prefix release. */
    private final Deque<PendingTransaction> transactionOrder = new ArrayDeque<>();

    /** Active transaction whose binary records do not expose an id until COMMIT. */
    private PendingTransaction anonymousTransaction;

    /**
     * Adds one decoded record and returns all newly releasable committed transactions.
     *
     * @param change decoded BEGIN, DML, or COMMIT record
     * @return committed transactions in safe checkpoint order
     */
    List<CommittedTransaction> add(MppdbWalChange change) {
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
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported mppdb transaction record " + change.getType());
        }
        return drainCommittedPrefix();
    }

    /** Starts a named or binary anonymous transaction. */
    private void begin(long transactionId) {
        if (transactionId == 0) {
            if (anonymousTransaction != null) {
                throw new IllegalStateException(
                        "Received overlapping anonymous mppdb transactions");
            }
            anonymousTransaction = new PendingTransaction(0, false);
            transactionOrder.addLast(anonymousTransaction);
            return;
        }
        if (transactionsById.containsKey(transactionId)) {
            throw new IllegalStateException(
                    "Received duplicate BEGIN for mppdb transaction " + transactionId);
        }
        PendingTransaction transaction = new PendingTransaction(transactionId, true);
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

        /** Creates pending state for one named or anonymous transaction. */
        private PendingTransaction(long transactionId, boolean indexed) {
            this.transactionId = transactionId;
            this.indexed = indexed;
        }
    }

    /** Immutable committed transaction released to the WAL fetch task. */
    static final class CommittedTransaction {

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
        long getTransactionId() {
            return transactionId;
        }

        /** Returns the server COMMIT LSN. */
        long getCommitLsn() {
            return commitLsn;
        }

        /** Returns the transaction DML records in decoding order. */
        List<MppdbWalChange> getChanges() {
            return changes;
        }
    }
}
