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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import java.sql.Connection;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * State of the manual-commit transaction that one or more sink writers share between two commits.
 *
 * <p>The writers of a multi-table sink that land on the same queue index share one pooled
 * connection, so they also share one open transaction. A batch that one writer flushed into that
 * transaction is lost if another writer rolls the transaction back, or if the connection dies and
 * the pool replaces it. This object is owned by whoever owns the connection (the queue slot in
 * {@code ConnectionPoolManager}, or the provider of a single writer), so every writer on that
 * connection sees the same state.
 *
 * <p>It records two facts:
 *
 * <ul>
 *   <li>the connection whose open transaction holds flushed but uncommitted work, and which owner
 *       flushed last (as a sequence number, so a savepoint rollback can tell whether another writer
 *       flushed after the savepoint);
 *   <li>whether flushed work has already been lost in this checkpoint interval. Once that is set
 *       ("poisoned"), no writer may commit, because a commit would report a checkpoint as complete
 *       while rows of some table are missing. The job has to fail and recover from the last
 *       checkpoint. A successful commit cannot happen while poisoned, so the flag stays set for the
 *       rest of this writer's life.
 * </ul>
 *
 * <p>Owners are compared by identity. A writer uses its connection provider as owner, because the
 * provider stays the same when the output format is rebuilt after a schema change.
 */
public class JdbcTransactionState {

    private Connection connection;
    private final Map<Object, Long> lastFlushByOwner = new IdentityHashMap<>();
    private long flushSequence;
    private String lostWorkReason;

    /**
     * Records a successful flush of {@code owner} into the open transaction of {@code
     * flushConnection}.
     *
     * <p>If earlier work is pending on a different connection, that connection was replaced between
     * two flushes and its work is gone.
     */
    public synchronized void recordFlush(Object owner, Connection flushConnection) {
        if (connection != null && connection != flushConnection) {
            poison(
                    "the JDBC connection that held flushed but uncommitted batches was replaced"
                            + " before another batch was flushed");
            lastFlushByOwner.clear();
        }
        connection = flushConnection;
        lastFlushByOwner.put(owner, ++flushSequence);
    }

    /** Sequence number of the latest recorded flush, used to mark a savepoint. */
    public synchronized long currentFlushSequence() {
        return flushSequence;
    }

    /**
     * Whether a failed flush must not be retried: some writer has flushed work in the open
     * transaction, or work has already been lost. Re-sending only the current batch could then
     * commit a partial result.
     */
    public synchronized boolean hasPendingOrLostWork() {
        return connection != null || lostWorkReason != null;
    }

    /** Whether flushed work was lost in this checkpoint interval. */
    public synchronized boolean isPoisoned() {
        return lostWorkReason != null;
    }

    /**
     * Called when a flush failed in a way that ends the transaction on the database side (a lost
     * connection, or SQLState class 40). Pending work is then gone.
     */
    public synchronized void markPendingWorkLost(String reason) {
        if (connection != null) {
            poison(reason);
            connection = null;
            lastFlushByOwner.clear();
        }
    }

    /**
     * Fails unless {@code commitConnection} can be committed without losing flushed work.
     *
     * @param commitConnection the connection a writer is about to commit
     */
    public synchronized void checkCommit(Connection commitConnection) {
        if (lostWorkReason != null) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "Flushed but uncommitted JDBC batches were lost in this checkpoint interval ("
                            + lostWorkReason
                            + "). Failing instead of committing, so the job can recover from"
                            + " the last checkpoint.");
        }
        if (connection != null && connection != commitConnection) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "The JDBC connection that held flushed but uncommitted batches was replaced"
                            + " before commit, so those batches may have been rolled back with it."
                            + " Failing instead of committing the new connection, so the job can"
                            + " recover from the last checkpoint.");
        }
    }

    /**
     * Called after {@code committedConnection} was committed successfully. This is the only normal
     * path that clears the pending work, and it clears it for every writer sharing the connection,
     * because the commit covered all of their batches.
     */
    public synchronized void markCommitted(Connection committedConnection) {
        if (connection == committedConnection) {
            connection = null;
            lastFlushByOwner.clear();
        }
    }

    /**
     * Called after {@code owner} rolled back the whole transaction of {@code rollbackConnection}.
     *
     * <p>The rollback ends that transaction, so nothing is pending on it any more. It poisons the
     * interval if it discarded flushed work that nobody reports: work of another writer, or the
     * owner's own work when {@code ownWorkReported} is false. Row-level error handling reports the
     * rows it rolls back to the row-error collector, so only that path passes true.
     *
     * @param owner the writer that rolled back
     * @param rollbackConnection the connection that was rolled back
     * @param ownWorkReported whether the owner reports its own discarded rows elsewhere
     */
    public synchronized void markRolledBack(
            Object owner, Connection rollbackConnection, boolean ownWorkReported) {
        if (connection == null) {
            return;
        }
        if (connection != rollbackConnection) {
            // The pending work lived on a connection that is already gone.
            poison("the JDBC connection that held flushed but uncommitted batches was replaced");
            connection = null;
            lastFlushByOwner.clear();
            return;
        }
        boolean othersFlushed = false;
        for (Object flushedOwner : lastFlushByOwner.keySet()) {
            if (flushedOwner != owner) {
                othersFlushed = true;
                break;
            }
        }
        if (othersFlushed) {
            poison("a rollback by one writer discarded batches flushed by another writer");
        } else if (!ownWorkReported) {
            poison("a rollback discarded flushed but uncommitted batches");
        }
        connection = null;
        lastFlushByOwner.clear();
    }

    /**
     * Called after {@code owner} rolled back to a savepoint it took at {@code savepointSequence}.
     *
     * <p>The work flushed before the savepoint stays in the open transaction, so the pending state
     * is kept. If another writer flushed after the savepoint, its batch was discarded too, so the
     * interval is poisoned.
     */
    public synchronized void markRolledBackToSavepoint(Object owner, long savepointSequence) {
        for (Map.Entry<Object, Long> entry : lastFlushByOwner.entrySet()) {
            if (entry.getKey() != owner && entry.getValue() > savepointSequence) {
                poison(
                        "a savepoint rollback by one writer discarded a batch flushed by another"
                                + " writer");
                return;
            }
        }
    }

    private void poison(String reason) {
        if (lostWorkReason == null) {
            lostWorkReason = reason;
        }
    }
}
