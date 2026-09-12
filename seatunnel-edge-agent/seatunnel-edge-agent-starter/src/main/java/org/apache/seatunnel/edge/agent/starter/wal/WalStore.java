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

package org.apache.seatunnel.edge.agent.starter.wal;

import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore;

import java.util.List;

public interface WalStore extends AutoCloseable {

    /**
     * Returns the source-position store associated with this WAL store, or {@code null} if position
     * tracking is not supported.
     */
    EdgeSourcePositionStore sourcePositionStore();

    /**
     * Persists one outbound event and assigns a monotonic row id.
     *
     * <p>Called from the scheduler when flushing the in-memory batch buffer. Assigns a monotonic
     * {@code batch_id} (from {@code edge_agent_meta}) stored on the row; transport uses that value
     * as the EdgeSocket {@code batchId}. The returned value is the durable WAL row id (for {@code
     * ack}).
     *
     * @param event connector-produced event (payload and metadata copied into the row)
     * @return durable WAL row id (positive)
     * @throws Exception if the SQLite insert fails
     */
    long append(EdgeEvent event) throws Exception;

    /**
     * Claims up to {@code maxRecords} rows in {@code PENDING} state for sending.
     *
     * <p>Each claimed row transitions to {@code SENDING}. The scheduler passes claimed rows to
     * transport; on send failure the row remains {@code SENDING} until {@code resurrectSending} or
     * a later claim cycle recovers it.
     *
     * @param maxRecords upper bound on rows to claim in one call
     * @param maxAttempts maximum {@code attempt_count} before a row is dead-lettered (exclusive)
     * @return claimed records (possibly empty); never {@code null}
     * @throws Exception if the database update fails
     */
    List<WalRecord> claimPending(int maxRecords, int maxAttempts) throws Exception;

    /**
     * Marks {@code PENDING} rows that exceeded the attempt budget as {@code DEAD}.
     *
     * <p>Called before {@code claimPending} so exhausted rows do not block the queue head.
     *
     * @param maxAttempts same threshold as {@code claimPending}
     * @param maxRecords cap on rows to mark per call
     * @return number of rows moved to {@code DEAD}
     * @throws Exception if the database update fails
     */
    int markExceededAsDead(int maxAttempts, int maxRecords) throws Exception;

    /**
     * Marks a row as successfully delivered after transport returns {@code RECEIVED}.
     *
     * <p>Transitions {@code SENDING} → {@code ACKED}. Must not be called before transport confirms
     * the batch; premature ack can lose data on downstream failure.
     *
     * @param recordId id returned from {@code append}
     * @throws Exception if the update fails or the row is not in {@code SENDING}
     */
    void ack(long recordId) throws Exception;

    /**
     * Recovers rows stuck in {@code SENDING} after a crash or hung send.
     *
     * <p>Called periodically from the scheduler main loop. Rows exceeding the configured attempt
     * budget may be excluded from future claims (implementation-specific). Returns the number of
     * rows moved back to {@code PENDING}.
     *
     * @param maxRecords cap on rows to resurrect per call
     * @return count of rows reset to {@code PENDING}
     * @throws Exception if the database update fails
     */
    int resurrectSending(int maxRecords) throws Exception;

    /**
     * Recovers rows stuck in {@code SENDING} that are older than {@code staleThresholdMs}.
     *
     * <p>Only rows whose {@code updated_at} is at least {@code staleThresholdMs} in the past are
     * eligible. This prevents resurrecting rows that are still actively being sent.
     *
     * @param maxRecords cap on rows to resurrect per call
     * @param staleThresholdMs minimum age (ms) of SENDING rows before resurrection; 0 means no
     *     threshold (same as {@link #resurrectSending(int)})
     * @return count of rows reset to {@code PENDING}
     * @throws Exception if the database update fails
     */
    int resurrectSending(int maxRecords, long staleThresholdMs) throws Exception;

    /**
     * Deletes old {@code ACKED} rows to bound SQLite growth.
     *
     * <p>Called from the scheduler after {@code resurrectSending}. Only rows acked longer than
     * {@code retentionMs} ago are eligible.
     *
     * @param retentionMs minimum age of acked rows before deletion
     * @param maxRecords maximum rows to delete per call
     * @return number of rows deleted
     * @throws Exception if the delete fails
     */
    int cleanupAcked(long retentionMs, int maxRecords) throws Exception;

    @Override
    default void close() throws Exception {}
}
