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

import org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb.source.reader.wal.GaussDBWalFetchTask;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Tests transaction-boundary and parallel-order guarantees of the mppdb WAL buffer. */
class MppdbTransactionBufferTest {

    /** Budget large enough that ordering tests never reach the capacity guard. */
    private static final long UNBOUNDED = Long.MAX_VALUE;

    /** Verifies DML remains unavailable until the matching COMMIT is decoded. */
    @Test
    void testReleaseOnlyAfterCommit() {
        MppdbTransactionBuffer buffer = new MppdbTransactionBuffer(UNBOUNDED);

        Assertions.assertTrue(buffer.add(change(10, 7, MppdbWalChange.Type.BEGIN)).isEmpty());
        Assertions.assertTrue(buffer.add(change(11, 7, MppdbWalChange.Type.INSERT)).isEmpty());
        List<MppdbTransactionBuffer.CommittedTransaction> committed =
                buffer.add(change(12, 7, MppdbWalChange.Type.COMMIT));

        Assertions.assertEquals(1, committed.size());
        Assertions.assertEquals(7, committed.get(0).getTransactionId());
        Assertions.assertEquals(12, committed.get(0).getCommitLsn());
        Assertions.assertEquals(1, committed.get(0).getChanges().size());
    }

    /** Verifies a later parallel COMMIT cannot pass an unfinished earlier transaction. */
    @Test
    void testReleaseContiguousCommittedPrefix() {
        MppdbTransactionBuffer buffer = new MppdbTransactionBuffer(UNBOUNDED);
        buffer.add(change(20, 1, MppdbWalChange.Type.BEGIN));
        buffer.add(change(21, 2, MppdbWalChange.Type.BEGIN));
        buffer.add(change(22, 1, MppdbWalChange.Type.INSERT));
        buffer.add(change(23, 2, MppdbWalChange.Type.UPDATE));

        Assertions.assertTrue(buffer.add(change(20, 2, MppdbWalChange.Type.COMMIT)).isEmpty());
        List<MppdbTransactionBuffer.CommittedTransaction> committed =
                buffer.add(change(30, 1, MppdbWalChange.Type.COMMIT));

        Assertions.assertEquals(2, committed.size());
        Assertions.assertEquals(1, committed.get(0).getTransactionId());
        Assertions.assertEquals(2, committed.get(1).getTransactionId());
        Assertions.assertEquals(30, GaussDBWalFetchTask.maximumCommitLsn(committed, 10));
    }

    /** Verifies binary records inherit the transaction id first exposed by COMMIT. */
    @Test
    void testAnonymousBinaryTransaction() {
        MppdbTransactionBuffer buffer = new MppdbTransactionBuffer(UNBOUNDED);
        buffer.add(change(30, 0, MppdbWalChange.Type.BEGIN));
        buffer.add(change(31, 0, MppdbWalChange.Type.DELETE));

        List<MppdbTransactionBuffer.CommittedTransaction> committed =
                buffer.add(change(32, 99, MppdbWalChange.Type.COMMIT));

        Assertions.assertEquals(1, committed.size());
        Assertions.assertEquals(99, committed.get(0).getTransactionId());
        Assertions.assertEquals(
                MppdbWalChange.Type.DELETE, committed.get(0).getChanges().get(0).getType());
    }

    /** Verifies anonymous DML fails instead of being guessed across parallel transactions. */
    @Test
    void testRejectAmbiguousAnonymousDml() {
        MppdbTransactionBuffer buffer = new MppdbTransactionBuffer(UNBOUNDED);
        buffer.add(change(40, 1, MppdbWalChange.Type.BEGIN));
        buffer.add(change(41, 2, MppdbWalChange.Type.BEGIN));

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> buffer.add(change(42, 0, MppdbWalChange.Type.INSERT)));
    }

    /**
     * Verifies idle warnings are rate limited and cannot release a blocked committed transaction.
     */
    @Test
    void testStalledPrefixWarningDoesNotChangeReleaseOrder() {
        AtomicLong now = new AtomicLong();
        MppdbTransactionBuffer buffer = new MppdbTransactionBuffer(UNBOUNDED, now::get);
        Assertions.assertFalse(buffer.warnIfStalled());
        buffer.add(change(10, 1, MppdbWalChange.Type.BEGIN));
        buffer.add(change(11, 2, MppdbWalChange.Type.BEGIN));
        buffer.add(change(12, 2, MppdbWalChange.Type.INSERT));
        Assertions.assertTrue(buffer.add(change(13, 2, MppdbWalChange.Type.COMMIT)).isEmpty());

        now.set(TimeUnit.MINUTES.toNanos(5) - 1);
        Assertions.assertFalse(buffer.warnIfStalled());
        now.incrementAndGet();
        Assertions.assertTrue(buffer.warnIfStalled());
        Assertions.assertFalse(buffer.warnIfStalled());
        now.set(TimeUnit.MINUTES.toNanos(10));
        Assertions.assertTrue(buffer.warnIfStalled());

        List<MppdbTransactionBuffer.CommittedTransaction> committed =
                buffer.add(change(14, 1, MppdbWalChange.Type.COMMIT));
        Assertions.assertEquals(2, committed.size());
        Assertions.assertEquals(1, committed.get(0).getTransactionId());
        Assertions.assertEquals(2, committed.get(1).getTransactionId());
        Assertions.assertEquals(1, committed.get(1).getChanges().size());
        Assertions.assertFalse(buffer.warnIfStalled());

        buffer.add(change(15, 0, MppdbWalChange.Type.BEGIN));
        Assertions.assertFalse(buffer.warnIfStalled());
        now.addAndGet(TimeUnit.MINUTES.toNanos(5));
        Assertions.assertTrue(buffer.warnIfStalled());
        Assertions.assertEquals(1, buffer.add(change(16, 3, MppdbWalChange.Type.COMMIT)).size());
        Assertions.assertFalse(buffer.warnIfStalled());
    }

    // Verifies changing the oldest transaction cannot bypass the task-wide warning cooldown.
    @Test
    void testStallWarningCooldownSurvivesHeadChange() {
        AtomicLong now = new AtomicLong();
        MppdbTransactionBuffer buffer = new MppdbTransactionBuffer(UNBOUNDED, now::get);
        buffer.add(change(10, 1, MppdbWalChange.Type.BEGIN));
        buffer.add(change(11, 2, MppdbWalChange.Type.BEGIN));
        now.set(TimeUnit.MINUTES.toNanos(5));
        Assertions.assertTrue(buffer.warnIfStalled());
        Assertions.assertEquals(1, buffer.add(change(12, 1, MppdbWalChange.Type.COMMIT)).size());
        Assertions.assertFalse(buffer.warnIfStalled());
        now.set(TimeUnit.MINUTES.toNanos(10) - 1);
        Assertions.assertFalse(buffer.warnIfStalled());
        now.incrementAndGet();
        Assertions.assertTrue(buffer.warnIfStalled());
    }

    /** Verifies one oversized transaction fails before its next row is retained. */
    @Test
    void testRejectTransactionLargerThanBudget() {
        MppdbTransactionBuffer buffer = new MppdbTransactionBuffer(2);
        buffer.add(change(10, 1, MppdbWalChange.Type.BEGIN));
        buffer.add(change(11, 1, MppdbWalChange.Type.INSERT));
        buffer.add(change(12, 1, MppdbWalChange.Type.INSERT));

        IllegalStateException failure =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () -> buffer.add(change(13, 1, MppdbWalChange.Type.INSERT)));
        Assertions.assertTrue(failure.getMessage().contains("max-buffered-changes"));
    }

    /**
     * Verifies the budget counts rows blocked behind an open head transaction and is freed again
     * once the prefix is released.
     */
    @Test
    void testBudgetCountsBlockedPrefixAndFreesOnRelease() {
        MppdbTransactionBuffer buffer = new MppdbTransactionBuffer(2);
        buffer.add(change(10, 1, MppdbWalChange.Type.BEGIN));
        buffer.add(change(11, 1, MppdbWalChange.Type.INSERT));
        buffer.add(change(12, 2, MppdbWalChange.Type.BEGIN));
        buffer.add(change(13, 2, MppdbWalChange.Type.INSERT));
        Assertions.assertTrue(buffer.add(change(14, 2, MppdbWalChange.Type.COMMIT)).isEmpty());
        buffer.add(change(15, 3, MppdbWalChange.Type.BEGIN));
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> buffer.add(change(16, 3, MppdbWalChange.Type.INSERT)));

        MppdbTransactionBuffer released = new MppdbTransactionBuffer(2);
        released.add(change(20, 1, MppdbWalChange.Type.BEGIN));
        released.add(change(21, 1, MppdbWalChange.Type.INSERT));
        released.add(change(22, 1, MppdbWalChange.Type.INSERT));
        Assertions.assertEquals(1, released.add(change(23, 1, MppdbWalChange.Type.COMMIT)).size());
        released.add(change(24, 2, MppdbWalChange.Type.BEGIN));
        released.add(change(25, 2, MppdbWalChange.Type.INSERT));
        released.add(change(26, 2, MppdbWalChange.Type.INSERT));
        List<MppdbTransactionBuffer.CommittedTransaction> committed =
                released.add(change(27, 2, MppdbWalChange.Type.COMMIT));
        Assertions.assertEquals(2, committed.get(0).getChanges().size());
    }

    /** Creates a minimal transaction or row record for buffer tests. */
    private MppdbWalChange change(long lsn, long transactionId, MppdbWalChange.Type type) {
        List<MppdbWalChange.ColumnValue> columns =
                Collections.singletonList(
                        new MppdbWalChange.ColumnValue("id", 23, null, "1", false));
        return new MppdbWalChange(
                lsn,
                transactionId,
                type,
                type == MppdbWalChange.Type.BEGIN || type == MppdbWalChange.Type.COMMIT
                        ? null
                        : "public",
                type == MppdbWalChange.Type.BEGIN || type == MppdbWalChange.Type.COMMIT
                        ? null
                        : "customers",
                type == MppdbWalChange.Type.DELETE ? columns : Collections.emptyList(),
                type == MppdbWalChange.Type.INSERT || type == MppdbWalChange.Type.UPDATE
                        ? columns
                        : Collections.emptyList());
    }
}
